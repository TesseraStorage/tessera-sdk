use std::sync::Arc;

use sia_core::signing::{PrivateKey, PublicKey};
use sia_core::types::v2::{NetAddress, Protocol};

use crate::rhp4::{Client, mock};
use crate::time::Duration;
use crate::{AppKey, BuilderError, Host, Sdk, app_client};

/// An in-memory Sia network: a mock indexer paired with a mock host transport.
///
/// [`MockNetwork::sdk`] builds a real [`Sdk`] against them, so tests drive the
/// same public API an application uses rather than a parallel mock surface.
pub struct MockNetwork {
    api: app_client::mock::Client,
    transport: mock::Client,
}

impl MockNetwork {
    pub fn new() -> Self {
        Self {
            api: app_client::mock::Client::new(),
            transport: mock::Client::new(),
        }
    }

    /// Adds `n` hosts that are usable for uploads and returns their keys.
    ///
    /// Hosts must be added before [`MockNetwork::sdk`]; the SDK reads the
    /// indexer's host list when it is created and then on a long interval.
    pub fn add_hosts(&self, n: usize) -> Vec<PublicKey> {
        let hosts: Vec<Host> = (0..n)
            .map(|_| Host {
                public_key: PrivateKey::from_seed(&rand::random::<[u8; 32]>()).public_key(),
                addresses: vec![NetAddress {
                    protocol: Protocol::QUIC,
                    address: "localhost:9984".to_string(),
                }],
                country_code: "US".to_string(),
                latitude: 0.0,
                longitude: 0.0,
                good_for_upload: true,
            })
            .collect();
        let keys = hosts.iter().map(|h| h.public_key).collect();
        self.api.add_hosts(hosts);
        keys
    }

    /// Sets the given hosts as "slow" - they will sleep for the specified
    /// duration before completing any write_sector or read_sector operation.
    pub fn set_slow_hosts(&self, hosts: impl IntoIterator<Item = PublicKey>, delay: Duration) {
        self.transport.set_slow_hosts(hosts, delay);
    }

    /// Clears all slow host settings.
    pub fn reset_slow_hosts(&self) {
        self.transport.reset_slow_hosts();
    }

    /// Discards every sector stored by the hosts. The indexer's objects and
    /// slabs are untouched, so anything pinned before this becomes
    /// undownloadable.
    pub fn clear_sectors(&self) {
        self.transport.clear();
    }

    /// The number of slabs currently pinned to the mock indexer.
    pub fn pinned_slabs(&self) -> usize {
        self.api.pinned_slabs()
    }

    /// Builds an [`Sdk`] backed by this network.
    pub async fn sdk(&self, app_key: AppKey) -> Result<Sdk, BuilderError> {
        Sdk::with_backends(
            app_client::Client::Mock(self.api.clone()),
            Client::Mock(self.transport.clone()),
            Arc::new(app_key),
            None,
        )
        .await
    }

    /// Builds an [`Sdk`] backed by this network that reports served-shard
    /// egress through `egress` (test-only).
    #[cfg(test)]
    pub(crate) async fn sdk_with_egress(
        &self,
        app_key: AppKey,
        egress: Arc<crate::EgressReporter>,
    ) -> Result<Sdk, BuilderError> {
        Sdk::with_backends(
            app_client::Client::Mock(self.api.clone()),
            Client::Mock(self.transport.clone()),
            Arc::new(app_key),
            Some(egress),
        )
        .await
    }
}

impl Default for MockNetwork {
    fn default() -> Self {
        Self::new()
    }
}

/// Test-only capture for the egress hook: a mocked reporting endpoint paired
/// with the reporter the read path records through.
///
/// The reporter is a real [`EgressReporter`](crate::EgressReporter), so tests
/// assert on the canonical signed bodies the mocked endpoint received instead
/// of reaching into private reporter state. Compiled on native and wasm test
/// targets; the transport mock itself is
/// [`app_client::mock::Client`](crate::app_client::mock::Client).
#[cfg(test)]
pub(crate) mod capture {
    use std::sync::Arc;

    use serde_json::Value;
    use sia_core::signing::PublicKey;
    use sia_core::types::Hash256;

    use crate::egress::EgressReporter;
    use crate::time::Duration;
    use crate::{AppKey, Url, app_client};

    /// One decoded event from a captured egress envelope.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct RecordedEvent {
        pub(crate) host_key: PublicKey,
        pub(crate) slab_id: Option<Hash256>,
        pub(crate) shard_index: u64,
        pub(crate) bytes: u64,
    }

    /// A mocked egress endpoint plus the reporter that posts to it.
    pub(crate) struct MockEgress {
        api: app_client::mock::Client,
        reporter: Arc<EgressReporter>,
    }

    impl MockEgress {
        /// Builds a capture whose reporter flushes on `interval`/`batch_max`
        /// exactly like production. Tests that want only explicit flushes pass
        /// a long interval.
        pub(crate) fn new(interval: Duration, batch_max: usize) -> Self {
            let api = app_client::mock::Client::new();
            let app_key = Arc::new(AppKey::import([0x42; 32]));
            let url: Url = "https://egress.test/api/v1/usage/egress"
                .parse()
                .expect("test url");
            let reporter = Arc::new(EgressReporter::new(
                app_client::Client::Mock(api.clone()),
                app_key,
                url,
                interval,
                batch_max,
                #[cfg(feature = "fs")]
                None,
                #[cfg(feature = "fs")]
                None,
            ));
            Self { api, reporter }
        }

        /// The reporter to hand to `Download::new` or `Sdk::with_backends`.
        pub(crate) fn reporter(&self) -> Arc<EgressReporter> {
            self.reporter.clone()
        }

        /// Makes the next `times` sends fail with a retryable transport error.
        #[cfg(not(target_arch = "wasm32"))]
        pub(crate) fn fail_sends(&self, times: usize) {
            self.api.fail_egress_reports(times);
        }

        /// The raw body of every captured report attempt, in order. Used by
        /// [`MockEgress::recorded`] and by tests that assert the signed
        /// envelope contract itself.
        pub(crate) fn attempts(&self) -> Vec<Vec<u8>> {
            self.api.egress_attempts()
        }

        /// Force-flushes and returns every event the endpoint received, in
        /// order. A retried envelope contributes its events again.
        pub(crate) async fn recorded(&self) -> Vec<RecordedEvent> {
            self.reporter.flush_now().await;
            self.attempts()
                .iter()
                .flat_map(|body| decode_events(body))
                .collect()
        }
    }

    /// Decodes the `events` array of one canonical envelope.
    fn decode_events(body: &[u8]) -> Vec<RecordedEvent> {
        let envelope: Value = serde_json::from_slice(body).expect("egress envelope is json");
        envelope["events"]
            .as_array()
            .expect("envelope has an events array")
            .iter()
            .map(|event| RecordedEvent {
                host_key: event["host_key"]
                    .as_str()
                    .expect("host_key is a string")
                    .parse()
                    .expect("host_key is a public key"),
                slab_id: event["slab_id"]
                    .as_str()
                    .map(|id| id.parse().expect("slab_id is a hash")),
                shard_index: event["shard_index"]
                    .as_u64()
                    .expect("shard_index is a number"),
                bytes: event["bytes"].as_u64().expect("bytes is a number"),
            })
            .collect()
    }
}
