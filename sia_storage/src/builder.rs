use crate::time::sleep;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rand::random;
use reqwest::IntoUrl;
use sia_core::seed::{self, Seed};
use sia_core::signing::PrivateKey;
use sia_core::types::Hash256;
use thiserror::Error;
use url::Url;

use crate::app_client::{self, Client};
use crate::egress::{EgressConfig, MAX_BATCH_EVENTS};
use crate::object_encryption::derive;
use crate::time::Duration;
use crate::{AppID, AppKey, AppMetadata, Sdk};

#[cfg(feature = "fs")]
use std::path::PathBuf;

/// The initial state of the SDK builder, before connecting to the indexd service.
pub struct DisconnectedState;

/// The state of the SDK builder after requesting approval for the application.
pub struct RequestingApprovalState {
    response_url: Url,
    register_url: Url,
    status_url: Url,
    expiration: DateTime<Utc>,
}

/// The state of the SDK builder after the application has been approved.
pub struct ApprovedState {
    register_url: Url,
    user_secret: Hash256,
}

/// A builder for creating an SDK instance.
pub struct Builder<S> {
    ephemeral_key: PrivateKey,
    state: S,
    client: Client,
    app_meta: AppMetadata,
    egress: EgressConfig,
}

/// Errors that can occur during the SDK building process.
#[derive(Error, Debug)]
pub enum BuilderError {
    /// A URL could not be parsed.
    #[error("url error: {0}")]
    Url(#[from] url::ParseError),

    /// An error from the indexer API client.
    #[error("client error: {0}")]
    Client(#[from] app_client::Error),

    /// A transport-level connection error.
    #[error("transport error: {0}")]
    Transport(String),

    /// The recovery phrase is invalid.
    #[error("mnemonic error: {0}")]
    Mnemonic(#[from] seed::SeedError),

    /// The connection approval request expired before the user approved it.
    #[error("request expired")]
    RequestExpired,
}

impl Builder<DisconnectedState> {
    /// Creates a new SDK builder with the provided indexer URL.
    ///
    /// After creating the builder, call [Builder::connected] to attempt
    /// to connect using an existing app key, or [Builder::request_connection]
    /// to request a new connection.
    ///
    /// # Example
    /// ```rust
    /// use sia_storage::{AppMetadata, Builder, app_id};
    ///
    /// const APP_META: AppMetadata = AppMetadata {
    ///     id: app_id!("a9f0bda1b97b7d44ae6369ac830851a115311bb59aa2d848beda6ae95d10ad18"),
    ///     name: "My App",
    ///     description: "My App Description",
    ///     service_url: "https://myapp.com",
    ///     logo_url: Some("https://myapp.com/logo.png"),
    ///     callback_url: Some("https://myapp.com/callback"),
    /// };
    ///
    /// let builder = Builder::new("https://sia.storage", APP_META).expect("failed to create builder");
    /// ```
    pub fn new<U: IntoUrl>(indexer_url: U, app_meta: AppMetadata) -> Result<Self, BuilderError> {
        let client = Client::new(indexer_url)?;
        Ok(Self {
            ephemeral_key: PrivateKey::from_seed(&random::<[u8; 32]>()),
            state: DisconnectedState,
            client,
            app_meta,
            egress: EgressConfig::default(),
        })
    }

    /// Attempts to connect using the provided app key.
    /// If the app key is valid, returns Some([Sdk]), otherwise returns None.
    ///
    /// If you receive None, call [Builder::request_connection] to request a new connection.
    ///
    /// # Arguments
    /// * `app_key` - The application key used for authentication.
    pub async fn connected(&self, app_key: &AppKey) -> Result<Option<Sdk>, BuilderError> {
        let connected = self.client.check_app_authenticated(&app_key.0).await?;
        if !connected {
            return Ok(None);
        }
        let app_key = Arc::new(app_key.clone());
        let egress = self.egress.reporter(self.client.clone(), app_key.clone());
        let sdk = Sdk::new(self.client.clone(), app_key, egress).await?;
        Ok(Some(sdk))
    }

    /// Requests a new connection for the application.
    ///
    /// # Arguments
    /// * `app` - Details of the application requesting connection.
    pub async fn request_connection(
        self,
    ) -> Result<Builder<RequestingApprovalState>, BuilderError> {
        let resp = self
            .client
            .request_app_connection(&self.ephemeral_key, &self.app_meta)
            .await?;
        Ok(Builder {
            ephemeral_key: self.ephemeral_key,
            app_meta: self.app_meta,
            state: RequestingApprovalState {
                response_url: Url::parse(&resp.response_url)?,
                register_url: Url::parse(&resp.register_url)?,
                status_url: Url::parse(&resp.status_url)?,
                expiration: resp.expiration,
            },
            client: self.client,
            egress: self.egress,
        })
    }
}

impl Builder<RequestingApprovalState> {
    /// Returns the response URL for the registration process. This
    /// should be displayed to the user so they can authorize the
    /// application.
    pub fn response_url(&self) -> &str {
        self.state.response_url.as_str()
    }

    /// Waits for the application registration to be approved. This
    /// polls the status URL until the registration is approved or
    /// rejected. This can take several minutes depending on user action.
    ///
    /// [Builder::response_url] should be displayed to the user
    /// before calling this method.
    pub async fn wait_for_approval(self) -> Result<Builder<ApprovedState>, BuilderError> {
        loop {
            if Utc::now() >= self.state.expiration {
                return Err(BuilderError::RequestExpired);
            }

            if let Some(user_secret) = self
                .client
                .check_request_status(&self.ephemeral_key, self.state.status_url.clone())
                .await?
            {
                return Ok(Builder {
                    ephemeral_key: self.ephemeral_key,
                    state: ApprovedState {
                        register_url: self.state.register_url.clone(),
                        user_secret,
                    },
                    app_meta: self.app_meta,
                    client: self.client,
                    egress: self.egress,
                });
            }
            sleep(Duration::from_secs(5)).await;
        }
    }
}

impl Builder<ApprovedState> {
    /// Completes the registration process and returns an SDK instance.
    ///
    /// # Arguments
    /// * `mnemonic` - The user's mnemonic phrase used to derive the application key.
    ///
    /// # Errors
    /// Returns [BuilderError] if the registration fails or the SDK cannot be created.
    pub async fn register(self, mnemonic: &str) -> Result<Sdk, BuilderError> {
        let private_key = derive_app_key(mnemonic, &self.app_meta.id, &self.state.user_secret)?;
        self.client
            .register_app(
                &self.ephemeral_key,
                &private_key,
                self.state.register_url.clone(),
            )
            .await?;
        let app_key = Arc::new(AppKey(private_key));
        let egress = self.egress.reporter(self.client.clone(), app_key.clone());
        Sdk::new(self.client, app_key, egress).await
    }
}

impl<S> Builder<S> {
    /// Sets the URL that signed egress batches are reported to (P3b).
    ///
    /// Egress reporting is disabled until this is set.
    pub fn egress_url(mut self, url: Url) -> Self {
        self.egress.url = Some(url);
        self
    }

    /// Sets how often queued egress events are flushed. Defaults to 30s.
    pub fn egress_interval(mut self, interval: Duration) -> Self {
        self.egress.interval = interval;
        self
    }

    /// Sets the maximum number of events per reported batch. Defaults to 500.
    /// Values above 1000 are clamped to 1000, the server's per-batch event
    /// limit.
    pub fn egress_batch_max(mut self, batch_max: usize) -> Self {
        self.egress.batch_max = batch_max.min(MAX_BATCH_EVENTS);
        self
    }

    /// Sets the path of the on-disk retry spool. Envelopes that fail to send
    /// are appended to it and retried before new events.
    #[cfg(feature = "fs")]
    pub fn egress_spool_path(mut self, path: PathBuf) -> Self {
        self.egress.spool_path = Some(path);
        self
    }
}

/// A helper function to derive an application key from a
/// mnemonic, app ID, and shared secret.
///
/// It is exposed to be able to test the app key derivation logic.
fn derive_app_key(
    mnemonic: &str,
    app_id: &AppID,
    shared_secret: &Hash256,
) -> Result<PrivateKey, BuilderError> {
    const KEY_DOMAIN: &[u8] = b"indexd app key derivation";
    let seed = Seed::new(mnemonic)?;
    let mut key = [0u8; 64];
    key[..32].copy_from_slice(seed.entropy());
    key[32..].copy_from_slice(shared_secret.as_ref());
    let mut okm = [0u8; 32];
    derive(&key, app_id.as_ref(), KEY_DOMAIN, &mut okm);
    Ok(PrivateKey::from_seed(&okm))
}

#[cfg(test)]
mod test {
    use crate::app_id;

    use super::*;
    use sia_core::hash_256;
    use sia_core::types::Hash256;

    #[sia_core_derive::cross_target_test]
    fn test_app_key_derivation_golden() {
        const MNEMONIC: &str =
            "glare own entire dish exact open theme family harsh room scrap rose";
        const APP_ID: AppID =
            app_id!("0e90d697f5045a6593f1c43ebf79a369e2bc72cc5c7b6282f3b5aeb0de6e4005");
        const SHARED_SECRET: Hash256 =
            hash_256!("cf02d945fe4bfe614d823dc13c19aa8501699e656d0f7915490c3056d5c97dc6");
        const EXPECTED_APP_KEY: &str =
            "b75061f34bb3aeab232b0671da2d0347c547343a0026bb5535c291d964fd09a1";

        let mut seed = [0u8; 32];
        hex::decode_to_slice(EXPECTED_APP_KEY, &mut seed).expect("decoding failed");
        let expected_app_key = PrivateKey::from_seed(&seed);

        let derived_app_key =
            derive_app_key(MNEMONIC, &APP_ID, &SHARED_SECRET).expect("derivation failed");
        assert_eq!(derived_app_key, expected_app_key);
    }

    #[test]
    fn test_egress_config_setters() {
        let meta = AppMetadata {
            id: AppID::new([0u8; 32]),
            name: "test",
            description: "test",
            service_url: "https://service.test",
            logo_url: None,
            callback_url: None,
        };
        let url: Url = "https://egress.test/api/v1/usage/egress".parse().unwrap();
        let builder = Builder::new("https://sia.test", meta)
            .expect("builder")
            .egress_url(url.clone())
            .egress_interval(Duration::from_secs(5))
            .egress_batch_max(7);
        assert_eq!(builder.egress.url.as_ref(), Some(&url));
        assert_eq!(builder.egress.interval, Duration::from_secs(5));
        assert_eq!(builder.egress.batch_max, 7);
    }

    /// The server rejects batches above 1000 events, so the builder clamps
    /// `egress_batch_max` to that limit.
    #[test]
    fn test_egress_batch_max_is_clamped() {
        let meta = AppMetadata {
            id: AppID::new([0u8; 32]),
            name: "test",
            description: "test",
            service_url: "https://service.test",
            logo_url: None,
            callback_url: None,
        };
        let builder = Builder::new("https://sia.test", meta)
            .expect("builder")
            .egress_batch_max(5000);
        assert_eq!(builder.egress.batch_max, crate::egress::MAX_BATCH_EVENTS);
        assert!(builder.egress.batch_max <= 1000);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn test_egress_spool_path_setter() {
        let meta = AppMetadata {
            id: AppID::new([0u8; 32]),
            name: "test",
            description: "test",
            service_url: "https://service.test",
            logo_url: None,
            callback_url: None,
        };
        let spool = std::path::PathBuf::from("/tmp/tessera-egress.spool");
        let builder = Builder::new("https://sia.test", meta)
            .expect("builder")
            .egress_spool_path(spool.clone());
        assert_eq!(builder.egress.spool_path, Some(spool));
    }
}
