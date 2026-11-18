//! Agent hooks: optional callbacks that report completed transfers and
//! failed writes. See [`AgentHooks`].

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use log::warn;
use sia_core::types::Hash256;

use crate::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type HookFn<E> = Arc<dyn Fn(E) + Send + Sync + 'static>;

#[cfg(target_arch = "wasm32")]
pub(crate) type HookFn<E> = Arc<dyn Fn(E) + 'static>;

/// A successful [`Sdk::upload`](crate::Sdk::upload) call.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UploadCompleteEvent {
    /// The object's ID after this call. Writing data to an object changes
    /// its ID.
    pub object_id: Hash256,
    /// The object's size in bytes after this call.
    pub size: u64,
    /// The number of slabs in the object after this call.
    pub slabs: usize,
    /// The number of shard writes that hosts acknowledged during this call.
    pub shards_acknowledged: usize,
    /// The time from the start of the call to its return.
    pub elapsed: Duration,
}

/// A download that delivered every requested byte.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DownloadCompleteEvent {
    /// The ID of the downloaded object.
    pub object_id: Hash256,
    /// The number of bytes the [`Download`](crate::Download) delivered.
    pub bytes: u64,
    /// The time from the [`Sdk::download`](crate::Sdk::download) call to the
    /// last byte.
    pub elapsed: Duration,
}

/// How far a failed write got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteFailureStage {
    /// [`Sdk::upload`](crate::Sdk::upload) failed before any shard write to
    /// a host started. No object data left the process.
    NotSent,
    /// [`Sdk::upload`](crate::Sdk::upload) failed after at least one shard
    /// write to a host started. Some shard data may be stored on hosts.
    /// Nothing was pinned, so no object exists on the indexer and the stored
    /// shards are not referenced by any object. Retrying with a new
    /// [`Object`](crate::Object) is safe.
    SentNotRegistered,
    /// The data was uploaded but [`Sdk::pin_object`](crate::Sdk::pin_object)
    /// failed, so the object was not saved on the indexer.
    /// [`WriteFailedEvent::slabs_pinned`] reports how many slabs the indexer
    /// had confirmed as pinned before the failure. Pinned slabs without an
    /// object stay on the account until the object is pinned or
    /// [`Sdk::prune_slabs`](crate::Sdk::prune_slabs) removes them. Calling
    /// `pin_object` again with the same object is safe.
    PinFailed,
}

impl fmt::Display for WriteFailureStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotSent => "not_sent",
            Self::SentNotRegistered => "sent_not_registered",
            Self::PinFailed => "pin_failed",
        })
    }
}

/// A failed [`Sdk::upload`](crate::Sdk::upload) or
/// [`Sdk::pin_object`](crate::Sdk::pin_object) call.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteFailedEvent {
    /// How far the write got.
    pub stage: WriteFailureStage,
    /// The error the method returned, formatted with `Display`.
    pub error: String,
    /// The ID of the object that failed to pin. Set only for
    /// [`WriteFailureStage::PinFailed`]; an upload that did not finish has
    /// no stable ID.
    pub object_id: Option<Hash256>,
    /// The number of shard writes to hosts that started during the upload,
    /// including retries and racing attempts. Zero for
    /// [`WriteFailureStage::PinFailed`].
    pub write_attempts_started: usize,
    /// The number of shard writes that hosts acknowledged during the
    /// upload. Zero for [`WriteFailureStage::PinFailed`].
    pub shards_acknowledged: usize,
    /// The number of slabs the indexer confirmed as pinned before the
    /// failure. Set only for [`WriteFailureStage::PinFailed`].
    pub slabs_pinned: usize,
    /// The time from the start of the call to its return.
    pub elapsed: Duration,
}

/// Optional callbacks that report the outcome of transfers, for example to
/// an agent that needs to record what it stored. Attach them with
/// [`Sdk::with_agent_hooks`](crate::Sdk::with_agent_hooks). An `Sdk`
/// without hooks behaves exactly as before.
///
/// Hooks are fire-and-forget: the SDK ignores their result, and a panic
/// inside a hook is caught and logged without changing the result of the
/// transfer. A hook runs on the task that finished the transfer, so keep it
/// short and hand longer work to a channel or a spawned task. On targets
/// built with `panic = "abort"`, such as most wasm builds, a panic cannot be
/// caught.
///
/// ```
/// use tessera_storage::AgentHooks;
///
/// let hooks = AgentHooks::new()
///     .on_upload_complete(|ev| println!("uploaded {} ({} bytes)", ev.object_id, ev.size))
///     .on_write_failed(|ev| eprintln!("write failed ({}): {}", ev.stage, ev.error));
/// ```
#[derive(Clone, Default)]
pub struct AgentHooks {
    pub(crate) upload_complete: Option<HookFn<UploadCompleteEvent>>,
    pub(crate) download_complete: Option<HookFn<DownloadCompleteEvent>>,
    pub(crate) write_failed: Option<HookFn<WriteFailedEvent>>,
}

impl fmt::Debug for AgentHooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentHooks")
            .field("upload_complete", &self.upload_complete.is_some())
            .field("download_complete", &self.download_complete.is_some())
            .field("write_failed", &self.write_failed.is_some())
            .finish()
    }
}

macro_rules! hook_setter {
    ($(#[$doc:meta])* $name:ident, $field:ident, $event:ty) => {
        $(#[$doc])*
        #[cfg(not(target_arch = "wasm32"))]
        pub fn $name<F>(mut self, callback: F) -> Self
        where
            F: Fn($event) + Send + Sync + 'static,
        {
            self.$field = Some(Arc::new(callback));
            self
        }

        $(#[$doc])*
        #[cfg(target_arch = "wasm32")]
        pub fn $name<F>(mut self, callback: F) -> Self
        where
            F: Fn($event) + 'static,
        {
            self.$field = Some(Arc::new(callback));
            self
        }
    };
}

impl AgentHooks {
    /// Creates an empty set of hooks.
    pub fn new() -> Self {
        Self::default()
    }

    hook_setter!(
        /// Called after [`Sdk::upload`](crate::Sdk::upload) returns `Ok`. The
        /// data is stored on hosts but is not tracked by the indexer until
        /// [`Sdk::pin_object`](crate::Sdk::pin_object) succeeds.
        on_upload_complete,
        upload_complete,
        UploadCompleteEvent
    );

    hook_setter!(
        /// Called when a [`Download`](crate::Download) returned by
        /// [`Sdk::download`](crate::Sdk::download) has delivered its last
        /// byte. A download that is dropped early or fails does not call it.
        on_download_complete,
        download_complete,
        DownloadCompleteEvent
    );

    hook_setter!(
        /// Called when [`Sdk::upload`](crate::Sdk::upload) or
        /// [`Sdk::pin_object`](crate::Sdk::pin_object) returns an error.
        /// [`WriteFailedEvent::stage`] tells whether any data was sent.
        on_write_failed,
        write_failed,
        WriteFailedEvent
    );

    pub(crate) fn observes_uploads(&self) -> bool {
        self.upload_complete.is_some() || self.write_failed.is_some()
    }
}

/// Calls a hook and catches a panic so it can never fail a transfer.
pub(crate) fn fire<E>(name: &str, hook: &HookFn<E>, event: E) {
    if catch_unwind(AssertUnwindSafe(|| hook(event))).is_err() {
        warn!("agent hook {name} panicked; the transfer result is unaffected");
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};

    use sia_core::signing::PrivateKey;
    use sia_core::types::v2::NetAddress;
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::rhp4::Client;
    use crate::{AppKey, DownloadOptions, Host, Object, Sdk, UploadOptions, app_client};

    fn hosts(n: usize, with_addresses: bool) -> Vec<Host> {
        (0..n)
            .map(|_| Host {
                public_key: PrivateKey::from_seed(&rand::random::<[u8; 32]>()).public_key(),
                addresses: if with_addresses {
                    vec![NetAddress {
                        protocol: sia_core::types::v2::Protocol::QUIC,
                        address: "localhost:9984".to_string(),
                    }]
                } else {
                    // the mock transport fails every write to these hosts
                    Vec::new()
                },
                country_code: "US".to_string(),
                latitude: 0.0,
                longitude: 0.0,
                good_for_upload: true,
            })
            .collect()
    }

    async fn sdk(api: &app_client::mock::Client) -> Sdk {
        Sdk::with_backends(
            app_client::Client::Mock(api.clone()),
            Client::mock(),
            Arc::new(AppKey::import(rand::random::<[u8; 32]>())),
        )
        .await
        .expect("sdk creation failed")
    }

    struct Recorder {
        uploads: mpsc::Receiver<UploadCompleteEvent>,
        downloads: mpsc::Receiver<DownloadCompleteEvent>,
        failures: mpsc::Receiver<WriteFailedEvent>,
    }

    fn recording_hooks() -> (AgentHooks, Recorder) {
        let (up_tx, uploads) = mpsc::channel();
        let (dl_tx, downloads) = mpsc::channel();
        let (fail_tx, failures) = mpsc::channel();
        let hooks = AgentHooks::new()
            .on_upload_complete(move |ev| up_tx.send(ev).unwrap())
            .on_download_complete(move |ev| dl_tx.send(ev).unwrap())
            .on_write_failed(move |ev| fail_tx.send(ev).unwrap());
        (
            hooks,
            Recorder {
                uploads,
                downloads,
                failures,
            },
        )
    }

    #[tokio::test]
    async fn test_hooks_upload_download_complete() {
        let api = app_client::mock::Client::new();
        api.add_hosts(hosts(40, true));
        let (hooks, rec) = recording_hooks();
        let sdk = sdk(&api).await.with_agent_hooks(hooks);

        // the caller's own progress callback still sees every shard
        let progress = Arc::new(AtomicUsize::new(0));
        let progress_clone = progress.clone();
        let options = UploadOptions::default().on_shard_uploaded(move |_| {
            progress_clone.fetch_add(1, Ordering::SeqCst);
        });
        let data: Vec<u8> = (0..(1 << 20)).map(|i| i as u8).collect();
        let object = sdk
            .upload(Object::default(), Cursor::new(data.clone()), options)
            .await
            .expect("upload failed");

        // hooks run before the call returns
        let up = rec.uploads.try_recv().expect("no upload event");
        assert_eq!(up.object_id, object.id());
        assert_eq!(up.size, data.len() as u64);
        assert_eq!(up.slabs, 1);
        assert_eq!(up.shards_acknowledged, 30);
        assert_eq!(progress.load(Ordering::SeqCst), 30);

        sdk.pin_object(&object).await.expect("pin failed");
        assert!(rec.failures.try_recv().is_err());

        // AsyncRead path
        let mut reader = sdk
            .download(&object, DownloadOptions::default())
            .await
            .expect("download failed");
        let mut downloaded = Vec::new();
        reader.read_to_end(&mut downloaded).await.unwrap();
        assert_eq!(downloaded, data);
        let dl = rec.downloads.try_recv().expect("no download event");
        assert_eq!(dl.object_id, object.id());
        assert_eq!(dl.bytes, data.len() as u64);
        // EOF reads after the first do not report again
        assert_eq!(reader.read(&mut [0u8; 8]).await.unwrap(), 0);
        assert!(rec.downloads.try_recv().is_err());

        // read_chunk path with a range
        let mut reader = sdk
            .download(
                &object,
                DownloadOptions {
                    offset: 100,
                    length: Some(5000),
                    ..Default::default()
                },
            )
            .await
            .expect("download failed");
        let mut got = Vec::new();
        loop {
            let chunk = reader.read_chunk().await.unwrap();
            if chunk.is_empty() {
                break;
            }
            got.extend(chunk);
        }
        assert_eq!(got, data[100..5100]);
        assert_eq!(rec.downloads.try_recv().expect("no event").bytes, 5000);
    }

    #[tokio::test]
    async fn test_hooks_download_dropped_early() {
        let api = app_client::mock::Client::new();
        api.add_hosts(hosts(40, true));
        let (hooks, rec) = recording_hooks();
        let sdk = sdk(&api).await.with_agent_hooks(hooks);

        let object = sdk
            .upload(
                Object::default(),
                Cursor::new(vec![7u8; 1 << 20]),
                UploadOptions::default(),
            )
            .await
            .unwrap();
        sdk.pin_object(&object).await.unwrap();
        let mut reader = sdk
            .download(&object, DownloadOptions::default())
            .await
            .unwrap();
        let mut buf = [0u8; 10];
        reader.read_exact(&mut buf).await.unwrap();
        drop(reader);
        assert!(rec.downloads.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_hooks_absent() {
        let api = app_client::mock::Client::new();
        api.add_hosts(hosts(40, true));
        let sdk = sdk(&api).await;

        let data = vec![3u8; 100_000];
        let object = sdk
            .upload(
                Object::default(),
                Cursor::new(data.clone()),
                UploadOptions::default(),
            )
            .await
            .unwrap();
        sdk.pin_object(&object).await.unwrap();
        let mut reader = sdk
            .download(&object, DownloadOptions::default())
            .await
            .unwrap();
        let mut downloaded = Vec::new();
        reader.read_to_end(&mut downloaded).await.unwrap();
        assert_eq!(downloaded, data);
        assert!(reader.completion.is_none());
    }

    #[tokio::test]
    async fn test_hooks_panic_does_not_fail_transfer() {
        let api = app_client::mock::Client::new();
        api.add_hosts(hosts(40, true));
        let calls = Arc::new(AtomicUsize::new(0));
        let (c1, c2, c3) = (calls.clone(), calls.clone(), calls.clone());
        let hooks = AgentHooks::new()
            .on_upload_complete(move |_| {
                c1.fetch_add(1, Ordering::SeqCst);
                panic!("upload hook");
            })
            .on_download_complete(move |_| {
                c2.fetch_add(1, Ordering::SeqCst);
                panic!("download hook");
            })
            .on_write_failed(move |_| {
                c3.fetch_add(1, Ordering::SeqCst);
                panic!("failed hook");
            });
        let sdk = sdk(&api).await.with_agent_hooks(hooks);

        let data = vec![5u8; 200_000];
        let object = sdk
            .upload(
                Object::default(),
                Cursor::new(data.clone()),
                UploadOptions::default(),
            )
            .await
            .expect("upload must succeed despite the hook panic");
        sdk.pin_object(&object).await.expect("pin failed");
        let mut reader = sdk
            .download(&object, DownloadOptions::default())
            .await
            .unwrap();
        let mut downloaded = Vec::new();
        reader
            .read_to_end(&mut downloaded)
            .await
            .expect("download must succeed despite the hook panic");
        assert_eq!(downloaded, data);

        // a failing write still returns its own error
        api.set_pin_errors(None, Some("pin down"));
        let err = sdk.pin_object(&object).await.unwrap_err();
        assert!(err.to_string().contains("pin down"), "{err}");

        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_hooks_write_failed_not_sent() {
        let api = app_client::mock::Client::new(); // no hosts
        let (hooks, rec) = recording_hooks();
        let sdk = sdk(&api).await.with_agent_hooks(hooks);

        let err = sdk
            .upload(
                Object::default(),
                Cursor::new(vec![1u8; 1000]),
                UploadOptions::default(),
            )
            .await
            .unwrap_err();
        let ev = rec.failures.try_recv().expect("no failure event");
        assert_eq!(ev.stage, WriteFailureStage::NotSent);
        assert_eq!(ev.stage.to_string(), "not_sent");
        assert_eq!(ev.write_attempts_started, 0);
        assert_eq!(ev.shards_acknowledged, 0);
        assert_eq!(ev.error, err.to_string());
        assert!(rec.uploads.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_hooks_write_failed_sent_not_registered() {
        let api = app_client::mock::Client::new();
        api.add_hosts(hosts(40, false));
        let (hooks, rec) = recording_hooks();
        let sdk = sdk(&api).await.with_agent_hooks(hooks);

        let err = sdk
            .upload(
                Object::default(),
                Cursor::new(vec![1u8; 1000]),
                UploadOptions::default(),
            )
            .await
            .unwrap_err();
        let ev = rec.failures.try_recv().expect("no failure event");
        assert_eq!(ev.stage, WriteFailureStage::SentNotRegistered);
        assert!(ev.write_attempts_started > 0);
        assert_eq!(ev.shards_acknowledged, 0);
        assert_eq!(ev.object_id, None);
        assert_eq!(ev.error, err.to_string());
        assert!(rec.uploads.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_hooks_write_failed_pin() {
        let api = app_client::mock::Client::new();
        api.add_hosts(hosts(40, true));
        let (hooks, rec) = recording_hooks();
        let sdk = sdk(&api).await.with_agent_hooks(hooks);

        let object = sdk
            .upload(
                Object::default(),
                Cursor::new(vec![1u8; 1000]),
                UploadOptions::default(),
            )
            .await
            .unwrap();

        // slabs pinned, object not saved
        api.set_pin_errors(None, Some("object rejected"));
        sdk.pin_object(&object).await.unwrap_err();
        let ev = rec.failures.try_recv().expect("no failure event");
        assert_eq!(ev.stage, WriteFailureStage::PinFailed);
        assert_eq!(ev.slabs_pinned, 1);
        assert_eq!(ev.object_id, Some(object.id()));
        assert!(ev.error.contains("object rejected"), "{}", ev.error);

        // nothing pinned
        api.set_pin_errors(Some("slabs rejected"), None);
        sdk.pin_object(&object).await.unwrap_err();
        let ev = rec.failures.try_recv().expect("no failure event");
        assert_eq!(ev.stage, WriteFailureStage::PinFailed);
        assert_eq!(ev.slabs_pinned, 0);

        // retrying succeeds and reports nothing
        api.set_pin_errors(None, None);
        sdk.pin_object(&object).await.unwrap();
        assert!(rec.failures.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_agent_metadata_seal_round_trip() {
        let api = app_client::mock::Client::new();
        api.add_hosts(hosts(40, true));
        let sdk = sdk(&api).await;

        let mut object = sdk
            .upload(
                Object::default(),
                Cursor::new(vec![1u8; 1000]),
                UploadOptions::default(),
            )
            .await
            .unwrap();
        object
            .set_agent_metadata(&crate::AgentMetadata {
                name: "bot".into(),
                task: "backup".into(),
                ..Default::default()
            })
            .unwrap();
        sdk.pin_object(&object).await.unwrap();

        // the sealed object does not carry the metadata in plaintext
        let sealed = object.seal(sdk.app_key());
        let encoded = serde_json::to_vec(&sealed).unwrap();
        assert!(!encoded.windows(6).any(|w| w == b"backup"));

        let fetched = sdk.object(&object.id()).await.unwrap();
        let meta = fetched
            .agent_metadata()
            .unwrap()
            .expect("no agent metadata");
        assert_eq!(meta.name, "bot");
        assert_eq!(meta.task, "backup");
    }
}
