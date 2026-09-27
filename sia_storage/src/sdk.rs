#[cfg(feature = "fs")]
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use log::debug;
use reqwest::IntoUrl;
use sia_core::types::Hash256;
use thiserror::Error;
use tokio::io::AsyncRead;
use url::Url;

use crate::app_client::{self, SlabPinParams};
use crate::hosts::Hosts;
use crate::rhp4::{Client, HostEndpoint};
use crate::upload::{PackedUpload, upload_object};
use crate::{
    Account, AppKey, BuilderError, Download, DownloadError, DownloadOptions, Host, HostQuery,
    Object, ObjectEvent, ObjectsCursor, PackedUploadOptions, PinnedSlab, SealedObjectError,
    UploadError, UploadOptions,
};

/// Errors that can occur when using the SDK.
#[derive(Error, Debug)]
pub enum Error {
    /// An error from the indexer API.
    #[error("app error: {0}")]
    App(String),

    /// An error during upload.
    #[error("upload error: {0}")]
    Upload(#[from] UploadError),

    /// An error during download.
    #[error("download error: {0}")]
    Download(#[from] DownloadError),

    /// A TLS connection error.
    #[error("TLS error: {0}")]
    Tls(String),

    /// An error opening or sealing an object.
    #[error("sealed object: {0}")]
    SealedObject(#[from] SealedObjectError),
}

/// The main interface with interacting with the Sia storage network. It provides methods for uploading and downloading objects, as well as managing hosts and account information.
#[derive(Clone)]
pub struct Sdk {
    app_key: Arc<AppKey>,
    api_client: app_client::Client,
    hosts: Hosts,
}

impl Sdk {
    /// Fetches the full write-path host directory (paginated `GET /hosts`)
    /// and replaces `hosts`'s standing list with it.
    ///
    /// TESSERA FORK: this used to
    /// also run once at connect time and on a 10-minute background loop
    /// (`Sdk::spawn_refresh_task`, removed). Product law: no standing host
    /// book. It is now called ONLY at the start of a write job
    /// (`Sdk::upload`, `Sdk::upload_packed`) -- never at
    /// `connect`/`unlock`/`list`/`account`, and never for downloads (see
    /// `Sdk::download`, which fetches its own per-object map instead).
    async fn refresh_hosts(
        app_key: &AppKey,
        api_client: &app_client::Client,
        hosts: &Hosts,
    ) -> Result<(), app_client::Error> {
        const PAGE_SIZE: usize = 100;
        let mut all_hosts = Vec::new();
        for i in (0..).step_by(PAGE_SIZE) {
            let page = api_client
                .hosts(
                    &app_key.0,
                    HostQuery {
                        offset: Some(i),
                        limit: Some(PAGE_SIZE as u64),
                        ..Default::default()
                    },
                )
                .await?;
            let done = page.len() < PAGE_SIZE;
            all_hosts.extend(page);
            if done {
                break;
            }
        }

        let good_for_upload: Vec<_> = all_hosts
            .iter()
            .filter(|h| h.good_for_upload)
            .map(|h| HostEndpoint {
                public_key: h.public_key,
                addresses: h.addresses.clone(),
            })
            .collect();

        debug!(
            "Refreshed hosts: total {}, good for upload {}",
            all_hosts.len(),
            good_for_upload.len()
        );
        hosts.update(all_hosts, true);
        let hosts = hosts.clone();
        maybe_spawn!(async move {
            hosts.warm_connections(good_for_upload).await;
        });
        Ok(())
    }

    /// Creates a new SDK instance.
    pub(crate) async fn new(
        api_client: app_client::Client,
        app_key: Arc<AppKey>,
    ) -> Result<Self, BuilderError> {
        Self::with_backends(api_client, Client::new(), app_key).await
    }

    /// Creates a new SDK instance with the provided backends
    ///
    /// TESSERA FORK: does NOT
    /// call `GET /hosts` -- `connected()`/`unlock` must not fetch the write
    /// plate. `hosts` starts empty; `upload`/`upload_packed` populate it at
    /// their own job start.
    pub(crate) async fn with_backends(
        api_client: app_client::Client,
        transport: Client,
        app_key: Arc<AppKey>,
    ) -> Result<Self, BuilderError> {
        let hosts = Hosts::new(transport);
        Ok(Self {
            app_key,
            api_client,
            hosts,
        })
    }

    /// Returns the application key used by the SDK.
    ///
    /// This should be kept secret and secure. Applications
    /// should store it safely.
    pub fn app_key(&self) -> &AppKey {
        &self.app_key
    }

    /// Reads until EOF and uploads all slabs. The data will be erasure coded,
    /// encrypted, and uploaded.
    ///
    /// Pass [Object::default] for new uploads. To resume a previous upload,
    /// pass the object returned from the earlier call. Appending data changes
    /// an object's ID. It must be re-pinned afterward and any references to
    /// the previous ID must be updated.
    ///
    /// # Arguments
    /// * `object` - The object to upload into. Use `Object::default()` for new uploads.
    /// * `r` - The reader to read the data from. It will be read until EOF.
    /// * `options` - The [UploadOptions] to use for the upload.
    ///
    /// # Returns
    /// The object containing the metadata needed to download. The caller must
    /// pin the object to the indexer after uploading.
    pub async fn upload<R: AsyncRead + Unpin + 'static>(
        &self,
        object: Object,
        reader: R,
        options: UploadOptions,
    ) -> Result<Object, UploadError> {
        // TESSERA FORK: write
        // start fetches the plate once, in RAM, for this job only (product
        // law). connect() no longer does this -- upload is now the only
        // place a write job's host list gets populated.
        Self::refresh_hosts(&self.app_key, &self.api_client, &self.hosts).await?;
        upload_object(
            self.hosts.clone(),
            self.app_key.clone(),
            object,
            reader,
            options,
        )
        .await
    }

    /// Uploads the file at `path`. Behaves like [upload](Self::upload)
    /// otherwise.
    ///
    /// # Arguments
    /// * `object` - The object to upload into. Use `Object::default()` for new uploads.
    /// * `path` - The path of the file to upload. It will be read until EOF.
    /// * `options` - The [UploadOptions] to use for the upload.
    ///
    /// # Returns
    /// The object containing the metadata needed to download. The caller must
    /// pin the object to the indexer after uploading.
    #[cfg(feature = "fs")]
    pub async fn upload_path<P: AsRef<Path>>(
        &self,
        object: Object,
        path: P,
        options: UploadOptions,
    ) -> Result<Object, UploadError> {
        let file = tokio::fs::File::open(path).await?;
        self.upload(object, file, options).await
    }

    /// Creates a new packed upload. This allows multiple objects to be packed together
    /// for more efficient uploads. The returned `PackedUpload` can be used to add objects to the upload, and then finalized to get the resulting objects.
    ///
    /// # Arguments
    /// * `options` - The [PackedUploadOptions] to use for the upload.
    ///
    /// # Returns
    /// A [PackedUpload] that can be used to add objects and finalize the upload.
    pub async fn upload_packed(
        &self,
        options: PackedUploadOptions,
    ) -> Result<PackedUpload, UploadError> {
        // TESSERA FORK: same
        // write-start-fetch law as `upload` -- see that method's comment.
        Self::refresh_hosts(&self.app_key, &self.api_client, &self.hosts).await?;
        PackedUpload::new(self.hosts.clone(), self.app_key.clone(), options)
    }

    /// Returns a [Download] handle that streams the object's data. The handle
    /// implements [tokio::io::AsyncRead] — pipe it into any writer with
    /// [tokio::io::copy] or read chunks directly. In-flight chunk recovery is
    /// cancelled when the handle is dropped.
    pub async fn download(
        &self,
        object: &Object,
        options: DownloadOptions,
    ) -> Result<Download, DownloadError> {
        // TESSERA FORK: read
        // start fetches THIS object's map -- GET /objects/:key/hosts --
        // once, into a job-scoped Hosts that shares nothing with the
        // write-path list `upload` populates. It is used for this download
        // only and dropped when the returned `Download` is dropped. Product
        // law: never call GET /hosts to help a download, never glue the
        // plate into the read path.
        let map_hosts = self
            .api_client
            .object_hosts(&self.app_key.0, &object.id())
            .await
            .map_err(|e| DownloadError::Custom(format!("map fetch: {e:?}")))?;
        let scoped_hosts = self.hosts.scoped(map_hosts);
        Download::new(object, scoped_hosts, self.app_key.clone(), options)
    }

    /// Retrieves a list of hosts from the indexer matching the provided query
    /// that can be used for uploading and downloading data.
    ///
    /// # Arguments
    /// * `query` - Filtering criteria to select hosts.
    pub async fn hosts(&self, query: HostQuery) -> Result<Vec<Host>, Error> {
        self.api_client
            .hosts(&self.app_key.0, query)
            .await
            .map_err(|e| Error::App(format!("{e:?}")))
    }

    /// Retrieves account information from the indexer.
    pub async fn account(&self) -> Result<Account, Error> {
        self.api_client
            .account(&self.app_key.0)
            .await
            .map_err(|e| Error::App(format!("{e:?}")))
    }

    /// Retrieves an object from the indexer by its key.
    ///
    /// # Arguments
    /// * `key` - The key of the object to retrieve.
    pub async fn object(&self, key: &Hash256) -> Result<Object, Error> {
        let sealed = self
            .api_client
            .object(&self.app_key.0, key)
            .await
            .map_err(|e| Error::App(format!("{e:?}")))?;

        let obj = sealed.open(self.app_key.as_ref())?;
        Ok(obj)
    }

    /// Retrieves a list of object events from the indexer. This
    /// can be used to synchronize local state with the indexer.
    ///
    /// # Arguments
    /// * `cursor` - An optional cursor to continue from a previous call.
    /// * `limit` - An optional limit on the number of events to retrieve.
    pub async fn object_events(
        &self,
        cursor: Option<ObjectsCursor>,
        limit: Option<usize>,
    ) -> Result<Vec<ObjectEvent>, Error> {
        let events = self
            .api_client
            .objects(&self.app_key.0, cursor, limit)
            .await
            .map_err(|e| Error::App(format!("{e:?}")))?;

        let objs = events
            .into_iter()
            .map(|event| {
                let object = match event.object {
                    Some(sealed) => Some(sealed.open(self.app_key.as_ref())?),
                    None => None,
                };
                Ok(ObjectEvent {
                    id: event.id,
                    deleted: event.deleted,
                    updated_at: event.updated_at,
                    object,
                })
            })
            .collect::<Result<_, Error>>()?;

        Ok(objs)
    }

    /// Prunes unused slabs from the indexer. This helps to free up
    /// storage space by removing slabs that are no longer
    /// referenced by objects.
    pub async fn prune_slabs(&self) -> Result<(), Error> {
        self.api_client
            .prune_slabs(&self.app_key.0)
            .await
            .map_err(|e| Error::App(format!("{e:?}")))?;
        Ok(())
    }

    /// Updates the metadata of an object in the indexer. The object
    /// must already be pinned to the indexer.
    ///
    /// # Arguments
    /// * `object` - The object to update.
    pub async fn update_object_metadata(&self, object: &Object) -> Result<(), Error> {
        let sealed = object.seal(self.app_key.as_ref());
        self.api_client
            .pin_object(&self.app_key.0, &sealed)
            .await
            .map_err(|e| Error::App(format!("{e:?}")))?;
        Ok(())
    }

    /// Deletes the object with the given id.
    ///
    /// # Arguments
    /// * `id` - The id of the object to delete.
    pub async fn delete_object(&self, id: &Hash256) -> Result<(), Error> {
        self.api_client
            .delete_object(&self.app_key.0, id)
            .await
            .map_err(|e| Error::App(format!("{e:?}")))
    }

    /// Generates a shared URL for the given object that is valid until the specified time.
    ///
    /// This object should be considered public even if the URL is kept secret,
    /// as anyone with the URL can access the object until the expiration time.
    ///
    /// # Arguments
    /// * `object` - The object to share.
    /// * `valid_until` - The time until which the shared URL is valid.
    pub fn share_object(&self, object: &Object, valid_until: DateTime<Utc>) -> Result<Url, Error> {
        self.api_client
            .shared_object_url(&self.app_key.0, object, valid_until)
            .map_err(|e| Error::App(format!("{e:?}")))
    }

    /// Retrieves a shared object from the given share URL.
    ///
    /// # Arguments
    /// * `share_url` - The URL of the shared object.
    pub async fn shared_object<U: IntoUrl>(&self, share_url: U) -> Result<Object, Error> {
        let share_url = share_url
            .into_url()
            .map_err(|e| Error::App(format!("{e:?}")))?;
        self.api_client
            .shared_object(share_url)
            .await
            .map_err(|e| Error::App(format!("{e:?}")))
    }

    /// Pins an object to the indexer
    pub async fn pin_object(&self, object: &Object) -> Result<(), Error> {
        let slabs: Vec<SlabPinParams> = object
            .slabs()
            .iter()
            .map(|s| SlabPinParams {
                version: s.version,
                encryption_key: s.encryption_key.clone(),
                min_shards: s.min_shards,
                sectors: s.sectors.clone(),
            })
            .collect();

        const BATCH_SIZE: usize = 50;
        for batch in slabs.chunks(BATCH_SIZE) {
            self.api_client
                .pin_slabs(&self.app_key.0, batch)
                .await
                .map_err(|e| Error::App(format!("{e:?}")))?;
        }

        self.api_client
            .pin_object(&self.app_key.0, &object.seal(self.app_key.as_ref()))
            .await
            .map_err(|e| Error::App(format!("{e:?}")))?;
        Ok(())
    }

    /// Retrieves a pinned slab from the indexer by its id.
    pub async fn slab(&self, id: &Hash256) -> Result<PinnedSlab, Error> {
        self.api_client
            .slab(&self.app_key.0, id)
            .await
            .map_err(|e| Error::App(format!("{e:?}")))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod test {
    use super::*;

    fn random_seed() -> [u8; 32] {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).unwrap();
        seed
    }

    #[tokio::test]
    async fn test_mock_network_object_roundtrip() {
        use std::io::Cursor;
        use tokio::io::AsyncReadExt;

        use crate::mock::MockNetwork;

        let network = MockNetwork::new();
        network.add_hosts(40);

        let sdk = network
            .sdk(AppKey::import(random_seed()))
            .await
            .expect("sdk creation failed");

        let data: Vec<u8> = (0..(1 << 20)).map(|i| i as u8).collect();
        let object = sdk
            .upload(
                Object::new(Some(b"metadata".to_vec())),
                Cursor::new(data.clone()),
                UploadOptions::default(),
            )
            .await
            .expect("upload failed");
        assert!(!object.slabs().is_empty());

        sdk.pin_object(&object).await.expect("pin failed");
        assert_eq!(network.pinned_slabs(), object.slabs().len());

        // the indexer stores the sealed object, so this round-trips through
        // seal/open rather than handing back what was pinned
        let fetched = sdk.object(&object.id()).await.expect("object fetch failed");
        assert_eq!(fetched.slabs(), object.slabs());
        assert_eq!(fetched.metadata, b"metadata".to_vec());

        let mut reader = sdk
            .download(&fetched, DownloadOptions::default())
            .await
            .expect("download failed");
        let mut downloaded = Vec::new();
        reader
            .read_to_end(&mut downloaded)
            .await
            .expect("read failed");
        assert_eq!(downloaded, data);

        sdk.delete_object(&object.id())
            .await
            .expect("delete failed");
        assert!(sdk.object(&object.id()).await.is_err());

        sdk.prune_slabs().await.expect("prune failed");
        assert_eq!(network.pinned_slabs(), 0);
    }

    #[tokio::test]
    async fn test_no_periodic_host_refresh() {
        // TESSERA FORK: replaces
        // the deleted `test_refresh_task_periodic_and_abort`. Product law:
        // no standing host book, no periodic refresh, `connected()` /
        // `with_backends` must not call `GET /hosts` at all. Asserts the
        // negative directly against a counting mock server instead of the
        // old timing-based spawn/drop dance.
        use std::sync::atomic::{AtomicUsize, Ordering};

        use httptest::http::{Response, StatusCode};
        use httptest::matchers::*;
        use httptest::{Expectation, Server};

        use crate::{AppKey, Host};

        let hits = Arc::new(AtomicUsize::new(0));
        let hits_clone = hits.clone();
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/hosts"))
                .times(..)
                .respond_with(move || {
                    hits_clone.fetch_add(1, Ordering::SeqCst);
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(serde_json::to_string(&Vec::<Host>::new()).unwrap())
                        .unwrap()
                }),
        );

        let app_key = Arc::new(AppKey::import(random_seed()));
        let client = crate::app_client::Client::new(server.url("/").to_string()).unwrap();

        let sdk = Sdk::with_backends(client, crate::rhp4::Client::mock(), app_key)
            .await
            .expect("sdk creation failed");

        // `connected()`/`with_backends` must not have touched /hosts.
        assert_eq!(hits.load(Ordering::SeqCst), 0, "connect must not GET /hosts");

        // No background task exists to fire later, either.
        tokio::time::sleep(crate::time::Duration::from_millis(300)).await;
        assert_eq!(hits.load(Ordering::SeqCst), 0, "no periodic refresh must fire");

        // sanity: the Sdk still exists and its (empty) host list reflects
        // that nothing was ever populated outside of an explicit job.
        drop(sdk);
    }
}
