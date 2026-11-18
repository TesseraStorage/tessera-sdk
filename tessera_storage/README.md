# tessera_storage

Rust SDK for storing and retrieving data through a [Tessera](https://tessera.storage) indexer. Fork of the Sia Foundation's `sia_storage` ([sia-sdk-rs](https://github.com/SiaFoundation/sia-sdk-rs)).

Data is erasure-coded, encrypted, and distributed across hosts on the Sia network. Download usage is metered on the server; this crate does not report it.

## Usage

### Connecting for the first time

Use `Builder` to start the approval flow with an indexer. The user must approve the connection through the indexer's UI, after which an `AppKey` is derived from their recovery phrase.

```rust
use tessera_storage::{Builder, AppMetadata, app_id};

const APP_META: AppMetadata = AppMetadata {
    id: app_id!("a9f0bda1b97b7d44ae6369ac830851a115311bb59aa2d848beda6ae95d10ad18"),
    name: "My App",
    description: "My App Description",
    service_url: "https://myapp.com",
    logo_url: Some("https://myapp.com/logo.png"),
    callback_url: None,
};

let builder = Builder::new("https://index.tessera.storage", APP_META)?;
let builder = builder.request_connection().await?;

// Display builder.response_url() to the user for approval
let builder = builder.wait_for_approval().await?;
let sdk = builder.register("twelve word recovery phrase goes here ...").await?;

// Save the app key for future connections
let exported = sdk.app_key().export();
```

### Reconnecting with an existing key

```rust
use tessera_storage::{Builder, AppKey};

let app_key = AppKey::import(saved_key);
let builder = Builder::new("https://index.tessera.storage", APP_META)?;
if let Some(sdk) = builder.connected(&app_key).await? {
    // Connected successfully
}
```

### Uploading and downloading

```rust
use tessera_storage::{Object, UploadOptions, DownloadOptions};

// Upload
let object = sdk.upload(Object::default(), reader, UploadOptions::default()).await?;
sdk.pin_object(&object).await?;

// Download
let mut reader = sdk.download(&object, DownloadOptions::default()).await?;
tokio::io::copy(&mut reader, &mut writer).await?;
```

## Agent hooks

Agents and other unattended callers often need to record what they stored and to know when a write did not land. Attach optional callbacks with `Sdk::with_agent_hooks`. An `Sdk` without hooks behaves exactly as before.

```rust
use tessera_storage::{AgentHooks, WriteFailureStage};

let sdk = sdk.with_agent_hooks(
    AgentHooks::new()
        .on_upload_complete(|ev| println!("uploaded {}: {} bytes in {:?}", ev.object_id, ev.size, ev.elapsed))
        .on_download_complete(|ev| println!("downloaded {}: {} bytes", ev.object_id, ev.bytes))
        .on_write_failed(|ev| match ev.stage {
            // no object data left the process; retry when ready
            WriteFailureStage::NotSent => eprintln!("not sent: {}", ev.error),
            // shards may be on hosts, but no object exists; retry with a new Object
            WriteFailureStage::SentNotRegistered => eprintln!("sent, not registered: {}", ev.error),
            // data uploaded but the object was not saved; call pin_object again
            WriteFailureStage::PinFailed => eprintln!("pin failed: {}", ev.error),
            _ => {}
        }),
);
```

- `on_upload_complete` runs after `upload` returns `Ok`. The object is not tracked by the indexer until `pin_object` succeeds.
- `on_download_complete` runs when a `Download` has delivered its last byte. A download dropped early or ending in an error does not call it.
- `on_write_failed` runs when `upload` or `pin_object` returns an error. `stage` tells a write that never left the process (`NotSent`, no shard write started) from one that sent data but was not registered (`SentNotRegistered`), and from a failed pin (`PinFailed`). `write_attempts_started`, `shards_acknowledged` and `slabs_pinned` give the counts behind the stage.

Hooks are fire-and-forget: the SDK ignores their result, and a panic inside a hook is caught and logged without changing the result of the transfer. A hook runs on the task that finished the transfer, so keep it short and send longer work to a channel or a spawned task. A caller that drops an `upload` future before it completes gets no event. `upload_packed` and the FFI, Node.js and browser bindings do not call the hooks yet; packed objects still report pin failures through `pin_object`.

### Agent metadata

`Object::set_agent_metadata` records which agent wrote an object, under the `agent` key of the object's metadata. The metadata is encrypted with the rest of the object metadata when the object is sealed, so the indexer never sees it in plaintext. Other metadata keys are kept unchanged.

```rust
use tessera_storage::AgentMetadata;

let mut object = sdk.upload(Object::default(), reader, UploadOptions::default()).await?;
object.set_agent_metadata(&AgentMetadata {
    name: "nightly-exporter".into(),
    run: run_id.clone(),
    task: "export orders table".into(),
    ..Default::default()
})?;
sdk.pin_object(&object).await?; // saves the metadata

// later
let meta: Option<AgentMetadata> = object.agent_metadata()?;
```

`set_agent_metadata` returns `AgentMetadataError::NotObject` and leaves the metadata unchanged if the existing metadata is not a JSON object.

## Key management

The `AppKey` grants full access to a user's data. After connecting, retrieve it with `Sdk::app_key()`, then persist it using `AppKey::export()` and restore it with `AppKey::import()` so users don't need to re-approve on every launch.

## Building for WebAssembly

For best performance in the browser, enable the `simd128` target feature:

```bash
RUSTFLAGS="-C target-feature=+simd128" \
  cargo build --target wasm32-unknown-unknown --release
```

The same `RUSTFLAGS` applies to `wasm-pack build`. SIMD-enabled WASM is supported by all modern browsers; no special hosting headers are required.

## License

MIT. See the repository [LICENSE](../LICENSE).
