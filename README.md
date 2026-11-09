# Tessera SDK for Rust

Rust SDK for storing and retrieving data through a [Tessera](https://tessera.storage) indexer. Fork of the Sia Foundation's [sia-sdk-rs](https://github.com/SiaFoundation/sia-sdk-rs).

Data is client-side encrypted, erasure-coded, and stored on the Sia network. This SDK does not report download usage itself; billing is metered on the server.

## Crates

| Crate | Description |
|-------|-------------|
| [`tessera_storage`](tessera_storage/) | High-level SDK for uploading and downloading data through an indexer. |
| [`tessera_storage_ffi`](tessera_storage_ffi/) | UniFFI bindings for `tessera_storage`, targeting Swift, Kotlin, and Python. |
| [`tessera_storage_napi`](tessera_storage_napi/) | Node.js bindings, published as `@tessera-storage/sdk`. |
| [`tessera_storage_wasm`](tessera_storage_wasm/) | Browser bindings, published as `@tessera-storage/sdk-wasm`. |

`sia_core`, `sia_core_derive`, and `sia_mux` are unchanged upstream crates consumed from crates.io.

## Status

Workspace version 0.1.0. The API can still change.

## License

MIT. Copyright in the upstream work remains with the Sia Foundation. See [LICENSE](LICENSE).
