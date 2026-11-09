# Running

From the repo root, run the following:

```sh
cargo build --release --package=tessera_storage_ffi
cargo run --package=tessera_storage_ffi --bin uniffi-bindgen generate --library target/release/libtessera_storage_ffi.dylib --language python --out-dir tessera_storage_ffi/examples/python
mv target/release/libtessera_storage_ffi.dylib tessera_storage_ffi/examples/python
(cd tessera_storage_ffi/examples/python && python3 example.py)
```
