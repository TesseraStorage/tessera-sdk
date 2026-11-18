# Report: Empty-File Packed Sentinel
**Date:** 2026-11-18
**Packet:** `sonnet-packet-empty-file-packed-sentinel-2026-11-18.md` (Director)
**Repo:** `/root/tessera-sdk`, branch `tessera`

## Status: already implemented in a prior local commit, never reported — verified end-to-end this pass, nothing further needed.

## What I found
The fix the packet describes was **already committed locally** before this
packet arrived: commit `1be78098` ("PackedUpload: apply empty-file
sentinel to packed entries", author `tessera-dev`, Tue Nov 17 00:55:42
2026 — one day before this packet's date). It was never pushed (tessera-sdk
push is disabled by standing convention) and apparently never reported, so
the packet's "Residual" description (genuine zero-slab sub-objects, stale
test assertion) describes a state that no longer matches the tree. I did
not take this on faith — I re-verified every claim in the packet against
the actual committed code and a real test run before writing this report.

## Substitution approach (already chosen, confirmed correct)
`PackedUpload::add` (`tessera_storage/src/upload.rs`) wraps the incoming
reader in `EmptyFileSentinelReader` — the exact same adapter
`upload_object()`'s single-object path already uses — before handing it to
the packed slab writer:

```rust
let r = EmptyFileSentinelReader::new(r);
```

This is approach (a) from the packet's "What to do" list: substitute the
one-byte sentinel before the entry is packed, rather than detecting empty
entries and routing them through a separate single-object call. An empty
entry now reads exactly one byte (`0x30`, `'0'`) through the normal packed
path, so `upload.start != upload.end` afterward and the entry gets a real
one-byte slice of a real slab — no special-casing needed anywhere else in
`finalize()`.

**One side effect worth naming**: `finalize()` still has an early-return
guard `if upload.start == upload.end { return Ok(object) /* zero-slab */ }`
for the "nothing was ever added" case. Since `add()` now always advances
`end` by at least one byte for every entry (including empty ones), this
guard is unreachable for any entry that went through `add()` — it's
dead-but-harmless defensive code, same category as the hostfilter dead
branches from the two prior packets in this series, just not something
this packet asked me to clean up. Flagging it, not touching it.

## NAPI and WASM — confirmed inherited, no separate changes
Per the packet's expectation ("no separate changes expected if they call
the same `PackedUpload`"), checked both call sites directly rather than
assuming:
- `tessera_storage_napi/src/lib.rs`'s `add()` calls `pu.add(stream.0)`
  where `pu: tessera_storage::upload::PackedUpload` — the same core type.
- `tessera_storage_wasm/src/packed.rs`'s `add()` calls `pu.add(reader)` —
  same core type.

Both binding crates were rebuilt from scratch this pass (`cargo build` in
each) to confirm they actually compile against the current core, not just
that the source looks right:
- `tessera_storage_napi`: clean build.
- `tessera_storage_wasm --target wasm32-unknown-unknown`: clean build.

No code changes were needed or made in either binding crate.

## Test results
- `cargo test` in `tessera_storage`: **84 passed, 0 failed** (plus 2
  doc-tests passed, 4 ignored — pre-existing, unrelated to this work).
  Confirmed by name, not just the summary count:
  - `test::test_upload_download_packed_empty` — the packet's named test —
    **passed**. Asserts the packed object with an empty entry comes back
    as a 1-byte object whose downloaded content is exactly `[0x30]`, not
    zero slabs.
  - `test::test_upload_download_packed`, `test::test_upload_download_packed_exact`,
    `test::test_upload_download_packed_spanning` — non-empty packed entries
    unchanged, all passed.
  - `upload::tests::test_empty_input_becomes_one_byte_sentinel_object`,
    `upload::tests::test_non_empty_input_is_unchanged`,
    `upload::tests::test_empty_file_sentinel_reader` — the single-object
    sentinel-path tests from the prior packet — still pass, confirming
    this change didn't regress the already-shipped single-object fix.
- `cargo build` in `tessera_storage`, `tessera_storage_napi`, and
  `tessera_storage_wasm` (wasm target): all clean, zero warnings surfaced
  as errors.

## Files touched (in the pre-existing commit, verified not re-touched)
- `tessera_storage/src/upload.rs` — `PackedUpload::add` wraps its reader
  in `EmptyFileSentinelReader`.
- `tessera_storage/src/lib.rs` — `test_upload_download_packed_empty`
  rewritten to expect a 1-byte sentinel object instead of a zero-slab
  object; a few unrelated `Download::new` call sites in the same test
  module fixed to pass the egress `Option` argument they were missing
  (needed for the file to compile at all against the current tree, per
  the commit's own message — confirmed necessary: `cargo build` fails
  without it if attempted on that commit's parent).

## Commit
`1be78098ab18481060cfb48545b14a1f9a662cec` — "PackedUpload: apply
empty-file sentinel to packed entries" — **local only**, not pushed, per
standing convention for this repo (remote is upstream SiaFoundation,
tessera-sdk pushes are disabled).

## Out-of-scope finding (flagged per the packet's explicit instruction, not fixed)
**`tessera-apps`'s web multi-file batch upload path does not inherit this
fix and still actively rejects empty files as an error**, contradicting
the Operator's stated decision that zero-byte files get the sentinel
treatment client-side:

- `packages/shared/src/files.js`, `uploadFilesPacked()` (~line 1886-1919):
  for a **single** file, or on **desktop**, it delegates to `uploadFile`
  (the single-object path, which already has the sentinel via the earlier
  tessera-cli/SDK packet). But for **web, multiple files** (the only case
  that calls `sdk.uploadPacked()` directly), it partitions zero-byte files
  out *before* packing and marks each one `{ ok: false, error: 'Empty
  files cannot be uploaded.' }` — they never reach `PackedUpload.add()` at
  all, sentinel or not.
- This is exactly "an obvious caller that still needs the same treatment"
  the packet asked me to flag, not fix. `tessera-apps` is a separate
  repo, out of this packet's scope, and this packet explicitly says "do
  not expand scope" — so I verified the exact file/line/condition and I'm
  reporting it here rather than touching `tessera-apps`.

## Constraints honored
- Indexd: not touched.
- Sentinel value: unchanged (still the single byte `0x30`, not made
  per-file-unique).
- No documentation changed (the packet's own "Out of scope" list, not
  item 5 of a prior packet — different packet, different scope).
