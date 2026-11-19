//! WebTransport-based RHP4 client for WASM targets.
//!
//! Provides a [`Client`] that implements [`super::Transport`] using the
//! browser's WebTransport API, mirroring the siamux client on native.
//! Connections are pooled per host — one WebTransport session per host,
//! with multiple bidirectional streams for concurrent RPCs.
//!
//! Writes use direct `JsFuture` calls (`write_all_async`) to avoid tokio
//! poll overhead. Reads use `AsyncRead` — the JS `reader.read()` already
//! returns large chunks which are buffered internally.

use std::cell::RefCell;
use std::collections::HashMap;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use bytes::Bytes;
use js_sys::Uint8Array;
use log::debug;
use sia_core::rhp4::protocol::{RPCReadSector, RPCSettings, RPCWriteSector};
use sia_core::rhp4::{AccountToken, HostPrices};
use sia_core::signing::{PrivateKey, PublicKey};
use sia_core::types::Hash256;
use sia_core::types::v2::Protocol;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{OnceCell, Semaphore};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::time::{Duration, Instant, timeout};

use super::dial_failures::DialFailures;
use super::{Error, HostEndpoint, Transport};

#[wasm_bindgen]
extern "C" {
    type ReadableStreamReadResult;

    #[wasm_bindgen(method, getter, js_name = "done")]
    fn is_done(this: &ReadableStreamReadResult) -> bool;

    #[wasm_bindgen(method, getter)]
    fn value(this: &ReadableStreamReadResult) -> JsValue;
}

/// The WebTransport URL path for the RHP4 protocol.
const RHP4_PATH: &str = "/sia/rhp/v4";

/// Maximum concurrent in-flight dials. Chrome caps pending HTTP/3
/// connections; gating dials here prevents the browser from rejecting or
/// stalling when many hosts are contacted at once.
const MAX_PENDING_CONNS: usize = 64;

/// Timeout for opening a bidirectional stream on an established connection.
/// Independent of the per-RPC timeout so a hung `create_bidirectional_stream`
/// can't consume the caller's full RPC budget.
const OPEN_STREAM_TIMEOUT: Duration = Duration::from_secs(10);

fn js_err_message(e: &JsValue) -> String {
    if let Some(err) = e.dyn_ref::<js_sys::Error>() {
        let message: String = err.message().into();
        if message.is_empty() {
            return "JavaScript error with no message".to_string();
        }
        return message;
    }
    e.as_string().unwrap_or_else(|| format!("{e:?}"))
}

// --- Connection ---

/// A WebTransport connection to a host. Supports opening multiple
/// bidirectional streams for sequential RPCs without reconnecting.
struct Connection {
    transport: web_sys::WebTransport,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.transport.close();
    }
}

impl Connection {
    async fn open_stream(&self) -> Result<Stream, Error> {
        open_bidi_stream(self.transport.create_bidirectional_stream()).await
    }
}

/// Waits for a `createBidirectionalStream` promise and wraps the result.
async fn open_bidi_stream(
    promise: js_sys::Promise<web_sys::WebTransportBidirectionalStream>,
) -> Result<Stream, Error> {
    let mut pending = PendingStream(Some(promise.clone()));
    let bidi: web_sys::WebTransportBidirectionalStream =
        timeout(OPEN_STREAM_TIMEOUT, JsFuture::from(promise))
            .await
            .map_err(|_| Error::Transport("createBidirectionalStream: timeout".into()))?
            .map_err(|e| {
                Error::Transport(format!("createBidirectionalStream: {}", js_err_message(&e)))
            })?;
    let reader = bidi
        .readable()
        .get_reader()
        .unchecked_into::<web_sys::ReadableStreamDefaultReader>();
    let writer = bidi
        .writable()
        .get_writer()
        .map_err(|e| Error::Transport(format!("get_writer: {}", js_err_message(&e))))?;
    pending.0 = None;
    Ok(Stream::new(reader, writer))
}

/// Closes a stream whose open was abandoned. Without this, a stream that
/// resolves after a timeout or a cancelled read stays open on the session.
struct PendingStream(Option<js_sys::Promise<web_sys::WebTransportBidirectionalStream>>);

impl Drop for PendingStream {
    fn drop(&mut self) {
        let Some(promise) = self.0.take() else {
            return;
        };
        wasm_bindgen_futures::spawn_local(async move {
            let Ok(bidi) = JsFuture::from(promise).await else {
                return;
            };
            let write = JsFuture::from(bidi.writable().abort());
            let read = JsFuture::from(bidi.readable().cancel());
            let _ = tokio::join!(write, read);
        });
    }
}

async fn connect(addr: &str) -> Result<Connection, Error> {
    let url = if addr.starts_with("https://") {
        addr.to_string()
    } else if addr.contains('/') {
        format!("https://{addr}")
    } else {
        format!("https://{addr}{RHP4_PATH}")
    };
    debug!("[WT] connecting to {url}");

    let options = web_sys::WebTransportOptions::new();
    let wt = web_sys::WebTransport::new_with_options(&url, &options).map_err(|e| {
        Error::DialRefused(format!("WebTransport constructor: {}", js_err_message(&e)))
    })?;

    let conn = Connection { transport: wt };

    // Attach a handler to `.closed` before awaiting ready — the session can
    // fail during the handshake, and if the future here is dropped (or ready
    // rejects after we've moved on), `Connection::drop` will call `close()`
    // on a transport whose `.closed` Promise is about to reject with a
    // `WebTransportError`. Without a handler attached, that rejection reaches
    // the browser's global unhandled-rejection handler. Transport errors are
    // already reported via individual read/write failures and pool eviction,
    // so this handler just logs and swallows.
    let closed = conn.transport.closed();
    let log_url = url.clone();
    wasm_bindgen_futures::spawn_local(async move {
        if let Err(e) = JsFuture::from(closed).await {
            debug!(
                "[WT] session closed with error: {log_url}: {}",
                js_err_message(&e)
            );
        }
    });

    JsFuture::from(conn.transport.ready())
        .await
        .map_err(|e| Error::DialRefused(format!("WebTransport ready: {}", js_err_message(&e))))?;

    debug!("[WT] connected to {url}");
    Ok(conn)
}

// --- Stream ---

struct Stream {
    reader: web_sys::ReadableStreamDefaultReader,
    pending_read: Option<JsFuture>,
    buf: Vec<u8>,
    writer: web_sys::WritableStreamDefaultWriter,
}

impl Stream {
    fn new(
        reader: web_sys::ReadableStreamDefaultReader,
        writer: web_sys::WritableStreamDefaultWriter,
    ) -> Self {
        Self {
            reader,
            pending_read: None,
            buf: Vec::new(),
            writer,
        }
    }

    /// Write all bytes in one JS call. This bypasses tokio's poll-based
    /// AsyncWrite which would yield to the JS event loop on every poll.
    /// RPC requests are encoded into a Vec<u8> first, then sent here.
    async fn write_all_async(&self, data: &[u8]) -> Result<(), std::io::Error> {
        let array = Uint8Array::new_with_length(data.len() as u32);
        array.copy_from(data);
        JsFuture::from(self.writer.write_with_chunk(&array))
            .await
            .map_err(|e| std::io::Error::other(js_err_message(&e)))?;
        Ok(())
    }

    /// Writes `request`, then starts closing the sending side without
    /// waiting for it. Hosts on coreutils 0.25.1 and later stop reading a
    /// stream when an RPC ends, and if that reaches Firefox while this side
    /// is still open, Firefox errors the readable side and the rest of the
    /// response is lost.
    async fn write_request(&self, request: &[u8]) -> Result<(), std::io::Error> {
        self.write_all_async(request).await?;
        let close = JsFuture::from(self.writer.close());
        wasm_bindgen_futures::spawn_local(async move {
            let _ = close.await;
        });
        Ok(())
    }
}

/// Explicitly close both halves of the bidirectional stream when dropped.
/// Without this, the browser holds the underlying transport resources
/// alive until the JS stream wrappers are garbage-collected
impl Drop for Stream {
    fn drop(&mut self) {
        let reader = self.reader.clone();
        let writer = self.writer.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let _ = JsFuture::from(writer.close()).await;
            let _ = JsFuture::from(reader.cancel()).await;
        });
    }
}

/// AsyncRead for reading RPC responses. The JS `reader.read()` returns
/// large chunks naturally, which are buffered in `self.buf`. Writes bypass
/// poll entirely via write_all_async.
impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        if !this.buf.is_empty() {
            let n = this.buf.len().min(buf.remaining());
            buf.put_slice(&this.buf[..n]);
            this.buf.drain(..n);
            return Poll::Ready(Ok(()));
        }

        if this.pending_read.is_none() {
            this.pending_read = Some(JsFuture::from(this.reader.read()));
        }

        let future = this.pending_read.as_mut().unwrap();
        let result = std::task::ready!(Pin::new(future).poll(cx))
            .map_err(|e| std::io::Error::other(js_err_message(&e)))?;
        this.pending_read = None;

        let chunk: ReadableStreamReadResult = result.unchecked_into();
        if chunk.is_done() {
            return Poll::Ready(Ok(()));
        }

        let data = Uint8Array::new(&chunk.value()).to_vec();
        let n = data.len().min(buf.remaining());
        buf.put_slice(&data[..n]);
        if n < data.len() {
            this.buf = data[n..].to_vec();
        }

        Poll::Ready(Ok(()))
    }
}

// --- Client with connection pooling ---

type ConnCell = Rc<OnceCell<Rc<Connection>>>;

#[derive(Clone)]
pub struct Client {
    pool: Rc<RefCell<HashMap<PublicKey, ConnCell>>>,
    dial_sema: Rc<Semaphore>,
    failures: Rc<RefCell<DialFailures>>,
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
    pub fn new() -> Self {
        Client {
            pool: Rc::new(RefCell::new(HashMap::new())),
            dial_sema: Rc::new(Semaphore::new(MAX_PENDING_CONNS)),
            failures: Rc::new(RefCell::new(DialFailures::default())),
        }
    }

    /// Get a pooled connection or create a new one. If a pooled connection
    /// turns out to be stale, the RPC method will call [`evict`] and the
    /// next call will establish a fresh connection.
    async fn connection(&self, host: &HostEndpoint) -> Result<Rc<Connection>, Error> {
        let cell = if let Some(cell) = self.pool.borrow().get(&host.public_key) {
            cell.clone()
        } else {
            self.pool
                .borrow_mut()
                .entry(host.public_key)
                .or_insert_with(|| Rc::new(OnceCell::new()))
                .clone()
        };
        let conn = cell
            .get_or_try_init(|| async {
                let now = Instant::now();
                let quic_addrs: Vec<&str> = host
                    .addresses
                    .iter()
                    .filter(|addr| addr.protocol == Protocol::QUIC)
                    .map(|addr| addr.address.as_str())
                    .collect();
                if quic_addrs.is_empty() {
                    return Err(Error::Transport(format!(
                        "no QUIC/WebTransport address for host {}",
                        host.public_key
                    )));
                }
                let dialable: Vec<&str> = {
                    let mut failures = self.failures.borrow_mut();
                    quic_addrs
                        .into_iter()
                        .filter(|addr| !failures.in_backoff(addr, now))
                        .collect()
                };
                if dialable.is_empty() {
                    return Err(Error::DialRefused(format!(
                        "every address of host {} refused a handshake in the last five minutes",
                        host.public_key
                    )));
                }

                // Gate concurrent dials to stay under Chrome's pending-connection cap.
                let _permit = self
                    .dial_sema
                    .acquire()
                    .await
                    .map_err(|e| Error::Transport(format!("dial semaphore closed: {e}")))?;

                // Connect to first available QUIC address. A timeout is not
                // recorded: Chrome can hold a healthy host back while it is
                // counting earlier failures.
                let mut last_err = None;
                for addr in dialable {
                    // Checked again after waiting for a dial slot, for an address
                    // listed twice or refused while this dial waited.
                    if self.failures.borrow_mut().in_backoff(addr, Instant::now()) {
                        last_err = Some(Error::DialRefused(format!(
                            "{addr} refused a handshake in the last five minutes"
                        )));
                        continue;
                    }
                    match connect(addr).await {
                        Ok(conn) => return Ok(Rc::new(conn)),
                        Err(e) => {
                            debug!("[WT] connect to {addr} failed: {e}");
                            if matches!(e, Error::DialRefused(_)) {
                                self.failures.borrow_mut().record(addr, Instant::now());
                            }
                            last_err = Some(e);
                        }
                    }
                }

                Err(last_err.unwrap_or_else(|| {
                    Error::Transport(format!(
                        "no QUIC/WebTransport address for host {}",
                        host.public_key
                    ))
                }))
            })
            .await?
            .clone();
        Ok(conn)
    }

    fn evict(&self, host_key: &PublicKey) {
        self.pool.borrow_mut().remove(host_key);
    }

    /// Returns true if the error indicates the connection is broken and
    /// should be evicted from the pool. Transport and I/O errors mean the
    /// session is dead; RPC-level errors (e.g. insufficient funds) are
    /// application errors on an otherwise healthy connection.
    fn should_evict(err: &Error) -> bool {
        matches!(err, Error::Transport(_) | Error::Io(_))
    }
}

// RPC writes: encode request into a Vec<u8> (instant — Vec impls AsyncWrite),
// then send the whole buffer with write_all_async in one JS Promise, and
// close the writer.
//
// RPC reads: use AsyncRead on Stream directly. The JS reader.read() already
// returns large chunks from the network buffer, which Stream stores in
// self.buf and serves to subsequent poll_read calls without further JS calls.
impl Transport for Client {
    async fn host_prices(&self, host: &HostEndpoint) -> Result<(HostPrices, Duration), Error> {
        let conn = self.connection(host).await?;
        let result: Result<(HostPrices, Duration), Error> = async {
            let mut stream = conn.open_stream().await?;
            let mut buf = Vec::new();
            let req = RPCSettings::send_request(&mut buf).await?;
            let start = Instant::now();
            stream.write_request(&buf).await?;
            let resp = req.complete(&mut stream).await?;
            Ok((resp.settings.prices, start.elapsed()))
        }
        .await;
        if let Err(e) = &result
            && Self::should_evict(e)
        {
            self.evict(&host.public_key);
        }
        result
    }

    async fn write_sector(
        &self,
        host: &HostEndpoint,
        prices: HostPrices,
        account_key: &PrivateKey,
        data: Bytes,
    ) -> Result<(Hash256, Duration), Error> {
        let token = AccountToken::new(account_key, host.public_key);
        let conn = self.connection(host).await?;
        let result: Result<(Hash256, Duration), Error> = async {
            let mut stream = conn.open_stream().await?;
            let mut buf = Vec::new();
            let req = RPCWriteSector::send_request(&mut buf, prices, token, data.clone()).await?;
            let start = Instant::now();
            stream.write_request(&buf).await?;
            let resp = req.complete(&mut stream).await?;
            Ok((resp.root, start.elapsed()))
        }
        .await;
        if let Err(e) = &result
            && Self::should_evict(e)
        {
            self.evict(&host.public_key);
        }
        result
    }

    async fn read_sector(
        &self,
        host: &HostEndpoint,
        prices: HostPrices,
        account_key: &PrivateKey,
        root: Hash256,
        offset: usize,
        length: usize,
    ) -> Result<(Bytes, Duration), Error> {
        let token = AccountToken::new(account_key, host.public_key);
        let conn = self.connection(host).await?;
        let result: Result<(Bytes, Duration), Error> = async {
            let mut stream = conn.open_stream().await?;
            let mut buf = Vec::new();
            let req =
                RPCReadSector::send_request(&mut buf, prices, token, root, offset, length).await?;
            let start = Instant::now();
            stream.write_request(&buf).await?;
            let resp = req.complete(&mut stream).await?;
            Ok((resp.data, start.elapsed()))
        }
        .await;
        if let Err(e) = &result
            && Self::should_evict(e)
        {
            self.evict(&host.public_key);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use js_sys::Uint8Array;
    use tokio::io::AsyncReadExt;
    use wasm_bindgen_futures::spawn_local;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    async fn test_refused_address_is_not_dialed_again() {
        // The space makes the URL invalid, so the browser rejects the session at once.
        let address = "not a host:9984";
        let host = HostEndpoint {
            public_key: PrivateKey::from_seed(&[1u8; 32]).public_key(),
            addresses: vec![sia_core::types::v2::NetAddress {
                protocol: Protocol::QUIC,
                address: address.to_string(),
            }],
        };
        let client = Client::new();

        assert!(client.connection(&host).await.is_err());
        assert!(
            client
                .failures
                .borrow_mut()
                .in_backoff(address, crate::time::Instant::now())
        );
        match client.connection(&host).await {
            Err(Error::DialRefused(message)) => {
                assert!(
                    message.contains("in the last five minutes"),
                    "the refused address was dialed again: {message}"
                );
            }
            Err(e) => panic!("expected DialRefused, got {e}"),
            Ok(_) => panic!("the refused address was dialed again"),
        }
    }

    #[wasm_bindgen_test]
    async fn test_address_listed_twice_is_dialed_once() {
        let address = "not a host either:9984";
        let quic = || sia_core::types::v2::NetAddress {
            protocol: Protocol::QUIC,
            address: address.to_string(),
        };
        let host = HostEndpoint {
            public_key: PrivateKey::from_seed(&[2u8; 32]).public_key(),
            addresses: vec![quic(), quic()],
        };
        let client = Client::new();

        // The second copy is skipped once the first is refused, so the error is the skip's.
        let err = client
            .connection(&host)
            .await
            .err()
            .expect("the dial fails");
        assert!(
            err.to_string().contains("in the last five minutes"),
            "the second copy was dialed: {err}"
        );
    }

    /// Creates a Stream backed by separate read/write TransformStreams.
    /// Returns (stream, feeder_for_reads, reader_for_writes).
    fn test_stream() -> (
        Stream,
        web_sys::WritableStreamDefaultWriter,
        web_sys::ReadableStreamDefaultReader,
    ) {
        // Read side: feeder → TransformStream → Stream.reader
        let read_ts = web_sys::TransformStream::new().unwrap();
        let stream_reader = read_ts
            .readable()
            .get_reader()
            .unchecked_into::<web_sys::ReadableStreamDefaultReader>();
        let feeder = read_ts.writable().get_writer().unwrap();

        // Write side: Stream.writer → TransformStream → out_reader
        let write_ts = web_sys::TransformStream::new().unwrap();
        let stream_writer = write_ts.writable().get_writer().unwrap();
        let out_reader = write_ts
            .readable()
            .get_reader()
            .unchecked_into::<web_sys::ReadableStreamDefaultReader>();

        (
            Stream::new(stream_reader, stream_writer),
            feeder,
            out_reader,
        )
    }

    /// Feed data into a WritableStreamDefaultWriter from a spawned microtask.
    /// The write and read proceed as separate tasks on the JS event loop,
    /// avoiding deadlock in single-threaded WASM.
    fn feed_async(feeder: web_sys::WritableStreamDefaultWriter, data: Vec<u8>) {
        spawn_local(async move {
            let array = Uint8Array::new_with_length(data.len() as u32);
            array.copy_from(&data);
            JsFuture::from(feeder.write_with_chunk(&array))
                .await
                .unwrap();
        });
    }

    #[wasm_bindgen_test]
    async fn test_stream_write_basic() {
        let (stream, _, out_reader) = test_stream();

        // Write from a spawned task — even with separate TransformStreams,
        // the write-side transform won't pull unless the readable side is
        // being consumed. Spawning lets the read and write interleave.
        let data = b"hello from rust";
        let data_clone = data.to_vec();
        spawn_local(async move {
            stream.write_all_async(&data_clone).await.unwrap();
        });

        let result = JsFuture::from(out_reader.read()).await.unwrap();
        let chunk: ReadableStreamReadResult = result.unchecked_into();
        assert!(!chunk.is_done());
        let received = Uint8Array::new(&chunk.value()).to_vec();
        assert_eq!(received, data);
    }

    #[wasm_bindgen_test]
    async fn test_stream_write_large() {
        let (stream, _, out_reader) = test_stream();

        let data = vec![0xABu8; 4096];
        let data_clone = data.clone();
        spawn_local(async move {
            stream.write_all_async(&data_clone).await.unwrap();
        });

        let mut received = Vec::new();
        while received.len() < data.len() {
            let result = JsFuture::from(out_reader.read()).await.unwrap();
            let chunk: ReadableStreamReadResult = result.unchecked_into();
            assert!(!chunk.is_done());
            received.extend_from_slice(&Uint8Array::new(&chunk.value()).to_vec());
        }
        assert_eq!(received, data);
    }

    #[wasm_bindgen_test]
    async fn test_stream_read_exact() {
        let (mut stream, feeder, _) = test_stream();
        feed_async(feeder, b"hello, world!".to_vec());

        let mut buf = vec![0u8; 5];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        let mut buf = vec![0u8; 8];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b", world!");
    }

    #[wasm_bindgen_test]
    async fn test_stream_read_buffering() {
        let (mut stream, feeder, _) = test_stream();
        feed_async(feeder, vec![42u8; 1024]);

        let mut total = Vec::new();
        for _ in 0..4 {
            let mut buf = vec![0u8; 256];
            stream.read_exact(&mut buf).await.unwrap();
            total.extend_from_slice(&buf);
        }
        assert_eq!(total, vec![42u8; 1024]);
    }

    #[wasm_bindgen_test]
    async fn test_stream_roundtrip() {
        // Use two separate TransformStreams: one for write, one for read.
        // Write side: our Stream writes → write_ts → out_reader verifies
        // Read side: feeder feeds → read_ts → our Stream reads
        let (mut stream, feeder, out_reader) = test_stream();

        let data = b"roundtrip test data!";

        // 1. Feed data into the read side and read it through our Stream
        feed_async(feeder, data.to_vec());
        let mut buf = vec![0u8; data.len()];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, data);

        // 2. Write data through our Stream and verify it on the write side
        let data_vec = data.to_vec();
        spawn_local(async move {
            stream.write_all_async(&data_vec).await.unwrap();
        });
        let result = JsFuture::from(out_reader.read()).await.unwrap();
        let chunk: ReadableStreamReadResult = result.unchecked_into();
        assert!(!chunk.is_done());
        let received = Uint8Array::new(&chunk.value()).to_vec();
        assert_eq!(received, data);
    }

    #[wasm_bindgen_test]
    async fn test_stream_read_multiple_feeds() {
        let (mut stream, feeder, _) = test_stream();

        spawn_local(async move {
            let array = Uint8Array::new_with_length(5);
            array.copy_from(b"hello");
            JsFuture::from(feeder.write_with_chunk(&array))
                .await
                .unwrap();

            let array = Uint8Array::new_with_length(5);
            array.copy_from(b"world");
            JsFuture::from(feeder.write_with_chunk(&array))
                .await
                .unwrap();
        });

        let mut buf = vec![0u8; 10];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"helloworld");
    }

    /// Returns a stand-in for a `WebTransportBidirectionalStream` with the
    /// feeder for its readable and the reader for its writable.
    fn bidi_fixture() -> (
        JsValue,
        web_sys::WritableStreamDefaultWriter,
        web_sys::ReadableStreamDefaultReader,
    ) {
        let read_ts = web_sys::TransformStream::new().unwrap();
        let write_ts = web_sys::TransformStream::new().unwrap();
        let bidi = js_sys::Object::new();
        js_sys::Reflect::set(&bidi, &"readable".into(), &read_ts.readable()).unwrap();
        js_sys::Reflect::set(&bidi, &"writable".into(), &write_ts.writable()).unwrap();
        let feeder = read_ts.writable().get_writer().unwrap();
        let out_reader = write_ts
            .readable()
            .get_reader()
            .unchecked_into::<web_sys::ReadableStreamDefaultReader>();
        (bidi.into(), feeder, out_reader)
    }

    /// Returns a promise that resolves to `bidi` once the sender fires.
    fn deferred(
        bidi: JsValue,
    ) -> (
        js_sys::Promise<web_sys::WebTransportBidirectionalStream>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let promise = wasm_bindgen_futures::future_to_promise(async move {
            let _ = rx.await;
            Ok(bidi)
        });
        (promise.unchecked_into(), tx)
    }

    #[wasm_bindgen_test]
    async fn test_abandoned_open_closes_late_stream() {
        let (bidi, feeder, out_reader) = bidi_fixture();
        let (promise, resolve) = deferred(bidi);
        assert!(
            timeout(Duration::from_millis(10), open_bidi_stream(promise))
                .await
                .is_err()
        );
        resolve.send(()).unwrap();

        // Cancelling the readable errors its feeder; aborting the writable
        // errors its reader.
        assert!(
            timeout(Duration::from_secs(1), JsFuture::from(feeder.closed()))
                .await
                .unwrap()
                .is_err()
        );
        assert!(
            timeout(Duration::from_secs(1), JsFuture::from(out_reader.closed()))
                .await
                .unwrap()
                .is_err()
        );
    }

    #[wasm_bindgen_test]
    async fn test_successful_open_keeps_stream() {
        let (bidi, feeder, out_reader) = bidi_fixture();
        let (promise, resolve) = deferred(bidi);
        resolve.send(()).unwrap();
        let mut stream = open_bidi_stream(promise).await.unwrap();
        // Give a wrongly armed cleanup time to run.
        crate::time::sleep(Duration::from_millis(50)).await;

        feed_async(feeder, b"ping".to_vec());
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        spawn_local(async move {
            stream.write_all_async(b"pong").await.unwrap();
        });
        let result = JsFuture::from(out_reader.read()).await.unwrap();
        let chunk: ReadableStreamReadResult = result.unchecked_into();
        assert_eq!(Uint8Array::new(&chunk.value()).to_vec(), b"pong");
    }

    #[wasm_bindgen_test]
    async fn test_write_request_closes_the_send_side() {
        let (stream, _, out_reader) = test_stream();
        // A browser TransformStream will not resolve `write()` until the
        // readable side pulls. Awaiting the write first deadlocks the test
        // (Node buffers the chunk and hides it). Read while the write runs.
        spawn_local(async move {
            stream.write_request(b"ping").await.unwrap();
        });

        let result = timeout(Duration::from_secs(1), JsFuture::from(out_reader.read()))
            .await
            .expect("request bytes")
            .unwrap();
        let chunk: ReadableStreamReadResult = result.unchecked_into();
        assert_eq!(Uint8Array::new(&chunk.value()).to_vec(), b"ping");

        // The next read is done because the writer was closed. Leaving the
        // send side open hangs here: the host's STOP_SENDING then hits Firefox
        // while this side is still open and drops the rest of the response.
        let result = timeout(Duration::from_secs(1), JsFuture::from(out_reader.read()))
            .await
            .expect("send side stayed open")
            .unwrap();
        let chunk: ReadableStreamReadResult = result.unchecked_into();
        assert!(chunk.is_done(), "send side stayed open");
    }
}
