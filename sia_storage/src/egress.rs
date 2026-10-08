//! Signed egress reporting (P3b).
//!
//! SDKs report served-shard egress as signed JSON batches. The wire format is
//! frozen in `docs/egress-wire-golden.md` and canonicalizes byte-for-byte
//! across Python, Rust and Go:
//!
//! - envelope: a JSON object, UTF-8, compact (`","`/`":"` separators), keys
//!   sorted recursively, no whitespace. Values are lowercase hex strings,
//!   integers and `null`.
//! - signed preimage: `b"tessera-egress-v1:"` followed by the canonical bytes
//!   of the envelope without its `signature` member.
//! - identity and signature encodings: `ed25519:<lowercase hex>`.
//!
//! [`EgressReporter`] is a cheap-to-clone handle that queues [`record`]ed
//! events in memory (bounded at 4096, oldest dropped) and owns exactly one
//! background flusher. The flusher builds and signs an envelope when the
//! queue reaches `egress_batch_max` events, when `egress_interval` elapses,
//! or (in tests) via [`EgressReporter::flush_now`], and POSTs it to the
//! configured egress URL. Batches are capped at 1000 events and 512 MiB of
//! cumulative event bytes, the client obligations under the server's wire
//! limits. Retryable failures (transport errors, 429 and 5xx) resend the
//! *identical* signed envelope with exponential backoff; other 4xx responses
//! drop the batch with a log line; a spool line that cannot be removed after
//! delivery or rejection is retried under the same backoff. Native builds can
//! configure a durable spool file behind the `fs` feature.
//!
//! [`record`]: EgressReporter::record

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use log::{debug, warn};
use serde_json::{Map, Value};
use sia_core::signing::PublicKey;
use sia_core::types::Hash256;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::app_client;
use crate::time::{Duration, Instant, timeout};
use crate::{AppKey, Url};

#[cfg(feature = "fs")]
use std::path::PathBuf;

/// Domain separation prefix for the signed preimage.
pub(crate) const PREIMAGE_PREFIX: &[u8] = b"tessera-egress-v1:";

/// Maximum number of events held in the in-memory queue before the oldest
/// are dropped.
const QUEUE_CAP: usize = 4096;

/// Default `egress_interval`.
const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);

/// Default `egress_batch_max`.
const DEFAULT_BATCH_MAX: usize = 500;

/// Maximum events per reported batch. The server rejects batches above 1000
/// events; the builder clamps `egress_batch_max` to it.
pub(crate) const MAX_BATCH_EVENTS: usize = 1000;

/// Maximum cumulative event bytes per reported batch. The server rejects
/// totals above 1 GiB; the client obligation in the wire contract is 512 MiB.
pub(crate) const MAX_BATCH_BYTES: u64 = 512 << 20;

/// First retry backoff step.
const RETRY_BASE: Duration = Duration::from_secs(1);

/// Maximum retry backoff.
const RETRY_MAX: Duration = Duration::from_secs(60);

/// Maximum spool file size, oldest envelopes are dropped beyond it.
#[cfg(feature = "fs")]
const SPOOL_MAX_BYTES: usize = 8 * 1024 * 1024;

/// One served shard: where it came from, how many bytes were read, and when.
#[derive(Debug, Clone)]
struct Event {
    host_key: PublicKey,
    slab_id: Option<Hash256>,
    shard_index: u64,
    bytes: u64,
    ts: i64,
}

/// A signed batch: the canonical body (without `signature`) and the canonical
/// wire bytes (with `signature`).
#[derive(Debug, Clone)]
struct Envelope {
    /// Canonical body; only read by the golden tests.
    #[allow(dead_code)]
    body: Vec<u8>,
    bytes: Vec<u8>,
}

/// Egress reporting configuration stored on [`crate::Builder`].
#[derive(Clone, Debug)]
pub(crate) struct EgressConfig {
    pub(crate) url: Option<Url>,
    pub(crate) interval: Duration,
    pub(crate) batch_max: usize,
    #[cfg(feature = "fs")]
    pub(crate) spool_path: Option<PathBuf>,
}

impl Default for EgressConfig {
    fn default() -> Self {
        Self {
            url: None,
            interval: DEFAULT_INTERVAL,
            batch_max: DEFAULT_BATCH_MAX,
            #[cfg(feature = "fs")]
            spool_path: None,
        }
    }
}

impl EgressConfig {
    /// Builds the reporter, or returns `None` when no egress URL is set
    /// (reporting disabled).
    pub(crate) fn reporter(
        &self,
        client: app_client::Client,
        app_key: Arc<AppKey>,
    ) -> Option<Arc<EgressReporter>> {
        let url = self.url.clone()?;
        #[cfg(feature = "fs")]
        let spool_path = self.spool_path.clone();
        Some(Arc::new(EgressReporter::new(
            client,
            app_key,
            url,
            self.interval,
            self.batch_max,
            #[cfg(feature = "fs")]
            spool_path,
            #[cfg(feature = "fs")]
            None,
        )))
    }
}

/// The result of one flush attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlushOutcome {
    /// The envelope was delivered (or a failed native build spooled it).
    Sent,
    /// The endpoint rejected the batch; it was dropped.
    Dropped,
    /// A retryable failure occurred or a delivered envelope could not leave
    /// the spool; the envelope stays recorded for a paced retry.
    Retryable,
    /// Nothing was ready to send.
    Idle,
}

/// Where an envelope being sent came from.
#[derive(Debug, Clone, Copy)]
enum Source {
    /// Built from the in-memory queue or held for retry in memory.
    Memory,
    /// Read from the on-disk spool.
    #[cfg(feature = "fs")]
    Spool,
}

/// The injected spool-writer function type.
#[cfg(feature = "fs")]
type SpoolWriteFn = dyn Fn(&std::path::Path, &[u8]) -> std::io::Result<()> + Send + Sync;

/// Overrides the spool writer so tests can inject write failures. Production
/// spools through `tokio::fs::write`; mirrors the Go SDK's `writeSpoolFile`
/// seam.
#[cfg(feature = "fs")]
#[derive(Clone)]
pub(crate) struct SpoolWriter(Arc<SpoolWriteFn>);

#[cfg(feature = "fs")]
impl std::fmt::Debug for SpoolWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SpoolWriter")
    }
}

#[cfg(all(test, feature = "fs"))]
impl SpoolWriter {
    /// Wraps a writer override (test seam).
    pub(crate) fn new(
        write: impl Fn(&std::path::Path, &[u8]) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(write))
    }
}

/// A bounded JSON-lines spool of signed envelopes awaiting delivery.
#[cfg(feature = "fs")]
#[derive(Debug, Clone)]
struct Spool {
    path: PathBuf,
    /// Test-injectable writer override; `None` writes with `tokio::fs`.
    writer: Option<SpoolWriter>,
}

#[cfg(feature = "fs")]
impl Spool {
    fn new(path: PathBuf, writer: Option<SpoolWriter>) -> Self {
        Self { path, writer }
    }

    /// Reads the spool, returning one line per envelope. Empty lines are
    /// ignored and a missing file is an empty spool.
    async fn read_lines(&self) -> std::io::Result<Vec<Vec<u8>>> {
        match tokio::fs::read(&self.path).await {
            Ok(data) => Ok(data
                .split(|byte| *byte == b'\n')
                .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
                .filter(|line| !line.is_empty())
                .map(<[u8]>::to_vec)
                .collect()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    /// The oldest spooled envelope.
    async fn first(&self) -> Option<Vec<u8>> {
        match self.read_lines().await {
            Ok(lines) => lines.into_iter().next(),
            Err(err) => {
                warn!("egress spool read failed ({}): {err}", self.path.display());
                None
            }
        }
    }

    /// Removes the oldest spooled envelope after it was delivered or dropped.
    /// Returns an error when the rewrite fails, leaving the envelope recorded
    /// on disk: the caller must keep it scheduled for a paced retry.
    async fn remove_first(&self) -> std::io::Result<()> {
        let mut lines = match self.read_lines().await {
            Ok(lines) => lines,
            Err(err) => {
                warn!("egress spool read failed ({}): {err}", self.path.display());
                return Err(err);
            }
        };
        if lines.is_empty() {
            return Ok(());
        }
        lines.remove(0);
        if let Err(err) = self.write_lines(&lines).await {
            warn!("egress spool write failed ({}): {err}", self.path.display());
            return Err(err);
        }
        Ok(())
    }

    /// Appends an envelope, dropping the oldest lines beyond the cap.
    async fn append(&self, bytes: &[u8]) -> std::io::Result<()> {
        let mut lines = self.read_lines().await?;
        lines.push(bytes.to_vec());
        let mut total: usize = lines.iter().map(|line| line.len() + 1).sum();
        while total > SPOOL_MAX_BYTES && lines.len() > 1 {
            let dropped = lines.remove(0);
            total -= dropped.len() + 1;
        }
        self.write_lines(&lines).await
    }

    /// Rewrites the spool as JSON lines, through the test seam when set.
    async fn write_lines(&self, lines: &[Vec<u8>]) -> std::io::Result<()> {
        let mut data = Vec::new();
        for line in lines {
            data.extend_from_slice(line);
            data.push(b'\n');
        }
        match &self.writer {
            Some(writer) => writer.0(&self.path, &data),
            None => tokio::fs::write(&self.path, data).await,
        }
    }
}

/// Shared reporter state. See [`EgressReporter`].
struct Inner {
    client: app_client::Client,
    app_key: Arc<AppKey>,
    url: Url,
    interval: Duration,
    batch_max: usize,
    queue: Mutex<VecDeque<Event>>,
    /// A signed envelope awaiting retry, kept in memory when it could not be
    /// spooled (or when no spool is configured).
    pending: Mutex<Option<Vec<u8>>>,
    /// Earliest instant at which the pending envelope/spool may be retried.
    retry_at: Mutex<Option<Instant>>,
    /// Consecutive retryable failures, drives the retry backoff.
    failures: AtomicU32,
    /// Batch sequence, restarts at 1 per process.
    seq: AtomicU64,
    /// Number of events dropped because the queue was full.
    dropped: AtomicUsize,
    /// Whether a flusher task is currently running (at most one).
    running: AtomicBool,
    /// Set by [`EgressReporter::flush_now`] to force an immediate flush.
    flush_requested: AtomicBool,
    /// Serializes flush attempts between the background flusher and
    /// `flush_now`.
    flush_lock: AsyncMutex<()>,
    /// Wakes the flusher when the queue changes or a flush is requested.
    wake: Notify,
    #[cfg(feature = "fs")]
    spool: Option<Spool>,
}

/// A cheap-to-clone handle that reports served-shard egress to the configured
/// endpoint in signed batches.
///
/// Reporting is disabled unless the builder was configured with
/// [`Builder::egress_url`](crate::Builder::egress_url). Cloning a reporter
/// shares its queue and flusher.
#[derive(Clone)]
pub struct EgressReporter {
    inner: Arc<Inner>,
}

impl EgressReporter {
    pub(crate) fn new(
        client: app_client::Client,
        app_key: Arc<AppKey>,
        url: Url,
        interval: Duration,
        batch_max: usize,
        #[cfg(feature = "fs")] spool_path: Option<PathBuf>,
        #[cfg(feature = "fs")] spool_writer: Option<SpoolWriter>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                client,
                app_key,
                url,
                interval,
                batch_max: batch_max.clamp(1, MAX_BATCH_EVENTS),
                queue: Mutex::new(VecDeque::new()),
                pending: Mutex::new(None),
                retry_at: Mutex::new(None),
                failures: AtomicU32::new(0),
                seq: AtomicU64::new(0),
                dropped: AtomicUsize::new(0),
                running: AtomicBool::new(false),
                flush_requested: AtomicBool::new(false),
                flush_lock: AsyncMutex::new(()),
                wake: Notify::new(),
                #[cfg(feature = "fs")]
                spool: spool_path.map(|path| Spool::new(path, spool_writer)),
            }),
        }
    }

    /// Records one successfully served shard: the host that served it, the
    /// slab it belonged to (`None` when the caller cannot name it), the shard
    /// index within the slab, and the number of bytes returned.
    ///
    /// Never blocks: the event is appended to a bounded in-memory queue
    /// (oldest dropped beyond 4096) and the flusher is woken.
    pub fn record(
        &self,
        host_key: PublicKey,
        slab_id: Option<Hash256>,
        shard_index: usize,
        bytes: usize,
    ) {
        let event = Event {
            host_key,
            slab_id,
            shard_index: shard_index as u64,
            bytes: bytes as u64,
            ts: Utc::now().timestamp_millis(),
        };
        {
            let mut queue = self.inner.queue.lock().unwrap();
            if queue.len() >= QUEUE_CAP {
                queue.pop_front();
                self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            }
            queue.push_back(event);
        }
        self.inner.ensure_flusher();
        self.inner.wake.notify_one();
    }

    /// Forces a flush and returns once no immediate work remains (test-only).
    #[cfg(test)]
    pub(crate) async fn flush_now(&self) {
        while let FlushOutcome::Sent | FlushOutcome::Dropped = self.inner.flush_attempt(true).await
        {
        }
    }

    /// Returns the number of events dropped by the bounded queue (test-only).
    #[cfg(test)]
    pub(crate) fn dropped(&self) -> usize {
        self.inner.dropped.load(Ordering::Relaxed)
    }
}

impl Inner {
    /// Spawns the flusher unless one is already running.
    fn ensure_flusher(self: &Arc<Self>) {
        if !self.running.swap(true, Ordering::AcqRel) {
            let inner = Arc::clone(self);
            maybe_spawn!(async move { inner.run().await });
        }
    }

    /// The number of queued events.
    fn queue_len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    /// Removes up to `batch_max` events from the queue, stopping before an
    /// event that would push the cumulative bytes past [`MAX_BATCH_BYTES`],
    /// the client obligation in the wire contract. Always takes at least one
    /// event when the queue is non-empty: the server rejects empty batches.
    fn take_events(&self) -> Vec<Event> {
        let mut queue = self.queue.lock().unwrap();
        let count = queue.len().min(self.batch_max);
        let mut total: u64 = 0;
        let mut take = 0;
        for event in queue.iter().take(count) {
            if take > 0 && total.saturating_add(event.bytes) > MAX_BATCH_BYTES {
                break;
            }
            total = total.saturating_add(event.bytes);
            take += 1;
        }
        queue.drain(..take).collect()
    }

    /// The earliest instant the pending envelope or spool may be retried.
    fn retry_at(&self) -> Option<Instant> {
        *self.retry_at.lock().unwrap()
    }

    /// The exponential backoff for the current failure streak (1s→60s cap).
    fn backoff(&self) -> Duration {
        let failures = self.failures.load(Ordering::Relaxed).min(6);
        RETRY_BASE.saturating_mul(1 << failures).min(RETRY_MAX)
    }

    /// Schedules the next retry of the pending envelope or spool.
    fn schedule_retry(&self) {
        let backoff = self.backoff();
        self.failures.fetch_add(1, Ordering::Relaxed);
        *self.retry_at.lock().unwrap() = Some(Instant::now() + backoff);
    }

    /// Clears the retry backoff after a completed send.
    fn clear_retry(&self) {
        self.failures.store(0, Ordering::Relaxed);
        *self.retry_at.lock().unwrap() = None;
    }

    /// Attempts to deliver at most one envelope. `force` bypasses the retry
    /// backoff and the batching thresholds (used by `flush_now`).
    async fn flush_attempt(&self, force: bool) -> FlushOutcome {
        let _guard = self.flush_lock.lock().await;
        if !force
            && let Some(at) = self.retry_at()
            && at > Instant::now()
        {
            return FlushOutcome::Idle;
        }

        let pending = self.pending.lock().unwrap().take();
        if let Some(bytes) = pending {
            return self.send(bytes, Source::Memory).await;
        }

        #[cfg(feature = "fs")]
        if let Some(spool) = &self.spool
            && let Some(bytes) = spool.first().await
        {
            return self.send(bytes, Source::Spool).await;
        }

        let events = self.take_events();
        if events.is_empty() {
            return FlushOutcome::Idle;
        }
        let batch_id = random_batch_id();
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let envelope = build_envelope(&self.app_key, batch_id, seq, &events);
        self.send(envelope.bytes, Source::Memory).await
    }

    /// Sends one envelope and applies the retry/drop policy. A spooled
    /// envelope must actually leave the spool before the reporter clears its
    /// retry state: when the removal rewrite fails, the envelope stays
    /// recorded and the normal backoff is armed, so the next attempt is paced
    /// instead of re-posting the same line in a hot loop (the resend is
    /// idempotent by `batch_id`).
    async fn send(&self, bytes: Vec<u8>, source: Source) -> FlushOutcome {
        match self.client.report_egress(&self.url, &bytes).await {
            Ok(()) => {
                #[cfg(feature = "fs")]
                if let Source::Spool = source
                    && let Some(spool) = &self.spool
                    && let Err(err) = spool.remove_first().await
                {
                    warn!(
                        "egress spool removal failed ({}): {err}; retrying under backoff",
                        spool.path.display()
                    );
                    self.schedule_retry();
                    return FlushOutcome::Retryable;
                }
                self.clear_retry();
                FlushOutcome::Sent
            }
            Err(app_client::EgressReportError::Rejected(reason)) => {
                warn!("egress reporter: batch rejected, dropping: {reason}");
                #[cfg(feature = "fs")]
                if let Source::Spool = source
                    && let Some(spool) = &self.spool
                    && let Err(err) = spool.remove_first().await
                {
                    warn!(
                        "egress spool removal failed ({}): {err}; retrying under backoff",
                        spool.path.display()
                    );
                    self.schedule_retry();
                    return FlushOutcome::Retryable;
                }
                self.clear_retry();
                FlushOutcome::Dropped
            }
            Err(err) => {
                warn!("egress reporter: report failed: {err}");
                self.schedule_retry();
                #[cfg(feature = "fs")]
                let keep_out_of_memory = match (&self.spool, source) {
                    // Spool write failures fall back to an in-memory retry.
                    (Some(spool), Source::Memory) => spool.append(&bytes).await.is_ok(),
                    // Already spooled: it stays in place for the next attempt.
                    (Some(_), Source::Spool) => true,
                    (None, _) => false,
                };
                #[cfg(not(feature = "fs"))]
                let keep_out_of_memory = {
                    let _ = source;
                    false
                };
                if !keep_out_of_memory {
                    *self.pending.lock().unwrap() = Some(bytes);
                }
                FlushOutcome::Retryable
            }
        }
    }

    /// Whether there is any outstanding work (queued, pending or spooled).
    async fn has_work(&self) -> bool {
        if self.queue_len() > 0 || self.pending.lock().unwrap().is_some() {
            return true;
        }
        self.has_spooled().await
    }

    /// Whether the spool holds at least one envelope.
    async fn has_spooled(&self) -> bool {
        #[cfg(feature = "fs")]
        if let Some(spool) = &self.spool {
            return spool.first().await.is_some();
        }
        false
    }

    /// How long the flusher should wait before the next possible attempt.
    fn next_wait(&self, last_flush: Instant) -> Duration {
        let now = Instant::now();
        if let Some(at) = self.retry_at()
            && at > now
        {
            return at - now;
        }
        if self.queue_len() > 0 {
            let due = last_flush + self.interval;
            return due.saturating_duration_since(now);
        }
        self.interval
    }

    /// The flusher task body. Exactly one flusher runs at a time.
    async fn run(self: Arc<Self>) {
        let mut last_flush = Instant::now();
        let mut idle_since = Instant::now();
        loop {
            let requested = self.flush_requested.swap(false, Ordering::AcqRel);
            let retry_ready = self.retry_at().is_none_or(|at| at <= Instant::now());
            let pending_ready = self.pending.lock().unwrap().is_some();
            let should_attempt = requested
                || (retry_ready && (pending_ready || self.has_spooled().await))
                || self.queue_len() >= self.batch_max
                || (self.queue_len() > 0 && last_flush.elapsed() >= self.interval);

            if should_attempt {
                match self.flush_attempt(requested).await {
                    FlushOutcome::Sent | FlushOutcome::Dropped | FlushOutcome::Retryable => {
                        last_flush = Instant::now();
                        idle_since = last_flush;
                        continue;
                    }
                    FlushOutcome::Idle => {}
                }
            }

            if !self.has_work().await {
                // Exit protocol: release the guard, then re-check for events
                // that raced with the exit; if any arrived, reacquire.
                let elapsed = idle_since.elapsed();
                if elapsed >= self.interval {
                    self.running.store(false, Ordering::Release);
                    if self.has_work().await
                        && self
                            .running
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                    {
                        continue;
                    }
                    debug!("egress reporter: idle, exiting flusher");
                    return;
                }
                let _ = timeout(self.interval.saturating_sub(elapsed), self.wake.notified()).await;
            } else {
                idle_since = Instant::now();
                let wait = self.next_wait(last_flush);
                let _ = timeout(wait, self.wake.notified()).await;
            }
        }
    }
}

/// Builds a JSON object with its keys inserted in sorted order, so the
/// compact serialization is canonical regardless of the map backing.
fn canonical_object(entries: Vec<(&str, Value)>) -> Value {
    let mut entries = entries;
    entries.sort_by_key(|(key, _)| *key);
    let mut map = Map::with_capacity(entries.len());
    for (key, value) in entries {
        map.insert(key.to_string(), value);
    }
    Value::Object(map)
}

/// Serializes one event with sorted keys.
fn event_value(event: &Event) -> Value {
    canonical_object(vec![
        ("bytes", Value::from(event.bytes)),
        ("host_key", Value::String(event.host_key.to_string())),
        ("shard_index", Value::from(event.shard_index)),
        (
            "slab_id",
            event
                .slab_id
                .map_or(Value::Null, |id| Value::String(id.to_string())),
        ),
        ("ts", Value::from(event.ts)),
    ])
}

/// Builds the canonical signed envelope for `events`.
fn build_envelope(app_key: &AppKey, batch_id: [u8; 16], seq: u64, events: &[Event]) -> Envelope {
    let total_bytes: u64 = events.iter().map(|event| event.bytes).sum();
    let mut entries = vec![
        ("batch_id", Value::String(hex::encode(batch_id))),
        (
            "events",
            Value::Array(events.iter().map(event_value).collect()),
        ),
        ("identity", Value::String(app_key.public_key().to_string())),
        ("seq", Value::from(seq)),
        (
            "totals",
            canonical_object(vec![
                ("bytes", Value::from(total_bytes)),
                ("shards", Value::from(events.len() as u64)),
            ]),
        ),
    ];
    let body =
        serde_json::to_vec(&canonical_object(entries.clone())).expect("envelope is serializable");
    let mut preimage = Vec::with_capacity(PREIMAGE_PREFIX.len() + body.len());
    preimage.extend_from_slice(PREIMAGE_PREFIX);
    preimage.extend_from_slice(&body);
    let signature = app_key.sign(&preimage);
    entries.push((
        "signature",
        Value::String(format!("ed25519:{}", hex::encode(signature.as_ref()))),
    ));
    let bytes = serde_json::to_vec(&canonical_object(entries)).expect("envelope is serializable");
    Envelope { body, bytes }
}

/// Generates a random 16-byte batch id.
fn random_batch_id() -> [u8; 16] {
    let mut batch_id = [0u8; 16];
    getrandom::fill(&mut batch_id).expect("failed to generate egress batch id");
    batch_id
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::Value;
    use sha2::{Digest, Sha256};
    use sia_core::signing::Signature;

    // Golden literals from `docs/egress-wire-golden.md` (Task 1). They are
    // the cross-language contract; do not regenerate.
    const GOLDEN_PUBLIC_KEY: &str =
        "d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737";
    const GOLDEN_IDENTITY: &str =
        "ed25519:d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737";
    const GOLDEN_SIGNATURE: &str = "ed25519:55430153c1313134096162667541e929208cde53967913157c3d0ce427811028278ce59ba7296b240b435cfb3b7cee18462e5d2c4a530d43bb77d76e89854c05";
    const GOLDEN_SHA256: &str = "30355f13ac6d508c8afd443e9a91719cddae20e3e18931eb35e80d452d6bc198";
    const GOLDEN_BATCH_ID: &str = "0123456789abcdef0123456789abcdef";
    const GOLDEN_BODY: &str = r#"{"batch_id":"0123456789abcdef0123456789abcdef","events":[{"bytes":4194304,"host_key":"ed25519:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff","shard_index":0,"slab_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","ts":1728345600000},{"bytes":1048576,"host_key":"ed25519:ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100","shard_index":3,"slab_id":null,"ts":1728345600123}],"identity":"ed25519:d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737","seq":1,"totals":{"bytes":5242880,"shards":2}}"#;
    const GOLDEN_CANONICAL: &str = r#"{"batch_id":"0123456789abcdef0123456789abcdef","events":[{"bytes":4194304,"host_key":"ed25519:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff","shard_index":0,"slab_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","ts":1728345600000},{"bytes":1048576,"host_key":"ed25519:ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100","shard_index":3,"slab_id":null,"ts":1728345600123}],"identity":"ed25519:d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737","seq":1,"signature":"ed25519:55430153c1313134096162667541e929208cde53967913157c3d0ce427811028278ce59ba7296b240b435cfb3b7cee18462e5d2c4a530d43bb77d76e89854c05","totals":{"bytes":5242880,"shards":2}}"#;

    fn golden_app_key() -> AppKey {
        AppKey::import([0x11; 32])
    }

    fn hex_32(text: &str) -> [u8; 32] {
        let mut out = [0u8; 32];
        hex::decode_to_slice(text, &mut out).expect("invalid hex");
        out
    }

    fn golden_batch_id() -> [u8; 16] {
        let mut out = [0u8; 16];
        hex::decode_to_slice(GOLDEN_BATCH_ID, &mut out).expect("invalid hex");
        out
    }

    fn golden_events() -> Vec<Event> {
        vec![
            Event {
                host_key: PublicKey::new(hex_32(
                    "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
                )),
                slab_id: Some(Hash256::new(hex_32(
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                ))),
                shard_index: 0,
                bytes: 4194304,
                ts: 1728345600000,
            },
            Event {
                host_key: PublicKey::new(hex_32(
                    "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100",
                )),
                slab_id: None,
                shard_index: 3,
                bytes: 1048576,
                ts: 1728345600123,
            },
        ]
    }

    fn golden_envelope() -> Envelope {
        build_envelope(&golden_app_key(), golden_batch_id(), 1, &golden_events())
    }

    /// The canonical envelope and body must be byte-identical to the frozen
    /// cross-language golden vector.
    #[sia_core_derive::cross_target_test]
    fn test_golden_envelope_matches_contract() {
        let envelope = golden_envelope();

        assert_eq!(
            String::from_utf8(envelope.body.clone()).expect("body is utf-8"),
            GOLDEN_BODY
        );
        assert_eq!(
            String::from_utf8(envelope.bytes.clone()).expect("envelope is utf-8"),
            GOLDEN_CANONICAL
        );

        let value: Value = serde_json::from_slice(&envelope.bytes).expect("envelope is json");
        assert_eq!(value["batch_id"], serde_json::json!(GOLDEN_BATCH_ID));
        assert_eq!(value["identity"], serde_json::json!(GOLDEN_IDENTITY));
        assert_eq!(value["signature"], serde_json::json!(GOLDEN_SIGNATURE));
        assert_eq!(value["seq"], serde_json::json!(1));
        assert_eq!(value["totals"]["bytes"], serde_json::json!(5242880));
        assert_eq!(value["totals"]["shards"], serde_json::json!(2));
        assert_eq!(
            value["events"][0]["slab_id"],
            serde_json::json!("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        );
        assert_eq!(value["events"][1]["slab_id"], Value::Null);

        let digest = Sha256::digest(&envelope.bytes);
        assert_eq!(hex::encode(digest), GOLDEN_SHA256);
    }

    /// The golden signature is a raw Ed25519 signature over
    /// `b"tessera-egress-v1:" + canonical body` and must verify with the raw
    /// public key.
    #[sia_core_derive::cross_target_test]
    fn test_golden_signature_verifies_with_raw_ed25519() {
        let envelope = golden_envelope();

        let mut preimage = PREIMAGE_PREFIX.to_vec();
        preimage.extend_from_slice(&envelope.body);
        assert_eq!(PREIMAGE_PREFIX, b"tessera-egress-v1:");
        assert_eq!(
            String::from_utf8(preimage.clone()).expect("preimage is utf-8"),
            format!("tessera-egress-v1:{GOLDEN_BODY}")
        );

        let mut sig = [0u8; 64];
        hex::decode_to_slice(
            GOLDEN_SIGNATURE
                .strip_prefix("ed25519:")
                .expect("signature prefix"),
            &mut sig,
        )
        .expect("invalid signature hex");
        let signature = Signature::from(sig);
        let public_key = PublicKey::new(hex_32(GOLDEN_PUBLIC_KEY));
        assert!(
            public_key.verify(&preimage, &signature),
            "golden signature must verify with raw Ed25519"
        );
        assert!(
            !public_key.verify(b"not the preimage", &signature),
            "signature must not verify other messages"
        );
    }

    /// Reporting is disabled until an egress URL is configured; the defaults
    /// match the wire contract (30s / 500 events, no spool).
    #[sia_core_derive::cross_target_test]
    fn test_egress_config_defaults_and_disabled_without_url() {
        let config = EgressConfig::default();
        assert!(config.url.is_none());
        assert_eq!(config.interval, Duration::from_secs(30));
        assert_eq!(config.batch_max, 500);

        let app_key = Arc::new(golden_app_key());
        let client = app_client::Client::Mock(app_client::mock::Client::new());
        assert!(
            config.reporter(client.clone(), app_key.clone()).is_none(),
            "reporting must be disabled when no URL is set"
        );

        let config = EgressConfig {
            url: Some("https://egress.test/api/v1/usage/egress".parse().unwrap()),
            ..config
        };
        assert!(
            config.reporter(client, app_key).is_some(),
            "reporting must be enabled once a URL is set"
        );
    }

    /// Wasm-side tests for the reporter's background flusher.
    ///
    /// `ensure_flusher` spawns via `maybe_spawn!` (`tokio::task::spawn_local`
    /// on wasm), which requires an entered `LocalSet`; each test enters a
    /// fresh current-thread runtime and `LocalSet` explicitly. Flushes are
    /// driven by `flush_now()` or the batch threshold only — never a
    /// wall-clock interval — and the "network" is
    /// [`app_client::mock::Client`], the test transport mock that captures
    /// every signed envelope the reporter attempts to POST.
    #[cfg(target_arch = "wasm32")]
    mod wasm {
        use super::*;
        use wasm_bindgen_test::*;

        const TEST_URL: &str = "https://egress.test/api/v1/usage/egress";

        /// Runs `future` on a fresh current-thread runtime with the runtime
        /// and a `LocalSet` entered, then drives the `LocalSet` to completion.
        async fn in_local_set<F: std::future::Future>(future: F) -> F::Output {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("creating a tokio runtime must succeed on wasm");
            let local = tokio::task::LocalSet::new();
            let output = {
                let _runtime_guard = runtime.enter();
                local.run_until(future).await
            };
            drop(local);
            drop(runtime);
            output
        }

        fn host(tag: u8) -> PublicKey {
            PublicKey::new([tag; 32])
        }

        /// Builds a reporter whose transport is the mocked indexer client and
        /// returns both; the mock captures every envelope the reporter sends.
        fn mock_reporter(
            interval: Duration,
            batch_max: usize,
        ) -> (EgressReporter, app_client::mock::Client) {
            let mock = app_client::mock::Client::new();
            let reporter = EgressReporter::new(
                app_client::Client::Mock(mock.clone()),
                Arc::new(golden_app_key()),
                TEST_URL.parse().expect("test url"),
                interval,
                batch_max,
                #[cfg(feature = "fs")]
                None,
                #[cfg(feature = "fs")]
                None,
            );
            (reporter, mock)
        }

        /// Drives the `LocalSet` until at least `count` envelopes were
        /// attempted. Only explicit scheduler turns: no wall-clock waits.
        async fn wait_for_attempts(mock: &app_client::mock::Client, count: usize) {
            for _ in 0..1000 {
                if mock.egress_attempts().len() >= count {
                    return;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                mock.egress_attempts().len() >= count,
                "the background flusher did not send within 1000 scheduler turns"
            );
        }

        /// Asserts the frozen wire contract on a captured signed envelope and
        /// returns its parsed value: canonical compact JSON, the golden
        /// `ed25519:<hex>` identity encodings, recomputed totals and a raw
        /// Ed25519 signature over `b"tessera-egress-v1:" + body` (the
        /// envelope without `signature`).
        fn assert_envelope_contract(bytes: &[u8], app_key: &AppKey) -> Value {
            let text = std::str::from_utf8(bytes).expect("envelope is utf-8");
            assert!(
                !text.contains(": ") && !text.contains(", "),
                "the canonical envelope has no whitespace separators"
            );
            let value: Value = serde_json::from_slice(bytes).expect("envelope is json");
            assert_eq!(
                value["identity"],
                Value::String(app_key.public_key().to_string()),
                "identity must be the signing app key"
            );

            let batch_id = value["batch_id"].as_str().expect("batch_id is a string");
            assert_eq!(batch_id.len(), 32, "batch_id is 16 random bytes hex");
            assert!(
                batch_id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "batch_id must be lowercase hex"
            );

            let events = value["events"].as_array().expect("events is an array");
            for event in events {
                assert!(
                    event["host_key"]
                        .as_str()
                        .is_some_and(|key| key.starts_with("ed25519:")),
                    "host_key is an ed25519:<hex> string"
                );
                assert!(
                    event["slab_id"].is_null() || event["slab_id"].is_string(),
                    "slab_id is a hex string or null"
                );
                assert!(
                    event["shard_index"].is_u64() && event["bytes"].is_u64(),
                    "shard_index and bytes are integers"
                );
                assert!(event["ts"].is_u64(), "ts is an integer");
            }
            let total_bytes: u64 = events
                .iter()
                .map(|event| event["bytes"].as_u64().expect("bytes is an integer"))
                .sum();
            assert_eq!(value["totals"]["bytes"], Value::from(total_bytes));
            assert_eq!(value["totals"]["shards"], Value::from(events.len() as u64));

            let mut body = value.clone();
            body.as_object_mut()
                .expect("envelope is an object")
                .remove("signature");
            let body_bytes = serde_json::to_vec(&body).expect("body serializes");
            let mut preimage = PREIMAGE_PREFIX.to_vec();
            preimage.extend_from_slice(&body_bytes);
            assert_eq!(PREIMAGE_PREFIX, b"tessera-egress-v1:");

            let signature = value["signature"].as_str().expect("signature is a string");
            assert_eq!(
                signature,
                signature.to_lowercase(),
                "signature must be lowercase hex"
            );
            let mut sig = [0u8; 64];
            hex::decode_to_slice(
                signature
                    .strip_prefix("ed25519:")
                    .expect("signature prefix"),
                &mut sig,
            )
            .expect("signature is 64 bytes of hex");
            assert!(
                app_key
                    .public_key()
                    .verify(&preimage, &Signature::from(sig)),
                "the envelope must carry a valid raw Ed25519 signature"
            );
            value
        }

        /// The batch threshold spawns the flusher on the `LocalSet` and
        /// produces exactly one signed envelope from the recorded events; the
        /// envelope names the golden identity and satisfies the contract.
        #[wasm_bindgen_test]
        async fn test_threshold_flush_produces_signed_envelope() {
            in_local_set(async {
                let (reporter, mock) = mock_reporter(Duration::from_secs(3600), 2);
                let slab_id = Hash256::new([0xAB; 32]);
                reporter.record(host(1), Some(slab_id), 0, 4096);
                reporter.record(host(2), None, 3, 1024);

                wait_for_attempts(&mock, 1).await;
                let attempts = mock.egress_attempts();
                assert_eq!(
                    attempts.len(),
                    1,
                    "the threshold must emit exactly one envelope"
                );

                let value = assert_envelope_contract(&attempts[0], &golden_app_key());
                assert_eq!(
                    value["identity"],
                    Value::String(GOLDEN_IDENTITY.to_string()),
                    "the golden app key must produce the golden identity"
                );
                assert_eq!(value["seq"], Value::from(1_u64));

                let events = value["events"].as_array().expect("events");
                assert_eq!(events.len(), 2);
                assert_eq!(events[0]["host_key"], Value::String(host(1).to_string()));
                assert_eq!(events[0]["slab_id"], Value::String(slab_id.to_string()));
                assert_eq!(events[0]["shard_index"], Value::from(0_u64));
                assert_eq!(events[0]["bytes"], Value::from(4096_u64));
                assert_eq!(events[1]["host_key"], Value::String(host(2).to_string()));
                assert_eq!(events[1]["slab_id"], Value::Null);
                assert_eq!(events[1]["shard_index"], Value::from(3_u64));
                assert_eq!(events[1]["bytes"], Value::from(1024_u64));
                assert_eq!(value["totals"]["bytes"], Value::from(5120_u64));

                // The threshold flush drained the queue: a forced flush has
                // nothing left to send.
                reporter.flush_now().await;
                assert_eq!(
                    mock.egress_attempts().len(),
                    1,
                    "a forced flush after the threshold must not resend"
                );
            })
            .await;
        }

        /// `flush_now` does not depend on the interval: with a one-hour
        /// interval a single queued event is delivered only when forced.
        #[wasm_bindgen_test]
        async fn test_flush_now_is_deterministic() {
            in_local_set(async {
                let (reporter, mock) = mock_reporter(Duration::from_secs(3600), 500);
                reporter.record(host(7), None, 5, 8192);

                // Give the flusher plenty of scheduler turns: below the
                // threshold, with the interval an hour away, it must not send.
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                }
                assert!(
                    mock.egress_attempts().is_empty(),
                    "the wall-clock interval must not fire"
                );

                reporter.flush_now().await;
                let attempts = mock.egress_attempts();
                assert_eq!(attempts.len(), 1, "flush_now sends the queued event");
                let value = assert_envelope_contract(&attempts[0], &golden_app_key());
                let events = value["events"].as_array().expect("events");
                assert_eq!(events.len(), 1);
                assert_eq!(events[0]["bytes"], Value::from(8192_u64));
                assert_eq!(value["totals"]["bytes"], Value::from(8192_u64));
            })
            .await;
        }

        /// The bounded queue drops the oldest event beyond 4096 without
        /// blocking the recorder.
        #[wasm_bindgen_test]
        async fn test_queue_overflow_drops_oldest() {
            in_local_set(async {
                let (reporter, _mock) = mock_reporter(Duration::from_secs(3600), 500);
                for i in 0..QUEUE_CAP + 1 {
                    reporter.record(host(0), None, i, 1);
                }

                let queue = reporter.inner.queue.lock().unwrap();
                assert_eq!(queue.len(), QUEUE_CAP);
                assert_eq!(reporter.dropped(), 1);
                assert_eq!(queue.front().unwrap().shard_index, 1);
                assert_eq!(queue.back().unwrap().shard_index, QUEUE_CAP as u64);
            })
            .await;
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    mod native {
        use super::*;
        use crate::app_client::{EgressReportError, mock};
        use httptest::http::{Response, StatusCode};
        use httptest::{Expectation, Server, matchers::request};

        const TEST_URL: &str = "https://egress.test/api/v1/usage/egress";

        fn test_reporter(
            client: app_client::Client,
            interval: Duration,
            batch_max: usize,
        ) -> EgressReporter {
            let app_key = Arc::new(AppKey::import([0x42; 32]));
            let url: Url = TEST_URL.parse().expect("test url");
            #[cfg(feature = "fs")]
            {
                EgressReporter::new(client, app_key, url, interval, batch_max, None, None)
            }
            #[cfg(not(feature = "fs"))]
            {
                EgressReporter::new(client, app_key, url, interval, batch_max)
            }
        }

        fn mock_reporter(interval: Duration, batch_max: usize) -> (EgressReporter, mock::Client) {
            let mock = mock::Client::new();
            let reporter =
                test_reporter(app_client::Client::Mock(mock.clone()), interval, batch_max);
            (reporter, mock)
        }

        fn host(tag: u8) -> PublicKey {
            PublicKey::new([tag; 32])
        }

        fn value_of(body: &[u8]) -> Value {
            serde_json::from_slice(body).expect("valid envelope json")
        }

        async fn wait_until(mut done: impl FnMut() -> bool) {
            for _ in 0..10_000 {
                if done() {
                    return;
                }
                tokio::task::yield_now().await;
            }
            assert!(done(), "condition was not reached");
        }

        /// Waits up to five wall-clock seconds for a condition (used when the
        /// condition depends on the interval timer).
        async fn wait_until_timeout(mut done: impl FnMut() -> bool) {
            for _ in 0..500 {
                if done() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(done(), "condition was not reached");
        }

        /// Threshold batching: exactly `batch_max` events produce exactly one
        /// envelope; below the threshold nothing is sent before the interval.
        #[tokio::test]
        async fn test_threshold_flush_emits_one_envelope() {
            let (reporter, mock) = mock_reporter(Duration::from_secs(3600), 3);
            for i in 0..2 {
                reporter.record(host(i as u8), None, i, 100);
            }
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            assert!(
                mock.egress_attempts().is_empty(),
                "below the threshold and before the interval nothing may be sent"
            );

            reporter.record(host(9), None, 2, 100);
            wait_until(|| !mock.egress_attempts().is_empty()).await;

            let attempts = mock.egress_attempts();
            assert_eq!(
                attempts.len(),
                1,
                "threshold flush emits exactly one envelope"
            );
            let value = value_of(&attempts[0]);
            assert_eq!(value["events"].as_array().unwrap().len(), 3);
            assert_eq!(value["totals"]["shards"], serde_json::json!(3));

            // The queue was drained: another flush has nothing to send.
            reporter.flush_now().await;
            assert_eq!(mock.egress_attempts().len(), 1);
        }

        /// `flush_now` does not depend on the interval timer or any wall-clock
        /// delay: one recorded event is sent immediately.
        #[tokio::test]
        async fn test_flush_now_is_deterministic() {
            let (reporter, mock) = mock_reporter(Duration::from_secs(3600), 500);
            reporter.record(host(1), None, 0, 4096);
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            assert!(
                mock.egress_attempts().is_empty(),
                "the interval must not have fired"
            );

            reporter.flush_now().await;

            let attempts = mock.egress_attempts();
            assert_eq!(attempts.len(), 1, "flush_now must send the queued event");
            let value = value_of(&attempts[0]);
            assert_eq!(value["events"].as_array().unwrap().len(), 1);
            assert_eq!(value["totals"]["bytes"], serde_json::json!(4096));
        }

        /// A retryable failure re-queues the identical signed envelope
        /// (same `batch_id`, `seq` and signature) and does not propagate.
        #[tokio::test]
        async fn test_failed_send_requeues_identical_envelope() {
            let (reporter, mock) = mock_reporter(Duration::from_secs(3600), 500);
            mock.fail_egress_reports(1);

            reporter.record(host(1), None, 0, 512);
            reporter.flush_now().await;
            assert_eq!(mock.egress_attempts().len(), 1, "first attempt fails");
            assert!(
                reporter.inner.pending.lock().unwrap().is_some(),
                "the failed envelope must be re-queued in memory"
            );

            // A forced flush bypasses the backoff and retries the same bytes.
            reporter.flush_now().await;
            let attempts = mock.egress_attempts();
            assert_eq!(attempts.len(), 2, "the failure must not propagate");
            assert_eq!(
                attempts[0], attempts[1],
                "retry sends the identical envelope"
            );

            let first = value_of(&attempts[0]);
            let second = value_of(&attempts[1]);
            assert_eq!(first["batch_id"], second["batch_id"]);
            assert_eq!(first["seq"], second["seq"]);
            assert_eq!(first["signature"], second["signature"]);
            assert!(reporter.inner.pending.lock().unwrap().is_none());

            let preimage = {
                let mut body = first.clone();
                body.as_object_mut().unwrap().remove("signature");
                let bytes = serde_json::to_vec(&body).unwrap();
                let mut preimage = PREIMAGE_PREFIX.to_vec();
                preimage.extend_from_slice(&bytes);
                preimage
            };
            let mut sig = [0u8; 64];
            hex::decode_to_slice(
                first["signature"]
                    .as_str()
                    .unwrap()
                    .strip_prefix("ed25519:")
                    .unwrap(),
                &mut sig,
            )
            .unwrap();
            assert!(
                reporter
                    .inner
                    .app_key
                    .public_key()
                    .verify(&preimage, &Signature::from(sig)),
                "the wire signature must verify"
            );
        }

        /// A non-retryable 4xx drops the batch and does not poison the queue.
        #[tokio::test]
        async fn test_rejected_envelope_is_dropped() {
            let (reporter, mock) = mock_reporter(Duration::from_secs(3600), 500);
            mock.reject_egress_reports(true);

            reporter.record(host(1), None, 0, 1);
            reporter.flush_now().await;
            assert_eq!(mock.egress_attempts().len(), 1);
            assert!(
                reporter.inner.pending.lock().unwrap().is_none(),
                "a rejected envelope must not be retried"
            );
            assert!(reporter.inner.queue.lock().unwrap().is_empty());

            mock.reject_egress_reports(false);
            reporter.record(host(2), None, 0, 2);
            reporter.flush_now().await;
            let attempts = mock.egress_attempts();
            assert_eq!(attempts.len(), 2, "the next batch must not be blocked");
            assert_ne!(
                value_of(&attempts[0])["batch_id"],
                value_of(&attempts[1])["batch_id"]
            );
        }

        /// The queue is bounded at 4096 events and drops the oldest; `record`
        /// never blocks (4097 synchronous records complete with no awaits).
        #[tokio::test]
        async fn test_overflow_drops_oldest() {
            let (reporter, _mock) = mock_reporter(Duration::from_secs(3600), 500);
            for i in 0..QUEUE_CAP + 1 {
                reporter.record(host(0), None, i, 1);
            }

            let queue = reporter.inner.queue.lock().unwrap();
            assert_eq!(queue.len(), QUEUE_CAP);
            assert_eq!(reporter.dropped(), 1);
            assert_eq!(queue.front().unwrap().shard_index, 1);
            assert_eq!(queue.back().unwrap().shard_index, QUEUE_CAP as u64);
        }

        /// The flusher exits after a quiet interval with no work and the next
        /// `record` respawns it.
        #[tokio::test]
        async fn test_flusher_exits_when_idle_and_respawns() {
            let (reporter, mock) = mock_reporter(Duration::from_millis(20), 500);
            reporter.record(host(1), None, 0, 1);
            reporter.flush_now().await;
            assert_eq!(mock.egress_attempts().len(), 1);
            assert!(reporter.inner.running.load(Ordering::SeqCst));

            // With an empty queue the flusher releases its guard after one
            // quiet interval.
            wait_until_timeout(|| !reporter.inner.running.load(Ordering::SeqCst)).await;

            // The next record respawns it and the new event is delivered.
            reporter.record(host(2), None, 0, 2);
            assert!(reporter.inner.running.load(Ordering::SeqCst));
            reporter.flush_now().await;
            assert_eq!(mock.egress_attempts().len(), 2);
        }

        /// End to end over a real local HTTP server: `flush_now` POSTs the
        /// signed envelope to the configured URL.
        #[tokio::test]
        async fn test_flush_now_posts_signed_envelope_over_http() {
            use std::sync::Mutex as StdMutex;

            let captured: Arc<StdMutex<Vec<Vec<u8>>>> = Arc::new(StdMutex::new(Vec::new()));
            let sink = captured.clone();
            let server = Server::run();
            server.expect(
                Expectation::matching(
                    move |req: &httptest::http::Request<httptest::bytes::Bytes>| {
                        sink.lock().unwrap().push(req.body().to_vec());
                        req.method() == httptest::http::Method::POST
                            && req.uri().path() == "/api/v1/usage/egress"
                    },
                )
                .respond_with(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body("{}")
                        .unwrap(),
                ),
            );

            let app_key = Arc::new(AppKey::import([0x11; 32]));
            let url: Url = server
                .url("/api/v1/usage/egress")
                .to_string()
                .parse()
                .expect("server url");
            let client = app_client::Client::new(server.url("/").to_string()).unwrap();
            let reporter = EgressReporter::new(
                client,
                app_key.clone(),
                url,
                Duration::from_secs(3600),
                500,
                #[cfg(feature = "fs")]
                None,
                #[cfg(feature = "fs")]
                None,
            );
            reporter.record(host(1), None, 0, 4096);
            reporter.flush_now().await;

            let captured = captured.lock().unwrap().clone();
            assert_eq!(captured.len(), 1, "exactly one POST");
            let value = value_of(&captured[0]);
            assert_eq!(
                value["identity"],
                serde_json::json!(app_key.public_key().to_string())
            );

            let mut body = value.clone();
            body.as_object_mut().unwrap().remove("signature");
            let body_bytes = serde_json::to_vec(&body).unwrap();
            let mut preimage = PREIMAGE_PREFIX.to_vec();
            preimage.extend_from_slice(&body_bytes);
            let mut sig = [0u8; 64];
            hex::decode_to_slice(
                value["signature"]
                    .as_str()
                    .unwrap()
                    .strip_prefix("ed25519:")
                    .unwrap(),
                &mut sig,
            )
            .unwrap();
            assert!(
                app_key
                    .public_key()
                    .verify(&preimage, &Signature::from(sig)),
                "the posted envelope must carry a valid raw Ed25519 signature"
            );
        }

        /// The HTTP client classifies transport errors, 429/5xx as retryable
        /// and other 4xx as rejected.
        #[tokio::test]
        async fn test_report_egress_status_classification() {
            let statuses = [
                (
                    "/retry/too_many",
                    StatusCode::TOO_MANY_REQUESTS,
                    "retryable",
                ),
                (
                    "/retry/server_error",
                    StatusCode::SERVICE_UNAVAILABLE,
                    "retryable",
                ),
                ("/rejected/bad_request", StatusCode::BAD_REQUEST, "rejected"),
                ("/ok", StatusCode::OK, "ok"),
            ];
            let server = Server::run();
            for (path, status, _) in statuses {
                server.expect(
                    Expectation::matching(request::method_path("POST", path)).respond_with(
                        Response::builder()
                            .status(status)
                            .body("something")
                            .unwrap(),
                    ),
                );
            }
            let client = app_client::Client::new(server.url("/").to_string()).unwrap();
            for (path, _, expected) in statuses {
                let url: Url = server.url(path).to_string().parse().unwrap();
                let result = client.report_egress(&url, b"{}").await;
                match expected {
                    "retryable" => assert!(
                        matches!(result, Err(EgressReportError::Retryable(_))),
                        "{path} must be retryable, got {result:?}"
                    ),
                    "rejected" => assert!(
                        matches!(result, Err(EgressReportError::Rejected(_))),
                        "{path} must be rejected, got {result:?}"
                    ),
                    _ => assert!(result.is_ok(), "{path} must succeed, got {result:?}"),
                }
            }
        }

        /// A transport failure spools the envelope on disk; once the transport
        /// recovers the spool drains (before new events) and the file empties.
        #[cfg(feature = "fs")]
        #[tokio::test]
        async fn test_spool_round_trip() {
            let dir = tempfile::tempdir().expect("tempdir");
            let spool_path = dir.path().join("egress.spool");
            let mock = mock::Client::new();
            let reporter = EgressReporter::new(
                app_client::Client::Mock(mock.clone()),
                Arc::new(AppKey::import([0x42; 32])),
                TEST_URL.parse().unwrap(),
                Duration::from_secs(3600),
                500,
                Some(spool_path.clone()),
                None,
            );
            mock.fail_egress_reports(1);

            reporter.record(host(1), None, 0, 4096);
            reporter.flush_now().await;
            let attempts = mock.egress_attempts();
            assert_eq!(attempts.len(), 1, "the first send fails");
            assert!(
                reporter.inner.pending.lock().unwrap().is_none(),
                "a spooled envelope is not kept in memory"
            );

            let spooled = tokio::fs::read(&spool_path).await.expect("spool file");
            assert_eq!(spooled.last(), Some(&b'\n'), "spool is JSON lines");
            assert_eq!(
                spooled.split(|b| *b == b'\n').next().unwrap(),
                attempts[0].as_slice(),
                "the spooled line is the canonical envelope"
            );

            // A new event arrives while the old batch is spooled; the spool
            // must drain first.
            reporter.record(host(2), None, 0, 1);
            reporter.flush_now().await;
            let attempts = mock.egress_attempts();
            assert_eq!(attempts.len(), 3, "old batch then new batch");
            assert_eq!(
                attempts[0], attempts[1],
                "the spooled envelope is retried as-is"
            );
            assert_ne!(
                attempts[1], attempts[2],
                "the new event forms a new envelope"
            );
            assert!(
                tokio::fs::read(&spool_path).await.unwrap().is_empty(),
                "a drained spool must be empty"
            );
        }

        /// A byte-heavy queue yields an envelope with at least one event and
        /// at most [`MAX_BATCH_BYTES`] cumulative event bytes: the server's
        /// `validate_batch` accepts it. The batch size is also clamped to the
        /// 1000-event server limit.
        #[tokio::test]
        async fn test_take_events_stops_at_client_byte_cap() {
            let (reporter, mock) = mock_reporter(Duration::from_secs(3600), 5000);
            assert_eq!(reporter.inner.batch_max, MAX_BATCH_EVENTS);

            for i in 0..6 {
                reporter.record(host(i as u8), None, i, (MAX_BATCH_BYTES / 4) as usize);
            }
            reporter.flush_now().await;

            let attempts = mock.egress_attempts();
            assert_eq!(
                attempts.len(),
                2,
                "six 128 MiB events form a capped batch and a remainder"
            );
            let value = value_of(&attempts[0]);
            let events = value["events"].as_array().expect("events is an array");
            assert_eq!(events.len(), 4, "512 MiB at 128 MiB per event");
            assert!(
                !events.is_empty() && events.len() <= MAX_BATCH_EVENTS,
                "events count must satisfy the server's shape: at least 1, at most {MAX_BATCH_EVENTS}"
            );
            let total: u64 = events
                .iter()
                .map(|event| event["bytes"].as_u64().expect("bytes is an integer"))
                .sum();
            assert_eq!(total, MAX_BATCH_BYTES);
            assert_eq!(value["totals"]["bytes"], Value::from(MAX_BATCH_BYTES));
        }

        /// A delivered or 4xx-rejected spooled envelope whose removal rewrite
        /// fails stays recorded and arms the normal backoff: the flusher does
        /// not re-post it in a hot loop. Once the disk recovers and the
        /// deadline elapses, the envelope drains normally.
        #[cfg(feature = "fs")]
        #[tokio::test]
        async fn test_spool_removal_failure_is_paced() {
            for (case, reject) in [("delivered", false), ("rejected-4xx", true)] {
                let dir = tempfile::tempdir().expect("tempdir");
                let spool_path = dir.path().join("egress.spool");

                let mock = mock::Client::new();
                if reject {
                    mock.reject_egress_reports(true);
                }

                let fail_writes = Arc::new(AtomicBool::new(false));
                let writes = Arc::new(AtomicUsize::new(0));
                let writer = {
                    let fail_writes = fail_writes.clone();
                    let writes = writes.clone();
                    SpoolWriter::new(move |path, data| {
                        writes.fetch_add(1, Ordering::SeqCst);
                        if fail_writes.load(Ordering::SeqCst) {
                            return Err(std::io::Error::other("injected spool write failure"));
                        }
                        std::fs::write(path, data)
                    })
                };
                let reporter = EgressReporter::new(
                    app_client::Client::Mock(mock.clone()),
                    Arc::new(AppKey::import([0x42; 32])),
                    TEST_URL.parse().unwrap(),
                    Duration::from_secs(3600),
                    500,
                    Some(spool_path.clone()),
                    Some(writer),
                );

                let envelope =
                    br#"{"batch_id":"0123456789abcdef0123456789abcdef"}"#.to_vec();
                reporter
                    .inner
                    .spool
                    .as_ref()
                    .expect("spool configured")
                    .append(&envelope)
                    .await
                    .expect("seeding the spool");
                assert_eq!(writes.load(Ordering::SeqCst), 1, "{case}: seed write");

                // The seed append succeeds; the removal rewrite fails.
                fail_writes.store(true, Ordering::SeqCst);
                let outcome = reporter.inner.flush_attempt(false).await;
                assert_eq!(
                    outcome,
                    FlushOutcome::Retryable,
                    "{case}: a failed removal must keep the envelope for a paced retry"
                );
                let attempts = mock.egress_attempts();
                assert_eq!(attempts.len(), 1, "{case}: exactly one POST");
                assert_eq!(attempts[0], envelope, "{case}: the spooled envelope was sent");
                assert_eq!(
                    writes.load(Ordering::SeqCst),
                    2,
                    "{case}: seed + failed removal attempt"
                );
                assert!(
                    reporter
                        .inner
                        .retry_at()
                        .is_some_and(|at| at > Instant::now()),
                    "{case}: a future retry deadline must be armed after a failed removal"
                );

                // An immediate (non-forced) pass honors the deadline and does
                // not re-post: a paced wait, not zero, prevents the hot loop.
                let idle = reporter.inner.flush_attempt(false).await;
                assert_eq!(idle, FlushOutcome::Idle, "{case}: gated by the backoff");
                assert_eq!(
                    mock.egress_attempts().len(),
                    1,
                    "{case}: no re-send during the backoff window"
                );
                let wait = reporter.inner.next_wait(Instant::now());
                assert!(
                    wait >= RETRY_BASE / 2,
                    "{case}: next wait {wait:?} must be backoff-scale, not zero"
                );
                let spool = reporter.inner.spool.as_ref().expect("spool configured");
                let lines = spool.read_lines().await.expect("reading the spool");
                assert_eq!(lines.len(), 1, "{case}: the envelope is still recorded");
                assert_eq!(lines[0], envelope, "{case}: the unremoved line is intact");

                // The disk recovers and the deadline elapses (rewound rather
                // than sleeping through the backoff).
                fail_writes.store(false, Ordering::SeqCst);
                if reject {
                    mock.reject_egress_reports(false);
                }
                *reporter.inner.retry_at.lock().unwrap() =
                    Some(Instant::now() - Duration::from_millis(1));

                let outcome = reporter.inner.flush_attempt(false).await;
                assert_eq!(outcome, FlushOutcome::Sent, "{case}: recovery delivers");
                assert_eq!(
                    mock.egress_attempts().len(),
                    2,
                    "{case}: the retained envelope is re-posted after recovery"
                );
                assert_eq!(
                    writes.load(Ordering::SeqCst),
                    3,
                    "{case}: recovery removal write"
                );
                assert!(
                    reporter.inner.retry_at().is_none(),
                    "{case}: the retry is cleared once the spool line is removed"
                );
                assert!(
                    spool.read_lines().await.expect("reading the spool").is_empty(),
                    "{case}: the spool is drained"
                );
            }
        }
    }
}
