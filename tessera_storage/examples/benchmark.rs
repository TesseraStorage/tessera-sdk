use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::io::{BufRead, stdin};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use directories::ProjectDirs;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tessera_storage::{
    AppKey, AppMetadata, Builder, DownloadOptions, Object, Sdk, ShardFailure, UploadOptions,
    generate_recovery_phrase,
};
use tokio::fs;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, copy};

const APP_META: AppMetadata = AppMetadata {
    id: tessera_storage::app_id!(
        "5c0b1af28e6ac76395b2087ea987297b9c496f90d2ab3e3d3d07980ae4c43633"
    ),
    name: "Benchmark",
    description: "A simple upload and download benchmark for the SDK",
    service_url: "https://sia.tech",
    logo_url: None,
    callback_url: None,
};

const DEFAULT_INDEXER: &str = "https://index.tessera.storage";
const DEFAULT_PROFILE: &str = "default";

#[derive(Parser)]
#[command(name = "benchmark", about = "Benchmark Sia uploads and downloads")]
struct Cli {
    /// Profile to use. Each profile binds an app key to an indexer.
    #[arg(long, default_value = DEFAULT_PROFILE, global = true)]
    profile: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Authorize the app against an indexer and store the resulting key under
    /// the chosen profile, so subsequent runs can skip the auth flow.
    Login {
        /// Indexer URL to authorize against. Recorded on the profile.
        #[arg(long, default_value = DEFAULT_INDEXER)]
        indexer: String,

        /// Generate a new recovery phrase instead of prompting for one.
        #[arg(long)]
        new: bool,
    },
    /// Run the upload/download benchmark using a stored profile.
    Run {
        /// Size of the data to upload and download in bytes (default: 120 MiB)
        #[arg(short, long, default_value_t = 120 * 1024 * 1024)]
        size: usize,

        /// Warms the SDK by doing a smaller upload and download before the actual benchmark
        /// to seed the upload and download pipeline's metrics.
        #[arg(short, long)]
        warm: bool,

        /// Maximum number of slabs buffered in memory during upload.
        #[arg(long)]
        upload_max_buffered_slabs: Option<usize>,

        /// Maximum number of chunks buffered in memory during download.
        #[arg(long)]
        download_max_buffered_chunks: Option<usize>,

        /// Print a per-host breakdown of shards and throughput after the run.
        #[arg(long)]
        host_summary: bool,

        /// Write a JSON report of every shard refusal (host low on funds,
        /// timeout, etc.) encountered during the download -- including ones
        /// recovered via substitution -- to this path. Includes the raw
        /// events plus a summary: peak refusals in any single slab, and
        /// refusal counts bucketed into 2-minute funding-cycle windows
        /// (indexd's maintenanceFrequency) measured from download start.
        #[arg(long)]
        shard_failures_report: Option<PathBuf>,
    },
    /// List configured profiles.
    Profiles,
}

fn progress_bar(size: u64, msg: &'static str) -> ProgressBar {
    let pb = ProgressBar::new(size);
    pb.set_style(
        ProgressStyle::with_template(
            "{msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bitrate}, {eta})",
        )
        .unwrap()
        .with_key("bitrate", |state: &ProgressState, w: &mut dyn std::fmt::Write| {
            let elapsed = state.elapsed();
            let rate = if elapsed.is_zero() {
                "0.00 bps".to_string()
            } else {
                format_bitrate(state.pos(), elapsed)
            };
            let _ = w.write_str(&rate);
        })
        .progress_chars("=>-"),
    );
    pb.set_message(msg);
    pb
}

// A reader that produces a deterministic stream of random bytes based on a seed.
struct SeededReader {
    rng: SmallRng,
    remaining: usize,
}

impl SeededReader {
    fn new(seed: u64, size: usize) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
            remaining: size,
        }
    }
}

impl AsyncRead for SeededReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let n = buf.remaining().min(self.remaining);
        let dst = buf.initialize_unfilled_to(n);
        self.rng.fill_bytes(dst);
        buf.advance(n);
        self.remaining -= n;
        Poll::Ready(Ok(()))
    }
}

struct SeededVerifier {
    rng: SmallRng,
    size: usize,
    remaining: usize,
    start: Instant,
    ttfb: Option<Duration>,
    elapsed: Vec<Duration>,
    progress: ProgressBar,
}

impl SeededVerifier {
    fn new(seed: u64, size: usize, progress: ProgressBar) -> Self {
        let now = Instant::now();
        Self {
            rng: SmallRng::seed_from_u64(seed),
            size,
            remaining: size,
            start: now,
            ttfb: None,
            elapsed: Vec::new(),
            progress,
        }
    }

    fn ttfb(&self) -> Option<Duration> {
        self.ttfb
    }

    fn gap_max(&self) -> Option<Duration> {
        if self.elapsed.is_empty() {
            return None;
        }
        self.elapsed.windows(2).map(|w| w[1] - w[0]).max()
    }
}

impl AsyncWrite for SeededVerifier {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let now = Instant::now();
        if self.ttfb.is_none() {
            self.ttfb = Some(now - self.start);
        }
        let elapsed = now - self.start;
        let mut expected = vec![0u8; buf.len()];
        self.rng.fill_bytes(&mut expected);
        if buf.len() > self.remaining {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("expected {} more bytes, got {}", self.remaining, buf.len()),
            )));
        }
        if buf != expected.as_slice() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("data mismatch at byte {}", self.size - self.remaining),
            )));
        }
        self.remaining -= buf.len();
        self.elapsed.push(elapsed);
        self.progress.inc(buf.len() as u64);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.remaining != 0 {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("expected {} more bytes", self.remaining),
            )));
        }
        Poll::Ready(Ok(()))
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    for &unit in UNITS {
        if value < 1024.0 {
            return format!("{value:.2} {unit}");
        }
        if unit == UNITS[UNITS.len() - 1] {
            return format!("{value:.2} {unit}");
        }
        value /= 1024.0;
    }
    unreachable!()
}

fn format_bitrate(bytes: u64, duration: Duration) -> String {
    let bits_per_sec = (bytes as f64 * 8.0) / duration.as_secs_f64();
    const UNITS: &[&str] = &["bps", "Kbps", "Mbps", "Gbps", "Tbps"];
    let mut value = bits_per_sec;
    for &unit in UNITS {
        if value < 1000.0 {
            return format!("{value:.2} {unit}");
        }
        if unit == UNITS[UNITS.len() - 1] {
            return format!("{value:.2} {unit}");
        }
        value /= 1000.0;
    }
    unreachable!()
}

#[derive(Default, Deserialize, Serialize)]
struct Config {
    #[serde(default)]
    profiles: BTreeMap<String, Profile>,
}

#[derive(Deserialize, Serialize)]
struct Profile {
    indexer: String,
    /// 32-byte app key, hex-encoded.
    app_key: String,
}

fn config_path() -> Result<PathBuf, Box<dyn Error>> {
    let dirs = ProjectDirs::from("tech", "Sia", "sia-benchmark")
        .ok_or("could not determine config directory")?;
    Ok(dirs.config_dir().join("config.toml"))
}

async fn load_config() -> Result<Config, Box<dyn Error>> {
    let path = config_path()?;
    match fs::read_to_string(&path).await {
        Ok(text) => Ok(toml::from_str(&text)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(e.into()),
    }
}

async fn save_config(config: &Config) -> Result<PathBuf, Box<dyn Error>> {
    let path = config_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(&path, toml::to_string_pretty(config)?).await?;
    Ok(path)
}

async fn read_profile(profile: &str) -> Result<(String, AppKey), Box<dyn Error>> {
    let config = load_config().await?;
    let p = config.profiles.get(profile).ok_or_else(|| {
        format!("profile `{profile}` not found; run `benchmark --profile {profile} login` first")
    })?;
    let mut buf = [0u8; 32];
    hex::decode_to_slice(&p.app_key, &mut buf)?;
    Ok((p.indexer.clone(), AppKey::import(buf)))
}

async fn upsert_profile(
    profile: &str,
    indexer: &str,
    key: &AppKey,
) -> Result<PathBuf, Box<dyn Error>> {
    let mut config = load_config().await?;
    config.profiles.insert(
        profile.to_string(),
        Profile {
            indexer: indexer.trim().to_string(),
            app_key: hex::encode(key.export()),
        },
    );
    save_config(&config).await
}

async fn login(profile: &str, indexer: &str, new: bool) -> Result<(), Box<dyn Error>> {
    let builder = Builder::new(indexer, APP_META)?;
    let builder = builder.request_connection().await?;
    println!(
        "Visit the following URL to authorize the application: {}",
        builder.response_url()
    );
    let builder = builder.wait_for_approval().await?;
    println!("Connection approved!");

    let phrase = if new {
        let phrase = generate_recovery_phrase();
        println!("Generated recovery phrase (write it down):\n  {phrase}");
        phrase
    } else {
        println!("Enter recovery phrase:");
        stdin()
            .lock()
            .lines()
            .next()
            .ok_or("failed to read recovery phrase")??
    };

    let sdk = builder.register(&phrase).await?;
    let path = upsert_profile(profile, indexer, sdk.app_key()).await?;
    println!(
        "Profile `{profile}` saved to {} (indexer: {indexer})",
        path.display()
    );
    Ok(())
}

async fn connect(profile: &str) -> Result<Sdk, Box<dyn Error>> {
    let (indexer, app_key) = read_profile(profile).await?;
    let builder = Builder::new(&indexer, APP_META)?;
    let sdk = builder.connected(&app_key).await?.ok_or_else(|| {
        format!("app key for profile `{profile}` is not authenticated; run `benchmark --profile {profile} login`")
    })?;
    let account = sdk.account().await?;
    if !account.ready {
        return Err("account is not ready yet — the indexer is still propagating registration on the network; try again shortly".into());
    }
    Ok(sdk)
}

async fn list_profiles() -> Result<(), Box<dyn Error>> {
    let config = load_config().await?;
    if config.profiles.is_empty() {
        println!("No profiles configured. Run `benchmark login` to create one.");
        return Ok(());
    }
    let pad = config.profiles.keys().map(|n| n.len()).max().unwrap_or(0);
    for (name, profile) in &config.profiles {
        println!("  {name:<pad$}  {}", profile.indexer);
    }
    Ok(())
}

#[derive(Default)]
struct HostStat {
    shards: usize,
    bytes: u64,
    /// Summed per-shard transfer time. Shards to the same host can overlap, so
    /// this overcounts wall-clock; it estimates the per-connection rate.
    elapsed: Duration,
}

type HostStats = Arc<Mutex<HashMap<String, HostStat>>>;

fn record_shard(stats: &HostStats, host: String, bytes: u64, elapsed: Duration) {
    let mut map = stats.lock().unwrap();
    let entry = map.entry(host).or_default();
    entry.shards += 1;
    entry.bytes += bytes;
    entry.elapsed += elapsed;
}

fn print_host_summary(label: &str, stats: &HostStats) {
    let map = stats.lock().unwrap();
    if map.is_empty() {
        return;
    }
    let rate = |s: &HostStat| {
        if s.elapsed.is_zero() {
            0.0
        } else {
            s.bytes as f64 / s.elapsed.as_secs_f64()
        }
    };
    let mut rows: Vec<_> = map.iter().collect();
    rows.sort_by(|a, b| rate(b.1).total_cmp(&rate(a.1)));
    println!("\n{label} per-host summary ({} hosts):", map.len());
    for (host, s) in &rows {
        println!(
            "  {host}  {:>4} shards  {:>11}  {}",
            s.shards,
            format_bytes(s.bytes),
            format_bitrate(s.bytes, s.elapsed),
        );
    }
    let total: u64 = map.values().map(|s| s.bytes).sum();
    println!("  total {} across {} hosts", format_bytes(total), map.len());
}

// Runs an upload/download roundtrip purely to seed the pipeline metrics. It
// shows progress bars and verifies the data, but records no host stats and
// prints no results.
async fn warmup(
    sdk: &Sdk,
    size: usize,
    upload_options: UploadOptions,
    download_options: DownloadOptions,
) -> Result<(), Box<dyn Error>> {
    let seed: u64 = rand::random();
    let reader = SeededReader::new(seed, size);

    let encoded_size = tessera_storage::encoded_size(
        size as u64,
        upload_options.data_shards,
        upload_options.parity_shards,
    );
    let upload_progress = progress_bar(size as u64, "warm up ");
    let upload_progress_cb = upload_progress.clone();
    let encoded_uploaded = Arc::new(AtomicU64::new(0));
    let unencoded_size = size as u64;

    let upload_options = upload_options.on_shard_uploaded(move |p| {
        let encoded = encoded_uploaded.fetch_add(p.shard_size as u64, Ordering::Relaxed)
            + p.shard_size as u64;
        let unencoded = (encoded as u128 * unencoded_size as u128 / encoded_size as u128) as u64;
        upload_progress_cb.set_position(unencoded);
    });
    let obj = sdk
        .upload(Object::default(), reader, upload_options)
        .await?;
    upload_progress.finish();

    let download_progress = progress_bar(size as u64, "warm dl ");
    let mut verifier = SeededVerifier::new(seed, size, download_progress.clone());
    let mut reader = sdk.download(&obj, download_options).await?;
    copy(&mut reader, &mut verifier).await?;
    download_progress.finish();
    Ok(())
}

async fn run_benchmark(
    sdk: Sdk,
    size: usize,
    mut upload_options: UploadOptions,
    mut download_options: DownloadOptions,
    host_summary: bool,
    shard_failures_report: Option<PathBuf>,
) {
    let seed: u64 = rand::random();
    let reader = SeededReader::new(seed, size);

    let encoded_size = tessera_storage::encoded_size(
        size as u64,
        upload_options.data_shards,
        upload_options.parity_shards,
    );

    let upload_progress = progress_bar(size as u64, "upload  ");
    upload_progress.set_message("upload");
    let upload_progress_cb = upload_progress.clone();
    let encoded_uploaded = Arc::new(AtomicU64::new(0));
    let unencoded_size = size as u64;
    let upload_hosts: HostStats = Arc::new(Mutex::new(HashMap::new()));
    let upload_hosts_cb = upload_hosts.clone();
    upload_options = upload_options.on_shard_uploaded(move |p| {
        let encoded = encoded_uploaded.fetch_add(p.shard_size as u64, Ordering::Relaxed)
            + p.shard_size as u64;
        let unencoded = (encoded as u128 * unencoded_size as u128 / encoded_size as u128) as u64;
        upload_progress_cb.set_position(unencoded);
        record_shard(
            &upload_hosts_cb,
            p.host_key.to_string(),
            p.shard_size as u64,
            p.elapsed,
        );
    });
    let start = Instant::now();
    let obj = sdk
        .upload(Object::default(), reader, upload_options)
        .await
        .expect("failed to upload object");
    let upload_duration = start.elapsed();
    upload_progress.finish();

    let download_hosts: HostStats = Arc::new(Mutex::new(HashMap::new()));
    let download_hosts_cb = download_hosts.clone();
    download_options = download_options.on_shard_downloaded(move |p| {
        record_shard(
            &download_hosts_cb,
            p.host_key.to_string(),
            p.shard_size as u64,
            p.elapsed,
        );
    });

    // Shard-refusal telemetry (added 2026-11-28 for the funding-floor
    // trial): every host refusal/failure during the download, timestamped
    // relative to download start so refusals can be bucketed into
    // funding-cycle windows after the fact.
    let failures: Arc<Mutex<Vec<(Duration, ShardFailure)>>> = Arc::new(Mutex::new(Vec::new()));
    let download_start_marker = Instant::now();
    if shard_failures_report.is_some() {
        let failures_cb = failures.clone();
        download_options = download_options.on_shard_failed(move |f: ShardFailure| {
            failures_cb
                .lock()
                .unwrap()
                .push((download_start_marker.elapsed(), f));
        });
    }

    let download_progress = progress_bar(size as u64, "download");
    let start = Instant::now();
    let mut verifier = SeededVerifier::new(seed, size, download_progress.clone());
    let mut reader = sdk
        .download(&obj, download_options)
        .await
        .expect("failed to start download");
    copy(&mut reader, &mut verifier)
        .await
        .expect("failed to copy data");
    let download_duration = start.elapsed();
    download_progress.finish();
    println!("\n{:<15}{}", "Size:", format_bytes(obj.size()));
    println!("{:<15}{}", "Encoded:", format_bytes(obj.encoded_size()));
    println!("\nUpload");
    println!("  {:<15}{:?}", "Elapsed:", upload_duration);
    println!(
        "  {:<15}{}",
        "Rate:",
        format_bitrate(obj.size(), upload_duration)
    );
    println!(
        "  {:<15}{}",
        "Encoded Rate:",
        format_bitrate(obj.encoded_size(), upload_duration)
    );

    println!("\nDownload");
    println!("  {:<15}{:?}", "Elapsed:", download_duration);
    println!("  {:<15}{:?}", "TTFB:", verifier.ttfb().unwrap_or_default());
    println!(
        "  {:<15}{}",
        "Rate:",
        format_bitrate(obj.size(), download_duration)
    );
    println!(
        "  {:<15}{:?}",
        "Max latency:",
        verifier.gap_max().unwrap_or_default()
    );

    if host_summary {
        print_host_summary("Upload", &upload_hosts);
        print_host_summary("Download", &download_hosts);
    }

    if let Some(path) = shard_failures_report {
        if let Err(e) = write_shard_failures_report(&path, &failures, download_duration) {
            eprintln!("failed to write shard failures report to {path:?}: {e}");
        } else {
            println!("\nShard failures report written to {}", path.display());
        }
    }
}

/// Funding cycle window used by indexd's maintenance loop
/// (`maintenanceFrequency: 2 * time.Minute`, contracts/manager.go). Used
/// to bucket refusal events into cycle windows for peak-depletion analysis.
const FUNDING_CYCLE: Duration = Duration::from_secs(120);

#[derive(Serialize)]
struct ShardFailureEvent {
    host_key: String,
    shard_index: usize,
    slab_index: usize,
    since_download_start_ms: u128,
    funding_cycle: u64,
    elapsed_ms: u128,
    error: String,
}

#[derive(Serialize)]
struct CycleBucket {
    funding_cycle: u64,
    refusal_count: usize,
    distinct_hosts_refused: usize,
}

#[derive(Serialize)]
struct ShardFailuresReport {
    download_duration_ms: u128,
    total_refusals: usize,
    distinct_hosts_refused: usize,
    /// The highest number of refusals observed for any single slab -- the
    /// number you get by asking "which slab had the worst time getting to
    /// min_shards successes".
    peak_refusals_per_slab: usize,
    /// slab_index of the slab that hit peak_refusals_per_slab (first one,
    /// if tied).
    peak_slab_index: Option<usize>,
    /// Refusals bucketed into 2-minute funding-cycle windows (cycle 0 =
    /// the first 2 minutes of the download, etc.). The cycle with the
    /// highest refusal_count is the empirical "max depletion within one
    /// funding cycle" for this run.
    cycle_buckets: Vec<CycleBucket>,
    events: Vec<ShardFailureEvent>,
}

fn write_shard_failures_report(
    path: &std::path::Path,
    failures: &Arc<Mutex<Vec<(Duration, ShardFailure)>>>,
    download_duration: Duration,
) -> Result<(), Box<dyn Error>> {
    let failures = failures.lock().unwrap();

    let mut per_slab: HashMap<usize, usize> = HashMap::new();
    let mut per_cycle: BTreeMap<u64, (usize, std::collections::HashSet<String>)> =
        BTreeMap::new();
    let mut distinct_hosts: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut events = Vec::with_capacity(failures.len());

    for (since_start, f) in failures.iter() {
        let cycle = since_start.as_secs() / FUNDING_CYCLE.as_secs();
        *per_slab.entry(f.slab_index).or_default() += 1;
        let entry = per_cycle.entry(cycle).or_default();
        entry.0 += 1;
        entry.1.insert(f.host_key.to_string());
        distinct_hosts.insert(f.host_key.to_string());
        events.push(ShardFailureEvent {
            host_key: f.host_key.to_string(),
            shard_index: f.shard_index,
            slab_index: f.slab_index,
            since_download_start_ms: since_start.as_millis(),
            funding_cycle: cycle,
            elapsed_ms: f.elapsed.as_millis(),
            error: f.error.clone(),
        });
    }

    let (peak_slab_index, peak_refusals_per_slab) = per_slab
        .iter()
        .max_by_key(|(_, count)| **count)
        .map(|(slab, count)| (Some(*slab), *count))
        .unwrap_or((None, 0));

    let cycle_buckets = per_cycle
        .into_iter()
        .map(|(funding_cycle, (refusal_count, hosts))| CycleBucket {
            funding_cycle,
            refusal_count,
            distinct_hosts_refused: hosts.len(),
        })
        .collect();

    let report = ShardFailuresReport {
        download_duration_ms: download_duration.as_millis(),
        total_refusals: events.len(),
        distinct_hosts_refused: distinct_hosts.len(),
        peak_refusals_per_slab,
        peak_slab_index,
        cycle_buckets,
        events,
    };

    let json = serde_json::to_string_pretty(&report)?;
    std::fs::write(path, json)?;
    Ok(())
}

#[cfg(test)]
mod shard_failures_report_tests {
    use super::*;

    #[test]
    fn peak_and_cycle_bucketing() {
        let failures = Arc::new(Mutex::new(vec![
            // cycle 0 (0-120s): slab 0 refused by 2 different hosts
            (
                Duration::from_secs(10),
                ShardFailure {
                    host_key: sia_core::signing::PrivateKey::from_seed(&[1; 32]).public_key(),
                    shard_index: 3,
                    slab_index: 0,
                    elapsed: Duration::from_millis(50),
                    error: "insufficient funds".to_string(),
                },
            ),
            (
                Duration::from_secs(20),
                ShardFailure {
                    host_key: sia_core::signing::PrivateKey::from_seed(&[2; 32]).public_key(),
                    shard_index: 7,
                    slab_index: 0,
                    elapsed: Duration::from_millis(60),
                    error: "insufficient funds".to_string(),
                },
            ),
            // cycle 1 (120-240s): slab 1 refused by 3 different hosts --
            // this is the real peak (3 > 2).
            (
                Duration::from_secs(125),
                ShardFailure {
                    host_key: sia_core::signing::PrivateKey::from_seed(&[3; 32]).public_key(),
                    shard_index: 1,
                    slab_index: 1,
                    elapsed: Duration::from_millis(40),
                    error: "insufficient funds".to_string(),
                },
            ),
            (
                Duration::from_secs(130),
                ShardFailure {
                    host_key: sia_core::signing::PrivateKey::from_seed(&[4; 32]).public_key(),
                    shard_index: 2,
                    slab_index: 1,
                    elapsed: Duration::from_millis(45),
                    error: "insufficient funds".to_string(),
                },
            ),
            (
                Duration::from_secs(135),
                ShardFailure {
                    // same host as cycle 0's first failure -- distinct-host
                    // counting must still be correct per-cycle, not just
                    // globally.
                    host_key: sia_core::signing::PrivateKey::from_seed(&[1; 32]).public_key(),
                    shard_index: 4,
                    slab_index: 1,
                    elapsed: Duration::from_millis(55),
                    error: "insufficient funds".to_string(),
                },
            ),
        ]));

        let path = std::env::temp_dir().join(format!(
            "shard_failures_test_{}.json",
            std::process::id()
        ));
        write_shard_failures_report(&path, &failures, Duration::from_secs(300)).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::remove_file(&path).ok();
        let report: ShardFailuresReportTest = serde_json::from_str(&text).unwrap();

        assert_eq!(report.total_refusals, 5);
        assert_eq!(report.distinct_hosts_refused, 4, "hosts seeded 1,2,3,4 -- host 1 repeats");
        assert_eq!(
            report.peak_refusals_per_slab, 3,
            "slab 1 has 3 refusals, slab 0 has 2 -- peak must be 3"
        );
        assert_eq!(report.peak_slab_index, Some(1));
        assert_eq!(report.cycle_buckets.len(), 2, "events fall in cycle 0 and cycle 1 only");
        assert_eq!(report.cycle_buckets[0].funding_cycle, 0);
        assert_eq!(report.cycle_buckets[0].refusal_count, 2);
        assert_eq!(report.cycle_buckets[0].distinct_hosts_refused, 2);
        assert_eq!(report.cycle_buckets[1].funding_cycle, 1);
        assert_eq!(report.cycle_buckets[1].refusal_count, 3);
        assert_eq!(
            report.cycle_buckets[1].distinct_hosts_refused, 3,
            "cycle 1 has 3 events but host [1;32] repeats cycle 0's host -- within cycle 1 itself all 3 hosts (3,4,1) are distinct"
        );
    }

    // Local mirror of the real report shape for deserializing in the test
    // without needing to make every field of the real structs `pub(crate)`.
    #[derive(serde::Deserialize)]
    struct ShardFailuresReportTest {
        total_refusals: usize,
        distinct_hosts_refused: usize,
        peak_refusals_per_slab: usize,
        peak_slab_index: Option<usize>,
        cycle_buckets: Vec<CycleBucketTest>,
    }

    #[derive(serde::Deserialize)]
    struct CycleBucketTest {
        funding_cycle: u64,
        refusal_count: usize,
        distinct_hosts_refused: usize,
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    // Write RUST_LOG output to a timestamped file instead of the terminal
    // so it doesn't interleave with the progress bars.
    let log_path = format!(
        "benchmark-{}.log",
        chrono::Local::now().format("%Y%m%dT%H%M%S")
    );
    let log_file = std::fs::File::create(&log_path)?;
    env_logger::Builder::from_default_env()
        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .init();
    eprintln!("logging to {log_path}");
    let cli = Cli::parse();
    match cli.command {
        Command::Login { indexer, new } => login(&cli.profile, &indexer, new).await,
        Command::Run {
            size,
            warm,
            upload_max_buffered_slabs,
            download_max_buffered_chunks,
            host_summary,
            shard_failures_report,
        } => {
            let sdk = connect(&cli.profile).await?;

            // upload the data to the network
            let upload_options = UploadOptions {
                max_buffered_slabs: upload_max_buffered_slabs,
                ..Default::default()
            };

            let download_options = DownloadOptions {
                max_buffered_chunks: download_max_buffered_chunks,
                ..Default::default()
            };

            if warm {
                let warm_size = size
                    .saturating_div(10)
                    .max(upload_options.optimal_data_size()); // at least a full slab
                warmup(
                    &sdk,
                    warm_size,
                    upload_options.clone(),
                    download_options.clone(),
                )
                .await?;
            }

            run_benchmark(
                sdk,
                size,
                upload_options,
                download_options,
                host_summary,
                shard_failures_report,
            )
            .await;
            Ok(())
        }
        Command::Profiles => list_profiles().await,
    }
}
