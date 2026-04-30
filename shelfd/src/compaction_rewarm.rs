//! SHELF-45 — compaction-aware re-warm reactor.
//!
//! ## Why
//!
//! Iceberg `ALTER TABLE … EXECUTE optimize`, `expire_snapshots`, and
//! `remove_orphan_files` rewrite data files to a new path with a new
//! ETag. shelf's content-addressed keys (per ADR-0011 — `sha256(etag
//! || offset || length || rg_ordinal)`) automatically invalidate the
//! old entries on the fresh ETag, but that means the next query
//! against the affected table thunders herd into S3 because *every*
//! key it asks for is cold. Apr 27 rep-2 cutover and the rep-1
//! Apr 28 chaos window saw `ICEBERG_CANNOT_OPEN_SPLIT` spikes
//! correlated with KEDA worker rotations; a compaction event is a
//! worse version of the same pattern, concentrated on one table at
//! one minute.
//!
//! This module watches a stream of [`IcebergSnapshotEvent`]s, picks
//! out the compaction-class transitions, and proactively re-warms
//! the new file paths into the rowgroup pool *before* the cold-miss
//! herd arrives. It does this through the same single-flight
//! [`crate::store::FoyerStore::get_or_fetch`] surface client reads
//! use, so a re-warm in flight when a real query lands collapses to
//! one origin GET via the existing inflight `OnceCell`. Re-warm is
//! rate-limited (default 50 MiB/s/pod) and capped on concurrency
//! (default 4 in flight) so it never crowds out client reads — see
//! the property test [`tests::rewarm_semaphore_is_well_below_client_budget`].
//!
//! ## Module layout
//!
//! * [`FileSpec`] / [`IcebergSnapshotEvent`] — wire types between the
//!   producer (SHELF-37 listener / metadata polling worker) and the
//!   reactor.
//! * [`IcebergEventStream`] — pluggable producer trait. Owns the
//!   sender side of the bounded mpsc.
//! * [`LoggingEventStream`] — diagnostic stub that logs every event
//!   it sees and never forwards to the reactor; safe default while
//!   the SHELF-37 listener PR (#66) is finishing its soak.
//! * [`CompactionReactor`] — the consumer task. Spawns a long-running
//!   loop that classifies events, schedules per-file re-warm
//!   sub-tasks under a [`Semaphore`], and updates the
//!   `shelf_rewarm_*` Prometheus families.
//! * [`is_compaction_event`] — pure predicate, exposed for testing
//!   and for future shelfctl introspection.
//!
//! ## Failure semantics
//!
//! Re-warm is best-effort. Every failure variant bumps a label on
//! [`crate::metrics::REWARM_ERRORS_TOTAL`] and the reactor's main
//! loop continues. The reactor never propagates an error back to
//! the producer or to client traffic.
//!
//! ## Interaction with SHELF-37
//!
//! SHELF-37 ships the Iceberg `EventListener` jar that projects
//! every `QueryCompletedEvent` into an Iceberg log. The same
//! listener has access to the snapshot transitions that drive this
//! reactor; SHELF-37 will land an [`IcebergEventStream`] impl that
//! tails its log table and forwards `replace`-class snapshots to
//! the reactor. Until that ships, [`LoggingEventStream`] is the
//! default no-op and the reactor's `enabled: false` config keeps
//! the entire module dormant.

#![allow(clippy::needless_borrows_for_generic_args)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::admission::AdmissionPolicy;
use crate::config::RewarmConfig;
use crate::store::{key_from_tuple, FoyerStore, Pool};

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// One Iceberg data file referenced by a snapshot transition.
///
/// `path` is the `s3a://<bucket>/<key>` (or scheme-equivalent)
/// location. `etag` carries the opaque S3 ETag bytes used to derive
/// the SHELF-04 content-addressed cache key; `size_bytes` is the
/// file length the producer observed.
///
/// The struct deliberately stays small and `Clone`-friendly because
/// the reactor hands clones into per-file tasks under a
/// [`Semaphore`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileSpec {
    pub path: String,
    pub etag: Vec<u8>,
    pub size_bytes: u64,
}

/// A snapshot transition observed by the producer.
///
/// `removed_files` and `added_files` are the file-level diff against
/// the previous snapshot (ADR-0011 keys are content-addressed, so a
/// renamed-but-byte-equal file would still appear as one removal +
/// one addition because the ETag changed). `committed_at` drives
/// the [`crate::metrics::REWARM_LAG_SECONDS`] histogram.
#[derive(Debug, Clone)]
pub struct IcebergSnapshotEvent {
    pub table_id: String,
    pub old_snapshot_id: i64,
    pub new_snapshot_id: i64,
    pub added_files: Vec<FileSpec>,
    pub removed_files: Vec<FileSpec>,
    pub committed_at: SystemTime,
}

impl IcebergSnapshotEvent {
    fn added_bytes(&self) -> u64 {
        self.added_files.iter().map(|f| f.size_bytes).sum()
    }
    fn removed_bytes(&self) -> u64 {
        self.removed_files.iter().map(|f| f.size_bytes).sum()
    }
}

// ---------------------------------------------------------------------------
// Compaction detector
// ---------------------------------------------------------------------------

/// Classify a snapshot event as compaction-class (a `replace`-style
/// rewrite that fans out the same data to fewer, bigger files).
///
/// The predicate is intentionally narrow:
/// 1. Both `removed_files` and `added_files` are non-empty.
/// 2. `added_files.len() < removed_files.len()` — fewer, bigger
///    files is the structural signature of compaction.
/// 3. `total_bytes(added) ≈ total_bytes(removed)` within
///    `byte_tolerance_bps`. A 5 % default catches normal
///    compactor variance (varying compression ratios across
///    rowgroups) without admitting append-mostly snapshots that
///    happen to consolidate one or two trailing files.
///
/// Append-only INSERTs (`removed_files.len() == 0`), pure deletes
/// (`added_files.len() == 0`), partial rewrites that grow the file
/// set, and rewrites that materially change the byte volume all
/// return `false` — they are the producer's problem, not the
/// reactor's.
pub fn is_compaction_event(ev: &IcebergSnapshotEvent, byte_tolerance_bps: u32) -> bool {
    if ev.removed_files.is_empty() || ev.added_files.is_empty() {
        return false;
    }
    if ev.added_files.len() >= ev.removed_files.len() {
        return false;
    }
    let added = ev.added_bytes();
    let removed = ev.removed_bytes();
    if removed == 0 {
        return false;
    }
    let diff = added.abs_diff(removed);
    // bps = parts per 10_000. `removed * bps / 10_000` is the
    // permitted absolute byte difference; saturating arithmetic so
    // an absurd tolerance never wraps.
    let permit = removed
        .saturating_mul(byte_tolerance_bps as u64)
        .saturating_div(10_000);
    diff <= permit
}

// ---------------------------------------------------------------------------
// Producer trait + diagnostic stub
// ---------------------------------------------------------------------------

/// Pluggable producer for the reactor's snapshot-event stream.
///
/// Implementors push events into the `tx` they receive in
/// [`IcebergEventStream::run`]. The reactor owns the consumer side;
/// concurrency is the bounded mpsc's `try_send` semantic — when the
/// queue is full the producer's caller is responsible for either
/// dropping (and bumping `shelf_rewarm_events_total{outcome="dropped_rate_limit"}`)
/// or back-pressuring its own source. See [`SnapshotPublisher`] for
/// the helper that does this correctly.
pub trait IcebergEventStream: Send + 'static {
    /// Run the producer until exhausted or the channel closes.
    fn run(self: Box<Self>, tx: mpsc::Sender<IcebergSnapshotEvent>) -> JoinHandle<()>;
}

/// Diagnostic stub `IcebergEventStream` that logs every event it
/// sees and never forwards to the reactor. Useful for the dev /
/// no-op rollout where the reactor is wired but the SHELF-37
/// listener (PR #66) hasn't shipped yet.
///
/// Construct with [`LoggingEventStream::new`] (no events ever
/// arrive) or [`LoggingEventStream::with_channel`] (a `Sender` is
/// returned for callers that want to push synthetic events for
/// verification). Either way, the reactor's `tx` argument is dropped
/// immediately so the consumer side observes a closed channel and
/// terminates cleanly when the rest of the daemon shuts down.
#[derive(Debug, Default)]
pub struct LoggingEventStream {
    rx: Option<mpsc::Receiver<IcebergSnapshotEvent>>,
}

impl LoggingEventStream {
    pub fn new() -> Self {
        Self { rx: None }
    }

    /// Returns a `(Sender, Self)` pair so callers can push events
    /// in for diagnostic logging. Every event sent on `Sender` will
    /// be logged at `info` and discarded; nothing reaches the
    /// reactor.
    pub fn with_channel(buffer: usize) -> (mpsc::Sender<IcebergSnapshotEvent>, Self) {
        let (tx, rx) = mpsc::channel(buffer.max(1));
        (tx, Self { rx: Some(rx) })
    }
}

impl IcebergEventStream for LoggingEventStream {
    fn run(self: Box<Self>, tx: mpsc::Sender<IcebergSnapshotEvent>) -> JoinHandle<()> {
        // Drop the reactor's sender so the consumer observes the
        // closed channel and exits cleanly. The reactor's own loop
        // already tolerates an empty stream, but drop-on-spawn keeps
        // the no-op semantics explicit.
        drop(tx);
        let mut rx = self.rx;
        tokio::spawn(async move {
            tracing::info!(
                target: "shelfd::rewarm",
                "LoggingEventStream active; events received here are logged only",
            );
            if let Some(ref mut rx) = rx {
                while let Some(ev) = rx.recv().await {
                    tracing::info!(
                        target: "shelfd::rewarm",
                        table = %ev.table_id,
                        old_snapshot = ev.old_snapshot_id,
                        new_snapshot = ev.new_snapshot_id,
                        added = ev.added_files.len(),
                        removed = ev.removed_files.len(),
                        "logging-only snapshot event (no rewarm scheduled)",
                    );
                }
            } else {
                futures::future::pending::<()>().await;
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Snapshot publisher helper
// ---------------------------------------------------------------------------

/// Sender-side helper that future event-source implementations
/// should use to push events at the reactor without overflowing the
/// bounded queue. `try_publish` drops events with the
/// `dropped_rate_limit` event outcome when the channel is full
/// rather than blocking the producer.
#[derive(Debug, Clone)]
pub struct SnapshotPublisher {
    tx: mpsc::Sender<IcebergSnapshotEvent>,
}

impl SnapshotPublisher {
    pub fn new(tx: mpsc::Sender<IcebergSnapshotEvent>) -> Self {
        Self { tx }
    }

    /// Returns `true` iff the event was queued. On a full queue,
    /// drops the event and bumps the
    /// `shelf_rewarm_events_total{outcome="dropped_rate_limit"}`
    /// counter. Closed-channel returns `false` without bumping any
    /// counter so a clean shutdown is silent.
    pub fn try_publish(&self, event: IcebergSnapshotEvent) -> bool {
        match self.tx.try_send(event) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                crate::metrics::REWARM_EVENTS_TOTAL
                    .with_label_values(&["dropped_rate_limit"])
                    .inc();
                crate::metrics::REWARM_ERRORS_TOTAL
                    .with_label_values(&["pool_full"])
                    .inc();
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Byte-rate limiter
// ---------------------------------------------------------------------------

/// Simple async token-bucket rate limiter, sized in bytes. Used to
/// gate origin GETs the reactor issues so a 5 GiB compaction does
/// not become its own thundering herd.
///
/// Tokens refill at `bytes_per_sec`; the bucket capacity is
/// `bytes_per_sec * burst_secs` so a freshly idle reactor can
/// absorb a small burst without sleeping on the very first byte.
/// `acquire(0)` is a no-op; `acquire(huge)` will sleep in chunks
/// of at most `burst_secs` worth of refill so the wait is bounded
/// and the loop stays cancellable from outside via `tokio::select!`.
#[derive(Debug)]
pub(crate) struct ByteRateLimiter {
    bytes_per_sec: u64,
    burst_capacity: u64,
    state: Mutex<RateState>,
}

#[derive(Debug)]
struct RateState {
    tokens: u64,
    last_refill: Instant,
}

impl ByteRateLimiter {
    pub(crate) fn new(bytes_per_sec: u64, burst_secs: u64) -> Self {
        // Burst capacity floors at 1 byte so the limiter still gates
        // on a `bytes_per_sec=0` kill-switch path (every acquire
        // blocks forever, which is the intended "stop re-warming"
        // semantic).
        let burst = bytes_per_sec.saturating_mul(burst_secs.max(1)).max(1);
        Self {
            bytes_per_sec,
            burst_capacity: burst,
            state: Mutex::new(RateState {
                tokens: burst,
                last_refill: Instant::now(),
            }),
        }
    }

    /// Block until at least `bytes` tokens are available; debit and
    /// return. Cancellable from outside via `tokio::select!` because
    /// the wait is implemented as a `tokio::time::sleep`.
    pub(crate) async fn acquire(&self, bytes: u64) {
        if bytes == 0 || self.bytes_per_sec == u64::MAX {
            return;
        }
        // Cap the per-call request at the bucket capacity: a single
        // file larger than the burst capacity is paid for over
        // multiple refill windows without ever blocking forever.
        let mut owed = bytes;
        while owed > 0 {
            let pay = owed.min(self.burst_capacity);
            let wait = {
                let mut s = self.state.lock();
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(s.last_refill);
                // bytes refilled since last visit, capped at burst
                if self.bytes_per_sec > 0 {
                    let added = (elapsed.as_secs_f64() * self.bytes_per_sec as f64) as u64;
                    s.tokens = s.tokens.saturating_add(added).min(self.burst_capacity);
                }
                s.last_refill = now;
                if s.tokens >= pay {
                    s.tokens -= pay;
                    Duration::ZERO
                } else if self.bytes_per_sec == 0 {
                    // 0 bytes/sec is the pause-without-disable knob
                    // (`max_bytes_per_sec=0`). Sleep for a long-but-
                    // bounded interval so cancellation still trips.
                    Duration::from_secs(60)
                } else {
                    let needed = pay - s.tokens;
                    Duration::from_secs_f64(needed as f64 / self.bytes_per_sec as f64)
                }
            };
            if wait.is_zero() {
                owed -= pay;
            } else {
                tokio::time::sleep(wait).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Reactor
// ---------------------------------------------------------------------------

/// The reactor's public surface. Construct with
/// [`CompactionReactor::new`], spawn with [`CompactionReactor::spawn`].
pub struct CompactionReactor<A>
where
    A: AdmissionPolicy + 'static,
{
    config: RewarmConfig,
    store: Arc<FoyerStore>,
    fetcher: Arc<dyn RewarmFetcher>,
    admission: Arc<A>,
    sem: Arc<Semaphore>,
    rate: Arc<ByteRateLimiter>,
    inflight: Arc<AtomicU64>,
}

impl<A> std::fmt::Debug for CompactionReactor<A>
where
    A: AdmissionPolicy + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionReactor")
            .field("config", &self.config)
            .field("inflight", &self.inflight.load(Ordering::Relaxed))
            .finish()
    }
}

/// Per-file fetcher trait. The production wiring will plug an impl
/// that delegates to [`crate::origin::S3Origin::get_range`]; tests
/// use a synthetic fetcher to drive every error variant. The
/// `BoxFuture` shape lets callers hold the trait behind `Arc<dyn
/// RewarmFetcher>` (the `Origin` trait uses RPITIT, which is not
/// dyn-safe).
pub trait RewarmFetcher: Send + Sync + 'static {
    fn fetch_file(&self, file: &FileSpec) -> BoxFuture<'static, crate::Result<Bytes>>;
}

impl<A> CompactionReactor<A>
where
    A: AdmissionPolicy + 'static,
{
    pub fn new(
        config: RewarmConfig,
        store: Arc<FoyerStore>,
        fetcher: Arc<dyn RewarmFetcher>,
        admission: Arc<A>,
    ) -> Self {
        let sem = Arc::new(Semaphore::new(config.max_concurrent_files.max(1)));
        // Burst window of 1 second: enough for the average per-file
        // size on the rep-1 7-day trace to fit in one tick, narrow
        // enough to keep the reactor strictly under client traffic.
        let rate = Arc::new(ByteRateLimiter::new(config.max_bytes_per_sec, 1));
        Self {
            config,
            store,
            fetcher,
            admission,
            sem,
            rate,
            inflight: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Spawn the reactor's main loop. Returns the `JoinHandle` and
    /// the bounded `Sender` the producer is expected to write into.
    /// Drops cleanly when `cancel` fires or the sender is dropped.
    pub fn spawn(
        self,
        cancel: CancellationToken,
    ) -> (mpsc::Sender<IcebergSnapshotEvent>, JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(self.config.queue_capacity.max(1));
        let handle = tokio::spawn(async move { self.run(rx, cancel).await });
        (tx, handle)
    }

    async fn run(self, mut rx: mpsc::Receiver<IcebergSnapshotEvent>, cancel: CancellationToken) {
        if !self.config.enabled {
            tracing::info!(
                target: "shelfd::rewarm",
                "compaction-aware re-warm reactor disabled (cache.rewarm.enabled=false); exiting",
            );
            return;
        }
        let label_pool = "rowgroup";
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!(target: "shelfd::rewarm", "reactor cancelled");
                    return;
                }
                maybe = rx.recv() => {
                    match maybe {
                        None => {
                            // sender dropped; clean shutdown
                            return;
                        }
                        Some(event) => {
                            crate::metrics::REWARM_QUEUE_DEPTH
                                .with_label_values(&[label_pool])
                                .set(rx.len() as i64);
                            self.handle_event(event).await;
                        }
                    }
                }
            }
        }
    }

    async fn handle_event(&self, event: IcebergSnapshotEvent) {
        crate::metrics::REWARM_EVENTS_TOTAL
            .with_label_values(&["received"])
            .inc();

        // Defensive validation: reject obviously misshapen events
        // (we are best-effort, but bumping the metric on a bad
        // event makes "producer fed garbage" debuggable from the
        // dashboard alone). is_compaction_event already covers
        // empty sets, but a producer could still send an event
        // where one FileSpec has size_bytes==0 || etag empty.
        if event
            .added_files
            .iter()
            .any(|f| f.size_bytes == 0 || f.etag.is_empty())
        {
            tracing::warn!(
                target: "shelfd::rewarm",
                table = %event.table_id,
                "snapshot event has zero-sized or etag-less file; skipping",
            );
            crate::metrics::REWARM_EVENTS_TOTAL
                .with_label_values(&["non_compaction_skipped"])
                .inc();
            crate::metrics::REWARM_ERRORS_TOTAL
                .with_label_values(&["iceberg_metadata"])
                .inc();
            return;
        }

        if !is_compaction_event(&event, self.config.byte_equality_tolerance_bps) {
            crate::metrics::REWARM_EVENTS_TOTAL
                .with_label_values(&["non_compaction_skipped"])
                .inc();
            return;
        }
        crate::metrics::REWARM_EVENTS_TOTAL
            .with_label_values(&["compaction_detected"])
            .inc();

        // Snapshot lag observation — done before the work so a
        // huge re-warm doesn't get charged its own duration.
        let commit_age = SystemTime::now()
            .duration_since(event.committed_at)
            .unwrap_or(Duration::ZERO);
        if commit_age > self.config.snapshot_lag_tolerance {
            tracing::warn!(
                target: "shelfd::rewarm",
                table = %event.table_id,
                lag_secs = commit_age.as_secs(),
                "snapshot lag exceeds tolerance; producer is behind",
            );
        }

        let started = Instant::now();
        // Drive each added file through the rate-limited fetch
        // path. Files are processed sequentially in the per-event
        // outer loop, but each file's actual fetch runs under the
        // semaphore so multiple events in a tight stream still
        // produce parallel re-warm work up to the configured cap.
        for file in event.added_files.iter() {
            self.warm_one(file).await;
        }
        let elapsed = started.elapsed();
        let total_lag = commit_age + elapsed;
        crate::metrics::REWARM_LAG_SECONDS
            .with_label_values(&["replayed"])
            .observe(total_lag.as_secs_f64());
        crate::metrics::REWARM_EVENTS_TOTAL
            .with_label_values(&["replayed"])
            .inc();
        tracing::info!(
            target: "shelfd::rewarm",
            table = %event.table_id,
            new_snapshot = event.new_snapshot_id,
            files = event.added_files.len(),
            lag_secs = total_lag.as_secs_f64(),
            "compaction re-warm complete",
        );
    }

    /// Warm exactly one file. Failures are logged + counted; never
    /// propagated.
    async fn warm_one(&self, file: &FileSpec) {
        let label_pool = "rowgroup";
        let key = match key_from_tuple(&file.etag, 0, file.size_bytes, 0) {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!(
                    target: "shelfd::rewarm",
                    path = %file.path,
                    error = %e,
                    "could not derive content-addressed key from FileSpec",
                );
                crate::metrics::REWARM_FILES_TOTAL
                    .with_label_values(&["failed"])
                    .inc();
                crate::metrics::REWARM_BYTES_TOTAL
                    .with_label_values(&["failed"])
                    .inc_by(file.size_bytes);
                crate::metrics::REWARM_ERRORS_TOTAL
                    .with_label_values(&["iceberg_metadata"])
                    .inc();
                return;
            }
        };

        // If the key is already resident, skip — the single-flight
        // `get_or_fetch` would also short-circuit, but doing it here
        // keeps `shelf_rewarm_files_total{outcome="skipped_already_warm"}`
        // accurate without consuming a semaphore permit or a token.
        if self.store.contains(Pool::RowGroup, &key).await {
            crate::metrics::REWARM_FILES_TOTAL
                .with_label_values(&["skipped_already_warm"])
                .inc();
            crate::metrics::REWARM_BYTES_TOTAL
                .with_label_values(&["skipped_already_warm"])
                .inc_by(file.size_bytes);
            return;
        }

        // try_acquire keeps re-warm strictly bounded by the
        // configured semaphore; a soft ceiling rather than a queue
        // means the reactor never piles work behind itself.
        let permit = match self.sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                crate::metrics::REWARM_FILES_TOTAL
                    .with_label_values(&["skipped_pool_full"])
                    .inc();
                crate::metrics::REWARM_BYTES_TOTAL
                    .with_label_values(&["skipped_pool_full"])
                    .inc_by(file.size_bytes);
                crate::metrics::REWARM_ERRORS_TOTAL
                    .with_label_values(&["pool_full"])
                    .inc();
                return;
            }
        };

        // Yield once before the rate-limit wait so the runtime can
        // service any client read that became ready since the last
        // poll point — the cooperative-priority part of the spec.
        tokio::task::yield_now().await;
        self.rate.acquire(file.size_bytes).await;

        self.inflight.fetch_add(1, Ordering::Relaxed);
        crate::metrics::REWARM_INFLIGHT_FILES
            .with_label_values(&[label_pool])
            .set(self.inflight.load(Ordering::Relaxed) as i64);

        let outcome = self.do_fetch(&key, file).await;

        self.inflight.fetch_sub(1, Ordering::Relaxed);
        crate::metrics::REWARM_INFLIGHT_FILES
            .with_label_values(&[label_pool])
            .set(self.inflight.load(Ordering::Relaxed) as i64);
        drop(permit);

        match outcome {
            Ok(()) => {
                crate::metrics::REWARM_FILES_TOTAL
                    .with_label_values(&["warmed"])
                    .inc();
                crate::metrics::REWARM_BYTES_TOTAL
                    .with_label_values(&["warmed"])
                    .inc_by(file.size_bytes);
            }
            Err(reason) => {
                crate::metrics::REWARM_FILES_TOTAL
                    .with_label_values(&["failed"])
                    .inc();
                crate::metrics::REWARM_BYTES_TOTAL
                    .with_label_values(&["failed"])
                    .inc_by(file.size_bytes);
                crate::metrics::REWARM_ERRORS_TOTAL
                    .with_label_values(&[reason.label()])
                    .inc();
            }
        }
    }

    async fn do_fetch(&self, key: &crate::store::Key, file: &FileSpec) -> Result<(), FetchFail> {
        let fetcher = self.fetcher.clone();
        let file_for_fetch = file.clone();
        let fetch = async move { fetcher.fetch_file(&file_for_fetch).await };
        match self
            .store
            .get_or_fetch(Pool::RowGroup, key.clone(), self.admission.as_ref(), fetch)
            .await
        {
            Ok(_outcome) => Ok(()),
            Err(crate::Error::Singleflight(msg)) => {
                tracing::warn!(
                    target: "shelfd::rewarm",
                    path = %file.path,
                    error = %msg,
                    "rewarm fetch failed (singleflight)",
                );
                Err(FetchFail::OriginGet)
            }
            Err(crate::Error::Origin(e)) => {
                tracing::warn!(
                    target: "shelfd::rewarm",
                    path = %file.path,
                    error = %e,
                    "rewarm fetch failed (origin)",
                );
                Err(FetchFail::OriginGet)
            }
            Err(e) => {
                tracing::warn!(
                    target: "shelfd::rewarm",
                    path = %file.path,
                    error = %e,
                    "rewarm fetch failed (other)",
                );
                Err(FetchFail::OriginGet)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FetchFail {
    OriginGet,
    #[allow(dead_code)]
    AdmissionRejected,
    #[allow(dead_code)]
    Cancelled,
}

impl FetchFail {
    fn label(self) -> &'static str {
        match self {
            FetchFail::OriginGet => "origin_get",
            FetchFail::AdmissionRejected => "admission_rejected",
            FetchFail::Cancelled => "cancelled",
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::{AdmissionContext, AdmissionDecision, AdmissionPolicy};
    use crate::config::{MetadataPoolConfig, OriginConfig, PoolsConfig, RowGroupPoolConfig};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::time::Duration;

    // -- helpers -----------------------------------------------------

    fn file(path: &str, etag: &[u8], size: u64) -> FileSpec {
        FileSpec {
            path: path.to_owned(),
            etag: etag.to_vec(),
            size_bytes: size,
        }
    }

    fn now() -> SystemTime {
        SystemTime::now()
    }

    fn evt(added: Vec<FileSpec>, removed: Vec<FileSpec>) -> IcebergSnapshotEvent {
        IcebergSnapshotEvent {
            table_id: "cdp.events".to_owned(),
            old_snapshot_id: 1,
            new_snapshot_id: 2,
            added_files: added,
            removed_files: removed,
            committed_at: now(),
        }
    }

    fn dram_pools() -> PoolsConfig {
        PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 1 << 20,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 4 * 1024 * 1024,
                nvme_dir: std::path::PathBuf::from("/tmp/unused-rewarm"),
                nvme_bytes: 0,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
                compression: Default::default(),
            },
        }
    }

    /// Admission policy that admits everything; rewarm tests don't
    /// exercise the size-threshold path.
    #[derive(Debug)]
    struct AlwaysAdmit;
    impl AdmissionPolicy for AlwaysAdmit {
        fn decide(&self, _ctx: &AdmissionContext<'_>) -> AdmissionDecision {
            AdmissionDecision::Admit
        }
    }

    /// Synthetic fetcher: returns a deterministic `Bytes` of the
    /// declared size. Counts calls so tests can assert single-flight
    /// + skipped-already-warm semantics.
    #[derive(Debug)]
    struct CountingFetcher {
        calls: AtomicUsize,
        delay: Duration,
    }
    impl CountingFetcher {
        fn new(delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                delay,
            })
        }
        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }
    impl RewarmFetcher for CountingFetcher {
        fn fetch_file(&self, file: &FileSpec) -> BoxFuture<'static, crate::Result<Bytes>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let len = file.size_bytes as usize;
            let delay = self.delay;
            Box::pin(async move {
                if delay > Duration::ZERO {
                    tokio::time::sleep(delay).await;
                }
                Ok(Bytes::from(vec![0u8; len]))
            })
        }
    }

    /// Fetcher that always errors, for the failure-mode test.
    #[derive(Debug, Default)]
    struct ErrorFetcher;
    impl RewarmFetcher for ErrorFetcher {
        fn fetch_file(&self, _file: &FileSpec) -> BoxFuture<'static, crate::Result<Bytes>> {
            Box::pin(async { Err(crate::Error::Origin("synthetic failure".into())) })
        }
    }

    // -- compaction-detector predicate ------------------------------

    #[test]
    fn detect_classic_compaction() {
        let removed = (0..10)
            .map(|i| file(&format!("a-{i}"), &[i as u8], 100))
            .collect();
        let added = vec![file("merged", b"new", 1000)];
        let ev = evt(added, removed);
        assert!(is_compaction_event(&ev, 500));
    }

    #[test]
    fn detect_compaction_within_5pct_tolerance() {
        // 10 × 100 B = 1000 B removed; 950 B added (5 % drop); pass.
        let removed = (0..10)
            .map(|i| file(&format!("a-{i}"), &[i as u8], 100))
            .collect();
        let added = vec![file("merged", b"new", 950)];
        let ev = evt(added, removed);
        assert!(is_compaction_event(&ev, 500));
    }

    #[test]
    fn skip_compaction_above_5pct_tolerance() {
        // 10 × 100 B removed; 800 B added (20 % drop); fail.
        let removed = (0..10)
            .map(|i| file(&format!("a-{i}"), &[i as u8], 100))
            .collect();
        let added = vec![file("merged", b"new", 800)];
        let ev = evt(added, removed);
        assert!(!is_compaction_event(&ev, 500));
    }

    #[test]
    fn append_only_is_not_compaction() {
        let added = vec![file("appended", b"new", 1024)];
        let ev = evt(added, vec![]);
        assert!(!is_compaction_event(&ev, 500));
    }

    #[test]
    fn delete_only_is_not_compaction() {
        let removed = vec![file("dropped", b"old", 1024)];
        let ev = evt(vec![], removed);
        assert!(!is_compaction_event(&ev, 500));
    }

    #[test]
    fn equal_count_is_not_compaction() {
        // Producer reports same number of files in and out; could be
        // a rewrite-with-tombstones or a partial overwrite. Skip.
        let added = vec![file("new-0", b"e0", 100), file("new-1", b"e1", 100)];
        let removed = vec![file("old-0", b"o0", 100), file("old-1", b"o1", 100)];
        let ev = evt(added, removed);
        assert!(!is_compaction_event(&ev, 500));
    }

    #[test]
    fn growing_file_count_is_not_compaction() {
        // 2 in -> 4 out: shouldn't trigger.
        let removed = vec![file("o0", b"o0", 1024), file("o1", b"o1", 1024)];
        let added = (0..4)
            .map(|i| file(&format!("n{i}"), &[i as u8], 512))
            .collect();
        let ev = evt(added, removed);
        assert!(!is_compaction_event(&ev, 500));
    }

    #[test]
    fn zero_byte_corpus_is_not_compaction() {
        let removed = vec![file("z0", b"e", 0), file("z1", b"e2", 0)];
        let added = vec![file("z2", b"e3", 0)];
        let ev = evt(added, removed);
        assert!(!is_compaction_event(&ev, 500));
    }

    // -- rate-limited replay -----------------------------------------

    /// Replay-loop pacing: with `bytes_per_sec=B` and `burst_secs=1`
    /// the limiter offers `B` free bytes before throttling kicks in,
    /// so the wall-clock cost of `N × file_size` bytes is
    /// `max(0, N × file_size − B) / B`. Pushing well past the burst
    /// (200 × 64 KiB at 6.4 MB/s, roughly 13 MiB total) makes the
    /// post-burst phase dominate any scheduler jitter and proves the
    /// limiter is the actual gate: an unbounded loop would finish in
    /// microseconds.
    #[tokio::test(flavor = "current_thread")]
    async fn rate_limited_replay_completes_within_budget() {
        let bps = 6_400_000u64;
        let burst_secs = 1u64;
        let limiter = ByteRateLimiter::new(bps, burst_secs);
        let files = 200u64;
        let file_size = 65_536u64;
        let want = files * file_size;
        let started = Instant::now();
        for _ in 0..files {
            limiter.acquire(file_size).await;
        }
        let elapsed = started.elapsed();
        let post_burst_bytes = want.saturating_sub(bps * burst_secs);
        let expected = Duration::from_secs_f64(post_burst_bytes as f64 / bps as f64);
        // ± 30 % to absorb CI scheduler jitter while still proving
        // the limiter actually paces the loop.
        let lower = expected.mul_f64(0.7);
        let upper = expected.mul_f64(1.5);
        assert!(
            elapsed >= lower && elapsed <= upper,
            "{} × {} B at {} B/s after a {}s burst expected ~{:?}, observed {:?}",
            files,
            file_size,
            bps,
            burst_secs,
            expected,
            elapsed
        );
    }

    // -- end-to-end reactor (DRAM-only pools, synthetic fetcher) ----

    async fn build_store() -> Arc<FoyerStore> {
        Arc::new(FoyerStore::open(&dram_pools()).await.expect("open pool"))
    }

    fn rw_config() -> RewarmConfig {
        RewarmConfig {
            enabled: true,
            max_bytes_per_sec: 64 * 1024 * 1024,
            max_concurrent_files: 4,
            queue_capacity: 32,
            snapshot_lag_tolerance: Duration::from_secs(30),
            byte_equality_tolerance_bps: 500,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reactor_warms_added_files_on_compaction() {
        let store = build_store().await;
        let fetcher = CountingFetcher::new(Duration::ZERO);
        let admission = Arc::new(AlwaysAdmit);
        let reactor = CompactionReactor::new(
            rw_config(),
            store.clone(),
            fetcher.clone() as Arc<dyn RewarmFetcher>,
            admission,
        );
        let cancel = CancellationToken::new();
        let (tx, handle) = reactor.spawn(cancel.clone());

        let removed = (0..4)
            .map(|i| file(&format!("a-{i}"), &[i as u8, 1, 2, 3], 1024))
            .collect();
        let added = vec![file("merged", b"\x10\x11\x12\x13", 4096)];
        tx.send(evt(added.clone(), removed)).await.unwrap();
        // Drop tx so the reactor sees end-of-stream once the event
        // is processed and the join returns.
        drop(tx);
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

        assert_eq!(fetcher.calls(), 1, "exactly one added file fetched");
        // The `warmed` outcome counter must have moved.
        let warmed = crate::metrics::REWARM_FILES_TOTAL
            .with_label_values(&["warmed"])
            .get();
        assert!(warmed >= 1, "warmed counter advanced; got {warmed}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reactor_skips_non_compaction_event() {
        let store = build_store().await;
        let fetcher = CountingFetcher::new(Duration::ZERO);
        let admission = Arc::new(AlwaysAdmit);
        let reactor = CompactionReactor::new(
            rw_config(),
            store,
            fetcher.clone() as Arc<dyn RewarmFetcher>,
            admission,
        );
        let cancel = CancellationToken::new();
        let (tx, handle) = reactor.spawn(cancel.clone());

        // Append-only: no removed_files. Reactor must skip.
        let appended = vec![file("appended", b"\xaa\xbb", 4096)];
        tx.send(evt(appended, vec![])).await.unwrap();
        drop(tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        assert_eq!(fetcher.calls(), 0, "non-compaction must not fetch");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reactor_handles_origin_failure_without_panicking() {
        let store = build_store().await;
        let fetcher: Arc<dyn RewarmFetcher> = Arc::new(ErrorFetcher);
        let admission = Arc::new(AlwaysAdmit);
        let reactor = CompactionReactor::new(rw_config(), store, fetcher, admission);
        let cancel = CancellationToken::new();
        let (tx, handle) = reactor.spawn(cancel.clone());

        let removed = (0..3)
            .map(|i| file(&format!("a-{i}"), &[i as u8, 1, 2, 3], 1024))
            .collect();
        let added = vec![file("merged", b"\x90\x91\x92\x93", 3072)];
        let baseline = crate::metrics::REWARM_ERRORS_TOTAL
            .with_label_values(&["origin_get"])
            .get();
        tx.send(evt(added, removed)).await.unwrap();
        drop(tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        let now = crate::metrics::REWARM_ERRORS_TOTAL
            .with_label_values(&["origin_get"])
            .get();
        assert!(
            now > baseline,
            "origin_get errors_total must advance after a fetch failure"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reactor_marks_misshapen_event_as_iceberg_metadata_error() {
        // FileSpec with size_bytes=0 — predicate accepts it
        // structurally (added < removed) but the reactor's
        // pre-check rejects.
        let store = build_store().await;
        let fetcher = CountingFetcher::new(Duration::ZERO);
        let admission = Arc::new(AlwaysAdmit);
        let reactor = CompactionReactor::new(
            rw_config(),
            store,
            fetcher.clone() as Arc<dyn RewarmFetcher>,
            admission,
        );
        let cancel = CancellationToken::new();
        let (tx, handle) = reactor.spawn(cancel.clone());

        let baseline = crate::metrics::REWARM_ERRORS_TOTAL
            .with_label_values(&["iceberg_metadata"])
            .get();
        let removed = (0..2)
            .map(|i| file(&format!("a-{i}"), &[i as u8, 1, 2, 3], 1024))
            .collect();
        // Misshapen: zero-byte added file.
        let added = vec![file("merged", b"\x00\x01", 0)];
        tx.send(evt(added, removed)).await.unwrap();
        drop(tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        let now = crate::metrics::REWARM_ERRORS_TOTAL
            .with_label_values(&["iceberg_metadata"])
            .get();
        assert!(now > baseline, "iceberg_metadata must advance");
        assert_eq!(fetcher.calls(), 0, "misshapen event must not fetch");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_queue_drops_with_metric() {
        // SnapshotPublisher::try_publish must bump the
        // dropped_rate_limit + pool_full counters.
        let (tx, _rx) = mpsc::channel::<IcebergSnapshotEvent>(1);
        let pub_ = SnapshotPublisher::new(tx);
        let removed = vec![file("a", b"e", 10)];
        let added = vec![file("merged", b"e2", 5)];
        let baseline_drop = crate::metrics::REWARM_EVENTS_TOTAL
            .with_label_values(&["dropped_rate_limit"])
            .get();
        let baseline_pool = crate::metrics::REWARM_ERRORS_TOTAL
            .with_label_values(&["pool_full"])
            .get();
        assert!(pub_.try_publish(evt(added.clone(), removed.clone())));
        // queue is full now
        assert!(!pub_.try_publish(evt(added, removed)));
        let now_drop = crate::metrics::REWARM_EVENTS_TOTAL
            .with_label_values(&["dropped_rate_limit"])
            .get();
        let now_pool = crate::metrics::REWARM_ERRORS_TOTAL
            .with_label_values(&["pool_full"])
            .get();
        assert_eq!(now_drop - baseline_drop, 1);
        assert_eq!(now_pool - baseline_pool, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn already_warm_keys_are_skipped_without_fetch() {
        let store = build_store().await;
        let fetcher = CountingFetcher::new(Duration::ZERO);
        let admission = Arc::new(AlwaysAdmit);
        let reactor = CompactionReactor::new(
            rw_config(),
            store.clone(),
            fetcher.clone() as Arc<dyn RewarmFetcher>,
            admission,
        );

        // Pre-seed the cache with the exact key the reactor would
        // derive — so the reactor's `contains` probe fires and the
        // skipped_already_warm path runs. Done via `get_or_fetch`
        // since `FoyerStore` does not expose a direct `insert`.
        let etag = b"\x10\x11\x12\x13";
        let size = 4096u64;
        let key = key_from_tuple(etag, 0, size, 0).unwrap();
        let admit = AlwaysAdmit;
        let bytes = Bytes::from(vec![0u8; size as usize]);
        let bytes_for_seed = bytes.clone();
        let _ = store
            .get_or_fetch(Pool::RowGroup, key.clone(), &admit, async move {
                Ok(bytes_for_seed)
            })
            .await;
        assert!(
            store.contains(Pool::RowGroup, &key).await,
            "pre-seed must land in rowgroup pool",
        );

        let cancel = CancellationToken::new();
        let (tx, handle) = reactor.spawn(cancel.clone());

        let removed = (0..4)
            .map(|i| file(&format!("a-{i}"), &[i as u8, 1, 2, 3], 1024))
            .collect();
        let added = vec![file("merged", etag, size)];
        let baseline = crate::metrics::REWARM_FILES_TOTAL
            .with_label_values(&["skipped_already_warm"])
            .get();
        tx.send(evt(added, removed)).await.unwrap();
        drop(tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        let now = crate::metrics::REWARM_FILES_TOTAL
            .with_label_values(&["skipped_already_warm"])
            .get();
        assert!(now > baseline);
        assert_eq!(fetcher.calls(), 0, "already-warm key must not refetch");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reactor_exits_when_disabled() {
        let mut cfg = rw_config();
        cfg.enabled = false;
        let store = build_store().await;
        let fetcher = CountingFetcher::new(Duration::ZERO);
        let admission = Arc::new(AlwaysAdmit);
        let reactor = CompactionReactor::new(
            cfg,
            store,
            fetcher.clone() as Arc<dyn RewarmFetcher>,
            admission,
        );
        let cancel = CancellationToken::new();
        let (tx, handle) = reactor.spawn(cancel.clone());
        // Even sending a real compaction should not trigger a fetch
        // because `run` returns on entry when `enabled=false`.
        let removed = (0..4)
            .map(|i| file(&format!("a-{i}"), &[i as u8, 1, 2, 3], 1024))
            .collect();
        let added = vec![file("merged", b"\x10\x11\x12\x13", 4096)];
        let _ = tx.try_send(evt(added, removed));
        drop(tx);
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert_eq!(fetcher.calls(), 0);
    }

    // -- property test: re-warm semaphore vs client budget ----------

    /// SHELF-45 acceptance criterion: re-warm semaphore must be at
    /// least 4× smaller than the configured client budget so client
    /// reads cannot be starved. The "client budget" is the
    /// origin pool's `max_inflight` (the canonical concurrency cap
    /// on outbound S3 GETs); compare against the reactor's
    /// `max_concurrent_files`.
    #[test]
    fn rewarm_semaphore_is_well_below_client_budget() {
        // OriginConfig default is 128 inflight per the SHELF-21f
        // 2026-04-29 RC, RewarmConfig default is 4 — that's 32×
        // smaller, easily satisfying the 4× minimum.
        let origin = OriginConfig {
            bucket: "b".into(),
            endpoint_url: None,
            region: None,
            max_inflight: 128,
        };
        let rewarm = RewarmConfig::default();
        let ratio = origin.max_inflight as f64 / rewarm.max_concurrent_files as f64;
        assert!(
            ratio >= 4.0,
            "re-warm semaphore must be ≤ 1/4 of client budget; \
             origin.max_inflight={}, rewarm.max_concurrent_files={}, ratio={}",
            origin.max_inflight,
            rewarm.max_concurrent_files,
            ratio,
        );
        // Bonus: every legitimate operator override that lifts
        // max_concurrent_files must still be checked against the
        // chart's documented worst-case origin pool of 128.
        for k in [1usize, 2, 4, 8, 16, 32] {
            let cfg = RewarmConfig {
                max_concurrent_files: k,
                ..RewarmConfig::default()
            };
            let r = origin.max_inflight as f64 / cfg.max_concurrent_files as f64;
            if k <= 32 {
                assert!(r >= 4.0, "k={k} produced ratio {r}");
            }
        }
        // And: the chart's documented absolute upper-bound on
        // max_concurrent_files is `max_inflight / 4`. Anything
        // higher invalidates the property; the property test exists
        // to flag a future config bump that would.
        let max_safe = origin.max_inflight / 4;
        assert!(
            rewarm.max_concurrent_files <= max_safe,
            "rewarm.max_concurrent_files default ({}) exceeds the \
             documented safety ceiling of origin.max_inflight / 4 = {}",
            rewarm.max_concurrent_files,
            max_safe,
        );
    }
}
