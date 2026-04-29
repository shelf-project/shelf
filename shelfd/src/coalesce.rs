//! SHELF-49 — coalesced range-GET dispatcher for the S3 shim.
//!
//! Trino's native S3 client issues many small, almost-adjacent ranges
//! within the same Parquet file when planning a row-group scan: footer
//! magic + footer struct (`bytes=-N`), then dictionary pages and a
//! handful of column chunks within a few MiB of one another. The
//! shim's stock GET path issues one origin GET per range, which
//! pays the S3 request charge and round-trip latency for every slice.
//!
//! This module batches concurrent ranges that share `(bucket, key,
//! etag)` into a single coalesced origin GET when the gaps and total
//! span fit operator-tunable budgets. Every requester receives
//! byte-identical bytes for its specific span via a per-requester
//! `tokio::sync::oneshot`.
//!
//! ## Scope
//!
//! - Only **closed** ranges (`bytes=<start>-<end>`) participate.
//!   Suffix (`bytes=-N`) and open-ended (`bytes=0-`) ranges require a
//!   `HeadObject` to resolve, which the shim already does upstream of
//!   this dispatcher; those callers route to the existing solo path
//!   verbatim per SHELF-22.
//! - When SHELF-23 peer-fetch wins the race against origin, the
//!   coalescer is **not** entered: peer hits short-circuit before the
//!   leader's origin future fires, so the shim never registers a
//!   waiter for a peer-served slice.
//! - Operates on **wire** bytes only. Compression / decoding (e.g. B1
//!   zstd on the Foyer rowgroup pool) lives downstream of this seam,
//!   so the dispatcher is framing-agnostic.
//!
//! ## Algorithm
//!
//! ```text
//!  request → enter group(bucket,key,etag)
//!         ├─ first arrival? spawn a dispatcher task (tokio::spawn)
//!         │  that sleeps for `wait_window`, then drains every
//!         │  registered waiter under lock.
//!         └─ later arrivals: append (offset,length,oneshot::Sender)
//!            to the same group, await the receiver.
//!
//!  dispatcher (drained list, sorted by offset):
//!    greedy-group: extend a running [start,end) while
//!      gap   ≤ max_gap_bytes
//!      span  ≤ max_coalesced_bytes
//!    for each group:
//!      if group.size > 1: one origin GET over [start,end), slice to
//!                          each waiter's (offset,length)
//!      if group.size == 1: solo GET for that exact range
//!    fan errors to every waiter in a failed group; bump the
//!    consecutive-failure counter; trip the circuit if it crosses
//!    `consecutive_failures`.
//! ```
//!
//! ## Failure semantics
//!
//! A coalesced GET that fails fans the error to **every** requester
//! in the group. No requester ever silently gets partial / zero bytes.
//! After `consecutive_failures` coalesced GETs fail in a row, the
//! breaker opens for `cool_off`: every request bypasses the
//! dispatcher and runs as a solo origin GET. The first successful
//! coalesced GET after the cool-off resets the failure counter.
//!
//! ## Interaction with SHELF-23 peer-fetch
//!
//! `peer_or_origin_fetch` short-circuits to **its own** `origin_fut`
//! when the local pod is the HRW primary, when the ring is empty, or
//! when peer-fetch is disabled at runtime. Callers in `s3_shim::
//! handle_get_object` must use the coalescer **inside** the
//! `origin_fut` closure, not around the peer race; that way a peer
//! hit never enters the dispatcher and a peer-miss-but-origin-wins
//! still benefits from coalescing across other in-flight readers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::config::CoalesceConfig;
use crate::metrics::{
    COALESCE_BYTES_SAVED_TOTAL, COALESCE_RANGES_TOTAL, COALESCE_WINDOW_SECONDS,
};

/// Trait for the underlying single-range origin fetcher. The
/// coalescer is generic over this so unit tests can pin a mock with
/// instrumented call counts; in production the implementation is
/// just a thin wrapper over [`crate::origin::Origin::get_range`].
pub trait RangeFetcher: std::fmt::Debug + Send + Sync {
    /// Fetch the exact `[offset, offset + length)` byte range from
    /// origin. The returned `Bytes` MUST be `length` bytes; the
    /// dispatcher slices that buffer for the merged-group path and
    /// will produce wrong results if the fetcher returns a different
    /// length than requested. (Origin S3 always honours an in-range
    /// closed `Range:` request, so this is the existing contract.)
    fn fetch_range<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        offset: u64,
        length: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Bytes>> + Send + 'a>>;
}

/// Adapter for the production [`crate::origin::S3Origin`].
#[derive(Debug)]
pub struct S3OriginFetcher {
    origin: Arc<crate::origin::S3Origin>,
}

impl S3OriginFetcher {
    pub fn new(origin: Arc<crate::origin::S3Origin>) -> Self {
        Self { origin }
    }
}

impl RangeFetcher for S3OriginFetcher {
    fn fetch_range<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        offset: u64,
        length: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Bytes>> + Send + 'a>> {
        let origin = self.origin.clone();
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        Box::pin(async move {
            use crate::origin::Origin;
            origin.get_range(&bucket, &key, offset, length).await
        })
    }
}

/// Per-`(bucket, key, etag)` collecting state.
#[derive(Debug)]
struct GroupState {
    waiters: Vec<Waiter>,
    /// When the leader registered the group. Used to observe the
    /// actual coalescing window via `shelf_coalesce_window_seconds`.
    /// The window includes both the configured wait + the dispatcher
    /// task's spawn latency, which is what an operator wants to see
    /// (it's the wall-clock cost paid by every request).
    started_at: Instant,
}

/// One pending requester.
#[derive(Debug)]
struct Waiter {
    offset: u64,
    length: u64,
    tx: oneshot::Sender<crate::Result<Bytes>>,
}

/// Dispatcher state. Cheap to clone (just an `Arc<...>` of inner
/// state), and intentionally not part of `ServerState` directly so
/// integration tests can stand up a coalescer without a full
/// `ServerState`.
#[derive(Debug)]
pub struct Coalescer {
    cfg: CoalesceConfig,
    fetcher: Arc<dyn RangeFetcher>,
    groups: Mutex<HashMap<GroupKey, GroupState>>,
    breaker: CircuitBreaker,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey {
    bucket: String,
    key: String,
    etag: String,
}

/// Outcome of a single requester from the dispatcher's perspective.
/// Used to label the `shelf_coalesce_ranges_total` counter.
#[derive(Debug, Clone, Copy)]
enum RangeOutcome {
    /// Request was merged into a multi-waiter group.
    Coalesced,
    /// Request ran on its own GET inside the dispatcher (lone in
    /// group, or coalescing disabled / circuit open at fast-path).
    Solo,
    /// Coalescing was disabled by config; request bypassed the
    /// dispatcher entirely.
    Disabled,
    /// Circuit breaker was open; request bypassed the dispatcher.
    CircuitOpen,
}

impl RangeOutcome {
    fn label(self) -> &'static str {
        match self {
            RangeOutcome::Coalesced => "coalesced",
            RangeOutcome::Solo => "solo",
            RangeOutcome::Disabled => "disabled",
            RangeOutcome::CircuitOpen => "circuit_open",
        }
    }
}

impl Coalescer {
    /// Construct a coalescer wrapping the given fetcher. The config
    /// is captured at construction time; runtime overrides go
    /// through a fresh `Coalescer` (typical lifetime is the daemon
    /// process — operators flip it via Helm + helm upgrade).
    pub fn new(cfg: CoalesceConfig, fetcher: Arc<dyn RangeFetcher>) -> Arc<Self> {
        Arc::new(Self {
            breaker: CircuitBreaker::new(cfg.consecutive_failures, cfg.cool_off),
            cfg,
            fetcher,
            groups: Mutex::new(HashMap::new()),
        })
    }

    /// Fetch a closed `[offset, offset + length)` byte range,
    /// possibly coalesced with other in-flight reads against the
    /// same `(bucket, key, etag)` triple.
    ///
    /// Suffix (`bytes=-N`) and open-ended (`bytes=0-`) ranges MUST
    /// NOT be passed here — they require a `HeadObject` resolution
    /// upstream, and the dispatcher cannot reason about "from the
    /// tail" extents at the time waiters race to register. Callers
    /// in `s3_shim::handle_get_object` enforce this via the
    /// `RangeSpec::Closed` branch; the other two branches go to
    /// `fetcher.fetch_range` directly.
    ///
    /// `etag` is the object's S3 ETag (any string the caller has
    /// already content-addressed on); a non-empty value is required
    /// because the cache layer keys on it. An empty string is
    /// treated as "uncoalescable" and routes solo.
    pub async fn fetch(
        self: &Arc<Self>,
        bucket: &str,
        key: &str,
        etag: &str,
        offset: u64,
        length: u64,
    ) -> crate::Result<Bytes> {
        if !self.cfg.enabled {
            COALESCE_RANGES_TOTAL
                .with_label_values(&[RangeOutcome::Disabled.label()])
                .inc();
            return self.fetcher.fetch_range(bucket, key, offset, length).await;
        }
        if self.breaker.is_open() {
            COALESCE_RANGES_TOTAL
                .with_label_values(&[RangeOutcome::CircuitOpen.label()])
                .inc();
            return self.fetcher.fetch_range(bucket, key, offset, length).await;
        }
        if etag.is_empty() {
            // No content-address available; cannot safely coalesce
            // because two waiters under the same (bucket, key) but
            // different ETags would silently see each other's bytes.
            // Account this as a `solo` outcome so dashboards still
            // see the request was opted out, not just "disabled".
            COALESCE_RANGES_TOTAL
                .with_label_values(&[RangeOutcome::Solo.label()])
                .inc();
            return self.fetcher.fetch_range(bucket, key, offset, length).await;
        }

        let group_key = GroupKey {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            etag: etag.to_owned(),
        };
        let (tx, rx) = oneshot::channel();
        let waiter = Waiter { offset, length, tx };

        let needs_dispatcher = {
            let mut groups = self.groups.lock();
            match groups.get_mut(&group_key) {
                Some(state) => {
                    state.waiters.push(waiter);
                    false
                }
                None => {
                    groups.insert(
                        group_key.clone(),
                        GroupState {
                            waiters: vec![waiter],
                            started_at: Instant::now(),
                        },
                    );
                    true
                }
            }
        };

        if needs_dispatcher {
            // Spawn the drain task so the leader can be cancelled
            // (e.g. by a SHELF-23 peer-hit) without orphaning
            // followers. The dispatcher owns its own `Arc<Self>`
            // and the cloned `GroupKey`; followers wait on their
            // oneshots regardless of leader liveness.
            let coalescer = Arc::clone(self);
            let dispatch_key = group_key.clone();
            let wait = coalescer.cfg.wait_window();
            tokio::spawn(async move {
                tokio::time::sleep(wait).await;
                coalescer.dispatch(dispatch_key).await;
            });
        }

        match rx.await {
            Ok(result) => result,
            Err(_) => Err(crate::Error::Origin(format!(
                "coalesce dispatcher dropped before delivering bytes for \
                 {bucket}/{key}@{offset}-{length}",
            ))),
        }
    }

    /// Take the group's waiters, group them by adjacency under the
    /// configured caps, dispatch each subgroup with a single origin
    /// GET (or a solo GET for size-1 groups), and signal each
    /// waiter via its `oneshot::Sender`.
    async fn dispatch(self: Arc<Self>, group_key: GroupKey) {
        let (mut waiters, started_at) = {
            let mut groups = self.groups.lock();
            match groups.remove(&group_key) {
                Some(state) => (state.waiters, state.started_at),
                // The group was already drained by another path
                // (shouldn't happen with the current design, but be
                // defensive — Foyer's main lesson is that a panic
                // here cannot be debugged from a Prom counter).
                None => return,
            }
        };
        if waiters.is_empty() {
            return;
        }

        COALESCE_WINDOW_SECONDS.observe(started_at.elapsed().as_secs_f64());

        // Sort by start offset so the greedy adjacency walk is
        // deterministic regardless of the (race-determined) waiter
        // arrival order.
        waiters.sort_by_key(|w| w.offset);

        let groups = group_waiters(&waiters, self.cfg.max_gap_bytes, self.cfg.max_coalesced_bytes);

        for sub in groups {
            self.dispatch_subgroup(&group_key, &mut waiters, sub).await;
        }
    }

    /// Issue one origin GET that covers the union of `subgroup`'s
    /// waiters and slice the result to each. On error, fan the
    /// error to every waiter in the subgroup and bump the breaker
    /// counter when the group had ≥ 2 waiters (a solo failure is
    /// indistinguishable from a stock-path failure and would unfairly
    /// trip the breaker).
    async fn dispatch_subgroup(
        self: &Arc<Self>,
        group_key: &GroupKey,
        waiters: &mut [Waiter],
        sub: SubgroupSpan,
    ) {
        let span_start = sub.start;
        let span_end = sub.end_exclusive;
        let span_len = span_end - span_start;
        let coalesced = sub.indices.len() > 1;
        let outcome = if coalesced {
            RangeOutcome::Coalesced
        } else {
            RangeOutcome::Solo
        };

        let result = self
            .fetcher
            .fetch_range(&group_key.bucket, &group_key.key, span_start, span_len)
            .await;

        match result {
            Ok(bytes) => {
                if bytes.len() as u64 != span_len {
                    // Defensive: surface a typed error rather than
                    // hand callers a short / long buffer.
                    let err = crate::Error::Origin(format!(
                        "coalesced GET returned {got} bytes, expected {want} \
                         for {bucket}/{key} bytes={start}-{end}",
                        got = bytes.len(),
                        want = span_len,
                        bucket = group_key.bucket,
                        key = group_key.key,
                        start = span_start,
                        end = span_end - 1,
                    ));
                    fan_err(waiters, &sub, err);
                    if coalesced {
                        self.breaker.record_failure();
                    }
                    bump_outcome(outcome, sub.indices.len());
                    return;
                }
                let mut sum_individual: u64 = 0;
                for &i in &sub.indices {
                    let w = &waiters[i];
                    sum_individual = sum_individual.saturating_add(w.length);
                    let local_off = (w.offset - span_start) as usize;
                    let slice = bytes.slice(local_off..local_off + w.length as usize);
                    let tx = std::mem::replace(
                        &mut waiters[i].tx,
                        oneshot::channel::<crate::Result<Bytes>>().0,
                    );
                    let _ = tx.send(Ok(slice));
                }
                if coalesced {
                    // Spec: `bytes_saved = max(0, sum(coalesced_bytes) -
                    // sum(original_bytes))`. With closed ranges the
                    // diff is non-negative when there are gaps between
                    // adjacent waiters; identical / overlapping
                    // requesters yield 0. We surface the saturating
                    // diff so the metric is monotone and gap-driven.
                    let extra = span_len.saturating_sub(sum_individual);
                    if extra > 0 {
                        COALESCE_BYTES_SAVED_TOTAL.inc_by(extra);
                    }
                    self.breaker.record_success();
                }
                bump_outcome(outcome, sub.indices.len());
            }
            Err(e) => {
                let msg = e.to_string();
                fan_err(
                    waiters,
                    &sub,
                    crate::Error::Origin(format!("coalesced GET failed: {msg}")),
                );
                if coalesced {
                    self.breaker.record_failure();
                }
                bump_outcome(outcome, sub.indices.len());
            }
        }
    }
}

/// Indices into the parent `waiters` vec describing one subgroup.
///
/// `start` and `end_exclusive` are the byte extents the merged GET
/// covers; the union of every `(waiter.offset, waiter.length)` is
/// guaranteed to lie inside `[start, end_exclusive)`.
#[derive(Debug, Clone)]
struct SubgroupSpan {
    indices: Vec<usize>,
    start: u64,
    end_exclusive: u64,
}

/// Greedy adjacency grouping used by the dispatcher.
///
/// Returns one [`SubgroupSpan`] per merge group. `waiters` is assumed
/// sorted ascending by `offset`. Caps that don't fit a candidate
/// extension start a new group rather than splitting the candidate.
///
/// This is split out as a free function (rather than a method) so the
/// unit-test suite can exercise the decision matrix without standing
/// up a full [`Coalescer`].
fn group_waiters(waiters: &[Waiter], max_gap: u64, max_span: u64) -> Vec<SubgroupSpan> {
    let mut out: Vec<SubgroupSpan> = Vec::new();
    for (idx, w) in waiters.iter().enumerate() {
        let w_end = w.offset.saturating_add(w.length);
        if let Some(group) = out.last_mut() {
            // Gap is the bytes between the last covered byte and
            // the candidate's start; saturating because a
            // pathological interleaving could place a candidate
            // strictly *inside* the running span (overlap → 0 gap,
            // always extends).
            let gap = w.offset.saturating_sub(group.end_exclusive);
            let new_end = group.end_exclusive.max(w_end);
            let new_span = new_end - group.start;
            if gap <= max_gap && new_span <= max_span {
                group.indices.push(idx);
                group.end_exclusive = new_end;
                continue;
            }
        }
        out.push(SubgroupSpan {
            indices: vec![idx],
            start: w.offset,
            end_exclusive: w_end,
        });
    }
    out
}

fn bump_outcome(outcome: RangeOutcome, n: usize) {
    if n == 0 {
        return;
    }
    COALESCE_RANGES_TOTAL
        .with_label_values(&[outcome.label()])
        .inc_by(n as u64);
}

fn fan_err(waiters: &mut [Waiter], sub: &SubgroupSpan, err: crate::Error) {
    let msg = err.to_string();
    for &i in &sub.indices {
        let tx = std::mem::replace(
            &mut waiters[i].tx,
            oneshot::channel::<crate::Result<Bytes>>().0,
        );
        // Re-create a typed error per waiter; `crate::Error` is not
        // `Clone` (anyhow + std::io::Error variants), so we rebuild
        // an `Error::Origin` carrying the printed reason.
        let _ = tx.send(Err(crate::Error::Origin(msg.clone())));
    }
}

/// Global circuit breaker. After `consecutive_failures` coalesced
/// GETs have failed back-to-back, the breaker opens and every
/// `Coalescer::fetch` falls through to the solo path until
/// `cool_off` elapses. The first successful coalesced GET after
/// the cool-off window resets the failure counter.
#[derive(Debug)]
struct CircuitBreaker {
    consecutive_failures_threshold: u64,
    cool_off: Duration,
    /// Packed state: low 32 bits are the consecutive-failure counter;
    /// high 32 bits encode an optional `Instant` offset (relative to
    /// `epoch`) of when the breaker re-closes. We use an
    /// `AtomicU64` for the failure count and a separate
    /// `Mutex<Option<Instant>>` for the open-until timestamp because
    /// `Instant` is not `AtomicU64`-shaped on all platforms.
    failures: AtomicU64,
    open_until: Mutex<Option<Instant>>,
}

impl CircuitBreaker {
    fn new(threshold: u64, cool_off: Duration) -> Self {
        Self {
            consecutive_failures_threshold: threshold.max(1),
            cool_off,
            failures: AtomicU64::new(0),
            open_until: Mutex::new(None),
        }
    }

    fn is_open(&self) -> bool {
        let mut guard = self.open_until.lock();
        match *guard {
            Some(deadline) if deadline > Instant::now() => true,
            Some(_) => {
                // Cool-off elapsed; close the breaker. Failures stay
                // at the threshold value until the next coalesced
                // success; any failure that lands while the breaker
                // is closed pre-success will re-open it instantly.
                *guard = None;
                false
            }
            None => false,
        }
    }

    fn record_failure(&self) {
        let prev = self.failures.fetch_add(1, Ordering::Relaxed);
        if prev + 1 >= self.consecutive_failures_threshold {
            let mut guard = self.open_until.lock();
            *guard = Some(Instant::now() + self.cool_off);
        }
    }

    fn record_success(&self) {
        self.failures.store(0, Ordering::Relaxed);
        // Don't proactively close the breaker here — `is_open` does
        // that lazily when the cool-off elapses. Closing on success
        // would let a single noisy success short-circuit the
        // operator-configured cool-off window.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn waiter(offset: u64, length: u64) -> Waiter {
        let (tx, _rx) = oneshot::channel();
        Waiter { offset, length, tx }
    }

    // ---- group_waiters: decision matrix ----

    #[test]
    fn group_waiters_merges_adjacent_within_caps() {
        let waiters = vec![waiter(0, 100), waiter(100, 100), waiter(200, 100)];
        let groups = group_waiters(&waiters, /*max_gap*/ 0, /*max_span*/ 1024);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].indices, vec![0, 1, 2]);
        assert_eq!(groups[0].start, 0);
        assert_eq!(groups[0].end_exclusive, 300);
    }

    #[test]
    fn group_waiters_allows_gap_within_max_gap() {
        // Gap=900 between [0,100) and [1000,1100); cap is 1024 → merges.
        let waiters = vec![waiter(0, 100), waiter(1000, 100)];
        let groups = group_waiters(&waiters, 1024, 1024 * 1024);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].start, 0);
        assert_eq!(groups[0].end_exclusive, 1100);
    }

    #[test]
    fn group_waiters_splits_when_gap_exceeds_cap() {
        // Gap=1024 > max_gap=1023 → must split.
        let waiters = vec![waiter(0, 100), waiter(1124, 100)];
        let groups = group_waiters(&waiters, 1023, 1024 * 1024);
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn group_waiters_splits_when_span_exceeds_cap() {
        // Adjacent (gap=0) but the merged span would exceed max_span.
        let waiters = vec![waiter(0, 800), waiter(800, 800)];
        let groups = group_waiters(&waiters, 1024, /*max_span*/ 1024);
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn group_waiters_handles_overlap() {
        // Overlapping ranges still group; end_exclusive is the max.
        let waiters = vec![waiter(0, 200), waiter(100, 200)];
        let groups = group_waiters(&waiters, 0, 1024);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].start, 0);
        assert_eq!(groups[0].end_exclusive, 300);
    }

    // ---- circuit breaker ----

    #[test]
    fn breaker_opens_after_threshold_consecutive_failures() {
        let b = CircuitBreaker::new(3, Duration::from_secs(60));
        assert!(!b.is_open());
        b.record_failure();
        b.record_failure();
        assert!(!b.is_open());
        b.record_failure();
        assert!(b.is_open());
    }

    #[test]
    fn breaker_resets_on_success() {
        let b = CircuitBreaker::new(3, Duration::from_secs(60));
        b.record_failure();
        b.record_failure();
        b.record_success();
        b.record_failure();
        b.record_failure();
        // Still 2 / 3 consecutive after the reset.
        assert!(!b.is_open());
        b.record_failure();
        assert!(b.is_open());
    }

    #[test]
    fn breaker_closes_after_cool_off() {
        let b = CircuitBreaker::new(1, Duration::from_millis(10));
        b.record_failure();
        assert!(b.is_open());
        std::thread::sleep(Duration::from_millis(15));
        assert!(!b.is_open(), "cool-off elapsed; breaker must close lazily");
    }

    // ---- mock fetcher + dispatcher ----

    #[derive(Debug)]
    struct MockFetcher {
        calls: AtomicUsize,
        // Bytes returned: deterministic based on offset+length.
        fail_after: AtomicUsize,
    }

    impl MockFetcher {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_after: AtomicUsize::new(usize::MAX),
            }
        }
    }

    impl RangeFetcher for MockFetcher {
        fn fetch_range<'a>(
            &'a self,
            _bucket: &'a str,
            _key: &'a str,
            offset: u64,
            length: u64,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::Result<Bytes>> + Send + 'a>,
        > {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let fail_after = self.fail_after.load(Ordering::SeqCst);
            Box::pin(async move {
                if n >= fail_after {
                    return Err(crate::Error::Origin("mock failure".into()));
                }
                // Deterministic content: byte i contains
                // ((offset + i) & 0xff) so slices are verifiable.
                let mut buf = Vec::with_capacity(length as usize);
                for i in 0..length {
                    buf.push(((offset + i) & 0xff) as u8);
                }
                Ok(Bytes::from(buf))
            })
        }
    }

    fn cfg_default() -> CoalesceConfig {
        CoalesceConfig {
            enabled: true,
            max_gap_bytes: 1024 * 1024,
            max_coalesced_bytes: 16 * 1024 * 1024,
            wait_window_micros: 200,
            consecutive_failures: 5,
            cool_off: Duration::from_secs(30),
        }
    }

    #[tokio::test]
    async fn disabled_bypasses_dispatcher() {
        let mock = Arc::new(MockFetcher::new());
        let mut cfg = cfg_default();
        cfg.enabled = false;
        let coalescer = Coalescer::new(cfg, mock.clone());
        let bytes = coalescer.fetch("b", "k", "etag", 0, 16).await.unwrap();
        assert_eq!(bytes.len(), 16);
        assert_eq!(mock.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn three_concurrent_adjacent_ranges_collapse_to_one_get() {
        let mock = Arc::new(MockFetcher::new());
        let coalescer = Coalescer::new(cfg_default(), mock.clone());
        // Three overlapping/adjacent ranges within the same key+etag.
        let f1 = {
            let c = coalescer.clone();
            tokio::spawn(async move { c.fetch("b", "k", "etag", 0, 100).await })
        };
        let f2 = {
            let c = coalescer.clone();
            tokio::spawn(async move { c.fetch("b", "k", "etag", 100, 100).await })
        };
        let f3 = {
            let c = coalescer.clone();
            tokio::spawn(async move { c.fetch("b", "k", "etag", 200, 100).await })
        };
        let r1 = f1.await.unwrap().unwrap();
        let r2 = f2.await.unwrap().unwrap();
        let r3 = f3.await.unwrap().unwrap();
        assert_eq!(r1.len(), 100);
        assert_eq!(r2.len(), 100);
        assert_eq!(r3.len(), 100);
        // Verify byte-content alignment: each slice carries the
        // expected (offset + i) & 0xff pattern.
        for (off, slice) in [(0, &r1), (100, &r2), (200, &r3)] {
            for (i, b) in slice.iter().enumerate() {
                assert_eq!(*b, ((off + i as u64) & 0xff) as u8);
            }
        }
        assert_eq!(
            mock.calls.load(Ordering::SeqCst),
            1,
            "3 adjacent ranges within the same etag must collapse to a single origin GET",
        );
    }

    #[tokio::test]
    async fn distinct_etags_do_not_share_a_group() {
        let mock = Arc::new(MockFetcher::new());
        let coalescer = Coalescer::new(cfg_default(), mock.clone());
        let f1 = {
            let c = coalescer.clone();
            tokio::spawn(async move { c.fetch("b", "k", "etag-a", 0, 100).await })
        };
        let f2 = {
            let c = coalescer.clone();
            tokio::spawn(async move { c.fetch("b", "k", "etag-b", 100, 100).await })
        };
        f1.await.unwrap().unwrap();
        f2.await.unwrap().unwrap();
        assert_eq!(
            mock.calls.load(Ordering::SeqCst),
            2,
            "different etags are different content keys; coalescing across them is unsound",
        );
    }

    #[tokio::test]
    async fn empty_etag_routes_solo() {
        let mock = Arc::new(MockFetcher::new());
        let coalescer = Coalescer::new(cfg_default(), mock.clone());
        let _ = coalescer.fetch("b", "k", "", 0, 100).await.unwrap();
        let _ = coalescer.fetch("b", "k", "", 100, 100).await.unwrap();
        assert_eq!(
            mock.calls.load(Ordering::SeqCst),
            2,
            "empty etag opts out of coalescing",
        );
    }

    #[tokio::test]
    async fn failure_fans_to_every_requester() {
        let mock = Arc::new(MockFetcher::new());
        mock.fail_after.store(0, Ordering::SeqCst);
        let coalescer = Coalescer::new(cfg_default(), mock.clone());
        let f1 = {
            let c = coalescer.clone();
            tokio::spawn(async move { c.fetch("b", "k", "etag", 0, 100).await })
        };
        let f2 = {
            let c = coalescer.clone();
            tokio::spawn(async move { c.fetch("b", "k", "etag", 100, 100).await })
        };
        let r1 = f1.await.unwrap();
        let r2 = f2.await.unwrap();
        assert!(r1.is_err(), "leader's failure must propagate");
        assert!(r2.is_err(), "follower must observe the same failure");
    }

    #[tokio::test]
    async fn circuit_breaker_falls_back_to_solo_when_open() {
        let mock = Arc::new(MockFetcher::new());
        mock.fail_after.store(0, Ordering::SeqCst);
        let mut cfg = cfg_default();
        cfg.consecutive_failures = 1;
        cfg.cool_off = Duration::from_secs(60);
        let coalescer = Coalescer::new(cfg, mock.clone());
        // First failure: trips the breaker.
        let f1 = {
            let c = coalescer.clone();
            tokio::spawn(async move { c.fetch("b", "k", "etag", 0, 100).await })
        };
        let f2 = {
            let c = coalescer.clone();
            tokio::spawn(async move { c.fetch("b", "k", "etag", 100, 100).await })
        };
        let _ = f1.await.unwrap();
        let _ = f2.await.unwrap();
        assert!(coalescer.breaker.is_open());

        // Subsequent fetch must bypass the dispatcher and hit the
        // fetcher directly without buffering / coalescing — the
        // post-trip mock-call count rises 1:1 with each fetch.
        let prev = mock.calls.load(Ordering::SeqCst);
        let r = coalescer.fetch("b", "k", "etag", 0, 100).await;
        assert!(r.is_err()); // mock still failing
        assert_eq!(
            mock.calls.load(Ordering::SeqCst),
            prev + 1,
            "circuit-open path must hit the fetcher exactly once",
        );
    }
}
