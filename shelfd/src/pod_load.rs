//! **K2 (rc.8)** — HRW-skew-aware autoscaler integration.
//!
//! ## Why
//!
//! HRW hashing pins each (bucket, key, etag) tuple to a single shelf
//! pod (ADR-0002). When a hot key family lands on the same primary
//! pod (e.g. all `mbuser_admin` Metabase queries hashing to a single
//! ETag prefix), that pod absorbs the read fan-out while peers stay
//! near-idle. Autoscaling on aggregate pool QPS hides the imbalance
//! because the cluster's average looks healthy even when one pod is
//! pegged. The 4-hour bench in
//! `benchmarks/results/2026-05-01/4hr/COMPREHENSIVE-RESULTS.md`
//! caught this directly: shelf-bench-{0,1,2} took ~14× more queries
//! than shelf-bench-{3,4,5} which joined the pool late.
//!
//! K2 closes the loop with a per-pod load gauge and a cluster-wide
//! skew gauge (max QPS / median QPS) that an HPA / KEDA scaler can
//! target. Skew > 1.5 (150 bps) → scale up; skew ≈ 1.0 → balanced
//! and the existing aggregate-QPS scale-down rule wins.
//!
//! ## Hot-path cost
//!
//! [`PodLoadAggregator::record_request`] is a single
//! [`AtomicU64::fetch_add`] (lock-free, `Relaxed`). The rolling
//! window's `VecDeque` snapshot is updated only by the background
//! [`PodLoadAggregator::run`] loop on the configured cadence
//! (default 30 s) — the hot path NEVER touches that lock.
//!
//! ## Integer basis-points convention
//!
//! `shelf_pod_load_skew_ratio_bps` is an `IntGauge` in basis points
//! (`× 100`) so chart values overlays don't trip the YAML
//! scientific-notation Helm landmine that bit
//! `shelf_rolling_hit_ratio_bps` historically. Operators read
//! `value / 100.0` for the human-facing ratio (`100 = 1.0` =
//! perfectly balanced; `200 = 2.0×` skewed; `>= 150` = warning).
//!
//! See `agents/out/adr/0042-rc8-shelf-pool-rightsizing.md` for the
//! shared K1 (NVMe sizing) + K2 (skew autoscaler) rationale.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::router::Router;

/// Default master switch — on. The hot-path cost is one atomic
/// `fetch_add` per accepted request (Relaxed); the background loop
/// runs at 30 s cadence by default. Operators flip
/// `cache.podLoad.enabled = false` only as a config-only revert.
fn default_enabled() -> bool {
    true
}

/// 60-second rolling window matches the
/// `shelf_rolling_hit_ratio_bps` precedent — long enough to smooth
/// out per-query spikes, short enough that an autoscaler reacts
/// to a real shift within ~ 2 minutes.
fn default_window() -> Duration {
    Duration::from_secs(60)
}

/// 30-second aggregator cadence: with an HRW-aware HPA latency
/// budget of ~ 60 s, two aggregations land inside one HPA cycle.
fn default_aggregation_interval() -> Duration {
    Duration::from_secs(30)
}

/// 2-second per-peer probe deadline. Stays well below
/// `MembershipConfig::stats_timeout` defaults (1 s) — but K2
/// includes the response body decode in the budget rather than
/// just the headers, so the cap is doubled.
fn default_probe_timeout() -> Duration {
    Duration::from_secs(2)
}

/// Operator-tunable knobs for [`PodLoadAggregator`]. Default-on with
/// minimal overhead; the only reason to flip `enabled` to `false`
/// is a config-only revert during incident triage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PodLoadConfig {
    /// Master switch. Default `true`. The hot-path cost when
    /// enabled is a single atomic increment per accepted s3-shim
    /// request; with the gate off the call is a check-then-return.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Trailing window over which `shelf_pod_load_qps` is computed.
    /// 60 s by default. Smaller windows react faster but amplify
    /// per-query spikes; larger windows mask real traffic shifts.
    #[serde(default = "default_window", with = "humantime_serde")]
    pub window: Duration,

    /// How often the aggregator wakes, snapshots `cumulative` into
    /// the rolling window, probes peers' `/stats?include=pod_load`,
    /// and republishes `shelf_pod_load_qps` +
    /// `shelf_pod_load_skew_ratio_bps`. Default 30 s.
    #[serde(default = "default_aggregation_interval", with = "humantime_serde")]
    pub aggregation_interval: Duration,

    /// Hard wall-clock deadline for one peer's `/stats` probe
    /// (HTTP request + body decode). A peer that misses the
    /// deadline is dropped from the round AND increments
    /// `shelf_pod_load_probe_errors_total`. Default 2 s.
    #[serde(default = "default_probe_timeout", with = "humantime_serde")]
    pub probe_timeout: Duration,
}

impl Default for PodLoadConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            window: default_window(),
            aggregation_interval: default_aggregation_interval(),
            probe_timeout: default_probe_timeout(),
        }
    }
}

/// Trait used by the aggregator to fetch a peer's reported QPS.
///
/// Extracted so unit tests can inject a deterministic stub instead
/// of standing up a full HTTP server. Production wiring lives in
/// [`HttpPeerLoadProber`].
pub trait PeerLoadProber: Send + Sync + std::fmt::Debug + 'static {
    /// Probe one peer's `/stats?include=pod_load` and return its
    /// reported QPS, or `Err(_)` on any failure (timeout, non-2xx,
    /// JSON decode, missing field). Implementors enforce their own
    /// timeout — the aggregator does NOT wrap the call in
    /// `tokio::time::timeout`.
    fn probe_one<'a>(
        &'a self,
        endpoint: SocketAddr,
        timeout: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, String>> + Send + 'a>>;
}

/// Production-side [`PeerLoadProber`] backed by `reqwest`.
#[derive(Debug)]
pub struct HttpPeerLoadProber {
    http: reqwest::Client,
}

impl HttpPeerLoadProber {
    pub fn new(http: reqwest::Client) -> Self {
        Self { http }
    }
}

impl PeerLoadProber for HttpPeerLoadProber {
    fn probe_one<'a>(
        &'a self,
        endpoint: SocketAddr,
        timeout: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, String>> + Send + 'a>> {
        Box::pin(async move {
            // `/stats?include=pod_load` enables the optional
            // `pod_load` block in the JSON payload. Existing
            // consumers that don't pass the param see the
            // pre-K2 `Stats` shape unchanged.
            let url = format!("http://{endpoint}/stats?include=pod_load");
            let send = self.http.get(&url).send();
            let resp = tokio::time::timeout(timeout, send)
                .await
                .map_err(|_| format!("probe timeout: {url}"))?
                .map_err(|e| format!("probe http: {e}"))?;
            if !resp.status().is_success() {
                return Err(format!("probe non-2xx: {url} status={}", resp.status()));
            }
            let body = tokio::time::timeout(timeout, resp.json::<crate::control::Stats>())
                .await
                .map_err(|_| format!("body timeout: {url}"))?
                .map_err(|e| format!("body decode: {e}"))?;
            match body.pod_load {
                Some(pl) => Ok(pl.qps),
                None => Err(format!("peer omitted pod_load block: {url}")),
            }
        })
    }
}

/// Snapshot ring of `(Instant, cumulative_count)` samples. The
/// aggregator pushes one sample per `aggregation_interval`; samples
/// older than `window` are evicted. Records-in-window =
/// `latest_cumulative - oldest_cumulative_within_window`.
///
/// Distinct from a sample-per-record design (which would force a
/// lock on the hot path); this carries one snapshot per aggregation
/// tick so the hot path stays lock-free atop the aggregator's
/// `cumulative` `AtomicU64`.
#[derive(Debug)]
pub(crate) struct RollingCounter {
    samples: VecDeque<(Instant, u64)>,
    window: Duration,
}

impl RollingCounter {
    pub(crate) fn new(window: Duration) -> Self {
        Self {
            samples: VecDeque::new(),
            window,
        }
    }

    /// Append a `(now, cumulative)` snapshot and evict any sample
    /// older than `window` from the front.
    pub(crate) fn record_at(&mut self, now: Instant, cumulative: u64) {
        // Defensive: `Instant` arithmetic saturates in stable Rust
        // 1.95, but `checked_sub` keeps the test API explicit and
        // the future Rust behaviour stable.
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        self.samples.push_back((now, cumulative));
        while let Some(&(t, _)) = self.samples.front() {
            if t < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Snapshot count over the trailing `window`. Returns `0` when
    /// fewer than two samples are present (the rate is undefined
    /// before the window has filled).
    pub(crate) fn count(&self) -> u64 {
        match (self.samples.front(), self.samples.back()) {
            (Some((_, oldest)), Some((_, newest))) if self.samples.len() >= 2 => {
                newest.saturating_sub(*oldest)
            }
            _ => 0,
        }
    }

    /// Number of snapshot samples currently retained. Used by tests
    /// to assert window-based eviction.
    #[cfg(test)]
    pub(crate) fn samples_len(&self) -> usize {
        self.samples.len()
    }
}

/// Outcome record returned by [`PodLoadAggregator::aggregate_once`].
/// Public so integration tests can assert on a single tick without
/// scraping `/metrics`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregationOutcome {
    pub local_qps: u64,
    pub skew_ratio_bps: u64,
    pub probe_errors: u64,
    pub peers_probed: usize,
}

/// **K2** — per-pod load aggregator + cluster-wide skew gauge.
///
/// Wired into the s3-shim accept handler via
/// [`PodLoadAggregator::record_request`] (single atomic
/// fetch-add per accepted GET / HEAD), and into `main` via
/// [`PodLoadAggregator::run`] (background tick at
/// `cfg.aggregation_interval`).
#[derive(Debug)]
pub struct PodLoadAggregator {
    cfg: PodLoadConfig,
    /// Stable identity of this pod, used to skip self in peer probes.
    self_id: String,
    /// Live HRW ring view; the aggregator filters its own row out
    /// and probes the rest.
    router: Arc<Router>,
    /// Stats port the peers expose `/stats` on (typically 9090).
    stats_port: u16,
    /// Pluggable peer prober. Production = [`HttpPeerLoadProber`];
    /// tests can pass any [`PeerLoadProber`] impl.
    prober: Arc<dyn PeerLoadProber>,
    /// Hot-path lock-free request counter. Bumped by
    /// [`Self::record_request`] under `Ordering::Relaxed`.
    cumulative: AtomicU64,
    /// Rolling snapshot ring used to convert `cumulative` into a
    /// QPS gauge. Touched only by the background loop / tests.
    rolling: Mutex<RollingCounter>,
    /// Number of times [`Self::aggregate_once`] has executed end
    /// to end. Exposed for the
    /// `aggregation_interval_respected` test; also useful for
    /// debugging from `shelfctl`.
    aggregations_total: AtomicU64,
}

impl PodLoadAggregator {
    /// Production constructor.
    pub fn new(
        cfg: PodLoadConfig,
        self_id: impl Into<String>,
        router: Arc<Router>,
        stats_port: u16,
        http: reqwest::Client,
    ) -> Self {
        Self::with_prober(
            cfg,
            self_id,
            router,
            stats_port,
            Arc::new(HttpPeerLoadProber::new(http)),
        )
    }

    /// Test-friendly constructor: inject any [`PeerLoadProber`].
    pub fn with_prober(
        cfg: PodLoadConfig,
        self_id: impl Into<String>,
        router: Arc<Router>,
        stats_port: u16,
        prober: Arc<dyn PeerLoadProber>,
    ) -> Self {
        let window = cfg.window;
        Self {
            cfg,
            self_id: self_id.into(),
            router,
            stats_port,
            prober,
            cumulative: AtomicU64::new(0),
            rolling: Mutex::new(RollingCounter::new(window)),
            aggregations_total: AtomicU64::new(0),
        }
    }

    /// **Hot path.** One atomic `fetch_add(Relaxed)` when enabled,
    /// a check-then-return when disabled. Safe to call from any
    /// task / thread without coordination.
    pub fn record_request(&self) {
        if !self.cfg.enabled {
            return;
        }
        self.cumulative.fetch_add(1, Ordering::Relaxed);
    }

    /// Cumulative request count since boot. Test helper; the
    /// hot-path consumer is `shelf_pod_load_qps`, not this raw
    /// counter.
    pub fn cumulative_requests(&self) -> u64 {
        self.cumulative.load(Ordering::Relaxed)
    }

    /// Number of successful aggregation rounds executed by
    /// [`Self::run`]. Exposed for the
    /// `aggregation_interval_respected` test and for `shelfctl`.
    pub fn aggregations_total(&self) -> u64 {
        self.aggregations_total.load(Ordering::Relaxed)
    }

    /// Snapshot the current rolling-window QPS without running a
    /// peer probe. Used by the `/stats?include=pod_load` handler
    /// so the response reflects the same value the metrics gauge
    /// publishes, without a synchronous peer probe.
    pub fn current_local_qps(&self) -> u64 {
        let rc = self.rolling.lock();
        let window_secs = self.cfg.window.as_secs();
        rc.count().checked_div(window_secs).unwrap_or(0)
    }

    /// Window the QPS gauge is averaged over, in seconds. Carried
    /// alongside the QPS value on the `/stats?include=pod_load`
    /// payload so an aggregator on a different cadence still
    /// renders comparable numbers.
    pub fn window_secs(&self) -> u64 {
        self.cfg.window.as_secs()
    }

    /// One end-to-end aggregation tick:
    ///
    /// 1. Snapshot `cumulative` into the rolling window.
    /// 2. Compute local QPS = records-in-window / window-secs.
    /// 3. Update [`crate::metrics::POD_LOAD_QPS`].
    /// 4. Probe each peer's `/stats?include=pod_load` with the
    ///    configured timeout; collect successful QPS values.
    /// 5. Compute skew = max(qps) / median(qps) across
    ///    `{local} ∪ peers`, expressed in basis points.
    /// 6. Update [`crate::metrics::POD_LOAD_SKEW_RATIO_BPS`] and
    ///    bump [`crate::metrics::POD_LOAD_PROBE_ERRORS_TOTAL`] for
    ///    every failed peer probe.
    pub async fn aggregate_once(&self) -> AggregationOutcome {
        let now = Instant::now();
        let cumulative = self.cumulative.load(Ordering::Relaxed);
        let local_qps = {
            let mut rc = self.rolling.lock();
            rc.record_at(now, cumulative);
            let window_secs = self.cfg.window.as_secs();
            // `checked_div` short-circuits the misconfig path
            // (`window: 0s`) without an explicit zero-comparison
            // — the gauge stays at 0 and the dashboards see the
            // misconfiguration instead of a panic.
            rc.count().checked_div(window_secs).unwrap_or(0)
        };
        crate::metrics::POD_LOAD_QPS.set(local_qps as i64);

        let peer_endpoints = self.peer_endpoints();
        let peers_probed = peer_endpoints.len();
        let mut probe_errors: u64 = 0;
        let mut peer_qps_values: Vec<u64> = Vec::with_capacity(peer_endpoints.len());
        if !peer_endpoints.is_empty() {
            let probes = peer_endpoints.iter().map(|ep| {
                let prober = Arc::clone(&self.prober);
                let timeout = self.cfg.probe_timeout;
                let endpoint = *ep;
                async move { prober.probe_one(endpoint, timeout).await }
            });
            for res in join_all(probes).await {
                match res {
                    Ok(qps) => peer_qps_values.push(qps),
                    Err(_) => {
                        probe_errors = probe_errors.saturating_add(1);
                    }
                }
            }
        }
        if probe_errors > 0 {
            crate::metrics::POD_LOAD_PROBE_ERRORS_TOTAL.inc_by(probe_errors);
        }

        let mut all_qps = Vec::with_capacity(peer_qps_values.len() + 1);
        all_qps.push(local_qps);
        all_qps.extend(peer_qps_values);
        let skew_ratio_bps = compute_skew_bps(&all_qps);
        crate::metrics::POD_LOAD_SKEW_RATIO_BPS.set(skew_ratio_bps as i64);

        self.aggregations_total.fetch_add(1, Ordering::Relaxed);
        AggregationOutcome {
            local_qps,
            skew_ratio_bps,
            probe_errors,
            peers_probed,
        }
    }

    /// Background loop. No-op when `cfg.enabled = false`. Exits
    /// cleanly on `shutdown.cancelled()`.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        if !self.cfg.enabled {
            tracing::debug!(
                target: "shelfd::pod_load",
                "PodLoadAggregator disabled (cache.podLoad.enabled=false); loop exiting"
            );
            return;
        }
        tracing::info!(
            target: "shelfd::pod_load",
            self_id = %self.self_id,
            window = ?self.cfg.window,
            interval = ?self.cfg.aggregation_interval,
            probe_timeout = ?self.cfg.probe_timeout,
            "K2 pod-load aggregator running"
        );
        let mut tick = tokio::time::interval(self.cfg.aggregation_interval);
        // First `interval.tick()` always returns immediately; do an
        // initial aggregation outside the select so the gauges are
        // populated within the first cadence rather than after one
        // full sleep.
        tick.tick().await;
        let _ = self.aggregate_once().await;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::info!(
                        target: "shelfd::pod_load",
                        self_id = %self.self_id,
                        "shutdown observed; aggregator loop exiting"
                    );
                    return;
                }
                _ = tick.tick() => {
                    let _ = self.aggregate_once().await;
                }
            }
        }
    }

    /// Resolve peer endpoints `<ip>:<stats_port>` from the live
    /// ring view, dropping our own pod row.
    fn peer_endpoints(&self) -> Vec<SocketAddr> {
        let view = self.router.view();
        let mut out = Vec::with_capacity(view.members().len());
        for m in view.members() {
            if m.id == self.self_id {
                continue;
            }
            // `Member::endpoint` is `<ip>:<data_port>`; rewrite
            // the port to `stats_port`.
            if let Some((host, _)) = m.endpoint.rsplit_once(':') {
                let candidate = format!("{host}:{}", self.stats_port);
                if let Ok(sa) = SocketAddr::from_str(&candidate) {
                    out.push(sa);
                }
            }
        }
        out
    }
}

/// Compute the K2 skew ratio in basis points (× 100) from a slice
/// of per-pod QPS values.
///
/// Definition (matches the docstring on
/// [`crate::metrics::POD_LOAD_SKEW_RATIO_BPS`]):
///
/// * `skew = max(qps) / median(qps)` over the cluster
/// * Reported in **basis points** (multiplied by 100); operators
///   read `value / 100.0` for the human-facing ratio.
/// * `100 bps = 1.0` (perfect balance); `>= 150 bps` (1.5×) is
///   the K2 scale-up threshold.
///
/// "Median" uses the lower-median convention `qps[(len-1)/2]`
/// after ascending sort. For two pods this returns the smaller
/// value, which makes a `(80, 40)` split read as `200 bps` (a 2×
/// imbalance) — the operationally correct signal that one pod
/// carries twice the other's load. A linear-interpolation median
/// would report `133 bps` and underplay the imbalance.
///
/// Edge cases:
/// * empty slice → `100 bps` (cannot say anything; report perfect
///   balance so the autoscaler never reacts to noise on a fresh
///   cluster).
/// * any zero in the input → `100 bps` if the median is 0 (avoids
///   divide-by-zero; same "no signal" semantics).
pub fn compute_skew_bps(qps_values: &[u64]) -> u64 {
    if qps_values.is_empty() {
        return 100;
    }
    let mut sorted: Vec<u64> = qps_values.to_vec();
    sorted.sort_unstable();
    let max = *sorted.last().unwrap();
    let median = sorted[(sorted.len() - 1) / 2];
    if median == 0 {
        return 100;
    }
    // `max / median` truncates; multiply first to retain bps
    // precision. `max <= u64::MAX / 100` always holds for any
    // realistic QPS (cap is ~ 1.8 × 10^17 ops/sec).
    max.saturating_mul(100) / median
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// Stub prober that returns canned `Result`s in order, cycling
    /// when the script is exhausted. Counts every probe attempt so
    /// the `aggregation_interval_respected` test can assert the
    /// background loop respects its cadence.
    #[derive(Debug)]
    struct ScriptedProber {
        script: parking_lot::Mutex<Vec<Result<u64, String>>>,
        attempts: AtomicUsize,
        delay: Duration,
    }

    impl ScriptedProber {
        fn new(script: Vec<Result<u64, String>>) -> Self {
            Self {
                script: parking_lot::Mutex::new(script),
                attempts: AtomicUsize::new(0),
                delay: Duration::from_millis(0),
            }
        }
    }

    impl PeerLoadProber for ScriptedProber {
        fn probe_one<'a>(
            &'a self,
            _endpoint: SocketAddr,
            _timeout: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, String>> + Send + 'a>>
        {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            let next = {
                let mut s = self.script.lock();
                if s.is_empty() {
                    Err("script exhausted".to_string())
                } else if s.len() == 1 {
                    s[0].clone()
                } else {
                    s.remove(0)
                }
            };
            let delay = self.delay;
            Box::pin(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                next
            })
        }
    }

    fn ring_with(members: &[(&str, &str)]) -> Arc<Router> {
        let r = Arc::new(Router::new());
        let m: Vec<crate::router::Member> = members
            .iter()
            .map(|(id, ip)| crate::router::Member {
                id: (*id).to_string(),
                endpoint: format!("{ip}:9092"),
                weight: 1,
            })
            .collect();
        r.update(m);
        r
    }

    fn cfg() -> PodLoadConfig {
        PodLoadConfig {
            enabled: true,
            window: Duration::from_secs(60),
            aggregation_interval: Duration::from_millis(40),
            probe_timeout: Duration::from_millis(50),
        }
    }

    #[tokio::test]
    async fn disabled_records_no_op() {
        let mut c = cfg();
        c.enabled = false;
        let agg = PodLoadAggregator::with_prober(
            c,
            "shelf-0",
            ring_with(&[("shelf-0", "10.0.0.1")]),
            9090,
            Arc::new(ScriptedProber::new(vec![Ok(0)])),
        );
        for _ in 0..1_000 {
            agg.record_request();
        }
        assert_eq!(agg.cumulative_requests(), 0, "disabled hot path must no-op");

        // run() must also no-op (return immediately) with disabled cfg.
        let agg = Arc::new(agg);
        let token = CancellationToken::new();
        let started = Instant::now();
        Arc::clone(&agg).run(token.clone()).await;
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "disabled run() must return promptly"
        );
    }

    #[test]
    fn rolling_window_evicts_old_samples() {
        let window = Duration::from_secs(60);
        let mut rc = RollingCounter::new(window);
        let t0 = Instant::now();
        // 10 samples 9 s apart over 81 s; window = 60 s ⇒ keep
        // samples whose `t >= t_last - 60s = t0 + 21s`.
        // Samples land at t0+0,9,18,27,36,45,54,63,72,81 ⇒
        // those at t0 + 27..81 stay (7 samples).
        for i in 0..10 {
            rc.record_at(t0 + Duration::from_secs(i as u64 * 9), i as u64 * 100);
        }
        assert_eq!(rc.samples_len(), 7, "expected 7 samples in window");
        // count() = newest_cumulative - oldest_in_window
        // Index of first kept sample = 3 (0..3 evicted), value=300; last value = 900.
        assert_eq!(rc.count(), 600);
    }

    #[test]
    fn rolling_window_count_undefined_with_one_sample() {
        let mut rc = RollingCounter::new(Duration::from_secs(60));
        rc.record_at(Instant::now(), 42);
        assert_eq!(
            rc.count(),
            0,
            "single-sample window has no defined rate yet"
        );
    }

    #[test]
    fn single_pod_skew_is_one_bps_100() {
        // 1 pod, qps=42. Max/median = 42/42 = 1.0 ⇒ 100 bps.
        assert_eq!(compute_skew_bps(&[42]), 100);
    }

    #[test]
    fn two_pods_balanced_skew_is_100() {
        assert_eq!(compute_skew_bps(&[42, 42]), 100);
    }

    #[test]
    fn two_pods_skewed_2_to_1_skew_is_200() {
        // pod-A qps=80, pod-B qps=40 ⇒ skew = max/median.
        // sorted = [40, 80]; lower-median = 40; 80/40 = 2.0 ⇒ 200 bps.
        assert_eq!(compute_skew_bps(&[80, 40]), 200);
        // Order independence:
        assert_eq!(compute_skew_bps(&[40, 80]), 200);
    }

    #[test]
    fn three_pods_two_hot_one_cold_uses_lower_median() {
        // sorted = [10, 80, 80]; lower-median = qps[(3-1)/2] = qps[1] = 80;
        // skew = 80/80 = 1.0 ⇒ 100 bps. The cold pod is an outlier the
        // median must absorb — that's the documented behaviour.
        assert_eq!(compute_skew_bps(&[80, 80, 10]), 100);
    }

    #[test]
    fn empty_input_reports_balanced() {
        assert_eq!(compute_skew_bps(&[]), 100);
    }

    #[test]
    fn zero_median_reports_balanced() {
        // 3 pods all at zero ⇒ median = 0 ⇒ avoid /0, return balanced.
        assert_eq!(compute_skew_bps(&[0, 0, 0]), 100);
        // Two pods, one zero: sorted [0, 100]; lower-median = 0 ⇒
        // signal is undefined. Reporting balanced is the safe default
        // (the autoscaler sees nothing actionable).
        assert_eq!(compute_skew_bps(&[100, 0]), 100);
    }

    #[tokio::test]
    async fn peer_probe_timeout_falls_back_to_local() {
        let c = cfg();
        let prober = Arc::new(ScriptedProber::new(vec![Err("timeout".to_string())]));
        let agg = PodLoadAggregator::with_prober(
            c,
            "shelf-0",
            ring_with(&[("shelf-0", "10.0.0.1"), ("shelf-1", "10.0.0.2")]),
            9090,
            Arc::clone(&prober) as Arc<dyn PeerLoadProber>,
        );
        for _ in 0..120 {
            agg.record_request();
        }

        let before = crate::metrics::POD_LOAD_PROBE_ERRORS_TOTAL.get();
        let outcome = agg.aggregate_once().await;
        let after = crate::metrics::POD_LOAD_PROBE_ERRORS_TOTAL.get();

        assert_eq!(outcome.peers_probed, 1, "one peer in ring after self drop");
        assert_eq!(outcome.probe_errors, 1, "probe scripted to fail");
        assert!(
            after >= before + 1,
            "POD_LOAD_PROBE_ERRORS_TOTAL must tick on probe failure"
        );
        // Local-only fallback ⇒ skew computed from {local_qps} alone
        // ⇒ always 100 bps regardless of local rate.
        assert_eq!(
            outcome.skew_ratio_bps, 100,
            "local-only fallback ⇒ balanced (single-element input)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_record_request_safe() {
        let agg = Arc::new(PodLoadAggregator::with_prober(
            cfg(),
            "shelf-0",
            ring_with(&[("shelf-0", "10.0.0.1")]),
            9090,
            Arc::new(ScriptedProber::new(vec![Ok(0)])),
        ));
        let mut handles = Vec::with_capacity(1_000);
        for _ in 0..1_000 {
            let a = Arc::clone(&agg);
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    a.record_request();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            agg.cumulative_requests(),
            100_000,
            "every record_request() call must be visible after join"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aggregation_interval_respected() {
        let mut c = cfg();
        c.aggregation_interval = Duration::from_millis(100);
        c.probe_timeout = Duration::from_millis(20);
        let prober = Arc::new(ScriptedProber::new(vec![Ok(0)]));
        let agg = Arc::new(PodLoadAggregator::with_prober(
            c.clone(),
            "shelf-0",
            // No peers in ring ⇒ probe path is not exercised; the
            // interval check is purely on aggregation cadence.
            ring_with(&[("shelf-0", "10.0.0.1")]),
            9090,
            Arc::clone(&prober) as Arc<dyn PeerLoadProber>,
        ));
        let token = CancellationToken::new();
        let runner = Arc::clone(&agg);
        let token_run = token.clone();
        let task = tokio::spawn(async move { runner.run(token_run).await });
        // 250 ms wall ÷ 100 ms cadence ≈ 2.5 ticks. Allow [1, 4]
        // to absorb scheduler slop on heavily loaded CI runners.
        tokio::time::sleep(Duration::from_millis(250)).await;
        token.cancel();
        let _ = task.await;
        let runs = agg.aggregations_total();
        assert!(
            (1..=4).contains(&runs),
            "expected 1..=4 aggregations in 250ms at 100ms cadence, got {runs}"
        );
    }
}
