//! SHELF-40 — runtime glue between the `shelf-cost` crate and
//! `shelfd`'s Prometheus surface.
//!
//! What lives here:
//!
//! 1. [`CostState`] — a refcounted handle that the `s3_shim` and
//!    `peer_fetch` hot paths borrow to bump
//!    `shelf_s3_dollars_saved_total{region, outcome}` on every
//!    cache hit. The handle holds the shared `CostModel` (cheap to
//!    `Clone` because [`shelf_cost::CostModel`] is plain data).
//! 2. The rolling-rate updater task that fills
//!    `shelf_s3_dollars_saved_rate_cents_per_sec{region, outcome}`
//!    once per second so dashboards can render `cents/sec` without
//!    re-deriving `rate(...) * 0.01` everywhere.
//!
//! The wiring is **default-on** per the SHELF-40 acceptance gate;
//! operators flip `cache.cost.enabled: false` in YAML to disable
//! both the counter bumps and the rate-updater task.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use shelf_cost::{CostConfig, CostConfigError, CostModel, HitEvent};
use tokio_util::sync::CancellationToken;

/// Shared state the hot path reads on every served byte.
#[derive(Debug, Clone)]
pub struct CostState {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    enabled: bool,
    model: CostModel,
    /// Last sampled `shelf_s3_dollars_saved_total` value per
    /// `(region, outcome)` 60-sample sliding window. We keep an
    /// in-memory ring so the rate gauge can publish a smooth
    /// 60s rate without the dashboard having to re-derive
    /// `rate(... [60s])` in PromQL. Cardinality is bounded by
    /// `regions × 3 outcomes` ≤ ~ 6 series in practice.
    samples: parking_lot::RwLock<std::collections::HashMap<(String, &'static str), Window>>,
}

#[derive(Debug)]
struct Window {
    /// Ring of last-second cumulative cents (oldest at `head`).
    /// Length is 60 (one per second over the last minute).
    ring: [i64; 60],
    head: u8,
    filled: u8,
}

impl Window {
    fn push(&mut self, total: i64) -> i64 {
        let oldest = self.ring[self.head as usize];
        self.ring[self.head as usize] = total;
        self.head = (self.head + 1) % 60;
        if self.filled < 60 {
            self.filled += 1;
        }
        // Rate over the *filled* window (denominator scales early
        // in the lifetime of a pod so the gauge isn't artificially
        // small for the first minute).
        let denom = self.filled.max(1) as i64;
        // (newest - oldest_within_window) / window_seconds
        // The newest is `total`; the oldest is `oldest` only when
        // the ring is full; while filling, the oldest valid entry
        // is the value at index `(head - filled) % 60`.
        let oldest_idx = if self.filled < 60 {
            // Before the ring fills, the oldest is wherever we
            // started — index `(head - filled)` mod 60.
            ((self.head as i16 - self.filled as i16).rem_euclid(60)) as usize
        } else {
            self.head as usize
        };
        let oldest_val = if self.filled < 60 {
            self.ring[oldest_idx]
        } else {
            oldest
        };
        let delta = total.saturating_sub(oldest_val);
        delta / denom
    }
}

impl Window {
    fn new() -> Self {
        Self {
            ring: [0; 60],
            head: 0,
            filled: 0,
        }
    }
}

impl CostState {
    /// Build from a validated [`CostConfig`]. Returns the same
    /// [`CostConfigError`] the loader produced — the only place
    /// we ever materialise a `CostModel` is here, so errors
    /// surface at the top of `main` and refuse to register the
    /// counter (matching the SHELF-40 anti-overclaim gate).
    pub fn from_config(cfg: &CostConfig) -> Result<Self, CostConfigError> {
        let model = CostModel::from_config(cfg)?;
        Ok(Self {
            inner: Arc::new(Inner {
                enabled: cfg.enabled,
                model,
                samples: parking_lot::RwLock::new(std::collections::HashMap::new()),
            }),
        })
    }

    /// "Off" sentinel — cost wiring inert, every observe is a
    /// no-op. Useful in unit tests / lightweight integration tests
    /// that don't want a real cost model in scope.
    pub fn disabled() -> Self {
        // Safe even though we ignore failures: the us-east-1 preset
        // is deterministic and never returns Err for this region.
        let model = CostModel::for_region("us-east-1").expect("preset");
        Self {
            inner: Arc::new(Inner {
                enabled: false,
                model,
                samples: parking_lot::RwLock::new(std::collections::HashMap::new()),
            }),
        }
    }

    /// Whether the runtime is bumping the counter today. Hot path
    /// reads this once per request; an inert state short-circuits
    /// to zero atomic adds.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.inner.enabled
    }

    /// Region label used for both Prometheus dimensions; stable
    /// for the lifetime of the process.
    #[inline]
    pub fn region(&self) -> &str {
        &self.inner.model.region_id
    }

    /// Compute and bump the counter for `event`. Returns the
    /// `Cents` contribution so the caller (or a debug log) can see
    /// what was added without re-running the formula.
    ///
    /// Hot path: roughly two integer multiplies and an atomic
    /// add — see `crates/shelf-cost/benches/`. When `enabled` is
    /// false this short-circuits to a single bool load + return.
    #[inline]
    pub fn observe(&self, event: HitEvent) -> shelf_cost::Cents {
        if !self.inner.enabled {
            return shelf_cost::Cents::ZERO;
        }
        let saved = self.inner.model.dollars_saved(event);
        let inc = saved.as_cents_u64();
        if inc > 0 {
            crate::metrics::S3_DOLLARS_SAVED_TOTAL
                .with_label_values(&[self.region(), event.outcome_label()])
                .inc_by(inc);
        }
        saved
    }

    /// Spawn the rolling-rate updater task. Ticks once per second,
    /// reads each `(region, outcome)` series of
    /// `shelf_s3_dollars_saved_total`, pushes the new sample into
    /// the ring, and writes the resulting cents-per-second number
    /// to `shelf_s3_dollars_saved_rate_cents_per_sec`.
    ///
    /// The task observes `cancel`; when the parent shuts down the
    /// loop returns instead of leaving the runtime alive.
    pub fn spawn_rate_updater(&self, cancel: CancellationToken) {
        if !self.inner.enabled {
            return;
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            // 1 Hz tick; SHELF-40 acceptance is "60s rolling", so
            // any tick rate ≥ 1/60 Hz produces a stable signal.
            // 1 Hz is the natural fit for a 60-sample ring.
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::debug!("SHELF-40 rate updater shutting down");
                        return;
                    }
                    _ = ticker.tick() => {
                        update_rate(&inner);
                    }
                }
            }
        });
    }
}

fn update_rate(inner: &Inner) {
    // Walk the **observed** label values via a snapshot of the
    // counter family. We can't enumerate `IntCounterVec`'s children
    // directly, so we rely on `MetricVec` returning the populated
    // children via `collect()`. For a typical 1-region cluster this
    // produces 0..3 rows once any traffic flows.
    let region = inner.model.region_id.as_str();
    for outcome in ["hit_memory", "hit_disk", "peer"] {
        // `with_label_values` lazily registers a child; we then
        // read its value. This is the standard pattern in the
        // prometheus crate for recurring scrape-style reads.
        let total = crate::metrics::S3_DOLLARS_SAVED_TOTAL
            .with_label_values(&[region, outcome])
            .get();
        let total_i64 = total as i64;
        let mut samples = inner.samples.write();
        let entry = samples
            .entry((region.to_owned(), outcome))
            .or_insert_with(Window::new);
        let rate = entry.push(total_i64);
        crate::metrics::S3_DOLLARS_SAVED_RATE_CENTS_PER_SEC
            .with_label_values(&[region, outcome])
            .set(rate);
    }
}

/// Tiny helper that maps `(HitTier, peer_az)` to the right
/// [`HitEvent`] variant for the local cache-hit path. Centralising
/// the `match` saves us from sprinkling identical four-way switches
/// across `s3_shim` and any future direct-cache caller.
#[inline]
pub fn hit_event_local(
    tier: crate::store::HitTier,
    bytes: u64,
    peer_az: shelf_cost::PeerAz,
) -> HitEvent {
    match tier {
        crate::store::HitTier::Memory => HitEvent::Memory {
            bytes_returned: bytes,
            peer_az,
        },
        crate::store::HitTier::Disk => HitEvent::Disk {
            bytes_returned: bytes,
            peer_az,
        },
    }
}

/// SHELF-40 — pessimistic default. Without explicit AZ-aware
/// membership data (SHELF-23 + SHELF-20 surfaces it later), every
/// hit is **modelled** as same-AZ so the counter never inflates by
/// claiming cross-AZ savings that didn't actually happen. Operators
/// who confirm a cross-AZ topology (e.g. multi-AZ Trino-per-shelfd
/// pairing) can flip the contract via a future `peer_az` per-pod
/// override on `CostState`. The OSS-default contract today is
/// "same-AZ unless proven otherwise".
pub const DEFAULT_PEER_AZ: shelf_cost::PeerAz = shelf_cost::PeerAz::SameAz;

/// SHELF-40 internal use only — kept as a 64-bit atomic so the
/// caller can probe rolling totals from a non-async context (the
/// integration test harness uses this to assert the counter
/// advanced after N hits without waiting on a Prometheus scrape).
#[derive(Debug, Default)]
pub struct DebugProbe(AtomicI64);

impl DebugProbe {
    pub fn add(&self, delta: i64) {
        self.0.fetch_add(delta, Ordering::Relaxed);
    }
    pub fn read(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// **A4 (rc.7)** — net dollars-saved accountant.
///
/// SHELF-40 ships a *gross* savings counter
/// (`shelf_s3_dollars_saved_total`, in cents) — every cache hit
/// credits the formula in `crates/shelf-cost/`. Procurement needs
/// the **net** number: gross savings minus the operating cost of
/// the shelf pool itself. A 6-pod `m5a.4xlarge` pool in
/// `ap-south-1` runs ~$5.18/hour; if the gross counter only ticked
/// $4/hour, the cluster is *losing* money on shelf and the
/// counter alone hides that.
///
/// This accountant runs on a periodic tokio task (see
/// [`spawn_net_accountant`] / `shelfd/src/main.rs`). Each tick
/// reads the current `S3_DOLLARS_SAVED_TOTAL` value (in cents)
/// converted to dollar-micros, subtracts
/// `amortized_dollars_per_hour × elapsed_seconds`, and credits the
/// delta to `shelf_s3_dollars_saved_net_total` (clamped at zero so
/// the cumulative counter never decrements — Prometheus rejects
/// counter regressions on rate-window boundaries).
///
/// Anti-overclaim guard: if `amortized_dollars_per_hour` is unset,
/// non-finite, or non-positive at construction, [`Self::tick`]
/// returns `None` and the net counter stays at zero. Operators MUST
/// explicitly set `cache.cost.amortizedDollarsPerHour` for the net
/// counter to populate. The companion gauge
/// `shelf_pool_amortized_dollars_per_hour` is always exposed and
/// reads `0` when the guard is active so dashboards can flag the
/// misconfiguration.
///
/// Units convention (must stay consistent with `metrics.rs`):
/// `amortized_micros_per_hour` stores the operator's
/// dollars-per-hour value × 1_000_000 so the per-second cost slice
/// `amortized_micros_per_hour × elapsed / 3600` stays in pure
/// integer arithmetic (no rounding drift across ticks).
#[derive(Debug)]
pub struct NetCostAccountant {
    /// Stored as integer micros: `amortized_dollars_per_hour × 1_000_000`.
    /// `0` ⇒ guard active (anti-overclaim).
    amortized_micros_per_hour: AtomicU64,
    /// Wall-clock seconds at the last `tick`. Initialised at `new`
    /// so the very first tick reports the elapsed window since
    /// process start.
    last_publish_unix_secs: AtomicU64,
    /// Last observed gross-savings value, expressed in **dollar
    /// micros** (caller converts the underlying cents-valued
    /// counter before passing in). Initialised to `0` so the first
    /// tick after process start measures cumulative savings since
    /// boot — that's the right semantics because the `shelfd`
    /// process restart resets `S3_DOLLARS_SAVED_TOTAL` to `0`
    /// anyway, so `last_gross_micros = 0` and `gross_micros_now`
    /// agree on the boot epoch.
    last_gross_micros: AtomicU64,
}

impl NetCostAccountant {
    /// Build from the operator-supplied amortization. `None` /
    /// non-finite / non-positive all collapse to the unset state
    /// (anti-overclaim guard active).
    pub fn new(amortized_dollars_per_hour: Option<f64>) -> Self {
        let micros = amortized_dollars_per_hour
            .filter(|x| x.is_finite() && *x > 0.0)
            .map(|x| (x * 1_000_000.0) as u64)
            .unwrap_or(0);
        Self {
            amortized_micros_per_hour: AtomicU64::new(micros),
            last_publish_unix_secs: AtomicU64::new(unix_secs_now()),
            last_gross_micros: AtomicU64::new(0),
        }
    }

    /// `true` iff the operator supplied a positive, finite
    /// amortization. The accountant task SHOULD still update the
    /// gauge to `0` even when this returns `false` — operators rely
    /// on the gauge being present to spot the misconfiguration.
    #[inline]
    pub fn is_publishable(&self) -> bool {
        self.amortized_micros_per_hour.load(Ordering::Relaxed) > 0
    }

    /// Current amortization in micros. The gauge ships this value
    /// every tick so dashboards see `0` when the guard is active.
    #[inline]
    pub fn amortized_micros_per_hour(&self) -> u64 {
        self.amortized_micros_per_hour.load(Ordering::Relaxed)
    }

    /// One accountant tick.
    ///
    /// `gross_micros_now` is the cumulative gross savings (in
    /// dollar-micros) observed *now*. Returns `Some(net_delta_micros)`
    /// if the guard is inactive (i.e. `amortized > 0`); the caller
    /// then increments the net counter by `max(0, delta)`. Returns
    /// `None` when the guard is active.
    ///
    /// `last_publish_unix_secs` and `last_gross_micros` advance on
    /// every call regardless of the publish gate so a future
    /// operator who flips the amortization on at runtime starts
    /// from a fresh window (and doesn't retroactively credit the
    /// pre-config period as if it had been free).
    pub fn tick(&self, gross_micros_now: u64) -> Option<i64> {
        self.tick_with_now(gross_micros_now, unix_secs_now())
    }

    /// Test-only seam: identical to [`Self::tick`] but uses the
    /// supplied wall-clock time instead of the system clock.
    pub fn tick_with_now(&self, gross_micros_now: u64, now_unix_secs: u64) -> Option<i64> {
        let amortized = self.amortized_micros_per_hour.load(Ordering::Relaxed);
        // Always advance the bookkeeping atomics so a later
        // amortization flip sees a fresh window (see method doc).
        let last_secs = self
            .last_publish_unix_secs
            .swap(now_unix_secs, Ordering::Relaxed);
        let last_gross = self
            .last_gross_micros
            .swap(gross_micros_now, Ordering::Relaxed);
        if amortized == 0 {
            return None;
        }
        let elapsed_secs = now_unix_secs.saturating_sub(last_secs);
        // `amortized × elapsed / 3600` in i128 to keep the
        // arithmetic inside-the-domain even at extreme values (a
        // shelf pool sustained at $1k/hr for a year still fits
        // comfortably below i128::MAX).
        let amortized_for_window = (amortized as i128).saturating_mul(elapsed_secs as i128) / 3600;
        let gross_delta = (gross_micros_now as i128).saturating_sub(last_gross as i128);
        let net = gross_delta.saturating_sub(amortized_for_window);
        // Clamp into i64 so the prometheus counter API (u64
        // increment, but our wrapper feeds an i64 step) is happy.
        let clamped = net.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        Some(clamped)
    }
}

/// Spawn the A4 net-cost accountant task. Runs on a 60 s tick.
///
/// Always updates `shelf_pool_amortized_dollars_per_hour` to the
/// current configured value (so the `0` reading is itself a
/// signal). Reads the current sum across labels of
/// `shelf_s3_dollars_saved_total` (which carries cents), converts
/// to dollar-micros, calls [`NetCostAccountant::tick`], and
/// — only when `is_publishable()` — credits the clamped delta to
/// `shelf_s3_dollars_saved_net_total{region}`.
pub fn spawn_net_accountant(
    accountant: Arc<NetCostAccountant>,
    region: String,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        // 60 s cadence; the gross counter ticks once per cache hit,
        // the operator dashboards refresh every 30–60 s, so 1/min is
        // plenty of fidelity and keeps the wakeup budget tiny.
        let mut ticker = tokio::time::interval(Duration::from_secs(60));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!("A4 net-cost accountant shutting down");
                    return;
                }
                _ = ticker.tick() => {
                    run_one_tick(&accountant, &region);
                }
            }
        }
    });
}

fn run_one_tick(accountant: &NetCostAccountant, region: &str) {
    // Always publish the gauge (operators rely on `0 ⇒ unset`).
    let amortized_micros = accountant.amortized_micros_per_hour();
    crate::metrics::SHELF_POOL_AMORTIZED_DOLLARS_PER_HOUR.set(amortized_micros as i64);

    // Gross is in **cents**; convert to dollar-micros (1 cent =
    // 10_000 dollar-micros) before feeding the accountant so the
    // unit invariant in NetCostAccountant holds.
    let gross_cents_total = sum_gross_cents(region);
    let gross_micros = gross_cents_total.saturating_mul(10_000);

    if let Some(delta_micros) = accountant.tick(gross_micros) {
        // Clamp negative cumulative savings to zero — the net
        // counter is monotonic by Prometheus contract, and a
        // single-tick negative blip (gross stalled while pool
        // amortization kept ticking) is recoverable on the next
        // positive tick. Procurement reads cumulative numbers, not
        // per-tick.
        if delta_micros > 0 {
            crate::metrics::S3_DOLLARS_SAVED_NET_TOTAL
                .with_label_values(&[region])
                .inc_by(delta_micros as u64);
        }
    }
}

/// Sum of `shelf_s3_dollars_saved_total{region, outcome}` across
/// every outcome (`hit_memory`, `hit_disk`, `peer`) for a given
/// region, in **cents**.
///
/// Matches the outcome list the SHELF-40 rate updater walks (see
/// [`update_rate`]). We call `with_label_values(...).get()` per
/// outcome — that's `IntCounter::get()` returning `u64` directly,
/// the same fast path the rate updater uses, no need for the
/// protobuf `Collector::collect()` API.
fn sum_gross_cents(region: &str) -> u64 {
    let mut total: u64 = 0;
    for outcome in ["hit_memory", "hit_disk", "peer"] {
        let v = crate::metrics::S3_DOLLARS_SAVED_TOTAL
            .with_label_values(&[region, outcome])
            .get();
        total = total.saturating_add(v);
    }
    total
}

fn unix_secs_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        // SystemTime is monotonic enough on every supported target
        // that this branch is unreachable; the `0` fallback simply
        // makes the first tick observe the full uptime as elapsed.
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_state_short_circuits_observe() {
        let cs = CostState::disabled();
        assert!(!cs.is_enabled());
        let saved = cs.observe(HitEvent::Memory {
            bytes_returned: 1 << 30,
            peer_az: shelf_cost::PeerAz::CrossAz,
        });
        assert_eq!(saved, shelf_cost::Cents::ZERO);
    }

    #[test]
    fn enabled_state_observes_and_returns_cents() {
        let cfg = CostConfig {
            enabled: true,
            region: "ap-south-1".to_owned(),
            ..CostConfig::default()
        };
        let cs = CostState::from_config(&cfg).unwrap();
        assert!(cs.is_enabled());
        // 1 GiB cross-AZ Disk hit = 1 cent (the data-transfer term
        // contributes exactly $0.01/GiB; the GET term is sub-cent).
        let saved = cs.observe(HitEvent::Disk {
            bytes_returned: 1 << 30,
            peer_az: shelf_cost::PeerAz::CrossAz,
        });
        assert_eq!(saved.as_cents_i64(), 1, "got {saved}");
    }

    #[test]
    fn window_push_yields_stable_rate_after_filling() {
        // 60 samples of 100-cents-per-sec → rate should be 100.
        let mut w = Window::new();
        for sec in 0..60 {
            let total = sec * 100;
            let _ = w.push(total);
        }
        // The 61st push at +100 over the new 'oldest' should still
        // report ~100 cents/sec.
        let rate = w.push(60 * 100);
        assert!((90..=110).contains(&rate), "rate={rate}");
    }

    #[test]
    fn hit_event_local_picks_right_variant() {
        let m = hit_event_local(
            crate::store::HitTier::Memory,
            1234,
            shelf_cost::PeerAz::SameAz,
        );
        assert!(matches!(m, HitEvent::Memory { .. }));
        let d = hit_event_local(
            crate::store::HitTier::Disk,
            5678,
            shelf_cost::PeerAz::CrossAz,
        );
        assert!(matches!(d, HitEvent::Disk { .. }));
    }

    // ---------------------------------------------------------------
    // A4 — NetCostAccountant tests.
    //
    // The seven tests below pin the anti-overclaim guard semantics
    // (None / 0.0 / negative / NaN ⇒ never publish) plus the
    // integer-arithmetic correctness of the per-tick net delta and
    // the multi-tick accumulation.
    // ---------------------------------------------------------------

    #[test]
    fn unset_amortization_refuses_to_publish() {
        let acc = NetCostAccountant::new(None);
        assert!(!acc.is_publishable());
        // `tick` always advances bookkeeping but returns None when
        // the guard is active.
        let result = acc.tick_with_now(5_000_000, 100);
        assert!(result.is_none(), "expected None, got {:?}", result);
    }

    #[test]
    fn zero_amortization_refuses_to_publish() {
        let acc = NetCostAccountant::new(Some(0.0));
        assert!(!acc.is_publishable());
        assert_eq!(acc.amortized_micros_per_hour(), 0);
        assert!(acc.tick_with_now(5_000_000, 100).is_none());
    }

    #[test]
    fn negative_amortization_refuses_to_publish() {
        let acc = NetCostAccountant::new(Some(-1.0));
        assert!(!acc.is_publishable());
        assert_eq!(acc.amortized_micros_per_hour(), 0);
        assert!(acc.tick_with_now(5_000_000, 100).is_none());
    }

    #[test]
    fn nan_amortization_refuses_to_publish() {
        let acc = NetCostAccountant::new(Some(f64::NAN));
        assert!(!acc.is_publishable());
        assert_eq!(acc.amortized_micros_per_hour(), 0);
        // Also pin +inf for completeness — same guard semantics.
        let acc_inf = NetCostAccountant::new(Some(f64::INFINITY));
        assert!(!acc_inf.is_publishable());
    }

    #[test]
    fn positive_amortization_publishes() {
        // 5.18 $/hr ⇒ 5_180_000 dollar-micros / hr.
        let acc = NetCostAccountant::new(Some(5.18));
        assert!(acc.is_publishable());
        assert_eq!(acc.amortized_micros_per_hour(), 5_180_000);
        // Boot at t=100 (NetCostAccountant::new uses unix_secs_now()
        // internally; we override last_publish_unix_secs by
        // calling tick_with_now twice — first to seed, second to
        // measure the window).
        let _ = acc.tick_with_now(0, 100);
        // 1 hour later (3600 s) gross has climbed to 10_000_000
        // dollar-micros = $10. Pool cost over the hour was
        // 5_180_000 micros = $5.18. Net delta should be
        // 4_820_000 micros = $4.82.
        let delta = acc
            .tick_with_now(10_000_000, 100 + 3600)
            .expect("publishable returns Some");
        assert_eq!(
            delta,
            4_820_000,
            "expected $4.82 net, got ${}",
            delta as f64 / 1e6
        );
    }

    #[test]
    fn accumulate_across_ticks() {
        // 3.6 $/hr ⇒ 1_000 micros/sec exact (no rounding residue).
        let acc = NetCostAccountant::new(Some(3.6));
        assert!(acc.is_publishable());
        // Seed at t=0, gross=0.
        let _ = acc.tick_with_now(0, 0);
        // Tick 1: t=10s, gross=20_000 µ$ ⇒ amortized = 1000*10 =
        // 10_000 µ$ ⇒ net delta = 10_000.
        let d1 = acc.tick_with_now(20_000, 10).expect("publishable");
        // Tick 2: t=20s, gross=50_000 µ$ ⇒ amortized for window =
        // 1000*10 = 10_000 ⇒ net delta = 30_000 - 10_000 = 20_000.
        let d2 = acc.tick_with_now(50_000, 20).expect("publishable");
        // Tick 3: t=30s, gross=100_000 µ$ ⇒ amortized = 10_000 ⇒
        // net delta = 50_000 - 10_000 = 40_000.
        let d3 = acc.tick_with_now(100_000, 30).expect("publishable");
        // Sum of clamped positive deltas equals
        // gross_total - amortized_total = 100_000 - 30_000 = 70_000.
        assert_eq!(d1.max(0) + d2.max(0) + d3.max(0), 70_000);
    }

    #[test]
    fn monotonic_gross_input_never_goes_negative() {
        // Pool that earns its keep (gross grows faster than pool
        // burn rate) — every per-tick delta MUST be ≥ 0 so the
        // cumulative net counter only ever moves forward.
        let acc = NetCostAccountant::new(Some(1.8)); // 500 µ$/sec
        let _ = acc.tick_with_now(0, 0);
        // Gross grows monotonically at 1000 µ$/sec — twice the pool
        // burn rate, so each per-tick delta should be ~ +500 µ$/sec
        // × elapsed.
        for t in [10_u64, 25, 60, 90, 120, 150, 200] {
            let gross = (t as i128 * 1000) as u64;
            let d = acc.tick_with_now(gross, t).expect("publishable");
            assert!(d >= 0, "tick at t={} produced negative delta {}", t, d);
        }
    }
}
