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

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

/// SHELF-A4 — net dollars-saved accountant.
///
/// Wraps a periodic tick that:
///
/// 1. Reads the current cumulative value of
///    `shelf_s3_dollars_saved_total` (the SHELF-40 gross counter)
///    across every `(region, outcome)` series for our region.
/// 2. Subtracts the operator-configured amortised pool cost over the
///    elapsed wall-clock since the previous tick (in **micro-cents**
///    so we keep fixed-point precision through the divide).
/// 3. Bumps `shelf_s3_dollars_saved_net_total` by the resulting
///    cents — but only when the delta is **positive**. Counter
///    semantics forbid decrements; intervals where amortisation
///    outpaces gross savings simply leave the counter flat (the
///    operator reads gross outpacing net as the dashboard signal).
///
/// Anti-overclaim guard: when [`CostConfig::amortized_dollars_per_hour`]
/// is `None`, [`NetCostAccountant::is_publishable`] returns `false`,
/// [`NetCostAccountant::tick`] returns `None`, and the updater task
/// short-circuits before its first sleep. The companion gauge
/// `shelf_pool_amortized_dollars_per_hour` is **always** published —
/// a value of `0` is the dashboard signal that net accounting is
/// dormant.
///
/// Why a separate type from [`CostState`]: [`CostState`] is a hot-path
/// counter (one atomic add per cache hit) that consumes the
/// crate-level [`CostModel`]. The accountant is a slow-path
/// background task — it samples the counter once every
/// [`NET_TICK_INTERVAL`] seconds, so we'd rather keep it out of the
/// hot path's symbol table.
#[derive(Debug, Clone)]
pub struct NetCostAccountant {
    inner: Arc<NetInner>,
}

#[derive(Debug)]
struct NetInner {
    /// Region label used to filter the gross counter and stamp the
    /// net counter. Stable for the process lifetime.
    region: String,

    /// Operator-configured amortisation rate, preserved as the
    /// originally-configured `Option<f64>` so [`is_publishable`]
    /// distinguishes "explicitly set to 0" from "never set".
    amortized_dollars_per_hour: Option<f64>,

    /// Pre-computed amortisation rate in **micro-cents per second**
    /// for the tick-time math. Zero when the operator left the field
    /// unset *or* explicitly set it to `0.0`. The hot subtraction
    /// uses this value; [`is_publishable`] uses the `Option` above.
    amortized_micro_cents_per_sec: u64,

    /// Tick state guarded by a parking_lot mutex. The mutex is held
    /// for ~µs (a couple of arithmetic ops) so contention is a non-issue
    /// even if a future call site hammers `tick` from multiple tasks.
    state: parking_lot::Mutex<NetTickState>,
}

#[derive(Debug, Default)]
struct NetTickState {
    /// `Instant` of the previous tick, or `None` before the first.
    last_tick: Option<Instant>,
    /// Cumulative gross cents observed at the previous tick. Used as
    /// the baseline for the next interval's gross-delta computation.
    last_gross_cents: i64,
}

/// Updater-task wake interval. 10 s is short enough that an operator
/// looking at `rate(shelf_s3_dollars_saved_net_total[1m])` sees a
/// fresh signal but long enough that the amortisation subtraction
/// converges to the configured $/hr without integer-rounding
/// artifacts (10 s × 5.18 $/hr ≈ 1.4 cents per tick — well above the
/// per-hit residue the gross counter drops at the µ¢→¢ boundary).
pub const NET_TICK_INTERVAL: Duration = Duration::from_secs(10);

impl NetCostAccountant {
    /// Build a new accountant.
    ///
    /// `region` flows directly to the `region` label on
    /// `shelf_s3_dollars_saved_net_total`. `amortized_dollars_per_hour`
    /// comes from validated config (see
    /// [`CostConfig::validated_amortized_dollars_per_hour`]); a
    /// `None` value disables [`tick`] and [`spawn_updater`] entirely
    /// (anti-overclaim).
    ///
    /// Side effect: the
    /// [`crate::metrics::POOL_AMORTIZED_DOLLARS_PER_HOUR`] gauge is
    /// set to the configured value (in cents per hour) before this
    /// function returns. The gauge is always published — even on the
    /// `None` path — so dashboards can detect dormant accountants by
    /// reading the gauge at zero.
    pub fn new(region: String, amortized_dollars_per_hour: Option<f64>) -> Self {
        // Treat NaN / negative as "disabled" defensively — config
        // validation should have caught these upstream, but we'd
        // rather emit a zero gauge than a panic on a misconfigured
        // cluster.
        let dph_clamped = amortized_dollars_per_hour
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.0);

        // µ¢/sec = $/hr × 100 ¢/$ × 1_000_000 µ¢/¢ ÷ 3600 s/hr
        //        = $/hr × (1.0e8 / 3600.0)
        let amortized_micro_cents_per_sec = dollars_per_hour_to_micro_cents_per_sec(dph_clamped);

        // Gauge is denominated in **integer cents per hour** to
        // avoid the "scientific notation in YAML" landmine the
        // workspace already documents for big-number floats.
        let cents_per_hour = (dph_clamped * 100.0).round().clamp(0.0, i64::MAX as f64) as i64;
        crate::metrics::POOL_AMORTIZED_DOLLARS_PER_HOUR.set(cents_per_hour);

        Self {
            inner: Arc::new(NetInner {
                region,
                amortized_dollars_per_hour,
                amortized_micro_cents_per_sec,
                state: parking_lot::Mutex::new(NetTickState::default()),
            }),
        }
    }

    /// Whether the accountant will publish to the net counter today.
    /// Returns `true` iff the operator explicitly set
    /// `cache.cost.amortized_dollars_per_hour` (any non-negative
    /// finite value, including `0.0`).
    #[inline]
    pub fn is_publishable(&self) -> bool {
        self.inner.amortized_dollars_per_hour.is_some()
    }

    /// Region label this accountant is bound to.
    #[inline]
    pub fn region(&self) -> &str {
        &self.inner.region
    }

    /// Read the cumulative gross cents this accountant tracks.
    ///
    /// Sums over the three published `outcome` labels for our
    /// region. Touching `with_label_values` for each outcome lazily
    /// registers a child if traffic has not yet flowed; the
    /// resulting child reads as `0`, which is harmless.
    pub fn current_gross_cents(&self) -> i64 {
        current_gross_cents(&self.inner.region)
    }

    /// Per-tick subtraction. Returns the *signed* net delta in cents
    /// since the previous tick, or `None` when amortisation is unset
    /// (anti-overclaim guard).
    ///
    /// The first call after construction returns `Some(0)` (no
    /// baseline to delta against), records `gross_cents` as the new
    /// baseline, and stamps `now`. Subsequent calls return
    /// `gross_delta - amortised_cents`, which can be negative when
    /// the cache was idle long enough for amortisation to outpace
    /// savings. The caller (the updater task) is responsible for
    /// gating the counter `inc_by` on a positive delta.
    pub fn tick(&self, gross_cents: i64) -> Option<i64> {
        self.tick_at(gross_cents, Instant::now())
    }

    /// Test-visible variant of [`tick`] that takes an explicit
    /// `Instant` so unit tests can simulate elapsed seconds without
    /// `tokio::time::pause`. Production callers always go through
    /// [`tick`].
    pub fn tick_at(&self, gross_cents: i64, now: Instant) -> Option<i64> {
        if !self.is_publishable() {
            return None;
        }
        let mut state = self.inner.state.lock();
        let net_delta_cents = match state.last_tick {
            None => 0i64,
            Some(prev) => {
                let elapsed = now.saturating_duration_since(prev);
                let elapsed_us = elapsed.as_micros();
                // amort_µ¢ = µ¢/sec × elapsed_us ÷ 1_000_000
                // amort_¢  = amort_µ¢ ÷ 1_000_000
                let amort_uc = (self.inner.amortized_micro_cents_per_sec as u128)
                    .saturating_mul(elapsed_us)
                    / 1_000_000u128;
                let amort_cents = (amort_uc / 1_000_000).min(i64::MAX as u128) as i64;
                let gross_delta = gross_cents.saturating_sub(state.last_gross_cents);
                gross_delta.saturating_sub(amort_cents)
            }
        };
        state.last_tick = Some(now);
        state.last_gross_cents = gross_cents;
        Some(net_delta_cents)
    }

    /// Spawn the periodic updater task. No-op (and the task does not
    /// run at all) when [`is_publishable`] is `false`. Cancellation
    /// flows from the parent shutdown token; on cancel the task
    /// returns instead of leaving the runtime alive.
    pub fn spawn_updater(&self, cancel: CancellationToken) {
        if !self.is_publishable() {
            tracing::info!(
                region = %self.inner.region,
                "SHELF-A4 net accountant dormant (cache.cost.amortized_dollars_per_hour unset)",
            );
            return;
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(NET_TICK_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::debug!("SHELF-A4 net accountant shutting down");
                        return;
                    }
                    _ = ticker.tick() => {
                        let me = NetCostAccountant { inner: inner.clone() };
                        let gross = me.current_gross_cents();
                        if let Some(delta) = me.tick(gross) {
                            if delta > 0 {
                                crate::metrics::S3_DOLLARS_SAVED_NET_TOTAL
                                    .with_label_values(&[&inner.region])
                                    .inc_by(delta as u64);
                            }
                            // Negative or zero delta: counter stays
                            // flat — Prom counters cannot decrement,
                            // and the dashboard reads "gross > net"
                            // as the underwater signal.
                        }
                    }
                }
            }
        });
    }
}

/// Convert dollars-per-hour to micro-cents-per-second using
/// floating-point intermediate (clamped to `u64::MAX` defensively).
/// Constant-folded by the optimiser since `dph` is the only runtime
/// input; the fixed multiplier is `1.0e8 / 3600.0 ≈ 27_777.778`.
#[inline]
fn dollars_per_hour_to_micro_cents_per_sec(dph: f64) -> u64 {
    if !dph.is_finite() || dph <= 0.0 {
        return 0;
    }
    const SCALE: f64 = 1.0e8 / 3600.0;
    (dph * SCALE).round().clamp(0.0, u64::MAX as f64) as u64
}

/// Read the cumulative cents tracked by `shelf_s3_dollars_saved_total`
/// for `region`, summed across the three published `outcome` labels.
fn current_gross_cents(region: &str) -> i64 {
    let mut total: i64 = 0;
    for outcome in ["hit_memory", "hit_disk", "peer"] {
        let v = crate::metrics::S3_DOLLARS_SAVED_TOTAL
            .with_label_values(&[region, outcome])
            .get();
        // `IntCounterVec::get` returns `u64`; saturate into `i64` so
        // a pod that has accumulated more than `i64::MAX` cents
        // (impossible in practice — would take ~292 years at $1B/sec)
        // does not wrap into a negative.
        let v_i64 = if v > i64::MAX as u64 {
            i64::MAX
        } else {
            v as i64
        };
        total = total.saturating_add(v_i64);
    }
    total
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

    // SHELF-A4 — NetCostAccountant unit tests.
    //
    // Region labels are unique per test so each accountant exercises
    // an isolated child of `shelf_s3_dollars_saved_total` /
    // `shelf_s3_dollars_saved_net_total`. This keeps tests
    // independent of the shared `prometheus::Registry`.

    #[test]
    fn unset_amortization_refuses_to_publish() {
        let acc = NetCostAccountant::new("net-test-unset".to_owned(), None);
        assert!(!acc.is_publishable());
        // `tick` returns None on the unset path regardless of the
        // gross input — anti-overclaim guard.
        assert_eq!(acc.tick(1_234), None);
        assert_eq!(acc.tick(99_999_999), None);
    }

    #[test]
    fn amortization_set_zero_publishes_gross_only() {
        // Some(0.0) is "configured zero" — publishable, but the
        // amortisation subtraction contributes nothing, so net == gross.
        let acc = NetCostAccountant::new("net-test-zero".to_owned(), Some(0.0));
        assert!(acc.is_publishable());

        let t0 = Instant::now();
        // First tick records baseline; returns Some(0).
        assert_eq!(acc.tick_at(0, t0), Some(0));
        // Second tick after some elapsed time — gross delta = 500 ¢.
        let t1 = t0 + Duration::from_secs(60);
        assert_eq!(
            acc.tick_at(500, t1),
            Some(500),
            "amort=0 must not subtract anything",
        );
    }

    #[test]
    fn amortization_set_positive_subtracts_correctly() {
        // 5.18 $/hr ≈ 0.001_438_88... ¢ per ms. Over 60 s the
        // subtraction is exactly:
        //   5.18 $/hr × 100 ¢/$ × 60 s ÷ 3600 s/hr = 8.6333... ¢
        // Truncates to 8 ¢ in the integer-cents pipeline.
        let acc = NetCostAccountant::new("net-test-positive".to_owned(), Some(5.18));
        assert!(acc.is_publishable());

        let t0 = Instant::now();
        assert_eq!(acc.tick_at(0, t0), Some(0));

        // After 60 s of wall-clock and a 100 ¢ gross gain, net delta
        // should be 100 - 8 = 92 ¢ (give or take the µs→s rounding).
        let t1 = t0 + Duration::from_secs(60);
        let net = acc.tick_at(100, t1).expect("publishable");
        // Allow ±1 ¢ for the integer truncation on the µ¢ → ¢ step.
        assert!(
            (91..=92).contains(&net),
            "expected net ≈ 92 ¢ (= 100 ¢ gross − ~8 ¢ amort over 60 s); got {net}",
        );
    }

    #[test]
    fn multiple_ticks_accumulate_correctly() {
        // Walk through three intervals at 5.18 $/hr: ticks at
        // t0, t0+60s, t0+120s, t0+180s with a steady 200 ¢/min
        // gross-savings rate. Each interval should net 200 - 8 ≈ 192 ¢.
        let acc = NetCostAccountant::new("net-test-multi".to_owned(), Some(5.18));
        let t0 = Instant::now();
        assert_eq!(acc.tick_at(0, t0), Some(0)); // baseline

        let mut sum_net: i64 = 0;
        for i in 1..=3 {
            let ti = t0 + Duration::from_secs(60 * i);
            let gross = (200 * i) as i64;
            let net = acc.tick_at(gross, ti).expect("publishable");
            assert!(
                (191..=193).contains(&net),
                "interval {i}: expected ~192 ¢, got {net}",
            );
            sum_net += net;
        }
        // After 3 intervals the total net ≈ 3 × 192 = 576 ¢, with a
        // few cents of accumulated truncation error allowed.
        assert!(
            (572..=580).contains(&sum_net),
            "summed net over 3 ticks should be ≈ 576 ¢; got {sum_net}",
        );
    }

    #[test]
    fn region_label_propagated() {
        // Construct the accountant against a unique region and
        // exercise a positive net delta through the public counter.
        // The Prometheus child for that label must reflect the bump.
        let region = "net-test-region-unique";
        let acc = NetCostAccountant::new(region.to_owned(), Some(0.0));
        let t0 = Instant::now();
        assert_eq!(acc.tick_at(0, t0), Some(0));
        let net = acc
            .tick_at(42, t0 + Duration::from_secs(10))
            .expect("publishable");
        assert_eq!(net, 42);

        // Mirror what the updater task does: bump the counter on a
        // positive delta.
        crate::metrics::S3_DOLLARS_SAVED_NET_TOTAL
            .with_label_values(&[region])
            .inc_by(net as u64);

        // Re-read the counter and assert the region-labelled child
        // observed exactly the bump we issued.
        let observed = crate::metrics::S3_DOLLARS_SAVED_NET_TOTAL
            .with_label_values(&[region])
            .get();
        assert_eq!(observed, 42, "region-labelled child must carry our bump");
    }

    #[test]
    fn dollars_per_hour_conversion_handles_edge_cases() {
        // Sanity: 5.18 $/hr → ~143_889 µ¢/sec.
        let v = dollars_per_hour_to_micro_cents_per_sec(5.18);
        assert!(
            (143_886..=143_891).contains(&v),
            "5.18 $/hr should convert to ~143_889 µ¢/sec; got {v}",
        );
        // Defensive paths: NaN, Inf, negative all clamp to 0.
        assert_eq!(dollars_per_hour_to_micro_cents_per_sec(f64::NAN), 0);
        assert_eq!(dollars_per_hour_to_micro_cents_per_sec(f64::INFINITY), 0);
        assert_eq!(dollars_per_hour_to_micro_cents_per_sec(-1.0), 0);
        assert_eq!(dollars_per_hour_to_micro_cents_per_sec(0.0), 0);
    }
}
