//! SHELF-29 — Independent-queue admission rate-limiter.
//!
//! ## Why this module exists
//!
//! [`crate::lodc_backpressure`] (SHELF-21e) provides a *level-based* gate
//! over the LODC submit queue: it drops new admissions when the in-flight
//! byte budget already exceeds 80% of Foyer's `submit_queue_size_threshold`.
//! Under a chronic burst envelope (~700 admit/s of ~4 MiB rowgroups,
//! observed on `1.0.0-rc.3` rep-1/rep-2 traffic) that gate trips, but
//! during the few hundred milliseconds it takes the level to *climb* to
//! the watermark, the cluster will still admit ~2 GiB of bytes that flow
//! into Foyer's submit queue + DRAM cache + in-flight S3 GET buffers all
//! at once. Worst-case RSS crosses the kubelet 27.3 GiB ceiling on the
//! `alluxio` NodePool's `m6a/m5a/m7a/c6a 4xlarge` instances and the kernel
//! kills the pod (`shelf-0` 11:32 UTC and `shelf-2` 12:05 UTC, 2026-04-29).
//!
//! This module bounds the *rate* of admissions feeding `cache.insert`
//! independently of the in-flight level. It is a token-bucket sized in
//! bytes:
//!
//! - Bucket capacity = `max_burst_bytes` (default 256 MiB — sized to hold
//!   ≈ 64 × 4 MiB rowgroups, the largest legitimate burst we observed in
//!   rc.2/rc.3 traces).
//! - Refill rate = `target_bytes_per_sec` (default 200 MiB/s — a hair below
//!   sustained EBS gp3 drain on the alluxio NodePool, which leaves headroom
//!   for parallel reads and the LODC flushers).
//!
//! ## Independence from the read path
//!
//! Workspace policy (post-2026-04-28 chaos window):
//!
//! > Foyer 0.12 `RateLimitPicker` is NOT a safe back-pressure knob — it
//! > shares a submit queue with the read path, so at 100 MiB/s `hit_disk`
//! > p99 pegs at the 16.384 s histogram-max bucket while writes are
//! > throttled. Any future admission rate-limiter must use a queue
//! > independent of reads.
//!
//! [`LodcAdmission::try_admit`] is a synchronous `fn`. It uses one
//! [`std::sync::atomic::AtomicU64`] to encode (epoch_ms, available_tokens)
//! packed into 64 bits and updates it via `compare_exchange_weak`. No
//! `tokio::Semaphore`, no channel `send`, no `await` — there is provably
//! no point at which the read path can block on this limiter.
//!
//! ## Two gates, three reasons
//!
//! The drop counter [`crate::metrics::LODC_DROPS_TOTAL`] gains a `reason`
//! label as part of this ticket. Existing call sites in
//! `lodc_backpressure` are migrated to label drops as
//! `"submit_queue_overflow"`; SHELF-29 drops are labelled `"rate_limit"`.
//! Dashboards and alerts that ignore the label keep working unchanged.
//!
//! ## What dropping costs us
//!
//! Identical trade-off to SHELF-21e: a dropped admission means the
//! triggering read still completes (bytes flow from origin → caller); the
//! cache simply doesn't cache them. The next request for that key takes
//! another origin trip. ADR-0009 § "Eviction" already accepts cache miss
//! next time as the fallback for any disk-side failure mode.
//!
//! ## Emergency rollback
//!
//! `SHELFD_LODC_ADMISSION=off` (handled in [`crate::config`]) sets
//! `enabled = false`, after which [`LodcAdmission::try_admit`] always
//! returns `true` — the gate becomes a no-op without a redeploy.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::{LodcAdmissionConfig, RssThrottleConfig};

/// Sentinel value stored in [`RssThrottle::current_rss`] until the
/// poller has produced its first reading. Picked as `u64::MAX` so the
/// "RSS unknown ⇒ no throttle" branch in [`RssThrottle::multiplier_bps`]
/// is a single integer compare on the hot path; any plausible real
/// VmRSS is multiple orders of magnitude smaller.
const RSS_UNKNOWN: u64 = u64::MAX;

/// "No throttle applied" — multiplier is `1.0` in basis points.
const MULT_BPS_FULL: u32 = 10_000;

/// "Full pause" — multiplier is `0.0` in basis points.
const MULT_BPS_NONE: u32 = 0;

/// Token bucket capacity below which the limiter degenerates to the
/// "always drop" kill-switch path. Used by the
/// [`LodcAdmission::try_admit`] short-circuit when an operator
/// intentionally configures `target_bytes_per_sec = 0` to disable
/// caching from the rowgroup pool.
const ZERO_RATE_KILL_SWITCH: u64 = 0;

/// **A1 (rc.7)** — RSS-aware admission multiplier layered on top of
/// the SHELF-29 byte-rate limiter.
///
/// ## Why this exists
///
/// The 2026-04-30 → 2026-05-01 c6a OOM cascade (workspace memory:
/// "May 1 morning OOM cascade RCA") showed the byte-rate gate alone
/// cannot stop a pod whose **process RSS** is climbing because of
/// inflight S3 buffers, Foyer DRAM, and the LODC submit queue all
/// expanding at once. The kernel kills the pod (exit 137) long
/// before the byte-rate budget would naturally throttle, because
/// the byte budget is sized for *steady-state drain*, not for the
/// transient bloat that happens during an admit burst.
///
/// This struct closes the loop: a periodic background poll of
/// `/proc/self/status` feeds the most recent RSS into a single
/// `AtomicU64`, and `multiplier_bps()` returns a linear-interpolated
/// throttle value the admission gate multiplies against its admit
/// decision.
///
/// ## Curve
///
/// Let `pressure = current_rss / rss_target_bytes`.
///
/// ```text
/// pressure < low_watermark   ⇒ multiplier = 1.0   (no throttle)
/// pressure >= high_watermark ⇒ multiplier = 0.0   (full pause)
/// otherwise                  ⇒ multiplier linearly interpolated
///                               from 1.0 at low_watermark
///                               to   0.0 at high_watermark
/// ```
///
/// Defaults: `low = 0.7`, `high = 0.9`, `rss_target_bytes = 40 GiB`
/// (matches the rc.7 m5a.4xlarge / 40 GiB-pod-limit baseline). At
/// 28 GiB RSS the multiplier starts dropping; by 36 GiB it is at
/// zero. The 4 GiB headroom between full-pause and the kubelet
/// allocatable ceiling absorbs the inflight buffers that are
/// already in flight when the multiplier first reaches zero.
///
/// ## Fail-open posture
///
/// `read_proc_rss` returns `None` on non-Linux hosts, container
/// `procfs` masks, or any I/O failure on `/proc/self/status`.
/// In that case `current_rss` stays at [`RSS_UNKNOWN`] and
/// [`multiplier_bps`] returns [`MULT_BPS_FULL`]. The throttle
/// silently degrades to a no-op rather than spuriously paging
/// admits — workspace policy: no throttle is preferable to a
/// throttle of unknown provenance.
///
/// ## Concurrency
///
/// One `AtomicU64` for the freshest RSS reading, one `AtomicBool`
/// for the master enable bit, and one `AtomicU64` for the
/// pressure-second integration anchor. No locks. The poller is a
/// detached `tokio::spawn`'d interval task that holds an
/// `Arc<RssThrottle>`; it self-exits when the Arc reaches the
/// last reference and `Weak::upgrade` returns `None`. There is no
/// `CancellationToken` plumbing because `FoyerStore` lives for
/// the entire process lifetime and the poller cost is negligible.
#[derive(Debug)]
pub struct RssThrottle {
    /// Master switch sourced from
    /// [`RssThrottleConfig::enabled`]. Read on every multiplier
    /// query; flipping it at runtime via `set_enabled` is allowed
    /// for tests but not used in production.
    enabled: AtomicBool,
    /// Reference RSS — `pressure = current_rss / target_bytes`.
    /// Static after construction.
    target_bytes: u64,
    /// Pressure (in basis points, `0..=10_000`) at and below which
    /// the multiplier is `1.0`. Computed from the f64
    /// [`RssThrottleConfig::low_watermark`] at construction.
    low_watermark_bps: u32,
    /// Pressure (in basis points) at and above which the
    /// multiplier is `0.0` (full pause). Computed from
    /// [`RssThrottleConfig::high_watermark`].
    high_watermark_bps: u32,
    /// Latest RSS reading, in bytes. Initialised to [`RSS_UNKNOWN`]
    /// so the multiplier returns `1.0` until the first poll.
    current_rss: AtomicU64,
    /// Stable Prometheus pool label, e.g. `"rowgroup"`.
    pool_label: &'static str,
    /// Pre-touch latch for the multiplier gauge. Bumps a one-shot
    /// `set(MULT_BPS_FULL)` on first construction so dashboards
    /// see a non-empty series before the first poll fires.
    initialised: AtomicBool,
}

impl RssThrottle {
    /// Construct from the operator-facing config. The `pool_label`
    /// is the Prometheus label this throttle's metrics are filed
    /// under; today the only caller passes `"rowgroup"`.
    pub fn from_config(cfg: &RssThrottleConfig, pool_label: &'static str) -> Self {
        // Convert the f64 watermarks to basis points. The clamp is
        // belt-and-braces against a misconfigured chart that sets
        // `low_watermark: 1.5` (pressure can't exceed 1.0 in any
        // sane regime, but if an operator does pick 1.5 we want
        // to floor to 1.0 = no throttle, not crash).
        let low_bps = (cfg.low_watermark.clamp(0.0, 1.0) * 10_000.0) as u32;
        // The high watermark must be ≥ low_watermark or the linear
        // band has zero width and we'd divide by zero. We clamp the
        // configured high up to at least `low + 1 bps` so the
        // arithmetic in `multiplier_bps` is always well-defined.
        let high_raw = (cfg.high_watermark.clamp(0.0, 1.0) * 10_000.0) as u32;
        let high_bps = high_raw.max(low_bps.saturating_add(1));

        let me = Self {
            enabled: AtomicBool::new(cfg.enabled),
            target_bytes: cfg.rss_target_bytes,
            low_watermark_bps: low_bps,
            high_watermark_bps: high_bps,
            current_rss: AtomicU64::new(RSS_UNKNOWN),
            pool_label,
            initialised: AtomicBool::new(false),
        };
        me.touch_metric_init();
        me
    }

    /// Pre-touch the gauge child for `pool_label` so a freshly
    /// scraped pod that has never seen pressure publishes the
    /// series at `MULT_BPS_FULL` rather than missing.
    fn touch_metric_init(&self) {
        if !self.initialised.swap(true, Ordering::Relaxed) {
            crate::metrics::LODC_RSS_THROTTLE_MULTIPLIER
                .with_label_values(&[self.pool_label])
                .set(MULT_BPS_FULL as i64);
            crate::metrics::LODC_RSS_PRESSURE_SECONDS_TOTAL
                .with_label_values(&[self.pool_label])
                .inc_by(0);
        }
    }

    /// Enable / disable the throttle at runtime. Used by tests; the
    /// production path sets this through
    /// [`RssThrottleConfig::enabled`].
    #[cfg(test)]
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Update the cached RSS and refresh the multiplier gauge.
    /// Called by the poller (and directly by tests).
    pub fn record_rss(&self, rss_bytes: u64) {
        self.current_rss.store(rss_bytes, Ordering::Relaxed);
        crate::metrics::LODC_RSS_THROTTLE_MULTIPLIER
            .with_label_values(&[self.pool_label])
            .set(self.multiplier_bps() as i64);
    }

    /// Current admission multiplier in basis points (`0..=10_000`).
    /// Pure function of the latest RSS reading; cheap to call from
    /// the hot path.
    pub fn multiplier_bps(&self) -> u32 {
        if !self.enabled.load(Ordering::Relaxed) {
            return MULT_BPS_FULL;
        }
        let rss = self.current_rss.load(Ordering::Relaxed);
        if rss == RSS_UNKNOWN || self.target_bytes == 0 {
            // Fail-open: on non-Linux, container restrictions, or
            // a degenerate config we leave the gate fully open so
            // the daemon does NOT spuriously throttle admits. Any
            // operator who wants a hard pause can flip
            // `cache.pools.rowgroup.diskCache.admission.enabled` to
            // `false`; this throttle is a *softening* layer.
            return MULT_BPS_FULL;
        }
        // Compute pressure in basis points. `rss / target_bytes`
        // is conceptually a fraction; we represent it as bps so
        // the comparisons against `low_watermark_bps` /
        // `high_watermark_bps` stay in integer arithmetic. The
        // saturating_mul guards against the (impossible-in-prod)
        // case where `rss * 10_000` overflows u64 — for that to
        // happen `rss` would need to exceed `u64::MAX / 10_000`
        // (≈ 1.8 × 10^15 bytes ≈ 1.6 PB), well beyond any pod's
        // RSS — but the saturation keeps the function total.
        let pressure_bps =
            ((rss.saturating_mul(10_000)) / self.target_bytes).min(u32::MAX as u64) as u32;
        if pressure_bps < self.low_watermark_bps {
            return MULT_BPS_FULL;
        }
        if pressure_bps >= self.high_watermark_bps {
            return MULT_BPS_NONE;
        }
        // Linear interpolation in the throttle band:
        //   above = pressure - low
        //   width = high - low
        //   mult  = FULL * (1 - above/width)
        //         = FULL - FULL * above / width
        // The `width >= 1` invariant is enforced by `from_config`
        // (high is clamped to at least low + 1), so the divide
        // is never zero.
        let above = pressure_bps - self.low_watermark_bps;
        let width = self.high_watermark_bps - self.low_watermark_bps;
        let drop = (u64::from(above) * u64::from(MULT_BPS_FULL)) / u64::from(width);
        let mult = u64::from(MULT_BPS_FULL).saturating_sub(drop);
        // The arithmetic above keeps `mult` in `[0, FULL]`, so
        // truncating to u32 is safe.
        mult as u32
    }

    /// Read `/proc/self/status` and return the `VmRSS:` value in
    /// bytes. `None` on any failure — non-Linux host, sandboxed
    /// procfs, transient I/O error, or kernel-format change.
    pub fn read_proc_rss() -> Option<u64> {
        let data = std::fs::read_to_string("/proc/self/status").ok()?;
        Self::parse_vmrss(&data)
    }

    /// Parse the `VmRSS:` line out of an in-memory `/proc/self/status`
    /// blob. Split out as a free function so the test suite can
    /// exercise the parser without needing a real `/proc` mount.
    fn parse_vmrss(s: &str) -> Option<u64> {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                // The procfs format is `VmRSS:    1234 kB`. We pick
                // the first whitespace-separated token after the
                // colon and parse it as decimal kibibytes.
                let kib_str = rest.split_whitespace().next()?;
                let kib: u64 = kib_str.parse().ok()?;
                return Some(kib.saturating_mul(1024));
            }
        }
        None
    }

    /// Read RSS once and update the cached value. No-op when the
    /// host doesn't expose `/proc/self/status`.
    pub fn poll_once(&self) {
        if let Some(rss) = Self::read_proc_rss() {
            self.record_rss(rss);
        }
    }

    /// Spawn a tokio interval task that polls RSS every
    /// `interval`. The task self-exits when the supplied
    /// `Arc<RssThrottle>` is dropped (last clone goes away).
    /// Production sites pass an `Arc` that the owning
    /// `FoyerStore` keeps alive for the entire process lifetime;
    /// in tests we exit when the test scope drops the Arc.
    ///
    /// Takes the Arc by value (not `&Arc<Self>`) because Rust's
    /// stable receiver-type list does not include `&Arc<Self>`.
    /// Internally we downgrade to a `Weak` immediately so the
    /// task does not extend the throttle's lifetime past its
    /// owning `LodcAdmission`.
    pub fn spawn_poller(arc: Arc<Self>, interval: Duration) {
        let weak = Arc::downgrade(&arc);
        let pool_label = arc.pool_label;
        // Run an initial sync poll so the dashboard shows a real
        // value within milliseconds of pod start, rather than
        // staying at the pre-touch sentinel until the first
        // interval tick fires `interval` seconds later.
        arc.poll_once();
        // Drop our own strong ref so the spawned task keeps a
        // `Weak` only — the LodcAdmission's Arc remains the sole
        // strong reference and the task self-exits when that
        // Arc drops at process exit.
        drop(arc);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skip missed ticks rather than firing a burst on
            // recovery; the poll is a snapshot, not an integral.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Tokio's first tick fires immediately; consume it so
            // the loop body starts sleeping.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let throttle = match weak.upgrade() {
                    Some(t) => t,
                    None => {
                        tracing::debug!(
                            target: "shelfd::rss_throttle",
                            pool = %pool_label,
                            "owner dropped; rss poller exiting",
                        );
                        return;
                    }
                };
                throttle.poll_once();
                // Pressure-seconds integrator: any tick where the
                // multiplier is below `MULT_BPS_FULL` counts as
                // "throttle was active for this poll interval".
                if throttle.multiplier_bps() < MULT_BPS_FULL {
                    crate::metrics::LODC_RSS_PRESSURE_SECONDS_TOTAL
                        .with_label_values(&[throttle.pool_label])
                        .inc_by(interval.as_secs());
                }
            }
        });
    }
}

/// Independent-queue token-bucket admission limiter.
///
/// Cheap to construct; lock-free at steady state (one atomic load + one
/// CAS on the hot path, plus a clock read). Held inside `FoyerStore`
/// behind an `Option` — `None` means the rowgroup pool is DRAM-only and
/// there is no LODC to gate, OR the operator turned the limiter off
/// via `lodc.admission.enabled = false`.
#[derive(Debug)]
pub struct LodcAdmission {
    /// `target_bytes_per_sec`. Static after construction. A value of `0`
    /// is the "kill switch" path: every call drops and increments
    /// `shelf_lodc_drops_total{reason="rate_limit"}`.
    refill_bytes_per_sec: u64,
    /// `max_burst_bytes`. Static after construction. The bucket can hold
    /// at most this many tokens regardless of how long it has been since
    /// the last consume.
    max_burst_bytes: u64,
    /// Optional secondary safety: a coarse counter of concurrent
    /// admissions. Maintained for forward compatibility; the byte budget
    /// is the dominant gate and this counter rarely binds in production
    /// under defaults.
    #[allow(dead_code)]
    max_inflight_admissions: u64,
    /// Packed `(epoch_ms_lo32, tokens_remaining_u32)` — see
    /// [`pack_state`] / [`unpack_state`]. CAS'd on every `try_admit`.
    /// Atomic so reads and writes from many tasks never tear.
    state: AtomicU64,
    /// Wall-clock anchor for converting `Instant` deltas into the packed
    /// `epoch_ms_lo32` field. Set once at construction.
    start: Instant,
    /// Stable Prometheus pool label, e.g. `"rowgroup"`. Held as a
    /// `&'static str` so each metric increment skips a clone.
    pool_label: &'static str,
    /// Pre-touch guard so the metric child for the `"rate_limit"` reason
    /// label is registered before the first scrape — prevents the panel
    /// from reading "no data" until the first drop fires.
    initialised: AtomicBool,
    /// **A1 (rc.7)** — RSS-aware throttle. `None` when the operator
    /// disabled the feature via `rss_throttle.enabled = false`. The
    /// `Arc` lets the background poller hold its own ref without
    /// forcing every `LodcAdmission` clone to copy the throttle
    /// state.
    rss_throttle: Option<Arc<RssThrottle>>,
    /// **A1 (rc.7)** — process-wide PRNG state for the
    /// probabilistic `rss_admit` decision. Held inside the limiter
    /// (rather than as a `static`) so test-only limiters with
    /// distinct labels do not race on a single global. Initialised
    /// at construction from a constant XOR'd with the pool label
    /// pointer; the splitmix64 mixer in [`rss_admit`] then produces
    /// a uniformly distributed u32 per call.
    rng_state: AtomicU64,
}

impl LodcAdmission {
    /// Construct from the operator-facing config. Returns `None` when
    /// the operator has disabled the limiter (or set zero refill **and**
    /// zero burst, which would be a misconfiguration that should also
    /// disable the gate rather than wedge every admit). Returning an
    /// `Option` keeps the `FoyerStore::open` site simple.
    pub fn from_config(cfg: &LodcAdmissionConfig, pool_label: &'static str) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }
        // Cap burst at `u32::MAX` because the packed state encodes
        // tokens in a u32 to save a second atomic. 4 GiB of burst is far
        // more than any sane production sizing (the default is 256 MiB)
        // and the cap is reached only when an operator picks an
        // unreasonably large value; we silently clamp rather than panic.
        let max_burst_bytes = cfg.max_burst_bytes.min(u32::MAX as u64);
        // A1 — install the RSS-aware throttle when enabled. The
        // throttle is independent of the `cfg.enabled` master switch
        // for the byte-rate limiter (operators can ship the byte
        // gate enabled but the RSS feedback disabled — useful while
        // tuning a new cluster), but a disabled byte limiter takes
        // the whole gate down to begin with so the throttle is
        // moot in that case.
        let rss_throttle = if cfg.rss_throttle.enabled {
            Some(Arc::new(RssThrottle::from_config(
                &cfg.rss_throttle,
                pool_label,
            )))
        } else {
            // Pre-touch the multiplier gauge at FULL even when the
            // feature is disabled so a freshly deployed pod that
            // never touches the throttle still publishes the series.
            crate::metrics::LODC_RSS_THROTTLE_MULTIPLIER
                .with_label_values(&[pool_label])
                .set(MULT_BPS_FULL as i64);
            crate::metrics::LODC_RSS_PRESSURE_SECONDS_TOTAL
                .with_label_values(&[pool_label])
                .inc_by(0);
            None
        };
        // Seed the PRNG with a value that varies between processes
        // and between distinct limiters in the same process. The
        // pool-label pointer is a stable per-build address; the
        // boot-time nanos give cross-restart variance.
        let pool_seed = pool_label.as_ptr() as usize as u64;
        let nanos_seed = Instant::now().elapsed().as_nanos() as u64;
        let rng_seed =
            pool_seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ nanos_seed ^ 0xDEAD_BEEF_CAFE_BABE;
        Some(Self {
            refill_bytes_per_sec: cfg.target_bytes_per_sec,
            max_burst_bytes,
            max_inflight_admissions: cfg.max_inflight_admissions,
            state: AtomicU64::new(pack_state(0, max_burst_bytes as u32)),
            start: Instant::now(),
            pool_label,
            initialised: AtomicBool::new(false),
            rss_throttle,
            rng_state: AtomicU64::new(rng_seed),
        })
    }

    /// **A1 (rc.7)** — return the cloned `Arc` to the RSS throttle,
    /// or `None` when the feature is disabled. Used by `FoyerStore`
    /// to spawn the background poller after construction.
    pub fn rss_throttle(&self) -> Option<Arc<RssThrottle>> {
        self.rss_throttle.clone()
    }

    /// Test-only constructor with explicit (refill, burst) values. The
    /// `pool_label` is required so the test can assert the exact
    /// counter row was incremented.
    #[cfg(test)]
    pub fn new(refill_bytes_per_sec: u64, max_burst_bytes: u64, pool_label: &'static str) -> Self {
        let max_burst_bytes = max_burst_bytes.min(u32::MAX as u64);
        Self {
            refill_bytes_per_sec,
            max_burst_bytes,
            max_inflight_admissions: u64::MAX,
            state: AtomicU64::new(pack_state(0, max_burst_bytes as u32)),
            start: Instant::now(),
            pool_label,
            initialised: AtomicBool::new(false),
            rss_throttle: None,
            rng_state: AtomicU64::new(0xDEAD_BEEF_CAFE_BABE),
        }
    }

    /// Test-only: install an `Arc<RssThrottle>` after construction
    /// so the test scope can drive `record_rss(...)` directly. Not
    /// exposed in production because production limiters install
    /// the throttle in [`from_config`].
    #[cfg(test)]
    pub fn with_rss_throttle(mut self, throttle: Arc<RssThrottle>) -> Self {
        self.rss_throttle = Some(throttle);
        self
    }

    /// Configured refill rate in bytes/sec. Test-only accessor.
    #[cfg(test)]
    pub fn refill_bytes_per_sec(&self) -> u64 {
        self.refill_bytes_per_sec
    }

    /// Configured burst capacity in bytes. Test-only accessor.
    #[cfg(test)]
    pub fn max_burst_bytes(&self) -> u64 {
        self.max_burst_bytes
    }

    /// Configured burst capacity, exposed to `FoyerStore::open` so it
    /// can pre-touch the burst-capacity gauge with the static value
    /// chosen at construction. Distinct from the `#[cfg(test)]`
    /// accessor above so the production binary keeps the field
    /// public-read but not publicly mutable.
    pub fn max_burst_bytes_for_init(&self) -> u64 {
        self.max_burst_bytes
    }

    /// Decide whether `entry_bytes` of admission should proceed.
    ///
    /// Returns `true` to admit (caller proceeds with `cache.insert`),
    /// `false` to drop (caller skips the insert; counter is incremented
    /// here, no further counter bump needed by caller).
    ///
    /// **Synchronous, non-blocking**: one atomic load + one CAS retry
    /// loop. No `await`, no `Mutex`, no channel send. The retry loop
    /// terminates because the CAS only fails when another thread won
    /// the race; in steady state this happens at most a handful of
    /// times under contention.
    ///
    /// Side effects:
    /// - Updates `shelf_lodc_admit_tokens_available{pool}` gauge to the
    ///   post-admit (or post-failed-admit) token count, so dashboards
    ///   see live signal even when no drops fire.
    /// - On reject: increments
    ///   `shelf_lodc_drops_total{pool, reason="rate_limit"}` exactly once.
    pub fn try_admit(&self, entry_bytes: u64) -> bool {
        // Pre-touch the rate-limit drop child the first time we're
        // called so Prometheus exposes the row before the first actual
        // drop. `compare_exchange` is overkill for a one-shot init — a
        // relaxed swap is fine because the worst case is two
        // pre-touches (idempotent inc_by(0)).
        if !self.initialised.swap(true, Ordering::Relaxed) {
            crate::metrics::LODC_DROPS_TOTAL
                .with_label_values(&[self.pool_label, "rate_limit"])
                .inc_by(0);
        }

        // Kill-switch: zero-rate config disables admission entirely.
        // Treated as "drop everything" so the operator-facing signal
        // (drops climbing) matches the configured intent. Bump the
        // counter so dashboards still tell the story.
        if self.refill_bytes_per_sec == ZERO_RATE_KILL_SWITCH {
            crate::metrics::LODC_DROPS_TOTAL
                .with_label_values(&[self.pool_label, "rate_limit"])
                .inc();
            return false;
        }

        // Entries that don't fit the burst cap will never admit even
        // after a full refill. Drop immediately to avoid an infinite
        // CAS loop trying to acquire tokens that will never accumulate.
        if entry_bytes > self.max_burst_bytes {
            crate::metrics::LODC_DROPS_TOTAL
                .with_label_values(&[self.pool_label, "rate_limit"])
                .inc();
            return false;
        }

        // **A1 (rc.7)** — RSS-aware multiplier gate. Layered on top
        // of the byte-rate token bucket so a pod whose RSS is
        // climbing because of inflight buffers / DRAM growth /
        // submit-queue spill stops admitting new work even when its
        // *byte budget* still has slack. The byte budget alone
        // could not stop the c6a OOM cascade observed
        // 2026-04-30 → 2026-05-01 because the bytes-per-second
        // budget is sized for steady-state drain, not for the
        // transient bloat that happens during an admit burst.
        //
        // Mechanism (see `RssThrottle::multiplier_bps`):
        //   - mult == FULL  ⇒ no throttle (skip this gate)
        //   - mult == NONE  ⇒ drop unconditionally
        //   - 0 < mult < FULL ⇒ drop probabilistically with
        //     probability `1 - mult/FULL`
        //
        // The probabilistic path uses a splitmix64-mixed PRNG seeded
        // at construction. Tearing on `rng_state` is harmless — we
        // only need a uniformly distributed u32 per call, and any
        // CAS-vs-fetch_add race produces a different but still
        // uniform draw.
        if !self.rss_admit() {
            crate::metrics::LODC_DROPS_TOTAL
                .with_label_values(&[self.pool_label, "rate_limit"])
                .inc();
            return false;
        }

        // Bound the entry bytes to u32 so the packed-state arithmetic
        // can subtract without crossing the boundary. We already
        // validated `entry_bytes <= max_burst_bytes <= u32::MAX`.
        let want = entry_bytes as u32;

        loop {
            let snap = self.state.load(Ordering::Acquire);
            let (last_ms, tokens) = unpack_state(snap);

            // CRITICAL: `now_ms` is captured *inside* the loop, after
            // the state load. If we captured it once outside the loop
            // and another caller raced ahead and CAS'd a fresher
            // `last_ms` into state, our captured `now_ms - last_ms`
            // would underflow (wrap to a near-`u32::MAX` value) and
            // the refill computation would clamp `tokens` straight
            // back up to `max_burst_bytes` — effectively giving each
            // contended retry a fresh burst credit. Re-reading the
            // clock per loop turn keeps `now_ms >= last_ms` (modulo
            // the deliberately-tolerated 49.7-day wrap) and bounds
            // refill to the actual elapsed wall-clock.
            let now_ms = self.now_ms_lo32();
            let elapsed_ms = now_ms.wrapping_sub(last_ms) as u64;
            let refilled = saturating_mul_div(self.refill_bytes_per_sec, elapsed_ms, 1000);
            let new_tokens =
                ((tokens as u64).saturating_add(refilled)).min(self.max_burst_bytes) as u32;

            // Snapshot the post-refill token count to a gauge for
            // observability. We do this once per attempt rather than
            // once per CAS retry to keep the gauge close to the wall
            // clock without the overhead of a write per loop turn.
            crate::metrics::LODC_ADMIT_TOKENS_AVAILABLE
                .with_label_values(&[self.pool_label])
                .set(new_tokens as i64);

            if new_tokens < want {
                // Update state to reflect the refill so the next call
                // doesn't re-credit the same elapsed window. If the
                // CAS races with another caller we don't retry the
                // accounting — the other caller's update already
                // captured a consistent view.
                let next = pack_state(now_ms, new_tokens);
                let _ =
                    self.state
                        .compare_exchange(snap, next, Ordering::AcqRel, Ordering::Relaxed);
                crate::metrics::LODC_DROPS_TOTAL
                    .with_label_values(&[self.pool_label, "rate_limit"])
                    .inc();
                return false;
            }

            let next = pack_state(now_ms, new_tokens - want);
            match self
                .state
                .compare_exchange_weak(snap, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(_) => {
                    // Another caller raced us. The CAS-weak retry loop
                    // is the entire reason this is a rate-limiter and
                    // not a Mutex<TokenBucket> — no thread parks, no
                    // priority inversion, no read-path interference.
                    continue;
                }
            }
        }
    }

    /// **A1 (rc.7)** — apply the RSS-aware multiplier. Returns
    /// `true` to allow the admit through, `false` to drop. When the
    /// throttle is disabled (or absent), always returns `true`. The
    /// caller is responsible for bumping the drop counter on
    /// `false`; we keep that side effect at the call site so all
    /// rate-limit drops funnel through a single counter bump.
    fn rss_admit(&self) -> bool {
        let throttle = match &self.rss_throttle {
            Some(t) => t,
            None => return true,
        };
        let mult = throttle.multiplier_bps();
        if mult >= MULT_BPS_FULL {
            return true;
        }
        if mult == MULT_BPS_NONE {
            return false;
        }
        // Probabilistic admission: draw a uniform `0..=10_000` and
        // admit iff the draw is strictly less than `mult`. With
        // `mult = 5000` this admits ~50% of attempts, matching
        // the spec's "if 0.5, refuse 50% probabilistically" line.
        let draw = self.next_rng_bps();
        draw < mult
    }

    /// **A1 (rc.7)** — splitmix64-mixed pseudo-random draw in
    /// `[0, 10_001)`, suitable for per-call probabilistic admit
    /// decisions. NOT cryptographically random; we explicitly
    /// don't need that here.
    fn next_rng_bps(&self) -> u32 {
        // Weyl-sequence increment + splitmix64 mixer. The
        // increment constant is the golden-ratio fraction of 2^64;
        // the two multiplies are the canonical splitmix64 finalising
        // constants (Stafford 13 / 14). Lock-free, contention-safe,
        // and uniformly distributed for our use case.
        let z = self
            .rng_state
            .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
            .wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // `% 10_001` gives uniform draws in `[0, 10_000]`. The
        // tiny non-uniformity from u64 % 10_001 (≈ 10_001 / 2^64)
        // is several orders below detectability.
        (z % 10_001) as u32
    }

    /// Current monotonic-since-construction milliseconds, truncated to
    /// the lo 32 bits. Wraps every ≈ 49.7 days; the wrap is harmless
    /// because the only consumer is a delta computation (`wrapping_sub`).
    fn now_ms_lo32(&self) -> u32 {
        let elapsed = Instant::now().saturating_duration_since(self.start);
        // Cast through u128 so the multiply does not overflow during
        // the seconds → millis conversion for very long-lived pods.
        // Truncating to u32 is intentional and documented above.
        let ms = (elapsed.as_secs() as u128) * 1000 + (elapsed.subsec_nanos() as u128) / 1_000_000;
        (ms as u64) as u32
    }
}

/// `target * num / den`, saturating. Used to translate elapsed
/// milliseconds into refilled bytes without an intermediate `f64`
/// (which loses precision over long elapses) or an overflowing u64
/// multiply.
fn saturating_mul_div(target: u64, num: u64, den: u64) -> u64 {
    if den == 0 {
        return 0;
    }
    let prod = (target as u128).saturating_mul(num as u128);
    (prod / den as u128).min(u64::MAX as u128) as u64
}

/// Pack `(timestamp_ms_lo32, tokens_u32)` into a single `u64` for atomic
/// CAS. The hi 32 bits hold the timestamp.
fn pack_state(ts_ms: u32, tokens: u32) -> u64 {
    ((ts_ms as u64) << 32) | (tokens as u64)
}

/// Inverse of [`pack_state`].
fn unpack_state(s: u64) -> (u32, u32) {
    ((s >> 32) as u32, (s & 0xFFFF_FFFF) as u32)
}

/// Parse the `SHELFD_LODC_ADMISSION` env var into an enable/disable
/// override. Anything other than `off` / `0` / `false` (case-insensitive)
/// is treated as "no override" so a misconfigured value never silently
/// disables the production limiter.
pub fn env_disable_override() -> bool {
    match std::env::var("SHELFD_LODC_ADMISSION") {
        Ok(v) => {
            let trimmed = v.trim().to_ascii_lowercase();
            matches!(trimmed.as_str(), "off" | "0" | "false")
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    /// Helper: read the rate-limit drops counter for the given pool
    /// label. Each test uses a unique label so concurrent test runs
    /// do not poison each other's counter.
    fn rate_limit_drops(label: &str) -> u64 {
        crate::metrics::LODC_DROPS_TOTAL
            .with_label_values(&[label, "rate_limit"])
            .get()
    }

    /// Invariant (i): `try_admit` is synchronous, non-blocking, and
    /// completes in O(1) atomics regardless of bucket state. The
    /// strong half of the invariant — that the function is `fn`, not
    /// `async fn` — is enforced at compile time by storing the call
    /// in a non-Future binding below; if anyone changes the signature
    /// to `async fn` this test fails to compile.
    ///
    /// The wall-clock half is a soft sanity check: 10k calls must
    /// complete fast enough that no possible blocking/parking
    /// implementation could have squeezed under the bar. We pick
    /// 1 second as the bound — generous enough for QEMU-emulated CI
    /// runners (where 10k atomic ops take ~150 ms even on cold cache),
    /// strict enough that any accidental `Mutex` or channel send
    /// would blow the budget by orders of magnitude.
    #[test]
    fn invariant_i_read_path_never_blocks() {
        let lim = LodcAdmission::new(1 << 30, 1 << 20, "test_inv_i");
        // Compile-time witness: `try_admit` returns `bool`, not a
        // `Future`. Replacing this binding with `.await` would fail
        // to compile, which is the load-bearing half of the
        // invariant.
        let _b: bool = lim.try_admit(4096);

        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let _ = lim.try_admit(4096);
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "10k try_admit calls must complete in <1s; got {elapsed:?}",
        );
    }

    /// Invariant (ii): under sustained ingress at rates well above
    /// `target_bytes_per_sec`, the limiter caps admitted bytes at the
    /// target rate within ±10%. The test runs for 500 ms at high
    /// ingress and asserts the admitted byte total is bounded by
    /// `target × 0.55s + max_burst_bytes` (the burst credit at start).
    #[test]
    fn invariant_ii_sustained_load_caps_at_target_rate() {
        // 10 MiB/s target, 1 MiB burst.
        let target_bps: u64 = 10 * 1024 * 1024;
        let burst: u64 = 1024 * 1024;
        let lim = LodcAdmission::new(target_bps, burst, "test_inv_ii");

        let entry: u64 = 4096;
        let deadline = Instant::now() + Duration::from_millis(500);
        let mut admitted: u64 = 0;
        let mut total_attempts: u64 = 0;
        while Instant::now() < deadline {
            for _ in 0..1000 {
                if lim.try_admit(entry) {
                    admitted += entry;
                }
                total_attempts += 1;
            }
        }

        // Headroom: target × 0.55s (10% over 0.5s test window) + full
        // burst credit at start. The limiter's accounting is
        // millisecond-resolution and saturating, so this is the tight
        // upper bound that any correct token bucket must respect.
        let upper_bound = saturating_mul_div(target_bps, 550, 1000) + burst;
        assert!(
            admitted <= upper_bound,
            "sustained load must cap at target rate; admitted={admitted} bound={upper_bound} attempts={total_attempts}",
        );
        // Sanity: we should have admitted *something* — otherwise the
        // test is vacuously true and the limiter could be dropping
        // every request, hiding a different bug.
        assert!(admitted > 0, "expected some admissions, got 0");
    }

    /// Invariant (iii): a burst up to the bucket capacity admits in
    /// full. The token bucket starts full at construction; consuming
    /// `max_burst_bytes` worth of admissions back-to-back must all
    /// succeed before a single drop fires.
    #[test]
    fn invariant_iii_burst_within_capacity_admits_fully() {
        let target_bps: u64 = 1_000_000_000;
        let burst: u64 = 1024 * 1024;
        let lim = LodcAdmission::new(target_bps, burst, "test_inv_iii");

        let entry: u64 = 4096;
        let baseline_drops = rate_limit_drops("test_inv_iii");
        let n = burst / entry;
        for i in 0..n {
            assert!(
                lim.try_admit(entry),
                "burst within capacity must admit fully; failed at {i}/{n}",
            );
        }
        assert_eq!(
            rate_limit_drops("test_inv_iii"),
            baseline_drops,
            "no drops must fire while bucket has capacity",
        );
    }

    /// Invariant (iv): a healthy steady state (calls spaced widely
    /// enough for tokens to refill in between) is a no-op — every
    /// call admits and the drop counter never moves.
    #[test]
    fn invariant_iv_healthy_capacity_is_a_noop() {
        // 100 MiB/s target, 10 MiB burst. With one 4 KiB request per
        // millisecond we only consume 4 MiB/s — well under target — so
        // every call admits.
        let lim = LodcAdmission::new(100 * 1024 * 1024, 10 * 1024 * 1024, "test_inv_iv");
        let baseline_drops = rate_limit_drops("test_inv_iv");

        for _ in 0..50 {
            assert!(lim.try_admit(4096), "healthy steady state must admit");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            rate_limit_drops("test_inv_iv"),
            baseline_drops,
            "healthy capacity must produce zero drops",
        );
    }

    /// Edge case: zero rate is the kill-switch. Every call drops; no
    /// silent admit on a misconfigured operator value. The drop
    /// counter must increment exactly once per attempt.
    #[test]
    fn edge_zero_rate_drops_every_call() {
        let lim = LodcAdmission::new(0, 1024 * 1024, "test_edge_zero_rate");
        let baseline_drops = rate_limit_drops("test_edge_zero_rate");
        for _ in 0..10 {
            assert!(!lim.try_admit(4096), "zero rate must drop");
        }
        assert_eq!(
            rate_limit_drops("test_edge_zero_rate") - baseline_drops,
            10,
            "drop counter must tick once per attempt under zero rate",
        );
    }

    /// Edge case: an entry larger than `max_burst_bytes` cannot ever
    /// fit the bucket, so the limiter drops immediately rather than
    /// CAS-looping waiting for tokens that never accumulate to the
    /// required size.
    #[test]
    fn edge_entry_too_large_drops_without_loop() {
        let lim = LodcAdmission::new(1 << 30, 1024, "test_edge_too_large");
        let baseline_drops = rate_limit_drops("test_edge_too_large");
        // 4 KiB request, 1 KiB bucket — entry > burst, so the limiter
        // takes the "always drop" short-circuit.
        let start = Instant::now();
        assert!(!lim.try_admit(4096));
        assert!(
            start.elapsed() < Duration::from_millis(5),
            "oversized-entry path must short-circuit, not CAS-loop",
        );
        assert_eq!(rate_limit_drops("test_edge_too_large") - baseline_drops, 1,);
    }

    /// The CAS retry loop must be safe under concurrent admission
    /// attempts from many threads. We fire 8 threads, each issuing
    /// 1000 admit attempts at a 4 KiB entry size; the limiter must
    /// not admit more bytes than `(burst + refill_during_test)`
    /// allows. The test bound is computed from the *measured* test
    /// duration so it stays correct on slow CI runners (notably
    /// QEMU-emulated amd64 on aarch64 hosts, which can stretch a
    /// "should be <50 ms" loop to several seconds).
    #[test]
    fn cas_retry_safe_under_contention() {
        let target_bps: u64 = 1024 * 1024;
        let burst: u64 = 1024 * 1024;
        let lim = Arc::new(LodcAdmission::new(target_bps, burst, "test_cas_contention"));
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let lim = lim.clone();
            handles.push(thread::spawn(move || {
                let mut local_admits = 0u64;
                for _ in 0..1000 {
                    if lim.try_admit(4096) {
                        local_admits += 1;
                    }
                }
                local_admits
            }));
        }
        let total_admits: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed();

        // Expected upper bound = burst credit + refill during test +
        // 50% slack for clock-resolution rounding and the "extra
        // credit" the limiter is allowed to give out across the
        // refill boundary. Sized in bytes for fidelity; converted to
        // entry count at the end.
        let elapsed_ms = elapsed.as_millis() as u64;
        let refill_during_test = saturating_mul_div(target_bps, elapsed_ms, 1000);
        let bytes_bound = (burst + refill_during_test) * 3 / 2;
        let admits_bound = bytes_bound / 4096;
        assert!(
            total_admits <= admits_bound,
            "CAS retry loop must respect rate budget; got {total_admits} admits over {elapsed:?}, bound {admits_bound}",
        );
        // Sanity: at least the burst capacity must have been
        // admitted, otherwise the test is vacuously true.
        assert!(
            total_admits >= burst / 4096 / 2,
            "expected at least burst-worth of admits; got {total_admits}",
        );
    }

    /// SHELFD_LODC_ADMISSION env override parsing — `off`, `0`, and
    /// `false` (case-insensitive) all disable; everything else leaves
    /// the configured `enabled` value untouched.
    #[test]
    fn env_override_disables_only_on_known_falsy() {
        let cases = [
            ("off", true),
            ("OFF", true),
            ("0", true),
            ("false", true),
            ("FALSE", true),
            ("on", false),
            ("1", false),
            ("true", false),
            ("garbage", false),
        ];
        for (val, want) in cases {
            // SAFETY: env var writes are unsafe in 2024 edition; the
            // project norm is to scope them to per-test names. We use
            // the same canonical name because the production reader
            // reads exactly that.
            unsafe {
                std::env::set_var("SHELFD_LODC_ADMISSION", val);
            }
            assert_eq!(
                env_disable_override(),
                want,
                "env override mismatch for value {val:?}",
            );
        }
        unsafe {
            std::env::remove_var("SHELFD_LODC_ADMISSION");
        }
        assert!(!env_disable_override(), "absent env must not disable");
    }

    /// `from_config` returns `None` when `enabled = false` — `FoyerStore`
    /// uses the `Option` to short-circuit the gate without an extra
    /// branch on the hot path.
    #[test]
    fn from_config_returns_none_when_disabled() {
        let cfg = LodcAdmissionConfig {
            enabled: false,
            target_bytes_per_sec: 1 << 20,
            max_burst_bytes: 1 << 20,
            max_inflight_admissions: 1024,
            rss_throttle: crate::config::RssThrottleConfig::default(),
        };
        assert!(LodcAdmission::from_config(&cfg, "test_disabled").is_none());
    }

    /// `from_config` clamps `max_burst_bytes` to `u32::MAX` because the
    /// packed state encodes tokens in 32 bits. Operator-facing values
    /// above 4 GiB are silently capped rather than rejected so a
    /// misconfigured chart does not crash the daemon at boot.
    #[test]
    fn from_config_clamps_oversized_burst() {
        let cfg = LodcAdmissionConfig {
            enabled: true,
            target_bytes_per_sec: 1 << 20,
            max_burst_bytes: u64::MAX,
            max_inflight_admissions: 1024,
            rss_throttle: crate::config::RssThrottleConfig::default(),
        };
        let lim = LodcAdmission::from_config(&cfg, "test_clamp")
            .expect("enabled config must produce a limiter");
        assert_eq!(lim.max_burst_bytes(), u32::MAX as u64);
    }

    /// `pack_state` / `unpack_state` round-trip without loss. Cheap
    /// regression guard against accidentally swapping the hi/lo halves.
    #[test]
    fn pack_unpack_roundtrip() {
        for (ts, tok) in [
            (0u32, 0u32),
            (1, 1),
            (u32::MAX, u32::MAX),
            (12345, 67890),
            (0, u32::MAX),
            (u32::MAX, 0),
        ] {
            let packed = pack_state(ts, tok);
            let (ts2, tok2) = unpack_state(packed);
            assert_eq!((ts, tok), (ts2, tok2));
        }
    }

    // -------------------------------------------------------------
    // **A1 (rc.7)** — RSS-aware admission multiplier tests.
    // -------------------------------------------------------------
    //
    // Each test constructs an `RssThrottle` with a fresh per-test
    // `pool_label` so the gauges can be inspected per-case without
    // cross-test pollution. The watermarks use the spec defaults
    // (low = 0.7, high = 0.9) and `rss_target_bytes = 100` so the
    // pressure values are easy to reason about (rss = pressure ×
    // 100 in arithmetic terms).

    /// Helper: build an `RssThrottle` with the spec defaults, scoped
    /// to a unique `pool_label`. Tests inject RSS via `record_rss`
    /// directly so the reading is deterministic regardless of the
    /// actual `/proc/self/status` on the test runner.
    fn rss_throttle_for_test(pool_label: &'static str) -> RssThrottle {
        RssThrottle::from_config(
            &crate::config::RssThrottleConfig {
                enabled: true,
                rss_target_bytes: 100,
                rss_poll_interval_secs: 5,
                low_watermark: 0.7,
                high_watermark: 0.9,
            },
            pool_label,
        )
    }

    /// Spec test 1 — `rss_pressure_below_low_watermark_no_throttle`.
    /// RSS = 50 (= 0.5× target = below 0.7 watermark) ⇒ multiplier
    /// must be `MULT_BPS_FULL` (no throttle).
    #[test]
    fn rss_pressure_below_low_watermark_no_throttle() {
        let t = rss_throttle_for_test("test_rss_below_low");
        t.record_rss(50);
        assert_eq!(t.multiplier_bps(), MULT_BPS_FULL);
    }

    /// Spec test 2 — `rss_pressure_at_low_watermark_no_throttle`.
    /// RSS = 70 (= 0.7× target = exactly at low watermark) ⇒
    /// multiplier must be `MULT_BPS_FULL`. The boundary is part of
    /// the no-throttle band per the spec curve (`pressure < low`
    /// gives full, and the linear interp at `pressure == low`
    /// also evaluates to FULL — this test pins both halves of the
    /// boundary to the same answer).
    #[test]
    fn rss_pressure_at_low_watermark_no_throttle() {
        let t = rss_throttle_for_test("test_rss_at_low");
        t.record_rss(70);
        assert_eq!(t.multiplier_bps(), MULT_BPS_FULL);
    }

    /// Spec test 3 — `rss_pressure_mid_band_partial_throttle`.
    /// RSS = 80 (= 0.8× target = midpoint of [0.7, 0.9]) ⇒ multiplier
    /// must be exactly half of FULL. The arithmetic in
    /// `multiplier_bps` is integer (bps), so the answer is exactly
    /// 5_000 — no floating-point fuzz to chase.
    #[test]
    fn rss_pressure_mid_band_partial_throttle() {
        let t = rss_throttle_for_test("test_rss_mid_band");
        t.record_rss(80);
        assert_eq!(t.multiplier_bps(), MULT_BPS_FULL / 2);
    }

    /// Spec test 4 — `rss_pressure_at_high_watermark_full_pause`.
    /// RSS = 90 (= 0.9× target = exactly at high watermark) ⇒
    /// multiplier must be `MULT_BPS_NONE` (full pause).
    #[test]
    fn rss_pressure_at_high_watermark_full_pause() {
        let t = rss_throttle_for_test("test_rss_at_high");
        t.record_rss(90);
        assert_eq!(t.multiplier_bps(), MULT_BPS_NONE);
    }

    /// Spec test 5 — `rss_pressure_above_high_watermark_full_pause`.
    /// RSS = 100 (= 1.0× target = at-or-above high watermark) ⇒
    /// multiplier still `MULT_BPS_NONE`. The "anything above
    /// high_watermark stays at zero" behaviour stops a runaway
    /// process from re-triggering admits via integer overflow.
    #[test]
    fn rss_pressure_above_high_watermark_full_pause() {
        let t = rss_throttle_for_test("test_rss_above_high");
        t.record_rss(100);
        assert_eq!(t.multiplier_bps(), MULT_BPS_NONE);

        // Belt-and-braces: a far-above-target RSS (e.g. a leak
        // scenario) must also pin to NONE — saturating arithmetic
        // must not wrap into a "looks healthy" reading.
        t.record_rss(u64::MAX / 2);
        assert_eq!(t.multiplier_bps(), MULT_BPS_NONE);
    }

    /// Spec test 6 — `rss_unread_falls_back_to_unthrottled`.
    /// When the host cannot expose `/proc/self/status` (non-Linux
    /// dev laptop, sandboxed container, transient I/O failure),
    /// `record_rss` is never called and `current_rss` stays at the
    /// `RSS_UNKNOWN` sentinel. The multiplier must be `FULL` —
    /// the daemon fails OPEN, never CLOSED, on a missing reading.
    /// We verify the panic-free path by:
    ///   (1) constructing a throttle without ever calling
    ///       `record_rss`,
    ///   (2) asserting `multiplier_bps() == FULL`, and
    ///   (3) calling `read_proc_rss()` directly — on Linux this
    ///       returns Some(_), on macOS / non-Linux it returns
    ///       None — and verifying neither branch panics.
    #[test]
    fn rss_unread_falls_back_to_unthrottled() {
        let t = rss_throttle_for_test("test_rss_unread");
        // Step 1: never record. Multiplier is FULL because the
        // sentinel is in place.
        assert_eq!(t.multiplier_bps(), MULT_BPS_FULL);

        // Step 2: a degenerate target_bytes = 0 also routes to the
        // fail-open branch. We construct a fresh throttle with a
        // zero target to exercise that exit path.
        let zero_target = RssThrottle::from_config(
            &crate::config::RssThrottleConfig {
                enabled: true,
                rss_target_bytes: 0,
                rss_poll_interval_secs: 5,
                low_watermark: 0.7,
                high_watermark: 0.9,
            },
            "test_rss_zero_target",
        );
        zero_target.record_rss(1024 * 1024 * 1024); // 1 GiB
        assert_eq!(zero_target.multiplier_bps(), MULT_BPS_FULL);

        // Step 3: read_proc_rss must be panic-free regardless of
        // whether `/proc/self/status` exists. On Linux it returns
        // Some(rss); on macOS / Windows / sandboxed it returns
        // None. Either way the call site stays alive.
        let _ = RssThrottle::read_proc_rss();
    }

    /// Spec test 7 — `rss_disabled_via_config_no_throttle`.
    /// `RssThrottleConfig::enabled = false` ⇒ multiplier always
    /// `MULT_BPS_FULL`, regardless of how high the recorded RSS
    /// climbs. This is the operator escape hatch: ship the byte-
    /// rate limiter live, keep the RSS feedback path off until
    /// the new lever is trusted.
    #[test]
    fn rss_disabled_via_config_no_throttle() {
        let disabled = RssThrottle::from_config(
            &crate::config::RssThrottleConfig {
                enabled: false,
                rss_target_bytes: 100,
                rss_poll_interval_secs: 5,
                low_watermark: 0.7,
                high_watermark: 0.9,
            },
            "test_rss_disabled",
        );
        // Even at full pressure, disabled config ⇒ no throttle.
        for rss in [50_u64, 80, 100, u64::MAX / 2] {
            disabled.record_rss(rss);
            assert_eq!(
                disabled.multiplier_bps(),
                MULT_BPS_FULL,
                "disabled throttle must never throttle (rss={rss})",
            );
        }
    }

    // -------------------------------------------------------------
    // Auxiliary regression tests for the `RssThrottle` plumbing.
    // Not part of the seven-test spec list, but cheap to keep so
    // future refactors don't silently break the parser or the
    // probabilistic admit path.
    // -------------------------------------------------------------

    /// `parse_vmrss` accepts the canonical procfs format and
    /// returns the value in bytes (procfs reports kibibytes).
    #[test]
    fn parse_vmrss_handles_canonical_format() {
        let blob = "Name:\tshelfd\nVmRSS:\t  4096 kB\nVmHWM:\t  8192 kB\n";
        assert_eq!(RssThrottle::parse_vmrss(blob), Some(4096 * 1024));
    }

    /// `parse_vmrss` returns `None` on a missing line, an empty
    /// blob, or junk input — never panics.
    #[test]
    fn parse_vmrss_returns_none_on_missing_or_garbage() {
        assert_eq!(RssThrottle::parse_vmrss(""), None);
        assert_eq!(RssThrottle::parse_vmrss("Name:\tshelfd\n"), None);
        assert_eq!(RssThrottle::parse_vmrss("VmRSS:\tnotanumber kB\n"), None);
        assert_eq!(RssThrottle::parse_vmrss("VmRSS:\n"), None);
    }

    /// `next_rng_bps` produces draws in `[0, 10_000]` and is not
    /// trivially constant. We don't assert distributional
    /// properties (that's `cargo bench` territory), only that the
    /// range is correct and that successive calls differ.
    #[test]
    fn next_rng_bps_is_in_range_and_varies() {
        let lim = LodcAdmission::new(1 << 30, 1 << 20, "test_rng_range");
        let mut seen = std::collections::HashSet::new();
        for _ in 0..256 {
            let v = lim.next_rng_bps();
            assert!(v <= 10_000, "draw out of range: {v}");
            seen.insert(v);
        }
        // 256 draws on a uniform distribution over 10_001 buckets
        // gives a near-certain ≥ 50 distinct values; we use a
        // generous lower bound to keep the test stable on any
        // remotely sane PRNG.
        assert!(
            seen.len() >= 32,
            "rng must produce varied output; got {} distinct in 256 draws",
            seen.len()
        );
    }

    /// `try_admit` with a throttle pinned at `MULT_BPS_NONE`
    /// drops every call regardless of token-bucket state, and the
    /// drop is filed as a `rate_limit` reason (no new label needed).
    #[test]
    fn try_admit_drops_when_rss_throttle_is_full_pause() {
        let throttle = Arc::new(rss_throttle_for_test("test_admit_full_pause"));
        // Pin pressure at the high watermark so multiplier == NONE.
        throttle.record_rss(95);
        assert_eq!(throttle.multiplier_bps(), MULT_BPS_NONE);

        let lim = LodcAdmission::new(1 << 30, 1 << 20, "test_admit_full_pause_lim")
            .with_rss_throttle(throttle);
        let baseline_drops = rate_limit_drops("test_admit_full_pause_lim");
        for _ in 0..16 {
            assert!(!lim.try_admit(4096), "RSS-paused throttle must drop");
        }
        assert_eq!(
            rate_limit_drops("test_admit_full_pause_lim") - baseline_drops,
            16,
            "every paused admit must bump the rate_limit drop counter",
        );
    }

    /// `try_admit` with a healthy throttle (`MULT_BPS_FULL`) is a
    /// no-op layered on top of the byte-rate gate — every call
    /// admits, no drops fire.
    #[test]
    fn try_admit_unaffected_when_rss_throttle_is_full() {
        let throttle = Arc::new(rss_throttle_for_test("test_admit_full"));
        throttle.record_rss(10); // well below low watermark
        assert_eq!(throttle.multiplier_bps(), MULT_BPS_FULL);

        let lim =
            LodcAdmission::new(1 << 30, 1 << 20, "test_admit_full_lim").with_rss_throttle(throttle);
        let baseline_drops = rate_limit_drops("test_admit_full_lim");
        for _ in 0..16 {
            assert!(
                lim.try_admit(4096),
                "healthy throttle must let admits through",
            );
        }
        assert_eq!(
            rate_limit_drops("test_admit_full_lim"),
            baseline_drops,
            "healthy throttle must produce zero rate-limit drops",
        );
    }
}
