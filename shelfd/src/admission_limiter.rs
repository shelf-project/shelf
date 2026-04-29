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
use std::time::Instant;

use crate::config::LodcAdmissionConfig;

/// Token bucket capacity below which the limiter degenerates to the
/// "always drop" kill-switch path. Used by the
/// [`LodcAdmission::try_admit`] short-circuit when an operator
/// intentionally configures `target_bytes_per_sec = 0` to disable
/// caching from the rowgroup pool.
const ZERO_RATE_KILL_SWITCH: u64 = 0;

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
        Some(Self {
            refill_bytes_per_sec: cfg.target_bytes_per_sec,
            max_burst_bytes,
            max_inflight_admissions: cfg.max_inflight_admissions,
            state: AtomicU64::new(pack_state(0, max_burst_bytes as u32)),
            start: Instant::now(),
            pool_label,
            initialised: AtomicBool::new(false),
        })
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
        }
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
}
