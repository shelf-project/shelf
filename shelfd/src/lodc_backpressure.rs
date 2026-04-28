//! SHELF-21e — bounded LODC submit-queue with drop-on-full back-pressure.
//!
//! ## Why this module exists
//!
//! Foyer's hybrid pool (DRAM L1 + NVMe L2) drains DRAM evictions
//! into the Large-Object Disk Cache (LODC) submit queue, which is a
//! bounded `flume` channel between the rowgroup pool's L1 evictor
//! and the L2 disk writers. When sustained write ingress (the
//! product of read-miss rate × payload size) exceeds the EBS gp3
//! drain rate, the queue fills, RSS grows by the size of the
//! buffered blob bodies, and the pod gets OOMKilled by the kubelet.
//!
//! The 2026-04-27 production incident on `shelf-1` is the canonical
//! example — see `shelfd/docs/runbooks/2026-04-shelf-1-oom.md`.
//!
//! ## Why this is not the previous `RateLimitPicker`
//!
//! preview-8 wired Foyer's stock [`foyer::RateLimitPicker`] (a
//! token-bucket bytes/sec admission picker) on the `HybridCacheBuilder`.
//! Under sustained ingress at the configured rate the bucket
//! immediately empties, the picker rejects every subsequent
//! admission, and `hit_disk` p99 pegged at 16 s (verified during
//! the 2026-04-28 chaos window — see workspace memory). The picker
//! also gates the *rate* of admissions even when the queue is empty,
//! so under burst-then-quiet workloads it adds latency to every
//! write regardless of actual queue pressure. Reverted in preview-9
//! / helm rev-22.
//!
//! This module replaces the rate-based gate with a *level-based*
//! gate at shelfd's own admission seam (inside
//! [`crate::store::FoyerStore::get_or_fetch`]). The level is the
//! observed in-flight byte count: `admitted_bytes − committed_bytes`,
//! both monotonic counters since pool open. As long as Foyer's
//! flushers are draining the submit queue, in-flight stays small
//! and every admission is accepted with O(1) atomics. Only when the
//! queue is genuinely backing up (e.g. EBS p99 latency spike, NVMe
//! near full, flushers saturated) does the watermark trip and the
//! picker drop the admission — emitting `shelf_lodc_drops_total` so
//! Grafana shows back-pressure events as a clear signal.
//!
//! ## What dropping costs us
//!
//! Dropping a single admission means the read that triggered the
//! miss still completes (bytes flow from origin → caller); the
//! cache simply doesn't cache them. The next request for that key
//! takes another origin trip. This is the documented trade-off the
//! task asked for: prefer dropping NVMe writes over crashing the
//! process or stalling reads. ADR-0009 § "Eviction" already accepts
//! cache miss next time as the fallback for any disk-side failure
//! mode (we run with origin S3 as the safety net).
//!
//! ## Auto-tuning the watermark
//!
//! The high-watermark is derived from the existing operator-facing
//! knob `pools.rowgroup.disk_cache.submit_queue_size_threshold_bytes`
//! (Foyer's hard cap; defaults to 1 GiB in production). We pick
//! 80% of the threshold so we drop a hair before Foyer would, and
//! our drop is observable via the Prometheus counter rather than a
//! `tracing::warn!` in pod logs.
//!
//! When the threshold isn't set (dev clusters, unit tests), we
//! derive a watermark from `buffer_pool_size_bytes` (× 2) or fall
//! back to 800 MiB so test pools still admit normally.
//!
//! No new ConfigMap key is introduced.

use std::sync::atomic::{AtomicU64, Ordering};

/// Default fallback watermark when neither
/// `submit_queue_size_threshold_bytes` nor `buffer_pool_size_bytes`
/// is set (typical for unit-test pools and local dev where the LODC
/// is barely exercised). 800 MiB matches "80% of Foyer's prod
/// default 1 GiB threshold" so test behaviour matches prod intent.
const DEFAULT_WATERMARK_BYTES: u64 = 800 * 1024 * 1024;

/// Ratio of the submit-queue threshold we trip the watermark at.
/// 80% leaves 20% headroom for Foyer's own hard cap — if our
/// observability lags by a tick, Foyer's `submit_queue_size_threshold`
/// still saves us. The constant is module-private; operators tune
/// the underlying `submit_queue_size_threshold_bytes` instead.
const WATERMARK_RATIO_NUM: u64 = 4;
const WATERMARK_RATIO_DEN: u64 = 5;

/// Level-based, drop-on-full back-pressure controller for the
/// rowgroup hybrid pool's LODC submit queue.
///
/// Cheap to construct, lock-free at steady state (two atomic
/// loads + one branch on the hot path). Held inside `FoyerStore`
/// behind an `Option` — `None` means the rowgroup pool is
/// DRAM-only and there is no LODC to gate.
#[derive(Debug)]
pub struct LodcBackpressure {
    /// In-flight bytes ceiling. When `inflight ≥ high_watermark`
    /// the next admission is dropped. Static after construction.
    high_watermark_bytes: u64,
    /// Monotonic count of bytes shelfd has admitted into the
    /// hybrid pool since pool open. Subtracting Foyer's
    /// `cache_write_bytes()` (the disk-committed counter) yields
    /// the approximate in-flight byte budget.
    admitted_bytes: AtomicU64,
    /// Monotonic count of admissions; combined with `admitted_bytes`
    /// it gives the average per-entry byte size, which we use to
    /// estimate `queue_depth` (the gauge is informational; the
    /// admission decision uses `admitted_bytes` directly).
    admitted_count: AtomicU64,
    /// Stable Prometheus pool label, e.g. `"rowgroup"`. Held as
    /// `&'static str` so each metric increment skips a clone.
    pool_label: &'static str,
}

impl LodcBackpressure {
    /// Construct a back-pressure controller from the operator-facing
    /// `RowGroupDiskCacheConfig`. Auto-tunes the watermark so no
    /// new ConfigMap key is needed.
    pub fn from_disk_cache_config(
        cfg: &crate::config::RowGroupDiskCacheConfig,
        pool_label: &'static str,
    ) -> Self {
        let submit_queue_threshold = cfg
            .submit_queue_size_threshold_bytes
            // If the queue threshold is unset, fall back to
            // `buffer_pool_size × 2` (Foyer's own internal default
            // when the threshold field is `None`).
            .or_else(|| cfg.buffer_pool_size_bytes.map(|b| b.saturating_mul(2)))
            .unwrap_or(DEFAULT_WATERMARK_BYTES);
        Self::new(submit_queue_threshold, pool_label)
    }

    /// Construct a controller with an explicit submit-queue
    /// threshold. Used by unit tests; production callers go through
    /// [`Self::from_disk_cache_config`].
    pub fn new(submit_queue_threshold_bytes: u64, pool_label: &'static str) -> Self {
        // The watermark ratio is intentionally `(threshold * 4) / 5`
        // and not `threshold / 5 * 4`: the order matters when the
        // threshold is small (e.g. 16 bytes in a unit test) so we
        // keep the higher-precision form. `saturating_mul` guards
        // against the threshold rolling over u64 (impossible at
        // disk-cache scales, but free).
        let high_watermark_bytes = submit_queue_threshold_bytes.saturating_mul(WATERMARK_RATIO_NUM)
            / WATERMARK_RATIO_DEN.max(1);
        Self {
            high_watermark_bytes,
            admitted_bytes: AtomicU64::new(0),
            admitted_count: AtomicU64::new(0),
            pool_label,
        }
    }

    /// Configured watermark in bytes. Test-only accessor.
    #[cfg(test)]
    pub fn high_watermark_bytes(&self) -> u64 {
        self.high_watermark_bytes
    }

    /// Bytes admitted since pool open. Test-only accessor.
    #[cfg(test)]
    pub fn admitted_bytes(&self) -> u64 {
        self.admitted_bytes.load(Ordering::Relaxed)
    }

    /// Decide whether the entry of `entry_bytes` should be admitted
    /// into the hybrid pool. `committed_bytes` is the caller-supplied
    /// snapshot of Foyer's `cache_write_bytes()` for this pool —
    /// the monotonic count of bytes already committed to NVMe. The
    /// admission decision is `inflight = admitted_bytes − committed_bytes`
    /// against the fixed watermark.
    ///
    /// Always non-blocking: no awaits, no mutex, no channel send.
    /// Two atomic loads and a branch on the happy path; one extra
    /// `fetch_add` when admitting.
    ///
    /// Side effects:
    /// - `shelf_lodc_inflight_bytes{pool}` gauge set to current inflight.
    /// - `shelf_lodc_queue_depth{pool}` gauge set to estimated depth
    ///   (inflight ÷ avg admitted entry size).
    /// - On reject: `shelf_lodc_drops_total{pool}` counter +1.
    ///
    /// Returns `false` when the watermark is exceeded; the caller
    /// should skip the disk insert (DRAM may stay hot via a separate
    /// path, but the rowgroup hybrid pool's `cache.insert` is
    /// fused — skipping the entire insert is the simplest correct
    /// behaviour; the user re-fetches from origin on the next miss).
    pub fn should_admit(&self, entry_bytes: u64, committed_bytes: u64) -> bool {
        let admitted = self.admitted_bytes.load(Ordering::Relaxed);
        let inflight = admitted.saturating_sub(committed_bytes);

        // Update gauges on every call so Grafana sees a live signal
        // even when no drops are happening. Both gauges accept the
        // value as `i64`; at u64 max the saturating cast clamps to
        // `i64::MAX` which is fine for a gauge in the EB range.
        crate::metrics::LODC_INFLIGHT_BYTES
            .with_label_values(&[self.pool_label])
            .set(saturating_u64_to_i64(inflight));

        let admitted_count = self.admitted_count.load(Ordering::Relaxed);
        let avg_size = admitted.checked_div(admitted_count).unwrap_or(0);
        let queue_depth = inflight.checked_div(avg_size).unwrap_or(0);
        crate::metrics::LODC_QUEUE_DEPTH
            .with_label_values(&[self.pool_label])
            .set(saturating_u64_to_i64(queue_depth));

        if inflight >= self.high_watermark_bytes {
            crate::metrics::LODC_DROPS_TOTAL
                .with_label_values(&[self.pool_label])
                .inc();
            return false;
        }
        // Reserve byte budget so concurrent admits see reduced
        // headroom — fetch_add is the same atomic op as a load on
        // x86/aarch64, no measurable overhead. A racing admit
        // between our load and fetch_add could bump us slightly past
        // the watermark, but Foyer's hard `submit_queue_size_threshold`
        // catches the overshoot.
        self.admitted_bytes
            .fetch_add(entry_bytes, Ordering::Relaxed);
        self.admitted_count.fetch_add(1, Ordering::Relaxed);
        true
    }
}

/// `i64::try_from(u64)` clamped to `i64::MAX` for gauge writes.
/// Foyer's stats counters are `usize` / `AtomicUsize`; on 64-bit
/// platforms the value fits in `i64` for any realistic cache size.
/// Negative-on-overflow would mislead dashboards, so we clamp.
fn saturating_u64_to_i64(v: u64) -> i64 {
    if v > i64::MAX as u64 {
        i64::MAX
    } else {
        v as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RowGroupDiskCacheConfig;

    /// Steady drain: every admission's bytes show up promptly in
    /// `committed_bytes`, so `inflight` never grows past zero and
    /// no drops fire regardless of how many admits we issue. This
    /// is the nominal happy path.
    #[test]
    fn steady_load_no_drops() {
        let bp = LodcBackpressure::new(1 << 20, "test_steady"); // 1 MiB threshold → 800 KiB watermark
        let baseline = drops_for("test_steady");
        let mut committed = 0u64;
        for _ in 0..1000 {
            assert!(bp.should_admit(4096, committed), "steady admit must accept");
            // Drain immediately — the flusher is keeping up.
            committed += 4096;
        }
        assert_eq!(
            drops_for("test_steady") - baseline,
            0,
            "steady drain must not produce a drop"
        );
    }

    /// Burst exceeding the watermark: hold `committed_bytes` flat
    /// (simulates flushers stalling on NVMe) and pour 16 MiB of
    /// admits past a 1 MiB watermark. The first ~800 KiB go through;
    /// the rest are dropped. Reads do not block — `should_admit` is
    /// synchronous and never awaits.
    #[test]
    fn burst_exceeding_watermark_drops_excess() {
        let threshold = 1 << 20; // 1 MiB submit-queue threshold
        let bp = LodcBackpressure::new(threshold, "test_burst");
        let watermark = bp.high_watermark_bytes();
        assert_eq!(watermark, threshold * 4 / 5, "watermark must be 80%");

        let baseline_drops = drops_for("test_burst");
        let entry_bytes: u64 = 4096;
        let total: u64 = 16 * 1024 * 1024;
        let n = total / entry_bytes;

        // committed_bytes is held at zero — flushers are stuck.
        let mut admitted = 0u64;
        let mut dropped = 0u64;
        for _ in 0..n {
            if bp.should_admit(entry_bytes, 0) {
                admitted += entry_bytes;
            } else {
                dropped += entry_bytes;
            }
        }

        assert!(
            admitted >= watermark && admitted <= watermark + entry_bytes,
            "first wave must admit roughly up to the watermark; got admitted={admitted}, watermark={watermark}",
        );
        assert!(dropped > 0, "burst must produce drops");
        assert_eq!(
            admitted + dropped,
            total,
            "every entry must be either admitted or dropped — no third state"
        );
        let observed = drops_for("test_burst") - baseline_drops;
        assert_eq!(
            observed,
            dropped / entry_bytes,
            "drop counter must increment exactly once per dropped admission",
        );
    }

    /// Once the flushers catch up — `committed_bytes` advances past
    /// `admitted_bytes` — the back-pressure releases and admits
    /// resume. This is the recovery path: a slow EBS spike must not
    /// permanently disable the cache.
    #[test]
    fn recovery_after_burst_resumes_admits() {
        let bp = LodcBackpressure::new(1 << 20, "test_recovery");
        // Burst until we trip the watermark.
        let mut committed = 0u64;
        let mut admitted_now = 0u64;
        loop {
            if bp.should_admit(4096, committed) {
                admitted_now += 4096;
            } else {
                break;
            }
        }
        assert!(admitted_now >= bp.high_watermark_bytes());
        // One more attempt while inflight is still above the watermark
        // — must drop.
        assert!(
            !bp.should_admit(4096, committed),
            "still over watermark after burst — must drop",
        );

        // Drain past the watermark.
        committed = bp.admitted_bytes() + 1;
        assert!(
            bp.should_admit(4096, committed),
            "after committed catches up, admits must resume",
        );
    }

    /// `should_admit` is synchronous and pure-atomics — explicit
    /// proof that calling it cannot suspend the read path. The test
    /// is a sanity check on the function signature: if anyone
    /// accidentally makes it `async fn`, this will fail to compile.
    #[test]
    fn should_admit_is_synchronous() {
        let bp = LodcBackpressure::new(1 << 20, "test_sync");
        let _: bool = bp.should_admit(4096, 0);
    }

    /// The watermark is auto-tuned from
    /// `submit_queue_size_threshold_bytes` when set, falling back to
    /// `buffer_pool_size_bytes × 2`, and finally the module default
    /// of 800 MiB. No new ConfigMap key is introduced.
    #[test]
    fn auto_tunes_from_existing_config_keys() {
        // Threshold set explicitly → watermark = threshold × 80%.
        let cfg_with_threshold = RowGroupDiskCacheConfig {
            submit_queue_size_threshold_bytes: Some(1 << 30), // 1 GiB
            ..Default::default()
        };
        let bp = LodcBackpressure::from_disk_cache_config(&cfg_with_threshold, "test_tune_a");
        assert_eq!(bp.high_watermark_bytes(), (1u64 << 30) * 4 / 5);

        // Only buffer_pool_size set → watermark = (buffer × 2) × 80%.
        let cfg_with_buffer = RowGroupDiskCacheConfig {
            buffer_pool_size_bytes: Some(256 * 1024 * 1024),
            ..Default::default()
        };
        let bp = LodcBackpressure::from_disk_cache_config(&cfg_with_buffer, "test_tune_b");
        assert_eq!(
            bp.high_watermark_bytes(),
            (256u64 * 1024 * 1024 * 2) * 4 / 5
        );

        // Neither set → fall back to module default.
        let cfg_empty = RowGroupDiskCacheConfig::default();
        let bp = LodcBackpressure::from_disk_cache_config(&cfg_empty, "test_tune_c");
        assert_eq!(bp.high_watermark_bytes(), DEFAULT_WATERMARK_BYTES * 4 / 5);
    }

    /// Helper: read the `shelf_lodc_drops_total` counter for the
    /// given pool label. Each test uses a unique label so concurrent
    /// test runs do not poison each other's counter.
    fn drops_for(label: &str) -> u64 {
        crate::metrics::LODC_DROPS_TOTAL
            .with_label_values(&[label])
            .get()
    }
}
