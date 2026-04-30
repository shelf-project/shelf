//! SHELF-23 — per-(bucket, key) freshness tracker for ETag-conditional
//! GETs.
//!
//! ## Why
//!
//! The cross-pod write-coherence fix (`If-None-Match` round-trip on
//! every local-cache hit) costs ~5 ms of latency per read. For a
//! workload that reads the same Iceberg manifest 10 000 times in a
//! tight scheduling loop and never observes a writer, that's 50 s of
//! wall time burned re-validating bytes the cluster never modified.
//!
//! Instead of fixed TTLs, we exploit the fact that 304s come in long
//! runs: if the last `N` validations all returned 304 and the most
//! recent was less than `T` ago, we trust the cache for the next
//! short window without going back to origin. Any 200 (object
//! changed) resets the counter so the next read re-validates.
//!
//! ## Defaults
//!
//! - `N = 10` consecutive 304s before the window opens.
//! - `T = 5 s` of trust between validation calls once the window is open.
//!
//! Both are configurable from `ServerState`; ops can set them to 0 to
//! disable the optimisation entirely (forcing every read to re-validate).
//!
//! ## Cost model
//!
//! Per entry: `~96 B` for `(String, String)` keys + `8 B` instant +
//! `4 B` u32. The cache is sized at `2 × head_lru.capacity()` so
//! typical workloads stay under a megabyte even at `head_lru =
//! 100k`. Foyer's S3FIFO evicts cold entries naturally.

use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default number of consecutive 304s required before the freshness
/// window opens. `10` is large enough to ignore one-off 200s on a
/// rapidly-changing object (a manifest list rotated every 10 reads
/// would never enter the window) but small enough that a long-tail
/// stable object reaches the window quickly.
pub const DEFAULT_FRESHNESS_THRESHOLD: u32 = 10;

/// Default trust window once `DEFAULT_FRESHNESS_THRESHOLD` has been
/// hit. `5 s` matches the negative-cache TTL elsewhere in shelfd —
/// short enough that a cross-pod write becomes visible within ~one
/// scan-loop iteration, long enough to absorb the burst.
pub const DEFAULT_FRESHNESS_WINDOW: Duration = Duration::from_secs(5);

/// Per-(bucket, key) freshness state. `consecutive_304s` saturates at
/// `u32::MAX` rather than wrapping; the trust window only widens with
/// the count, so the worst case is "trust the cache for 5 s, same as
/// any other long-stable object".
#[derive(Debug, Clone)]
struct FreshnessEntry {
    consecutive_304s: u32,
    last_validated_at: Instant,
}

/// Tracker shared via `ServerState`. Internally backed by a `foyer::Cache`
/// keyed identically to `head_lru` so an operator can correlate freshness
/// state with HEAD-LRU residency by (bucket, key) pair.
#[derive(Debug)]
pub struct FreshnessTracker {
    entries: foyer::Cache<(String, String), Arc<FreshnessEntry>>,
    threshold: u32,
    window: Duration,
}

impl FreshnessTracker {
    pub fn new(max_entries: u64) -> Self {
        Self::with_params(
            max_entries,
            DEFAULT_FRESHNESS_THRESHOLD,
            DEFAULT_FRESHNESS_WINDOW,
        )
    }

    /// Test-friendly constructor exposing `threshold` and `window`.
    pub fn with_params(max_entries: u64, threshold: u32, window: Duration) -> Self {
        let capped = max_entries.max(1) as usize;
        let entries = foyer::CacheBuilder::new(capped)
            .with_weighter(|_k: &(String, String), _v: &Arc<FreshnessEntry>| 1)
            .build();
        Self {
            entries,
            threshold,
            window,
        }
    }

    /// Returns `true` when the freshness window allows the caller to
    /// skip the conditional-GET round-trip and serve directly from
    /// the local cache.
    ///
    /// The window opens iff the last `threshold` validations all
    /// returned 304 *and* the most recent validation was inside `window`.
    /// Any other state (no entry, fewer than `threshold` 304s, or an
    /// expired window) returns `false` so the caller re-validates.
    pub fn can_skip(&self, bucket: &str, key: &str) -> bool {
        if self.threshold == 0 || self.window.is_zero() {
            return false;
        }
        let entry = match self.entries.get(&(bucket.to_owned(), key.to_owned())) {
            Some(e) => e.value().clone(),
            None => return false,
        };
        if entry.consecutive_304s < self.threshold {
            return false;
        }
        Instant::now().duration_since(entry.last_validated_at) < self.window
    }

    /// Record a 304 (cache validated, object unchanged). Bumps the
    /// counter and refreshes `last_validated_at` so a long-stable
    /// object stays in the window.
    pub fn record_not_modified(&self, bucket: &str, key: &str) {
        let next = self
            .entries
            .get(&(bucket.to_owned(), key.to_owned()))
            .map(|e| e.value().clone())
            .map(|prev| FreshnessEntry {
                consecutive_304s: prev.consecutive_304s.saturating_add(1),
                last_validated_at: Instant::now(),
            })
            .unwrap_or(FreshnessEntry {
                consecutive_304s: 1,
                last_validated_at: Instant::now(),
            });
        self.entries
            .insert((bucket.to_owned(), key.to_owned()), Arc::new(next));
    }

    /// Record a 200 (object changed). Resets the counter; the next
    /// read will re-validate against origin.
    pub fn record_modified(&self, bucket: &str, key: &str) {
        self.entries.remove(&(bucket.to_owned(), key.to_owned()));
    }

    /// Test-only: peek the current consecutive-304 count for a key.
    #[cfg(test)]
    pub fn consecutive_304s(&self, bucket: &str, key: &str) -> u32 {
        self.entries
            .get(&(bucket.to_owned(), key.to_owned()))
            .map(|e| e.value().consecutive_304s)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tracker_does_not_skip() {
        let f = FreshnessTracker::new(8);
        assert!(!f.can_skip("b", "k"));
    }

    #[test]
    fn skip_only_after_threshold_reached() {
        let f = FreshnessTracker::with_params(8, 3, Duration::from_secs(60));
        f.record_not_modified("b", "k");
        assert!(!f.can_skip("b", "k"), "1/3 — must still validate");
        f.record_not_modified("b", "k");
        assert!(!f.can_skip("b", "k"), "2/3 — must still validate");
        f.record_not_modified("b", "k");
        assert!(f.can_skip("b", "k"), "3/3 — window opens");
    }

    #[test]
    fn modified_resets_counter() {
        let f = FreshnessTracker::with_params(8, 2, Duration::from_secs(60));
        f.record_not_modified("b", "k");
        f.record_not_modified("b", "k");
        assert!(f.can_skip("b", "k"));
        f.record_modified("b", "k");
        assert!(!f.can_skip("b", "k"), "must re-validate after a 200");
        assert_eq!(f.consecutive_304s("b", "k"), 0);
    }

    #[test]
    fn window_expires() {
        let f = FreshnessTracker::with_params(8, 1, Duration::from_millis(20));
        f.record_not_modified("b", "k");
        assert!(f.can_skip("b", "k"));
        std::thread::sleep(Duration::from_millis(40));
        assert!(
            !f.can_skip("b", "k"),
            "expired freshness window must re-validate",
        );
    }

    #[test]
    fn threshold_zero_disables_optimisation() {
        let f = FreshnessTracker::with_params(8, 0, Duration::from_secs(60));
        f.record_not_modified("b", "k");
        f.record_not_modified("b", "k");
        f.record_not_modified("b", "k");
        assert!(
            !f.can_skip("b", "k"),
            "threshold=0 means always validate (kill switch)",
        );
    }

    #[test]
    fn window_zero_disables_optimisation() {
        let f = FreshnessTracker::with_params(8, 1, Duration::ZERO);
        f.record_not_modified("b", "k");
        assert!(
            !f.can_skip("b", "k"),
            "window=0 means always validate (kill switch)",
        );
    }

    #[test]
    fn consecutive_count_saturates_safely() {
        let f = FreshnessTracker::with_params(8, 1, Duration::from_secs(60));
        // Bump enough times to verify the saturating_add path doesn't
        // wrap (we can't realistically count to u32::MAX in a test,
        // but we sanity-check the bookkeeping holds beyond the
        // threshold).
        for _ in 0..100 {
            f.record_not_modified("b", "k");
        }
        assert_eq!(f.consecutive_304s("b", "k"), 100);
        assert!(f.can_skip("b", "k"));
    }
}
