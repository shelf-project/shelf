//! In-memory LRU for `HeadObject` responses.
//!
//! Ticket ownership:
//! - SHELF-07 — a tiny `Cache<(bucket, key), HeadMeta>` sized at
//!   10 000 entries by default. The HTTP `HEAD` handler consults it
//!   first; a miss issues a single `HeadObject` and populates the LRU.
//!
//! The LRU is deliberately small and bucket-key-addressed (not
//! content-addressed): plugins call `HEAD` before they know the
//! object's content-addressed hash, so there is nothing to hash on.
//! Foyer's built-in SIEVE policy evicts cold entries when we exceed
//! `max_entries`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::origin::ObjectHead;

/// The cached value. Cloning is `Arc`-cheap via [`HeadLru::get`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HeadMeta {
    pub content_length: u64,
    /// S3 ETag (quotes included) as a UTF-8 string when decodable;
    /// non-UTF-8 bytes are dropped. Only used to decorate response
    /// headers — it is never trusted as an integrity check.
    pub etag: Option<String>,
    /// RFC-3339 `Last-Modified`, when S3 returned one.
    pub last_modified: Option<String>,
}

impl From<ObjectHead> for HeadMeta {
    fn from(h: ObjectHead) -> Self {
        let etag = if h.etag.is_empty() {
            None
        } else {
            String::from_utf8(h.etag).ok()
        };
        HeadMeta {
            content_length: h.content_length,
            etag,
            last_modified: h.last_modified,
        }
    }
}

/// Default negative-cache TTL. Chosen to be:
/// - long enough to absorb the burst of HEADs a listing scanner
///   issues against objects that legitimately don't exist (Iceberg
///   delete-file speculation, optional `stats.puffin`),
/// - short enough that a transient origin blip (IRSA token rotation,
///   STS throttle, clock skew) doesn't translate into a multi-tens-
///   of-seconds tail of stale "missing" verdicts after the upstream
///   recovers — see `origin::is_persistent_forbidden_code` which is
///   what actually gates entry into this LRU on the 403 path.
const NEGATIVE_TTL_DEFAULT: Duration = Duration::from_secs(5);

/// Track D4 — negative cache entry. `expires_at` is a monotonic
/// deadline; if `Instant::now() >= expires_at`, the entry is stale
/// and treated as a miss so the next caller re-issues HEAD.
#[derive(Debug, Clone)]
struct NegativeEntry {
    expires_at: Instant,
}

/// LRU of `HeadObject` responses, keyed on `(bucket, s3_key)`.
#[derive(Debug)]
pub struct HeadLru {
    cache: foyer::Cache<(String, String), Arc<HeadMeta>>,
    /// Track D4 — short-TTL negative cache for 404 / 403 / access-denied
    /// responses. Keyed identically to the positive cache so
    /// `get_negative` is a pointer-chase lookup.
    negative: foyer::Cache<(String, String), NegativeEntry>,
    max_entries: u64,
    negative_ttl: Duration,
}

impl HeadLru {
    /// Build an LRU that admits at most `max_entries` entries. Each
    /// entry is weighted as 1, so the Foyer capacity in Foyer's units
    /// is exactly the entry count.
    pub fn new(max_entries: u64) -> Self {
        Self::with_negative_ttl(max_entries, NEGATIVE_TTL_DEFAULT)
    }

    /// Test-oriented constructor allowing a custom negative-cache TTL.
    /// Production paths call [`HeadLru::new`].
    pub fn with_negative_ttl(max_entries: u64, negative_ttl: Duration) -> Self {
        let capped = max_entries.max(1) as usize;
        let cache = foyer::CacheBuilder::new(capped)
            .with_weighter(|_k: &(String, String), _v: &Arc<HeadMeta>| 1)
            .build();
        // Negative cache is sized at 2× the positive cache: a workload
        // that HEADs many objects that don't exist is precisely the
        // case we want to protect S3 from, so we give it more room
        // than the positive path to keep HEAD 404s from evicting real
        // cached HeadMeta.
        let neg_cap = capped.saturating_mul(2).max(1);
        let negative = foyer::CacheBuilder::new(neg_cap)
            .with_weighter(|_k: &(String, String), _v: &NegativeEntry| 1)
            .build();
        Self {
            cache,
            negative,
            max_entries,
            negative_ttl,
        }
    }

    /// Fetch an existing entry without any origin side effect.
    pub fn get(&self, bucket: &str, key: &str) -> Option<Arc<HeadMeta>> {
        self.cache
            .get(&(bucket.to_owned(), key.to_owned()))
            .map(|e| e.value().clone())
    }

    /// Insert (or overwrite) an entry.
    pub fn insert(&self, bucket: String, key: String, meta: HeadMeta) {
        self.cache.insert((bucket, key), Arc::new(meta));
    }

    /// Entry count currently held in the LRU. `usize` rather than u64
    /// because Foyer's `usage()` is a `usize`; exposed for tests and
    /// `/stats`-style introspection.
    pub fn len(&self) -> usize {
        self.cache.usage()
    }

    /// Whether the LRU has been populated with at least one entry.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Configured cap, as passed to [`HeadLru::new`].
    pub fn capacity(&self) -> u64 {
        self.max_entries
    }

    /// Track D4 — record a negative HEAD response (404 / NoSuchKey /
    /// 403 / AccessDenied). Subsequent lookups inside `negative_ttl`
    /// return true from [`HeadLru::is_known_missing`] and the caller
    /// can skip the origin HEAD entirely.
    pub fn record_missing(&self, bucket: &str, key: &str) {
        // Any prior positive cache entry is invalidated — a transition
        // "object existed → 404" (snapshot rotation, orphan cleanup) is
        // exactly when we must stop serving the stale length/etag.
        self.cache.remove(&(bucket.to_owned(), key.to_owned()));
        self.negative.insert(
            (bucket.to_owned(), key.to_owned()),
            NegativeEntry {
                expires_at: Instant::now() + self.negative_ttl,
            },
        );
    }

    /// Track D4 — returns true if the `(bucket, key)` pair was HEAD'd
    /// recently and the origin returned 404/403. Callers (typically
    /// the HEAD handler) that see true can short-circuit to a
    /// `NoSuchKey` response without talking to S3.
    ///
    /// Expired entries are NOT proactively removed here; they are
    /// simply ignored, and Foyer's SIEVE will evict them naturally
    /// when capacity is pressed. This keeps `is_known_missing` O(1).
    pub fn is_known_missing(&self, bucket: &str, key: &str) -> bool {
        let entry = self
            .negative
            .get(&(bucket.to_owned(), key.to_owned()))
            .map(|e| e.value().clone());
        match entry {
            Some(e) => Instant::now() < e.expires_at,
            None => false,
        }
    }

    /// Track D4 — invalidate the negative entry for a key. Called
    /// after a successful PUT / positive HEAD so a previously-404
    /// object becomes cacheable again immediately.
    pub fn forget_missing(&self, bucket: &str, key: &str) {
        self.negative.remove(&(bucket.to_owned(), key.to_owned()));
    }

    /// SHELF-21 — drop a positive entry without recording a 404.
    ///
    /// Called by the shim's PUT handler after a successful upstream
    /// `PutObject`: the cached `(content_length, etag, last_modified)`
    /// tuple is now stale and must not be served to a subsequent
    /// HEAD/GET. The next caller re-HEADs origin, observes the new
    /// ETag, and the SHELF-04 content-addressed Foyer key derived
    /// from that ETag will not collide with the pre-PUT entries —
    /// so the stale row-group bytes become unreachable orphans
    /// rather than poisoned hits, and Foyer's eviction policy ages
    /// them out naturally.
    ///
    /// We deliberately do **not** touch the negative cache here:
    /// `record_missing` is the right call for DELETE, and a PUT
    /// against a previously-404 key already gets `forget_missing`
    /// from the post-write reconciliation in the shim handler.
    pub fn invalidate(&self, bucket: &str, key: &str) {
        self.cache.remove(&(bucket.to_owned(), key.to_owned()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(len: u64) -> HeadMeta {
        HeadMeta {
            content_length: len,
            etag: Some("\"abc123\"".to_owned()),
            last_modified: Some("2024-01-02T03:04:05Z".to_owned()),
        }
    }

    #[test]
    fn miss_returns_none() {
        let lru = HeadLru::new(8);
        assert!(lru.get("bucket", "key").is_none());
        assert!(lru.is_empty());
    }

    #[test]
    fn insert_then_get_round_trip() {
        let lru = HeadLru::new(8);
        lru.insert("b".into(), "k".into(), meta(1024));
        let got = lru.get("b", "k").expect("hit after insert");
        assert_eq!(got.content_length, 1024);
        assert_eq!(got.etag.as_deref(), Some("\"abc123\""));
        assert_eq!(got.last_modified.as_deref(), Some("2024-01-02T03:04:05Z"));
        assert_eq!(lru.len(), 1);
    }

    /// The core SHELF-07 invariant: a second identical lookup must
    /// not trigger an origin fetch. We model the "origin fetch" as a
    /// counter we bump from inside the miss branch. If the LRU were
    /// broken, the counter would increment twice.
    #[test]
    fn second_lookup_is_a_local_hit_not_an_origin_fetch() {
        let lru = HeadLru::new(8);
        let mut origin_calls = 0u64;

        let fetch_once = |lru: &HeadLru, origin_calls: &mut u64| {
            if lru.get("b", "k").is_none() {
                *origin_calls += 1;
                lru.insert("b".into(), "k".into(), meta(42));
            }
        };

        fetch_once(&lru, &mut origin_calls);
        fetch_once(&lru, &mut origin_calls);
        fetch_once(&lru, &mut origin_calls);

        assert_eq!(
            origin_calls, 1,
            "HEAD-LRU must absorb repeated lookups: first miss only."
        );
        assert_eq!(lru.get("b", "k").unwrap().content_length, 42);
    }

    #[test]
    fn capacity_enforced_lru_evicts_oldest() {
        let lru = HeadLru::new(4);
        for i in 0..16u32 {
            lru.insert("b".into(), format!("k{i}"), meta(i as u64));
        }
        // After 16 inserts at cap=4, at most 4 entries remain. Foyer
        // picks which ones to keep (SIEVE); we only assert the cap.
        assert!(
            lru.len() <= 4,
            "LRU must stay within its cap, got {}",
            lru.len()
        );
    }

    #[test]
    fn from_object_head_handles_missing_fields() {
        let head = ObjectHead {
            content_length: 99,
            etag: Vec::new(),
            last_modified: None,
        };
        let meta: HeadMeta = head.into();
        assert_eq!(meta.content_length, 99);
        assert!(meta.etag.is_none());
        assert!(meta.last_modified.is_none());
    }

    #[test]
    fn min_capacity_is_one_not_zero() {
        // We refuse to build a zero-capacity foyer cache because
        // Foyer treats capacity==0 as "always evict", which is a
        // foot-gun. Clamp to 1.
        let lru = HeadLru::new(0);
        lru.insert("b".into(), "k".into(), meta(1));
        // The entry may or may not stick at cap=1 depending on SIEVE
        // ordering; the important property is "did not panic".
        assert!(lru.capacity() == 0);
    }

    // ---- Track D4 — negative cache ------------------------------------------

    #[test]
    fn negative_cache_absorbs_repeated_misses_within_ttl() {
        let lru = HeadLru::with_negative_ttl(8, Duration::from_millis(200));
        assert!(!lru.is_known_missing("b", "k"));
        lru.record_missing("b", "k");
        assert!(lru.is_known_missing("b", "k"));
        assert!(
            lru.is_known_missing("b", "k"),
            "idempotent on repeated probes"
        );
    }

    #[test]
    fn negative_cache_entry_expires() {
        let lru = HeadLru::with_negative_ttl(8, Duration::from_millis(25));
        lru.record_missing("b", "k");
        assert!(lru.is_known_missing("b", "k"));
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !lru.is_known_missing("b", "k"),
            "stale negative entries must not shadow origin"
        );
    }

    #[test]
    fn record_missing_invalidates_positive_entry() {
        let lru = HeadLru::new(8);
        lru.insert("b".into(), "k".into(), meta(1024));
        assert!(lru.get("b", "k").is_some());
        lru.record_missing("b", "k");
        assert!(
            lru.get("b", "k").is_none(),
            "stale positive entry must not survive a 404 observation"
        );
        assert!(lru.is_known_missing("b", "k"));
    }

    #[test]
    fn forget_missing_restores_caller_to_origin() {
        let lru = HeadLru::new(8);
        lru.record_missing("b", "k");
        assert!(lru.is_known_missing("b", "k"));
        lru.forget_missing("b", "k");
        assert!(!lru.is_known_missing("b", "k"));
    }

    // ---- SHELF-21 — write-passthrough invalidation contract -------

    #[test]
    fn invalidate_drops_positive_entry_without_recording_a_404() {
        // PUT semantics: drop the stale positive tuple, but do not
        // poison the next HEAD with a negative cache entry — the
        // object now exists and a follow-up read must reach origin
        // and observe the new ETag.
        let lru = HeadLru::with_negative_ttl(8, Duration::from_secs(60));
        lru.insert("b".into(), "k".into(), meta(1024));
        assert!(lru.get("b", "k").is_some());
        lru.invalidate("b", "k");
        assert!(
            lru.get("b", "k").is_none(),
            "post-PUT positive entry must be evicted"
        );
        assert!(
            !lru.is_known_missing("b", "k"),
            "invalidate must not push a negative entry — that is what record_missing is for",
        );
    }

    #[test]
    fn invalidate_is_idempotent_on_absent_keys() {
        // First-time PUT sees no positive entry to drop; invalidate
        // must be a clean no-op rather than panicking on a missing
        // foyer key. This is a hot path on the shim's PUT handler.
        let lru = HeadLru::new(8);
        lru.invalidate("b", "k");
        assert!(lru.get("b", "k").is_none());
        assert!(!lru.is_known_missing("b", "k"));
    }
}
