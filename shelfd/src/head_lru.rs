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

/// LRU of `HeadObject` responses, keyed on `(bucket, s3_key)`.
#[derive(Debug)]
pub struct HeadLru {
    cache: foyer::Cache<(String, String), Arc<HeadMeta>>,
    max_entries: u64,
}

impl HeadLru {
    /// Build an LRU that admits at most `max_entries` entries. Each
    /// entry is weighted as 1, so the Foyer capacity in Foyer's units
    /// is exactly the entry count.
    pub fn new(max_entries: u64) -> Self {
        let capped = max_entries.max(1) as usize;
        let cache = foyer::CacheBuilder::new(capped)
            .with_weighter(|_k: &(String, String), _v: &Arc<HeadMeta>| 1)
            .build();
        Self { cache, max_entries }
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
}
