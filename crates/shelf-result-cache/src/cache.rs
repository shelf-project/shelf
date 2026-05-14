//! In-memory result cache with LRU eviction.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use tracing::{debug, info};

use crate::canonicalizer::CacheKey;
use crate::config::Config;

/// Cached query result.
#[derive(Debug, Clone)]
pub struct CachedResult {
    /// Serialized result data (Arrow IPC format).
    pub data: Bytes,
    /// Time when the result was cached.
    pub cached_at: Instant,
    /// Original query latency in milliseconds.
    pub original_latency_ms: u64,
    /// Number of rows in the result.
    pub row_count: u64,
    /// Size of the result in bytes.
    pub size_bytes: u64,
}

impl CachedResult {
    /// Check if the cached result has expired.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

/// Thread-safe result cache with LRU eviction.
pub struct ResultCache {
    /// The underlying LRU cache.
    cache: Mutex<LruCache<CacheKey, CachedResult>>,
    /// Configuration.
    config: Config,
    /// Current total size in bytes.
    total_bytes: Mutex<u64>,
}

impl std::fmt::Debug for ResultCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultCache")
            .field("config", &self.config)
            .field("total_bytes", &*self.total_bytes.lock())
            .finish()
    }
}

impl ResultCache {
    /// Create a new result cache with the given configuration.
    pub fn new(config: Config) -> Self {
        let capacity = std::num::NonZeroUsize::new(config.max_entries)
            .unwrap_or(std::num::NonZeroUsize::new(1000).unwrap());

        Self {
            cache: Mutex::new(LruCache::new(capacity)),
            config,
            total_bytes: Mutex::new(0),
        }
    }

    /// Look up a cached result.
    pub fn get(&self, key: &CacheKey) -> Option<CachedResult> {
        let mut cache = self.cache.lock();

        if let Some(result) = cache.get(key) {
            // Check expiration
            if result.is_expired(self.config.ttl) {
                debug!(key = %key.to_string_key(), "Cache entry expired");
                cache.pop(key);
                crate::metrics::CACHE_EXPIRATIONS_TOTAL.inc();
                return None;
            }

            crate::metrics::CACHE_HITS_TOTAL.inc();
            return Some(result.clone());
        }

        crate::metrics::CACHE_MISSES_TOTAL.inc();
        None
    }

    /// Insert a result into the cache.
    pub fn insert(&self, key: CacheKey, result: CachedResult) {
        // Check if result is too large
        if result.size_bytes > self.config.max_result_bytes {
            debug!(
                key = %key.to_string_key(),
                size = result.size_bytes,
                max = self.config.max_result_bytes,
                "Result too large to cache"
            );
            crate::metrics::CACHE_REJECTIONS_TOTAL.inc();
            return;
        }

        let mut cache = self.cache.lock();
        let mut total_bytes = self.total_bytes.lock();

        // Evict entries if we're over the byte limit
        while *total_bytes + result.size_bytes > self.config.max_cache_bytes {
            if let Some((_, evicted)) = cache.pop_lru() {
                *total_bytes = total_bytes.saturating_sub(evicted.size_bytes);
                crate::metrics::CACHE_EVICTIONS_TOTAL.inc();
            } else {
                break;
            }
        }

        // Insert the new entry
        if let Some(old) = cache.put(key.clone(), result.clone()) {
            *total_bytes = total_bytes.saturating_sub(old.size_bytes);
        }
        *total_bytes += result.size_bytes;

        debug!(
            key = %key.to_string_key(),
            size = result.size_bytes,
            rows = result.row_count,
            "Cached result"
        );
        crate::metrics::CACHE_INSERTS_TOTAL.inc();
        crate::metrics::CACHE_BYTES_TOTAL.set(*total_bytes as i64);
    }

    /// Remove a specific entry from the cache.
    pub fn remove(&self, key: &CacheKey) -> Option<CachedResult> {
        let mut cache = self.cache.lock();
        let mut total_bytes = self.total_bytes.lock();

        if let Some(result) = cache.pop(key) {
            *total_bytes = total_bytes.saturating_sub(result.size_bytes);
            crate::metrics::CACHE_BYTES_TOTAL.set(*total_bytes as i64);
            Some(result)
        } else {
            None
        }
    }

    /// Clear all entries from the cache.
    pub fn clear(&self) {
        let mut cache = self.cache.lock();
        let mut total_bytes = self.total_bytes.lock();

        let count = cache.len();
        cache.clear();
        *total_bytes = 0;

        info!(entries = count, "Cache cleared");
        crate::metrics::CACHE_BYTES_TOTAL.set(0);
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.lock();
        let total_bytes = *self.total_bytes.lock();

        CacheStats {
            entries: cache.len(),
            total_bytes,
            max_bytes: self.config.max_cache_bytes,
            max_entries: self.config.max_entries,
        }
    }

    /// Invalidate all entries for a given snapshot ID.
    ///
    /// Called when a table's snapshot changes.
    pub fn invalidate_snapshot(&self, snapshot_id: i64) {
        let mut cache = self.cache.lock();
        let mut total_bytes = self.total_bytes.lock();
        let mut invalidated = 0;

        // Collect keys to remove (can't modify while iterating)
        let keys_to_remove: Vec<_> = cache
            .iter()
            .filter(|(k, _)| k.snapshot_id == snapshot_id)
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            if let Some(result) = cache.pop(&key) {
                *total_bytes = total_bytes.saturating_sub(result.size_bytes);
                invalidated += 1;
            }
        }

        if invalidated > 0 {
            info!(
                snapshot_id = snapshot_id,
                invalidated = invalidated,
                "Invalidated cache entries for snapshot"
            );
            crate::metrics::CACHE_INVALIDATIONS_TOTAL.inc_by(invalidated);
            crate::metrics::CACHE_BYTES_TOTAL.set(*total_bytes as i64);
        }
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of entries in the cache.
    pub entries: usize,
    /// Total bytes used.
    pub total_bytes: u64,
    /// Maximum bytes allowed.
    pub max_bytes: u64,
    /// Maximum entries allowed.
    pub max_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config {
            max_entries: 100,
            max_cache_bytes: 1024 * 1024, // 1 MiB
            max_result_bytes: 100 * 1024, // 100 KiB
            ttl: Duration::from_secs(60),
            ..Default::default()
        }
    }

    fn test_result(size: u64) -> CachedResult {
        CachedResult {
            data: Bytes::from(vec![0u8; size as usize]),
            cached_at: Instant::now(),
            original_latency_ms: 100,
            row_count: 10,
            size_bytes: size,
        }
    }

    #[test]
    fn test_insert_and_get() {
        let cache = ResultCache::new(test_config());
        let key = CacheKey {
            fingerprint: 12345,
            user: "test".to_string(),
            snapshot_id: 1,
        };

        cache.insert(key.clone(), test_result(1000));

        let result = cache.get(&key);
        assert!(result.is_some());
        assert_eq!(result.unwrap().size_bytes, 1000);
    }

    #[test]
    fn test_eviction_by_size() {
        let mut config = test_config();
        config.max_cache_bytes = 2000;
        let cache = ResultCache::new(config);

        // Insert entries that exceed max size
        for i in 0..5 {
            let key = CacheKey {
                fingerprint: i,
                user: "test".to_string(),
                snapshot_id: 1,
            };
            cache.insert(key, test_result(1000));
        }

        // Should have evicted some entries
        let stats = cache.stats();
        assert!(stats.total_bytes <= 2000);
    }

    #[test]
    fn test_reject_large_result() {
        let cache = ResultCache::new(test_config());
        let key = CacheKey {
            fingerprint: 1,
            user: "test".to_string(),
            snapshot_id: 1,
        };

        // Try to insert a result larger than max_result_bytes
        cache.insert(key.clone(), test_result(200 * 1024)); // 200 KiB

        // Should not be cached
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_invalidate_snapshot() {
        let cache = ResultCache::new(test_config());

        // Insert entries for two snapshots
        for i in 0..5 {
            let key = CacheKey {
                fingerprint: i,
                user: "test".to_string(),
                snapshot_id: 100,
            };
            cache.insert(key, test_result(100));
        }
        for i in 5..10 {
            let key = CacheKey {
                fingerprint: i,
                user: "test".to_string(),
                snapshot_id: 200,
            };
            cache.insert(key, test_result(100));
        }

        assert_eq!(cache.stats().entries, 10);

        // Invalidate snapshot 100
        cache.invalidate_snapshot(100);

        // Should have 5 entries left
        assert_eq!(cache.stats().entries, 5);
    }
}
