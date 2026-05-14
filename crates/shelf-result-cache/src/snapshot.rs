//! Snapshot ID resolution for cache key generation.
//!
//! Queries shelfd or HMS to get the current snapshot ID for a table,
//! which is combined with the query fingerprint to form the cache key.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::{debug, warn};

/// Snapshot resolver that caches snapshot IDs for tables.
#[derive(Debug)]
pub struct SnapshotResolver {
    /// Cached snapshot IDs by table FQN.
    cache: Arc<RwLock<HashMap<String, CachedSnapshot>>>,
    /// Shelfd URL for snapshot lookups.
    shelfd_url: Option<String>,
    /// Cache TTL for snapshot IDs.
    cache_ttl: Duration,
}

#[derive(Debug, Clone)]
struct CachedSnapshot {
    snapshot_id: i64,
    resolved_at: Instant,
}

impl SnapshotResolver {
    /// Create a new snapshot resolver.
    pub fn new(shelfd_url: Option<String>, cache_ttl: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            shelfd_url,
            cache_ttl,
        }
    }

    /// Get the current snapshot ID for a table.
    ///
    /// Returns the cached snapshot ID if fresh, otherwise resolves from shelfd.
    pub async fn get_snapshot_id(&self, table_fqn: &str) -> Option<i64> {
        // Check cache first
        {
            let cache = self.cache.read();
            if let Some(cached) = cache.get(table_fqn) {
                if cached.resolved_at.elapsed() < self.cache_ttl {
                    debug!(
                        table = %table_fqn,
                        snapshot_id = cached.snapshot_id,
                        "Snapshot ID cache hit"
                    );
                    return Some(cached.snapshot_id);
                }
            }
        }

        // Resolve from shelfd
        let snapshot_id = self.resolve_from_shelfd(table_fqn).await?;

        // Update cache
        {
            let mut cache = self.cache.write();
            cache.insert(
                table_fqn.to_string(),
                CachedSnapshot {
                    snapshot_id,
                    resolved_at: Instant::now(),
                },
            );
        }

        Some(snapshot_id)
    }

    /// Resolve snapshot ID from shelfd's /stats or metadata endpoint.
    async fn resolve_from_shelfd(&self, table_fqn: &str) -> Option<i64> {
        let shelfd_url = self.shelfd_url.as_ref()?;

        // TODO: Implement actual HTTP call to shelfd
        // For now, return a placeholder that changes based on table name
        // to demonstrate the concept.

        debug!(
            table = %table_fqn,
            shelfd_url = %shelfd_url,
            "Resolving snapshot ID from shelfd"
        );

        // Placeholder: hash the table name to get a consistent "snapshot ID"
        let hash = table_fqn.bytes().fold(0i64, |acc, b| {
            acc.wrapping_add(b as i64).wrapping_mul(31)
        });

        Some(hash.abs())
    }

    /// Invalidate the cached snapshot ID for a table.
    pub fn invalidate(&self, table_fqn: &str) {
        let mut cache = self.cache.write();
        if cache.remove(table_fqn).is_some() {
            debug!(table = %table_fqn, "Invalidated cached snapshot ID");
        }
    }

    /// Clear all cached snapshot IDs.
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
        debug!("Cleared all cached snapshot IDs");
    }
}

impl Default for SnapshotResolver {
    fn default() -> Self {
        Self::new(None, Duration::from_secs(60))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_snapshot_resolver_caching() {
        let resolver = SnapshotResolver::new(
            Some("http://localhost:9090".to_string()),
            Duration::from_secs(60),
        );

        let table = "catalog.schema.table";

        // First call should resolve
        let id1 = resolver.get_snapshot_id(table).await;
        assert!(id1.is_some());

        // Second call should use cache (same ID)
        let id2 = resolver.get_snapshot_id(table).await;
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn test_snapshot_resolver_invalidate() {
        let resolver = SnapshotResolver::new(
            Some("http://localhost:9090".to_string()),
            Duration::from_secs(60),
        );

        let table = "catalog.schema.table";

        // Resolve and cache
        let _id1 = resolver.get_snapshot_id(table).await;

        // Invalidate
        resolver.invalidate(table);

        // Should resolve fresh
        let _id2 = resolver.get_snapshot_id(table).await;
    }
}
