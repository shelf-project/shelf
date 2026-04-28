//! MV → key registry (Track H5).
//!
//! Maps content-addressed cache keys back to the Iceberg materialized
//! view they were pinned on behalf of so the hit-accounting path
//! (`hits_total` / `s3_shim_response_bytes_total`) can *also* fire the
//! MV-scoped counters [`crate::metrics::MV_HITS_TOTAL`] and
//! [`crate::metrics::MV_BYTES_SERVED_TOTAL`].
//!
//! The H3 mv-pin-watcher is the only writer: on every
//! `CREATE_MATERIALIZED_VIEW` / `ALTER_MATERIALIZED_VIEW` HMS event
//! it resolves every file the snapshot references, content-addresses
//! it with the same hash `gen_pin_list` uses, and calls `POST
//! /admin/pin` with a `{pool, key_hex, mv_name}` body. The handler
//! forwards to [`MvRegistry::pin`].
//!
//! On every served `GET /cache/*` response the shim looks the key up
//! and, if it resolves to an MV, increments the two H5 counters.
//! Non-MV keys never touch this map so the common-case overhead is
//! one `RwLock` read + one `HashMap` lookup.
//!
//! # Cardinality
//!
//! The `mv_name` label is bounded by the number of MVs in a cluster
//! (typically <500; the hard ceiling before Prometheus becomes
//! unhappy is ~10k). We do not record per-file labels — that would
//! explode cardinality without answering any question a Grafana
//! panel actually asks.
//!
//! # Memory
//!
//! Each entry is (SHA-256 hex, MV name) ≈ 64 B + ~48 B string =
//! 112 B. 1M pinned files costs 112 MB, which is well under any
//! shelfd budget; in practice a cluster has <100k MV files.

use std::collections::HashMap;
use std::sync::RwLock;

/// In-memory registry keyed by the hex SHA-256 content address used
/// everywhere else in shelfd. Values are the fully-qualified MV name
/// (`schema.table`).
#[derive(Debug, Default)]
pub struct MvRegistry {
    by_key: RwLock<HashMap<String, String>>,
}

impl MvRegistry {
    /// Build an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `key_hex` as belonging to `mv_name`. Idempotent;
    /// re-pinning the same key with the same MV is a no-op, and
    /// re-pinning with a different MV overwrites — the last
    /// `ALTER MATERIALIZED VIEW` wins, which matches Iceberg
    /// snapshot semantics.
    pub fn pin(&self, key_hex: &str, mv_name: &str) {
        let mut guard = self.by_key.write().expect("mv_registry poisoned");
        guard.insert(key_hex.to_owned(), mv_name.to_owned());
    }

    /// Drop the registration for `key_hex`. No-op if the key isn't
    /// registered. Used by eviction + admin unpin paths.
    pub fn unpin(&self, key_hex: &str) {
        let mut guard = self.by_key.write().expect("mv_registry poisoned");
        guard.remove(key_hex);
    }

    /// Resolve a content-addressed key to its MV, if any.
    pub fn mv_of(&self, key_hex: &str) -> Option<String> {
        self.by_key
            .read()
            .expect("mv_registry poisoned")
            .get(key_hex)
            .cloned()
    }

    /// Record an MV-scoped cache hit. Bumps
    /// [`crate::metrics::MV_HITS_TOTAL`] and
    /// [`crate::metrics::MV_BYTES_SERVED_TOTAL`] only when `key_hex`
    /// resolves to an MV; non-MV keys are ignored so the counter
    /// surface remains sparse.
    pub fn record_hit(&self, key_hex: &str, response_bytes: u64) {
        let Some(name) = self.mv_of(key_hex) else {
            return;
        };
        crate::metrics::MV_HITS_TOTAL
            .with_label_values(&[name.as_str()])
            .inc();
        crate::metrics::MV_BYTES_SERVED_TOTAL
            .with_label_values(&[name.as_str()])
            .inc_by(response_bytes);
    }

    /// Number of keys currently registered. O(1).
    pub fn len(&self) -> usize {
        self.by_key.read().expect("mv_registry poisoned").len()
    }

    /// True when the registry has zero keys. Convenience for the
    /// ServerState bootstrap — a fresh pod starts empty and only
    /// picks up entries from the first mv-pin-watcher tick.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_then_resolve_returns_mv_name() {
        let reg = MvRegistry::new();
        reg.pin("abc", "analytics.top_ten");
        assert_eq!(reg.mv_of("abc"), Some("analytics.top_ten".into()));
        assert_eq!(reg.mv_of("missing"), None);
    }

    #[test]
    fn alter_overwrites_prior_mapping() {
        let reg = MvRegistry::new();
        reg.pin("abc", "analytics.v1");
        reg.pin("abc", "analytics.v2");
        assert_eq!(reg.mv_of("abc"), Some("analytics.v2".into()));
    }

    #[test]
    fn unpin_removes_mapping() {
        let reg = MvRegistry::new();
        reg.pin("abc", "analytics.top_ten");
        reg.unpin("abc");
        assert_eq!(reg.mv_of("abc"), None);
    }

    #[test]
    fn record_hit_is_noop_for_unregistered_key() {
        let reg = MvRegistry::new();
        reg.record_hit("not-an-mv", 1_000);
        assert!(reg.is_empty());
    }

    #[test]
    fn record_hit_bumps_counters_for_registered_key() {
        let reg = MvRegistry::new();
        reg.pin("xyz", "analytics.events_weekly");

        let hits_before = crate::metrics::MV_HITS_TOTAL
            .with_label_values(&["analytics.events_weekly"])
            .get();
        let bytes_before = crate::metrics::MV_BYTES_SERVED_TOTAL
            .with_label_values(&["analytics.events_weekly"])
            .get();

        reg.record_hit("xyz", 123_456);

        let hits_after = crate::metrics::MV_HITS_TOTAL
            .with_label_values(&["analytics.events_weekly"])
            .get();
        let bytes_after = crate::metrics::MV_BYTES_SERVED_TOTAL
            .with_label_values(&["analytics.events_weekly"])
            .get();

        assert_eq!(hits_after - hits_before, 1);
        assert_eq!(bytes_after - bytes_before, 123_456);
    }
}
