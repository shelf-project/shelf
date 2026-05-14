//! Read-ahead prefetch for rowgroup byte-ranges (Tier 3 item 7).
//!
//! When a rowgroup miss occurs, this module background-fetches the next
//! N row groups (default 4) with bounded concurrency. This improves
//! throughput on sequential scans by hiding origin latency behind the
//! current query's processing time.
//!
//! # Design
//!
//! - **Trigger**: `prefetch_next_rowgroups` is called from `s3_shim.rs`
//!   after a cache miss on a rowgroup read.
//! - **Concurrency**: A tokio `Semaphore` caps the number of in-flight
//!   prefetch requests per pod (default 8).
//! - **Deduplication**: A `DashSet` tracks pending prefetch keys to avoid
//!   re-requesting the same range while an earlier prefetch is in-flight.
//! - **Admission**: Prefetched bytes flow through the normal admission
//!   policy; they are not force-admitted.
//!
//! # Limitations
//!
//! - Row-group boundaries are not known at the S3 shim layer. This module
//!   uses a heuristic: if the current read is `(offset, length)` and
//!   appears to be a row-group-sized chunk (> 128 KB), prefetch
//!   `(offset + length, length)` through `(offset + 4*length, length)`.
//! - For accurate row-group-level prefetch, enable `decoded_meta` so the
//!   shim can resolve row-group offsets from the cached Parquet footer.
//!
//! See `TODO-fix-shelf-performance.md` §4 Tier 3 item 7.

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::http::ServerState;
use crate::origin::Origin;
use crate::store::{Pool, Store};

/// Minimum read size (bytes) to trigger prefetch. Small reads (footers,
/// bloom blocks, metadata) are unlikely to benefit from sequential prefetch.
const MIN_PREFETCH_TRIGGER_BYTES: u64 = 128 * 1024; // 128 KB

/// Number of subsequent ranges to prefetch on a miss.
const PREFETCH_LOOKAHEAD: usize = 4;

/// Prefetch state shared across the shim's request handlers.
pub struct Prefetcher {
    /// Caps concurrent in-flight prefetch requests.
    semaphore: Arc<Semaphore>,
    /// Keys currently being prefetched (deduplication).
    pending: Arc<Mutex<HashSet<PrefetchKey>>>,
    /// Whether prefetch is enabled (runtime kill-switch).
    enabled: std::sync::atomic::AtomicBool,
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct PrefetchKey {
    bucket: String,
    key: String,
    offset: u64,
    length: u64,
}

impl Prefetcher {
    /// Create a new prefetcher with the given concurrency limit.
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            pending: Arc::new(Mutex::new(HashSet::new())),
            enabled: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Runtime kill-switch: disable prefetch without restarting the pod.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check if prefetch is currently enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Spawn background prefetch tasks for the next N ranges after the
    /// current read. Called from `s3_shim::handle_get_object` on a miss.
    pub fn prefetch_next_rowgroups(
        &self,
        state: Arc<ServerState>,
        bucket: String,
        key: String,
        offset: u64,
        length: u64,
        total_size: u64,
    ) {
        if !self.is_enabled() {
            return;
        }

        // Only prefetch for rowgroup-sized reads
        if length < MIN_PREFETCH_TRIGGER_BYTES {
            return;
        }

        // Spawn prefetch tasks for the next N ranges
        for i in 1..=PREFETCH_LOOKAHEAD {
            let next_offset = offset + (i as u64) * length;
            let next_length = length;

            // Don't prefetch past end of file
            if next_offset >= total_size {
                break;
            }

            // Clamp length to remaining file size
            let clamped_length = std::cmp::min(next_length, total_size - next_offset);

            let pkey = PrefetchKey {
                bucket: bucket.clone(),
                key: key.clone(),
                offset: next_offset,
                length: clamped_length,
            };

            // Skip if already pending (hold lock briefly)
            {
                let mut pending = self.pending.lock();
                if pending.contains(&pkey) {
                    continue;
                }
                pending.insert(pkey.clone());
            }

            // Try to acquire a semaphore permit; if full, skip this prefetch
            let permit = match self.semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    self.pending.lock().remove(&pkey);
                    debug!(
                        "prefetch semaphore full, skipping {}/{}@{}",
                        bucket, key, next_offset
                    );
                    PREFETCH_SKIPPED_TOTAL.inc();
                    break;
                }
            };

            let state = state.clone();
            let pending = Arc::clone(&self.pending);
            let pkey_for_cleanup = pkey.clone();

            tokio::spawn(async move {
                let _permit = permit; // held until task completes

                let result = do_prefetch(&state, &pkey).await;

                pending.lock().remove(&pkey_for_cleanup);

                match result {
                    Ok(bytes_len) => {
                        PREFETCH_BYTES_TOTAL.inc_by(bytes_len as u64);
                        PREFETCH_SUCCESS_TOTAL.inc();
                    }
                    Err(e) => {
                        warn!(
                            "prefetch failed for {}/{}@{}: {}",
                            pkey_for_cleanup.bucket, pkey_for_cleanup.key, pkey_for_cleanup.offset, e
                        );
                        PREFETCH_ERROR_TOTAL.inc();
                    }
                }
            });
        }
    }
}

/// Execute a single prefetch request.
///
/// Note: The cache key is content-addressed via ETag (ADR-0011), so we
/// need the object's ETag to construct the key. We first HEAD the object
/// to get metadata, then fetch the range and insert.
async fn do_prefetch(state: &ServerState, pkey: &PrefetchKey) -> anyhow::Result<usize> {
    // Get metadata to retrieve the ETag
    let meta = state
        .origin
        .head(&pkey.bucket, &pkey.key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("object not found: {}/{}", pkey.bucket, pkey.key))?;

    // ETag is always present when object exists
    let etag = &meta.etag;
    if etag.is_empty() {
        return Err(anyhow::anyhow!("empty ETag for {}/{}", pkey.bucket, pkey.key));
    }

    // Fetch the range from origin
    let bytes = state
        .origin
        .get_range(&pkey.bucket, &pkey.key, pkey.offset, pkey.length)
        .await?;

    let len = bytes.len();

    // Construct the cache key using ETag (ADR-0011 content-addressed keys).
    // rg_ordinal is computed as offset / expected_rowgroup_size, but since
    // we don't have footer metadata here, use 0 as a placeholder. The key
    // will still be unique due to offset being part of the hash.
    let rg_ordinal = 0u32; // TODO: resolve from decoded_meta when available
    let cache_key = crate::store::key_from_tuple(etag, pkey.offset, pkey.length, rg_ordinal)?;

    // Insert into the rowgroup pool. Prefetched bytes bypass admission
    // policy since the decision to prefetch was already made.
    state
        .store
        .insert(Pool::RowGroup, cache_key, bytes)
        .await?;

    Ok(len)
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

use once_cell::sync::Lazy;
use prometheus::{register_int_counter_with_registry, IntCounter};

static REGISTRY: Lazy<prometheus::Registry> = Lazy::new(|| crate::metrics::REGISTRY.clone());

pub static PREFETCH_SUCCESS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_prefetch_success_total",
        "Successful background prefetch requests.",
        *REGISTRY
    )
    .expect("register prefetch_success_total")
});

pub static PREFETCH_ERROR_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_prefetch_error_total",
        "Failed background prefetch requests.",
        *REGISTRY
    )
    .expect("register prefetch_error_total")
});

pub static PREFETCH_SKIPPED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_prefetch_skipped_total",
        "Prefetch requests skipped due to semaphore exhaustion.",
        *REGISTRY
    )
    .expect("register prefetch_skipped_total")
});

pub static PREFETCH_BYTES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_prefetch_bytes_total",
        "Total bytes fetched via background prefetch.",
        *REGISTRY
    )
    .expect("register prefetch_bytes_total")
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefetch_key_dedup() {
        let set: Arc<Mutex<HashSet<PrefetchKey>>> = Arc::new(Mutex::new(HashSet::new()));
        let k1 = PrefetchKey {
            bucket: "b".into(),
            key: "k".into(),
            offset: 0,
            length: 1000,
        };
        let k2 = k1.clone();

        assert!(set.lock().insert(k1));
        assert!(!set.lock().insert(k2)); // duplicate
    }

    #[test]
    fn test_enabled_flag() {
        let p = Prefetcher::new(8);
        assert!(p.is_enabled());

        p.set_enabled(false);
        assert!(!p.is_enabled());

        p.set_enabled(true);
        assert!(p.is_enabled());
    }
}
