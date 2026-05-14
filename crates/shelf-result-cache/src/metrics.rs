//! Prometheus metrics for shelf-result-cache.

use once_cell::sync::Lazy;
use prometheus::{
    register_int_counter, register_int_gauge, IntCounter, IntGauge,
};

/// Total cache hits.
pub static CACHE_HITS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "shelf_result_cache_hits_total",
        "Number of cache hits."
    )
    .expect("register cache_hits_total")
});

/// Total cache misses.
pub static CACHE_MISSES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "shelf_result_cache_misses_total",
        "Number of cache misses."
    )
    .expect("register cache_misses_total")
});

/// Total cache inserts.
pub static CACHE_INSERTS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "shelf_result_cache_inserts_total",
        "Number of results inserted into cache."
    )
    .expect("register cache_inserts_total")
});

/// Total cache evictions.
pub static CACHE_EVICTIONS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "shelf_result_cache_evictions_total",
        "Number of results evicted from cache."
    )
    .expect("register cache_evictions_total")
});

/// Total cache expirations.
pub static CACHE_EXPIRATIONS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "shelf_result_cache_expirations_total",
        "Number of results expired from cache."
    )
    .expect("register cache_expirations_total")
});

/// Total cache rejections (result too large).
pub static CACHE_REJECTIONS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "shelf_result_cache_rejections_total",
        "Number of results rejected from cache (too large)."
    )
    .expect("register cache_rejections_total")
});

/// Total cache invalidations.
pub static CACHE_INVALIDATIONS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "shelf_result_cache_invalidations_total",
        "Number of cache entries invalidated due to snapshot changes."
    )
    .expect("register cache_invalidations_total")
});

/// Current cache size in bytes.
pub static CACHE_BYTES_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "shelf_result_cache_bytes_total",
        "Current cache size in bytes."
    )
    .expect("register cache_bytes_total")
});

/// Requests forwarded to Trino.
pub static REQUESTS_FORWARDED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "shelf_result_cache_requests_forwarded_total",
        "Number of requests forwarded to Trino."
    )
    .expect("register requests_forwarded_total")
});

/// Requests served from cache.
pub static REQUESTS_SERVED_FROM_CACHE_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "shelf_result_cache_requests_served_from_cache_total",
        "Number of requests served from cache."
    )
    .expect("register requests_served_from_cache_total")
});

/// Latency saved by cache hits (cumulative milliseconds).
pub static LATENCY_SAVED_MS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "shelf_result_cache_latency_saved_ms_total",
        "Cumulative milliseconds of latency saved by cache hits."
    )
    .expect("register latency_saved_ms_total")
});
