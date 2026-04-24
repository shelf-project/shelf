//! Prometheus metric registry.
//!
//! Ticket ownership:
//! - SHELF-08 — Prometheus `/metrics` exposed on the control port +
//!   OTel traces exported to Tempo. Low-cardinality label set per
//!   agents/4-shelfd-builder.md Pass 4.
//! - SHELF-06 — `shelf_hits_total` / `shelf_misses_total` by pool are
//!   the primary metric of the v0.5 gate (see
//!   `agents/out/adr/0010-v05-gate-beat-alluxio-on-rep2.md`).
//!
//! Every metric added here must also appear in `shelfd/docs/metrics.md`
//! so the Grafana dashboard (SHELF-27) stays in sync.

use once_cell::sync::Lazy;
use prometheus::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_vec_with_registry, HistogramVec, IntCounterVec, IntGaugeVec,
};

/// Global Prometheus registry.
///
/// `Lazy` is the single allowed use of global state in `shelfd` per
/// agents/4-shelfd-builder.md Pass 2 ("No global mutable state.
/// `once_cell::sync::Lazy` is allowed for metric registries only.").
pub static REGISTRY: Lazy<prometheus::Registry> = Lazy::new(prometheus::Registry::new);

/// Handle to the set of Shelf metrics. Held inside `ServerState`.
#[derive(Debug)]
pub struct Registry {
    pub hits_total: IntCounterVec,
    pub misses_total: IntCounterVec,
    pub head_hits_total: IntCounterVec,
    pub head_misses_total: IntCounterVec,
    pub errors_total: IntCounterVec,
    pub bytes_used: IntGaugeVec,
    pub request_seconds: HistogramVec,
    /// SHELF-18 — disk-tier hits for pools that run as a Foyer
    /// `HybridCache`. Only the `rowgroup` pool ever observes a
    /// non-zero value today; the label is kept so future pools
    /// can join without a metric rename.
    pub disk_hits_total: IntCounterVec,
    /// SHELF-18 — disk-tier misses (memory miss → disk miss).
    pub disk_misses_total: IntCounterVec,
    /// SHELF-18 — best-effort bytes resident on the NVMe tier. See
    /// `FoyerStore::disk_bytes_used` for the approximation used.
    pub disk_bytes_used: IntGaugeVec,
    /// SHELF-18 — NVMe quota from `pools.rowgroup.nvme_bytes`.
    pub disk_bytes_capacity: IntGaugeVec,
}

impl Registry {
    /// Register every Shelf metric. Safe to call once per process.
    pub fn init() -> crate::Result<Self> {
        let hits_total = register_int_counter_vec_with_registry!(
            "shelf_hits_total",
            "Cache hits, partitioned by Foyer pool (see ADR-0008).",
            &["pool"],
            REGISTRY
        )
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("register hits: {e}")))?;

        let misses_total = register_int_counter_vec_with_registry!(
            "shelf_misses_total",
            "Cache misses that fell through to origin.",
            &["pool"],
            REGISTRY
        )
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("register misses: {e}")))?;

        let head_hits_total = register_int_counter_vec_with_registry!(
            "shelf_head_hits_total",
            "HEAD /cache/... responses served from the HEAD-LRU (SHELF-07).",
            &["pool"],
            REGISTRY
        )
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("register head_hits: {e}")))?;

        let head_misses_total = register_int_counter_vec_with_registry!(
            "shelf_head_misses_total",
            "HEAD /cache/... responses that required a live HeadObject.",
            &["pool"],
            REGISTRY
        )
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("register head_misses: {e}")))?;

        let errors_total = register_int_counter_vec_with_registry!(
            "shelfd_error_total",
            "Typed error counter. Low-cardinality; see error::Error::component.",
            &["component", "kind"],
            REGISTRY
        )
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("register errors: {e}")))?;

        let bytes_used = register_int_gauge_vec_with_registry!(
            "shelf_bytes_used",
            "Bytes currently held in each pool (DRAM + NVMe combined).",
            &["pool", "tier"],
            REGISTRY
        )
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("register bytes_used: {e}")))?;

        let request_seconds = register_histogram_vec_with_registry!(
            "shelf_request_seconds",
            "End-to-end request latency. Label `path` is /cache, /stats …",
            &["path", "outcome"],
            prometheus::exponential_buckets(0.0005, 2.0, 16)
                .map_err(|e| crate::Error::Internal(anyhow::anyhow!("bucket gen: {e}")))?,
            REGISTRY
        )
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("register req_seconds: {e}")))?;

        // SHELF-18 disk-tier series.
        //
        // These live as module-level `Lazy` statics so the
        // `FoyerStore::get` hot path can bump them without owning
        // an `Arc<Registry>` handle. `Registry::init` clones the
        // handles in so `/metrics` touches and the `Registry`
        // struct surface stay symmetric with the other series.
        let disk_hits_total = DISK_HITS_TOTAL.clone();
        let disk_misses_total = DISK_MISSES_TOTAL.clone();
        let disk_bytes_used = DISK_BYTES_USED.clone();
        let disk_bytes_capacity = DISK_BYTES_CAPACITY.clone();

        Ok(Self {
            hits_total,
            misses_total,
            head_hits_total,
            head_misses_total,
            errors_total,
            bytes_used,
            request_seconds,
            disk_hits_total,
            disk_misses_total,
            disk_bytes_used,
            disk_bytes_capacity,
        })
    }
}

/// SHELF-18 disk-tier counters / gauges, registered lazily into the
/// global [`REGISTRY`].
///
/// Exposed as module-level statics so modules that do not hold an
/// `Arc<Registry>` (e.g. the hot-path `store.rs`) can increment them
/// directly. `Registry::init` clones these handles for the `Registry`
/// struct so consumers that already read from the struct keep
/// working.
pub static DISK_HITS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_disk_hits_total",
        "Cache hits served from the NVMe tier of a hybrid pool (SHELF-18).",
        &["pool"],
        REGISTRY
    )
    .expect("register disk_hits")
});

pub static DISK_MISSES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_disk_misses_total",
        "Misses that reached the NVMe tier of a hybrid pool and still missed.",
        &["pool"],
        REGISTRY
    )
    .expect("register disk_misses")
});

pub static DISK_BYTES_USED: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_disk_bytes_used",
        "Best-effort bytes held on the NVMe tier of each pool.",
        &["pool"],
        REGISTRY
    )
    .expect("register disk_bytes_used")
});

pub static DISK_BYTES_CAPACITY: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_disk_bytes_capacity",
        "Configured NVMe capacity for each pool (pools.<pool>.nvme_bytes).",
        &["pool"],
        REGISTRY
    )
    .expect("register disk_bytes_capacity")
});

/// Stable list of metric series `shelfd` exposes on `/metrics` in the
/// Phase-0 gate build. Kept as module-level data so `docs/metrics.md`
/// and the tests can both reference a single source of truth; the
/// integration dashboard relies on these names.
pub const EXPOSED_SERIES: &[&str] = &[
    "shelf_hits_total",
    "shelf_misses_total",
    "shelf_head_hits_total",
    "shelf_head_misses_total",
    "shelfd_error_total",
    "shelf_bytes_used",
    "shelf_request_seconds",
    // SHELF-18 — disk-tier telemetry.
    "shelf_disk_hits_total",
    "shelf_disk_misses_total",
    "shelf_disk_bytes_used",
    "shelf_disk_bytes_capacity",
];

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::core::Collector;
    use std::collections::HashSet;
    use std::sync::Once;

    static INIT: Once = Once::new();

    fn ensure_registered() -> &'static Registry {
        // `Registry::init` panics on the second call because the
        // underlying `prometheus::Registry` rejects duplicate
        // registrations. Gate with `Once` so multiple tests in the
        // same binary can share a single registration.
        static HANDLE: once_cell::sync::OnceCell<Registry> = once_cell::sync::OnceCell::new();
        INIT.call_once(|| {
            let reg = Registry::init().expect("register metrics");
            HANDLE.set(reg).expect("set handle");
        });
        HANDLE.get().expect("handle set")
    }

    /// Regression guard: every series listed in `EXPOSED_SERIES` must
    /// be registered as a Collector on the global `REGISTRY`. We
    /// inspect collector descriptors directly because
    /// `Registry::gather()` prunes `*Vec` families with no observed
    /// children — asserting on descriptors proves the series is
    /// *registered* regardless of whether a label has been touched.
    #[test]
    fn registry_exposes_documented_series() {
        let reg = ensure_registered();
        let mut names: HashSet<String> = HashSet::new();
        for collector in [
            reg.hits_total.desc(),
            reg.misses_total.desc(),
            reg.head_hits_total.desc(),
            reg.head_misses_total.desc(),
            reg.errors_total.desc(),
            reg.bytes_used.desc(),
            reg.request_seconds.desc(),
            reg.disk_hits_total.desc(),
            reg.disk_misses_total.desc(),
            reg.disk_bytes_used.desc(),
            reg.disk_bytes_capacity.desc(),
        ] {
            for d in collector {
                names.insert(d.fq_name.clone());
            }
        }
        for want in EXPOSED_SERIES {
            assert!(
                names.contains(*want),
                "registry missing {want:?}; registered: {names:?}",
            );
        }
    }

    /// Secondary regression guard: once every metric has at least one
    /// observed child, the `/metrics` scrape must include the full
    /// documented series set. This mirrors what a Prometheus scrape
    /// actually sees in production once traffic begins flowing.
    #[test]
    fn metrics_scrape_contains_documented_series_after_touch() {
        let reg = ensure_registered();
        reg.hits_total.with_label_values(&["metadata"]).inc_by(0);
        reg.misses_total.with_label_values(&["metadata"]).inc_by(0);
        reg.head_hits_total
            .with_label_values(&["metadata"])
            .inc_by(0);
        reg.head_misses_total
            .with_label_values(&["metadata"])
            .inc_by(0);
        reg.errors_total
            .with_label_values(&["test", "test"])
            .inc_by(0);
        reg.bytes_used
            .with_label_values(&["metadata", "dram"])
            .set(0);
        reg.request_seconds
            .with_label_values(&["/cache", "hit"])
            .observe(0.0);
        reg.disk_hits_total
            .with_label_values(&["rowgroup"])
            .inc_by(0);
        reg.disk_misses_total
            .with_label_values(&["rowgroup"])
            .inc_by(0);
        reg.disk_bytes_used.with_label_values(&["rowgroup"]).set(0);
        reg.disk_bytes_capacity
            .with_label_values(&["rowgroup"])
            .set(0);

        let families = REGISTRY.gather();
        let names: HashSet<String> = families.iter().map(|f| f.get_name().to_owned()).collect();
        for want in EXPOSED_SERIES {
            assert!(
                names.contains(*want),
                "`/metrics` missing {want:?}; scraped: {names:?}",
            );
        }
    }
}
