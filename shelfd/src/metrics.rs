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
    register_int_counter_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec,
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
    /// Track B3 — bytes returned by origin GET/HEAD requests,
    /// partitioned by bucket + outcome (`hit` is unused here;
    /// outcomes are `ok` / `not_found` / `error` / `timeout`).
    /// Subtract from `shelf_s3_shim_response_bytes_total` to see
    /// how many bytes the cache saved going over the wire.
    pub origin_request_bytes_total: IntCounterVec,
    /// Track B3 — origin latency histogram, one observation per
    /// `get_range` / `head` call.
    pub origin_request_seconds: HistogramVec,
    /// Track B3 — bytes the S3 shim returned to Trino. Partitioned
    /// by outcome (`hit_memory` / `hit_disk` / `miss` / `passthrough`).
    /// This is the numerator of the cache byte-efficiency KPI:
    ///   1 - (shelf_origin_request_bytes_total / shelf_s3_shim_response_bytes_total)
    pub s3_shim_response_bytes_total: IntCounterVec,
}

/// Track G-11 — global handle for `shelf_hits_total` so the
/// background warm-sampler (`crate::warm_sampler`) can read the
/// per-pool counter without holding an `Arc<Registry>`. `Registry::init`
/// clones this handle into the struct so existing call sites keep
/// working.
pub static HITS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_hits_total",
        "Cache hits, partitioned by Foyer pool (see ADR-0008).",
        &["pool"],
        REGISTRY
    )
    .expect("register hits_total")
});

/// Track G-11 — global handle for `shelf_misses_total`. See
/// [`HITS_TOTAL`] for rationale.
pub static MISSES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_misses_total",
        "Cache misses that fell through to origin.",
        &["pool"],
        REGISTRY
    )
    .expect("register misses_total")
});

impl Registry {
    /// Register every Shelf metric. Safe to call once per process.
    pub fn init() -> crate::Result<Self> {
        let hits_total = HITS_TOTAL.clone();
        let misses_total = MISSES_TOTAL.clone();

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

        // Track B3 — origin + shim byte / latency telemetry.
        let origin_request_bytes_total = ORIGIN_REQUEST_BYTES_TOTAL.clone();
        let origin_request_seconds = ORIGIN_REQUEST_SECONDS.clone();
        let s3_shim_response_bytes_total = S3_SHIM_RESPONSE_BYTES_TOTAL.clone();

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
            origin_request_bytes_total,
            origin_request_seconds,
            s3_shim_response_bytes_total,
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

/// Track B3 — bytes returned by origin calls. Label `outcome` is
/// one of `ok`, `not_found`, `error`, `timeout`. Label `op` is
/// `get_range` or `head`. `bucket` is cardinality-bounded (one per
/// origin client, typically 1-5 in practice).
pub static ORIGIN_REQUEST_BYTES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_origin_request_bytes_total",
        "Bytes returned by origin (S3) requests, excludes HTTP headers.",
        &["bucket", "op", "outcome"],
        REGISTRY
    )
    .expect("register origin_request_bytes_total")
});

/// Track B3 — origin latency in seconds. Histogram buckets chosen
/// to cover ~1 ms (cache-side miss retry) up to 30 s (request
/// timeout).
pub static ORIGIN_REQUEST_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_with_registry!(
        "shelf_origin_request_seconds",
        "End-to-end origin request latency, per op + outcome.",
        &["bucket", "op", "outcome"],
        prometheus::exponential_buckets(0.001, 2.0, 15).expect("origin bucket gen"),
        REGISTRY
    )
    .expect("register origin_request_seconds")
});

/// Track B3 — bytes the S3 shim returned to Trino. `outcome`
/// mirrors the cache outcome: `hit_memory`, `hit_disk`, `miss`,
/// `passthrough`. `op` is `get_object` / `head_object`.
pub static S3_SHIM_RESPONSE_BYTES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_s3_shim_response_bytes_total",
        "Response body bytes served by the S3 shim, by op + outcome.",
        &["op", "outcome"],
        REGISTRY
    )
    .expect("register s3_shim_response_bytes_total")
});

/// Track E8 — admission-policy outcomes. `decision` is one of
/// `admit`, `reject_size`, `reject_model`, `reject_other`. `pool`
/// matches the cache pool label used elsewhere.
pub static ADMISSIONS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_admissions_total",
        "Admission-policy decisions, per pool.",
        &["pool", "decision"],
        REGISTRY
    )
    .expect("register admissions_total")
});

/// Track E8 — eviction cause. `reason` is one of
/// `capacity`, `ttl`, `admin`, `unpin`, `reload`. The counter is
/// intentionally coarse; Foyer's own internal eviction callbacks
/// feed it from `store.rs`.
pub static EVICTIONS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_evictions_total",
        "Cache evictions, per pool + reason.",
        &["pool", "reason"],
        REGISTRY
    )
    .expect("register evictions_total")
});

/// Track E8 — live single-flight fan-in count. Gauge because it's
/// a snapshot; counters would require a (pool, key) cardinality
/// explosion. See [`FoyerStore::get_or_fetch`].
pub static INFLIGHT_SINGLEFLIGHT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_inflight_singleflight",
        "In-flight single-flight fetches per pool.",
        &["pool"],
        REGISTRY
    )
    .expect("register inflight_singleflight")
});

/// SHELF-30 — count of GET requests that registered as a coalesce
/// leader, partitioned by Foyer pool. Pair with
/// [`COALESCE_FOLLOWERS_TOTAL`] to compute the per-pool follower
/// fan-in ratio (followers / leaders) — the hit-ratio of the
/// range-coalesce layer itself, independent of Foyer.
pub static COALESCE_LEADERS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_coalesce_leaders_total",
        "SHELF-30 — GET requests registered as a coalesce leader, per Foyer pool.",
        &["pool"],
        REGISTRY
    )
    .expect("register coalesce_leaders_total")
});

/// SHELF-30 — count of GET requests that joined an in-flight leader
/// and sliced its payload, per pool. Each follower represents one
/// origin GET (and one Foyer insert) that did NOT happen.
pub static COALESCE_FOLLOWERS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_coalesce_followers_total",
        "SHELF-30 — GET requests that joined an in-flight leader and sliced its bytes.",
        &["pool"],
        REGISTRY
    )
    .expect("register coalesce_followers_total")
});

/// SHELF-30 — bytes returned to a follower from a leader's payload
/// without a fresh origin GET. Use as the numerator of the SHELF-30
/// byte-savings panel; subtract from `shelf_origin_request_bytes_total`
/// at the same scrape to estimate `$ saved` against the AWS S3
/// `$0.0004 / 1k requests` GET unit cost.
pub static COALESCE_FOLLOWER_BYTES_SAVED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_coalesce_follower_bytes_saved_total",
        "SHELF-30 — bytes served to a follower from the leader's payload, per pool.",
        &["pool"],
        REGISTRY
    )
    .expect("register coalesce_follower_bytes_saved_total")
});

/// SHELF-30 — count of follower attempts that fell through to the
/// standard fetch path because the leader either dropped its guard
/// without completing, returned an error, or returned a payload
/// shorter than the follower's expected window. The `reason` label
/// is one of: `leader_dropped`, `leader_error`, `truncated`. Treat a
/// non-zero rate as a correctness signal — followers must not silently
/// produce wrong bytes; the fall-through is the safe default but a
/// sustained rate means leader bookkeeping has a bug.
pub static COALESCE_FALLTHROUGH_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_coalesce_fallthrough_total",
        "SHELF-30 — followers that fell through to the standard fetch path, per pool + reason.",
        &["pool", "reason"],
        REGISTRY
    )
    .expect("register coalesce_fallthrough_total")
});

/// Track E7 — per-fingerprint query count. `fingerprint` is the
/// canonicalised jsonPlan fingerprint the plugin tags on each
/// request via an `X-Shelf-Query-Fingerprint` HTTP header (or, in
/// absence, derives from the split identifier). `tenant` is the
/// Trino resource group or user prefix we report cost against.
///
/// Cardinality cap: the plugin truncates unique fingerprints to the
/// top-200 by rolling window; anything outside the cap is mapped to
/// the sentinel `other`. This keeps the series count bounded even
/// under pathological one-shot workloads.
pub static QUERIES_SERVED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_queries_served_total",
        "Queries served by shelf, grouped by jsonPlan fingerprint + tenant. \
         Feeds the MV advisor (H1) and the $/query cost dashboard.",
        &["fingerprint", "tenant"],
        REGISTRY
    )
    .expect("register queries_served_total")
});

/// Track E7 — per-fingerprint bytes saved by cache hits. Bytes
/// saved = bytes served from shelf that did **not** go to the S3
/// origin. Paired with `QUERIES_SERVED_TOTAL` to compute
/// bytes-saved-per-query, which is the primary signal the MV
/// advisor (H1) uses to rank candidate materialised views.
pub static BYTES_SAVED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_bytes_saved_total",
        "Bytes served out of the shelf cache (i.e. saved from the S3 origin), \
         grouped by jsonPlan fingerprint + tenant. Combine with \
         shelf_queries_served_total for a per-fingerprint $/query picture.",
        &["fingerprint", "tenant"],
        REGISTRY
    )
    .expect("register bytes_saved_total")
});

/// Track H5 — per-MV hit count. `mv_name` is the fully-qualified
/// materialized view name (`schema.table`) resolved from the pinned
/// file set maintained by H3's mv-pin-watcher. Bounded by the number
/// of MVs published in a cluster (typically <500 in production) so
/// cardinality is a non-issue; queries that touch an unpinned file
/// are not counted here — this series intentionally *only* fires
/// on MV-backed hits so the numerator matches "work the MV saved
/// us", which is what H1's advisor and the $/query dashboard want.
pub static MV_HITS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_mv_hits_total",
        "Cache hits served from a pinned Iceberg materialized view, \
         per fully-qualified MV name. Incremented when a /cache GET \
         resolves a key that the H3 mv-pin-watcher registered as \
         belonging to an MV snapshot.",
        &["mv_name"],
        REGISTRY
    )
    .expect("register mv_hits_total")
});

/// Track H5 — per-MV bytes returned to Trino from the cache. Paired
/// with `MV_HITS_TOTAL` to drive the "MV served bytes / MV hits"
/// panel (average rowgroup size per MV) and the "MV served bytes /
/// origin bytes" panel (how much origin traffic the MV killed).
pub static MV_BYTES_SERVED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_mv_bytes_served_total",
        "Bytes served from a pinned Iceberg materialized view, per \
         fully-qualified MV name. Excludes HTTP headers; matches the \
         semantics of shelf_s3_shim_response_bytes_total but scoped \
         to MV-backed hits only.",
        &["mv_name"],
        REGISTRY
    )
    .expect("register mv_bytes_served_total")
});

/// Track G-4 — per-table hit counter. Adds a `table` label that is
/// derived in the S3 shim from the Iceberg-on-S3 path layout
/// (`<bucket>/<schema>/<table>/{data,metadata}/...`). Carried as a
/// **separate** series — not a new label on `shelf_hits_total` — so
/// existing PromQL (`sum(shelf_hits_total)`, alert rules, dashboard
/// panels) keeps the exact label set it has today.
///
/// Cardinality: cardinality is the prod table count (≤ ~500 per
/// the cdp catalog as of 2026-04-27) × 2 pools = ≤ 1_000 series
/// per metric. Unparsed keys (e.g. `.alluxio_s3_api_metadata/*`
/// from prior deployments, presigned junk, manifest temp files)
/// fold into the sentinel label `other` so a freshly-deployed
/// daemon never explodes its label set.
pub static HITS_BY_TABLE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_hits_by_table_total",
        "Cache hits, partitioned by Foyer pool + Iceberg table \
         (`schema.table`) parsed from the S3 key. Layered alongside \
         shelf_hits_total so existing dashboards keep working; \
         cardinality is bounded by the prod table count.",
        &["pool", "table"],
        REGISTRY
    )
    .expect("register hits_by_table_total")
});

/// Track G-4 companion — per-table miss counter. Same labelling
/// convention as [`HITS_BY_TABLE_TOTAL`]. Together they answer
/// "which dashboard / pipeline is cold?" without needing to
/// cross-join Trino query logs against shelf metrics.
pub static MISSES_BY_TABLE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_misses_by_table_total",
        "Cache misses, partitioned by Foyer pool + Iceberg table.",
        &["pool", "table"],
        REGISTRY
    )
    .expect("register misses_by_table_total")
});

/// SHELF-42 — per-tag hit counter. `tag` is the URL-encoded JSON wire
/// form normalised by lexicographic key order; values above the
/// per-pod cardinality cap fold into the sentinel `"other"` (mirroring
/// `HITS_BY_TABLE_TOTAL`). Carried as a SEPARATE series — not a new
/// label on `shelf_hits_total` — so existing PromQL stays valid and
/// the cardinality budget is opt-in via `cache.abTag.enabled`.
///
/// The receive path (`crate::ab_tag::AbTagState`) only resolves a
/// non-`None` tag label when `enabled=true`, so a freshly deployed
/// shelfd that has not opted into tagging publishes this series with
/// zero non-`none` children.
pub static HITS_BY_TAG_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_hits_by_tag_total",
        "Cache hits, partitioned by Foyer pool + A/B tag (SHELF-42). \
         The `tag` label is the canonical wire form of the request's \
         X-Shelf-Tag header, or `none` when the header was absent / \
         feature-disabled, or `other` when the per-pod cardinality \
         cap fired.",
        &["pool", "tag"],
        REGISTRY
    )
    .expect("register hits_by_tag_total")
});

/// SHELF-42 companion — per-tag miss counter. Same conventions as
/// [`HITS_BY_TAG_TOTAL`].
pub static MISSES_BY_TAG_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_misses_by_tag_total",
        "Cache misses, partitioned by Foyer pool + A/B tag (SHELF-42).",
        &["pool", "tag"],
        REGISTRY
    )
    .expect("register misses_by_tag_total")
});

/// SHELF-42 — per-tag bytes the S3 shim returned to Trino. Mirrors
/// `S3_SHIM_RESPONSE_BYTES_TOTAL` plus a `tag` dimension so dashboards
/// can split byte-efficiency by experiment cohort. Same `tag` label
/// rules as the per-tag hit/miss counters.
pub static S3_SHIM_RESPONSE_BYTES_BY_TAG_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_s3_shim_response_bytes_by_tag_total",
        "Response body bytes served by the S3 shim (SHELF-22) \
         partitioned by op + outcome + A/B tag (SHELF-42). \
         The `tag` label follows the same `none` / `other` / wire-form \
         rules as `shelf_hits_by_tag_total`.",
        &["op", "outcome", "tag"],
        REGISTRY
    )
    .expect("register s3_shim_response_bytes_by_tag_total")
});

/// SHELF-23 — peer-fetch outcome counters.
///
/// On a local cache miss we may race a peer (the HRW primary) against
/// origin S3. Each request increments exactly one of the four
/// peer-fetch counters so the operator-facing payoff ratio
/// `peer_hit_total / sum(peer_*_total)` is well-defined per pool.
/// Wired in `s3_shim::handle_get_object` and `store::get_or_fetch`.
pub static PEER_HIT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_peer_hit_total",
        "Local-miss reads served from a peer pod (HRW primary) instead of origin.",
        &["pool"],
        REGISTRY
    )
    .expect("register peer_hit_total")
});

/// SHELF-23 — peer probe returned `Miss` (peer does not hold the key)
/// or its body fetch found a stale slot. Caller falls through to the
/// already-running origin fetch.
pub static PEER_MISS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_peer_miss_total",
        "Local-miss reads where the HRW primary peer reported miss; \
         caller fell through to origin.",
        &["pool"],
        REGISTRY
    )
    .expect("register peer_miss_total")
});

/// SHELF-23 — peer probe deadline elapsed before a verdict. Caller
/// falls through to origin. A high rate here usually means a peer
/// is overloaded (probe latency > 10 ms p99 on same-AZ pod network)
/// rather than unreachable.
pub static PEER_TIMEOUT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_peer_timeout_total",
        "Local-miss reads where the peer probe / fetch exceeded the \
         configured deadline; caller fell through to origin.",
        &["pool"],
        REGISTRY
    )
    .expect("register peer_timeout_total")
});

/// SHELF-23 — peer returned a non-2xx, the body decode failed, or a
/// network-layer error short-circuited the probe. `kind` lets the
/// dashboard split transient transport failures (`network`) from
/// programmer-visible bugs (`decode`, `status_5xx`).
pub static PEER_ERROR_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_peer_error_total",
        "Local-miss reads where the peer probe / fetch failed with a \
         non-timeout error; caller fell through to origin.",
        &["pool", "kind"],
        REGISTRY
    )
    .expect("register peer_error_total")
});

/// SHELF-23 — origin agreed our cached ETag is still current; we
/// served from the local cache without a body transfer. This is the
/// happy path for the cross-pod write-coherence check: a 5 ms
/// network round-trip in exchange for snapshot-correct reads.
pub static CONDITIONAL_NOT_MODIFIED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_conditional_not_modified_total",
        "Local cache hits where the ETag-conditional GET to origin \
         returned 304 Not Modified; bytes were served from cache.",
        &["pool"],
        REGISTRY
    )
    .expect("register conditional_not_modified_total")
});

/// SHELF-23 — origin reported a different ETag than the one in our
/// local cache (a cross-pod PUT, an out-of-band rewrite, or first
/// observation of a key after restart). We invalidated the local
/// entry and served the fresh body. A high rate here means writers
/// are racing readers; investigate whether the freshness window
/// is appropriate for the workload.
pub static CONDITIONAL_MODIFIED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_conditional_modified_total",
        "Local cache hits where the ETag-conditional GET to origin \
         returned 200 OK with a new ETag; cache was invalidated and \
         the fresh body was served.",
        &["pool"],
        REGISTRY
    )
    .expect("register conditional_modified_total")
});

/// SHELF-23 — local cache hits where the freshness-window
/// optimisation (≥ N consecutive 304s within the trust window) let
/// the shim skip the conditional GET entirely. Steady-state on a
/// hot, stable working set, this counter dominates by 1–2 orders of
/// magnitude over `shelf_conditional_not_modified_total`.
pub static CONDITIONAL_SKIPPED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_conditional_skipped_total",
        "Local cache hits where the freshness window let the shim \
         skip the ETag-conditional GET round-trip.",
        &["pool"],
        REGISTRY
    )
    .expect("register conditional_skipped_total")
});

/// SHELF-23 — the conditional GET itself failed (origin error,
/// timeout, or a malformed 304 the client couldn't classify). The
/// shim falls back to serving the cached bytes — the prior
/// content-addressed key is still valid until proven otherwise.
pub static CONDITIONAL_ERROR_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_conditional_error_total",
        "Local cache hits where the ETag-conditional GET to origin \
         returned an error or timed out; cached bytes were served \
         as a fallback.",
        &["pool"],
        REGISTRY
    )
    .expect("register conditional_error_total")
});

/// Track G-10 — Foyer / pool engine resets that wiped in-memory or
/// on-disk state without a process restart. The post-cutover snapshot
/// 2026-04-27 caught `shelf_hits_total` rolling back to 0 multiple
/// times on shelf-2 with no pod restart, suggesting Foyer was
/// re-initialising one of the pools mid-flight. There was no metric
/// to confirm or alert on that, so the symptom only surfaced via
/// hand-eyeballing dashboards. `reason` is one of:
///   `pool_open_retry`  — `FoyerStore::open` retried after a Foyer
///                        device init failure.
///   `nvme_format`      — disk ring was reformatted (e.g. UFS root
///                        change, Foyer compaction abort).
///   `oom_recovery`     — pool was rebuilt after a controlled
///                        eviction loop broke containment.
///   `manual`           — operator-triggered via `POST /admin/reset`
///                        (when SHELF-23 surfaces it).
///   `other`            — unclassified; investigate.
pub static ENGINE_RESETS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_engine_resets_total",
        "Foyer pool engine resets that wiped resident state without \
         a process restart. Non-zero on a healthy cluster is a paging \
         signal: dashboards must alert at rate > 0 over 15m.",
        &["pool", "reason"],
        REGISTRY
    )
    .expect("register engine_resets_total")
});

/// Track G-11 — wall-clock seconds from pod ready until the rolling
/// hit-ratio first crosses the operator-configured warm threshold
/// (default 0.50). Captured once per pod lifetime; subsequent
/// crossings are no-ops. The signal answers the Karpenter
/// spot-churn question "how long does a freshly-rotated shelfd
/// pod take to start *being* a cache" and is the SLI for the
/// post-cutover canary gate (≥ 80% hit ratio after 12h warm).
///
/// Implementation: a one-shot gauge — once the threshold crosses we
/// `set()` the elapsed seconds and never overwrite. Operators read
/// the gauge by `max_over_time(...)` so a missing pod (rotated
/// before warming) shows up as a gap instead of a phantom 0.
pub static WARM_THRESHOLD_CROSSED_SECONDS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_warm_threshold_crossed_seconds",
        "Seconds from pod ready until rolling hit ratio first crossed \
         the configured warm threshold. Set once per pod lifetime; \
         absent until the threshold actually crosses.",
        &["pool"],
        REGISTRY
    )
    .expect("register warm_threshold_crossed_seconds")
});

/// SHELF-21e — counter of LODC submit-queue admissions dropped by
/// shelfd's level-based back-pressure (see [`crate::lodc_backpressure`]).
/// Increments **once per dropped admission**, scoped to the pool
/// (only `rowgroup` is hybrid in v1; the label is kept for forward
/// compatibility with future hybrid pools). A non-zero rate over a
/// 5-min window is the operator signal that NVMe drain is falling
/// behind ingress; a sustained non-zero rate over 30 min is a
/// paging-grade alert because it means the cache is doing less
/// work than it could.
pub static LODC_DROPS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_lodc_drops_total",
        "Hybrid-pool admissions dropped by shelfd's LODC back-pressure. \
         The `reason` label distinguishes the SHELF-21e level gate \
         (`submit_queue_overflow`) from the SHELF-29 token-bucket gate \
         (`rate_limit`). Sustained non-zero rate of either ⇒ NVMe \
         drain is falling behind ingress and the cache is doing less \
         work than it could.",
        &["pool", "reason"],
        REGISTRY
    )
    .expect("register lodc_drops_total")
});

/// SHELF-29 — current bytes available in the rate-limiter's token
/// bucket for the rowgroup pool. Climbs toward `max_burst_bytes` when
/// the pod is idle, drains as admits consume tokens. Pair with
/// [`LODC_ADMIT_BURST_CAPACITY`] for a "% of burst available" panel.
pub static LODC_ADMIT_TOKENS_AVAILABLE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_lodc_admit_tokens_available",
        "Bytes available in the SHELF-29 admission token bucket. \
         Drains under burst, refills at the configured target rate. \
         Reaching zero is the leading indicator of a `rate_limit` \
         drop on the next admit.",
        &["pool"],
        REGISTRY
    )
    .expect("register lodc_admit_tokens_available")
});

/// SHELF-29 — configured burst capacity (`max_burst_bytes`) of the
/// rate-limiter, emitted once at boot. Constant per pod for the
/// process lifetime; exposed as a gauge so the dashboard's
/// "tokens-available / burst-capacity" panel can compute the ratio
/// without hard-coding the denominator.
pub static LODC_ADMIT_BURST_CAPACITY: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_lodc_admit_burst_capacity",
        "Configured `max_burst_bytes` of the SHELF-29 admission \
         rate-limiter. Constant per pod; exposed for dashboard ratios.",
        &["pool"],
        REGISTRY
    )
    .expect("register lodc_admit_burst_capacity")
});

/// **A1 (rc.7)** — current RSS-aware admission multiplier for the
/// SHELF-29 limiter, in basis points (`0..=10_000`). `10_000` means
/// the multiplier is at full (no RSS throttle applied); `0` means the
/// gate is fully paused. Sampled by the
/// [`crate::admission_limiter::RssThrottle`] poller every
/// `rss_poll_interval_secs`. Stored as an integer gauge to match
/// the `shelf_rolling_hit_ratio_bps` precedent (and to dodge the
/// YAML scientific-notation Helm landmine if any operator pulls
/// the value into a chart values overlay).
///
/// Operators reading the value: divide by 10_000 to render as a
/// fraction (`mult = bps / 10000.0`).
pub static LODC_RSS_THROTTLE_MULTIPLIER: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_lodc_rss_throttle_multiplier",
        "A1 RSS-aware admission multiplier for the SHELF-29 limiter, \
         in basis points (0..=10_000). 10_000 = no throttle, 0 = full \
         pause. Updated every `rss_poll_interval_secs` (default 5s) by \
         the RssThrottle poller. Divide by 10_000 to render as a \
         fraction.",
        &["pool"],
        REGISTRY
    )
    .expect("register lodc_rss_throttle_multiplier")
});

/// **A1 (rc.7)** — cumulative seconds the RSS-aware multiplier has
/// been active (i.e. multiplier < `1.0`). Incremented by the poll
/// interval every tick the multiplier is below the no-throttle
/// ceiling, so `rate(...[1m])` gives the fraction of wall-clock
/// time the pod spent under RSS pressure.
///
/// A non-zero rate sustained for more than 30 min on any pod is
/// the operator signal that `rss_target_bytes` is sized too low
/// for the steady-state working set OR that an unrelated leak is
/// bloating RSS — both warrant investigation, neither is a
/// "drop the throttle" signal.
pub static LODC_RSS_PRESSURE_SECONDS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_lodc_rss_pressure_seconds_total",
        "A1 cumulative seconds the RSS-aware admission multiplier was \
         below 1.0 (i.e. some throttle was active). Incremented by the \
         poll interval each tick the multiplier is sub-unity. \
         `rate(...[1m])` ≈ fraction of time under RSS pressure.",
        &["pool"],
        REGISTRY
    )
    .expect("register lodc_rss_pressure_seconds_total")
});

/// **A2 (rc.7)** — admits refused because the local pod is
/// draining. Bumped by [`crate::store::FoyerStore::get_or_fetch`]
/// when the SHELF-20 [`crate::membership::DrainSignal`] is active
/// **and** [`crate::config::DrainConfig::refuse_admits`] is `true`.
///
/// `reason` is a free-form label kept for forward-compat with a
/// possible v2 (e.g. pre-SIGTERM warning via kube-rs); v1 only
/// emits `"draining"`. Pair with `shelf_drain_active` for a "did
/// the gate engage on the right signal" cross-check on dashboards.
///
/// Why a dedicated counter (vs another `decision` value on
/// [`ADMISSIONS_TOTAL`]): the existing counter conflates
/// admission *policy* outcomes with *operational* gates; drain is
/// strictly the latter and graphs at a different timescale (one
/// burst per pod-lifetime, not steady-state). Keeping the two
/// series separate lets `rate(shelf_admit_refused_total[5m])`
/// stand alone as the SLO trip wire.
pub static ADMIT_REFUSED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_admit_refused_total",
        "A2 admits refused for an operational reason rather than \
         an admission-policy decision. v1 only emits \
         `reason=\"draining\"`, fired by the rowgroup admit gate \
         when the local DrainSignal is active and \
         `cache.drain.refuse_admits=true`.",
        &["reason"],
        REGISTRY
    )
    .expect("register admit_refused_total")
});

/// **A2 (rc.7)** — `0`/`1` snapshot of the local pod's
/// [`crate::membership::DrainSignal`]. Kept as a labelless
/// [`IntGauge`] (not `IntGaugeVec`) because the value is per-pod
/// by definition; the operator-facing dashboard gets the pod
/// dimension for free from the `pod` external label Prometheus
/// stamps onto every series. Updated by `main` once on SIGTERM
/// receipt — no polling overhead.
pub static DRAIN_ACTIVE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_with_registry!(
        "shelf_drain_active",
        "A2 SHELF-20 DrainSignal state for this pod. 0 = healthy, \
         1 = draining (SIGTERM observed). Read alongside \
         `shelf_admit_refused_total{reason=\"draining\"}` to see \
         the gate engage.",
        REGISTRY
    )
    .expect("register drain_active")
});

/// SHELF-21e — current in-flight bytes the back-pressure controller
/// observes per pool. Computed as `admitted_bytes − cache_write_bytes`
/// (both monotonic). Zero in steady state; climbs toward the
/// watermark under burst. The companion metric to
/// [`LODC_DROPS_TOTAL`]: drops fire when this gauge crosses ~80%
/// of the configured submit-queue threshold.
pub static LODC_INFLIGHT_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_lodc_inflight_bytes",
        "Approximate bytes admitted to the hybrid pool but not yet \
         committed to NVMe. Updated on every admission decision.",
        &["pool"],
        REGISTRY
    )
    .expect("register lodc_inflight_bytes")
});

/// SHELF-21e — estimated submit-queue depth in entries. Computed as
/// `inflight_bytes / avg_admitted_entry_size` so dashboards can
/// graph "how many entries are stacked behind the flushers"
/// alongside the byte-level [`LODC_INFLIGHT_BYTES`] gauge. Strictly
/// informational; the admission decision uses bytes, not depth.
pub static LODC_QUEUE_DEPTH: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_lodc_queue_depth",
        "Estimated count of entries admitted to the hybrid pool but \
         not yet committed to NVMe. Derived from the byte-level \
         shelf_lodc_inflight_bytes; informational only.",
        &["pool"],
        REGISTRY
    )
    .expect("register lodc_queue_depth")
});

/// SHELF-42 — A/B tag cap-violation counter.
///
/// Bumped exactly once per (scrape window, distinct over-cap tag) by
/// [`crate::ab_tag::AbTagState::tag_label_for`]. The value is the
/// number of *distinct* tag wire forms that had to fall back onto the
/// `other` sentinel during the current window, NOT the per-request
/// drop count — that would re-bump on every subsequent request landing
/// the same offending tag and erase the "how many distinct cohorts did
/// we drop?" signal we actually want for capacity planning.
///
/// `reason` discriminates today's only cap (`cardinality`) from any
/// future cap shapes (`size`, `epoch`, …) without renaming the metric.
pub static AB_TAG_CAP_VIOLATIONS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_ab_tag_cap_violations_total",
        "Number of distinct A/B tag wire forms that exceeded the per-pod \
         cardinality cap and were folded into the `other` sentinel \
         (SHELF-42). One bump per (scrape window, distinct offending tag).",
        &["reason"],
        REGISTRY
    )
    .expect("register ab_tag_cap_violations_total")
});

/// SHELF-50 — decoded-metadata in-process LRU hit counter. `kind`
/// is one of `manifest` (Iceberg manifest list/file) or
/// `parquet_footer`. The decoded LRU lives shelf-side in
/// `decoded_meta.rs`; this counter increments on every accessor
/// call that finds the entry resident, mirroring
/// `shelf_hits_total` for byte-cache hits.
pub static DECODED_META_HITS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_decoded_meta_hits_total",
        "Hits in the SHELF-50 decoded-metadata in-process LRU. \
         `kind` is one of `manifest` or `parquet_footer`. Compare \
         with `shelf_hits_total{pool=\"metadata\"}` for the share of \
         metadata reads that skip the deserialise step.",
        &["kind"],
        REGISTRY
    )
    .expect("register decoded_meta_hits_total")
});

/// SHELF-50 — decoded-metadata LRU miss counter. Same `kind` label
/// as `DECODED_META_HITS_TOTAL`; pair them for hit-ratio panels.
pub static DECODED_META_MISSES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_decoded_meta_misses_total",
        "Misses in the SHELF-50 decoded-metadata LRU. A miss does \
         NOT trigger an origin GET — it just means the byte cache \
         is the source of truth and the next caller will re-parse \
         (or, on warm pools, the fire-and-forget decoder will \
         eventually backfill).",
        &["kind"],
        REGISTRY
    )
    .expect("register decoded_meta_misses_total")
});

/// SHELF-50 — fire-and-forget decode latency histogram, observed
/// once per spawn. Buckets cover ~10 µs (warm-thread no-op) up to
/// ~500 ms (manifest with thousands of data-file entries).
pub static DECODED_META_DECODE_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_with_registry!(
        "shelf_decoded_meta_decode_seconds",
        "Wall-clock seconds the SHELF-50 decode worker spent \
         parsing one entry. Observed once per spawned decode \
         regardless of success/failure; pair with \
         `shelf_decoded_meta_decode_errors_total` to discount \
         failed parses.",
        &["kind"],
        prometheus::exponential_buckets(0.000_010, 2.0, 16)
            .expect("decoded_meta_decode_seconds bucket gen"),
        REGISTRY
    )
    .expect("register decoded_meta_decode_seconds")
});

/// SHELF-50 — current resident entry count per kind. Refreshed
/// after every insert/invalidate. Operators read it as a
/// quick-look gauge against `cache.decodedMeta.maxManifestEntries`
/// and `cache.decodedMeta.maxFooterEntries` — a steady-state value
/// at the cap means the cache is saturated and SHELF-50b sizing
/// should be revisited.
pub static DECODED_META_ENTRIES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_decoded_meta_entries",
        "Resident entry count in the SHELF-50 decoded-metadata LRU.",
        &["kind"],
        REGISTRY
    )
    .expect("register decoded_meta_entries")
});

/// SHELF-50 — decode-error counter. `reason` is a low-cardinality
/// label (`bad_magic`, `parquet_thrift`, `avro_header`, plus future
/// additions when SHELF-50b lands the iceberg-rust integration).
/// A non-zero rate is an investigation signal: either the byte
/// cache admitted something that isn't actually a manifest /
/// footer, or the parser version drifted from the writer.
pub static DECODED_META_DECODE_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_decoded_meta_decode_errors_total",
        "Decode failures observed by the SHELF-50 decode worker. \
         The `reason` label classifies the failure mode; entries \
         are NOT installed into the LRU on error.",
        &["kind", "reason"],
        REGISTRY
    )
    .expect("register decoded_meta_decode_errors_total")
});

/// SHELF-45 — compaction-aware re-warm reactor event outcomes.
///
/// Every snapshot event the reactor consumes lands on exactly one
/// of the labels below:
///   `received`              — observed on the channel.
///   `compaction_detected`   — predicate matched; rewarm scheduled.
///   `non_compaction_skipped`— predicate did not match (append /
///                             delete / partial / size mismatch).
///   `replayed`              — added-files set finished re-warming.
///   `dropped_rate_limit`    — the bounded mpsc was full at send;
///                             the producer's `try_send` returned
///                             `Full` and the event was discarded.
///                             Distinct from `non_compaction_skipped`
///                             because this one signals a *queueing*
///                             pressure, not a classification result.
pub static REWARM_EVENTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_rewarm_events_total",
        "SHELF-45 compaction-aware re-warm reactor: snapshot events \
         consumed, partitioned by lifecycle outcome.",
        &["outcome"],
        REGISTRY
    )
    .expect("register rewarm_events_total")
});

/// SHELF-45 — per-file re-warm outcomes. Splits the reactor's
/// best-effort fetch path into:
///   `warmed`              — `get_or_fetch` admitted the bytes.
///   `failed`              — fetch / admission errored; reason is
///                            classified separately by
///                            [`REWARM_ERRORS_TOTAL`].
///   `skipped_already_warm`— the content-addressed key was already
///                            resident in the rowgroup pool, no
///                            origin GET issued.
///   `skipped_pool_full`   — the in-flight semaphore was at capacity
///                            and `try_acquire` failed; the file is
///                            re-queued for the next snapshot tick.
pub static REWARM_FILES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_rewarm_files_total",
        "SHELF-45 re-warm reactor: per-file outcomes for the rowgroup \
         pool. `warmed` is the success path; the rest are diagnostic.",
        &["outcome"],
        REGISTRY
    )
    .expect("register rewarm_files_total")
});

/// SHELF-45 — bytes re-warmed, partitioned by `outcome`. Uses the
/// same label domain as [`REWARM_FILES_TOTAL`] so the dashboard
/// can join "files admitted" against "bytes admitted" without a
/// metric rename. Excludes HTTP framing — body size only, the way
/// `shelf_origin_request_bytes_total` already counts.
pub static REWARM_BYTES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_rewarm_bytes_total",
        "SHELF-45 re-warm reactor: bytes touched per outcome label.",
        &["outcome"],
        REGISTRY
    )
    .expect("register rewarm_bytes_total")
});

/// SHELF-45 — wall-clock seconds from snapshot commit (per the
/// event's `committed_at`) until the reactor finishes warming the
/// last added file in the snapshot. The histogram intentionally
/// extends out to 30 min to absorb large compactions that take
/// multiple budget windows; the dashboard's SLO is the p95.
pub static REWARM_LAG_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_with_registry!(
        "shelf_rewarm_lag_seconds",
        "SHELF-45: seconds from snapshot commit -> last added-file \
         re-warmed. p95 is the SLO.",
        &["outcome"],
        prometheus::exponential_buckets(1.0, 2.0, 12).expect("rewarm bucket gen"),
        REGISTRY
    )
    .expect("register rewarm_lag_seconds")
});

/// SHELF-45 — current count of re-warm fetches in flight on this
/// pod. Bounded by the configured `max_concurrent_files` semaphore
/// and therefore strictly small (default 4); the gauge exists so
/// dashboards can see the reactor is alive without scraping the
/// counter rate.
pub static REWARM_INFLIGHT_FILES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_rewarm_inflight_files",
        "SHELF-45: number of re-warm fetches currently in flight.",
        &["pool"],
        REGISTRY
    )
    .expect("register rewarm_inflight_files")
});

/// SHELF-45 — current depth of the bounded snapshot-event queue.
/// Approaching the configured capacity is the leading indicator
/// that the producer (SHELF-37 listener or polling worker) is
/// running ahead of the reactor's fetch budget.
pub static REWARM_QUEUE_DEPTH: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_rewarm_queue_depth",
        "SHELF-45: depth of the snapshot-event mpsc queue feeding \
         the reactor. Climbing toward queue_capacity = back-pressure.",
        &["pool"],
        REGISTRY
    )
    .expect("register rewarm_queue_depth")
});

/// SHELF-45 — fail-open error counter. Every failure variant the
/// reactor encounters bumps exactly one of these labels and the
/// task itself stays alive (best-effort semantics; re-warm never
/// propagates errors back to client traffic). `reason` domain:
///   `iceberg_metadata`    — misshapen event (empty sets,
///                           bad sizes, missing etag).
///   `origin_get`          — fetcher returned `Err(_)`.
///   `admission_rejected`  — admission policy refused the bytes.
///   `pool_full`           — semaphore / queue capacity exhausted.
///   `cancelled`           — task aborted via cancellation token.
pub static REWARM_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_rewarm_errors_total",
        "SHELF-45: per-reason failure counter for the re-warm reactor. \
         Every increment is paired with a `failed`/`skipped_*` outcome \
         on shelf_rewarm_files_total; this metric carves the failure \
         reason out for paging.",
        &["reason"],
        REGISTRY
    )
    .expect("register rewarm_errors_total")
});

/// **A3 (rc.7)** — total `metadata.json` polls per watched table,
/// partitioned by `result`:
///   `no_change`     — etag matched (304 fast path) or nothing
///                     interesting changed since `last_seen`.
///   `new_snapshot`  — a fresh snapshot was observed (regardless of
///                     `summary["operation"]`).
///   `error`         — S3 read / parse failed; the loop continues.
///
/// One increment per (table, poll_interval) tick; the rate is the
/// "cheap GET" telemetry the ADR uses to argue the loop's overhead
/// is trivial (~12 polls/min for 100 tables at the 30 s default).
pub static REWARM_POLLS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_rewarm_polls_total",
        "A3 (rc.7): metadata.json polls by the rewarm poller, \
         partitioned by table label and result \
         (no_change | new_snapshot | error).",
        &["table", "result"],
        REGISTRY
    )
    .expect("register rewarm_polls_total")
});

/// **A3 (rc.7)** — compaction snapshots (`summary["operation"]
/// == "replace"`) detected per watched table. Subset of the
/// `new_snapshot` polls — the headline counter for "did the loop
/// catch a compaction this morning?" alerting.
pub static REWARM_SNAPSHOTS_DETECTED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_rewarm_snapshots_detected_total",
        "A3 (rc.7): compaction-class (operation=replace) snapshots \
         detected per watched table label.",
        &["table"],
        REGISTRY
    )
    .expect("register rewarm_snapshots_detected_total")
});

/// **A3 (rc.7)** — added data-files the poller successfully
/// enqueued onto the SHELF-45 reactor (per detected compaction).
/// Increments are bounded by `max_bytes_per_snapshot`; files
/// dropped by the cap bump [`REWARM_BYTES_CAPPED_TOTAL`] instead.
pub static REWARM_FILES_ENQUEUED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_rewarm_files_enqueued_total",
        "A3 (rc.7): data files enqueued for SHELF-45 re-warm after \
         compaction detection, per watched table label.",
        &["table"],
        REGISTRY
    )
    .expect("register rewarm_files_enqueued_total")
});

/// **A3 (rc.7)** — bytes corresponding to enqueued data files.
/// Capped per detected snapshot at `max_bytes_per_snapshot`; the
/// excess increments [`REWARM_BYTES_CAPPED_TOTAL`].
pub static REWARM_BYTES_ENQUEUED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_rewarm_bytes_enqueued_total",
        "A3 (rc.7): bytes (under the per-snapshot cap) enqueued for \
         SHELF-45 re-warm after compaction detection.",
        &["table"],
        REGISTRY
    )
    .expect("register rewarm_bytes_enqueued_total")
});

/// **A3 (rc.7)** — bytes refused by the per-snapshot cap. Movement
/// here means the poller saw a compaction whose new-file payload
/// exceeded `max_bytes_per_snapshot`; the cap fired correctly and
/// the operator should consider raising the cap or excluding the
/// table.
pub static REWARM_BYTES_CAPPED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_rewarm_bytes_capped_total",
        "A3 (rc.7): bytes refused by the rewarm poller's per-snapshot \
         cap (max_bytes_per_snapshot); paired with one or more \
         omitted files from shelf_rewarm_files_enqueued_total.",
        &["table"],
        REGISTRY
    )
    .expect("register rewarm_bytes_capped_total")
});

/// Track G-11 companion — current rolling hit ratio per pool, in
/// basis points (0–10_000). Sampled by the same `warm_sampler`
/// task that flips `WARM_THRESHOLD_CROSSED_SECONDS`. Exposed as
/// an integer gauge to dodge the "scientific notation in YAML"
/// landmine the Helm chart hit on big-number floats; clients
/// divide by 100 for a percentage. A separate gauge from
/// `(hits / (hits+misses))` because the Foyer counters are
/// monotonic-since-boot and can hide the *current* warmth state
/// behind a giant cold-start tail.
pub static ROLLING_HIT_RATIO_BPS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_rolling_hit_ratio_bps",
        "Rolling hit ratio per pool, in basis points (0-10_000). \
         Window is the last 60s of hits/misses; resets once per \
         minute. Divide by 100 for a percentage.",
        &["pool"],
        REGISTRY
    )
    .expect("register rolling_hit_ratio_bps")
});

/// SHELF-40 — cumulative S3 + data-transfer cents *saved* by serving
/// reads out of cache instead of origin S3.
///
/// **Unit is integer cents**, not dollars. Dashboards multiply by
/// `0.01` explicitly when rendering — any panel that drops the
/// multiplier reads off by a factor of 100, which is exactly the
/// "lying to operators by calling the unit dollars" failure mode
/// SHELF-40 acceptance forbids. The series carries `region`
/// (`us-east-1`, `ap-south-1`, …) and `outcome` (`hit_memory`,
/// `hit_disk`, `peer`) labels so a multi-region cluster can split
/// savings by region while a single-region cluster gets a
/// constant `region` label that compresses cleanly in PromQL.
///
/// Values come from `shelf_cost::CostModel::dollars_saved` —
/// the audit-able formula lives in `crates/shelf-cost/`. The
/// counter never decrements; rollback is "delete the series" via
/// a Prometheus relabel, not a runtime knob.
pub static S3_DOLLARS_SAVED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_s3_dollars_saved_total",
        "Cumulative S3 + cross-AZ + NAT dollars *saved* by serving \
         from shelf, in **integer cents**. Multiply by 0.01 to \
         render dollars. Region + outcome carry the same shape as \
         shelf_hits_total / hit_memory|hit_disk plus the SHELF-23 \
         `peer` outcome. Source formula: crates/shelf-cost/.",
        &["region", "outcome"],
        REGISTRY
    )
    .expect("register s3_dollars_saved_total")
});

/// SHELF-40 — 60s rolling rate helper for [`S3_DOLLARS_SAVED_TOTAL`].
///
/// Operators care most about *rate* ("are we saving money right
/// now?"); Prometheus can compute `rate(... [60s])` itself, but
/// every dashboard that wants the cents-per-second number ends up
/// re-deriving the same expression with the unit-conversion
/// multiplier (`0.01` to render dollars/sec, `3600` to project to
/// dollars/hour) baked in. This gauge ships the rolling rate as
/// already-correct **cents/sec** so the dashboard just drops it
/// into a `stat` panel.
///
/// Sampled by the SHELF-40 rate-updater task (see `crate::cost`)
/// once per second over a 60-sample sliding window. Reset to zero
/// at boot; the first 60 s of any pod's lifetime under-reports
/// (the window has not filled yet) — that's the same trade-off
/// `shelf_rolling_hit_ratio_bps` accepts and dashboards already
/// know how to read.
pub static S3_DOLLARS_SAVED_RATE_CENTS_PER_SEC: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_s3_dollars_saved_rate_cents_per_sec",
        "60s rolling rate of shelf_s3_dollars_saved_total, in \
         **integer cents per second**. Updated once per second by \
         the SHELF-40 rate-updater task. Multiply by 0.01 for \
         $/sec, ×60 for $/min, ×3600 for $/hr.",
        &["region", "outcome"],
        REGISTRY
    )
    .expect("register s3_dollars_saved_rate_cents_per_sec")
});

/// **A4 (rc.7)** — *net* dollars-saved counter.
///
/// SHELF-40 ships the gross savings counter
/// ([`S3_DOLLARS_SAVED_TOTAL`]); procurement asks the next
/// question — *minus the cost of running the shelf pool itself*.
/// This counter answers it by subtracting the operator-supplied
/// amortized pool cost (`cache.cost.amortizedDollarsPerHour`,
/// stored in [`SHELF_POOL_AMORTIZED_DOLLARS_PER_HOUR`]) from the
/// per-tick gross delta. The accountant runs in
/// `crate::cost::spawn_net_accountant`.
///
/// **Unit**: integer **dollar-micros** (`1 cent = 10_000 µ$`,
/// `1 dollar = 1_000_000 µ$`). Divide by `1e6` to render dollars.
/// The gross counter ships in cents; the accountant converts
/// before subtracting so the units inside this counter stay
/// consistent.
///
/// **Anti-overclaim guard**: this counter is only credited when
/// the operator has explicitly set
/// `cache.cost.amortizedDollarsPerHour` to a positive, finite
/// number. Unset / zero / negative / NaN ⇒ counter stays at zero
/// (reading the gauge confirms the misconfig). Defaulting to a
/// silent zero would inflate net savings procurement-side by the
/// full pool cost.
pub static S3_DOLLARS_SAVED_NET_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_s3_dollars_saved_net_total",
        "A4 (rc.7) Cumulative *net* dollars saved (gross S3 + \
         data-transfer savings on shelf_s3_dollars_saved_total \
         minus amortized shelf-pool cost). Stored as integer \
         **dollar-micros**; divide by 1e6 for dollars. Only \
         credited when cache.cost.amortizedDollarsPerHour is \
         explicitly set; the SHELF_POOL_AMORTIZED_DOLLARS_PER_HOUR \
         gauge reports the active configuration.",
        &["region"],
        REGISTRY
    )
    .expect("register s3_dollars_saved_net_total")
});

/// **A6 (rc.7)** — bytes-from-peer admissions accepted by the
/// cooperative gate. Numerator of the "what fraction of peer-fetched
/// bytes did we admit?" panel. Pair with [`COOP_PEER_DROPS_TOTAL`]
/// for the full denominator (both counters tick exactly once per
/// `FetchSource::Peer` admit decision in
/// [`crate::store::FoyerStore::get_or_fetch`]).
///
/// `pool` matches the existing `shelf_admissions_total` cardinality
/// (`metadata` / `rowgroup`). The gate is consulted at the rowgroup
/// admit site only in v1 — metadata pool peer-fetch races are rare —
/// but the label is kept symmetric with the rest of the admit-chain
/// counters so dashboards can graph both pools without renaming the
/// metric later.
pub static COOP_PEER_ADMITS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_coop_peer_admits_total",
        "A6 (rc.7) Bytes-from-peer admissions accepted by the \
         cooperative gate (numerator of the admit-ratio panel). \
         Ticks exactly once per `FetchSource::Peer` admit decision; \
         pair with `shelf_coop_peer_drops_total` for the denominator.",
        &["pool"],
        REGISTRY
    )
    .expect("register coop_peer_admits_total")
});

/// **A6 (rc.7)** — bytes-from-peer admissions dropped by the
/// cooperative probabilistic gate. Each tick represents one Foyer
/// insert AND one NVMe write the cache *did not* pay; the dashboard
/// "saved NVMe bytes" panel uses
/// `rate(shelf_coop_peer_drops_total) × avg_admission_size` as a
/// rough estimate.
///
/// A drop here means the upstream pressure-aware chain (drain /
/// policy / LODC / rate-limiter) all said admit, but A6's secondary
/// gate said "trust the primary". Compose with
/// `shelf_admissions_total{decision="reject_coop"}` for cross-check.
pub static COOP_PEER_DROPS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_coop_peer_drops_total",
        "A6 (rc.7) Bytes-from-peer admissions dropped by the \
         cooperative probabilistic gate. Saved NVMe bytes per drop \
         ≈ avg admission size; pair with shelf_admissions_total\
         {decision=reject_coop} for cross-check.",
        &["pool"],
        REGISTRY
    )
    .expect("register coop_peer_drops_total")
});

/// **A6 (rc.7)** — peer-fetch admits force-accepted because this
/// pod is the HRW primary for the key (defensive invariant — see
/// [`crate::coop_admission::CoopAdmissionGate::should_admit_peer_bytes`]).
///
/// Today this counter stays flat: by construction
/// [`crate::peer_fetch::peer_or_origin_fetch`] short-circuits to
/// `Origin` before returning `Peer` when the local pod is primary,
/// so the gate never observes `key_primary_is_self = true`. The
/// counter exists so a future code path that ever lands `Peer`
/// bytes on the primary (e.g. a manual replay tool, an admin
/// `/admin/replay` endpoint) bumps it visibly — operators read the
/// counter as the "did the invariant ever fire?" telemetry.
pub static COOP_PRIMARY_FORCE_ADMITS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_coop_primary_force_admits_total",
        "A6 (rc.7) Peer-fetch admissions force-accepted because the \
         local pod is the HRW primary for the key (defensive \
         invariant). Stays at 0 in v1 — peer_or_origin_fetch never \
         tags primary-resident bytes as `Peer`. A non-zero value \
         means a future code path landed `Peer` bytes on the primary \
         and the invariant kept the cache populated.",
        &["pool"],
        REGISTRY
    )
    .expect("register coop_primary_force_admits_total")
});

/// **B3 (rc.7)** — admit refusals from the transient-table gate.
/// Each tick is one Foyer insert AND one NVMe write the cache
/// declined to pay because the table's snapshot retention (or
/// explicit `shelf.cache-policy` property) flagged it as
/// intermediate / scratch. Pair with
/// `shelf_admissions_total{decision="reject_transient"}` for
/// cross-check. Stays flat on a stock OSS deployment because the
/// gate ships default-off (`cache.transientAdmission.enabled =
/// false`). See ADR-0038.
pub static TRANSIENT_REFUSALS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_transient_refusals_total",
        "B3 (rc.7) Admit refusals for tables flagged transient \
         (intermediate / scratch). Saves NVMe + write amplification.",
        &["table"],
        REGISTRY
    )
    .expect("register transient_refusals_total")
});

/// **B3 (rc.7)** — number of per-table policy decisions held in
/// the in-memory cache. Bounded by the actual table count seen
/// since boot (≤ ~500 tables in cdp; see
/// `HITS_BY_TABLE_TOTAL` for the cardinality budget). Useful as a
/// quick saturation signal: if the gauge exceeds the expected
/// table count the operator should investigate cardinality leaks
/// (hex-blob keys, attacker traffic).
pub static TRANSIENT_DECISIONS_CACHED: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_with_registry!(
        "shelf_transient_decisions_cached",
        "B3 (rc.7) Number of table-level policy decisions \
         currently held in the in-memory decision cache.",
        REGISTRY
    )
    .expect("register transient_decisions_cached")
});

/// **B3 (rc.7)** — `metadata.json` fetch failures during a
/// transient-policy refresh. Per-table label (low cardinality —
/// bounded by the cluster's table count). On error the gate
/// stays at fail-open `Admit`, so a non-zero value here is an
/// operability signal (S3 throttling? wrong bucket policy?), not
/// a correctness signal.
pub static TRANSIENT_REFRESH_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_transient_refresh_errors_total",
        "B3 (rc.7) metadata.json fetch errors during \
         transient-policy refresh. Falls back to Admit (fail-open).",
        &["table"],
        REGISTRY
    )
    .expect("register transient_refresh_errors_total")
});

/// **A4 (rc.7)** — amortized shelf-pool cost gauge.
///
/// Always exposed (regardless of whether the net counter
/// publishes) so dashboards can flag the unset state explicitly.
/// `0` ⇒ operator has not configured `cache.cost.amortizedDollarsPerHour`
/// and the net counter will stay at zero until they do.
///
/// **Unit**: integer **dollar-micros per hour**. Divide by `1e6`
/// for dollars-per-hour.
pub static SHELF_POOL_AMORTIZED_DOLLARS_PER_HOUR: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_with_registry!(
        "shelf_pool_amortized_dollars_per_hour",
        "A4 (rc.7) Amortized shelf-pool cost per hour, in integer \
         **dollar-micros** (divide by 1e6 for $/hr). 0 = unset; \
         operator must configure cache.cost.amortizedDollarsPerHour \
         for shelf_s3_dollars_saved_net_total to publish.",
        REGISTRY
    )
    .expect("register shelf_pool_amortized_dollars_per_hour")
});

/// SHELF-46 — bloom-aware footer admission classification counter.
/// Bumped once per s3-shim GET request that runs through
/// [`crate::parquet_admit::BloomAdmission::record_classification`].
/// `kind` is one of `footer`, `bloom_block`, `not_applicable`.
pub static BLOOM_ADMIT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_bloom_admit_total",
        "SHELF-46 bloom-aware admission classifications. \
         `kind` is one of `footer`, `bloom_block`, `not_applicable`.",
        &["kind"],
        REGISTRY
    )
    .expect("register bloom_admit_total")
});

/// SHELF-46 — current size of the in-process etag → bloom-block-list
/// LRU. Capped at `cache.bloom.maxIndexEntries` (default 50 000).
pub static BLOOM_INDEX_ENTRIES: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_with_registry!(
        "shelf_bloom_index_entries",
        "SHELF-46 entries currently held in the etag → bloom-block-list LRU.",
        REGISTRY
    )
    .expect("register bloom_index_entries")
});

/// SHELF-46 — Parquet footer parse failures. Fail-open: a non-zero
/// rate means the bloom-block lookup path is dormant for the
/// affected etags but the footer-suffix heuristic still runs.
pub static BLOOM_PARSE_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_bloom_parse_errors_total",
        "SHELF-46 footer parser errors, partitioned by parse-time reason.",
        &["reason"],
        REGISTRY
    )
    .expect("register bloom_parse_errors_total")
});

/// **B1** — bytes presented to the compression pipeline before
/// encoding (one increment per `insert` on a compression-enabled
/// pool).
pub static COMPRESS_BYTES_IN_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_compress_bytes_in_total",
        "Bytes presented to the compression pipeline before encoding, per pool (B1).",
        &["pool"],
        REGISTRY
    )
    .expect("register compress_bytes_in_total")
});

/// **B1** — bytes returned by the compression pipeline (post-encode,
/// header byte included).
pub static COMPRESS_BYTES_OUT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_compress_bytes_out_total",
        "Bytes stored after the compression pipeline (encoded frame size), per pool (B1).",
        &["pool"],
        REGISTRY
    )
    .expect("register compress_bytes_out_total")
});

/// **B1** — encode/decode outcome counter.
pub static COMPRESS_OUTCOMES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_compress_outcomes_total",
        "Compression / decompression outcomes per pool (B1).",
        &["pool", "outcome"],
        REGISTRY
    )
    .expect("register compress_outcomes_total")
});

/// **B1** — compression pipeline latency, per pool + op.
pub static COMPRESS_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_with_registry!(
        "shelf_compress_seconds",
        "Latency of the compression pipeline, per pool + op (B1).",
        &["pool", "op"],
        prometheus::exponential_buckets(0.000_01, 2.0, 16).expect("compress bucket gen"),
        REGISTRY
    )
    .expect("register compress_seconds")
});

/// SHELF-33 — W-TinyLFU admission gate decisions, per outcome.
pub static WTINYLFU_DECISIONS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_wtinylfu_decisions_total",
        "W-TinyLFU admission decisions, by outcome label.",
        &["outcome"],
        REGISTRY
    )
    .expect("register wtinylfu_decisions_total")
});

/// SHELF-33 — W-TinyLFU sketch / doorkeeper decay events.
pub static WTINYLFU_DECAYS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_wtinylfu_decays_total",
        "W-TinyLFU window-decay events (sketch + doorkeeper halve / clear).",
        &["component"],
        REGISTRY
    )
    .expect("register wtinylfu_decays_total")
});

/// SHELF-34 — `/predicate-prune` request counter.
pub static PREDICATE_PRUNE_REQUESTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_with_registry!(
        "shelf_predicate_prune_requests_total",
        "Requests to /predicate-prune, partitioned by outcome.",
        &["outcome"],
        REGISTRY
    )
    .expect("register predicate_prune_requests_total")
});

/// SHELF-34 — `/predicate-prune` end-to-end latency in seconds.
pub static PREDICATE_PRUNE_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_with_registry!(
        "shelf_predicate_prune_seconds",
        "End-to-end /predicate-prune handler latency in seconds.",
        &["outcome"],
        prometheus::exponential_buckets(0.0005, 2.0, 16).expect("predicate_prune bucket gen"),
        REGISTRY
    )
    .expect("register predicate_prune_seconds")
});

/// SHELF-34 — approximate bytes held in the in-process PageIndex cache.
pub static PAGE_INDEX_CACHED_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_with_registry!(
        "shelf_page_index_cached_bytes",
        "Approximate bytes held in the in-process Parquet PageIndex cache.",
        &["pool"],
        REGISTRY
    )
    .expect("register page_index_cached_bytes")
});

/// SHELF-34 — Parquet page-index parse latency (per cache miss).
pub static PAGE_INDEX_PARSE_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_with_registry!(
        "shelf_page_index_parse_seconds",
        "Parquet page-index parse latency in seconds (per cache miss).",
        &["outcome"],
        prometheus::exponential_buckets(0.0005, 2.0, 16).expect("page_index_parse bucket gen"),
        REGISTRY
    )
    .expect("register page_index_parse_seconds")
});

/// **K2 (rc.8)** — per-pod request rate over the trailing 60 s
/// window (configurable via `cache.podLoad.window`). Per-pod gauge:
/// the cluster-wide skew is computed externally via Prometheus
/// aggregation: `max(shelf_pod_load_qps) / quantile(0.5,
/// shelf_pod_load_qps)`. Updated every
/// `cache.podLoad.aggregationInterval` (default 30 s) by
/// [`crate::pod_load::PodLoadAggregator::run`]. Bench evidence in
/// `benchmarks/results/2026-05-01/4hr/COMPREHENSIVE-RESULTS.md`
/// motivated this metric: shelf-bench-{0,1,2} took ~14× more
/// queries than {3,4,5} and the existing aggregate-pool QPS gauge
/// could not surface the imbalance. See
/// `agents/out/adr/0042-rc8-shelf-pool-rightsizing.md`.
pub static POD_LOAD_QPS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_with_registry!(
        "shelf_pod_load_qps",
        "K2 (rc.8): per-pod request rate over the trailing 60s window. \
         Per-pod gauge; cluster-wide skew computed externally via Prometheus \
         aggregation: max(shelf_pod_load_qps) / quantile(0.5, shelf_pod_load_qps).",
        REGISTRY
    )
    .expect("register pod_load_qps")
});

/// **K2 (rc.8)** — HRW skew ratio = `max(per-pod qps) / median(per-pod
/// qps)`, expressed in **basis points** (× 100 to dodge YAML
/// scientific-notation Helm landmine, per workspace memory rule).
///
/// `100 bps = 1.0` (perfect balance); `>= 150 bps` (1.5×) is the
/// scale-up threshold the example KEDA `ScaledObject` at
/// `charts/shelf/examples/keda-scaledobject-skew-aware.yaml` uses.
/// Updated every `cache.podLoad.aggregationInterval` (default 30 s)
/// by an in-cluster aggregator that probes peer `/stats` endpoints.
/// See [`crate::pod_load::compute_skew_bps`] for the lower-median
/// rationale.
pub static POD_LOAD_SKEW_RATIO_BPS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_with_registry!(
        "shelf_pod_load_skew_ratio_bps",
        "K2 (rc.8): HRW skew ratio = max(per-pod qps) / median(per-pod qps), \
         expressed in basis-points (× 100 to dodge YAML scientific-notation \
         Helm landmine, per workspace memory rule). 100 bps = 1.0 (perfect \
         balance); > 150 bps = skew warning. Updated every 30s by an \
         in-cluster aggregator that probes peer /stats endpoints.",
        REGISTRY
    )
    .expect("register pod_load_skew_ratio_bps")
});

/// **K2 (rc.8)** — peer `/stats?include=pod_load` probe failures
/// during pod-load aggregation. Bumped once per failed peer probe
/// (timeout, non-2xx, JSON decode error, missing `pod_load` block).
/// On a non-zero rate, the skew gauge falls back to a local-only
/// computation (which always reads as `100 bps`); operators
/// investigate via peer pod logs / network policies.
pub static POD_LOAD_PROBE_ERRORS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_pod_load_probe_errors_total",
        "K2 (rc.8): errors during peer /stats probe for pod_load \
         aggregation. Falls back to local-only ratio.",
        REGISTRY
    )
    .expect("register pod_load_probe_errors_total")
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
    // Track B3 — origin + shim byte / latency telemetry.
    "shelf_origin_request_bytes_total",
    "shelf_origin_request_seconds",
    "shelf_s3_shim_response_bytes_total",
    // Track E8 — admission + eviction + single-flight telemetry.
    "shelf_admissions_total",
    "shelf_evictions_total",
    "shelf_inflight_singleflight",
    // Track E7 — per-fingerprint telemetry substrate for MV advisor.
    "shelf_queries_served_total",
    "shelf_bytes_saved_total",
    // Track H5 — per-MV hit / byte counters feeding the MV Grafana panel.
    "shelf_mv_hits_total",
    "shelf_mv_bytes_served_total",
    // Track G-10 / G-11 — engine-reset alert + warm-up SLI.
    "shelf_engine_resets_total",
    "shelf_warm_threshold_crossed_seconds",
    "shelf_rolling_hit_ratio_bps",
    // Track G-4 — per-table hit / miss counters.
    "shelf_hits_by_table_total",
    "shelf_misses_by_table_total",
    // SHELF-21e — LODC back-pressure observability.
    "shelf_lodc_drops_total",
    "shelf_lodc_inflight_bytes",
    "shelf_lodc_queue_depth",
    // SHELF-29 — independent-queue admission rate-limiter.
    "shelf_lodc_admit_tokens_available",
    "shelf_lodc_admit_burst_capacity",
    // A1 (rc.7) — RSS-aware admission multiplier on top of SHELF-29.
    "shelf_lodc_rss_throttle_multiplier",
    "shelf_lodc_rss_pressure_seconds_total",
    // A2 (rc.7) — SIGTERM-only drain-aware admission.
    "shelf_admit_refused_total",
    "shelf_drain_active",
    // SHELF-33 — W-TinyLFU admission gate observability.
    "shelf_wtinylfu_decisions_total",
    "shelf_wtinylfu_decays_total",
    // SHELF-23 — peer-fetch outcome counters.
    "shelf_peer_hit_total",
    "shelf_peer_miss_total",
    "shelf_peer_timeout_total",
    "shelf_peer_error_total",
    // SHELF-23 — ETag-conditional GET outcome counters.
    "shelf_conditional_not_modified_total",
    "shelf_conditional_modified_total",
    "shelf_conditional_skipped_total",
    "shelf_conditional_error_total",
    // SHELF-40 — audit-able dollars-saved counter + rolling rate.
    "shelf_s3_dollars_saved_total",
    "shelf_s3_dollars_saved_rate_cents_per_sec",
    // A4 (rc.7) — net dollars-saved counter + amortized-cost gauge.
    "shelf_s3_dollars_saved_net_total",
    "shelf_pool_amortized_dollars_per_hour",
    // A6 (rc.7) — cooperative peer-admission probabilistic gate.
    "shelf_coop_peer_admits_total",
    "shelf_coop_peer_drops_total",
    "shelf_coop_primary_force_admits_total",
    // B3 (rc.7) — intermediate-table opt-out admission gate.
    "shelf_transient_refusals_total",
    "shelf_transient_decisions_cached",
    "shelf_transient_refresh_errors_total",
    // SHELF-42 — A/B tag receive path.
    "shelf_hits_by_tag_total",
    "shelf_misses_by_tag_total",
    "shelf_s3_shim_response_bytes_by_tag_total",
    "shelf_ab_tag_cap_violations_total",
    // SHELF-50 — decoded-metadata in-process LRU.
    "shelf_decoded_meta_hits_total",
    "shelf_decoded_meta_misses_total",
    "shelf_decoded_meta_decode_seconds",
    "shelf_decoded_meta_entries",
    "shelf_decoded_meta_decode_errors_total",
    // SHELF-46 — bloom-aware footer admission telemetry.
    "shelf_bloom_admit_total",
    "shelf_bloom_index_entries",
    "shelf_bloom_parse_errors_total",
    // B1 — per-pool zstd compression telemetry.
    "shelf_compress_bytes_in_total",
    "shelf_compress_bytes_out_total",
    "shelf_compress_outcomes_total",
    "shelf_compress_seconds",
    // SHELF-33 — W-TinyLFU admission gate telemetry.
    "shelf_wtinylfu_decisions_total",
    "shelf_wtinylfu_decays_total",
    // SHELF-34 — page-index sidecar telemetry.
    "shelf_predicate_prune_requests_total",
    "shelf_predicate_prune_seconds",
    "shelf_page_index_cached_bytes",
    "shelf_page_index_parse_seconds",
    // SHELF-45 — compaction-aware re-warm reactor.
    "shelf_rewarm_events_total",
    "shelf_rewarm_files_total",
    "shelf_rewarm_bytes_total",
    "shelf_rewarm_lag_seconds",
    "shelf_rewarm_inflight_files",
    "shelf_rewarm_queue_depth",
    "shelf_rewarm_errors_total",
    // A3 (rc.7) — compaction-rewarm metadata-json poller.
    "shelf_rewarm_polls_total",
    "shelf_rewarm_snapshots_detected_total",
    "shelf_rewarm_files_enqueued_total",
    "shelf_rewarm_bytes_enqueued_total",
    "shelf_rewarm_bytes_capped_total",
    // K2 (rc.8) — HRW-skew-aware autoscaler integration.
    "shelf_pod_load_qps",
    "shelf_pod_load_skew_ratio_bps",
    "shelf_pod_load_probe_errors_total",
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
            reg.origin_request_bytes_total.desc(),
            reg.origin_request_seconds.desc(),
            reg.s3_shim_response_bytes_total.desc(),
            ADMISSIONS_TOTAL.desc(),
            EVICTIONS_TOTAL.desc(),
            INFLIGHT_SINGLEFLIGHT.desc(),
            QUERIES_SERVED_TOTAL.desc(),
            BYTES_SAVED_TOTAL.desc(),
            MV_HITS_TOTAL.desc(),
            MV_BYTES_SERVED_TOTAL.desc(),
            ENGINE_RESETS_TOTAL.desc(),
            WARM_THRESHOLD_CROSSED_SECONDS.desc(),
            ROLLING_HIT_RATIO_BPS.desc(),
            HITS_BY_TABLE_TOTAL.desc(),
            MISSES_BY_TABLE_TOTAL.desc(),
            LODC_DROPS_TOTAL.desc(),
            LODC_INFLIGHT_BYTES.desc(),
            LODC_QUEUE_DEPTH.desc(),
            LODC_ADMIT_TOKENS_AVAILABLE.desc(),
            LODC_ADMIT_BURST_CAPACITY.desc(),
            LODC_RSS_THROTTLE_MULTIPLIER.desc(),
            LODC_RSS_PRESSURE_SECONDS_TOTAL.desc(),
            ADMIT_REFUSED_TOTAL.desc(),
            DRAIN_ACTIVE.desc(),
            WTINYLFU_DECISIONS_TOTAL.desc(),
            WTINYLFU_DECAYS_TOTAL.desc(),
            PEER_HIT_TOTAL.desc(),
            PEER_MISS_TOTAL.desc(),
            PEER_TIMEOUT_TOTAL.desc(),
            PEER_ERROR_TOTAL.desc(),
            CONDITIONAL_NOT_MODIFIED_TOTAL.desc(),
            CONDITIONAL_MODIFIED_TOTAL.desc(),
            CONDITIONAL_SKIPPED_TOTAL.desc(),
            CONDITIONAL_ERROR_TOTAL.desc(),
            S3_DOLLARS_SAVED_TOTAL.desc(),
            S3_DOLLARS_SAVED_RATE_CENTS_PER_SEC.desc(),
            S3_DOLLARS_SAVED_NET_TOTAL.desc(),
            SHELF_POOL_AMORTIZED_DOLLARS_PER_HOUR.desc(),
            COOP_PEER_ADMITS_TOTAL.desc(),
            COOP_PEER_DROPS_TOTAL.desc(),
            COOP_PRIMARY_FORCE_ADMITS_TOTAL.desc(),
            TRANSIENT_REFUSALS_TOTAL.desc(),
            TRANSIENT_DECISIONS_CACHED.desc(),
            TRANSIENT_REFRESH_ERRORS_TOTAL.desc(),
            HITS_BY_TAG_TOTAL.desc(),
            MISSES_BY_TAG_TOTAL.desc(),
            S3_SHIM_RESPONSE_BYTES_BY_TAG_TOTAL.desc(),
            AB_TAG_CAP_VIOLATIONS_TOTAL.desc(),
            DECODED_META_HITS_TOTAL.desc(),
            DECODED_META_MISSES_TOTAL.desc(),
            DECODED_META_DECODE_SECONDS.desc(),
            DECODED_META_ENTRIES.desc(),
            DECODED_META_DECODE_ERRORS_TOTAL.desc(),
            BLOOM_ADMIT_TOTAL.desc(),
            BLOOM_INDEX_ENTRIES.desc(),
            BLOOM_PARSE_ERRORS_TOTAL.desc(),
            COMPRESS_BYTES_IN_TOTAL.desc(),
            COMPRESS_BYTES_OUT_TOTAL.desc(),
            COMPRESS_OUTCOMES_TOTAL.desc(),
            COMPRESS_SECONDS.desc(),
            PREDICATE_PRUNE_REQUESTS_TOTAL.desc(),
            PREDICATE_PRUNE_SECONDS.desc(),
            PAGE_INDEX_CACHED_BYTES.desc(),
            PAGE_INDEX_PARSE_SECONDS.desc(),
            REWARM_EVENTS_TOTAL.desc(),
            REWARM_FILES_TOTAL.desc(),
            REWARM_BYTES_TOTAL.desc(),
            REWARM_LAG_SECONDS.desc(),
            REWARM_INFLIGHT_FILES.desc(),
            REWARM_QUEUE_DEPTH.desc(),
            REWARM_ERRORS_TOTAL.desc(),
            REWARM_POLLS_TOTAL.desc(),
            REWARM_SNAPSHOTS_DETECTED_TOTAL.desc(),
            REWARM_FILES_ENQUEUED_TOTAL.desc(),
            REWARM_BYTES_ENQUEUED_TOTAL.desc(),
            REWARM_BYTES_CAPPED_TOTAL.desc(),
            POD_LOAD_QPS.desc(),
            POD_LOAD_SKEW_RATIO_BPS.desc(),
            POD_LOAD_PROBE_ERRORS_TOTAL.desc(),
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
        reg.origin_request_bytes_total
            .with_label_values(&["b", "get_range", "ok"])
            .inc_by(0);
        reg.origin_request_seconds
            .with_label_values(&["b", "get_range", "ok"])
            .observe(0.0);
        reg.s3_shim_response_bytes_total
            .with_label_values(&["get_object", "miss"])
            .inc_by(0);
        ADMISSIONS_TOTAL
            .with_label_values(&["metadata", "admit"])
            .inc_by(0);
        EVICTIONS_TOTAL
            .with_label_values(&["metadata", "admin"])
            .inc_by(0);
        INFLIGHT_SINGLEFLIGHT
            .with_label_values(&["metadata"])
            .set(0);
        QUERIES_SERVED_TOTAL
            .with_label_values(&["fp-abc", "tenant-x"])
            .inc_by(0);
        BYTES_SAVED_TOTAL
            .with_label_values(&["fp-abc", "tenant-x"])
            .inc_by(0);
        MV_HITS_TOTAL
            .with_label_values(&["analytics.top_ten"])
            .inc_by(0);
        MV_BYTES_SERVED_TOTAL
            .with_label_values(&["analytics.top_ten"])
            .inc_by(0);
        ENGINE_RESETS_TOTAL
            .with_label_values(&["metadata", "pool_open_retry"])
            .inc_by(0);
        WARM_THRESHOLD_CROSSED_SECONDS
            .with_label_values(&["metadata"])
            .set(0);
        ROLLING_HIT_RATIO_BPS
            .with_label_values(&["metadata"])
            .set(0);
        HITS_BY_TABLE_TOTAL
            .with_label_values(&["metadata", "other"])
            .inc_by(0);
        MISSES_BY_TABLE_TOTAL
            .with_label_values(&["metadata", "other"])
            .inc_by(0);
        LODC_DROPS_TOTAL
            .with_label_values(&["rowgroup", "submit_queue_overflow"])
            .inc_by(0);
        LODC_DROPS_TOTAL
            .with_label_values(&["rowgroup", "rate_limit"])
            .inc_by(0);
        LODC_INFLIGHT_BYTES.with_label_values(&["rowgroup"]).set(0);
        LODC_QUEUE_DEPTH.with_label_values(&["rowgroup"]).set(0);
        LODC_ADMIT_TOKENS_AVAILABLE
            .with_label_values(&["rowgroup"])
            .set(0);
        LODC_ADMIT_BURST_CAPACITY
            .with_label_values(&["rowgroup"])
            .set(0);
        LODC_RSS_THROTTLE_MULTIPLIER
            .with_label_values(&["rowgroup"])
            .set(10_000);
        LODC_RSS_PRESSURE_SECONDS_TOTAL
            .with_label_values(&["rowgroup"])
            .inc_by(0);
        ADMIT_REFUSED_TOTAL
            .with_label_values(&["draining"])
            .inc_by(0);
        DRAIN_ACTIVE.set(0);
        WTINYLFU_DECISIONS_TOTAL
            .with_label_values(&["admit"])
            .inc_by(0);
        WTINYLFU_DECAYS_TOTAL.with_label_values(&["both"]).inc_by(0);
        PEER_HIT_TOTAL.with_label_values(&["metadata"]).inc_by(0);
        PEER_MISS_TOTAL.with_label_values(&["metadata"]).inc_by(0);
        PEER_TIMEOUT_TOTAL
            .with_label_values(&["metadata"])
            .inc_by(0);
        PEER_ERROR_TOTAL
            .with_label_values(&["metadata", "network"])
            .inc_by(0);
        CONDITIONAL_NOT_MODIFIED_TOTAL
            .with_label_values(&["metadata"])
            .inc_by(0);
        CONDITIONAL_MODIFIED_TOTAL
            .with_label_values(&["metadata"])
            .inc_by(0);
        CONDITIONAL_SKIPPED_TOTAL
            .with_label_values(&["metadata"])
            .inc_by(0);
        CONDITIONAL_ERROR_TOTAL
            .with_label_values(&["metadata"])
            .inc_by(0);
        S3_DOLLARS_SAVED_TOTAL
            .with_label_values(&["us-east-1", "hit_memory"])
            .inc_by(0);
        S3_DOLLARS_SAVED_RATE_CENTS_PER_SEC
            .with_label_values(&["us-east-1", "hit_memory"])
            .set(0);
        // A4 (rc.7) — touch net counter + amortized-cost gauge.
        S3_DOLLARS_SAVED_NET_TOTAL
            .with_label_values(&["us-east-1"])
            .inc_by(0);
        SHELF_POOL_AMORTIZED_DOLLARS_PER_HOUR.set(0);
        // A6 (rc.7) — touch every label the cooperative gate can ever
        // bump so dashboards see the series on a freshly booted pod.
        for pool in ["metadata", "rowgroup"] {
            COOP_PEER_ADMITS_TOTAL.with_label_values(&[pool]).inc_by(0);
            COOP_PEER_DROPS_TOTAL.with_label_values(&[pool]).inc_by(0);
            COOP_PRIMARY_FORCE_ADMITS_TOTAL
                .with_label_values(&[pool])
                .inc_by(0);
        }
        // A6 (rc.7) — also pre-touch the new `reject_coop` decision
        // label so the existing `shelf_admissions_total` series
        // includes it on every freshly booted pod.
        ADMISSIONS_TOTAL
            .with_label_values(&["rowgroup", "reject_coop"])
            .inc_by(0);
        // B3 (rc.7) — pre-touch the new transient-gate series so
        // a stock OSS deploy publishes them as zeros even with the
        // gate default-off.
        TRANSIENT_REFUSALS_TOTAL
            .with_label_values(&["other"])
            .inc_by(0);
        TRANSIENT_DECISIONS_CACHED.set(0);
        TRANSIENT_REFRESH_ERRORS_TOTAL
            .with_label_values(&["other"])
            .inc_by(0);
        // B3 (rc.7) — also pre-touch the new `reject_transient`
        // decision label on the existing `shelf_admissions_total`
        // series so dashboards see the child on every booted pod.
        ADMISSIONS_TOTAL
            .with_label_values(&["rowgroup", "reject_transient"])
            .inc_by(0);
        HITS_BY_TAG_TOTAL
            .with_label_values(&["metadata", "none"])
            .inc_by(0);
        MISSES_BY_TAG_TOTAL
            .with_label_values(&["metadata", "none"])
            .inc_by(0);
        S3_SHIM_RESPONSE_BYTES_BY_TAG_TOTAL
            .with_label_values(&["get_object", "miss", "none"])
            .inc_by(0);
        AB_TAG_CAP_VIOLATIONS_TOTAL
            .with_label_values(&["cardinality"])
            .inc_by(0);
        DECODED_META_HITS_TOTAL
            .with_label_values(&["manifest"])
            .inc_by(0);
        DECODED_META_MISSES_TOTAL
            .with_label_values(&["manifest"])
            .inc_by(0);
        DECODED_META_DECODE_SECONDS
            .with_label_values(&["manifest"])
            .observe(0.0);
        DECODED_META_ENTRIES.with_label_values(&["manifest"]).set(0);
        DECODED_META_DECODE_ERRORS_TOTAL
            .with_label_values(&["manifest", "bad_magic"])
            .inc_by(0);
        // SHELF-46 — touch the new bloom-admission series.
        BLOOM_ADMIT_TOTAL.with_label_values(&["footer"]).inc_by(0);
        BLOOM_ADMIT_TOTAL
            .with_label_values(&["bloom_block"])
            .inc_by(0);
        BLOOM_ADMIT_TOTAL
            .with_label_values(&["not_applicable"])
            .inc_by(0);
        BLOOM_INDEX_ENTRIES.set(0);
        BLOOM_PARSE_ERRORS_TOTAL
            .with_label_values(&["bad_magic"])
            .inc_by(0);
        COMPRESS_BYTES_IN_TOTAL
            .with_label_values(&["rowgroup"])
            .inc_by(0);
        COMPRESS_BYTES_OUT_TOTAL
            .with_label_values(&["rowgroup"])
            .inc_by(0);
        COMPRESS_OUTCOMES_TOTAL
            .with_label_values(&["rowgroup", "compressed"])
            .inc_by(0);
        COMPRESS_SECONDS
            .with_label_values(&["rowgroup", "encode"])
            .observe(0.0);
        PREDICATE_PRUNE_REQUESTS_TOTAL
            .with_label_values(&["miss"])
            .inc_by(0);
        PREDICATE_PRUNE_SECONDS
            .with_label_values(&["miss"])
            .observe(0.0);
        PAGE_INDEX_CACHED_BYTES
            .with_label_values(&["metadata"])
            .set(0);
        PAGE_INDEX_PARSE_SECONDS
            .with_label_values(&["ok"])
            .observe(0.0);
        // SHELF-45 — touch every label the reactor can ever bump so
        // dashboards see the series on a freshly booted, idle pod.
        for outcome in [
            "received",
            "compaction_detected",
            "non_compaction_skipped",
            "replayed",
            "dropped_rate_limit",
        ] {
            REWARM_EVENTS_TOTAL.with_label_values(&[outcome]).inc_by(0);
        }
        for outcome in [
            "warmed",
            "failed",
            "skipped_already_warm",
            "skipped_pool_full",
        ] {
            REWARM_FILES_TOTAL.with_label_values(&[outcome]).inc_by(0);
            REWARM_BYTES_TOTAL.with_label_values(&[outcome]).inc_by(0);
        }
        REWARM_LAG_SECONDS
            .with_label_values(&["replayed"])
            .observe(0.0);
        REWARM_INFLIGHT_FILES
            .with_label_values(&["rowgroup"])
            .set(0);
        REWARM_QUEUE_DEPTH.with_label_values(&["rowgroup"]).set(0);
        for reason in [
            "iceberg_metadata",
            "origin_get",
            "admission_rejected",
            "pool_full",
            "cancelled",
        ] {
            REWARM_ERRORS_TOTAL.with_label_values(&[reason]).inc_by(0);
        }
        // A3 (rc.7) — touch every poller series so a freshly booted
        // pod with `cache.rewarm.enabled=false` still publishes the
        // documented label set as zeros.
        for result in ["no_change", "new_snapshot", "error"] {
            REWARM_POLLS_TOTAL
                .with_label_values(&["__none__", result])
                .inc_by(0);
        }
        REWARM_SNAPSHOTS_DETECTED_TOTAL
            .with_label_values(&["__none__"])
            .inc_by(0);
        REWARM_FILES_ENQUEUED_TOTAL
            .with_label_values(&["__none__"])
            .inc_by(0);
        REWARM_BYTES_ENQUEUED_TOTAL
            .with_label_values(&["__none__"])
            .inc_by(0);
        REWARM_BYTES_CAPPED_TOTAL
            .with_label_values(&["__none__"])
            .inc_by(0);
        // K2 (rc.8) — touch the pod-load gauges so a freshly booted
        // pod publishes them as zeros.
        POD_LOAD_QPS.set(0);
        POD_LOAD_SKEW_RATIO_BPS.set(100);
        POD_LOAD_PROBE_ERRORS_TOTAL.inc_by(0);

        let families = REGISTRY.gather();
        let names: HashSet<String> = families.iter().map(|f| f.name().to_owned()).collect();
        for want in EXPOSED_SERIES {
            assert!(
                names.contains(*want),
                "`/metrics` missing {want:?}; scraped: {names:?}",
            );
        }
    }
}
