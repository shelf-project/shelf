//! Configuration for `shelfd`.
//!
//! Ticket ownership:
//! - SHELF-02 — base config loader (YAML → `Config`), env overrides,
//!   `--config` flag wiring.
//! - SHELF-17 / SHELF-18 — pool sub-configs for `pool.metadata` (DRAM
//!   only) and `pool.rowgroup` (DRAM + NVMe hybrid). See ADR-0008.
//! - SHELF-24 — `pin_list` source (S3 bucket/key + reload interval).
//! - SHELF-25 — `admission` size threshold (see ADR-0003).
//!
//! The canonical key registry lives at `contracts/config-keys.md`; this
//! module is the Rust mirror. Any field added here that users configure
//! must be added to the contracts file in the same PR.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Top-level daemon configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Cluster identity. Used in HRW hashing (SHELF-19) and emitted as
    /// a metric label (`shelf_pod`).
    pub node: NodeConfig,

    /// HTTP data-plane listener (SHELF-02).
    pub http: HttpConfig,

    /// Control-plane listener (SHELF-23). HTTP/gRPC stub for
    /// `shelfctl` + `/stats` scraping.
    pub control: ControlConfig,

    /// S3 origin client (SHELF-05).
    pub origin: OriginConfig,

    /// Foyer cache pools. ADR-0008 mandates exactly two pools in v1:
    /// `metadata` (DRAM only) and `rowgroup` (DRAM + NVMe hybrid).
    pub pools: PoolsConfig,

    /// Admission policy (SHELF-25 / ADR-0003).
    pub admission: AdmissionConfig,

    /// Membership resolver (SHELF-20).
    pub membership: MembershipConfig,

    /// Pin list source + reload cadence (SHELF-24). Optional
    /// because dev clusters and the unit-test harness boot without a
    /// config-bucket; `None` means the pin-list loader is never
    /// spawned and the in-memory pin-set stays empty.
    #[serde(default)]
    pub pin_list: Option<PinListConfig>,

    /// Cap on the HEAD-response LRU (SHELF-07).
    #[serde(default = "default_head_lru_entries")]
    pub head_lru_entries: u64,

    /// Observability toggles (SHELF-08). Defaults to "no OTLP export";
    /// `observability.otlp_endpoint` (or the `SHELFD_OTLP_ENDPOINT`
    /// env override) enables the `tracing-opentelemetry` exporter.
    #[serde(default)]
    pub observability: ObservabilityConfig,

    /// S3-compatibility read shim (SHELF-22; see ADR-0003 scope).
    /// When `enabled`, `shelfd` binds a second HTTP listener on
    /// [`S3ShimConfig::bind_address`] that speaks `HeadObject`
    /// and `GetObject(Range)` so boto3 / DuckDB / Polars / `aws s3
    /// cp` can read through the cache without the Trino plugin.
    #[serde(default)]
    pub s3_shim: S3ShimConfig,

    /// SHELF-40 — audit-able S3 + data-transfer cost model. Maps
    /// every cache hit through `shelf-cost::CostModel` to a cents
    /// contribution on the `shelf_s3_dollars_saved_total` counter,
    /// using published AWS prices (citations live in the
    /// `crates/shelf-cost/` README).
    ///
    /// Defaults to "on, region us-east-1" so the OSS chart does
    /// the right thing out of the box. Operators on a different
    /// region override `region: ap-south-1` (or any other
    /// supported preset) in their values overlay and turn the
    /// counter off via `cache.cost.enabled: false` if needed.
    #[serde(default)]
    pub cost: shelf_cost::CostConfig,

    /// SHELF-42 — A/B tag receive path. Defaults to **off** so a
    /// freshly deployed OSS cluster never opens up Prometheus
    /// cardinality surface area until the operator has sized retention
    /// for the cap. Penpencil overlay flips `enabled: true`.
    /// See `docs/contracts/ab-tag.md`.
    #[serde(default)]
    pub ab_tag: AbTagConfig,

    /// SHELF-50 — decoded-metadata in-process LRU. Off by default;
    /// see `shelfd/docs/design-notes/SHELF-50-decoded-metadata-cache.md`
    /// for the rollout sequencing (downstream tickets SHELF-46 /
    /// SHELF-37 / SHELF-47 flip it on once they consume the
    /// decoded data).
    #[serde(default)]
    pub decoded_meta: DecodedMetaConfig,
}

fn default_head_lru_entries() -> u64 {
    10_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// Pod name, e.g. `shelf-2` from the StatefulSet ordinal.
    pub id: String,
    /// Optional capacity weight override. Normally pulled from
    /// `/stats`; this is an ops escape hatch.
    #[serde(default)]
    pub weight_override: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    /// `0.0.0.0:9090` in the Helm values. HTTP/2 only per ADR-0004.
    pub listen: SocketAddr,
    /// Per-request server budget. Enforced by a `tower` layer.
    #[serde(with = "humantime_serde", default = "default_http_timeout")]
    pub request_timeout: Duration,
}

fn default_http_timeout() -> Duration {
    Duration::from_secs(30)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlConfig {
    pub listen: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginConfig {
    /// Default S3 bucket. The plugin may override per-request.
    pub bucket: String,
    /// Optional override for LocalStack / MinIO in integration tests.
    #[serde(default)]
    pub endpoint_url: Option<String>,
    /// AWS region. Falls back to SDK default chain when absent.
    #[serde(default)]
    pub region: Option<String>,
    /// Max in-flight S3 GET requests per pod.
    ///
    /// SHELF-21f (2026-04-29) — lowered the default from 256 → 128
    /// after the LODC submit-queue overflow regression on shelf-0/1
    /// in the alluxio NodePool. Each in-flight request reserves a
    /// receive buffer for up to one ~32 MiB Parquet rowgroup, so
    /// `max_inflight × 32 MiB` is the worst-case RSS footprint of
    /// the origin pool. With the previous default of 256, this was
    /// up to 8 GiB on top of the 19 GiB Foyer DRAM caps and the
    /// 1 GiB LODC submit-queue cap, leaving zero headroom under
    /// the ~27.3 GiB node-allocatable ceiling on the m6a/c6a-4xlarge
    /// alluxio pool. 128 caps the worst case at ~4 GiB and keeps
    /// the budget feasible. Operators who run on bigger nodes can
    /// override via `origin.pool.maxConnections` in the chart values.
    /// See `shelfd/docs/runbooks/2026-04-shelf-1-oom.md` and the
    /// 2026-04-29 LODC regression entry in `CHANGELOG.md`.
    #[serde(default = "default_max_inflight")]
    pub max_inflight: usize,
}

fn default_max_inflight() -> usize {
    128
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolsConfig {
    pub metadata: MetadataPoolConfig,
    pub rowgroup: RowGroupPoolConfig,
}

/// Absolute default for `pool.metadata`'s DRAM budget, in bytes.
///
/// 5 GiB per ADR-0008 and SHELF-17. The Rust side has no
/// `Default` impl today — config comes from `charts/shelf/values.yaml`
/// (`cache.pools.metadata.sizeBytes`) — so this constant is the
/// single source of truth for anyone constructing a `PoolsConfig`
/// in-process (benchmarks, integration tests, future `Default`
/// impls). Keep it in sync with the Helm value and with ADR-0008
/// §Decision.
pub const DEFAULT_METADATA_DRAM_BYTES: u64 = 5 * (1 << 30);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataPoolConfig {
    /// DRAM quota in bytes. 5 GiB absolute per ADR-0008 — see
    /// [`DEFAULT_METADATA_DRAM_BYTES`] and SHELF-17.
    pub dram_bytes: u64,
}

/// In-memory eviction policy for the row-group pool's DRAM tier.
///
/// SHELF-E1b — ADR-0009 originally pinned the hybrid pool to S3-FIFO
/// for scan resistance. In production we observed that S3-FIFO's
/// "small queue → main queue → disk" promotion path keeps **one-shot**
/// reads (Metabase admin dashboards, ad-hoc BI) off NVMe entirely:
/// items expire from the small queue before they ever earn promotion,
/// so `shelf_disk_bytes_used` stays at zero indefinitely.
///
/// LRU is a workload-agnostic alternative — every memory eviction
/// flows straight through to the NVMe ring, so disk gets populated
/// even on one-shot patterns. The trade-off is reduced scan
/// resistance under bursty `INSERT INTO` rewrites; we accept that
/// for v0.5 and revisit once SHELF-26 replays produce per-policy
/// hit-ratio numbers on rep-2's 7-day trace.
///
/// `s3_fifo` remains available behind the config flag so operators
/// can flip back without a code change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvictionPolicy {
    /// Foyer S3-FIFO — ADR-0009 default. Memory tier holds a small
    /// probationary queue; only entries re-accessed there are
    /// promoted to the main queue (and consequently to NVMe).
    S3Fifo,
    /// Foyer LRU. Every memory eviction flows through to disk.
    /// Default for v0.5 onwards (SHELF-E1b).
    Lru,
    /// Foyer LFU (W-TinyLFU). Frequency-aware; useful when a small
    /// hot set dominates.
    Lfu,
    /// Foyer FIFO. Insertion-order eviction; cheapest, no promotion
    /// machinery — used primarily by replay benchmarks.
    Fifo,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::Lru
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowGroupPoolConfig {
    /// DRAM portion of the hybrid pool.
    pub dram_bytes: u64,
    /// NVMe directory (the PVC mount point). Ignored when
    /// [`RowGroupPoolConfig::nvme_bytes`] is `0`.
    pub nvme_dir: PathBuf,
    /// NVMe capacity in bytes. `0` disables the hybrid tier and
    /// keeps `rowgroup` DRAM-only — see ADR-0009 and SHELF-18.
    pub nvme_bytes: u64,
    /// In-memory eviction policy. See [`EvictionPolicy`].
    ///
    /// Defaults to [`EvictionPolicy::Lru`] (SHELF-E1b) so freshly
    /// deployed clusters populate NVMe out of the box. Existing
    /// YAML without this field continues to parse — `serde(default)`
    /// fills in the LRU default.
    #[serde(default)]
    pub eviction_policy: EvictionPolicy,
    /// Foyer Large-Object Disk Cache (LODC) tunables for the NVMe
    /// tier. The defaults are deliberately *higher* than Foyer's
    /// own defaults (`flushers=1`, `buffer_pool_size=16 MiB`,
    /// `submit_queue_size_threshold=32 MiB`) because shelfd's
    /// production workload spills 256 in-flight × ~32 MiB Parquet
    /// rowgroups; Foyer's stock sizing causes
    /// `[lodc] submit queue overflow` warnings + RSS bloat that
    /// previously OOM-killed `shelf-1` (2026-04-27). See
    /// `shelfd/docs/runbooks/2026-04-shelf-1-oom.md`.
    ///
    /// Field is `#[serde(default)]` so existing config YAML keeps
    /// parsing.
    #[serde(default)]
    pub disk_cache: RowGroupDiskCacheConfig,
}

/// Foyer LODC pipeline tunables for the rowgroup hybrid pool.
///
/// All fields are optional; an absent value leaves the matching
/// Foyer 0.12 default in place. The chart's `values.yaml`
/// surfaces all three under
/// `cache.pools.rowgroup.diskCache.{flushers,bufferPoolSizeBytes,
/// submitQueueSizeThresholdBytes}`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowGroupDiskCacheConfig {
    /// Concurrent flusher tasks. Default in Foyer 0.12 is `1`,
    /// which serialises every region write to NVMe and saturates
    /// trivially under burst. Production target is `4`.
    #[serde(default)]
    pub flushers: Option<usize>,
    /// Total flush buffer pool size in bytes (shared across
    /// flushers). Foyer 0.12 default is 16 MiB. We bump this to
    /// 256 MiB in production so a single burst of inflight Parquet
    /// rowgroups does not immediately fill the submit queue.
    #[serde(default)]
    pub buffer_pool_size_bytes: Option<u64>,
    /// Submit-queue size threshold in bytes. Once the total
    /// estimated size of entries waiting to be flushed crosses this
    /// threshold, **further entries are dropped** (Foyer logs
    /// `[lodc] submit queue overflow`). The Foyer default is
    /// `buffer_pool_size * 2`; we set this explicitly to bound RSS
    /// growth from the LODC pipeline. Production target is 1 GiB.
    #[serde(default)]
    pub submit_queue_size_threshold_bytes: Option<u64>,
    /// **Deprecated (SHELF-21e, preview-10):** rate-based admission
    /// throttling was removed from the LODC pipeline because the
    /// underlying Foyer 0.12 [`foyer::RateLimitPicker`] adds latency
    /// to every write regardless of actual queue pressure (the token
    /// bucket fills on time, not on observed drain rate). The
    /// preview-8 attempt pegged `hit_disk` p99 at the histogram max
    /// during sustained ingress; reverted in preview-9 / helm rev-22.
    ///
    /// Back-pressure now lives in
    /// [`crate::lodc_backpressure::LodcBackpressure`] — a level-based
    /// gate at shelfd's own admission seam, watermarked off
    /// `submit_queue_size_threshold_bytes`. No new ConfigMap key
    /// is required; tune the existing
    /// `submit_queue_size_threshold_bytes` to move the watermark.
    ///
    /// The field is retained (with `#[serde(default)]`) so existing
    /// `values.yaml` overlays that still set it continue to parse;
    /// the value is silently ignored at runtime.
    #[serde(default)]
    pub admission_bytes_per_sec: Option<u64>,
    /// SHELF-29 — Independent-queue admission rate-limiter. Sits in
    /// front of `cache.insert(...)` and bounds the **rate** of
    /// admissions feeding Foyer's submit queue, complementing the
    /// SHELF-21e *level* gate. See
    /// `agents/out/SHELF-29-independent-queue-rate-limiter.md`.
    ///
    /// All sub-fields are optional via `#[serde(default)]` on the
    /// nested struct; an absent block leaves the limiter on with
    /// production defaults, which is the desired behaviour after
    /// the 2026-04-29 OOM incident.
    #[serde(default)]
    pub admission: LodcAdmissionConfig,
}

/// SHELF-29 — Independent-queue token-bucket admission limiter config.
///
/// Defaults target the chronic ~700 admit/s × 4 MiB burst envelope
/// observed on `1.0.0-rc.3` (rep-1 + rep-2): refill at 200 MiB/s
/// (a hair below sustained EBS gp3), burst capacity 256 MiB
/// (≈ 64 × 4 MiB rowgroups). The limiter is on by default; operators
/// can disable per-pod via `enabled: false` or globally via the
/// `SHELFD_LODC_ADMISSION=off` env var.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LodcAdmissionConfig {
    /// Master switch. Default `true`. The env var
    /// `SHELFD_LODC_ADMISSION=off` (case-insensitive `off`/`0`/`false`)
    /// flips this to `false` at config load without a redeploy.
    #[serde(default = "default_admission_enabled")]
    pub enabled: bool,
    /// Token bucket refill rate, in bytes/sec. `0` is the kill-switch
    /// path — every admit drops with `reason="rate_limit"`, useful as
    /// a one-shot "stop accepting writes" knob without taking the
    /// pod down.
    #[serde(default = "default_target_bytes_per_sec")]
    pub target_bytes_per_sec: u64,
    /// Token bucket capacity, in bytes. Caps to `u32::MAX` (4 GiB) at
    /// limiter construction because the packed atomic state encodes
    /// the live token count in 32 bits.
    #[serde(default = "default_max_burst_bytes")]
    pub max_burst_bytes: u64,
    /// Forward-compatibility knob: optional secondary safety on the
    /// concurrent admit count. The byte budget is the dominant gate
    /// in v1; this field rarely binds under defaults.
    #[serde(default = "default_max_inflight_admissions")]
    pub max_inflight_admissions: u64,
}

fn default_admission_enabled() -> bool {
    true
}

fn default_target_bytes_per_sec() -> u64 {
    200 * 1024 * 1024
}

fn default_max_burst_bytes() -> u64 {
    256 * 1024 * 1024
}

fn default_max_inflight_admissions() -> u64 {
    1024
}

impl Default for LodcAdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: default_admission_enabled(),
            target_bytes_per_sec: default_target_bytes_per_sec(),
            max_burst_bytes: default_max_burst_bytes(),
            max_inflight_admissions: default_max_inflight_admissions(),
        }
    }
}

impl RowGroupPoolConfig {
    /// Validate the NVMe block. SHELF-18: if `nvme_bytes > 0` the
    /// directory must be non-empty, absolute, and either already
    /// exist or be creatable. When `nvme_bytes == 0` we skip
    /// validation entirely so an unused `nvme_dir` field (dev,
    /// unit tests) does not block startup.
    pub fn validate_nvme(&self) -> crate::Result<()> {
        if self.nvme_bytes == 0 {
            return Ok(());
        }
        if self.nvme_dir.as_os_str().is_empty() {
            return Err(crate::Error::Config(
                "pools.rowgroup.nvme_dir must be non-empty when nvme_bytes > 0".into(),
            ));
        }
        if !self.nvme_dir.is_absolute() {
            return Err(crate::Error::Config(format!(
                "pools.rowgroup.nvme_dir must be absolute, got `{}`",
                self.nvme_dir.display()
            )));
        }
        // If the path exists it must be a directory. If it does
        // not exist we do a dry-run `create_dir_all` so the daemon
        // fails at boot rather than at first insert. `FoyerStore::open`
        // will call `create_dir_all` again — that is a cheap no-op
        // once the dir exists and keeps the two callsites honest.
        if self.nvme_dir.exists() {
            if !self.nvme_dir.is_dir() {
                return Err(crate::Error::Config(format!(
                    "pools.rowgroup.nvme_dir `{}` exists but is not a directory",
                    self.nvme_dir.display()
                )));
            }
        } else {
            std::fs::create_dir_all(&self.nvme_dir).map_err(|e| {
                crate::Error::Config(format!(
                    "pools.rowgroup.nvme_dir `{}` is not creatable: {e}",
                    self.nvme_dir.display()
                ))
            })?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionConfig {
    /// Refuse admit for objects larger than this unless pinned.
    /// Default 1 GiB per ADR-0003.
    pub size_threshold_bytes: u64,
    /// If true, pinned objects bypass the size threshold.
    #[serde(default = "default_true")]
    pub pinned_bypass: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipConfig {
    /// The K8s headless service DNS name, e.g. `shelf.shelf.svc.cluster.local`.
    pub headless_service: String,
    /// DNS re-resolution cadence. 5 s per SHELF-20.
    #[serde(with = "humantime_serde", default = "default_dns_refresh")]
    pub dns_refresh: Duration,
    /// Set to `false` to skip spawning the resolver (e.g. dev / single-pod
    /// boots, the unit-test harness, or when running shelfd outside K8s
    /// where the headless DNS name does not resolve). When `false` the
    /// local `Router` stays empty and `is_local_owner` returns `false`
    /// for every key — i.e. shelfd serves only what it has, no peer
    /// rebalancing. Defaults to `true`.
    #[serde(default = "default_membership_enabled")]
    pub enabled: bool,
    /// Control-plane port the resolver scrapes for `/stats`. Defaults to
    /// `9090` to match `charts/shelf/values.yaml service.adminPort`.
    #[serde(default = "default_membership_stats_port")]
    pub stats_port: u16,
    /// Data-plane port baked into `Member::endpoint` so peers know
    /// where to send forwards. Defaults to `9092` to match
    /// `charts/shelf/values.yaml service.s3shimPort`.
    #[serde(default = "default_membership_data_port")]
    pub data_port: u16,
    /// Hard wall-clock deadline for one peer's `/stats` probe. Defaults
    /// to 1 s — generous against same-AZ p99 (< 5 ms) but small enough
    /// that one slow peer cannot stall a refresh round.
    #[serde(with = "humantime_serde", default = "default_stats_timeout")]
    pub stats_timeout: Duration,
    /// Time to advertise `draining: true` on `/stats` before the
    /// process exits. Must be ≥ 2× `dns_refresh` so every peer has
    /// observed at least one refresh window with our drain bit set.
    /// Defaults to 15 s.
    #[serde(with = "humantime_serde", default = "default_drain_grace")]
    pub drain_grace: Duration,
    /// Capacity-bytes per HRW weight unit. A pod with 1 GiB of cache
    /// has weight 1; a pod with 100 GiB has weight 100. Defaults to
    /// 1 GiB.
    #[serde(default = "default_weight_unit_bytes")]
    pub weight_unit_bytes: u64,
}

fn default_dns_refresh() -> Duration {
    Duration::from_secs(5)
}

fn default_membership_enabled() -> bool {
    true
}

fn default_membership_stats_port() -> u16 {
    9090
}

fn default_membership_data_port() -> u16 {
    9092
}

fn default_stats_timeout() -> Duration {
    Duration::from_secs(1)
}

fn default_drain_grace() -> Duration {
    Duration::from_secs(15)
}

fn default_weight_unit_bytes() -> u64 {
    1024 * 1024 * 1024
}

/// SHELF-24 pin-list config.
///
/// The loader reads `s3://{bucket}/{key}` on boot and then refreshes
/// on both a timer and `SIGHUP`. We split bucket + key rather than
/// accepting a single `s3://…` URI because:
///
/// 1. The `aws-sdk-s3` client already owns the region + endpoint
///    resolution — a URI would duplicate that logic.
/// 2. Helm charts already template bucket + key as separate values
///    (`configBucket`, `pinListKey`); matching the chart shape saves
///    an adapter layer in the daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinListConfig {
    /// S3 bucket name (no `s3://` prefix).
    pub bucket: String,
    /// Object key, e.g. `shelf/pin_list.json`.
    #[serde(default = "default_pin_key")]
    pub key: String,
    /// SIGHUP + periodic reload cadence. 15 min per SHELF-24.
    #[serde(with = "humantime_serde", default = "default_pin_reload")]
    pub refresh_period: Duration,
    /// Allow operators to keep the config stanza but silence the
    /// loader (e.g. during incident response).
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_pin_key() -> String {
    "shelf/pin_list.json".to_owned()
}

fn default_pin_reload() -> Duration {
    Duration::from_secs(15 * 60)
}

/// Observability subsystem config (SHELF-08).
///
/// The OTLP exporter is optional — when `otlp_endpoint` is `None`,
/// `shelfd` runs without a background exporter and never requires a
/// collector. A misconfigured endpoint must not take the daemon down:
/// [`crate::telemetry::init`] is expected to log a warning and
/// continue.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// `grpc://tempo-distributor:4317` or similar. Overridable via
    /// `SHELFD_OTLP_ENDPOINT` so operators can point a pod at a
    /// sidecar collector without editing the mounted YAML.
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
}

/// S3-compatibility read shim listener (SHELF-22).
///
/// See `shelfd/docs/design-notes/SHELF-22-s3-compat-shim.md` +
/// ADR-0003. This listener runs on a dedicated port so it can be
/// firewalled independently of the native `/cache/...` data plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3ShimConfig {
    /// Master switch. Defaults to `true` — generic clients are
    /// the headline SHELF-22 use case so disabling them is the
    /// opt-out path, not the default.
    #[serde(default = "S3ShimConfig::default_enabled")]
    pub enabled: bool,
    /// `0.0.0.0:9092` by convention; operators can narrow to
    /// `127.0.0.1:9092` in dev.
    #[serde(default = "S3ShimConfig::default_bind_address")]
    pub bind_address: String,
    /// Cap on unbounded `GetObject` (no `Range:` header). A
    /// request above this size returns `501 NotImplemented` with
    /// an S3 XML envelope instructing the client to issue a
    /// ranged read. 256 MiB keeps worst-case memory bounded for
    /// Polars / DuckDB full-object reads while still covering
    /// 99% of the Parquet files we see in rep-2 trino_logs.
    #[serde(default = "S3ShimConfig::default_max_full_object_bytes")]
    pub max_full_object_bytes: u64,
}

impl S3ShimConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_bind_address() -> String {
        "0.0.0.0:9092".to_owned()
    }
    fn default_max_full_object_bytes() -> u64 {
        256 * 1024 * 1024
    }
}

/// SHELF-50 — decoded-metadata cache configuration.
///
/// `enabled` is the master flag; the entry-count caps default to
/// 10 000 each (matches `cache.decodedMeta.maxManifestEntries` /
/// `cache.decodedMeta.maxFooterEntries` in `values.yaml`). The
/// runtime cap is exposed for SHELF-50b sizing follow-ups; v1
/// memory budgeting is by entry count only — see the design note
/// for the trade-off.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecodedMetaConfig {
    /// Master switch. Defaults to **false** because v1 ships the
    /// cache infrastructure ahead of any consumer; flipping this
    /// on without a downstream consumer just wastes RSS. Downstream
    /// tickets (SHELF-46 / SHELF-37 / SHELF-47) flip it on as part
    /// of their rollouts.
    #[serde(default = "DecodedMetaConfig::default_enabled")]
    pub enabled: bool,
    /// Cap on resident decoded `ManifestFile` entries. Bounded by
    /// `parking_lot::Mutex<LruCache>`; the per-entry size is
    /// dominated by the raw avro bytes (typically 32–256 KiB on
    /// production manifests).
    #[serde(default = "DecodedMetaConfig::default_max_manifest_entries")]
    pub max_manifest_entries: usize,
    /// Cap on resident decoded `ParquetMetaData` entries. Per-entry
    /// size scales with row-group count (typical 4–32 KiB).
    #[serde(default = "DecodedMetaConfig::default_max_footer_entries")]
    pub max_footer_entries: usize,
}

impl DecodedMetaConfig {
    fn default_enabled() -> bool {
        false
    }
    fn default_max_manifest_entries() -> usize {
        10_000
    }
    fn default_max_footer_entries() -> usize {
        10_000
    }
}

impl Default for DecodedMetaConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            max_manifest_entries: Self::default_max_manifest_entries(),
            max_footer_entries: Self::default_max_footer_entries(),
        }
    }
}

impl Default for S3ShimConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            bind_address: Self::default_bind_address(),
            max_full_object_bytes: Self::default_max_full_object_bytes(),
        }
    }
}

/// SHELF-42 — A/B tag receive path config.
///
/// The contract spec lives at `docs/contracts/ab-tag.md`; this struct
/// is the Rust mirror of the `cache.abTag.*` Helm values. Default-off
/// for OSS deployments; operators with sized Prometheus retention flip
/// `enabled: true` in their overlay.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbTagConfig {
    /// Master switch. The chart default is `false`; operator overlays
    /// (e.g. `<prod-overlay>/values-prod.yaml`) flip it to `true`. The
    /// Trino plugin's *forwarding* side has no kill switch — session
    /// properties are metadata, the per-request HTTP-header cost is
    /// negligible, and a tag with no downstream consumer (because
    /// shelfd is `enabled: false`) is silently ignored.
    #[serde(default = "AbTagConfig::default_enabled")]
    pub enabled: bool,

    /// Per-pod cardinality cap. The 17th distinct tag wire form within
    /// a scrape window folds into the sentinel `"other"` and bumps
    /// `shelf_ab_tag_cap_violations_total{reason="cardinality"}` once
    /// per (window, distinct offending tag).
    #[serde(default = "AbTagConfig::default_max_distinct_tags")]
    pub max_distinct_tags: usize,

    /// Length of one cap-enforcement window. Defaults to 60 s — long
    /// enough to bracket a 30 s Prometheus scrape, short enough that a
    /// stale cohort eventually rolls out of the sentinel state.
    #[serde(
        with = "humantime_serde",
        default = "AbTagConfig::default_scrape_window"
    )]
    pub scrape_window: Duration,
}

impl AbTagConfig {
    fn default_enabled() -> bool {
        false
    }
    fn default_max_distinct_tags() -> usize {
        crate::ab_tag::DEFAULT_MAX_DISTINCT_TAGS
    }
    fn default_scrape_window() -> Duration {
        crate::ab_tag::DEFAULT_SCRAPE_WINDOW
    }
}

impl Default for AbTagConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            max_distinct_tags: Self::default_max_distinct_tags(),
            scrape_window: Self::default_scrape_window(),
        }
    }
}

impl Config {
    /// Load and validate a config from disk.
    ///
    /// Order: read YAML → parse with `deny_unknown_fields` → apply
    /// `SHELFD_*` env overrides → validate. Any failure returns
    /// [`crate::Error::Config`] with the path and cause in the message.
    pub fn from_path(path: &Path) -> crate::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| crate::Error::Config(format!("read {}: {e}", path.display())))?;
        Self::from_yaml_str(&contents, Some(path))
    }

    /// Parse from an in-memory YAML string (unit-test entry point).
    ///
    /// The `origin_path` parameter is only used to produce clearer
    /// error messages; it is optional for tests.
    pub fn from_yaml_str(s: &str, origin_path: Option<&Path>) -> crate::Result<Self> {
        let path_label = origin_path
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<inline>".to_owned());
        let mut cfg: Config = serde_yaml::from_str(s)
            .map_err(|e| crate::Error::Config(format!("parse {path_label}: {e}")))?;
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    /// Apply `SHELFD_*` env overrides. Kept narrow on purpose: only the
    /// knobs an operator needs to flip without editing the mounted
    /// YAML (MinIO endpoint for dev, node id from the K8s downward
    /// API, bucket for cross-env reuse). Everything else stays in YAML
    /// so misconfigurations are reviewable.
    fn apply_env_overrides(&mut self) {
        // `SHELFD_POD_ID` is the preferred alias Agent 5's SHELF-20
        // membership loader reads; it wins over `SHELFD_NODE_ID` when
        // both are set so operators can flip pods without editing YAML.
        if let Ok(v) = std::env::var("SHELFD_NODE_ID") {
            self.node.id = v;
        }
        if let Ok(v) = std::env::var("SHELFD_POD_ID") {
            self.node.id = v;
        }
        if let Ok(v) = std::env::var("SHELFD_ORIGIN_ENDPOINT") {
            self.origin.endpoint_url = Some(v);
        }
        if let Ok(v) = std::env::var("SHELFD_ORIGIN_BUCKET") {
            self.origin.bucket = v;
        }
        if let Ok(v) = std::env::var("SHELFD_ORIGIN_REGION") {
            self.origin.region = Some(v);
        }
        if let Ok(v) = std::env::var("SHELFD_OTLP_ENDPOINT") {
            if !v.is_empty() {
                self.observability.otlp_endpoint = Some(v);
            }
        }
        // SHELF-29 — emergency off-switch for the admission rate
        // limiter without a redeploy. Anything other than the canonical
        // falsy values is a no-op, so a misconfigured value never
        // silently disables production back-pressure.
        if crate::admission_limiter::env_disable_override() {
            self.pools.rowgroup.disk_cache.admission.enabled = false;
        }
        // SHELF-42 — operator override for the A/B tag receive path.
        // Accepts canonical truthy/falsy values only so a typo can't
        // silently flip Prometheus cardinality on or off.
        if let Ok(v) = std::env::var("SHELFD_AB_TAG") {
            match v.trim().to_ascii_lowercase().as_str() {
                "on" | "1" | "true" | "yes" => self.ab_tag.enabled = true,
                "off" | "0" | "false" | "no" => self.ab_tag.enabled = false,
                _ => {}
            }
        }
    }

    /// Enforce the invariants the type system cannot. Add checks here
    /// rather than sprinkling `assert!`s through the codebase.
    fn validate(&self) -> crate::Result<()> {
        if self.node.id.is_empty() {
            return Err(crate::Error::Config("node.id must be non-empty".into()));
        }
        if self.origin.bucket.is_empty() {
            return Err(crate::Error::Config(
                "origin.bucket must be non-empty".into(),
            ));
        }
        if self.pools.metadata.dram_bytes == 0 {
            return Err(crate::Error::Config(
                "pools.metadata.dram_bytes must be > 0".into(),
            ));
        }
        if self.pools.rowgroup.dram_bytes == 0 {
            return Err(crate::Error::Config(
                "pools.rowgroup.dram_bytes must be > 0".into(),
            ));
        }
        self.pools.rowgroup.validate_nvme()?;
        if self.admission.size_threshold_bytes == 0 {
            return Err(crate::Error::Config(
                "admission.size_threshold_bytes must be > 0".into(),
            ));
        }
        if self.membership.headless_service.is_empty() {
            return Err(crate::Error::Config(
                "membership.headless_service must be non-empty".into(),
            ));
        }
        if self.head_lru_entries == 0 {
            return Err(crate::Error::Config("head_lru_entries must be > 0".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Kept as a string constant rather than a fixture file so the test
    // stays self-contained. The canonical shape is mirrored in
    // `charts/shelf/values.yaml` (cache.*, origin.*).
    const MINIMAL: &str = r#"
node:
  id: shelf-0
http:
  listen: "0.0.0.0:9090"
control:
  listen: "0.0.0.0:9093"
origin:
  bucket: test-bucket
pools:
  metadata:
    dram_bytes: 1048576
  rowgroup:
    dram_bytes: 4194304
    nvme_dir: /var/lib/shelf/rg
    nvme_bytes: 0
admission:
  size_threshold_bytes: 1073741824
membership:
  headless_service: shelf.shelf.svc.cluster.local
pin_list:
  bucket: "cfg"
  key: "shelf/pin_list.json"
  refresh_period: "15m"
"#;

    #[test]
    fn parses_minimal_config() {
        let cfg = Config::from_yaml_str(MINIMAL, None).expect("minimal config must parse");
        assert_eq!(cfg.node.id, "shelf-0");
        assert_eq!(cfg.origin.bucket, "test-bucket");
        assert_eq!(cfg.pools.metadata.dram_bytes, 1_048_576);
        assert_eq!(cfg.admission.size_threshold_bytes, 1_073_741_824);
        // Defaults applied.
        assert_eq!(cfg.origin.max_inflight, 128);
        assert!(cfg.admission.pinned_bypass);
    }

    #[test]
    fn env_override_replaces_endpoint() {
        // Tests set env vars on the process — keep them scoped to
        // names we own so concurrent tests don't collide.
        // SAFETY: env var writes are unsafe in 2024 edition; single-
        // threaded test mutex is the project norm elsewhere. Here we
        // use a unique var name and unset after.
        unsafe {
            std::env::set_var("SHELFD_ORIGIN_ENDPOINT", "http://127.0.0.1:9000");
        }
        let cfg = Config::from_yaml_str(MINIMAL, None).expect("parse");
        assert_eq!(
            cfg.origin.endpoint_url.as_deref(),
            Some("http://127.0.0.1:9000")
        );
        unsafe {
            std::env::remove_var("SHELFD_ORIGIN_ENDPOINT");
        }
    }

    #[test]
    fn rejects_zero_metadata_dram() {
        let bad = MINIMAL.replace("dram_bytes: 1048576", "dram_bytes: 0");
        let err = Config::from_yaml_str(&bad, None).unwrap_err();
        assert!(
            matches!(&err, crate::Error::Config(m) if m.contains("metadata.dram_bytes")),
            "expected metadata.dram_bytes error, got: {err:?}"
        );
    }

    #[test]
    fn rejects_unknown_field() {
        let bad = MINIMAL.to_owned() + "\ngrafana: true\n";
        let err = Config::from_yaml_str(&bad, None).unwrap_err();
        assert!(matches!(err, crate::Error::Config(_)));
    }

    // SHELF-E1b — eviction policy parsing.
    //
    // The pre-E1b on-disk shape (no `eviction_policy:` field) must
    // continue to parse so existing values files in deployments-repo
    // don't need a synchronized bump. Default == LRU.

    #[test]
    fn rowgroup_eviction_policy_defaults_to_lru() {
        let cfg = Config::from_yaml_str(MINIMAL, None).expect("parse");
        assert_eq!(cfg.pools.rowgroup.eviction_policy, EvictionPolicy::Lru);
    }

    #[test]
    fn rowgroup_eviction_policy_accepts_all_known_variants() {
        for (yaml_value, expected) in [
            ("lru", EvictionPolicy::Lru),
            ("s3_fifo", EvictionPolicy::S3Fifo),
            ("lfu", EvictionPolicy::Lfu),
            ("fifo", EvictionPolicy::Fifo),
        ] {
            let yaml = MINIMAL.replace(
                "    nvme_bytes: 0",
                &format!("    nvme_bytes: 0\n    eviction_policy: {yaml_value}"),
            );
            let cfg = Config::from_yaml_str(&yaml, None)
                .unwrap_or_else(|e| panic!("parse {yaml_value}: {e:?}"));
            assert_eq!(cfg.pools.rowgroup.eviction_policy, expected);
        }
    }

    #[test]
    fn rowgroup_eviction_policy_rejects_unknown_variant() {
        let yaml = MINIMAL.replace(
            "    nvme_bytes: 0",
            "    nvme_bytes: 0\n    eviction_policy: arc",
        );
        let err = Config::from_yaml_str(&yaml, None).unwrap_err();
        assert!(matches!(err, crate::Error::Config(_)));
    }

    // SHELF-21e-v2 — `RowGroupDiskCacheConfig::admission_bytes_per_sec`
    // plumbing. Verifies the new field is optional (unset YAML keeps
    // parsing) and that a set value round-trips into the struct, so
    // `build_rowgroup_pool` can hand it to Foyer's `RateLimitPicker`.
    #[test]
    fn rowgroup_disk_cache_admission_defaults_to_none() {
        let cfg = Config::from_yaml_str(MINIMAL, None).expect("parse");
        assert!(
            cfg.pools
                .rowgroup
                .disk_cache
                .admission_bytes_per_sec
                .is_none(),
            "default must be unset so pre-preview-8 values.yaml keeps working"
        );
    }

    #[test]
    fn rowgroup_disk_cache_admission_accepts_set_value() {
        let yaml = MINIMAL.replace(
            "    nvme_bytes: 0",
            "    nvme_bytes: 0\n    disk_cache:\n      admission_bytes_per_sec: 209715200",
        );
        let cfg = Config::from_yaml_str(&yaml, None).expect("parse");
        assert_eq!(
            cfg.pools.rowgroup.disk_cache.admission_bytes_per_sec,
            Some(209_715_200)
        );
    }

    // SHELF-18 — `RowGroupPoolConfig::validate_nvme` unit tests.
    //
    // Path handling is intentionally strict at boot (absolute path,
    // reject files-that-look-like-dirs) so operators hear about
    // misconfigurations before the daemon binds its listener. The
    // `nvme_bytes == 0` path is the noop-escape used by SHELF-17
    // tests and the no-PVC dev cluster.

    #[test]
    fn validate_nvme_noop_when_zero_bytes() {
        let cfg = RowGroupPoolConfig {
            dram_bytes: 1,
            nvme_dir: PathBuf::from(""),
            nvme_bytes: 0,
            eviction_policy: EvictionPolicy::default(),
            disk_cache: RowGroupDiskCacheConfig::default(),
        };
        cfg.validate_nvme().expect("zero nvme bytes must be valid");
    }

    #[test]
    fn validate_nvme_rejects_empty_dir_when_enabled() {
        let cfg = RowGroupPoolConfig {
            dram_bytes: 1,
            nvme_dir: PathBuf::from(""),
            nvme_bytes: 1,
            eviction_policy: EvictionPolicy::default(),
            disk_cache: RowGroupDiskCacheConfig::default(),
        };
        let err = cfg.validate_nvme().unwrap_err();
        assert!(
            matches!(&err, crate::Error::Config(m) if m.contains("nvme_dir must be non-empty"))
        );
    }

    #[test]
    fn validate_nvme_rejects_relative_path() {
        let cfg = RowGroupPoolConfig {
            dram_bytes: 1,
            nvme_dir: PathBuf::from("relative/path"),
            nvme_bytes: 1,
            eviction_policy: EvictionPolicy::default(),
            disk_cache: RowGroupDiskCacheConfig::default(),
        };
        let err = cfg.validate_nvme().unwrap_err();
        assert!(matches!(&err, crate::Error::Config(m) if m.contains("must be absolute")));
    }

    #[test]
    fn validate_nvme_creates_missing_absolute_dir() {
        // Pick a deterministic per-test path under `std::env::temp_dir`
        // so we do not pull in `tempfile` for what is a ~ms
        // existence check. The test cleans up after itself.
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "shelfd-validate-nvme-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        assert!(!dir.exists(), "precondition: dir must not exist");
        let cfg = RowGroupPoolConfig {
            dram_bytes: 1,
            nvme_dir: dir.clone(),
            nvme_bytes: 1,
            eviction_policy: EvictionPolicy::default(),
            disk_cache: RowGroupDiskCacheConfig::default(),
        };
        cfg.validate_nvme().expect("creatable dir must validate");
        assert!(dir.is_dir(), "validator must create the missing dir");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_nvme_rejects_non_directory_path() {
        // Create a temp *file* and point the validator at it — the
        // validator must refuse rather than silently accept.
        let mut path = std::env::temp_dir();
        path.push(format!(
            "shelfd-validate-nvme-file-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, b"x").expect("seed file");
        let cfg = RowGroupPoolConfig {
            dram_bytes: 1,
            nvme_dir: path.clone(),
            nvme_bytes: 1,
            eviction_policy: EvictionPolicy::default(),
            disk_cache: RowGroupDiskCacheConfig::default(),
        };
        let err = cfg.validate_nvme().unwrap_err();
        assert!(matches!(&err, crate::Error::Config(m) if m.contains("not a directory")));
        let _ = std::fs::remove_file(&path);
    }
}
