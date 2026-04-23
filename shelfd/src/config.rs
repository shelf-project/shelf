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

    /// Pin list source + reload cadence (SHELF-24).
    pub pin_list: PinListConfig,
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
    #[serde(default = "default_max_inflight")]
    pub max_inflight: usize,
}

fn default_max_inflight() -> usize {
    256
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolsConfig {
    pub metadata: MetadataPoolConfig,
    pub rowgroup: RowGroupPoolConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataPoolConfig {
    /// DRAM quota in bytes. 5 GiB absolute per ADR-0008.
    pub dram_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowGroupPoolConfig {
    /// DRAM portion of the hybrid pool.
    pub dram_bytes: u64,
    /// NVMe directory (the PVC mount point).
    pub nvme_dir: PathBuf,
    /// NVMe capacity in bytes.
    pub nvme_bytes: u64,
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
}

fn default_dns_refresh() -> Duration {
    Duration::from_secs(5)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinListConfig {
    /// S3 bucket + key, e.g. `s3://config-bucket/shelf/pin_list.json`.
    pub source: String,
    /// SIGHUP + periodic reload cadence. 15 min per SHELF-24.
    #[serde(with = "humantime_serde", default = "default_pin_reload")]
    pub reload_interval: Duration,
}

fn default_pin_reload() -> Duration {
    Duration::from_secs(15 * 60)
}

impl Config {
    /// Load and validate a config from disk.
    ///
    /// SHELF-02 wires this up; until then the body panics with a
    /// descriptive ticket message so an accidental runtime invocation
    /// fails loud.
    pub fn from_path(_path: &Path) -> crate::Result<Self> {
        todo!(
            "SHELF-02: config: implement YAML loader + env overrides + validation; \
             see 03-plan.md §4 SHELF-02 and contracts/config-keys.md"
        )
    }
}
