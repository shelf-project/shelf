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

    /// Cap on the HEAD-response LRU (SHELF-07).
    #[serde(default = "default_head_lru_entries")]
    pub head_lru_entries: u64,

    /// Observability toggles (SHELF-08). Defaults to "no OTLP export";
    /// `observability.otlp_endpoint` (or the `SHELFD_OTLP_ENDPOINT`
    /// env override) enables the `tracing-opentelemetry` exporter.
    #[serde(default)]
    pub observability: ObservabilityConfig,
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
  source: "s3://cfg/pin_list.json"
"#;

    #[test]
    fn parses_minimal_config() {
        let cfg = Config::from_yaml_str(MINIMAL, None).expect("minimal config must parse");
        assert_eq!(cfg.node.id, "shelf-0");
        assert_eq!(cfg.origin.bucket, "test-bucket");
        assert_eq!(cfg.pools.metadata.dram_bytes, 1_048_576);
        assert_eq!(cfg.admission.size_threshold_bytes, 1_073_741_824);
        // Defaults applied.
        assert_eq!(cfg.origin.max_inflight, 256);
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
}
