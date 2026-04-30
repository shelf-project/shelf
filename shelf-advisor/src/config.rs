//! Advisor run configuration.
//!
//! [`AdvisorConfig`] is the de-CLI'd, de-env'd struct that the
//! recommender pipeline consumes. The CLI in `main.rs` is the only
//! place that knows about clap; everything below `lib.rs` takes an
//! `AdvisorConfig` so the pipeline is callable from integration
//! tests + future in-process embeddings without re-parsing argv.
//!
//! ## File format
//!
//! The YAML config at `~/.shelf-advisor/config.yaml` (or whatever
//! path `--config` points to) is the only persistent surface. The
//! schema is intentionally narrow — every field maps 1:1 to a
//! recommender threshold or an input endpoint:
//!
//! ```yaml
//! event_log_table: cdp.icesheet.shelf_advisor_query_log
//! window: 7d
//! top_n_per_table: 8
//! min_confidence: 0.5
//!
//! shelfd_stats_urls:
//!   - http://shelf-0.shelf.svc.cluster.local:8080/stats
//!   - http://shelf-1.shelf.svc.cluster.local:8080/stats
//!
//! optimize:
//!   small_file_bytes: 33554432   # 32 MiB
//!   small_file_ratio_min: 0.30   # only emit when ≥ 30 % small files
//!   min_files_per_table: 8       # below this, the table is too young
//!
//! pin_list:
//!   min_frequency: 5             # at least N queries / table / window
//!   min_confidence: 0.6          # default per-recommender floor
//!   default_pool_capacity_bytes: 11811160064  # 11 GiB rowgroup pool
//! ```
//!
//! See [`AdvisorConfig::DEFAULT_*`] constants below for everything
//! the OSS-distributed `config.example.yaml` ships. Site-specific
//! overlays live under the operator's own `infra/<cluster>/`
//! directory and are stripped from the OSS publish surface by the
//! release pipeline; they typically only need to override the
//! table name + the pod URLs.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Fully-resolved configuration for one advisor run. Consumed by
/// every recommender via [`AnalysisContext`].
///
/// [`AnalysisContext`]: crate::recommenders::AnalysisContext
#[derive(Debug, Clone)]
pub struct AdvisorConfig {
    /// Fully-qualified `catalog.schema.table` of the listener log
    /// table. Configurable; the OSS-distributed default is a
    /// generic, deployment-agnostic placeholder. Deployment-specific
    /// table names only appear in the operator's overlay file
    /// (the release pipeline strips overlay directories from the
    /// OSS publish surface).
    pub event_log_table: String,

    /// Where the advisor writes the bare-array compatibility
    /// output (`analyze` mode). `recommend --output-dir` overrides
    /// this on a per-invocation basis.
    pub output_path: PathBuf,

    /// Lookback window for the event-log scan.
    pub window: Duration,

    /// Hard cap on recommendations returned per
    /// `(table, recommendation_type)` pair. Mirrors the
    /// false-positive-flood mitigation in the canonical SHELF-53
    /// design note.
    pub top_n_per_table: usize,

    /// Global confidence floor below which a recommendation is
    /// dropped regardless of per-recommender opinion. Default 0.5.
    pub min_confidence: f32,

    /// Pre-fetched list of shelfd `/stats` URLs. The advisor never
    /// resolves DNS at recommender time — that resolution happens
    /// in `main.rs` so test runs and dry-runs can swap in fixture
    /// readers without depending on a live cluster.
    pub shelfd_stats_urls: Vec<String>,

    /// Per-recommender knobs. Each block is independently
    /// versioned so SHELF-65 / SHELF-52 can grow their own without
    /// disturbing this struct's shape.
    pub optimize: OptimizeConfig,
    pub pin_list: PinListConfig,
    pub bloom: BloomConfig,
    pub mv: MvConfig,
}

/// Knobs for [`OptimizeRecommender`].
///
/// [`OptimizeRecommender`]: crate::recommenders::OptimizeRecommender
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OptimizeConfig {
    /// Files with `file_size_bytes < small_file_bytes` count
    /// against the small-file ratio. Iceberg's own `OPTIMIZE`
    /// `target-file-size-bytes` defaults to 512 MiB; we mark
    /// "small" at 32 MiB, matching the RisingWave small-file
    /// threshold cited in the canonical SHELF-53 design note.
    pub small_file_bytes: u64,
    /// Minimum small-file ratio (0.0..=1.0) before the
    /// recommender will suggest an `OPTIMIZE` target.
    pub small_file_ratio_min: f32,
    /// Tables with fewer than this many data files are too young
    /// to consider — `OPTIMIZE` on a 3-file table is noise.
    pub min_files_per_table: u64,
}

impl Default for OptimizeConfig {
    fn default() -> Self {
        Self {
            small_file_bytes: 32 * 1024 * 1024,
            small_file_ratio_min: 0.30,
            min_files_per_table: 8,
        }
    }
}

/// Knobs for [`PinListRecommender`].
///
/// [`PinListRecommender`]: crate::recommenders::PinListRecommender
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PinListConfig {
    /// Tables seen in fewer than this many queries within the
    /// window are dropped before scoring (cold workloads do not
    /// benefit from pinning).
    pub min_frequency: u64,
    /// Per-recommender confidence floor. Layered above the global
    /// `min_confidence`; whichever is higher wins.
    pub min_confidence: f32,
    /// Fallback pool capacity used when no shelfd `/stats` sample
    /// is available. The score formula's denominator
    /// `1 + total_bytes / pool_capacity` would otherwise divide
    /// by zero. 11 GiB matches the rc.2 rowgroup pool default in
    /// `charts/shelf/values.yaml`; revisit when the hot-path
    /// compression land changes the working set.
    pub default_pool_capacity_bytes: u64,
}

impl Default for PinListConfig {
    fn default() -> Self {
        Self {
            min_frequency: 5,
            min_confidence: 0.6,
            default_pool_capacity_bytes: 11 * 1024 * 1024 * 1024,
        }
    }
}

/// Knobs reserved for SHELF-52's `BloomFilterRecommender`. Today
/// the recommender returns empty regardless of these; we expose
/// the block so the OSS config example and per-cluster overlays
/// can carry the future values in the same file.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BloomConfig {
    pub enabled: bool,
    pub min_confidence: f32,
}

impl Default for BloomConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_confidence: 0.7,
        }
    }
}

/// Knobs reserved for SHELF-65's `MaterializedViewRecommender`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MvConfig {
    pub enabled: bool,
    pub min_confidence: f32,
    /// Cap on pinned-bytes per recommendation, expressed as a
    /// fraction of the pool capacity. Carries the SHELF-65
    /// `nvme_quota * pin_fraction` invariant.
    pub pin_fraction: f32,
}

impl Default for MvConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_confidence: 0.5,
            pin_fraction: 0.30,
        }
    }
}

/// On-disk YAML representation. Only the fields below are read
/// from the config file; the CLI fills in `output_path` and
/// `window` from flags so a single config can drive many runs
/// against different windows.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct ConfigFile {
    event_log_table: Option<String>,
    #[serde(with = "humantime_serde::option")]
    window: Option<Duration>,
    top_n_per_table: Option<usize>,
    min_confidence: Option<f32>,
    shelfd_stats_urls: Vec<String>,
    optimize: OptimizeConfig,
    pin_list: PinListConfig,
    bloom: BloomConfig,
    mv: MvConfig,
}

impl AdvisorConfig {
    /// Phase-1 placeholder default. Deployment-specific table
    /// names belong in the operator's overlay file (stripped from
    /// the OSS publish surface by the release pipeline); this
    /// default is OSS-clean.
    pub const DEFAULT_EVENT_LOG_TABLE: &'static str = "shelf_advisor.events.query_log";

    /// Default per-table cap.
    pub const DEFAULT_TOP_N_PER_TABLE: usize = 8;

    /// Default global confidence floor.
    pub const DEFAULT_MIN_CONFIDENCE: f32 = 0.5;

    /// Build a default-everything config rooted at `output_path`
    /// over `window`. Used by the `analyze` compat command and by
    /// every test that doesn't care about overrides.
    pub fn defaults(output_path: PathBuf, window: Duration) -> Self {
        Self {
            event_log_table: Self::DEFAULT_EVENT_LOG_TABLE.to_string(),
            output_path,
            window,
            top_n_per_table: Self::DEFAULT_TOP_N_PER_TABLE,
            min_confidence: Self::DEFAULT_MIN_CONFIDENCE,
            shelfd_stats_urls: Vec::new(),
            optimize: OptimizeConfig::default(),
            pin_list: PinListConfig::default(),
            bloom: BloomConfig::default(),
            mv: MvConfig::default(),
        }
    }

    /// Load `~/.shelf-advisor/config.yaml` (or the path passed via
    /// `--config`) and overlay it onto the defaults. Missing keys
    /// keep their default. The CLI then layers its own flags
    /// (`--window`, `--output`) on top of the result.
    pub fn from_yaml_file(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        let parsed: ConfigFile = serde_yaml::from_slice(&bytes)?;
        Ok(Self::from_file(parsed))
    }

    /// Build from an in-memory parsed YAML doc. Useful for tests
    /// that synthesise config without round-tripping through
    /// the filesystem.
    fn from_file(f: ConfigFile) -> Self {
        Self {
            event_log_table: f
                .event_log_table
                .unwrap_or_else(|| Self::DEFAULT_EVENT_LOG_TABLE.to_string()),
            output_path: PathBuf::new(),
            window: f.window.unwrap_or_else(|| Duration::from_secs(7 * 86_400)),
            top_n_per_table: f.top_n_per_table.unwrap_or(Self::DEFAULT_TOP_N_PER_TABLE),
            min_confidence: f.min_confidence.unwrap_or(Self::DEFAULT_MIN_CONFIDENCE),
            shelfd_stats_urls: f.shelfd_stats_urls,
            optimize: f.optimize,
            pin_list: f.pin_list,
            bloom: f.bloom,
            mv: f.mv,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_yaml_yields_full_defaults() {
        let parsed: ConfigFile = serde_yaml::from_str("").expect("empty yaml");
        let cfg = AdvisorConfig::from_file(parsed);
        assert_eq!(cfg.event_log_table, AdvisorConfig::DEFAULT_EVENT_LOG_TABLE);
        assert_eq!(cfg.top_n_per_table, AdvisorConfig::DEFAULT_TOP_N_PER_TABLE);
        assert_eq!(cfg.min_confidence, AdvisorConfig::DEFAULT_MIN_CONFIDENCE);
        assert_eq!(cfg.optimize.small_file_bytes, 32 * 1024 * 1024);
        assert_eq!(cfg.pin_list.min_frequency, 5);
        assert!(!cfg.bloom.enabled);
        assert!(!cfg.mv.enabled);
    }

    #[test]
    fn yaml_overrides_defaults() {
        let yaml = r#"
event_log_table: example.events.query_log
window: 24h
top_n_per_table: 3
min_confidence: 0.7
shelfd_stats_urls:
  - http://shelf-0:8080/stats
optimize:
  small_file_bytes: 16777216
  small_file_ratio_min: 0.5
  min_files_per_table: 4
pin_list:
  min_frequency: 10
  min_confidence: 0.8
  default_pool_capacity_bytes: 8589934592
"#;
        let parsed: ConfigFile = serde_yaml::from_str(yaml).unwrap();
        let cfg = AdvisorConfig::from_file(parsed);
        assert_eq!(cfg.event_log_table, "example.events.query_log");
        assert_eq!(cfg.window, Duration::from_secs(86_400));
        assert_eq!(cfg.top_n_per_table, 3);
        assert_eq!(cfg.min_confidence, 0.7);
        assert_eq!(cfg.shelfd_stats_urls.len(), 1);
        assert_eq!(cfg.optimize.small_file_bytes, 16 * 1024 * 1024);
        assert_eq!(cfg.pin_list.min_frequency, 10);
    }
}
