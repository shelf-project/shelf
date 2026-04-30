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
//! event_log_table: your_catalog.your_schema.shelf_advisor_query_log
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

use crate::cost::S3_REWRITE_TARIFF_CENTS_PER_GIB;
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
    /// SHELF-52 bloom-write advisor knobs. Default values are
    /// sourced from the design note and are tuned to the Tier-3
    /// "high-volume tables only" rollout.
    pub bloom_write: BloomWriteConfig,
    /// SHELF-65 — MV-aware pinning recommender knobs.
    pub mv_pinning: MvPinningConfig,
}

/// SHELF-52 bloom-write advisor configuration.
///
/// All fields have safe defaults; operators tune via the YAML
/// config file or by mutating the struct directly when embedding
/// the advisor in-process.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BloomWriteConfig {
    /// Minimum number of distinct queries against a table inside
    /// the lookback window before it is considered as a candidate.
    pub min_query_count: u64,

    /// Minimum *average* per-query `physical_input_bytes` for a
    /// candidate table, in bytes. Defaults to 1 GiB.
    pub min_query_bytes: u64,

    /// Default selectivity estimate when Iceberg NDV is not
    /// available. 0.1 (10 %) per the SHELF-52 design note.
    pub default_selectivity: f64,

    /// Tariff used by [`crate::cost::Cents::from_bytes_rewrite`]
    /// to translate "bytes rewritten" into a dollar figure.
    pub cost_cents_per_gib: u64,

    /// Top-N predicate columns reported per table (default 5).
    pub top_n_columns: usize,

    /// Regex used to extract column names from `WHERE col =
    /// literal` predicates in `QueryRecord::query_text`. Capture
    /// group 1 holds the bare column name.
    pub predicate_column_regex: String,
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
    #[serde(default = "BloomWriteConfig::defaults")]
    bloom_write: BloomWriteConfig,
    /// SHELF-65 — knobs for the MV-aware pinning recommender.
    #[serde(default)]
    mv_pinning: MvPinningConfig,
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
            bloom_write: BloomWriteConfig::defaults(),
            mv_pinning: MvPinningConfig::default(),
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
            bloom_write: f.bloom_write,
            mv_pinning: f.mv_pinning,
        }
    }
}

impl BloomWriteConfig {
    /// SHELF-52 design-note default: a table must have ≥ 50 queries
    /// in the lookback window AND average ≥ 1 GiB scanned per query
    /// to be considered a candidate. Selectivity defaults to 0.1
    /// (90 % skip projection) when no Iceberg NDV is available.
    pub fn defaults() -> Self {
        Self {
            min_query_count: 50,
            min_query_bytes: 1024 * 1024 * 1024, // 1 GiB
            default_selectivity: 0.1,
            cost_cents_per_gib: S3_REWRITE_TARIFF_CENTS_PER_GIB,
            top_n_columns: 5,
            predicate_column_regex: DEFAULT_PREDICATE_COLUMN_REGEX.to_string(),
        }
    }
}

impl Default for BloomWriteConfig {
    fn default() -> Self {
        Self::defaults()
    }
}

/// Default predicate-column extraction regex.
///
/// Captures column identifiers on the LHS of equality predicates
/// (`WHERE col = '…'` or `AND tbl.col = 42`). Capture group 1 is
/// the bare column name (table qualifier stripped). Designed to
/// be greedy on identifiers and conservative on literals — it
/// will miss, not over-match, on any fancy expression. See
/// `docs/design-notes/SHELF-52-bloom-write-advisor.md` for the
/// limitations table.
pub const DEFAULT_PREDICATE_COLUMN_REGEX: &str =
    r"(?i)\b(?:WHERE|AND)\s+(?:[a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s*=";

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

/// SHELF-65 — MV-detection + refresh-detection + cap-protection
/// knobs for the `MaterializedViewPinningRecommender`.
///
/// Each strategy below is opt-in via `detect_strategies`. A table is
/// classified as an MV iff *any* enabled strategy fires (logical OR /
/// union semantics). Refresh detection is the same OR-union over
/// `refresh_user_pattern` (matches the `user` field on a
/// `RefreshEvent` — see `IcebergRefreshLogReader`) and
/// `refresh_sql_pattern` (matches the `query_sql` field).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MvPinningConfig {
    /// MV-detection strategies, OR-unioned. The recommender will WARN
    /// once per run if a strategy is enabled but the corresponding
    /// reader is unavailable (e.g. `IcebergProperty` requested but
    /// no `IcebergTablePropertiesReader` plumbed in) — degrading to
    /// the remaining strategies rather than failing the run.
    pub detect_strategies: Vec<MvDetectStrategy>,

    /// Regex matched against the table portion of a fully-qualified
    /// `catalog.schema.table` name when `MvDetectStrategy::NameRegex`
    /// is enabled. Default `^(mv_|materialized_)` covers the common
    /// `cdp.gold.mv_orders` / `cdp.silver.materialized_dau` shapes.
    pub name_regex: String,

    /// Regex matched against `RefreshEvent::user` when
    /// `IcebergRefreshLogReader` is provided. Default `^airflow_`
    /// catches the standard `airflow_etl_<dag>` user pattern;
    /// deployer-specific overlays may widen this to e.g.
    /// `airflow_*|.*_etl` to cover bespoke ETL accounts.
    pub refresh_user_pattern: String,

    /// Regex matched against `RefreshEvent::query_sql`. Default
    /// `(?i)^\s*REFRESH\s+MATERIALIZED\s+VIEW\s+` is the canonical
    /// Trino DDL — case-insensitive, leading-whitespace-tolerant.
    pub refresh_sql_pattern: String,

    /// Lookback window for the refresh-history scan, in hours.
    /// Independent from `AdvisorConfig::window` because the MV
    /// recommender wants a *short* window (one or two refresh
    /// cycles) to avoid mixing predicate columns across MV redefs,
    /// even when the broader advisor lookback is 7 d. Default 24 h
    /// per the design note.
    pub lookback_hours: u64,

    /// Cap protection — the maximum fraction of `nvme_capacity_bytes`
    /// the aggregate pin-set across MV recommendations may consume.
    /// If exceeded, every recommendation in the run is downgraded
    /// one severity tier and tagged with `pin_bytes_too_large`.
    /// Default 0.5 — leaves half of NVMe for non-MV traffic.
    pub max_pin_bytes_pct: f32,

    /// NVMe capacity per shelfd pod in bytes. Used as the denominator
    /// for the `max_pin_bytes_pct` cap. Default 240 GiB matches the
    /// chart's `storage.size`. The deployer's production overlay
    /// should set this to the live cluster value if it diverges.
    pub nvme_capacity_bytes: u64,

    /// Fallback constant used when the `shelf_dollars_saved` cargo
    /// feature is OFF. Picodollars per byte; the default
    /// `0.0004 / 1024 / 1024 * 1e12 ≈ 381` picodollars/byte
    /// approximates the AWS S3 GET request charge amortised over a
    /// 1 MiB rowgroup at the us-east-1 list rate. The recommender
    /// emits `cost_savings_per_refresh: null` + a
    /// `cost_model_unavailable` flag when the feature is off; this
    /// constant is only surfaced in `rationale` for traceability.
    /// Once SHELF-61's `Cents` newtype lands the feature flips on
    /// and this field becomes documentation-only.
    pub s3_get_cost_picodollars_per_byte: u64,
}

/// MV-detection strategies. Each variant maps 1:1 to a reader the
/// recommender consults; missing readers degrade to a single
/// per-run WARN log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum MvDetectStrategy {
    /// Match `MvPinningConfig::name_regex` against the table portion
    /// of a fully-qualified table name. Always available — needs no
    /// reader.
    NameRegex,
    /// Inspect Trino's per-table Iceberg properties for
    /// `trino.materialized-view.storage-table` /
    /// `trino.materialized-view.fresh-snapshot-id`. Requires an
    /// `IcebergTablePropertiesReader`.
    TrinoProperty,
    /// Inspect the canonical Iceberg `is_materialized_view = true`
    /// table property. Requires an `IcebergTablePropertiesReader`.
    /// The plain Iceberg flag is rare in practice (Trino-Iceberg
    /// integration uses the `trino.*` keys above) but we honour the
    /// upstream spec where it appears.
    IcebergProperty,
}

impl Default for MvPinningConfig {
    fn default() -> Self {
        Self {
            detect_strategies: vec![
                MvDetectStrategy::NameRegex,
                MvDetectStrategy::TrinoProperty,
                MvDetectStrategy::IcebergProperty,
            ],
            name_regex: r"^(mv_|materialized_)".to_string(),
            refresh_user_pattern: r"^airflow_".to_string(),
            refresh_sql_pattern: r"(?i)^\s*REFRESH\s+MATERIALIZED\s+VIEW\s+".to_string(),
            lookback_hours: 24,
            max_pin_bytes_pct: 0.5,
            nvme_capacity_bytes: 240 * 1024 * 1024 * 1024,
            // 0.0004 USD per 1k GETs ÷ 1 MiB per GET ≈ 381 picodollars/byte.
            // Documentation only when `shelf_dollars_saved` is OFF.
            s3_get_cost_picodollars_per_byte: 381,
        }
    }
}
