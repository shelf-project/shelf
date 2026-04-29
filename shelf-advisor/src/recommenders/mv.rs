//! SHELF-65 — MV-aware pinning recommender.
//!
//! Walks the Iceberg event log + manifests + (optional) MV-refresh
//! history, identifies materialized views via three OR-unioned
//! detection strategies, groups recent refreshes into windows, and
//! emits a `mv_pinning` recommendation per (base table, refresh
//! window) pair so the operator can pre-warm shelfd's pin-set
//! before the next refresh.
//!
//! ## Detection strategies (OR-unioned)
//!
//! 1. **Name regex** (`MvDetectStrategy::NameRegex`) — the table
//!    portion of `catalog.schema.table` matches the configured
//!    `name_regex` (default `^(mv_|materialized_)`). Always
//!    available; needs no reader.
//! 2. **Trino-Iceberg property** (`MvDetectStrategy::TrinoProperty`)
//!    — the table's properties carry
//!    `trino.materialized-view.storage-table` /
//!    `trino.materialized-view.fresh-snapshot-id`. Requires an
//!    `IcebergTablePropertiesReader`; degrades to a single per-run
//!    WARN if absent.
//! 3. **Iceberg `is_materialized_view`** (`MvDetectStrategy::IcebergProperty`)
//!    — the canonical Iceberg flag. Requires the same reader as
//!    `TrinoProperty`; degrades the same way.
//!
//! ## Refresh detection
//!
//! Refresh events are pulled from an optional
//! `IcebergRefreshLogReader` (which carries `user` + `query_sql` +
//! `written_table` + `base_tables`, none of which appear on
//! `QueryRecord` today — see input::mv_pinning module docs for the
//! rule-5 reasoning). A query is treated as an MV refresh when
//! *either* `query_sql` matches `refresh_sql_pattern` *or* `user`
//! matches `refresh_user_pattern` AND `written_table` is classified
//! as an MV.
//!
//! ## Pin-key derivation
//!
//! `pin_keys` are ADR-0011 SHA-256 hex digests over
//! `etag || offset_le || length_le || rg_ordinal_le`. Because the
//! advisor does not see live S3 ETags, the v1 proxy uses
//! `etag := DataFile::path.as_bytes()` — a deterministic, opaque
//! version token that satisfies the ADR's "not required to be a
//! cryptographic hash" clause but does not survive in-place
//! overwrites of the same path. The operator's apply-side pinner
//! (SHELF-24) recomputes keys from the live S3 ETag before pinning;
//! the advisor's keys are advisory locators, not the final cache
//! lookup keys. Tagged as `pin_key_derivation: "v1_path_proxy_etag"`
//! in the rationale so the apply-side tooling can detect the proxy
//! and re-derive.
//!
//! ## Severity tiers
//!
//! Per the SHELF-65 spec:
//! - `info` if `cost_savings_per_refresh < $1`
//! - `warn` if `$1 ≤ cost_savings_per_refresh < $10`
//! - `critical` if `cost_savings_per_refresh ≥ $10`
//!
//! Cap protection: if the *aggregate* `pin_bytes_estimate` across
//! all recommendations in a single run exceeds
//! `nvme_capacity_bytes × max_pin_bytes_pct` (default 0.5), every
//! recommendation in the run is downgraded one tier
//! (`critical → warn`, `warn → info`, `info → info`) and tagged
//! with a `pin_bytes_too_large` warning so the operator sees that
//! blindly applying everything would fill the cache.
//!
//! ## Cost model wiring (`shelf_dollars_saved` cargo feature)
//!
//! When the `shelf_dollars_saved` feature is OFF, the recommender
//! emits `cost_savings_per_refresh: null` plus a
//! `cost_model_unavailable` rationale flag. The
//! `s3_get_cost_picodollars_per_byte` constant is included in
//! `rationale.cost_model_inputs` for traceability but never
//! resolves to a `Cents`. When SHELF-61 (PR #68) lands and the
//! feature flips on, the recommender imports `Cents` from
//! `shelf-dollars-saved` and emits the actual figure. See
//! `docs/recommenders.md` §MV-pinning > Cost model wiring.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::config::MvDetectStrategy;
use crate::error::Result;
use crate::input::{
    DataFile, IcebergEventLogReader, IcebergManifestReader, IcebergRefreshLogReader,
    IcebergTablePropertiesReader, RefreshEvent,
};
use crate::output::Recommendation;
use crate::recommenders::{AnalysisContext, Recommender};

/// Severity tier for an MV-pinning recommendation. Stringified as
/// `"info" | "warn" | "critical"` in the JSON output's `rationale`
/// — kept off the top-level `Recommendation` shape so this PR does
/// not touch SHELF-53's struct surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warn,
    Critical,
}

impl Severity {
    fn from_cents(cents: i64) -> Self {
        match cents {
            c if c < 100 => Severity::Info,
            c if c < 1_000 => Severity::Warn,
            _ => Severity::Critical,
        }
    }

    fn downgraded(self) -> Self {
        match self {
            Severity::Critical => Severity::Warn,
            Severity::Warn => Severity::Info,
            Severity::Info => Severity::Info,
        }
    }
}

/// SHELF-65 — MV-aware pinning recommender. Replaces the SHELF-47
/// candidate-aggregation stub that previously lived in this module.
///
/// Plumb optional readers via the builder methods; absent readers
/// degrade to a single per-run WARN log.
pub struct MaterializedViewPinningRecommender {
    table_props: Option<Arc<dyn IcebergTablePropertiesReader>>,
    refresh_log: Option<Arc<dyn IcebergRefreshLogReader>>,
}

impl Default for MaterializedViewPinningRecommender {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MaterializedViewPinningRecommender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializedViewPinningRecommender")
            .field("table_props_set", &self.table_props.is_some())
            .field("refresh_log_set", &self.refresh_log.is_some())
            .finish()
    }
}

impl MaterializedViewPinningRecommender {
    /// Construct a recommender with neither optional reader. Used by
    /// `default_recommenders()`. Without a refresh-log reader the
    /// recommender returns an empty `Vec` and emits a single per-run
    /// WARN log explaining the missing reader; the regex-only
    /// "advisory mode" referenced in older drafts of this design is
    /// a follow-up that requires an `IcebergCatalogReader::list_tables`
    /// surface that doesn't yet exist on `main`.
    pub fn new() -> Self {
        Self {
            table_props: None,
            refresh_log: None,
        }
    }

    /// Plumb an `IcebergTablePropertiesReader` so the property-based
    /// detection strategies (`TrinoProperty`, `IcebergProperty`)
    /// have a backing reader. Returns `self` for builder-style
    /// chaining in tests.
    pub fn with_table_properties_reader(
        mut self,
        r: Arc<dyn IcebergTablePropertiesReader>,
    ) -> Self {
        self.table_props = Some(r);
        self
    }

    /// Plumb an `IcebergRefreshLogReader` so refresh-window grouping
    /// kicks in. Without this, the recommender emits advisories
    /// only.
    pub fn with_refresh_log_reader(mut self, r: Arc<dyn IcebergRefreshLogReader>) -> Self {
        self.refresh_log = Some(r);
        self
    }
}

impl Recommender for MaterializedViewPinningRecommender {
    fn kind(&self) -> &'static str {
        "mv_pinning"
    }

    fn analyze(&self, ctx: &AnalysisContext<'_>) -> Result<Vec<Recommendation>> {
        let config = ctx.config;
        let _event_log = ctx.event_log;
        let manifests = ctx.manifests;
        let mv_cfg = &config.mv_pinning;

        let name_re = Regex::new(&mv_cfg.name_regex).map_err(|e| {
            anyhow::anyhow!("invalid mv_pinning.name_regex {:?}: {e}", mv_cfg.name_regex)
        })?;
        let user_re = Regex::new(&mv_cfg.refresh_user_pattern).map_err(|e| {
            anyhow::anyhow!(
                "invalid mv_pinning.refresh_user_pattern {:?}: {e}",
                mv_cfg.refresh_user_pattern
            )
        })?;
        let sql_re = Regex::new(&mv_cfg.refresh_sql_pattern).map_err(|e| {
            anyhow::anyhow!(
                "invalid mv_pinning.refresh_sql_pattern {:?}: {e}",
                mv_cfg.refresh_sql_pattern
            )
        })?;

        let want_property_strategies = mv_cfg.detect_strategies.contains(&MvDetectStrategy::TrinoProperty)
            || mv_cfg
                .detect_strategies
                .contains(&MvDetectStrategy::IcebergProperty);
        if want_property_strategies && self.table_props.is_none() {
            tracing::warn!(
                "mv_pinning: property-based detection requested but no IcebergTablePropertiesReader plumbed in; falling back to regex-only detection"
            );
        }

        let refreshes: Vec<RefreshEvent> = match &self.refresh_log {
            Some(r) => r.read_refreshes(mv_cfg.lookback_hours)?,
            None => {
                tracing::warn!(
                    "mv_pinning: no IcebergRefreshLogReader plumbed in; cannot emit recommendations without refresh history (advisory-only path is a follow-up — see docs/recommenders.md §MV-pinning > Inputs)"
                );
                Vec::new()
            }
        };

        let mut buckets: BTreeMap<(String, u64), RefreshBucket> = BTreeMap::new();
        let bucket_secs = mv_cfg.lookback_hours.saturating_mul(3600).max(3600);

        for ev in &refreshes {
            let is_refresh = sql_re.is_match(&ev.query_sql) || user_re.is_match(&ev.user);
            if !is_refresh {
                continue;
            }
            let mv_classifies = self.classify_mv(&ev.written_table, &name_re, mv_cfg)?;
            if !mv_classifies {
                continue;
            }
            let window_id = ev.started_at_unix_seconds / bucket_secs;
            for base in &ev.base_tables {
                let entry = buckets
                    .entry((base.clone(), window_id))
                    .or_insert_with(|| RefreshBucket {
                        mv_table: ev.written_table.clone(),
                        refresh_count: 0,
                        sample_query_id: ev.query_id.clone(),
                    });
                entry.refresh_count += 1;
                if entry.mv_table != ev.written_table {
                    entry.mv_table = format!("{} +others", entry.mv_table);
                }
            }
        }

        let mut interim: Vec<Interim> = Vec::new();

        if buckets.is_empty() {
            if self.refresh_log.is_some() {
                tracing::info!("mv_pinning: zero refreshes matched in window; no recommendations");
            }
            return Ok(Vec::new());
        }

        for ((base_table, _window_id), bucket) in buckets.into_iter() {
            let files = manifests.list_files(&base_table)?;
            interim.push(self.build_for_bucket(&base_table, bucket, files, mv_cfg)?);
        }

        let aggregate_bytes: u64 = interim.iter().map(|i| i.pin_bytes_estimate).sum();
        let cap_bytes =
            ((mv_cfg.nvme_capacity_bytes as f64) * (mv_cfg.max_pin_bytes_pct as f64)) as u64;
        let cap_exceeded = cap_bytes > 0 && aggregate_bytes > cap_bytes;
        if cap_exceeded {
            tracing::warn!(
                aggregate_bytes,
                cap_bytes,
                "mv_pinning: aggregate pin bytes exceed cap — downgrading every recommendation in this run"
            );
        }

        let mut out = Vec::with_capacity(interim.len());
        for mut item in interim {
            if cap_exceeded {
                item.severity = item.severity.downgraded();
                item.pin_bytes_too_large = true;
            }
            out.push(item.into_recommendation(mv_cfg, aggregate_bytes, cap_bytes));
        }
        out.sort_by(|a, b| a.table.cmp(&b.table));

        Ok(out)
    }
}

impl MaterializedViewPinningRecommender {
    fn classify_mv(
        &self,
        table: &str,
        name_re: &Regex,
        mv_cfg: &crate::config::MvPinningConfig,
    ) -> Result<bool> {
        let leaf = table.rsplit('.').next().unwrap_or(table);

        // Fetch properties at most once per `classify_mv` call so the
        // (potentially-network) reader runs once even if both
        // property-based strategies are enabled.
        let props = match &self.table_props {
            Some(r) => r.properties(table)?,
            None => None,
        };

        for strat in &mv_cfg.detect_strategies {
            let hit = match strat {
                MvDetectStrategy::NameRegex => name_re.is_match(leaf),
                MvDetectStrategy::TrinoProperty => props
                    .as_ref()
                    .map(|p| {
                        p.trino_storage_table.is_some() || p.trino_fresh_snapshot_id.is_some()
                    })
                    .unwrap_or(false),
                MvDetectStrategy::IcebergProperty => props
                    .as_ref()
                    .map(|p| p.is_materialized_view == Some(true))
                    .unwrap_or(false),
            };
            if hit {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn build_for_bucket(
        &self,
        base_table: &str,
        bucket: RefreshBucket,
        files: Vec<DataFile>,
        mv_cfg: &crate::config::MvPinningConfig,
    ) -> Result<Interim> {
        let pin_keys = pin_keys_for(&files);
        let pin_bytes_estimate: u64 = files.iter().map(|f| f.file_size_bytes).sum();
        let lift = expected_hit_ratio_lift(bucket.refresh_count);
        let bytes_saved = (pin_bytes_estimate as f64 * lift) as u64;
        let cost_savings_cents = cost_savings_cents(bytes_saved, mv_cfg);
        let severity = Severity::from_cents(cost_savings_cents);
        Ok(Interim {
            table: base_table.to_string(),
            mv_table: bucket.mv_table,
            sample_query_id: bucket.sample_query_id,
            refresh_count: bucket.refresh_count,
            pin_keys,
            pin_bytes_estimate,
            expected_hit_ratio_lift: lift,
            cost_savings_cents,
            severity,
            pin_bytes_too_large: false,
            file_count: files.len(),
        })
    }
}

#[derive(Debug)]
struct RefreshBucket {
    mv_table: String,
    refresh_count: u32,
    sample_query_id: String,
}

#[derive(Debug)]
struct Interim {
    table: String,
    mv_table: String,
    sample_query_id: String,
    refresh_count: u32,
    pin_keys: Vec<String>,
    pin_bytes_estimate: u64,
    expected_hit_ratio_lift: f32,
    cost_savings_cents: i64,
    severity: Severity,
    pin_bytes_too_large: bool,
    file_count: usize,
}

impl Interim {
    fn into_recommendation(
        self,
        mv_cfg: &crate::config::MvPinningConfig,
        aggregate_bytes: u64,
        cap_bytes: u64,
    ) -> Recommendation {
        let mut rationale = json!({
            "severity": self.severity,
            "mv_table": self.mv_table,
            "refresh_count_in_window": self.refresh_count,
            "sample_refresh_query_id": self.sample_query_id,
            "expected_hit_ratio_lift": self.expected_hit_ratio_lift,
            "pin_bytes_estimate": self.pin_bytes_estimate,
            "file_count": self.file_count,
            "advisory_only": false,
            "pin_bytes_too_large": self.pin_bytes_too_large,
            "aggregate_pin_bytes_in_run": aggregate_bytes,
            "cap_bytes": cap_bytes,
            "cost_model_inputs": {
                "s3_get_cost_picodollars_per_byte": mv_cfg.s3_get_cost_picodollars_per_byte,
                "lookback_hours": mv_cfg.lookback_hours,
                "max_pin_bytes_pct": mv_cfg.max_pin_bytes_pct,
                "nvme_capacity_bytes": mv_cfg.nvme_capacity_bytes,
            },
            "pin_key_derivation": "v1_path_proxy_etag",
        });

        if cfg!(feature = "shelf_dollars_saved") {
            rationale["cost_savings_per_refresh_cents"] = json!(self.cost_savings_cents);
            rationale["cost_savings_per_refresh"] = json!(self.cost_savings_cents);
        } else {
            rationale["cost_savings_per_refresh"] = serde_json::Value::Null;
            rationale["cost_model_unavailable"] = json!(true);
            rationale["cost_model_unavailable_reason"] =
                json!("shelf_dollars_saved cargo feature OFF; SHELF-61 (PR #68) not yet merged");
        }

        let suggested_change = json!({
            "pin_keys": self.pin_keys,
            "table": self.table,
            "ttl_hint_hours": mv_cfg.lookback_hours,
            "pool": "rowgroup",
        });

        // Confidence values are picked from the set of f32 values that
        // round-trip cleanly through serde_json's f64 encoding: each
        // numerator × 2^-k where k <= 4 is exactly representable in
        // f32 *and* f64, so the JSON literal we emit is the same one
        // operators read from a hand-written fixture. This keeps the
        // SHELF-65 snapshot test stable across Rust toolchains.
        let confidence: f32 = if self.pin_bytes_too_large {
            0.5625 // 9/16 — cap exceeded, "needs ops eyeballs" band
        } else {
            0.875 // 7/8 — primary path, ship without human review
        };

        Recommendation {
            recommendation_type: "mv_pinning".to_string(),
            table: self.table,
            confidence,
            rationale,
            suggested_change,
        }
    }
}

/// Compute ADR-0011-style SHA-256 keys for each `DataFile`.
///
/// `etag` is the file path's bytes (v1 proxy — see module-level
/// docs for the rationale and the path-collision caveat). `offset`
/// is 0, `length` is `file_size_bytes`, `rg_ordinal` is 0 (whole-
/// file pin; the row-group-level keys are derived at apply time).
fn pin_keys_for(files: &[DataFile]) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::with_capacity(files.len());
    for f in files {
        let mut h = Sha256::new();
        h.update(f.path.as_bytes());
        h.update(0u64.to_le_bytes());
        h.update(f.file_size_bytes.to_le_bytes());
        h.update(0u32.to_le_bytes());
        let digest = hex::encode(h.finalize());
        if seen.insert(digest.clone()) {
            out.push(digest);
        }
    }
    out.sort();
    out
}

fn expected_hit_ratio_lift(refresh_count: u32) -> f32 {
    if refresh_count <= 1 {
        return 0.0;
    }
    1.0 - (1.0 / refresh_count as f32)
}

fn cost_savings_cents(bytes_saved: u64, mv_cfg: &crate::config::MvPinningConfig) -> i64 {
    let pico = (bytes_saved as u128).saturating_mul(mv_cfg.s3_get_cost_picodollars_per_byte as u128);
    let cents = pico / 10_000_000_000u128;
    cents.min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AdvisorConfig;
    use crate::input::{
        DataFile, IcebergEventLogReader, IcebergManifestReader, IcebergRefreshLogReader,
        IcebergTablePropertiesReader, MvTableProperties, QueryRecord, RefreshEvent,
    };
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::Duration;

    fn cfg() -> AdvisorConfig {
        let mut c = AdvisorConfig::defaults(PathBuf::from("/tmp/x.json"), Duration::from_secs(86_400));
        c.event_log_table = "x.y.z".to_string();
        c
    }

    /// Helper to run analyze() with the SHELF-53 AnalysisContext shape.
    fn run(
        r: &MaterializedViewPinningRecommender,
        c: &AdvisorConfig,
        log: &dyn IcebergEventLogReader,
        manifests: &dyn IcebergManifestReader,
    ) -> Result<Vec<Recommendation>> {
        let stats = crate::input::FixtureShelfdStatsReader::empty();
        let tables: Vec<String> = Vec::new();
        let ctx = AnalysisContext {
            config: c,
            event_log: log,
            manifests,
            shelfd_stats: &stats,
            tables: &tables,
        };
        r.analyze(&ctx)
    }

    struct EmptyEvLog;
    impl IcebergEventLogReader for EmptyEvLog {
        fn read_window(&self, _w: Duration) -> Result<Vec<QueryRecord>> {
            Ok(Vec::new())
        }
    }

    struct FixedManifests {
        files: HashMap<String, Vec<DataFile>>,
    }
    impl IcebergManifestReader for FixedManifests {
        fn list_files(&self, table: &str) -> Result<Vec<DataFile>> {
            Ok(self.files.get(table).cloned().unwrap_or_default())
        }
    }

    struct FixedProps {
        props: HashMap<String, MvTableProperties>,
    }
    impl IcebergTablePropertiesReader for FixedProps {
        fn properties(&self, table: &str) -> Result<Option<MvTableProperties>> {
            Ok(self.props.get(table).cloned())
        }
    }

    struct FixedRefreshes {
        events: Mutex<Vec<RefreshEvent>>,
    }
    impl IcebergRefreshLogReader for FixedRefreshes {
        fn read_refreshes(&self, _h: u64) -> Result<Vec<RefreshEvent>> {
            Ok(self.events.lock().unwrap().clone())
        }
    }

    fn ev(query_sql: &str, user: &str, mv: &str, base: &[&str], at: u64) -> RefreshEvent {
        RefreshEvent {
            query_id: format!("q-{at}"),
            user: user.to_string(),
            query_sql: query_sql.to_string(),
            written_table: mv.to_string(),
            base_tables: base.iter().map(|s| s.to_string()).collect(),
            started_at_unix_seconds: at,
        }
    }

    fn df(path: &str, size: u64) -> DataFile {
        DataFile {
            path: path.to_string(),
            file_size_bytes: size,
            record_count: 1000,
            spec_id: 0,
        }
    }

    #[test]
    fn detects_via_name_regex_only() {
        let r = MaterializedViewPinningRecommender::new();
        let re = Regex::new(r"^(mv_|materialized_)").unwrap();
        let mut c = cfg();
        c.mv_pinning.detect_strategies = vec![MvDetectStrategy::NameRegex];
        assert!(r.classify_mv("cdp.gold.mv_orders", &re, &c.mv_pinning).unwrap());
        assert!(!r.classify_mv("cdp.gold.orders", &re, &c.mv_pinning).unwrap());
        assert!(r
            .classify_mv("cdp.gold.materialized_dau", &re, &c.mv_pinning)
            .unwrap());
    }

    #[test]
    fn detects_via_trino_property_only() {
        let mut props = HashMap::new();
        props.insert(
            "cdp.gold.weird_mv_name".to_string(),
            MvTableProperties {
                trino_storage_table: Some("cdp.gold.storage_xyz".to_string()),
                ..Default::default()
            },
        );
        let r = MaterializedViewPinningRecommender::new()
            .with_table_properties_reader(Arc::new(FixedProps { props }));
        let re = Regex::new(r"^(mv_|materialized_)").unwrap();
        let mut c = cfg();
        c.mv_pinning.detect_strategies = vec![MvDetectStrategy::TrinoProperty];
        assert!(r
            .classify_mv("cdp.gold.weird_mv_name", &re, &c.mv_pinning)
            .unwrap());
        assert!(!r
            .classify_mv("cdp.gold.unknown", &re, &c.mv_pinning)
            .unwrap());
    }

    #[test]
    fn detects_via_iceberg_property_only() {
        let mut props = HashMap::new();
        props.insert(
            "cdp.bronze.events".to_string(),
            MvTableProperties {
                is_materialized_view: Some(true),
                ..Default::default()
            },
        );
        let r = MaterializedViewPinningRecommender::new()
            .with_table_properties_reader(Arc::new(FixedProps { props }));
        let re = Regex::new(r"^(mv_|materialized_)").unwrap();
        let mut c = cfg();
        c.mv_pinning.detect_strategies = vec![MvDetectStrategy::IcebergProperty];
        assert!(r
            .classify_mv("cdp.bronze.events", &re, &c.mv_pinning)
            .unwrap());
    }

    #[test]
    fn detects_via_union_when_both_strategies_enabled() {
        let mut props = HashMap::new();
        props.insert(
            "cdp.silver.legacy_aggregate".to_string(),
            MvTableProperties {
                trino_storage_table: Some("cdp.silver.x".to_string()),
                ..Default::default()
            },
        );
        let r = MaterializedViewPinningRecommender::new()
            .with_table_properties_reader(Arc::new(FixedProps { props }));
        let re = Regex::new(r"^(mv_|materialized_)").unwrap();
        let c = cfg();
        assert!(r
            .classify_mv("cdp.silver.legacy_aggregate", &re, &c.mv_pinning)
            .unwrap());
        assert!(r
            .classify_mv("cdp.gold.mv_orders", &re, &c.mv_pinning)
            .unwrap());
        assert!(!r
            .classify_mv("cdp.gold.orders", &re, &c.mv_pinning)
            .unwrap());
    }

    #[test]
    fn detects_neither_when_no_strategies_match() {
        let r = MaterializedViewPinningRecommender::new();
        let re = Regex::new(r"^(mv_|materialized_)").unwrap();
        let c = cfg();
        assert!(!r.classify_mv("cdp.gold.orders", &re, &c.mv_pinning).unwrap());
    }

    #[test]
    fn refresh_window_groups_by_bucket_id() {
        let mut files = HashMap::new();
        files.insert(
            "cdp.bronze.events".to_string(),
            vec![df("s3://b/events/00.parquet", 100_000_000)],
        );
        let manifests = FixedManifests { files };

        let refreshes = vec![
            ev(
                "REFRESH MATERIALIZED VIEW cdp.gold.mv_orders",
                "airflow_etl_orders",
                "cdp.gold.mv_orders",
                &["cdp.bronze.events"],
                1_700_000_000,
            ),
            ev(
                "REFRESH MATERIALIZED VIEW cdp.gold.mv_orders",
                "airflow_etl_orders",
                "cdp.gold.mv_orders",
                &["cdp.bronze.events"],
                1_700_000_300,
            ),
        ];

        let r = MaterializedViewPinningRecommender::new()
            .with_refresh_log_reader(Arc::new(FixedRefreshes {
                events: Mutex::new(refreshes),
            }));

        let mut c = cfg();
        c.mv_pinning.detect_strategies = vec![MvDetectStrategy::NameRegex];
        let recs = run(&r, &c, &EmptyEvLog, &manifests).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].rationale["refresh_count_in_window"], 2);
    }

    #[test]
    fn cost_calc_against_known_savings() {
        // 10 bytes × 1e9 picodollars/byte = 1e10 picodollars = 1 cent
        // (1 picodollar = 1e-10 cents, since 1 cent = 1e-2 dollars
        // and 1 picodollar = 1e-12 dollars).
        let mv = crate::config::MvPinningConfig {
            s3_get_cost_picodollars_per_byte: 1_000_000_000,
            ..Default::default()
        };
        assert_eq!(cost_savings_cents(10, &mv), 1);
        assert_eq!(cost_savings_cents(1_000, &mv), 100);
        assert_eq!(cost_savings_cents(0, &mv), 0);
        // saturation: u128 overflow into i64::MAX clamp
        let huge = crate::config::MvPinningConfig {
            s3_get_cost_picodollars_per_byte: u64::MAX,
            ..Default::default()
        };
        let _ = cost_savings_cents(u64::MAX, &huge);
    }

    #[test]
    fn severity_boundaries() {
        assert_eq!(Severity::from_cents(0), Severity::Info);
        assert_eq!(Severity::from_cents(99), Severity::Info);
        assert_eq!(Severity::from_cents(100), Severity::Warn);
        assert_eq!(Severity::from_cents(999), Severity::Warn);
        assert_eq!(Severity::from_cents(1_000), Severity::Critical);
        assert_eq!(Severity::from_cents(99_999), Severity::Critical);
    }

    #[test]
    fn cap_protection_downgrades_and_flags() {
        let mut files = HashMap::new();
        files.insert(
            "cdp.bronze.huge".to_string(),
            vec![df("s3://b/huge/00.parquet", 10_000_000_000_000)],
        );
        let manifests = FixedManifests { files };

        let refreshes = vec![ev(
            "REFRESH MATERIALIZED VIEW cdp.gold.mv_huge",
            "airflow_etl_huge",
            "cdp.gold.mv_huge",
            &["cdp.bronze.huge"],
            1_700_000_000,
        )];

        let r = MaterializedViewPinningRecommender::new()
            .with_refresh_log_reader(Arc::new(FixedRefreshes {
                events: Mutex::new(refreshes),
            }));

        let mut c = cfg();
        c.mv_pinning.detect_strategies = vec![MvDetectStrategy::NameRegex];
        c.mv_pinning.nvme_capacity_bytes = 1_000_000_000;
        c.mv_pinning.max_pin_bytes_pct = 0.5;
        let recs = run(&r, &c, &EmptyEvLog, &manifests).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].rationale["pin_bytes_too_large"], true);
    }

    #[test]
    fn pin_keys_are_deterministic_and_sorted() {
        let files = vec![
            df("s3://b/a.parquet", 100),
            df("s3://b/b.parquet", 200),
            df("s3://b/a.parquet", 100),
        ];
        let keys = pin_keys_for(&files);
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "pin_keys must come out sorted");
        assert_eq!(keys.len(), 2, "pin_keys must be deduplicated");
        let again = pin_keys_for(&files);
        assert_eq!(keys, again, "pin_keys must be deterministic");
    }

    #[test]
    fn invalid_regex_surfaces_as_error() {
        let r = MaterializedViewPinningRecommender::new();
        let mut c = cfg();
        c.mv_pinning.name_regex = "(unclosed".to_string();
        let manifests = FixedManifests {
            files: HashMap::new(),
        };
        let err = run(&r, &c, &EmptyEvLog, &manifests).unwrap_err();
        assert!(err.to_string().contains("name_regex"));
    }

    #[test]
    fn refresh_user_pattern_alone_classifies_when_sql_misses() {
        let mut files = HashMap::new();
        files.insert(
            "cdp.bronze.events".to_string(),
            vec![df("s3://b/events/00.parquet", 100_000_000)],
        );
        let manifests = FixedManifests { files };

        let refreshes = vec![ev(
            "INSERT INTO cdp.gold.mv_orders SELECT * FROM cdp.bronze.events",
            "airflow_etl_orders",
            "cdp.gold.mv_orders",
            &["cdp.bronze.events"],
            1_700_000_000,
        )];

        let r = MaterializedViewPinningRecommender::new()
            .with_refresh_log_reader(Arc::new(FixedRefreshes {
                events: Mutex::new(refreshes),
            }));
        let mut c = cfg();
        c.mv_pinning.detect_strategies = vec![MvDetectStrategy::NameRegex];
        let recs = run(&r, &c, &EmptyEvLog, &manifests).unwrap();
        assert_eq!(recs.len(), 1);
    }

    #[test]
    fn refresh_log_absent_returns_empty_vec_with_warn() {
        let r = MaterializedViewPinningRecommender::new();
        let manifests = FixedManifests {
            files: HashMap::new(),
        };
        let c = cfg();
        let stats = crate::input::FixtureShelfdStatsReader::empty();
        let tables: Vec<String> = Vec::new();
        let ctx = AnalysisContext {
            config: &c,
            event_log: &EmptyEvLog,
            manifests: &manifests,
            shelfd_stats: &stats,
            tables: &tables,
        };
        let recs = r.analyze(&ctx).unwrap();
        assert!(
            recs.is_empty(),
            "no refresh-log reader -> no recommendations (advisory mode lands later)"
        );
    }
}
