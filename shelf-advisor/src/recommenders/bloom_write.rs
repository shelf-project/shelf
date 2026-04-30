//! SHELF-52 — bloom-write advisor.
//!
//! Identifies Iceberg tables that would benefit most from being
//! rewritten with Parquet bloom filters on selected predicate
//! columns. The advisor does **not** perform the rewrite; it emits
//! a `Recommendation` that an operator (or an external CI/CD
//! pipeline) consumes to issue the actual `ALTER TABLE … SET
//! PROPERTIES` + `OPTIMIZE` sequence.
//!
//! Detection algorithm (see also
//! `docs/design-notes/SHELF-52-bloom-write-advisor.md`):
//!
//! 1. Group `QueryRecord`s by `table`. Drop tables with fewer than
//!    `BloomWriteConfig::min_query_count` queries or with average
//!    `physical_input_bytes < BloomWriteConfig::min_query_bytes`.
//! 2. For each candidate table, run
//!    `BloomWriteConfig::predicate_column_regex` over each row's
//!    `query_text` (and fall back to `equality_predicate_columns`
//!    when the text is empty), tally column hits, and keep the
//!    top-N (`BloomWriteConfig::top_n_columns`).
//! 3. Project per-query bytes saved as
//!    `avg_input_bytes * (1 - selectivity)`, where `selectivity`
//!    comes from `IcebergManifestReader::ndv()` if available, else
//!    `BloomWriteConfig::default_selectivity`.
//! 4. Compute payback as `2 * table_total_bytes / saving_per_query`
//!    (read + write rewrite cost in *bytes*; we never multiply by
//!    cents to compute the payback so a missing tariff cannot
//!    deflate the urgency). Severity buckets: `critical` (< 100),
//!    `warn` (100..1000), `info` (≥ 1000).
//! 5. Emit one `Recommendation` per candidate table.
//!
//! ### Why not check for *existing* writer-side blooms first
//!
//! Per SHELF-46 (PR #50, footer admission): when a table already
//! has writer-side bloom filters, shelfd's footer admission
//! exposes per-key `shelf_bloom_admit_total{kind="bloom_block"}`
//! counters scoped by table. The honest detection rule is "≈ 0
//! bloom-block admissions for this table's keys" — i.e. no blooms
//! were found in the footer. The current `IcebergManifestReader`
//! trait does not surface that signal yet (PR #50 is open at the
//! time of writing); we treat *all* high-traffic tables as
//! candidates and rely on the operator review step + the
//! recommendation evidence (which links to the runbook for
//! checking `shelf_bloom_admit_total` before applying) to filter.
//! This is documented in the design note as a known soft-spot.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::json;

#[cfg(test)]
use crate::config::AdvisorConfig;
use crate::config::BloomWriteConfig;
use crate::cost::Cents;
use crate::error::Result;
use crate::input::QueryRecord;
#[cfg(test)]
use crate::input::{IcebergEventLogReader, IcebergManifestReader};
use crate::output::Recommendation;
use crate::recommenders::Recommender;

/// Recommender id under which output rows are emitted.
pub const RECOMMENDATION_TYPE: &str = "bloom_write";

/// SHELF-52 recommender entry point.
#[derive(Debug, Default)]
pub struct BloomWriteRecommender {}

impl BloomWriteRecommender {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Recommender for BloomWriteRecommender {
    fn kind(&self) -> &'static str {
        RECOMMENDATION_TYPE
    }

    fn analyze(
        &self,
        ctx: &crate::recommenders::AnalysisContext<'_>,
    ) -> Result<Vec<Recommendation>> {
        let config = ctx.config;
        let event_log = ctx.event_log;
        let manifests = ctx.manifests;
        let cfg = &config.bloom_write;
        let records = event_log.read_window(config.window)?;
        let agg = aggregate_by_table(&records);

        let mut out = Vec::new();
        for (table, stats) in agg {
            let candidate = stats.is_candidate(cfg);
            if !candidate {
                tracing::debug!(
                    table = %table,
                    queries = stats.query_count,
                    avg_bytes = stats.avg_input_bytes(),
                    "skipping non-candidate table"
                );
                continue;
            }

            let columns = rank_columns(&records, &table, cfg)?;
            if columns.is_empty() {
                tracing::debug!(
                    table = %table,
                    "candidate table had no extractable predicate columns"
                );
                continue;
            }

            let table_files = manifests.list_files(&table).unwrap_or_default();
            let table_total_bytes: u64 = table_files.iter().map(|f| f.file_size_bytes).sum();
            // Operators sometimes run the advisor against tables the
            // manifest reader can't open (mismatched catalog, bad
            // creds, etc.) — fall back to the largest observed query
            // input so the rewrite estimate is at least directionally
            // honest rather than zero.
            let table_total_bytes = if table_total_bytes == 0 {
                stats.max_input_bytes
            } else {
                table_total_bytes
            };

            // Use the top-ranked column for selectivity; the rest are
            // co-recommended in the same `ALTER TABLE`. NDV access
            // only meaningful on the primary column.
            let primary = columns[0].column.clone();
            let ndv = manifests.ndv(&table, &primary)?;
            let selectivity = selectivity_estimate(ndv, stats.row_count_hint(), cfg);

            let saving_per_query_bytes =
                projected_saving_bytes(stats.avg_input_bytes(), selectivity);
            let rewrite_bytes = table_total_bytes.saturating_mul(2);
            let payback = payback_queries(rewrite_bytes, saving_per_query_bytes);
            let severity = Severity::from_payback(payback);
            let confidence = severity.confidence();

            let rewrite_cents = Cents::from_bytes_rewrite(rewrite_bytes, cfg.cost_cents_per_gib);

            out.push(build_recommendation(
                &table,
                &columns,
                &stats,
                selectivity,
                saving_per_query_bytes,
                rewrite_bytes,
                rewrite_cents,
                payback,
                severity,
                confidence,
                ndv.is_some(),
            ));
        }

        // Stable order so the snapshot test can assert byte-equality.
        out.sort_by(|a, b| a.table.cmp(&b.table));
        Ok(out)
    }
}

// --- aggregate / candidate selection -------------------------------------

#[derive(Debug, Clone, Default)]
pub(crate) struct TableStats {
    pub query_count: u64,
    pub total_input_bytes: u128,
    pub max_input_bytes: u64,
    pub total_wall_time_ms: u128,
}

impl TableStats {
    fn add(&mut self, rec: &QueryRecord) {
        self.query_count = self.query_count.saturating_add(1);
        self.total_input_bytes = self
            .total_input_bytes
            .saturating_add(rec.physical_input_bytes as u128);
        self.max_input_bytes = self.max_input_bytes.max(rec.physical_input_bytes);
        self.total_wall_time_ms = self
            .total_wall_time_ms
            .saturating_add(rec.wall_time.as_millis());
    }

    fn avg_input_bytes(&self) -> u64 {
        if self.query_count == 0 {
            0
        } else {
            (self.total_input_bytes / self.query_count as u128) as u64
        }
    }

    fn avg_wall_time(&self) -> Duration {
        if self.query_count == 0 {
            Duration::ZERO
        } else {
            Duration::from_millis((self.total_wall_time_ms / self.query_count as u128) as u64)
        }
    }

    /// Crude row-count proxy when the manifest reader doesn't supply
    /// one — used only as the denominator in the
    /// `selectivity ≈ 1 / NDV` fallback when NDV is known but row
    /// count isn't. Conservative because higher row counts depress
    /// selectivity and shrink the recommendation.
    fn row_count_hint(&self) -> u64 {
        self.max_input_bytes.max(1)
    }

    fn is_candidate(&self, cfg: &BloomWriteConfig) -> bool {
        self.query_count >= cfg.min_query_count && self.avg_input_bytes() >= cfg.min_query_bytes
    }
}

pub(crate) fn aggregate_by_table(records: &[QueryRecord]) -> BTreeMap<String, TableStats> {
    let mut map: BTreeMap<String, TableStats> = BTreeMap::new();
    for rec in records {
        map.entry(rec.table.clone()).or_default().add(rec);
    }
    map
}

// --- column ranking ------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnHit {
    pub column: String,
    pub frequency: u64,
}

/// Rank predicate columns by occurrence count across queries against
/// `table`. Uses `cfg.predicate_column_regex` over `query_text` and
/// falls back to `equality_predicate_columns` when the text is empty
/// (e.g. legacy event-log rows). Returns at most
/// `cfg.top_n_columns`.
pub(crate) fn rank_columns(
    records: &[QueryRecord],
    table: &str,
    cfg: &BloomWriteConfig,
) -> Result<Vec<ColumnHit>> {
    let re = regex::Regex::new(&cfg.predicate_column_regex)
        .map_err(|e| anyhow::anyhow!("invalid predicate_column_regex: {e}"))?;

    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for rec in records.iter().filter(|r| r.table == table) {
        if rec.query_text.is_empty() {
            for col in &rec.equality_predicate_columns {
                bump(&mut counts, col);
            }
            continue;
        }
        for cap in re.captures_iter(&rec.query_text) {
            if let Some(c) = cap.get(1) {
                bump(&mut counts, c.as_str());
            }
        }
    }

    // Stable secondary order on column name so determinism tests pass.
    let mut hits: Vec<ColumnHit> = counts
        .into_iter()
        .map(|(column, frequency)| ColumnHit { column, frequency })
        .collect();
    hits.sort_by(|a, b| {
        b.frequency
            .cmp(&a.frequency)
            .then_with(|| a.column.cmp(&b.column))
    });
    hits.truncate(cfg.top_n_columns);
    Ok(hits)
}

fn bump(map: &mut BTreeMap<String, u64>, key: &str) {
    let normalized = key.to_ascii_lowercase();
    *map.entry(normalized).or_insert(0) += 1;
}

// --- cost / savings projection -------------------------------------------

/// Translate (NDV, row count) → equality selectivity.
///
/// `selectivity = 1 / NDV` when NDV is known, clamped to
/// `[1e-6, 0.99]` so we don't divide by zero downstream and we
/// don't let NDV=1 collapse the projection. Falls back to
/// `cfg.default_selectivity` when NDV is `None`.
pub(crate) fn selectivity_estimate(
    ndv: Option<u64>,
    _row_count_hint: u64,
    cfg: &BloomWriteConfig,
) -> f64 {
    match ndv {
        Some(0) | None => cfg.default_selectivity,
        Some(n) => (1.0_f64 / n as f64).clamp(1e-6, 0.99),
    }
}

/// `expected_bytes_saved_per_query = total_query_input_bytes * (1 - selectivity)`.
/// Saturating cast back to `u64` so a pathological selectivity of 0 doesn't overflow.
pub(crate) fn projected_saving_bytes(avg_input_bytes: u64, selectivity: f64) -> u64 {
    let s = selectivity.clamp(0.0, 1.0);
    let saved = (avg_input_bytes as f64) * (1.0 - s);
    if saved.is_finite() && saved >= 0.0 {
        saved as u64
    } else {
        0
    }
}

/// `payback_queries = rewrite_bytes / saving_per_query`. Returns
/// `u64::MAX` when the saving is zero (rewrite never repays).
pub(crate) fn payback_queries(rewrite_bytes: u64, saving_per_query_bytes: u64) -> u64 {
    if saving_per_query_bytes == 0 {
        u64::MAX
    } else {
        rewrite_bytes.saturating_div(saving_per_query_bytes)
    }
}

// --- severity ladder -----------------------------------------------------

/// Three-step severity ladder anchored on the SHELF-52 design note.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// `payback_queries < 100` — pays for itself within a single
    /// busy day on a hot table; recommend operator action this
    /// sprint.
    Critical,
    /// `100 ≤ payback_queries < 1000` — pays back inside ~a week
    /// of typical traffic; recommend operator review next sprint.
    Warn,
    /// `payback_queries ≥ 1000` — long-tail recommendation;
    /// surface but don't gate on it.
    Info,
}

impl Severity {
    pub fn from_payback(payback_queries: u64) -> Self {
        if payback_queries < 100 {
            Severity::Critical
        } else if payback_queries < 1000 {
            Severity::Warn
        } else {
            Severity::Info
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Severity::Critical => "critical",
            Severity::Warn => "warn",
            Severity::Info => "info",
        }
    }

    /// Calibrated confidence per the README guidance:
    /// 0.8+ = "ship without human review", 0.5–0.8 = "needs ops".
    /// Bloom-write recommendations always benefit from operator
    /// review (they trigger an `OPTIMIZE` rewrite); we cap at 0.75
    /// even for `critical`.
    ///
    /// Values are deliberately exact-binary-fraction f32s
    /// (`0.75 = 3/4`, `0.625 = 5/8`, `0.5 = 1/2`) so the f32 → f64
    /// extension during JSON serialisation is lossless. That keeps
    /// the `serde_json::Value`-based snapshot test stable across
    /// platforms; tweaking these to "round" decimals like `0.65` /
    /// `0.45` re-introduces the f32 → f64 mismatch trap and breaks
    /// `it_bloom_write::bloom_write_matches_committed_fixture`.
    pub fn confidence(&self) -> f32 {
        match self {
            Severity::Critical => 0.75,
            Severity::Warn => 0.625,
            Severity::Info => 0.5,
        }
    }
}

// --- recommendation construction -----------------------------------------

#[allow(clippy::too_many_arguments)]
fn build_recommendation(
    table: &str,
    columns: &[ColumnHit],
    stats: &TableStats,
    selectivity: f64,
    saving_per_query_bytes: u64,
    rewrite_bytes: u64,
    rewrite_cents: Cents,
    payback: u64,
    severity: Severity,
    confidence: f32,
    ndv_available: bool,
) -> Recommendation {
    let column_csv = columns
        .iter()
        .map(|c| c.column.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let action_yaml = format!(
        "-- SHELF-52 bloom-write recommendation\n\
         ALTER TABLE {table} SET PROPERTIES (\n  \
         'write.parquet.bloom-filter-columns' = '{column_csv}'\n);\n\
         ALTER TABLE {table} EXECUTE optimize;\n"
    );

    let evidence = json!([
        {
            "metric": "query_count",
            "value": stats.query_count,
            "threshold": "min_query_count",
            "comment": "distinct queries against this table inside the lookback window"
        },
        {
            "metric": "avg_input_bytes",
            "value": stats.avg_input_bytes(),
            "threshold": "min_query_bytes",
            "comment": "average physical_input_bytes per query — drives saving_per_query"
        },
        {
            "metric": "avg_wall_time_ms",
            "value": stats.avg_wall_time().as_millis() as u64,
            "threshold": null,
            "comment": "average wall-clock per query, observational only"
        },
        {
            "metric": "selectivity_estimate",
            "value": selectivity,
            "threshold": "default_selectivity",
            "comment": if ndv_available {
                "1 / NDV from Iceberg manifest stats"
            } else {
                "NDV unavailable — falling back to BloomWriteConfig::default_selectivity (see design note)"
            }
        },
        {
            "metric": "saving_per_query_bytes",
            "value": saving_per_query_bytes,
            "threshold": null,
            "comment": "avg_input_bytes * (1 - selectivity)"
        },
        {
            "metric": "rewrite_bytes",
            "value": rewrite_bytes,
            "threshold": null,
            "comment": "2 * table_total_bytes (read + write for a full OPTIMIZE)"
        },
        {
            "metric": "rewrite_cost_cents",
            "value": rewrite_cents.as_cents(),
            "threshold": null,
            "comment": "placeholder tariff (see SHELF-52 design note); replaced by shelf_cost::Cents once PR #68 lands"
        },
        {
            "metric": "payback_queries",
            "value": payback,
            "threshold": "100/1000",
            "comment": "rewrite_bytes / saving_per_query_bytes; severity buckets at <100 (critical), 100..1000 (warn), >=1000 (info)"
        }
    ]);

    let rationale = json!({
        "id": format!("bloom_write_{table}"),
        "severity": severity.as_str(),
        "columns": columns
            .iter()
            .map(|hit| json!({"column": hit.column, "frequency": hit.frequency}))
            .collect::<Vec<_>>(),
        "evidence": evidence,
        "regex_caveat": "Predicate columns are extracted via a configurable regex over raw SQL text (BloomWriteConfig::predicate_column_regex). CTE inlining, function-wrapped predicates, and subqueries are silently missed; review the column list before applying.",
        "tier4_link": "If >30% of advisor cost concentrates in tables with no writer-side blooms, this recommender's output gates Tier-4 SHELF-G2 (`shelfd::side_bloom`) — see plan §Tier-4."
    });

    let suggested_change = json!({
        "action_yaml": action_yaml,
        "rewrite_bytes": rewrite_bytes,
        "rewrite_cost_cents": rewrite_cents.as_cents(),
        "rewrite_cost_dollars": rewrite_cents.fmt_dollars(),
        "payback_queries": payback,
        "severity": severity.as_str(),
    });

    Recommendation {
        recommendation_type: RECOMMENDATION_TYPE.to_string(),
        table: table.to_string(),
        confidence,
        rationale,
        suggested_change,
    }
}

// =========================================================================
// unit tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_PREDICATE_COLUMN_REGEX;
    use crate::cost::GIB;
    use crate::input::DataFile;

    fn cfg() -> BloomWriteConfig {
        BloomWriteConfig::defaults()
    }

    fn record(
        table: &str,
        bytes: u64,
        wall_ms: u64,
        cols: &[&str],
        query_text: &str,
    ) -> QueryRecord {
        QueryRecord {
            query_id: format!("q-{table}-{bytes}-{wall_ms}"),
            table: table.to_string(),
            equality_predicate_columns: cols.iter().map(|c| c.to_string()).collect(),
            wall_time: Duration::from_millis(wall_ms),
            physical_input_bytes: bytes,
            query_text: query_text.to_string(),
        }
    }

    // --- column ranking --------------------------------------------------

    #[test]
    fn column_ranking_extracts_from_query_text() {
        let cfg = cfg();
        let records = vec![
            record(
                "cat.s.t",
                1,
                1,
                &[],
                "SELECT * FROM cat.s.t WHERE user_id = 'u1' AND session_id = 'a'",
            ),
            record(
                "cat.s.t",
                1,
                1,
                &[],
                "SELECT * FROM cat.s.t WHERE user_id = 'u2'",
            ),
            record(
                "cat.s.t",
                1,
                1,
                &[],
                "SELECT * FROM cat.s.t WHERE region = 'IN'",
            ),
        ];
        let hits = rank_columns(&records, "cat.s.t", &cfg).unwrap();
        let names: Vec<_> = hits.iter().map(|h| h.column.clone()).collect();
        assert_eq!(names, vec!["user_id", "region", "session_id"]);
        assert_eq!(hits[0].frequency, 2);
        assert_eq!(hits[1].frequency, 1);
        assert_eq!(hits[2].frequency, 1);
    }

    #[test]
    fn column_ranking_handles_table_qualified_columns() {
        let cfg = cfg();
        let records = vec![record(
            "cat.s.t",
            1,
            1,
            &[],
            "SELECT * FROM cat.s.t t WHERE t.user_id = 7 AND t.event_kind = 'x'",
        )];
        let hits = rank_columns(&records, "cat.s.t", &cfg).unwrap();
        let names: Vec<_> = hits.iter().map(|h| h.column.clone()).collect();
        assert_eq!(names, vec!["event_kind", "user_id"]);
    }

    #[test]
    fn column_ranking_falls_back_to_pre_extracted_when_query_text_empty() {
        let cfg = cfg();
        let records = vec![record(
            "cat.s.t",
            1,
            1,
            &["user_id", "user_id", "region"],
            "",
        )];
        let hits = rank_columns(&records, "cat.s.t", &cfg).unwrap();
        let names: Vec<_> = hits.iter().map(|h| h.column.clone()).collect();
        assert_eq!(names, vec!["user_id", "region"]);
    }

    #[test]
    fn column_ranking_truncates_to_top_n() {
        let mut cfg = cfg();
        cfg.top_n_columns = 2;
        let records = vec![record(
            "cat.s.t",
            1,
            1,
            &[],
            "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 AND e = 5",
        )];
        let hits = rank_columns(&records, "cat.s.t", &cfg).unwrap();
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn column_ranking_skips_records_for_other_tables() {
        let cfg = cfg();
        let records = vec![
            record("a", 1, 1, &[], "SELECT * FROM a WHERE x = 1"),
            record("b", 1, 1, &[], "SELECT * FROM b WHERE y = 2"),
        ];
        let hits = rank_columns(&records, "a", &cfg).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].column, "x");
    }

    #[test]
    fn column_ranking_invalid_regex_errors() {
        let mut cfg = cfg();
        cfg.predicate_column_regex = "((((".to_string();
        let records: Vec<QueryRecord> = vec![];
        let err = rank_columns(&records, "t", &cfg).unwrap_err();
        assert!(err.to_string().contains("invalid predicate_column_regex"));
    }

    #[test]
    fn default_regex_compiles() {
        // Sanity: the default regex must always be valid.
        regex::Regex::new(DEFAULT_PREDICATE_COLUMN_REGEX).expect("default regex compiles");
    }

    // --- cost-savings projection ----------------------------------------

    #[test]
    fn projected_saving_at_default_selectivity_010() {
        // 1 GiB query, 0.1 selectivity → 90 % bytes saved.
        let saved = projected_saving_bytes(GIB, 0.1);
        assert_eq!(saved, (GIB as f64 * 0.9) as u64);
    }

    #[test]
    fn projected_saving_at_selectivity_050() {
        let saved = projected_saving_bytes(2 * GIB, 0.5);
        assert_eq!(saved, ((2 * GIB) as f64 * 0.5) as u64);
    }

    #[test]
    fn projected_saving_at_selectivity_090_is_small() {
        let saved = projected_saving_bytes(GIB, 0.9);
        assert_eq!(saved, (GIB as f64 * 0.1) as u64);
    }

    #[test]
    fn projected_saving_at_selectivity_zero_saves_everything() {
        assert_eq!(projected_saving_bytes(1024, 0.0), 1024);
    }

    #[test]
    fn projected_saving_at_selectivity_one_saves_nothing() {
        assert_eq!(projected_saving_bytes(1024, 1.0), 0);
    }

    #[test]
    fn selectivity_estimate_uses_ndv_when_available() {
        let cfg = cfg();
        // NDV=10 → selectivity=0.1.
        let s = selectivity_estimate(Some(10), 1_000_000, &cfg);
        assert!((s - 0.1).abs() < 1e-9);
    }

    #[test]
    fn selectivity_estimate_clamps_extreme_ndv() {
        let cfg = cfg();
        // NDV=1 → 1.0 → clamped to 0.99 (so projected_saving_bytes
        // doesn't return zero).
        let s = selectivity_estimate(Some(1), 1, &cfg);
        assert_eq!(s, 0.99);
    }

    #[test]
    fn selectivity_estimate_falls_back_when_ndv_missing() {
        let cfg = cfg();
        let s = selectivity_estimate(None, 1, &cfg);
        assert_eq!(s, cfg.default_selectivity);
    }

    #[test]
    fn selectivity_estimate_handles_ndv_zero_as_missing() {
        let cfg = cfg();
        let s = selectivity_estimate(Some(0), 1, &cfg);
        assert_eq!(s, cfg.default_selectivity);
    }

    // --- payback / severity ---------------------------------------------

    #[test]
    fn payback_queries_basic() {
        // 1000 bytes rewrite, 10 bytes saved per query → 100 queries.
        assert_eq!(payback_queries(1000, 10), 100);
    }

    #[test]
    fn payback_queries_zero_saving_returns_max() {
        assert_eq!(payback_queries(1000, 0), u64::MAX);
    }

    #[test]
    fn severity_boundary_critical() {
        assert_eq!(Severity::from_payback(0), Severity::Critical);
        assert_eq!(Severity::from_payback(50), Severity::Critical);
        assert_eq!(Severity::from_payback(99), Severity::Critical);
    }

    #[test]
    fn severity_boundary_warn() {
        assert_eq!(Severity::from_payback(100), Severity::Warn);
        assert_eq!(Severity::from_payback(500), Severity::Warn);
        assert_eq!(Severity::from_payback(999), Severity::Warn);
    }

    #[test]
    fn severity_boundary_info() {
        assert_eq!(Severity::from_payback(1000), Severity::Info);
        assert_eq!(Severity::from_payback(10_000), Severity::Info);
        assert_eq!(Severity::from_payback(u64::MAX), Severity::Info);
    }

    #[test]
    fn severity_confidence_monotonic() {
        assert!(Severity::Critical.confidence() > Severity::Warn.confidence());
        assert!(Severity::Warn.confidence() > Severity::Info.confidence());
    }

    // --- end-to-end candidate filter ------------------------------------

    #[test]
    fn analyze_skips_low_volume_tables() {
        struct StubLog {
            recs: Vec<QueryRecord>,
        }
        impl IcebergEventLogReader for StubLog {
            fn read_window(&self, _w: Duration) -> Result<Vec<QueryRecord>> {
                Ok(self.recs.clone())
            }
        }
        struct StubManifests;
        impl IcebergManifestReader for StubManifests {
            fn list_files(&self, _t: &str) -> Result<Vec<DataFile>> {
                Ok(vec![DataFile {
                    path: "s3://demo/foo".into(),
                    file_size_bytes: 100 * GIB,
                    record_count: 1000,
                    spec_id: 0,
                }])
            }
        }

        // 5 queries (well below default min_query_count=50) → skip.
        let recs = (0..5)
            .map(|i| {
                record(
                    "cat.s.t",
                    2 * GIB,
                    1,
                    &[],
                    &format!("SELECT * FROM t WHERE x = {i}"),
                )
            })
            .collect();
        let cfg = AdvisorConfig::defaults("/dev/null".into(), Duration::from_secs(86_400));
        let log = StubLog { recs };
        let manifests = StubManifests;
        let stats = crate::input::FixtureShelfdStatsReader::empty();
        let tables: Vec<String> = vec!["cat.s.t".to_string()];
        let ctx = crate::recommenders::AnalysisContext {
            config: &cfg,
            event_log: &log,
            manifests: &manifests,
            shelfd_stats: &stats,
            tables: &tables,
        };
        let r = BloomWriteRecommender::new().analyze(&ctx).unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn analyze_emits_recommendation_for_qualifying_table() {
        struct StubLog {
            recs: Vec<QueryRecord>,
        }
        impl IcebergEventLogReader for StubLog {
            fn read_window(&self, _w: Duration) -> Result<Vec<QueryRecord>> {
                Ok(self.recs.clone())
            }
        }
        struct StubManifests;
        impl IcebergManifestReader for StubManifests {
            fn list_files(&self, _t: &str) -> Result<Vec<DataFile>> {
                Ok(vec![DataFile {
                    path: "s3://demo/foo".into(),
                    file_size_bytes: 100 * GIB,
                    record_count: 1_000_000,
                    spec_id: 0,
                }])
            }
        }

        // 60 queries × 2 GiB → comfortably qualifies.
        let recs = (0..60)
            .map(|i| {
                record(
                    "cat.s.t",
                    2 * GIB,
                    100,
                    &[],
                    &format!("SELECT * FROM t WHERE user_id = {i}"),
                )
            })
            .collect();

        let cfg = AdvisorConfig::defaults("/dev/null".into(), Duration::from_secs(86_400));
        let log = StubLog { recs };
        let manifests = StubManifests;
        let stats = crate::input::FixtureShelfdStatsReader::empty();
        let tables: Vec<String> = vec!["cat.s.t".to_string()];
        let ctx = crate::recommenders::AnalysisContext {
            config: &cfg,
            event_log: &log,
            manifests: &manifests,
            shelfd_stats: &stats,
            tables: &tables,
        };
        let r = BloomWriteRecommender::new().analyze(&ctx).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].recommendation_type, "bloom_write");
        assert_eq!(r[0].table, "cat.s.t");
        // Recommendation rationale carries the SHELF-52 id.
        assert_eq!(
            r[0].rationale.get("id").and_then(|v| v.as_str()),
            Some("bloom_write_cat.s.t")
        );
        // Action carries an ALTER TABLE SET PROPERTIES line.
        let action = r[0].suggested_change["action_yaml"]
            .as_str()
            .expect("action_yaml present");
        assert!(action.contains("ALTER TABLE cat.s.t SET PROPERTIES"));
        assert!(action.contains("write.parquet.bloom-filter-columns"));
        assert!(action.contains("EXECUTE optimize"));
    }

    #[test]
    fn analyze_uses_iceberg_ndv_when_present() {
        struct StubLog {
            recs: Vec<QueryRecord>,
        }
        impl IcebergEventLogReader for StubLog {
            fn read_window(&self, _w: Duration) -> Result<Vec<QueryRecord>> {
                Ok(self.recs.clone())
            }
        }
        struct StubManifests;
        impl IcebergManifestReader for StubManifests {
            fn list_files(&self, _t: &str) -> Result<Vec<DataFile>> {
                Ok(vec![DataFile {
                    path: "s3://demo/foo".into(),
                    file_size_bytes: 100 * GIB,
                    record_count: 1_000_000,
                    spec_id: 0,
                }])
            }
            fn ndv(&self, _t: &str, _c: &str) -> Result<Option<u64>> {
                Ok(Some(1000))
            }
        }

        let recs = (0..60)
            .map(|i| {
                record(
                    "cat.s.t",
                    2 * GIB,
                    100,
                    &[],
                    &format!("SELECT * FROM t WHERE user_id = {i}"),
                )
            })
            .collect();
        let cfg = AdvisorConfig::defaults("/dev/null".into(), Duration::from_secs(86_400));
        let log = StubLog { recs };
        let manifests = StubManifests;
        let stats = crate::input::FixtureShelfdStatsReader::empty();
        let tables: Vec<String> = vec!["cat.s.t".to_string()];
        let ctx = crate::recommenders::AnalysisContext {
            config: &cfg,
            event_log: &log,
            manifests: &manifests,
            shelfd_stats: &stats,
            tables: &tables,
        };
        let r = BloomWriteRecommender::new().analyze(&ctx).unwrap();
        assert_eq!(r.len(), 1);
        let ev = &r[0].rationale["evidence"];
        let selectivity = ev
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["metric"] == "selectivity_estimate")
            .unwrap();
        // NDV=1000 → selectivity=0.001 (clamped above 1e-6).
        let val = selectivity["value"].as_f64().unwrap();
        assert!(val > 0.0 && val < 0.01);
        assert!(selectivity["comment"]
            .as_str()
            .unwrap()
            .contains("Iceberg manifest stats"));
    }

    #[test]
    fn determinism_two_runs_identical_bytes() {
        struct StubLog {
            recs: Vec<QueryRecord>,
        }
        impl IcebergEventLogReader for StubLog {
            fn read_window(&self, _w: Duration) -> Result<Vec<QueryRecord>> {
                Ok(self.recs.clone())
            }
        }
        struct StubManifests;
        impl IcebergManifestReader for StubManifests {
            fn list_files(&self, _t: &str) -> Result<Vec<DataFile>> {
                Ok(vec![DataFile {
                    path: "s3://demo/foo".into(),
                    file_size_bytes: 100 * GIB,
                    record_count: 1_000_000,
                    spec_id: 0,
                }])
            }
        }
        let recs: Vec<QueryRecord> = (0..60)
            .map(|i| {
                record(
                    "cat.s.t",
                    2 * GIB,
                    100,
                    &[],
                    &format!("SELECT * FROM t WHERE user_id = {i}"),
                )
            })
            .collect();
        let cfg = AdvisorConfig::defaults("/dev/null".into(), Duration::from_secs(86_400));
        let stats = crate::input::FixtureShelfdStatsReader::empty();
        let tables: Vec<String> = vec!["cat.s.t".to_string()];
        let log1 = StubLog { recs: recs.clone() };
        let manifests1 = StubManifests;
        let ctx1 = crate::recommenders::AnalysisContext {
            config: &cfg,
            event_log: &log1,
            manifests: &manifests1,
            shelfd_stats: &stats,
            tables: &tables,
        };
        let r1 = BloomWriteRecommender::new().analyze(&ctx1).unwrap();
        let log2 = StubLog { recs };
        let manifests2 = StubManifests;
        let ctx2 = crate::recommenders::AnalysisContext {
            config: &cfg,
            event_log: &log2,
            manifests: &manifests2,
            shelfd_stats: &stats,
            tables: &tables,
        };
        let r2 = BloomWriteRecommender::new().analyze(&ctx2).unwrap();
        let j1 = serde_json::to_string(&r1).unwrap();
        let j2 = serde_json::to_string(&r2).unwrap();
        assert_eq!(j1, j2, "two analyze runs must be byte-identical");
    }
}
