//! `OPTIMIZE` target recommender — full implementation.
//!
//! ## What it does
//!
//! For each Iceberg table the manifest reader knows about, count
//! how many of its current-snapshot data files fall below the
//! configured `small_file_bytes` threshold (default 32 MiB).
//! Tables whose small-file *ratio* exceeds
//! `optimize.small_file_ratio_min` (default 0.30) and whose
//! total file count clears `optimize.min_files_per_table`
//! (default 8) earn an `optimize_targets` recommendation.
//!
//! ## Why these defaults
//!
//! * The 32 MiB small-file threshold matches the RisingWave
//!   small-file blog cited in the canonical SHELF-53 design note;
//!   Iceberg's own `OPTIMIZE` `target-file-size-bytes` is 512 MiB,
//!   well above where bloom / footer / page-index amortisation
//!   start to break.
//! * The 30 % ratio threshold is conservative — it has to be
//!   high enough to not drown ops in noise on tables that just
//!   wrote a streaming append, and low enough that a partition
//!   that's drifted past 50 % small files gets flagged.
//! * The 8-file floor avoids recommending `OPTIMIZE` on tables
//!   that haven't accumulated enough writes to be interesting
//!   (any small file count is "100 % small" on a 3-file table).
//!
//! ## Score / confidence
//!
//! `confidence = clamp(small_file_ratio, 0.5, 0.95)` — a table
//! at exactly the ratio threshold ships at 0.5; pure-small-file
//! tables ship at 0.95 and never 1.0 (the 1.0 reserve is for
//! recommendations the advisor itself has measured the post-fix
//! win on).
//!
//! ## Suggested change
//!
//! `ALTER TABLE … EXECUTE optimize` is the standard Trino /
//! Iceberg incantation; we supply the FQDN-flavoured form so an
//! operator can paste it straight into a `dbt-cloud` job or
//! `Bytebase` issue. We do **not** issue the statement ourselves
//! — the advisor is read-only by SHELF-53 design.

use serde_json::json;

use crate::error::Result;
use crate::input::DataFile;
use crate::output::Recommendation;
use crate::recommenders::{AnalysisContext, Recommender};

#[derive(Debug, Default)]
pub struct OptimizeRecommender {}

impl OptimizeRecommender {
    pub const KIND: &'static str = "optimize_targets";

    pub fn new() -> Self {
        Self::default()
    }

    /// Per-table scorer. Pulled out so unit tests can drive it
    /// directly without going through the full pipeline.
    pub(crate) fn score_table(
        &self,
        table: &str,
        files: &[DataFile],
        small_file_bytes: u64,
        small_file_ratio_min: f32,
        min_files_per_table: u64,
    ) -> Option<Recommendation> {
        let total_files = files.len() as u64;
        if total_files < min_files_per_table {
            tracing::debug!(
                table,
                total_files,
                min_files_per_table,
                "optimize: table too young, skipping",
            );
            return None;
        }
        let small_files: u64 = files
            .iter()
            .filter(|f| f.file_size_bytes < small_file_bytes)
            .count() as u64;
        let ratio = small_files as f32 / total_files as f32;
        if ratio < small_file_ratio_min {
            tracing::debug!(
                table,
                ratio,
                small_file_ratio_min,
                "optimize: ratio below threshold",
            );
            return None;
        }
        let total_bytes: u64 = files.iter().map(|f| f.file_size_bytes).sum();
        let small_bytes: u64 = files
            .iter()
            .filter(|f| f.file_size_bytes < small_file_bytes)
            .map(|f| f.file_size_bytes)
            .sum();
        let avg_file_bytes = if total_files == 0 {
            0
        } else {
            total_bytes / total_files
        };
        let confidence = round_conf(ratio.clamp(0.5, 0.95));
        Some(Recommendation {
            recommendation_type: Self::KIND.to_string(),
            table: table.to_string(),
            confidence,
            rationale: json!({
                "small_file_bytes_threshold": small_file_bytes,
                "small_files": small_files,
                "total_files": total_files,
                "small_file_ratio": round_to_4(ratio as f64),
                "small_bytes": small_bytes,
                "total_bytes": total_bytes,
                "avg_file_bytes": avg_file_bytes,
            }),
            suggested_change: json!({
                "alter_table": format!(
                    "ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '{}MB')",
                    small_file_bytes / (1024 * 1024)
                ),
                "rewrite_data_files": true,
            }),
        })
    }
}

/// Round to 4 decimal places via integer math so the JSON output
/// is deterministic across architectures (no f32 → f64 → string
/// drift). Used in the rationale block; consumers parse the raw
/// number, never the formatted string.
fn round_to_4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

/// Round a confidence to 4 decimal places. Keeps the JSON-rendered
/// f32 byte-stable across f32 → f64 → ryu re-encoding.
fn round_conf(x: f32) -> f32 {
    let v = ((x as f64) * 10_000.0).round() / 10_000.0;
    v as f32
}

impl Recommender for OptimizeRecommender {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn analyze(&self, ctx: &AnalysisContext<'_>) -> Result<Vec<Recommendation>> {
        let cfg = &ctx.config.optimize;
        let mut out: Vec<Recommendation> = Vec::new();
        let mut sorted_tables: Vec<String> = ctx.tables.to_vec();
        sorted_tables.sort();
        for table in &sorted_tables {
            let files = ctx.manifests.list_files(table)?;
            if files.is_empty() {
                continue;
            }
            if let Some(rec) = self.score_table(
                table,
                &files,
                cfg.small_file_bytes,
                cfg.small_file_ratio_min,
                cfg.min_files_per_table,
            ) {
                if rec.confidence >= ctx.config.min_confidence {
                    out.push(rec);
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn df(size: u64) -> DataFile {
        DataFile {
            path: "s3://x/p".to_string(),
            file_size_bytes: size,
            record_count: 1,
            spec_id: 0,
        }
    }

    #[test]
    fn empty_table_skipped() {
        let r = OptimizeRecommender::new();
        let out = r.score_table("demo.t", &[], 32 * 1024 * 1024, 0.3, 8);
        assert!(out.is_none());
    }

    #[test]
    fn small_table_skipped_below_min_files() {
        // 7 small files, threshold 8: dropped.
        let r = OptimizeRecommender::new();
        let files: Vec<DataFile> = (0..7).map(|_| df(1_024)).collect();
        let out = r.score_table("demo.t", &files, 32 * 1024 * 1024, 0.3, 8);
        assert!(out.is_none());
    }

    #[test]
    fn no_small_files_skipped() {
        // 16 files all >= 64 MiB.
        let r = OptimizeRecommender::new();
        let files: Vec<DataFile> = (0..16).map(|_| df(64 * 1024 * 1024)).collect();
        let out = r.score_table("demo.t", &files, 32 * 1024 * 1024, 0.3, 8);
        assert!(out.is_none());
    }

    #[test]
    fn high_small_file_ratio_emits() {
        // 12 files, 10 of which are 1 MiB → ratio ~0.83.
        let r = OptimizeRecommender::new();
        let mut files: Vec<DataFile> = (0..10).map(|_| df(1 * 1024 * 1024)).collect();
        files.extend((0..2).map(|_| df(64 * 1024 * 1024)));
        let out = r
            .score_table("demo.t", &files, 32 * 1024 * 1024, 0.3, 8)
            .expect("emit");
        assert_eq!(out.recommendation_type, "optimize_targets");
        assert_eq!(out.table, "demo.t");
        assert!(out.confidence >= 0.5 && out.confidence <= 0.95);
        let small_files = out.rationale["small_files"].as_u64().unwrap();
        let total_files = out.rationale["total_files"].as_u64().unwrap();
        assert_eq!(small_files, 10);
        assert_eq!(total_files, 12);
    }

    #[test]
    fn ratio_just_below_threshold_skipped() {
        // 10 files, 2 small → ratio 0.20, below 0.30 threshold.
        let r = OptimizeRecommender::new();
        let mut files: Vec<DataFile> = (0..2).map(|_| df(1 * 1024 * 1024)).collect();
        files.extend((0..8).map(|_| df(64 * 1024 * 1024)));
        let out = r.score_table("demo.t", &files, 32 * 1024 * 1024, 0.3, 8);
        assert!(out.is_none());
    }

    #[test]
    fn confidence_clamped_below_max() {
        // 100 % small files → confidence clamped at 0.95.
        let r = OptimizeRecommender::new();
        let files: Vec<DataFile> = (0..16).map(|_| df(1 * 1024 * 1024)).collect();
        let out = r
            .score_table("demo.t", &files, 32 * 1024 * 1024, 0.3, 8)
            .expect("emit");
        assert!(out.confidence <= 0.95 + f32::EPSILON);
        assert!(out.confidence >= 0.94);
    }
}
