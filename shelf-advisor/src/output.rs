//! Recommendation output schema.
//!
//! The advisor's only contract with downstream tooling is the JSON
//! shape of `Recommendation`. CI/CD pipelines (GitHub Actions /
//! Bytebase / dbt-cloud) consume the `[Recommendation]` array and
//! decide whether to open a PR / issue / dbt run.
//!
//! Schema stability rules:
//! - `recommendation_type` is a free-form string today, but each
//!   variant the advisor emits is documented in the README. New
//!   types are additive; renames are a breaking change and must
//!   bump the binary's major version.
//! - `rationale` and `suggested_change` are free-form
//!   `serde_json::Value` so individual recommenders can attach
//!   their own scoring inputs without forcing a new top-level
//!   field every time. Downstream consumers should treat unknown
//!   keys as forward-compatible additions.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// One recommendation row in the advisor output array.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recommendation {
    /// Discriminator: `bloom_filter_columns`, `optimize_targets`,
    /// `mv_candidates`, … See `recommenders/*` for the canonical
    /// list per release.
    pub recommendation_type: String,

    /// Fully-qualified table name (catalog.schema.table) the
    /// recommendation applies to.
    pub table: String,

    /// Confidence in `[0.0, 1.0]`. Recommenders SHOULD calibrate so
    /// that 0.8+ is "ship without human review" and 0.5–0.8 is
    /// "needs ops eyeballs".
    pub confidence: f32,

    /// Per-recommendation scoring inputs (selectivity, frequency,
    /// wall-time, file count, …). Free-form so each recommender can
    /// emit its own numeric breakdown.
    pub rationale: serde_json::Value,

    /// Suggested concrete change as a JSON object — typically an
    /// `alter_table` or `optimize` SQL string the downstream pipeline
    /// can splice into a PR.
    pub suggested_change: serde_json::Value,
}

/// Serialise `recs` as a pretty JSON array to `path`.
///
/// Pretty-printed because the primary consumer is a code-review tool
/// (PR diffs read better with one recommendation per line of nesting)
/// and the byte-count overhead is negligible at advisor cardinality.
pub fn write_recommendations_json(path: &Path, recs: &[Recommendation]) -> Result<()> {
    let json = serde_json::to_string_pretty(recs)?;
    std::fs::write(path, json)?;
    Ok(())
}
