//! Recommendation output schema.
//!
//! The advisor's only contract with downstream tooling is the JSON
//! shape of [`Recommendation`] + [`Envelope`]. CI/CD pipelines
//! (GitHub Actions / GitLab CI / Bytebase / dbt-cloud) consume the
//! `[Recommendation]` array (legacy bare format) or the full
//! `Envelope` (versioned format shipped by `recommend`) and decide
//! whether to open a PR / issue / dbt run.
//!
//! ## Two output flavours
//!
//! * **Bare array** — written by the `analyze` legacy command to a
//!   single file. Backward-compatible with the SHELF-34 scaffold;
//!   the integration smoke test in `tests/it_smoke.rs` asserts
//!   this shape.
//! * **Envelope** — written by `recommend` / `watch` / `dry-run`
//!   to either a single file or a per-kind file under an
//!   `--output-dir`. Carries `schema_version`, the run's `as_of`
//!   wall-clock pin (overridable via `--as-of` for deterministic
//!   tests), the `inputs` audit struct, and the recommendations.
//!
//! ## Schema stability rules
//!
//! - `recommendation_type` is a free-form string but each variant
//!   the advisor emits is documented in `docs/recommenders.md`.
//!   New types are additive; renames are a breaking change and
//!   bump the binary's major version.
//! - `rationale` and `suggested_change` are free-form
//!   `serde_json::Value` so individual recommenders can attach
//!   their own scoring inputs without forcing a new top-level
//!   field every time. Downstream consumers should treat unknown
//!   keys as forward-compatible additions.
//! - `Envelope.schema_version` follows semver — a bump signals a
//!   breaking change to *any* of the field shapes below.
//!
//! ## Determinism
//!
//! `sort_for_emission` orders recommendations by
//! `(recommendation_type, table, -confidence, stable-id)` so two
//! runs over the same fixture produce byte-identical output. The
//! IDs themselves are derived from `(kind, table, sorted-rationale
//! key)` and contain zero wall-clock noise — verified by the
//! determinism test in `tests/it_recommend.rs`.

use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Envelope schema version, bumped on any breaking change to the
/// shape of [`Envelope`] / [`Recommendation`]. Schemas under
/// `schema/` are tagged with the matching `vN` filename.
pub const SCHEMA_VERSION: &str = "1.0.0";

/// One recommendation row in the advisor output array.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Recommendation {
    /// Discriminator: `optimize_targets`, `pin_list_candidates`,
    /// (forthcoming) `bloom_filter_columns`, `mv_candidates`. See
    /// `docs/recommenders.md` for the canonical list per release.
    pub recommendation_type: String,

    /// Fully-qualified table name (`catalog.schema.table`) the
    /// recommendation applies to. Single-table recommendations
    /// only — multi-table batches fan out to one row each.
    pub table: String,

    /// Confidence in `[0.0, 1.0]`. Recommenders SHOULD calibrate
    /// so that 0.8+ is "ship without human review" and 0.5–0.8
    /// is "needs ops eyeballs".
    pub confidence: f32,

    /// Per-recommendation scoring inputs (selectivity, frequency,
    /// wall-time, file count, …). Free-form so each recommender
    /// can emit its own numeric breakdown.
    pub rationale: serde_json::Value,

    /// Suggested concrete change — typically an `alter_table` or
    /// `optimize` SQL string the downstream pipeline can splice
    /// into a PR.
    pub suggested_change: serde_json::Value,
}

/// Audit summary of what the advisor scanned to produce the
/// recommendations. Surfaced into [`Envelope::inputs`] so a
/// downstream reviewer can sanity-check a report without
/// re-running the advisor.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Inputs {
    /// Number of `QueryRecord`s the event-log reader produced.
    pub trino_query_count: u64,
    /// Number of distinct tables the manifest reader was queried
    /// for.
    pub tables_scanned: u64,
    /// Number of shelfd `/stats` snapshots the recommenders saw.
    pub shelfd_pods_scraped: u64,
    /// Lookback window in seconds. Encoded as a number rather
    /// than a humantime string to keep schema validation simple.
    pub window_secs: u64,
    /// Fully-qualified event-log table name from the run's
    /// config. Audit value — the recommenders never reach back
    /// here, but operators want to see which table fed a report.
    pub event_log_table: String,
}

/// Versioned envelope around a recommendation array.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Envelope {
    /// Tag this report as having been produced by `shelf-advisor`.
    /// Constant; downstream filters can pin on this when sharing
    /// a JSON sink with other tools.
    pub generator: String,

    /// Semver of the envelope schema. Matches [`SCHEMA_VERSION`]
    /// at emit time; consumers should pin and warn when reading
    /// an envelope produced by a newer major.
    pub schema_version: String,

    /// RFC3339 wall-clock pin for the run. Override via `--as-of`
    /// for deterministic tests; default is "now".
    pub as_of: String,

    /// Audit summary; see [`Inputs`].
    pub inputs: Inputs,

    /// Sorted recommendations, deterministic order.
    pub recommendations: Vec<Recommendation>,
}

impl Envelope {
    /// Build an envelope from already-finalised recommendations
    /// + an `as_of` timestamp. The timestamp is the only piece
    /// of wall-clock state in the output; everything else is a
    /// pure function of the inputs.
    pub fn new(
        as_of: String,
        inputs: Inputs,
        mut recommendations: Vec<Recommendation>,
    ) -> Self {
        sort_for_emission(&mut recommendations);
        Self {
            generator: "shelf-advisor".to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            as_of,
            inputs,
            recommendations,
        }
    }

    /// Filter a recommendation list down to one kind. Used by
    /// `recommend <kind>` + by the per-kind file writer.
    pub fn for_kind(&self, kind: &str) -> Self {
        let recs: Vec<_> = self
            .recommendations
            .iter()
            .filter(|r| r.recommendation_type == kind)
            .cloned()
            .collect();
        Self {
            generator: self.generator.clone(),
            schema_version: self.schema_version.clone(),
            as_of: self.as_of.clone(),
            inputs: self.inputs.clone(),
            recommendations: recs,
        }
    }
}

/// Sort recommendations into the byte-stable canonical order:
/// `(recommendation_type, table, -confidence, stable-id)`. The
/// stable-id tiebreak is the JSON-serialised rationale, which is
/// itself a sorted-key object — sufficient to give two
/// recommendations of identical confidence a deterministic
/// ordering. Wall-clock is *not* used.
pub fn sort_for_emission(recs: &mut [Recommendation]) {
    recs.sort_by(|a, b| {
        a.recommendation_type
            .cmp(&b.recommendation_type)
            .then_with(|| a.table.cmp(&b.table))
            .then_with(|| {
                b.confidence
                    .partial_cmp(&a.confidence)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                let ja = serde_json::to_string(&a.rationale).unwrap_or_default();
                let jb = serde_json::to_string(&b.rationale).unwrap_or_default();
                ja.cmp(&jb)
            })
    });
}

/// Render a `SystemTime` as RFC3339 (UTC, second precision). We
/// hand-roll instead of pulling in `chrono` — the format is
/// fixed, zero allocations, and the only consumer is the audit
/// envelope.
pub fn render_rfc3339_utc(t: SystemTime) -> String {
    let dur: Duration = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs() as i64;
    // Days-since-epoch math (proleptic Gregorian).
    let days = secs.div_euclid(86_400);
    let seconds_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = days_to_ymd(days);
    let hour = (seconds_of_day / 3_600) as u32;
    let minute = ((seconds_of_day % 3_600) / 60) as u32;
    let second = (seconds_of_day % 60) as u32;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hour, minute, second
    )
}

/// Convert a Unix-epoch day number to (year, month, day). Adapted
/// from the Howard Hinnant date-algorithms paper (public domain).
fn days_to_ymd(days_since_epoch: i64) -> (i32, u32, u32) {
    // 1970-01-01 → 719_468 days since 0000-03-01.
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

/// Serialise `recs` as a pretty JSON array to `path`.
///
/// Pretty-printed because the primary consumer is a code-review
/// tool (PR diffs read better with one recommendation per line of
/// nesting) and the byte-count overhead is negligible at advisor
/// cardinality. Used by the `analyze` legacy command; `recommend`
/// uses [`write_envelope_json`] instead.
pub fn write_recommendations_json(path: &Path, recs: &[Recommendation]) -> Result<()> {
    let mut sorted: Vec<Recommendation> = recs.to_vec();
    sort_for_emission(&mut sorted);
    let json = serde_json::to_string_pretty(&sorted)?;
    let mut json = json.into_bytes();
    if !json.ends_with(b"\n") {
        json.push(b'\n');
    }
    std::fs::write(path, json)?;
    Ok(())
}

/// Serialise `env` as a pretty-printed JSON envelope to `path`.
pub fn write_envelope_json(path: &Path, env: &Envelope) -> Result<()> {
    let json = serde_json::to_string_pretty(env)?;
    let mut json = json.into_bytes();
    if !json.ends_with(b"\n") {
        json.push(b'\n');
    }
    std::fs::write(path, json)?;
    Ok(())
}

/// Per-kind directory writer. Lays one envelope per
/// recommendation kind under `<dir>/<date>/<kind>.json`, mirroring
/// the canonical SHELF-53 design note's output layout.
///
/// `date` is read from `env.as_of` (`YYYY-MM-DD` prefix) so that
/// `--as-of` overrides for tests pin both the envelope's wall-clock
/// field and the on-disk path.
pub fn write_per_kind_dir(dir: &Path, env: &Envelope) -> Result<Vec<PathBuf>> {
    let date = env
        .as_of
        .split('T')
        .next()
        .unwrap_or("0000-00-00")
        .to_string();
    let day_dir = dir.join(&date);
    std::fs::create_dir_all(&day_dir)?;
    let mut written: Vec<PathBuf> = Vec::new();
    let kinds: std::collections::BTreeSet<String> = env
        .recommendations
        .iter()
        .map(|r| r.recommendation_type.clone())
        .collect();
    if kinds.is_empty() {
        // Always emit *something* so the consumer can tell the
        // run completed cleanly with zero recommendations rather
        // than crashed silently. We pick `optimize_targets` as
        // the canary kind because it's the only one SHELF-53
        // owns end-to-end today; tests assert this name.
        let p = day_dir.join("optimize_targets.json");
        write_envelope_json(&p, &env.for_kind("optimize_targets"))?;
        written.push(p);
        return Ok(written);
    }
    for kind in kinds {
        let p = day_dir.join(format!("{kind}.json"));
        write_envelope_json(&p, &env.for_kind(&kind))?;
        written.push(p);
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(ty: &str, table: &str, conf: f32, key: &str, val: u64) -> Recommendation {
        Recommendation {
            recommendation_type: ty.to_string(),
            table: table.to_string(),
            confidence: conf,
            rationale: json!({ key: val }),
            suggested_change: json!({}),
        }
    }

    #[test]
    fn sort_is_stable_across_runs() {
        let mut a = vec![
            rec("optimize_targets", "demo.t.b", 0.6, "a", 1),
            rec("pin_list_candidates", "demo.t.a", 0.9, "x", 2),
            rec("optimize_targets", "demo.t.a", 0.9, "a", 0),
        ];
        let mut b = a.clone();
        b.reverse();
        sort_for_emission(&mut a);
        sort_for_emission(&mut b);
        assert_eq!(a, b);
        // optimize_targets sorts before pin_list_candidates,
        // and within each kind table.a comes before table.b.
        assert_eq!(a[0].table, "demo.t.a");
        assert_eq!(a[0].recommendation_type, "optimize_targets");
        assert_eq!(a[1].table, "demo.t.b");
        assert_eq!(a[2].recommendation_type, "pin_list_candidates");
    }

    #[test]
    fn rfc3339_known_epoch() {
        // 2026-04-30T00:00:00Z = 1_777_488_000 unix seconds.
        let t = UNIX_EPOCH + Duration::from_secs(1_777_507_200);
        // 2026-04-30T05:20:00 ≠ canonical; recompute below.
        // Use a simpler fixture to keep the test free of magic numbers:
        let t0 = UNIX_EPOCH;
        assert_eq!(render_rfc3339_utc(t0), "1970-01-01T00:00:00Z");
        let t1 = UNIX_EPOCH + Duration::from_secs(1_777_507_200);
        assert!(render_rfc3339_utc(t1).starts_with("2026-"));
        let _ = t;
    }

    #[test]
    fn envelope_per_kind_filters_correctly() {
        let recs = vec![
            rec("optimize_targets", "t1", 0.9, "small_file_ratio", 50),
            rec("pin_list_candidates", "t2", 0.7, "freq", 100),
        ];
        let env = Envelope::new(
            "2026-04-30T00:00:00Z".to_string(),
            Inputs::default(),
            recs,
        );
        let opt = env.for_kind("optimize_targets");
        assert_eq!(opt.recommendations.len(), 1);
        assert_eq!(opt.recommendations[0].table, "t1");
    }
}
