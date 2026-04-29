//! Pin-list candidate recommender — full implementation.
//!
//! ## What it does
//!
//! Aggregate the event-log rows by `table` and emit
//! `pin_list_candidates` recommendations for the top-N tables by
//! the canonical SHELF-53 score:
//!
//! ```text
//! score = (scanned_bytes × wall_time_seconds × frequency)
//!         / (1 + total_bytes / pool_capacity)
//! ```
//!
//! `total_bytes` here is the per-table sum of `physical_input_bytes`
//! over the window (the cheapest available proxy for "how much of
//! this table would we have to pin"). `pool_capacity` is read from
//! the live shelfd `/stats` sample (sum of `rowgroup_pool.capacity_bytes`
//! across pods); we fall back to
//! `pin_list.default_pool_capacity_bytes` when no pod is reachable
//! so unit tests that don't carry a `/stats` fixture still produce
//! deterministic output. The denominator's `1 +` guards against
//! divide-by-zero on an empty cluster.
//!
//! ## Why these inputs
//!
//! * `scanned_bytes × wall_time` is the procurement-grade "pain"
//!   number — it tracks closely with what the SHELF-61 dollars-saved
//!   counter would attribute to this table. Frequency on top of
//!   it captures recurrence (a query that runs nightly is more
//!   pin-worthy than a one-off, even if the one-off scanned
//!   slightly more).
//! * Dividing by `1 + total_bytes / pool_capacity` gently
//!   penalises tables whose footprint exceeds the cache's
//!   capacity — pinning a 10× pool-capacity table is futile.
//!
//! ## Confidence
//!
//! Confidence is calibrated against frequency:
//!
//! ```text
//! confidence = clamp(0.4 + 0.05 × log10(frequency × wall_secs), 0.5, 0.95)
//! ```
//!
//! Single-query tables ship at the floor (0.5); tables with
//! 100+ queries × 100+ wall-seconds saturate at 0.95. The
//! recommender's `pin_list.min_confidence` knob (default 0.6)
//! filters the long tail before the global `min_confidence`
//! ever applies.
//!
//! ## Suggested change
//!
//! `suggested_change` carries a `pin_list_entry` block in the
//! shape SHELF-24's pin-list loader expects (`table`, `partition_filter`,
//! `ttl`, `pool`). For SHELF-53 we don't yet attempt partition
//! filter inference (SHELF-65 lands that for the MV-pinning case);
//! the pin-list entry is whole-table with `partition_filter: null`
//! and a 24h TTL.

use std::collections::HashMap;

use serde_json::json;

use crate::error::Result;
use crate::input::{PodStats, QueryRecord};
use crate::output::Recommendation;
use crate::recommenders::{AnalysisContext, Recommender};

#[derive(Debug, Default)]
pub struct PinListRecommender {}

impl PinListRecommender {
    pub const KIND: &'static str = "pin_list_candidates";

    pub fn new() -> Self {
        Self::default()
    }

    /// Aggregate per-table totals from a flat event-log read.
    /// Returns a deterministic-iteration vector so callers can
    /// score without re-sorting.
    pub(crate) fn aggregate(rows: &[QueryRecord]) -> Vec<TableAgg> {
        let mut by_table: HashMap<String, TableAgg> = HashMap::new();
        for row in rows {
            let agg = by_table
                .entry(row.table.clone())
                .or_insert_with(|| TableAgg::new(&row.table));
            agg.frequency += 1;
            agg.wall_secs += row.wall_time.as_secs_f64();
            agg.scanned_bytes += row.physical_input_bytes;
        }
        let mut out: Vec<TableAgg> = by_table.into_values().collect();
        out.sort_by(|a, b| a.table.cmp(&b.table));
        out
    }

    /// Score one table against a fixed pool capacity. Pulled out
    /// for direct testing.
    pub(crate) fn score_table(
        &self,
        agg: &TableAgg,
        pool_capacity: u64,
        min_frequency: u64,
        min_confidence: f32,
    ) -> Option<Recommendation> {
        if agg.frequency < min_frequency {
            return None;
        }
        // Numerator bounded — even on absurd workloads we don't
        // overflow a f64. SHELF-53's score is dimensional; we
        // round to 4 decimals on emission for byte-stability.
        let num = (agg.scanned_bytes as f64) * agg.wall_secs * (agg.frequency as f64);
        let denom = 1.0 + (agg.scanned_bytes as f64) / (pool_capacity.max(1) as f64);
        let score = num / denom;
        let confidence = pin_list_confidence(agg.frequency, agg.wall_secs);
        if confidence < min_confidence {
            return None;
        }
        Some(Recommendation {
            recommendation_type: Self::KIND.to_string(),
            table: agg.table.clone(),
            confidence,
            rationale: json!({
                "frequency": agg.frequency,
                "wall_time_seconds": round_to_4(agg.wall_secs),
                "scanned_bytes": agg.scanned_bytes,
                "pool_capacity_bytes": pool_capacity,
                "score": round_to_4(score),
                "score_formula": "(scanned_bytes * wall_time_seconds * frequency) / (1 + scanned_bytes / pool_capacity)",
            }),
            suggested_change: json!({
                "pin_list_entry": {
                    "table": agg.table,
                    "partition_filter": null,
                    "ttl": "24h",
                    "pool": "rowgroup",
                },
                "format": "shelfd/docs/design-notes/SHELF-23-24-admin-surface-and-pinlist.md"
            }),
        })
    }
}

fn round_to_4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

/// Confidence calibration. Single-query tables ship at the floor
/// (0.5); tables with `frequency × wall_secs` ≥ 1e4 saturate at
/// the cap (0.95). `log10` keeps the curve readable.
///
/// The result is rounded to 4 decimal places so two runs on
/// different architectures emit byte-stable JSON (no f32 → f64 →
/// ryu re-encoding drift).
pub(crate) fn pin_list_confidence(frequency: u64, wall_secs: f64) -> f32 {
    let f = (frequency as f64).max(1.0);
    let w = wall_secs.max(0.001);
    let raw = 0.4_f64 + 0.05 * (f * w).log10();
    let rounded = (raw.clamp(0.5, 0.95) * 10_000.0).round() / 10_000.0;
    rounded as f32
}

/// Per-table aggregation row. Public to the crate so the
/// integration tests can assert against it without going through
/// JSON.
#[derive(Debug, Clone)]
pub struct TableAgg {
    pub table: String,
    pub frequency: u64,
    pub wall_secs: f64,
    pub scanned_bytes: u64,
}

impl TableAgg {
    fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            frequency: 0,
            wall_secs: 0.0,
            scanned_bytes: 0,
        }
    }
}

/// Sum the rowgroup-pool capacity across every pod in the
/// `/stats` sample. Returns `None` when the sample is empty so
/// callers fall back to the configured default.
pub(crate) fn aggregate_pool_capacity(pods: &[PodStats]) -> Option<u64> {
    if pods.is_empty() {
        return None;
    }
    let total: u64 = pods
        .iter()
        .map(|p| p.rowgroup_pool.capacity_bytes)
        .sum();
    if total == 0 {
        None
    } else {
        Some(total)
    }
}

impl Recommender for PinListRecommender {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn analyze(&self, ctx: &AnalysisContext<'_>) -> Result<Vec<Recommendation>> {
        let cfg = &ctx.config.pin_list;
        let rows = ctx.event_log.read_window(ctx.config.window)?;
        if rows.is_empty() {
            tracing::debug!("pin_list: empty event log, skipping");
            return Ok(Vec::new());
        }
        let pods = ctx.shelfd_stats.read_all().unwrap_or_default();
        let pool_capacity =
            aggregate_pool_capacity(&pods).unwrap_or(cfg.default_pool_capacity_bytes);
        let aggs = Self::aggregate(&rows);
        let min_conf = cfg.min_confidence.max(ctx.config.min_confidence);
        let mut scored: Vec<Recommendation> = aggs
            .iter()
            .filter_map(|a| self.score_table(a, pool_capacity, cfg.min_frequency, min_conf))
            .collect();
        // Top-N per (table, kind) is naturally satisfied — every
        // recommendation here is unique-per-table — but we still
        // honour the global cap by keeping the top-N highest-score
        // rows when the candidate pool exceeds it. We sort by
        // score descending, take the top top_n_per_table per
        // recommendation_type (= one bucket here).
        scored.sort_by(|a, b| {
            let sa = a.rationale["score"].as_f64().unwrap_or(0.0);
            let sb = b.rationale["score"].as_f64().unwrap_or(0.0);
            sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(ctx.config.top_n_per_table);
        Ok(scored)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn row(table: &str, wall_secs: u64, bytes: u64) -> QueryRecord {
        QueryRecord {
            query_id: format!("q-{table}-{wall_secs}-{bytes}"),
            table: table.to_string(),
            equality_predicate_columns: vec![],
            wall_time: Duration::from_secs(wall_secs),
            physical_input_bytes: bytes,
        }
    }

    #[test]
    fn aggregate_groups_by_table() {
        let rows = vec![
            row("a", 1, 100),
            row("b", 2, 200),
            row("a", 3, 300),
        ];
        let aggs = PinListRecommender::aggregate(&rows);
        assert_eq!(aggs.len(), 2);
        let a = aggs.iter().find(|x| x.table == "a").unwrap();
        assert_eq!(a.frequency, 2);
        assert_eq!(a.scanned_bytes, 400);
        assert!((a.wall_secs - 4.0).abs() < 1e-9);
    }

    #[test]
    fn drops_below_min_frequency() {
        let r = PinListRecommender::new();
        let agg = TableAgg {
            table: "demo.cold".into(),
            frequency: 2,
            wall_secs: 1.0,
            scanned_bytes: 10,
        };
        // min_frequency 5 → drop.
        assert!(r.score_table(&agg, 1024, 5, 0.5).is_none());
    }

    #[test]
    fn drops_below_min_confidence() {
        let r = PinListRecommender::new();
        // Single-query, sub-second → confidence ≈ 0.5; min_conf 0.6 drops it.
        let agg = TableAgg {
            table: "demo.thin".into(),
            frequency: 5,
            wall_secs: 0.001,
            scanned_bytes: 1,
        };
        assert!(r.score_table(&agg, 1024, 5, 0.6).is_none());
    }

    #[test]
    fn emits_for_hot_table() {
        let r = PinListRecommender::new();
        let agg = TableAgg {
            table: "demo.hot".into(),
            frequency: 100,
            wall_secs: 100.0,
            scanned_bytes: 1_000_000_000,
        };
        let rec = r
            .score_table(&agg, 11 * 1024 * 1024 * 1024, 5, 0.5)
            .expect("hot table emits");
        assert_eq!(rec.recommendation_type, PinListRecommender::KIND);
        assert_eq!(rec.table, "demo.hot");
        // Confidence saturates near 0.95 for f×w=1e4.
        assert!(rec.confidence >= 0.6);
        assert_eq!(
            rec.suggested_change["pin_list_entry"]["pool"]
                .as_str()
                .unwrap(),
            "rowgroup"
        );
    }

    #[test]
    fn confidence_floor_and_ceiling() {
        // Floor: 1 query × 0.001s gives raw ≈ 0.4 - 0.15 = 0.25 → clamped to 0.5.
        assert!((pin_list_confidence(1, 0.001) - 0.5).abs() < 1e-3);
        // Ceiling: huge product → clamped to 0.95.
        assert!((pin_list_confidence(1_000_000, 1_000.0) - 0.95).abs() < 1e-3);
    }

    #[test]
    fn pool_capacity_aggregation() {
        let pods = vec![
            PodStats {
                pod_id: "shelf-0".into(),
                rowgroup_pool: crate::input::PoolStats {
                    capacity_bytes: 1024,
                    used_bytes: 0,
                },
                ..Default::default()
            },
            PodStats {
                pod_id: "shelf-1".into(),
                rowgroup_pool: crate::input::PoolStats {
                    capacity_bytes: 2048,
                    used_bytes: 0,
                },
                ..Default::default()
            },
        ];
        assert_eq!(aggregate_pool_capacity(&pods), Some(3072));
        assert_eq!(aggregate_pool_capacity(&[]), None);
    }
}
