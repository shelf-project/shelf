//! Trino event-listener input.
//!
//! Reads `QueryCompletedEvent`-shaped rows produced by the
//! Shelf-maintained Iceberg-sink event-listener jar (SHELF-60 in the
//! cost-reduction plan, design note still filed under
//! `agents/out/SHELF-37-iceberg-event-listener-jar.md`, tracked in
//! PR #66 at HEAD). The advisor only needs a tiny slice of that
//! event today — table name, equality-predicate columns, wall time,
//! bytes scanned — so we model the *minimum* shape here and grow it
//! as the recommenders ask for more.
//!
//! Why this is trait-shaped: the production reader will pull from
//! the listener's Iceberg log table over JDBC (the user override
//! on SHELF-53 explicitly forbids adding `prusto` or any other
//! heavy Trino-Rust client to the workspace in this PR), but the
//! recommenders shouldn't care whether the rows came from JDBC, a
//! direct Iceberg manifest scan, or a JSON fixture. The
//! `FixtureEventLogReader` below is the in-tree implementation
//! that powers the `dry-run` CLI mode and every unit / integration
//! test.

use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// One materialised event-log row. Field set is intentionally
/// minimal; SHELF-52 will extend this with operator-summary
/// columns (`bytesReadFromCache` / `bytesReadExternally`) once the
/// bloom-write advisor needs them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRecord {
    /// Trino `query_id`. Useful for joining against the engine's
    /// own audit logs when an operator wants to drill into a
    /// specific recommendation.
    pub query_id: String,

    /// Three-part `catalog.schema.table` of the *primary* scanned
    /// table. Multi-table queries fan out to one record per scanned
    /// table at write time so the advisor can stay table-keyed.
    pub table: String,

    /// Columns appearing on the LHS of an equality predicate
    /// (`WHERE col = literal`). Drives the (future) bloom-filter
    /// recommender's frequency tally.
    #[serde(default)]
    pub equality_predicate_columns: Vec<String>,

    /// Wall-clock time for the *whole* query (not this table's
    /// scan). Approximate, but sufficient for the
    /// `(scanned_bytes × wall_time × frequency) / (1 + total_bytes /
    /// pool_capacity)` ranking used by `PinListRecommender`.
    #[serde(with = "humantime_serde")]
    pub wall_time: Duration,

    /// Total bytes physically scanned for this table.
    pub physical_input_bytes: u64,

    /// Raw SQL text from `QueryCompletedEvent.metadata.query`.
    /// Optional because the SHELF-37 event-listener jar does not
    /// universally project the column today; the SHELF-52
    /// bloom-write advisor's regex extractor falls back to
    /// `equality_predicate_columns` when the text is absent.
    ///
    /// **Caveat (documented in the SHELF-52 design note):** the
    /// regex over raw SQL only captures lexically-visible
    /// `WHERE col = literal` patterns. CTE inlining, subqueries,
    /// and function-wrapped predicates (e.g. `lower(col) = 'x'`)
    /// silently miss; relying on this field is a heuristic, not a
    /// precise predicate-pushdown trace.
    #[serde(default)]
    pub query_text: String,
}

/// Reader contract for the event-log table.
///
/// Implementations may hit Trino directly, read the underlying
/// Iceberg table from object storage, or replay a fixture — the
/// advisor pipeline only cares that it gets a `Vec<QueryRecord>`
/// covering the requested window.
///
/// Errors should be reserved for *catastrophic* failures (auth
/// failure, connection refused). A successfully-opened but empty
/// table is `Ok(vec![])`.
pub trait IcebergEventLogReader: Send + Sync {
    /// Pull every `QueryRecord` whose `created_at` is within
    /// `window` of "now". Implementations are expected to apply
    /// the standard Iceberg snapshot-isolation read.
    fn read_window(&self, window: Duration) -> Result<Vec<QueryRecord>>;
}

/// Implementation of [`IcebergEventLogReader`] that returns rows
/// straight from a serialised JSON array.
///
/// The JSON shape is `Vec<QueryRecord>` — the same shape every
/// fixture under `tests/fixtures/` ships. The reader is
/// intentionally window-agnostic (it returns every row regardless
/// of `window`) because the fixtures already represent a single
/// pre-cropped window; encoding "look-back from now" inside a
/// frozen fixture would re-introduce wall-clock noise into the
/// snapshot tests.
pub struct FixtureEventLogReader {
    rows: Vec<QueryRecord>,
}

impl FixtureEventLogReader {
    /// Wrap a pre-built row list.
    pub fn new(rows: Vec<QueryRecord>) -> Self {
        Self { rows }
    }

    /// Load `Vec<QueryRecord>` from a JSON file on disk. Used by
    /// the `dry-run` CLI mode and by the integration tests.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = std::fs::read(path.as_ref())?;
        let rows: Vec<QueryRecord> = serde_json::from_slice(&bytes)?;
        Ok(Self::new(rows))
    }

    /// Number of rows currently held. Read by the envelope's
    /// `inputs.trino_query_count` so the report is auditable.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Convenience: empty-fixture fast path.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl IcebergEventLogReader for FixtureEventLogReader {
    fn read_window(&self, _window: Duration) -> Result<Vec<QueryRecord>> {
        Ok(self.rows.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_round_trips_through_json() {
        let row = QueryRecord {
            query_id: "20260430_111111_00001_abcde".to_string(),
            table: "demo.events.purchases".to_string(),
            equality_predicate_columns: vec!["user_id".to_string()],
            wall_time: Duration::from_secs(7),
            physical_input_bytes: 1_234_567,
            query_text: String::new(),
        };
        let json = serde_json::to_string(std::slice::from_ref(&row)).expect("encode");
        let decoded: Vec<QueryRecord> = serde_json::from_str(&json).expect("decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].query_id, row.query_id);
        assert_eq!(decoded[0].wall_time, row.wall_time);
    }

    #[test]
    fn fixture_reader_ignores_window() {
        let r = FixtureEventLogReader::new(vec![QueryRecord {
            query_id: "q1".to_string(),
            table: "demo.t".to_string(),
            equality_predicate_columns: vec![],
            wall_time: Duration::from_secs(1),
            physical_input_bytes: 1,
            query_text: String::new(),
        }]);
        let a = r.read_window(Duration::from_secs(60)).unwrap();
        let b = r.read_window(Duration::from_secs(86_400)).unwrap();
        assert_eq!(a.len(), b.len());
    }
}
