//! Trino event-listener input.
//!
//! Reads `QueryCompletedEvent`-shaped rows produced by the
//! Shelf-maintained Iceberg-sink event-listener jar (see
//! `feature-ideas-ranked.md` Tier S #2). The advisor only needs a
//! tiny slice of that event today — table name, predicate columns,
//! wall time, bytes scanned — so we model the *minimum* shape here
//! and grow it as the recommenders ask for more.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// One materialised event-log row. Field set is intentionally
/// minimal; SHELF-46 / SHELF-53 will extend this with operator
/// summaries (`bytesReadFromCache` / `bytesReadExternally`) once
/// the recommenders need them.
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
    /// (`WHERE col = literal`). Drives the bloom-filter
    /// recommender's frequency tally.
    pub equality_predicate_columns: Vec<String>,

    /// Wall-clock time for the *whole* query (not this table's
    /// scan). Approximate, but sufficient for the
    /// `selectivity × frequency × wall_time` ranking in
    /// BLUEPRINT §7.4.1.
    pub wall_time: Duration,

    /// Total bytes physically scanned for this table.
    pub physical_input_bytes: u64,
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
