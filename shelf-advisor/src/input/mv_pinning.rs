//! SHELF-65 — MV-detection inputs (Iceberg table properties +
//! refresh-history reader).
//!
//! These two readers extend the advisor's input surface *without*
//! touching `IcebergEventLogReader` (the SHELF-37 schema, in flight
//! on PR #66). Rule 5 of the cost-reduction plan forbids modifying
//! in-flight surfaces beyond consuming their public ones; carrying
//! the MV-specific signals on dedicated readers keeps the
//! `QueryRecord` shape untouched and lets each upstream PR land
//! independently.
//!
//! Both readers are *optional* on the recommender — the advisor
//! binary's `default_recommenders()` constructs the recommender
//! without them and the recommender self-degrades to regex-only MV
//! detection (one WARN per run, lower confidence). The fixture-
//! driven snapshot test exercises the full path with both readers
//! plumbed in.

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Subset of an Iceberg table's `properties` map relevant to MV
/// detection.
///
/// Keys are taken from two sources:
/// - Trino-Iceberg integration writes
///   `trino.materialized-view.storage-table` and
///   `trino.materialized-view.fresh-snapshot-id` on the storage
///   table backing a Trino MV. (Verified against
///   <https://github.com/trinodb/trino/pull/26149>.)
/// - The canonical Iceberg flag `is_materialized_view = true` is
///   sometimes written by other engines. Rare in a Trino-only
///   stack but supported because the user spec explicitly calls
///   it out.
///
/// Missing fields are `None`; an empty `MvTableProperties` is *not*
/// the same as "no properties at all" (the latter is signalled by
/// `IcebergTablePropertiesReader::properties` returning `Ok(None)`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MvTableProperties {
    /// Value of the canonical Iceberg `is_materialized_view`
    /// property if present.
    #[serde(default)]
    pub is_materialized_view: Option<bool>,

    /// Value of `trino.materialized-view.storage-table` if present.
    /// Surfaces the storage-table that backs the Trino MV view.
    #[serde(default)]
    pub trino_storage_table: Option<String>,

    /// Value of `trino.materialized-view.fresh-snapshot-id` if
    /// present. Currently surfaced for downstream debugging only;
    /// the recommender does not consume it.
    #[serde(default)]
    pub trino_fresh_snapshot_id: Option<i64>,
}

impl MvTableProperties {
    /// Returns true iff *any* of the recognised MV-flag properties
    /// classify the table as a materialized view.
    pub fn classifies_as_mv(&self) -> bool {
        self.is_materialized_view == Some(true)
            || self.trino_storage_table.is_some()
            || self.trino_fresh_snapshot_id.is_some()
    }
}

/// Reader contract for a per-table Iceberg properties lookup.
///
/// `Ok(None)` = table exists but has no properties readable by this
/// adapter (e.g. catalog client wasn't configured with property
/// access). `Err` is reserved for catastrophic failures.
pub trait IcebergTablePropertiesReader: Send + Sync {
    /// Return the recognised MV-related properties for `table`.
    /// `table` is fully-qualified `catalog.schema.table`.
    fn properties(&self, table: &str) -> Result<Option<MvTableProperties>>;
}

/// One row from the MV-refresh history log.
///
/// Distinct from `QueryRecord` (input::event_listener) because:
/// - Refresh events carry the `user` (used by `refresh_user_pattern`
///   detection) and `query_sql` (used by `refresh_sql_pattern`
///   detection) fields, neither of which appears on `QueryRecord` in
///   the SHELF-37 PR #66 schema as merged.
/// - Refresh events are write-target-keyed (`written_table`),
///   whereas `QueryRecord` is read-target-keyed (`table`). MV-pinning
///   needs both: the WRITE target is the MV being refreshed; the
///   READ targets are the base tables to pin.
///
/// When SHELF-37 PR #66 merges and `QueryRecord` grows `user` /
/// `query_sql` / `inputs_json` fields, this trait can either
/// degrade to a thin adapter over `IcebergEventLogReader` or be
/// retired entirely.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshEvent {
    /// Trino `query_id` of the refresh query. Joins back to
    /// `QueryRecord::query_id` when both readers are populated.
    pub query_id: String,

    /// Trino user that issued the refresh. The
    /// `MvPinningConfig::refresh_user_pattern` regex is matched
    /// against this field.
    pub user: String,

    /// Verbatim SQL text of the refresh query. The
    /// `MvPinningConfig::refresh_sql_pattern` regex is matched
    /// against this field.
    pub query_sql: String,

    /// Fully-qualified `catalog.schema.table` of the MV being
    /// refreshed (the WRITE target).
    pub written_table: String,

    /// Fully-qualified base tables read by the refresh. The
    /// recommender pins data files belonging to these tables.
    #[serde(default)]
    pub base_tables: Vec<String>,

    /// Approximate refresh start time as UNIX epoch seconds.
    /// Used to bucket multiple refreshes into a single
    /// "refresh window" for grouping.
    pub started_at_unix_seconds: u64,
}

/// Reader contract for the MV-refresh history.
///
/// Implementations should return refresh events whose
/// `started_at_unix_seconds` falls within the requested
/// `lookback_hours`; the recommender does not re-filter by time.
pub trait IcebergRefreshLogReader: Send + Sync {
    /// Read every `RefreshEvent` whose `started_at_unix_seconds`
    /// falls within the last `lookback_hours` from "now".
    fn read_refreshes(&self, lookback_hours: u64) -> Result<Vec<RefreshEvent>>;
}
