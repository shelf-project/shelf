//! Advisor run configuration.
//!
//! `AdvisorConfig` is the de-CLI'd, de-env'd struct that the
//! recommender pipeline consumes. The CLI in `main.rs` is the only
//! place that knows about clap; everything below `lib.rs` takes an
//! `AdvisorConfig` so the pipeline is callable from integration tests
//! and future in-process embeddings without re-parsing argv.

use std::path::PathBuf;
use std::time::Duration;

/// Static configuration for a single advisor run.
#[derive(Debug, Clone)]
pub struct AdvisorConfig {
    /// Fully-qualified table name where the Iceberg-sink event
    /// listener writes `QueryCompletedEvent` rows. Matches the
    /// `cdp.trino_logs.trino_queries`-style three-part name. The
    /// Phase-1 stub does not actually open the table — see
    /// `IcebergEventLogReader`.
    pub event_log_table: String,

    /// Where the advisor writes the JSON array of recommendations.
    pub output_path: PathBuf,

    /// Lookback window for the event-log scan. The CLI accepts
    /// humantime-style strings (`1d`, `7d`, `24h`) and parses them
    /// into a `Duration` before constructing this struct.
    pub window: Duration,

    /// Hard cap on recommendations returned per `(table,
    /// recommendation_type)` pair. Mirrors the "false-positive
    /// flood" mitigation in `feature-ideas-ranked.md` Tier S #4 —
    /// without a per-table cap a single chatty workload could
    /// drown out everything else.
    pub top_n_per_table: usize,
}

impl AdvisorConfig {
    /// Phase-1 placeholder default. The real defaults will land with
    /// SHELF-53 once we have an answer for "where does the
    /// event-log table live in a vanilla Trino deployment?".
    pub const DEFAULT_EVENT_LOG_TABLE: &'static str = "shelf.advisor.trino_queries";

    /// Default per-table cap. Chosen to match the "top-N columns per
    /// table" framing in BLUEPRINT §7.4.1.
    pub const DEFAULT_TOP_N_PER_TABLE: usize = 8;
}
