//! SHELF-65 — strict-gated live integration smoke for the
//! MV-aware pinning recommender.
//!
//! Compiles only under the `integration` cargo feature AND panics
//! at runtime if `SHELF_INTEGRATION=1` is not set. This is the
//! double-gate the cost-reduction plan §8 mandates to avoid the
//! SHELF-09 trap (silent-skip pretending tests pass). The plan and
//! the in-flight `chore/integration-test-strict-gate` (PR #65) both
//! call out that an `if env_var_unset { return; }` "skip" gate is
//! the wrong shape — fail loud OR don't compile.
//!
//! ## Scope of the live smoke (this PR)
//!
//! The cost-reduction plan calls for a synthetic shelfd + Trino +
//! MinIO stack driving an end-to-end MV-pinning regression. Such a
//! stack does not yet exist for `shelf-advisor` (the closest
//! existing artefact is `shelfd/tests/docker-compose.yml`, which
//! covers the SHELF-22 read-shim only). Standing one up is bigger
//! than this PR's scope; the canonical regression for SHELF-65 is
//! the deterministic snapshot test in `it_mv_pinning.rs`.
//!
//! This live test is the placeholder seam where the fuller stack
//! hookup will land (one follow-up PR per stack component). For
//! now it verifies that:
//! - The strict gate works (build fails without `--features
//!   integration`, runtime panics without `SHELF_INTEGRATION=1`).
//! - The recommender can be constructed and invoked against a
//!   minimal in-memory fixture WITHOUT silently passing on an
//!   empty result.

#![cfg(feature = "integration")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use shelf_advisor::{
    run_pipeline, AdvisorConfig, DataFile, IcebergEventLogReader, IcebergManifestReader,
    IcebergRefreshLogReader, MaterializedViewPinningRecommender, MvPinningConfig, QueryRecord,
    Recommender, RefreshEvent, Result,
};

fn require_strict_gate() {
    let v = std::env::var("SHELF_INTEGRATION").unwrap_or_default();
    assert_eq!(
        v, "1",
        "SHELF_INTEGRATION=1 must be set when running the `integration` feature; \
         silent-skip is forbidden by the cost-reduction plan §8 (SHELF-09 trap)"
    );
}

struct EmptyEvLog;
impl IcebergEventLogReader for EmptyEvLog {
    fn read_window(&self, _w: Duration) -> Result<Vec<QueryRecord>> {
        Ok(Vec::new())
    }
}

struct InMemoryManifests(HashMap<String, Vec<DataFile>>);
impl IcebergManifestReader for InMemoryManifests {
    fn list_files(&self, table: &str) -> Result<Vec<DataFile>> {
        Ok(self.0.get(table).cloned().unwrap_or_default())
    }
}

struct InMemoryRefreshes(Vec<RefreshEvent>);
impl IcebergRefreshLogReader for InMemoryRefreshes {
    fn read_refreshes(&self, _h: u64) -> Result<Vec<RefreshEvent>> {
        Ok(self.0.clone())
    }
}

#[test]
fn live_smoke_recommender_emits_against_minimal_fixture() {
    require_strict_gate();

    let mut files = HashMap::new();
    files.insert(
        "live.bronze.events".to_string(),
        vec![DataFile {
            path: "s3://live-bronze/events/00.parquet".to_string(),
            file_size_bytes: 1_000_000,
            record_count: 1,
            spec_id: 0,
        }],
    );

    let refreshes = vec![RefreshEvent {
        query_id: "live-q-1".to_string(),
        user: "airflow_etl_live".to_string(),
        query_sql: "REFRESH MATERIALIZED VIEW live.gold.mv_live".to_string(),
        written_table: "live.gold.mv_live".to_string(),
        base_tables: vec!["live.bronze.events".to_string()],
        started_at_unix_seconds: 1_700_000_000,
    }];

    let cfg = AdvisorConfig {
        event_log_table: "live.trino_logs.trino_queries".to_string(),
        output_path: PathBuf::from("/tmp/shelf-65-live.json"),
        window: Duration::from_secs(86_400),
        top_n_per_table: 8,
        mv_pinning: MvPinningConfig::default(),
    };

    let recommenders: Vec<Box<dyn Recommender>> = vec![Box::new(
        MaterializedViewPinningRecommender::new()
            .with_refresh_log_reader(Arc::new(InMemoryRefreshes(refreshes))),
    )];

    let recs = run_pipeline(&cfg, &EmptyEvLog, &InMemoryManifests(files), &recommenders)
        .expect("pipeline must succeed under live gate");

    assert!(
        !recs.is_empty(),
        "live smoke must not silent-pass on empty output"
    );
    assert_eq!(recs[0].recommendation_type, "mv_pinning");
}
