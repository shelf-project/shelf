//! SHELF-65 — fixture-driven snapshot + determinism test for the
//! MV-aware pinning recommender.
//!
//! Lives under `tests/` so it builds against the public `lib.rs`
//! surface (no access to `mv.rs`'s `Interim` type or the test-only
//! mocks defined in the recommender's own `#[cfg(test)] mod tests`
//! block). The whole point of this test is to exercise the same
//! seam the production binary uses.
//!
//! This test does NOT require `SHELF_INTEGRATION=1`. It runs on
//! every `cargo test -p shelf-advisor` because the canonical
//! regression for this PR is a deterministic JSON snapshot — the
//! live-stack integration smoke is in `it_mv_pinning_live.rs`,
//! gated behind both the `integration` cargo feature and the
//! `SHELF_INTEGRATION=1` env var.
//!
//! ## What's being asserted
//!
//! - `recommendation_type`, `table`, `confidence`, every key under
//!   `rationale` *except* `pin_keys`, and every key under
//!   `suggested_change` *except* `pin_keys` match the fixture's
//!   expected JSON byte-for-byte once both sides are normalised
//!   through `serde_json::Value`.
//! - Every `pin_keys` entry is a valid 64-character lowercase hex
//!   digest (ADR-0011 SHA-256 shape).
//! - Two consecutive runs over the same fixture produce
//!   byte-identical output (the determinism test).
//!
//! `pin_keys` are excluded from the value-by-value snapshot because
//! re-deriving the SHA-256 by hand for a snapshot file is error
//! prone; the determinism test catches any non-determinism in the
//! key derivation, and the format test catches any bug that would
//! emit a non-hex or wrong-length key.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use shelf_advisor::{
    render_rfc3339_utc, run_pipeline, AdvisorConfig, AnalysisContext, DataFile,
    FixtureShelfdStatsReader, IcebergEventLogReader, IcebergManifestReader,
    IcebergRefreshLogReader, IcebergTablePropertiesReader, MaterializedViewPinningRecommender,
    MvPinningConfig, MvTableProperties, QueryRecord, Recommendation, Recommender, RefreshEvent,
    Result,
};
use std::time::SystemTime;

const FIXTURE: &str = "tests/fixtures/mv_pinning";

struct EmptyEvLog;
impl IcebergEventLogReader for EmptyEvLog {
    fn read_window(&self, _w: Duration) -> Result<Vec<QueryRecord>> {
        Ok(Vec::new())
    }
}

struct FixedManifests {
    files: HashMap<String, Vec<DataFile>>,
}
impl IcebergManifestReader for FixedManifests {
    fn list_files(&self, table: &str) -> Result<Vec<DataFile>> {
        Ok(self.files.get(table).cloned().unwrap_or_default())
    }
}

struct FixedProps {
    props: HashMap<String, MvTableProperties>,
}
impl IcebergTablePropertiesReader for FixedProps {
    fn properties(&self, table: &str) -> Result<Option<MvTableProperties>> {
        Ok(self.props.get(table).cloned())
    }
}

struct FixedRefreshes {
    events: Vec<RefreshEvent>,
}
impl IcebergRefreshLogReader for FixedRefreshes {
    fn read_refreshes(&self, _h: u64) -> Result<Vec<RefreshEvent>> {
        Ok(self.events.clone())
    }
}

fn load_fixtures() -> (
    AdvisorConfig,
    FixedManifests,
    Arc<FixedProps>,
    Arc<FixedRefreshes>,
) {
    let manifests_raw =
        std::fs::read_to_string(format!("{FIXTURE}/manifests.json")).expect("manifests fixture");
    let files: HashMap<String, Vec<DataFile>> =
        serde_json::from_str(&manifests_raw).expect("manifests parse");

    let props_raw = std::fs::read_to_string(format!("{FIXTURE}/table_properties.json"))
        .expect("table properties fixture");
    let props: HashMap<String, MvTableProperties> =
        serde_json::from_str(&props_raw).expect("table properties parse");

    let refresh_raw =
        std::fs::read_to_string(format!("{FIXTURE}/refresh_log.json")).expect("refresh fixture");
    let refreshes: Vec<RefreshEvent> = serde_json::from_str(&refresh_raw).expect("refresh parse");

    let mut cfg = AdvisorConfig::defaults(
        PathBuf::from("/tmp/shelf-65-snapshot.json"),
        Duration::from_secs(7 * 24 * 60 * 60),
    );
    cfg.event_log_table = "cdp.trino_logs.trino_queries".to_string();
    cfg.mv_pinning = MvPinningConfig::default();

    (
        cfg,
        FixedManifests { files },
        Arc::new(FixedProps { props }),
        Arc::new(FixedRefreshes { events: refreshes }),
    )
}

fn run_once(
    cfg: &AdvisorConfig,
    manifests: &FixedManifests,
    props: Arc<FixedProps>,
    refreshes: Arc<FixedRefreshes>,
) -> Vec<Recommendation> {
    let props_dyn: Arc<dyn IcebergTablePropertiesReader> = props;
    let refreshes_dyn: Arc<dyn IcebergRefreshLogReader> = refreshes;
    let recommenders: Vec<Box<dyn Recommender>> = vec![Box::new(
        MaterializedViewPinningRecommender::new()
            .with_table_properties_reader(props_dyn)
            .with_refresh_log_reader(refreshes_dyn),
    )];
    let stats = FixtureShelfdStatsReader::empty();
    let tables: Vec<String> = Vec::new();
    let log = EmptyEvLog;
    let ctx = AnalysisContext {
        config: cfg,
        event_log: &log,
        manifests,
        shelfd_stats: &stats,
        tables: &tables,
    };
    run_pipeline(
        &ctx,
        &recommenders,
        render_rfc3339_utc(SystemTime::UNIX_EPOCH),
    )
    .expect("pipeline")
    .recommendations
}

fn strip_pin_keys(v: &mut serde_json::Value) {
    if let Some(arr) = v.as_array_mut() {
        for rec in arr {
            if let Some(sc) = rec
                .get_mut("suggested_change")
                .and_then(|s| s.as_object_mut())
            {
                sc.remove("pin_keys");
            }
        }
    }
}

#[test]
fn snapshot_matches_fixture() {
    let (cfg, manifests, props, refreshes) = load_fixtures();
    let recs = run_once(&cfg, &manifests, props, refreshes);

    let mut actual = serde_json::to_value(&recs).expect("serialise actual");
    strip_pin_keys(&mut actual);

    let expected_raw = std::fs::read_to_string(format!("{FIXTURE}/expected_recommendations.json"))
        .expect("expected fixture");
    let mut expected: serde_json::Value =
        serde_json::from_str(&expected_raw).expect("expected parse");
    strip_pin_keys(&mut expected);

    let actual_pp = serde_json::to_string_pretty(&actual).unwrap();
    let expected_pp = serde_json::to_string_pretty(&expected).unwrap();

    assert_eq!(
        actual_pp, expected_pp,
        "snapshot mismatch.\nACTUAL:\n{actual_pp}\nEXPECTED:\n{expected_pp}"
    );
}

#[test]
fn pin_keys_are_64_char_lowercase_hex() {
    let (cfg, manifests, props, refreshes) = load_fixtures();
    let recs = run_once(&cfg, &manifests, props, refreshes);

    assert!(!recs.is_empty(), "fixture must produce at least one rec");
    for rec in &recs {
        let keys = rec
            .suggested_change
            .get("pin_keys")
            .and_then(|v| v.as_array())
            .expect("pin_keys array");
        assert!(
            !keys.is_empty(),
            "expected non-empty pin_keys for table {}",
            rec.table
        );
        for k in keys {
            let s = k.as_str().expect("pin_key string");
            assert_eq!(s.len(), 64, "ADR-0011 keys are 64 hex chars, got {s:?}");
            assert!(
                s.chars()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
                "ADR-0011 keys are lowercase hex, got {s:?}"
            );
        }
    }
}

#[test]
fn output_is_byte_deterministic_across_runs() {
    let (cfg, manifests, props, refreshes) = load_fixtures();
    let first = run_once(&cfg, &manifests, props.clone(), refreshes.clone());
    let second = run_once(&cfg, &manifests, props, refreshes);

    let first_json = serde_json::to_string_pretty(&first).unwrap();
    let second_json = serde_json::to_string_pretty(&second).unwrap();
    assert_eq!(
        first_json, second_json,
        "two consecutive runs must produce byte-identical output"
    );
}
