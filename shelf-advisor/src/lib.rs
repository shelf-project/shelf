//! `shelf-advisor` library surface.
//!
//! The advisor is primarily a binary, but the same pipeline is
//! useful from integration tests and (later) from in-process
//! embeddings — so the trait + type definitions live in `lib.rs`
//! and `main.rs` is a thin clap shim on top.
//!
//! See the binary entrypoint for the CLI contract; see `README.md`
//! for the JSON output schema and `docs/recommenders.md` for the
//! per-recommender thresholds.
//!
//! ## Architecture in one paragraph
//!
//! Three input adapters ([`IcebergEventLogReader`],
//! [`IcebergManifestReader`], [`ShelfdStatsReader`]) feed an
//! [`AnalysisContext`] which the [`Recommender`] trait consumes.
//! The default recommender set ships [`OptimizeRecommender`] +
//! [`PinListRecommender`] (real implementations) plus
//! [`BloomFilterRecommender`] + [`MaterializedViewRecommender`]
//! stubs that SHELF-52 / SHELF-65 fill in. Output is either a
//! bare JSON array (legacy `analyze` mode) or a versioned
//! [`Envelope`] (new `recommend` / `watch` / `dry-run` modes).

pub mod config;
pub mod error;
pub mod input;
pub mod output;
pub mod recommenders;
pub mod runtime;

pub use config::{AdvisorConfig, BloomConfig, MvConfig, OptimizeConfig, PinListConfig};
pub use error::{Error, Result};
pub use input::{
    DataFile, FixtureEventLogReader, FixtureManifestReader, FixtureShelfdStatsReader,
    HttpShelfdStatsReader, IcebergEventLogReader, IcebergManifestReader, PodStats, PoolStats,
    QueryRecord, ShelfdStatsReader,
};
pub use output::{
    render_rfc3339_utc, sort_for_emission, write_envelope_json, write_per_kind_dir,
    write_recommendations_json, Envelope, Inputs, Recommendation, SCHEMA_VERSION,
};
pub use recommenders::{
    default_recommenders, kind_filter, AnalysisContext, BloomFilterRecommender,
    MaterializedViewRecommender, OptimizeRecommender, PinListRecommender, Recommender,
};

/// Run every recommender against the supplied context and return
/// a single deterministic [`Envelope`].
///
/// Recommenders run sequentially: the workload is bounded by
/// event-log table size, not CPU, and parallelism would only buy
/// us noise on the `tracing` output. Order is irrelevant for
/// correctness — [`Envelope::new`] resorts the output.
pub fn run_pipeline(
    ctx: &AnalysisContext<'_>,
    recommenders: &[Box<dyn Recommender>],
    as_of: String,
) -> Result<Envelope> {
    let mut all: Vec<Recommendation> = Vec::new();
    for r in recommenders {
        let kind = r.kind();
        let recs = r.analyze(ctx)?;
        tracing::debug!(kind = kind, count = recs.len(), "recommender produced");
        all.extend(recs);
    }
    let inputs = Inputs {
        trino_query_count: ctx.event_log.read_window(ctx.config.window)?.len() as u64,
        tables_scanned: ctx.tables.len() as u64,
        shelfd_pods_scraped: ctx.shelfd_stats.read_all().unwrap_or_default().len() as u64,
        window_secs: ctx.config.window.as_secs(),
        event_log_table: ctx.config.event_log_table.clone(),
    };
    Ok(Envelope::new(as_of, inputs, all))
}

/// Bare-array compatibility runner for the `analyze` legacy
/// command. Discards the envelope and returns just the recs.
pub fn run_pipeline_bare(
    ctx: &AnalysisContext<'_>,
    recommenders: &[Box<dyn Recommender>],
) -> Result<Vec<Recommendation>> {
    let env = run_pipeline(ctx, recommenders, "1970-01-01T00:00:00Z".to_string())?;
    Ok(env.recommendations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;

    fn empty_ctx_inner(
        cfg: &AdvisorConfig,
        rows: Vec<QueryRecord>,
        manifests: HashMap<String, Vec<DataFile>>,
        pods: Vec<PodStats>,
    ) -> (
        FixtureEventLogReader,
        FixtureManifestReader,
        FixtureShelfdStatsReader,
        Vec<String>,
    ) {
        let _ = cfg;
        let mut tables: Vec<String> = manifests.keys().cloned().collect();
        for r in &rows {
            if !tables.contains(&r.table) {
                tables.push(r.table.clone());
            }
        }
        tables.sort();
        (
            FixtureEventLogReader::new(rows),
            FixtureManifestReader::new(manifests),
            FixtureShelfdStatsReader::new(pods),
            tables,
        )
    }

    #[test]
    fn empty_pipeline_emits_empty_envelope() {
        let cfg = AdvisorConfig::defaults(PathBuf::from("/tmp/x"), Duration::from_secs(86_400));
        let (e, m, s, tables) = empty_ctx_inner(&cfg, vec![], HashMap::new(), vec![]);
        let ctx = AnalysisContext {
            config: &cfg,
            event_log: &e,
            manifests: &m,
            shelfd_stats: &s,
            tables: &tables,
        };
        let env = run_pipeline(&ctx, &default_recommenders(), "2026-04-30T00:00:00Z".into())
            .expect("run");
        assert_eq!(env.schema_version, SCHEMA_VERSION);
        assert!(env.recommendations.is_empty());
        assert_eq!(env.inputs.trino_query_count, 0);
        assert_eq!(env.inputs.tables_scanned, 0);
    }

    #[test]
    fn pipeline_is_deterministic_byte_for_byte() {
        let cfg = AdvisorConfig::defaults(PathBuf::from("/tmp/x"), Duration::from_secs(86_400));
        // One table with 16 1-MiB files → optimize triggers;
        // 8 hot queries × 50s × 100 MiB scanned → pin_list triggers.
        let mut manifests: HashMap<String, Vec<DataFile>> = HashMap::new();
        manifests.insert(
            "demo.events.purchases".to_string(),
            (0..16)
                .map(|i| DataFile {
                    path: format!("s3://x/p/{i}.parquet"),
                    file_size_bytes: 1 * 1024 * 1024,
                    record_count: 1,
                    spec_id: 0,
                })
                .collect(),
        );
        let rows: Vec<QueryRecord> = (0..8)
            .map(|i| QueryRecord {
                query_id: format!("q-{i}"),
                table: "demo.events.purchases".to_string(),
                equality_predicate_columns: vec![],
                wall_time: Duration::from_secs(50),
                physical_input_bytes: 100 * 1024 * 1024,
            })
            .collect();
        let (e1, m1, s1, t1) = empty_ctx_inner(&cfg, rows.clone(), manifests.clone(), vec![]);
        let ctx1 = AnalysisContext {
            config: &cfg,
            event_log: &e1,
            manifests: &m1,
            shelfd_stats: &s1,
            tables: &t1,
        };
        let env1 = run_pipeline(&ctx1, &default_recommenders(), "2026-04-30T00:00:00Z".into())
            .expect("run 1");
        let (e2, m2, s2, t2) = empty_ctx_inner(&cfg, rows, manifests, vec![]);
        let ctx2 = AnalysisContext {
            config: &cfg,
            event_log: &e2,
            manifests: &m2,
            shelfd_stats: &s2,
            tables: &t2,
        };
        let env2 = run_pipeline(&ctx2, &default_recommenders(), "2026-04-30T00:00:00Z".into())
            .expect("run 2");
        let j1 = serde_json::to_string_pretty(&env1).unwrap();
        let j2 = serde_json::to_string_pretty(&env2).unwrap();
        assert_eq!(j1, j2);
        // Both recommenders must have fired.
        let kinds: std::collections::BTreeSet<_> = env1
            .recommendations
            .iter()
            .map(|r| r.recommendation_type.as_str())
            .collect();
        assert!(kinds.contains("optimize_targets"));
        assert!(kinds.contains("pin_list_candidates"));
    }
}
