//! `shelf-advisor` library surface.
//!
//! The advisor is primarily a binary, but the same pipeline is
//! useful from integration tests and (later) from in-process
//! embeddings — so the trait + type definitions live in `lib.rs`
//! and `main.rs` is a thin clap shim on top.
//!
//! See the binary entrypoint for the CLI contract; see `README.md`
//! for the JSON output schema.

pub mod config;
pub mod error;
pub mod input;
pub mod output;
pub mod recommenders;

pub use config::AdvisorConfig;
pub use error::{Error, Result};
pub use input::{DataFile, IcebergEventLogReader, IcebergManifestReader, QueryRecord};
pub use output::{write_recommendations_json, Recommendation};
pub use recommenders::{
    default_recommenders, BloomFilterRecommender, MaterializedViewRecommender,
    OptimizeRecommender, Recommender,
};

/// Run every recommender in `recommenders` against the supplied
/// readers and concatenate their outputs into a single
/// `Vec<Recommendation>`. The result is what the CLI serialises to
/// JSON.
///
/// Recommenders run sequentially today — the workload is bounded by
/// event-log table size, not CPU, and parallelism would only buy us
/// noise on the `tracing` output. SHELF-53 may revisit if a
/// recommender grows expensive enough to dominate runtime.
pub fn run_pipeline(
    config: &AdvisorConfig,
    event_log: &dyn IcebergEventLogReader,
    manifests: &dyn IcebergManifestReader,
    recommenders: &[Box<dyn Recommender>],
) -> Result<Vec<Recommendation>> {
    let mut out = Vec::new();
    for r in recommenders {
        let kind = r.kind();
        let recs = r.analyze(config, event_log, manifests)?;
        tracing::debug!(kind = kind, count = recs.len(), "recommender produced");
        out.extend(recs);
    }
    Ok(out)
}
