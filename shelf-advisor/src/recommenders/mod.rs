//! Recommender trait + concrete (stub) implementations.
//!
//! Each recommender consumes the two input adapters defined in
//! `input::*` and emits zero or more `Recommendation`s. Phase-1
//! ships traits + stub impls returning `Ok(vec![])`; the real
//! scoring logic lands under SHELF-46 / SHELF-47 / SHELF-53.

pub mod bloom;
pub mod mv;
pub mod optimize;

use crate::config::AdvisorConfig;
use crate::error::Result;
use crate::input::{IcebergEventLogReader, IcebergManifestReader};
use crate::output::Recommendation;

pub use bloom::BloomFilterRecommender;
pub use mv::MaterializedViewRecommender;
pub use optimize::OptimizeRecommender;

/// Pipeline contract.
///
/// `analyze` is invoked once per advisor run, with full read access
/// to both the event-log and the per-table manifest reader. The
/// recommender owns the trade-off between "scan once, dispatch many"
/// (cheaper) and "scan per recommendation type" (simpler) — the
/// trait is intentionally unopinionated.
pub trait Recommender: Send + Sync {
    /// Stable identifier (e.g. `"bloom_filter_columns"`). Returned
    /// recommendations MUST set `Recommendation::recommendation_type`
    /// to this string so consumers can route by type without
    /// peeking inside `rationale`.
    fn kind(&self) -> &'static str;

    /// Run the recommender against the provided readers and emit
    /// zero or more recommendations.
    fn analyze(
        &self,
        config: &AdvisorConfig,
        event_log: &dyn IcebergEventLogReader,
        manifests: &dyn IcebergManifestReader,
    ) -> Result<Vec<Recommendation>>;
}

/// The default recommender set wired into `main.rs`. Kept as a free
/// function rather than a const so that downstream callers (and
/// integration tests) can swap individual recommenders without
/// reconstructing the whole pipeline.
pub fn default_recommenders() -> Vec<Box<dyn Recommender>> {
    vec![
        Box::new(BloomFilterRecommender::new()),
        Box::new(OptimizeRecommender::new()),
        Box::new(MaterializedViewRecommender::new()),
    ]
}
