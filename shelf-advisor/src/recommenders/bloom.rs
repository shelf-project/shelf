//! Bloom-filter column recommender — **stub**.
//!
//! Target heuristic (SHELF-46): for every `(table, column)` pair
//! seen in `WHERE col = literal` predicates, score
//! `equality_selectivity × frequency × wall_time_p50` and emit the
//! top-N per table as `bloom_filter_columns` recommendations.
//!
//! See BLUEPRINT §7.4.1 for the full design. The Phase-1 stub
//! returns `Ok(vec![])` so the CLI smoke test exercises the JSON
//! emission path without depending on the real scorer.

use crate::config::AdvisorConfig;
use crate::error::Result;
use crate::input::{IcebergEventLogReader, IcebergManifestReader};
use crate::output::Recommendation;
use crate::recommenders::Recommender;

#[derive(Debug, Default)]
pub struct BloomFilterRecommender {}

impl BloomFilterRecommender {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Recommender for BloomFilterRecommender {
    fn kind(&self) -> &'static str {
        "bloom_filter_columns"
    }

    fn analyze(
        &self,
        _config: &AdvisorConfig,
        _event_log: &dyn IcebergEventLogReader,
        _manifests: &dyn IcebergManifestReader,
    ) -> Result<Vec<Recommendation>> {
        tracing::debug!("bloom-filter recommender stub — SHELF-46 not yet wired");
        Ok(Vec::new())
    }
}
