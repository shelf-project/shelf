//! Materialized-view candidate recommender — **stub**.
//!
//! Target heuristic (SHELF-47): mine the event log for repeated
//! aggregation subqueries that beat their base-scan cost by a
//! configurable factor; emit `mv_candidates` recommendations with
//! the proposed `CREATE MATERIALIZED VIEW` SQL in `suggested_change`.
//!
//! See `feature-ideas-ranked.md` Tier S #7 and BLUEPRINT §7.4 for
//! the MV freshness / pinning design that consumes these
//! recommendations downstream.

use crate::config::AdvisorConfig;
use crate::error::Result;
use crate::input::{IcebergEventLogReader, IcebergManifestReader};
use crate::output::Recommendation;
use crate::recommenders::Recommender;

#[derive(Debug, Default)]
pub struct MaterializedViewRecommender {}

impl MaterializedViewRecommender {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Recommender for MaterializedViewRecommender {
    fn kind(&self) -> &'static str {
        "mv_candidates"
    }

    fn analyze(
        &self,
        _config: &AdvisorConfig,
        _event_log: &dyn IcebergEventLogReader,
        _manifests: &dyn IcebergManifestReader,
    ) -> Result<Vec<Recommendation>> {
        tracing::debug!("MV recommender stub — SHELF-47 not yet wired");
        Ok(Vec::new())
    }
}
