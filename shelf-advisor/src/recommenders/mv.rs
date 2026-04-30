//! Materialized-view candidate recommender — **stub**, owned by
//! SHELF-65 (the cost-reduction-plan rename of the design note
//! filed at `agents/out/SHELF-47-mv-aware-pinning.md`).
//!
//! SHELF-65 lands the real implementation: parse Trino MV
//! definitions stored as Iceberg metadata-table properties, mine
//! the SHELF-60 event-listener log for `REFRESH MATERIALIZED VIEW`
//! frequency, and emit pin-list entries scoped to the MV's
//! defining predicate + TTL'd to next refresh + 1h. SHELF-53 only
//! ships the trait seam — this stub returns `Ok(vec![])` so the
//! default pipeline keeps exercising the trait surface.
//!
//! The sibling agent reads event_log + manifests + shelfd_stats
//! from [`AnalysisContext`]; the `nvme_quota * pin_fraction` cap
//! lives on `MvConfig::pin_fraction`.

use crate::error::Result;
use crate::output::Recommendation;
use crate::recommenders::{AnalysisContext, Recommender};

#[derive(Debug, Default)]
pub struct MaterializedViewRecommender {}

impl MaterializedViewRecommender {
    pub const KIND: &'static str = "mv_candidates";

    pub fn new() -> Self {
        Self::default()
    }
}

impl Recommender for MaterializedViewRecommender {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn analyze(&self, ctx: &AnalysisContext<'_>) -> Result<Vec<Recommendation>> {
        if !ctx.config.mv.enabled {
            tracing::debug!("MV recommender disabled in config; SHELF-65 not yet wired");
        }
        Ok(Vec::new())
    }
}
