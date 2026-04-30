//! Bloom-filter column recommender — **stub**, owned by SHELF-52.
//!
//! SHELF-52 (`agents/out/SHELF-52-bloom-advisor.md`) lands the
//! real implementation: predicate mining via the SHELF-60
//! event-listener log table + a sqlglot sidecar; per-`(table, column)`
//! scoring against equality-selectivity and column cardinality
//! pulled from Iceberg manifests. SHELF-53 only ships the trait
//! seam — this stub returns `Ok(vec![])` so the default pipeline
//! continues to exercise the trait surface end-to-end.
//!
//! The sibling agent reads from `AnalysisContext` (event log +
//! manifests; no extra reader needed) and emits recommendations
//! with `recommendation_type = "bloom_filter_columns"`.

use crate::error::Result;
use crate::output::Recommendation;
use crate::recommenders::{AnalysisContext, Recommender};

#[derive(Debug, Default)]
pub struct BloomFilterRecommender {}

impl BloomFilterRecommender {
    pub const KIND: &'static str = "bloom_filter_columns";

    pub fn new() -> Self {
        Self::default()
    }
}

impl Recommender for BloomFilterRecommender {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn analyze(&self, ctx: &AnalysisContext<'_>) -> Result<Vec<Recommendation>> {
        if !ctx.config.bloom.enabled {
            tracing::debug!("bloom recommender disabled in config; SHELF-52 not yet wired");
        }
        Ok(Vec::new())
    }
}
