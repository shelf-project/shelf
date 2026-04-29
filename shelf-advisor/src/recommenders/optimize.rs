//! `OPTIMIZE` target recommender — **stub**.
//!
//! Target heuristic (SHELF-53): per table, compute the small-file
//! ratio (`files < 32 MiB` over total file count) and the
//! write-amplification of the most recent N snapshots; emit an
//! `optimize_targets` recommendation when both cross threshold.
//!
//! See `feature-ideas-ranked.md` Tier S #4 and Iceberg
//! [#9674](https://github.com/apache/iceberg/issues/9674).

use crate::config::AdvisorConfig;
use crate::error::Result;
use crate::input::{IcebergEventLogReader, IcebergManifestReader};
use crate::output::Recommendation;
use crate::recommenders::Recommender;

#[derive(Debug, Default)]
pub struct OptimizeRecommender {}

impl OptimizeRecommender {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Recommender for OptimizeRecommender {
    fn kind(&self) -> &'static str {
        "optimize_targets"
    }

    fn analyze(
        &self,
        _config: &AdvisorConfig,
        _event_log: &dyn IcebergEventLogReader,
        _manifests: &dyn IcebergManifestReader,
    ) -> Result<Vec<Recommendation>> {
        tracing::debug!("optimize recommender stub — SHELF-53 not yet wired");
        Ok(Vec::new())
    }
}
