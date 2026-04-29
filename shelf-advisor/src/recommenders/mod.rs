//! Recommender trait + concrete implementations.
//!
//! Each recommender consumes the input adapters bundled in
//! [`AnalysisContext`] and emits zero or more
//! [`Recommendation`](crate::output::Recommendation)s. SHELF-53
//! ships:
//!
//! * the trait + the [`AnalysisContext`] aggregation struct (the
//!   contract that sibling tickets SHELF-65 and SHELF-52 import),
//! * a real [`OptimizeRecommender`] (small-file detection over
//!   the Iceberg manifests), and
//! * a real [`PinListRecommender`] (top-N hot-table pin candidates
//!   scored against the live shelfd `/stats` capacity sample).
//!
//! `BloomFilterRecommender` and `MaterializedViewRecommender`
//! are kept as stubs so the trait surface stays exercised by the
//! default pipeline; their real implementations land in SHELF-52
//! and SHELF-65 respectively. Both stubs return `Ok(vec![])`.
//!
//! ## Why `AnalysisContext` (and not three separate `&dyn` args)
//!
//! The phase-1 scaffold passed `(config, event_log, manifests)` as
//! three positional arguments. SHELF-65's MV-aware-pinning
//! advisor wants a fourth reader (`/stats`); SHELF-52's
//! bloom-write advisor will want a fifth (`predicate_extractor`)
//! once the sqlglot sidecar lands. Bundling everything into a
//! struct future-proofs the trait — adding a new reader is a
//! field append, not a breaking signature change for every
//! existing recommender.

pub mod bloom;
pub mod bloom_write;
pub mod mv;
pub mod optimize;
pub mod pin_list;

use crate::config::AdvisorConfig;
use crate::error::Result;
use crate::input::{IcebergEventLogReader, IcebergManifestReader, ShelfdStatsReader};
use crate::output::Recommendation;

pub use bloom::BloomFilterRecommender;
pub use bloom_write::BloomWriteRecommender;
pub use mv::MaterializedViewRecommender;
pub use optimize::OptimizeRecommender;
pub use pin_list::PinListRecommender;

/// Bundle of every input the advisor pipeline can pass to a
/// recommender. Adding a new reader is a field append; existing
/// recommenders simply ignore the new field.
pub struct AnalysisContext<'a> {
    pub config: &'a AdvisorConfig,
    pub event_log: &'a dyn IcebergEventLogReader,
    pub manifests: &'a dyn IcebergManifestReader,
    pub shelfd_stats: &'a dyn ShelfdStatsReader,
    /// List of fully-qualified `catalog.schema.table` names the
    /// recommenders should scan. The pipeline pre-derives this
    /// from the union of the event-log row table column and any
    /// fixture-provided table list, so individual recommenders
    /// don't all re-walk the event log to discover tables.
    pub tables: &'a [String],
}

/// Pipeline contract. SHELF-65 + SHELF-52 import this trait
/// verbatim and add new recommenders to the default set in their
/// own PRs.
pub trait Recommender: Send + Sync {
    /// Stable identifier (e.g. `"optimize_targets"`). Returned
    /// recommendations MUST set [`Recommendation::recommendation_type`]
    /// to this string so consumers can route by type without
    /// peeking inside `rationale`.
    ///
    /// [`Recommendation::recommendation_type`]: crate::output::Recommendation::recommendation_type
    fn kind(&self) -> &'static str;

    /// Run the recommender against the provided context and emit
    /// zero or more recommendations.
    fn analyze(&self, ctx: &AnalysisContext<'_>) -> Result<Vec<Recommendation>>;
}

/// The default recommender set wired into `main.rs`. Order is
/// stable; the pipeline runs them in this order and the output
/// sort then re-orders by `(kind, table, …)` so emission is
/// deterministic regardless.
pub fn default_recommenders() -> Vec<Box<dyn Recommender>> {
    vec![
        Box::new(OptimizeRecommender::new()),
        Box::new(PinListRecommender::new()),
        Box::new(BloomFilterRecommender::new()),
        Box::new(BloomWriteRecommender::new()),
        Box::new(MaterializedViewRecommender::new()),
    ]
}

/// Lookup helper for the `recommend <kind>` CLI subcommand. Maps
/// the user-visible kind name to its canonical string used in
/// `Recommendation::recommendation_type`. Returns `None` for
/// `all`; callers handle that as "no filter".
pub fn kind_filter(kind_arg: &str) -> Option<&'static str> {
    match kind_arg {
        "all" => None,
        "optimize" | "optimize_targets" => Some("optimize_targets"),
        "pin_list" | "pin" | "pin_list_candidates" => Some("pin_list_candidates"),
        "bloom" | "bloom_filter_columns" => Some("bloom_filter_columns"),
        "mv" | "mv_candidates" => Some("mv_candidates"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_filter_aliases_resolve() {
        assert_eq!(kind_filter("optimize"), Some("optimize_targets"));
        assert_eq!(kind_filter("pin"), Some("pin_list_candidates"));
        assert_eq!(kind_filter("pin_list"), Some("pin_list_candidates"));
        assert_eq!(kind_filter("bloom"), Some("bloom_filter_columns"));
        assert_eq!(kind_filter("mv"), Some("mv_candidates"));
        assert_eq!(kind_filter("all"), None);
        assert_eq!(kind_filter("nonsense"), None);
    }

    #[test]
    fn default_set_has_five_kinds() {
        let kinds: Vec<&'static str> = default_recommenders().iter().map(|r| r.kind()).collect();
        assert!(kinds.contains(&"optimize_targets"));
        assert!(kinds.contains(&"pin_list_candidates"));
        assert!(kinds.contains(&"bloom_filter_columns"));
        assert!(kinds.contains(&"bloom_write"));
        assert!(kinds.contains(&"mv_candidates"));
        assert_eq!(kinds.len(), 5);
    }
}
