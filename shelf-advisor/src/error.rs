//! Advisor error type.
//!
//! The advisor is a batch CLI: failure modes are coarse (bad config,
//! unreachable event-log table, malformed manifest) and we want full
//! `?`-chain context in the operator-facing log line. `anyhow::Error`
//! gives us that for free; the local `Result` alias keeps the
//! signatures across `input::*` and `recommenders::*` short.
//!
//! When a recommender needs to flag a *partial* failure (e.g. one
//! table's manifest was unreadable but the rest of the run should
//! continue), prefer logging at `warn!` and returning `Ok(vec![])`
//! over bubbling an `Err` — the downstream consumer expects a JSON
//! array, not a hard exit.

pub use anyhow::Error;

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
