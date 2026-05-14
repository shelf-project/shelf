//! shelf-result-cache — SQL result-cache proxy for Shelf.
//!
//! §7 from TODO-fix-shelf-performance.md.
//!
//! # Architecture
//!
//! ```text
//!                      ┌──────────────────────────────────┐
//!                      │        BI tool / JDBC client      │
//!                      └────────────────┬─────────────────┘
//!                                       │ SQL (JDBC/HTTP)
//!                                       ▼
//!                 ┌──────────────────────────────────────────┐
//!                 │  shelf-result-cache  (this crate)         │
//!                 │  key = canonical_plan_hash ‖ snapshot_id  │
//!                 │  hit → Arrow IPC; miss → forward to Trino │
//!                 └────────────────┬─────────────────────────┘
//!                                  │ SQL passthrough on miss
//!                                  ▼
//!                        ┌──────────────────┐
//!                        │      Trino       │
//!                        └──────────────────┘
//! ```
//!
//! # Why a separate binary
//!
//! Result caching is a SQL-engine surface concern (canonicalise SQL, respect
//! row-level security, key on `(sql, role, snapshot)`). That's a JDBC/HTTP
//! gateway concern, not a byte-range cache concern.
//!
//! The shelf project's [`BLUEPRINT.md`] already names this as a sibling binary
//! for exactly this role.
//!
//! # Hit rate expectations
//!
//! Snowflake's [result cache docs](https://docs.snowflake.com/en/user-guide/querying-persisted-results)
//! report exact-text reuse persists 24h default, extends to 31d.
//!
//! This implementation uses **plan-fingerprint** matching (literals erased,
//! commutative operands sorted) + snapshot-id, so dashboards that refresh
//! with different date literals but the same plan shape can share cached
//! results when the underlying data hasn't changed.
//!
//! # Speedup on hit
//!
//! 100–1000× — results are pre-computed; only network round-trip remains.
//! [Napa (VLDB 2021)](https://research.google/pubs/napa-powering-scalable-data-warehousing-with-robust-query-performance-at-google/)
//! reports sub-second response on materialized-view-driven queries.

#![warn(missing_docs, missing_debug_implementations)]

pub mod cache;
pub mod canonicalizer;
pub mod config;
pub mod metrics;
pub mod proxy;
pub mod snapshot;

pub use cache::ResultCache;
pub use canonicalizer::PlanCanonicalizer;
pub use config::Config;
pub use proxy::ResultCacheProxy;
