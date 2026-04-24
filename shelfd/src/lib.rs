//! `shelfd` — the Shelf cache daemon, library surface.
//!
//! This crate is the Rust half of the Iceberg-native, row-group-granular
//! read cache described in `shelf/BLUEPRINT.md` §6.1 / §8 and further
//! scoped by `shelf/agents/out/03-plan.md` Phases 0 and 1.
//!
//! The library exposes the internal modules (`config`, `router`,
//! `store`, `origin`, `admission`, `http`, `control`, `metrics`,
//! `membership`, `error`) so that integration tests, benches, and the
//! `shelfctl` sibling crate can link against them without going through
//! the binary entrypoint.
//!
//! Scope boundary (see `agents/out/adr/0001` … `0009`): no embedded
//! Raft, no ONNX MLP admission, HTTP/2 only in v1, two Foyer pools
//! only, S3-FIFO eviction.
//!
//! Tickets that will flesh out this skeleton: SHELF-01 (workspace),
//! SHELF-02 (server), SHELF-03 (DRAM pool), SHELF-05 (origin),
//! SHELF-06 (read-through), SHELF-08 (metrics), SHELF-17 / SHELF-18
//! (pools + NVMe), SHELF-19 / SHELF-20 (HRW + membership),
//! SHELF-23 (shelfctl / control plane), SHELF-24 / SHELF-25
//! (pin list + admission).

#![warn(missing_debug_implementations)]
#![warn(rust_2018_idioms)]

pub mod admission;
pub mod config;
pub mod control;
pub mod error;
pub mod head_lru;
pub mod http;
pub mod membership;
pub mod metrics;
pub mod origin;
pub mod router;
pub mod store;

/// Re-export of the top-level error type so callers can `use shelfd::Error`.
pub use error::Error;

/// Crate result alias.
pub type Result<T> = std::result::Result<T, Error>;
