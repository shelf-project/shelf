//! Input adapters — Iceberg event-log table and manifest readers.
//!
//! These traits are the pluggable boundary between the advisor's
//! recommenders and the outside world. The Phase-1 scaffold ships
//! traits + value types only; the concrete `iceberg-rust` /
//! `parquet` / `aws-sdk-s3` plumbing lands under SHELF-53.
//!
//! Why traits-first: the recommenders carry the "interesting" logic
//! (selectivity scoring, small-file detection, MV candidate ranking)
//! and we want to test that logic against deterministic in-memory
//! fixtures rather than spinning up an Iceberg catalog in CI. The
//! integration test in `tests/it_smoke.rs` already exercises the
//! CLI end-to-end against the stub recommenders; once SHELF-53
//! lands, the same harness drives a `MockEventLogReader` per
//! recommender unit test.

pub mod event_listener;
pub mod manifest;

pub use event_listener::{IcebergEventLogReader, QueryRecord};
pub use manifest::{DataFile, IcebergManifestReader};
