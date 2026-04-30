//! Input adapters consumed by the recommender pipeline.
//!
//! Three reader contracts cover everything SHELF-53 needs and what
//! the sibling tickets (SHELF-65 MV-pinning, SHELF-52 bloom-write
//! advisor) plan to consume:
//!
//! * [`IcebergEventLogReader`] — Trino `QueryCompletedEvent` rows
//!   projected into an Iceberg log table by the listener jar
//!   (SHELF-60 in the cost-reduction plan, design note still filed
//!   under `agents/out/SHELF-37-iceberg-event-listener-jar.md`,
//!   tracked in PR #66 at HEAD).
//! * [`IcebergManifestReader`] — current-snapshot data files for one
//!   Iceberg table.
//! * [`ShelfdStatsReader`] — per-pod capacity / used-bytes snapshot
//!   from a single shelfd pod's `/stats` endpoint. Required by the
//!   `pin_list` scoring denominator and (later) by SHELF-65's
//!   `nvme_quota * pin_fraction` cap.
//!
//! Why traits-first: the recommenders carry the interesting logic
//! (selectivity scoring, small-file detection, MV candidate ranking)
//! and we want to test that logic against deterministic in-memory
//! fixtures rather than spinning up an Iceberg catalog + a Trino
//! coordinator + a shelfd pod in CI. The fixture readers in this
//! module ship the JSON shape the integration tests + the
//! `dry-run` CLI mode replay.
//!
//! Production readers — JDBC against the listener log table, an
//! `iceberg-rust` manifest walker, and an HTTP client for shelfd —
//! are deferred to follow-up tickets per the user override on this
//! ticket: a Rust Trino client (`prusto`, etc.) is **not** added
//! to the workspace dep graph in SHELF-53. Operators wire their
//! own bridge against the trait in the meantime; the in-tree CLI
//! supports the dry-run / fixture path end-to-end.

pub mod event_listener;
pub mod manifest;
pub mod mv_pinning;
pub mod shelfd_stats;

pub use event_listener::{FixtureEventLogReader, IcebergEventLogReader, QueryRecord};
pub use manifest::{DataFile, FixtureManifestReader, IcebergManifestReader};
pub use mv_pinning::{
    IcebergRefreshLogReader, IcebergTablePropertiesReader, MvTableProperties, RefreshEvent,
};
pub use shelfd_stats::{
    FixtureShelfdStatsReader, HttpShelfdStatsReader, PodStats, PoolStats, ShelfdStatsReader,
};
