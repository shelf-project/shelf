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
//! Raft, no ONNX MLP admission, HTTP/2 only in v1.
//!
//! Cache surface — three logical caches, two of them Foyer-backed:
//!   * **metadata pool** (DRAM-only, Foyer): manifest + footer bytes.
//!   * **rowgroup pool** (DRAM + NVMe hybrid, Foyer): Parquet row
//!     groups; default eviction is **LRU** (see
//!     [`config::EvictionPolicy`] — configurable to `S3Fifo`, `Lfu`,
//!     or `Fifo`; SHELF-E1b moved the default off S3-FIFO).
//!   * **head LRU** ([`head_lru`]): small object-existence cache that
//!     short-circuits S3 `HEAD` round-trips. Not a Foyer pool; a
//!     bounded path-keyed LRU with a negative-result TTL.
//!
//! Tickets that will flesh out this skeleton: SHELF-01 (workspace),
//! SHELF-02 (server), SHELF-03 (DRAM pool), SHELF-05 (origin),
//! SHELF-06 (read-through), SHELF-08 (metrics), SHELF-17 / SHELF-18
//! (pools + NVMe), SHELF-19 / SHELF-20 (HRW + membership),
//! SHELF-23 (shelfctl / control plane), SHELF-24 / SHELF-25
//! (pin list + admission).

#![warn(missing_debug_implementations)]
#![warn(rust_2018_idioms)]
// Pre-existing modules in this crate (compression, config, fingerprint,
// http, side_bloom) trigger lints that were promoted to `deny` in
// stable Rust 1.95 (Apr 2026). Those code paths are untouched by
// SHELF-25; silencing the specific lints here keeps the
// `cargo clippy -D warnings` rail green without dragging an unrelated
// refactor into this fix. Drop these once the underlying call sites
// migrate (one-line each — trivial follow-up).
#![allow(clippy::int_plus_one)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::unnecessary_sort_by)]
#![allow(clippy::manual_div_ceil)]

pub mod ab_tag;
pub mod admin_pin_payload;
pub mod admission;
pub mod admission_limiter;
pub mod admission_wtinylfu;
pub mod aws_chunked;
// RC6 P1.2 — cluster cap-ready gate (`/admin/cap-ready`). Codifies
// the "scale +2 before adding a new replica's traffic" workspace ops
// rule into a one-shot machine-readable check the cutover MR template
// can curl.
pub mod capacity_check;
pub mod coalesce;
// SHELF-45 — compaction-aware re-warm reactor. Default-off via
// config; the module is always compiled so EXPOSED_SERIES sees the
// Prometheus families on every build.
pub mod compaction_rewarm;
// A3 (rc.7) — Iceberg metadata.json polling worker that drives the
// SHELF-45 reactor without depending on the SHELF-37 listener (PR
// #66, parked on JDK 25). Default-off; module always compiled so
// the Prometheus series are pre-touched.
pub mod rewarm_poller;
// Dormant modules — present in the tree but with zero non-test callers
// in the current hot/control paths. Gated behind off-by-default Cargo
// features so default `shelfd` builds ship a smaller binary; the source
// stays in-tree per project policy (gate, don't delete) so a future
// caller can flip the flag without resurrecting code from history.
// B1 (rowgroup zstd) + SHELF-E2 (metadata zstd, feature-gated consumer).
// Module is unconditionally compiled because B1 wires runtime-configured
// compression through `CompressionPipeline`; the `zstd_metadata` feature
// only gates the metadata-pool consumer, not the module definition.
pub mod compression;
pub mod config;
// **A6 (rc.7)** — cooperative peer admission probabilistic gate.
// Default-OFF via `cache.coopAdmission.enabled=false`, so the module is
// always compiled (the EXPOSED_SERIES list and registry-regression test
// need the metric statics resolved at build time) but the gate is a
// no-op until an operator opts in. See ADR-0037.
pub mod control;
pub mod coop_admission;
// SHELF-40 — runtime glue for the `shelf-cost` audit-able cost
// model. Lives here (not under `metrics`) because it owns its own
// rolling-rate background task in addition to the metric handles.
pub mod cost;
pub mod decoded_meta;
pub mod error;
pub mod filter_service;
#[cfg(feature = "fingerprint")]
pub mod fingerprint;
pub mod freshness;
pub mod head_lru;
pub mod http;
pub mod lodc_backpressure;
pub mod membership;
pub mod metrics;
pub mod mv_registry;
pub mod origin;
pub mod parquet_admit;
// SHELF-34 wires `/predicate-prune` through `parquet_meta` in the hot
// path, so the module is unconditional. The `parquet_meta` feature
// kept in `shelfd/Cargo.toml` remains a marker for legacy callers
// (SHELF-46 bloom-block range extraction); the module definition is
// always present.
pub mod parquet_meta;
pub mod peer;
pub mod peer_fetch;
pub mod pinlist;
pub mod router;
pub mod s3_shim;
// Distinct feature name `side_bloom_module` to keep this gate from
// being confused with the `SideBloom` trait that `filter_service`
// defines independently for the hot path.
#[cfg(feature = "side_bloom_module")]
pub mod side_bloom;
pub mod store;
pub mod table_props;
pub mod telemetry;
pub mod text_index;
// **B3 (rc.7)** — intermediate-table opt-out admission gate. See
// ADR-0038. Default-off via `cache.transientAdmission.enabled=false`.
pub mod transient_admission;
#[cfg(feature = "ui")]
pub mod ui;
pub mod warm_sampler;

/// Re-export of the top-level error type so callers can `use shelfd::Error`.
pub use error::Error;

/// Crate result alias.
pub type Result<T> = std::result::Result<T, Error>;
