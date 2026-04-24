//! Control-plane surface for `shelfctl` + Grafana scraping.
//!
//! Ticket ownership:
//! - SHELF-23 — `shelfctl` subcommands (`stats`, `pin`, `evict`,
//!   `ring`, `reload`) route through this module.
//! - SHELF-24 — `reload pin-list` raises SIGHUP → pin-list loader.
//! - SHELF-08 — Prometheus `/metrics` is served here (kept off the
//!   data plane so a hot-loop client cannot starve metrics scrapes).
//! - SHELF-20 — `/stats` returns capacity + used bytes used by the
//!   plugin's HRW weighting.
//!
//! The control plane is HTTP-first for v1; a `tonic` gRPC service is
//! scaffolded here so SHELF-23 can drop in the proto without churning
//! callers. ADR-0004 scopes HTTP/2 only for the data plane — the
//! control plane may accept HTTP/1.1 for kubectl-style probes.

use std::net::SocketAddr;
use std::sync::Arc;

/// Handle to the live pin-list reloader. `reload()` sends a SIGHUP-
/// equivalent to the owner task.
#[derive(Debug, Clone, Default)]
pub struct PinListReloadHandle {
    _private: (),
}

impl PinListReloadHandle {
    /// Trigger an out-of-band pin list reload.
    pub fn reload(&self) -> crate::Result<()> {
        todo!(
            "SHELF-24: control: signal the pin-list reloader task; see \
             03-plan.md §4 SHELF-24"
        )
    }
}

/// Stats payload returned by `GET /stats`. The plugin polls this when
/// building HRW weights (SHELF-20); the key set is the contract
/// Agent 5 consumes, so changes here must be coordinated.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Stats {
    /// Pod identity (StatefulSet name, e.g. `shelf-2`).
    pub pod_id: String,
    /// Sum of both pools' capacity.
    pub capacity_bytes: u64,
    /// Sum of both pools' current usage.
    pub used_bytes: u64,
    /// DRAM-only metadata pool (Iceberg manifests + Parquet footers).
    pub metadata_pool: PoolStats,
    /// Hybrid DRAM + NVMe row-group pool.
    pub rowgroup_pool: PoolStats,
    /// SHELF-24: sum of resident byte length of every pinned key
    /// across both pools. Unresident pinned keys contribute zero.
    #[serde(default)]
    pub pinned_bytes: u64,
    /// SHELF-24: number of distinct pinned keys, regardless of
    /// residency.
    #[serde(default)]
    pub pinned_count: usize,
}

/// Per-pool capacity / usage section of [`Stats`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PoolStats {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
}

/// Serve the control plane (HTTP + gRPC stub).
pub async fn serve(
    _addr: SocketAddr,
    _reload: PinListReloadHandle,
    _store: Arc<crate::store::FoyerStore>,
    _shutdown: tokio_util::sync::CancellationToken,
) -> crate::Result<()> {
    todo!(
        "SHELF-23: control: serve /stats + /metrics + admin gRPC \
         (pin/unpin/evict/reload) on addr; see 03-plan.md §4 SHELF-23"
    )
}
