//! Control-plane wire types for `shelfctl` + Grafana scraping.
//!
//! ## Module shape (post-SHELF-23)
//!
//! This module is now **thin** — it only carries the on-the-wire
//! payload types ([`Stats`], [`PoolStats`]) that `GET /stats`
//! returns. The actual control-plane HTTP server (`/healthz`,
//! `/readyz`, `/metrics`, `/stats`, `/admin/*`) was folded into the
//! data-plane Axum router and now lives at
//! [`crate::http::serve`]; the pin-list reload signal is wired via
//! [`crate::pinlist::ReloadHandle::reload_now`], driven by the
//! `POST /admin/reload` handler in `http.rs` and the SIGHUP path in
//! `main.rs`.
//!
//! Anything below the type definitions is preserved only as a
//! `#[deprecated]` shim returning [`crate::Error::Unimplemented`] so
//! external callers of the older public surface get a structured
//! error rather than the previous panic. The shim will be removed
//! in v1.1.
//!
//! Ticket history:
//! - SHELF-23 — `shelfctl` subcommands (`stats`, `pin`, `evict`,
//!   `ring`, `reload`) now route through `http.rs`.
//! - SHELF-24 — `reload pin-list` is implemented by
//!   `crate::pinlist::ReloadHandle::reload_now`.
//! - SHELF-08 — Prometheus `/metrics` is served by `http.rs`.
//! - SHELF-20 — `/stats` returns capacity + used bytes used by the
//!   plugin's HRW weighting; this module owns the wire types only.

/// Handle to the live pin-list reloader.
///
/// **Deprecated**: this type predates the real loader in
/// [`crate::pinlist`]. Use [`crate::pinlist::ReloadHandle`] (acquired
/// from `PinListLoader::boot_and_spawn`) instead.
#[derive(Debug, Clone, Default)]
#[deprecated(
    since = "0.1.0",
    note = "use crate::pinlist::ReloadHandle (wired in main.rs); this shim will be removed in v1.1"
)]
pub struct PinListReloadHandle {
    _private: (),
}

#[allow(deprecated)]
impl PinListReloadHandle {
    /// Trigger an out-of-band pin list reload.
    ///
    /// **Deprecated**: returns [`crate::Error::Unimplemented`]. The
    /// actual reload signal is exposed by
    /// [`crate::pinlist::ReloadHandle::reload_now`], driven by
    /// `POST /admin/reload` in `crate::http` and the SIGHUP path in
    /// `main.rs`.
    #[deprecated(
        since = "0.1.0",
        note = "use crate::pinlist::ReloadHandle::reload_now; this shim will be removed in v1.1"
    )]
    pub fn reload(&self) -> crate::Result<()> {
        Err(crate::Error::Unimplemented(
            "control::PinListReloadHandle::reload moved to crate::pinlist::ReloadHandle::reload_now \
             (driven by POST /admin/reload in crate::http and the SIGHUP path in main.rs); \
             will be removed in v1.1"
                .to_string(),
        ))
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
    /// SHELF-20: this pod is in lameduck mode and should be removed
    /// from peers' rings on their next refresh. The local data plane
    /// continues to serve in-flight reads until shutdown completes;
    /// only **routing** is steered away.
    ///
    /// `#[serde(default)]` keeps the `/stats` wire compatible with
    /// pre-SHELF-20 clients (e.g. `shelfctl stats` from a v0.4 build):
    /// missing field => `false` => peer is healthy.
    #[serde(default)]
    pub draining: bool,
    /// RC6 P1.2 — process resident-set size in bytes. Populated by
    /// [`crate::capacity_check::read_self_rss_bytes`]; consumed by
    /// the `/admin/cap-ready` cluster-capacity gate. `0` on
    /// non-Linux dev hosts (and on any platform where we couldn't
    /// read `/proc/self/status`); `#[serde(default)]` keeps the
    /// wire compatible with pre-RC6 peers, which simply contribute
    /// `0` to the max-RSS aggregation (treated as "no signal" by
    /// the gate, not as "healthy").
    #[serde(default)]
    pub rss_bytes: u64,
}

/// Per-pool capacity / usage section of [`Stats`].
///
/// SHELF-17 keeps `metadata` DRAM-only so the two disk fields are
/// `0` there by definition. SHELF-18 adds the disk tier to
/// `rowgroup`; both fields use `#[serde(default)]` so old clients
/// that never request them (and the metadata serialization path)
/// stay byte-compatible with pre-SHELF-18 payloads.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PoolStats {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    /// Bytes held on the NVMe tier. See
    /// [`crate::store::FoyerStore::disk_bytes_used`] for the
    /// best-effort approximation the daemon reports.
    #[serde(default)]
    pub disk_used_bytes: u64,
    /// Configured NVMe capacity (`pools.<pool>.nvme_bytes`). `0`
    /// when the pool runs DRAM-only.
    #[serde(default)]
    pub disk_capacity_bytes: u64,
}
