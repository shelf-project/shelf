//! DNS / headless-service membership resolver.
//!
//! Ticket ownership:
//! - SHELF-20 — resolve `shelf.shelf.svc.cluster.local` every 5 s,
//!   poll each pod's `/stats` for capacity weights, push results into
//!   `router::Router::update`. No Raft per ADR-0001.
//! - Phase 3 (SHELF-3x) — chaos-drill robustness, KEDA rotation test.
//!
//! References:
//! - `agents/out/adr/0001-no-embedded-raft.md`
//! - `agents/out/adr/0002-hrw-hashing-over-vnode-ring.md`

use std::sync::Arc;
use std::time::Duration;

use crate::router::Router;

/// Membership resolver handle. Drives a background task that refreshes
/// the ring periodically.
#[derive(Debug)]
pub struct Resolver {
    _private: (),
}

/// Configuration carried into the resolver task.
#[derive(Debug, Clone)]
pub struct ResolverConfig {
    pub headless_service: String,
    pub dns_refresh: Duration,
    pub self_id: String,
}

impl Resolver {
    /// Spawn the membership-resolver task. The returned handle owns
    /// its `JoinHandle` internally so nothing escapes silently per
    /// agents/4-shelfd-builder.md Pass 2 ("No `tokio::spawn` without
    /// an owner that tracks its `JoinHandle`").
    pub fn spawn(
        _config: ResolverConfig,
        _router: Arc<Router>,
        _shutdown: tokio_util::sync::CancellationToken,
    ) -> crate::Result<Self> {
        todo!(
            "SHELF-20: membership: resolve K8s headless service every \
             `dns_refresh` seconds, poll /stats for capacity weights, \
             call router.update(members); see 03-plan.md §4 SHELF-20 \
             and adr/0001"
        )
    }
}
