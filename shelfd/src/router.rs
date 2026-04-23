//! Rendezvous (HRW) routing for Shelf.
//!
//! Ticket ownership:
//! - SHELF-19 — `shelf-hashring` Rust crate + `ShelfHashRing` Java
//!   class. The Rust side lives here until the function stabilises,
//!   then it moves into its own crate so the plugin can depend on it
//!   without pulling in tokio/foyer.
//! - SHELF-20 — membership glue: `Router::update(members)` is called
//!   by the DNS-refresh loop.
//!
//! Reference: `agents/out/adr/0002-hrw-hashing-over-vnode-ring.md`.
//!
//! The owner function is:
//!
//! ```text
//! owner(key) = argmax_node ( weight(node) /
//!                            -ln( (sha256(key || node_id) as u64)
//!                                  / max_u64 ) )
//! ```
//!
//! Golden-vector tests will live in `tests/hashring_golden.rs` and in
//! the Java side's `ShelfHashRingTest`. Both must agree byte-identically
//! on 10 k random inputs (see SHELF-19 acceptance criteria).

use parking_lot::RwLock;
use std::sync::Arc;

/// A single peer pod in the ring.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Member {
    /// Stable pod identity (e.g. `shelf-2`).
    pub id: String,
    /// Network address (e.g. `10.0.1.17:9090`).
    pub endpoint: String,
    /// Capacity weight. 1 = default, 2 = twice the expected load.
    pub weight: u32,
}

/// Hash-ring view owned by `Router`. Cheap to clone — reads are
/// lock-free via `Arc`; writes take a single short critical section.
#[derive(Debug, Clone)]
pub struct RingView {
    members: Arc<Vec<Member>>,
}

impl Default for RingView {
    fn default() -> Self {
        Self {
            members: Arc::new(Vec::new()),
        }
    }
}

impl RingView {
    pub fn members(&self) -> &[Member] {
        &self.members
    }
}

/// HRW router. Holds the current ring view behind an `RwLock`; all hot
/// reads clone the `Arc<Vec<Member>>` in O(1).
#[derive(Debug, Default)]
pub struct Router {
    view: RwLock<RingView>,
}

impl Router {
    /// Construct an empty router. `update` is expected to be called by
    /// the membership resolver before the first `route` call.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the ring with a new membership set.
    pub fn update(&self, members: Vec<Member>) {
        let mut view = self.view.write();
        view.members = Arc::new(members);
    }

    /// Current view (cheap clone of the `Arc`).
    pub fn view(&self) -> RingView {
        self.view.read().clone()
    }

    /// Return the pod that owns `key`.
    ///
    /// The real implementation uses capacity-weighted HRW per ADR-0002.
    /// This scaffold intentionally does not return `Option` so the
    /// caller shape is stable once the function lands.
    pub fn owner(&self, _key: &[u8]) -> Member {
        todo!(
            "SHELF-19: router: implement capacity-weighted HRW over \
             view.members; see 03-plan.md §4 SHELF-19 and \
             agents/out/adr/0002-hrw-hashing-over-vnode-ring.md"
        )
    }

    /// Whether this pod should serve `key` (HRW says we are the owner).
    /// Scaffold: always false until SHELF-19 lands.
    pub fn is_local_owner(&self, _self_id: &str, _key: &[u8]) -> bool {
        todo!(
            "SHELF-19: router: compare owner(key).id to self_id; \
             see 03-plan.md §4 SHELF-19"
        )
    }
}
