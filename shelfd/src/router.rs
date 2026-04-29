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
//! ## Scoring function
//!
//! ```text
//! score(key, node) = weight(node) / -ln(x)
//! where
//!     h      = sha256(key || node_id)
//!     u64_be = u64::from_be_bytes(h[0..8])
//!     // Use only the top 53 bits so the f64 conversion is exact and
//!     // cross-language reproducible (Java and Rust agree byte-for-byte
//!     // on IEEE-754 `double` for integers that fit in the mantissa).
//!     top53  = u64_be >> 11
//!     x      = (top53 as f64) / ((1u64 << 53) as f64)   in [0, 1)
//! owner(key) = argmax_node score(key, node)
//! ```
//!
//! Ties are broken by lexicographically-smaller `podId`, which keeps the
//! Java + Rust decision deterministic even in the astronomically unlikely
//! case of identical f64 scores.
//!
//! Golden-vector tests in `shelfd/tests/fixtures/hrw_golden_vectors.txt`
//! list the expected owner for 1000 deterministically-generated keys
//! against a fixed three-node ring with weights {1, 2, 3}. The same
//! fixture is consumed by `io.shelf.client.HashRingTest` on the Java
//! side so any drift between implementations is caught in CI.

use parking_lot::RwLock;
use sha2::{Digest, Sha256};
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
    /// Capacity-weighted HRW per ADR-0002.
    ///
    /// # Panics
    /// Panics if the ring is empty — empty rings are a config error, not
    /// a recoverable runtime condition.
    pub fn owner(&self, key: &[u8]) -> Member {
        let view = self.view.read();
        owner_in(&view.members, key).cloned().expect(
            "Router::owner called on an empty ring; call update() with at least one member first",
        )
    }

    /// Whether this pod should serve `key` (HRW says we are the owner).
    ///
    /// Returns `false` for empty rings so the server can degrade to a
    /// "forward every request" state during startup instead of crashing.
    pub fn is_local_owner(&self, self_id: &str, key: &[u8]) -> bool {
        let view = self.view.read();
        match owner_in(&view.members, key) {
            Some(m) => m.id == self_id,
            None => false,
        }
    }
}

/// HRW lookup over an explicit member slice. Crate-private — `peer_fetch`
/// uses this to take the empty-check and the owner lookup against the
/// **same** `RingView` snapshot, avoiding the `Router::owner` panic path
/// when membership flips between two separate reads.
pub(crate) fn owner_in<'a>(members: &'a [Member], key: &[u8]) -> Option<&'a Member> {
    let mut best: Option<&'a Member> = None;
    let mut best_score = f64::NEG_INFINITY;
    for m in members {
        let score = hrw_score(key, m);
        let take = match best {
            None => true,
            Some(b) => score > best_score || (score == best_score && m.id < b.id),
        };
        if take {
            best_score = score;
            best = Some(m);
        }
    }
    best
}

/// The capacity-weighted HRW score defined in ADR-0002, using a 53-bit
/// mantissa so the intermediate `x` is cross-language reproducible.
pub fn hrw_score(key: &[u8], member: &Member) -> f64 {
    let mut h = Sha256::new();
    h.update(key);
    h.update(member.id.as_bytes());
    let digest = h.finalize();

    let u64_be = u64::from_be_bytes(digest[..8].try_into().expect("sha256 emits >= 8 bytes"));
    let top53 = u64_be >> 11;
    let x = (top53 as f64) / ((1u64 << 53) as f64);

    if x <= 0.0 {
        return f64::INFINITY;
    }
    let neg_ln = -x.ln();
    (member.weight as f64) / neg_ln
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn ring3() -> Vec<Member> {
        vec![
            Member {
                id: "shelf-0".into(),
                endpoint: "10.0.0.10:9090".into(),
                weight: 1,
            },
            Member {
                id: "shelf-1".into(),
                endpoint: "10.0.0.11:9090".into(),
                weight: 2,
            },
            Member {
                id: "shelf-2".into(),
                endpoint: "10.0.0.12:9090".into(),
                weight: 3,
            },
        ]
    }

    /// Deterministic keys shared with the Java side: key_i = sha256("shelf-hrw-golden-v1" || le_u32(i)).
    fn golden_key(i: u32) -> Vec<u8> {
        let mut h = Sha256::new();
        h.update(b"shelf-hrw-golden-v1");
        h.update(i.to_le_bytes());
        h.finalize().to_vec()
    }

    fn fixture_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("hrw_golden_vectors.txt")
    }

    #[test]
    fn owner_matches_golden_vectors() {
        let router = Router::new();
        router.update(ring3());

        let fixture = fs::read_to_string(fixture_path()).expect(
            "missing hrw_golden_vectors.txt; regenerate with `SHELF_REGEN_FIXTURES=1 cargo test -p shelfd hrw_`",
        );

        let mut regen = std::env::var("SHELF_REGEN_FIXTURES").ok().as_deref() == Some("1");
        let mut expected_lines: Vec<String> = fixture
            .lines()
            .filter(|l| !l.trim().is_empty() && !l.trim_start().starts_with('#'))
            .map(|s| s.to_string())
            .collect();

        if expected_lines.len() != 1000 {
            regen = true;
        }

        let mut fresh = Vec::with_capacity(1000);
        for i in 0..1000u32 {
            let key = golden_key(i);
            let owner = router.owner(&key);
            fresh.push(format!("{i}\t{}", owner.id));
        }

        if regen {
            let header = "# Generated by shelfd::router::tests::owner_matches_golden_vectors.\n# Format: <counter_index>\\t<expected_owner_pod_id>\n# Ring: {shelf-0 w=1, shelf-1 w=2, shelf-2 w=3}\n# Keys: key_i = sha256(\"shelf-hrw-golden-v1\" || le_u32(i)), i in [0, 1000).\n";
            let body = fresh.join("\n");
            fs::write(fixture_path(), format!("{header}{body}\n")).expect("write fixture");
            expected_lines = fresh.clone();
        }

        assert_eq!(
            fresh, expected_lines,
            "HRW owner decisions drifted vs. golden fixture; re-run with SHELF_REGEN_FIXTURES=1 \
             only after confirming the Java side still agrees."
        );
    }

    #[test]
    fn owner_is_stable_when_unrelated_member_joins() {
        let router = Router::new();
        router.update(ring3());
        let before: Vec<String> = (0..200u32)
            .map(|i| router.owner(&golden_key(i)).id.clone())
            .collect();

        let mut bigger = ring3();
        bigger.push(Member {
            id: "shelf-3".into(),
            endpoint: "10.0.0.13:9090".into(),
            weight: 1,
        });
        router.update(bigger);

        let after: Vec<String> = (0..200u32)
            .map(|i| router.owner(&golden_key(i)).id.clone())
            .collect();

        let moved = before
            .iter()
            .zip(after.iter())
            .filter(|(a, b)| a != b)
            .count();
        // New node's share is ~weight / sum_weights = 1/7 ≈ 14 %. Over 200
        // keys we expect ~28 moves; allow a generous sigma envelope.
        assert!(
            moved <= 60,
            "HRW rebalance moved too many keys on single-member add: {moved} of 200"
        );
    }

    #[test]
    fn heavier_node_wins_more_often() {
        let router = Router::new();
        router.update(ring3());
        let mut counts = std::collections::HashMap::<String, usize>::new();
        for i in 0..3000u32 {
            *counts
                .entry(router.owner(&golden_key(i)).id.clone())
                .or_insert(0) += 1;
        }
        let c0 = *counts.get("shelf-0").unwrap_or(&0);
        let c1 = *counts.get("shelf-1").unwrap_or(&0);
        let c2 = *counts.get("shelf-2").unwrap_or(&0);
        assert!(
            c0 < c1,
            "shelf-0 (w=1) should see fewer keys than shelf-1 (w=2); got {c0} vs {c1}"
        );
        assert!(
            c1 < c2,
            "shelf-1 (w=2) should see fewer keys than shelf-2 (w=3); got {c1} vs {c2}"
        );
    }

    #[test]
    fn is_local_owner_agrees_with_owner() {
        let router = Router::new();
        router.update(ring3());
        for i in 0..50u32 {
            let key = golden_key(i);
            let winner = router.owner(&key).id;
            for node_id in ["shelf-0", "shelf-1", "shelf-2"] {
                assert_eq!(
                    router.is_local_owner(node_id, &key),
                    node_id == winner,
                    "disagreement for key {i}"
                );
            }
        }
    }

    #[test]
    fn empty_ring_is_not_local_owner() {
        let router = Router::new();
        assert!(!router.is_local_owner("shelf-0", b"any"));
    }

    #[test]
    #[should_panic(expected = "empty ring")]
    fn owner_panics_on_empty_ring() {
        let router = Router::new();
        let _ = router.owner(b"any");
    }
}
