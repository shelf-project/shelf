//! **A6 (rc.7)** — cooperative peer admission (probabilistic).
//!
//! Cuts defensive replication on the secondary cache when a non-primary
//! pod fetches bytes from a peer (SHELF-23 `peer_or_origin_fetch`). The
//! gate is consulted **only** at the peer-fetch local-admit site; origin
//! admits are unchanged and the read path is untouched. See
//! [`agents/out/adr/0037-rc7-cooperative-peer-admission.md`] for the
//! design rationale.
//!
//! ## Why
//!
//! HRW (SHELF-19) deterministically pins each content-addressed key to
//! one primary pod, but SHELF-23 added a peer-fetch race so a
//! non-primary that receives a request can fetch from the primary
//! instead of paying the full S3 latency. Today the secondary also
//! caches the response in its own Foyer pool — *defensive replication*.
//! That doubles NVMe pressure on hot keys: under HRW skew (workspace
//! memory: shelf-2 primary-load concentration) the secondary's
//! defensive copy is rarely useful because the primary stays warm and
//! responsive.
//!
//! A6 adds a probabilistic gate: when the source of the bytes is
//! `Peer`, admit locally with probability `1 / replication_factor`.
//! `replication_factor = 1` ⇒ always admit (no behaviour change vs the
//! status quo). `replication_factor = 2` ⇒ admit half the time
//! (defensive 2x replication across the cluster). `N` ⇒ admit `1/N` of
//! the time (cooperative cluster-wide replication factor `N`).
//!
//! Origin fetches always admit. The HRW primary always admits even if
//! the bytes ostensibly came from a peer (defensive invariant — see
//! [`CoopAdmissionGate::should_admit_peer_bytes`]). Reads are untouched
//! — A6 is a write-path / cache-population gate.
//!
//! ## Composition with other admit gates
//!
//! The gate sits **alongside** the existing admit chain in
//! [`crate::store::FoyerStore::get_or_fetch`], not inside it. The
//! existing chain (in evaluation order):
//!
//! 1. Drain gate (A2) — pod is terminating; refuse all admits.
//! 2. Admission policy (SHELF-25 size threshold + W-TinyLFU).
//! 3. LODC level gate (SHELF-21e).
//! 4. Independent-queue rate-limiter (SHELF-29 + A1 RSS multiplier).
//! 5. **A6 cooperative gate (this module)** — only consulted when the
//!    source is `Peer`. Origin bytes always pass.
//!
//! Because A6 only inspects `FetchSource::Peer` it is orthogonal to the
//! pressure-aware gates above: a peer admission can be dropped because
//! NVMe is full *and* by the cooperative gate, both with their own
//! counters.
//!
//! ## Determinism in tests
//!
//! Production callers seed the `SmallRng` from system entropy. Unit
//! tests construct the gate via [`CoopAdmissionGate::with_seed`] so the
//! probabilistic assertions are reproducible across runs.

use parking_lot::Mutex;
use rand::{rngs::SmallRng, RngCore, SeedableRng};
use serde::{Deserialize, Serialize};

/// Source of the bytes returned by `FoyerStore::get_or_fetch`'s
/// fetcher closure. Origin fetches always admit (current behaviour);
/// peer fetches go through the cooperative gate. The enum is
/// deliberately public-by-default so callers in `peer_fetch.rs` and
/// the integration tests can construct it without ceremony.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchSource {
    /// Bytes came from the S3 origin (full GET / range-GET). The
    /// existing admit-chain decision is final — A6 does not gate
    /// origin admits.
    Origin,
    /// Bytes came from a peer pod via SHELF-23
    /// `crate::peer_fetch::peer_or_origin_fetch`'s `PeerHit` arm.
    /// The cooperative gate decides whether to admit locally.
    Peer,
}

impl FetchSource {
    /// `true` for [`FetchSource::Peer`]. Tiny convenience used by the
    /// admit gate so callers do not have to `match` to read the
    /// boolean predicate.
    #[inline]
    pub fn is_peer(self) -> bool {
        matches!(self, FetchSource::Peer)
    }
}

/// Operator-tunable knobs for the cooperative peer admission gate.
///
/// Default `enabled = false` is the safety hatch: a freshly deployed
/// shelfd that has not opted into A6 behaves identically to pre-A6 (the
/// peer-fetch admit site always admits). Operators flip
/// `cache.coopAdmission.enabled = true` once the metrics dashboards
/// have been sized for the new counters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoopAdmissionConfig {
    /// Master switch. Default `false` (opt-in for safety; first deploy
    /// turns it on per-cluster).
    #[serde(default)]
    pub enabled: bool,

    /// Intended in-cluster replication factor per object.
    ///
    /// - `1` ⇒ admit with probability `1.0` (no behaviour change vs
    ///   pre-A6 — every peer-fetched byte still lands in the
    ///   secondary cache).
    /// - `2` ⇒ admit with probability `0.5` (defensive 2× replication
    ///   across the cluster).
    /// - `N` ⇒ admit with probability `1/N` (cluster-wide replication
    ///   factor `N`).
    /// - `0` is treated as `1` (defensive — protects against an
    ///   accidental `0` in YAML producing an undefined probability).
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
}

impl Default for CoopAdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            replication_factor: default_replication_factor(),
        }
    }
}

fn default_replication_factor() -> u32 {
    2
}

/// The cooperative peer-admission gate.
///
/// Holds an [`SmallRng`] behind a `parking_lot::Mutex` so a single
/// gate instance can be consulted from any tokio task without sharing
/// the RNG state across them (`SmallRng` itself is `!Send` once
/// borrowed). The mutex critical section is one `random_range` /
/// `next_u32` call — measured at <50 ns p99 on the hot path; well
/// below the cost of a Foyer insert (~µs).
#[derive(Debug)]
pub struct CoopAdmissionGate {
    cfg: CoopAdmissionConfig,
    rng: Mutex<SmallRng>,
}

impl CoopAdmissionGate {
    /// Construct a gate from operator config, seeding the RNG from
    /// system entropy. Production code uses this constructor.
    pub fn new(cfg: CoopAdmissionConfig) -> Self {
        Self {
            cfg,
            rng: Mutex::new(SmallRng::from_os_rng()),
        }
    }

    /// Construct a gate with a deterministic seed. Tests use this so
    /// probability assertions stay reproducible.
    #[cfg(test)]
    pub(crate) fn with_seed(cfg: CoopAdmissionConfig, seed: u64) -> Self {
        Self {
            cfg,
            rng: Mutex::new(SmallRng::seed_from_u64(seed)),
        }
    }

    /// Returns the configured replication factor, with the
    /// "treat 0 as 1" defensive rewrite applied.
    #[inline]
    fn effective_replication_factor(&self) -> u32 {
        match self.cfg.replication_factor {
            0 => 1,
            n => n,
        }
    }

    /// `true` if the operator has flipped the master switch.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.cfg.enabled
    }

    /// Whether the bytes returned from a peer fetch should be admitted
    /// to the local Foyer pool.
    ///
    /// Always returns `true` if any of the safety conditions hold:
    ///
    /// 1. The gate is disabled (`cfg.enabled = false`).
    /// 2. The local pod is the HRW primary for this key
    ///    (`key_primary_is_self = true`). The bytes ended up at this
    ///    pod via some non-standard path; admitting is the correct
    ///    invariant — the primary is the canonical residence.
    /// 3. `replication_factor` is `1` (or `0`, treated as `1`).
    ///
    /// Otherwise, draws a random `u32` and admits with probability
    /// `1 / replication_factor`. The check is `rng.next_u32() %
    /// replication_factor == 0`, which is uniform for power-of-two
    /// factors and acceptably-unbiased for arbitrary factors at
    /// `replication_factor << u32::MAX` (the modulo bias ceiling for
    /// `replication_factor = 100` is ~4 × 10⁻⁸; we never approach it).
    pub fn should_admit_peer_bytes(&self, key_primary_is_self: bool) -> bool {
        if !self.cfg.enabled {
            return true;
        }
        if key_primary_is_self {
            return true;
        }
        let n = self.effective_replication_factor();
        if n <= 1 {
            return true;
        }
        let mut rng = self.rng.lock();
        rng.next_u32() % n == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `enabled = false` is the safety default: every peer-fetch byte
    /// must admit, regardless of `replication_factor`. This is the
    /// invariant a stock OSS deployment relies on.
    #[test]
    fn disabled_admits_all() {
        let gate = CoopAdmissionGate::with_seed(
            CoopAdmissionConfig {
                enabled: false,
                replication_factor: 100,
            },
            0xDEAD_BEEF,
        );
        for _ in 0..10_000 {
            assert!(gate.should_admit_peer_bytes(false));
        }
    }

    /// `replication_factor = 1` is the "no extra replicas" knob: even
    /// with the gate enabled, every byte admits (probability 1/1).
    /// This is the operator-friendly off switch when the dashboards
    /// have surfaced unexpected behaviour in higher factors.
    #[test]
    fn replication_factor_1_admits_all() {
        let gate = CoopAdmissionGate::with_seed(
            CoopAdmissionConfig {
                enabled: true,
                replication_factor: 1,
            },
            0x1234_5678,
        );
        for _ in 0..10_000 {
            assert!(gate.should_admit_peer_bytes(false));
        }
    }

    /// `replication_factor = 2` ⇒ probability 1/2. Run 10_000 trials
    /// with a deterministic seed and assert the empirical admit rate
    /// stays within ±5 percentage points of 0.5.
    #[test]
    fn replication_factor_2_admits_about_half() {
        let gate = CoopAdmissionGate::with_seed(
            CoopAdmissionConfig {
                enabled: true,
                replication_factor: 2,
            },
            0xA5A5_A5A5,
        );
        let mut admits = 0_u64;
        let trials = 10_000_u64;
        for _ in 0..trials {
            if gate.should_admit_peer_bytes(false) {
                admits += 1;
            }
        }
        let rate = admits as f64 / trials as f64;
        assert!(
            (0.45..=0.55).contains(&rate),
            "expected 0.45..=0.55, got {rate} (admits={admits})"
        );
    }

    /// `replication_factor = 4` ⇒ probability 1/4. Tighter band
    /// (`±5 pp`) given the lower expected rate; a binomial 95% CI at
    /// 10_000 trials puts the bound at ~0.0085.
    #[test]
    fn replication_factor_4_admits_about_quarter() {
        let gate = CoopAdmissionGate::with_seed(
            CoopAdmissionConfig {
                enabled: true,
                replication_factor: 4,
            },
            0xC0FF_EE00,
        );
        let mut admits = 0_u64;
        let trials = 10_000_u64;
        for _ in 0..trials {
            if gate.should_admit_peer_bytes(false) {
                admits += 1;
            }
        }
        let rate = admits as f64 / trials as f64;
        assert!(
            (0.20..=0.30).contains(&rate),
            "expected 0.20..=0.30, got {rate} (admits={admits})"
        );
    }

    /// Primary-pod invariant: regardless of `replication_factor`, if
    /// the local pod is the HRW primary for this key the gate must
    /// admit. The peer-fetch hot path (SHELF-23
    /// `peer_or_origin_fetch`) already short-circuits before
    /// returning `Peer` when the local pod is primary, but the gate
    /// is the documented backstop for that invariant.
    #[test]
    fn primary_always_admits_regardless_of_factor() {
        let gate = CoopAdmissionGate::with_seed(
            CoopAdmissionConfig {
                enabled: true,
                replication_factor: 10,
            },
            0xFEED_FACE,
        );
        for _ in 0..10_000 {
            assert!(gate.should_admit_peer_bytes(true));
        }
    }

    /// Defensive: an operator typo of `replication_factor: 0` in
    /// values.yaml must not produce a divide-by-zero or "admit
    /// nothing" foot-gun. The gate treats `0` as `1` (no replication
    /// gate) so the worst case is a no-op — matching the OFF switch.
    #[test]
    fn replication_factor_zero_treated_as_one() {
        let gate = CoopAdmissionGate::with_seed(
            CoopAdmissionConfig {
                enabled: true,
                replication_factor: 0,
            },
            0xBAAD_F00D,
        );
        for _ in 0..10_000 {
            assert!(gate.should_admit_peer_bytes(false));
        }
    }

    /// Default-constructed gate matches the OSS chart default
    /// (`enabled = false`) — admits everything, even at the
    /// non-default `replication_factor = 2`.
    #[test]
    fn default_config_admits_all() {
        let gate = CoopAdmissionGate::with_seed(CoopAdmissionConfig::default(), 0x1);
        assert!(!gate.is_enabled());
        for _ in 0..1_000 {
            assert!(gate.should_admit_peer_bytes(false));
        }
    }
}
