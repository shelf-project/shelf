//! Admission policy for `shelfd`.
//!
//! Ticket ownership:
//! - SHELF-25 — size-threshold admission per ADR-0003. Refuse inserts
//!   for objects > 1 GiB unless the key is in the pin list.
//! - SHELF-24 — `PinList` lookup (reloaded from S3 on SIGHUP / 15 min).
//! - Phase 4 (SHELF-4x) — optional LightGBM escape hatch. Only
//!   shipped if it adds ≥ 5 pp hit rate over size-threshold on the
//!   `trino_logs` replay harness (SHELF-26).
//!
//! References:
//! - `agents/out/adr/0003-size-threshold-admission-over-onnx-mlp.md`
//! - `agents/out/adr/0010-v05-gate-beat-alluxio-on-rep2.md` — the
//!   kill-switch metric this policy directly affects (hit rate).

use std::fmt::Debug;

/// Decision returned by the policy.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AdmissionDecision {
    /// Insert into the Foyer pool.
    Admit,
    /// Serve the bytes to the client but do not insert.
    Reject,
}

/// Context the policy inspects before admitting an object.
#[derive(Debug, Clone)]
pub struct AdmissionContext<'a> {
    pub pool: crate::store::Pool,
    pub key: &'a crate::store::Key,
    pub size_bytes: u64,
    /// Whether the key is in the pin list (see `PinList::contains`).
    pub pinned: bool,
}

/// The admission policy interface.
///
/// Kept sync + lock-free so the HTTP hot path can call it without
/// awaiting. The pin list is held behind `arc-swap` and refreshed
/// off-path (SHELF-24).
pub trait AdmissionPolicy: Send + Sync + Debug + 'static {
    fn decide(&self, ctx: &AdmissionContext<'_>) -> AdmissionDecision;
}

/// Size-threshold policy: admit everything ≤ `size_threshold_bytes`,
/// plus anything pinned if `pinned_bypass` is true.
#[derive(Debug, Clone)]
pub struct SizeThresholdPolicy {
    pub size_threshold_bytes: u64,
    pub pinned_bypass: bool,
}

impl SizeThresholdPolicy {
    pub fn from_config(cfg: &crate::config::AdmissionConfig) -> Self {
        Self {
            size_threshold_bytes: cfg.size_threshold_bytes,
            pinned_bypass: cfg.pinned_bypass,
        }
    }
}

impl AdmissionPolicy for SizeThresholdPolicy {
    fn decide(&self, ctx: &AdmissionContext<'_>) -> AdmissionDecision {
        if ctx.size_bytes > self.size_threshold_bytes {
            if self.pinned_bypass && ctx.pinned {
                AdmissionDecision::Admit
            } else {
                AdmissionDecision::Reject
            }
        } else {
            AdmissionDecision::Admit
        }
    }
}

/// Pin list — reloaded from S3 (SHELF-24). Phase-0 exposes only the
/// lookup surface with an empty set; the S3-backed loader lands with
/// SHELF-24.
#[derive(Debug, Default)]
pub struct PinList {
    _private: (),
}

impl PinList {
    /// Whether `key` belongs to a pinned table + partition combination.
    /// Phase-0 always returns `false` (SHELF-24 will swap the body for
    /// a real lookup).
    pub fn contains(&self, _key: &crate::store::Key) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{key_from_tuple, Pool};

    fn ctx(size: u64, pinned: bool) -> AdmissionContext<'static> {
        static KEY: once_cell::sync::Lazy<crate::store::Key> =
            once_cell::sync::Lazy::new(|| key_from_tuple(b"etag", 0, 1, 0).unwrap());
        AdmissionContext {
            pool: Pool::RowGroup,
            key: &KEY,
            size_bytes: size,
            pinned,
        }
    }

    #[test]
    fn admits_below_threshold() {
        let policy = SizeThresholdPolicy {
            size_threshold_bytes: 1024,
            pinned_bypass: true,
        };
        assert_eq!(policy.decide(&ctx(512, false)), AdmissionDecision::Admit);
        assert_eq!(policy.decide(&ctx(1024, false)), AdmissionDecision::Admit);
    }

    #[test]
    fn rejects_above_threshold() {
        let policy = SizeThresholdPolicy {
            size_threshold_bytes: 1024,
            pinned_bypass: true,
        };
        assert_eq!(policy.decide(&ctx(1025, false)), AdmissionDecision::Reject);
    }

    #[test]
    fn pinned_bypasses_threshold_when_enabled() {
        let policy = SizeThresholdPolicy {
            size_threshold_bytes: 1024,
            pinned_bypass: true,
        };
        assert_eq!(policy.decide(&ctx(1 << 30, true)), AdmissionDecision::Admit);
    }

    #[test]
    fn pinned_does_not_bypass_when_disabled() {
        let policy = SizeThresholdPolicy {
            size_threshold_bytes: 1024,
            pinned_bypass: false,
        };
        assert_eq!(
            policy.decide(&ctx(1 << 30, true)),
            AdmissionDecision::Reject
        );
    }
}
