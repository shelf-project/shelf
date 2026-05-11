//! Admission policy for `shelfd`.
//!
//! Ticket ownership:
//! - SHELF-25 — size-threshold admission per ADR-0003. Refuse inserts
//!   for objects > 1 GiB unless the key is in the pin list.
//! - SHELF-24 — Pinned keys bypass admission via
//!   [`crate::store::FoyerStore::is_pinned`] (fed by [`crate::pinlist`]
//!   S3 reload on boot, timer, SIGHUP, and `/admin/reload`).
//! - Daily ops loop — frequency-sketch admission for the rowgroup pool
//!   (Count–Min style) so one-shot cold scans do not flood the LODC
//!   queue; keys must reach `frequency_min_hits` observations before
//!   NVMe insert (metadata pool skips frequency gate).
//!
//! References:
//! - `agents/out/adr/0003-size-threshold-admission-over-onnx-mlp.md`
//! - `agents/out/adr/0010-v05-gate-beat-alluxio-on-rep2.md` — the
//!   kill-switch metric this policy directly affects (hit rate).

use std::fmt::Debug;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::config::{AdmissionConfig, AdmissionPolicyKind};
use crate::store::Key;

/// Decision returned by the policy.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AdmissionDecision {
    /// Insert into the Foyer pool.
    Admit,
    /// Serve the bytes to the client but do not insert.
    Reject,
}

/// Sub-reason when [`AdmissionDecision::Reject`] — drives
/// `shelf_admissions_total{decision="reject_*"}` in `store.rs`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AdmissionRejectKind {
    ObjectTooLarge,
    FrequencyBelowThreshold,
    Other,
}

/// Context the policy inspects before admitting an object.
#[derive(Debug, Clone)]
pub struct AdmissionContext<'a> {
    pub pool: crate::store::Pool,
    pub key: &'a crate::store::Key,
    pub size_bytes: u64,
    /// Whether the key is in the pin set (see `FoyerStore::is_pinned`).
    pub pinned: bool,
}

/// The admission policy interface.
///
/// Kept sync so the HTTP hot path can call it without awaiting.
pub trait AdmissionPolicy: Send + Sync + Debug + 'static {
    fn decide(&self, ctx: &AdmissionContext<'_>) -> (AdmissionDecision, AdmissionRejectKind);
}

/// Build the production policy from operator config.
pub fn build_admission_policy(cfg: &AdmissionConfig) -> Arc<dyn AdmissionPolicy> {
    match cfg.policy {
        AdmissionPolicyKind::SizeThreshold => Arc::new(SizeThresholdPolicy::from_config(cfg)),
        AdmissionPolicyKind::Frequency => Arc::new(CompositeAdmissionPolicy::from_config(cfg)),
    }
}

/// Size-threshold policy: admit everything ≤ `size_threshold_bytes`,
/// plus anything pinned if `pinned_bypass` is true.
#[derive(Debug, Clone)]
pub struct SizeThresholdPolicy {
    pub size_threshold_bytes: u64,
    pub pinned_bypass: bool,
}

impl SizeThresholdPolicy {
    pub fn from_config(cfg: &AdmissionConfig) -> Self {
        Self {
            size_threshold_bytes: cfg.size_threshold_bytes,
            pinned_bypass: cfg.pinned_bypass,
        }
    }
}

impl AdmissionPolicy for SizeThresholdPolicy {
    fn decide(&self, ctx: &AdmissionContext<'_>) -> (AdmissionDecision, AdmissionRejectKind) {
        if ctx.size_bytes > self.size_threshold_bytes {
            if self.pinned_bypass && ctx.pinned {
                (AdmissionDecision::Admit, AdmissionRejectKind::Other)
            } else {
                (AdmissionDecision::Reject, AdmissionRejectKind::ObjectTooLarge)
            }
        } else {
            (AdmissionDecision::Admit, AdmissionRejectKind::Other)
        }
    }
}

/// Size threshold, then optional frequency sketch for `rowgroup` pool.
#[derive(Debug)]
pub struct CompositeAdmissionPolicy {
    size: SizeThresholdPolicy,
    frequency: FrequencySketchAdmission,
}

impl CompositeAdmissionPolicy {
    pub fn from_config(cfg: &AdmissionConfig) -> Self {
        Self {
            size: SizeThresholdPolicy::from_config(cfg),
            frequency: FrequencySketchAdmission::new(cfg.frequency_min_hits.max(1)),
        }
    }
}

impl AdmissionPolicy for CompositeAdmissionPolicy {
    fn decide(&self, ctx: &AdmissionContext<'_>) -> (AdmissionDecision, AdmissionRejectKind) {
        let (d, k) = self.size.decide(ctx);
        if d == AdmissionDecision::Reject {
            return (d, k);
        }
        if ctx.pinned || ctx.pool == crate::store::Pool::Metadata {
            return (AdmissionDecision::Admit, AdmissionRejectKind::Other);
        }
        self.frequency.decide_rowgroup(ctx)
    }
}

/// Count–Min sketch (4 × 2048 × u32) — frequency estimate for cache keys.
///
/// Implements the same *frequency-estimation* role TinyLFU admission needs;
/// avoids coupling to the `tinyufo` crate’s full cache API (pingora TinyUFO
/// is an integrated policy + storage structure, not a drop-in sketch-only dep).
/// collisions, which is safe for admission (only admits *more* eagerly).
#[derive(Debug)]
pub struct FrequencySketchAdmission {
    min_hits: u32,
    /// Row-major: index `row * COLS + col`.
    cells: Vec<AtomicU32>,
}

const CMS_ROWS: usize = 4;
const CMS_COLS: usize = 2048;

impl FrequencySketchAdmission {
    pub fn new(min_hits: u32) -> Self {
        let len = CMS_ROWS * CMS_COLS;
        let mut cells = Vec::with_capacity(len);
        for _ in 0..len {
            cells.push(AtomicU32::new(0));
        }
        Self { min_hits, cells }
    }

    fn row_hash(key: &Key, row: usize) -> usize {
        let k = key.0;
        let mut h: u64 = u64::from_le_bytes(k[0..8].try_into().unwrap_or([0u8; 8]));
        h ^= (row as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        h = h.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        (h as usize) % CMS_COLS
    }

    fn increment_and_estimate(&self, key: &Key) -> u32 {
        let mut min_seen = u32::MAX;
        for row in 0..CMS_ROWS {
            let col = Self::row_hash(key, row);
            let idx = row * CMS_COLS + col;
            let v = self.cells[idx].fetch_add(1, Ordering::Relaxed).wrapping_add(1);
            min_seen = min_seen.min(v);
        }
        min_seen
    }

    fn decide_rowgroup(
        &self,
        ctx: &AdmissionContext<'_>,
    ) -> (AdmissionDecision, AdmissionRejectKind) {
        let est = self.increment_and_estimate(ctx.key);
        if est >= self.min_hits {
            (AdmissionDecision::Admit, AdmissionRejectKind::Other)
        } else {
            (AdmissionDecision::Reject, AdmissionRejectKind::FrequencyBelowThreshold)
        }
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
        assert_eq!(
            policy.decide(&ctx(512, false)),
            (AdmissionDecision::Admit, AdmissionRejectKind::Other)
        );
        assert_eq!(
            policy.decide(&ctx(1024, false)),
            (AdmissionDecision::Admit, AdmissionRejectKind::Other)
        );
    }

    #[test]
    fn rejects_above_threshold() {
        let policy = SizeThresholdPolicy {
            size_threshold_bytes: 1024,
            pinned_bypass: true,
        };
        assert_eq!(
            policy.decide(&ctx(1025, false)),
            (
                AdmissionDecision::Reject,
                AdmissionRejectKind::ObjectTooLarge
            )
        );
    }

    #[test]
    fn pinned_bypasses_threshold_when_enabled() {
        let policy = SizeThresholdPolicy {
            size_threshold_bytes: 1024,
            pinned_bypass: true,
        };
        assert_eq!(
            policy.decide(&ctx(1 << 30, true)),
            (AdmissionDecision::Admit, AdmissionRejectKind::Other)
        );
    }

    #[test]
    fn pinned_does_not_bypass_when_disabled() {
        let policy = SizeThresholdPolicy {
            size_threshold_bytes: 1024,
            pinned_bypass: false,
        };
        assert_eq!(
            policy.decide(&ctx(1 << 30, true)),
            (
                AdmissionDecision::Reject,
                AdmissionRejectKind::ObjectTooLarge
            )
        );
    }

    #[test]
    fn frequency_rejects_first_admit_second_same_key() {
        let policy = CompositeAdmissionPolicy {
            size: SizeThresholdPolicy {
                size_threshold_bytes: 1 << 30,
                pinned_bypass: true,
            },
            frequency: FrequencySketchAdmission::new(2),
        };
        assert_eq!(
            policy.decide(&ctx(100, false)),
            (
                AdmissionDecision::Reject,
                AdmissionRejectKind::FrequencyBelowThreshold
            )
        );
        assert_eq!(
            policy.decide(&ctx(100, false)),
            (AdmissionDecision::Admit, AdmissionRejectKind::Other)
        );
    }

    #[test]
    fn frequency_skips_metadata_pool() {
        static KEY: once_cell::sync::Lazy<crate::store::Key> =
            once_cell::sync::Lazy::new(|| key_from_tuple(b"etag2", 0, 1, 0).unwrap());
        let ctx_meta = AdmissionContext {
            pool: Pool::Metadata,
            key: &KEY,
            size_bytes: 100,
            pinned: false,
        };
        let policy = CompositeAdmissionPolicy {
            size: SizeThresholdPolicy {
                size_threshold_bytes: 1 << 30,
                pinned_bypass: true,
            },
            frequency: FrequencySketchAdmission::new(5),
        };
        assert_eq!(
            policy.decide(&ctx_meta),
            (AdmissionDecision::Admit, AdmissionRejectKind::Other)
        );
    }

    /// SHELF-24 regression: when the key is in the store's pin-set,
    /// `FoyerStore::get_or_fetch` must populate `ctx.pinned = true`
    /// so the size-threshold policy admits a payload that would
    /// otherwise be rejected.
    #[tokio::test]
    async fn pinned_keys_bypass_size_threshold() {
        use crate::config::{MetadataPoolConfig, PoolsConfig, RowGroupPoolConfig};
        use crate::store::{key_from_tuple, FoyerStore, Pool, ReadOutcome, Store};
        use bytes::Bytes;
        use std::path::PathBuf;

        let pools = PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 4 * 1024 * 1024,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 4 * 1024 * 1024,
                nvme_dir: PathBuf::from("/tmp/unused"),
                nvme_bytes: 0,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
            },
        };
        let store = FoyerStore::open(&pools).await.expect("open");

        let key = key_from_tuple(b"pin-etag", 0, 1, 0).expect("key");

        // Seed + pin the key so the next get_or_fetch sees `is_pinned`.
        store
            .insert(Pool::RowGroup, key.clone(), Bytes::from_static(&[0u8; 8]))
            .await
            .expect("seed");
        assert!(store.pin(Pool::RowGroup, &key));

        // Evict the cached bytes so the fetch path is exercised.
        assert!(store.evict(Pool::RowGroup, &key).await);

        // Size-threshold policy that rejects everything > 16 bytes
        // unless pinned.
        let policy = SizeThresholdPolicy {
            size_threshold_bytes: 16,
            pinned_bypass: true,
        };

        // Payload is 32 bytes — over the threshold. Without the pin
        // bypass this would be served but not cached.
        let big = Bytes::from(vec![0xAB; 32]);
        let outcome = store
            .get_or_fetch(Pool::RowGroup, key.clone(), &policy, async move { Ok(big) })
            .await
            .expect("fetch");
        assert!(matches!(outcome, ReadOutcome::Miss(_)));

        // Pinned bypass means the bytes got admitted after all.
        let hit = store.get(Pool::RowGroup, &key).await.unwrap();
        assert!(
            hit.is_some(),
            "pinned key must be cached even above size threshold"
        );
        assert_eq!(hit.unwrap().len(), 32);
    }
}
