//! Foyer-backed `Store` — the core caching surface.
//!
//! Ticket ownership:
//! - SHELF-03 — DRAM-only `pool.metadata` with SIEVE (Foyer built-in).
//! - SHELF-17 — separate DRAM pool for Iceberg manifests / Parquet
//!   footers / page indexes. ADR-0008 mandates exactly two pools in v1.
//! - SHELF-18 — hybrid DRAM + NVMe `pool.rowgroup` with S3-FIFO
//!   admission per ADR-0009. When `pools.rowgroup.nvme_bytes > 0`
//!   the pool is built as a `foyer::HybridCache`; otherwise it
//!   stays DRAM-only so dev clusters and CI without a PVC keep
//!   working. See `shelfd/docs/design-notes/SHELF-18-nvme-hybrid-pool.md`.
//! - SHELF-04 — content-addressed keys:
//!   `sha256(etag_bytes || le_u64(offset) || le_u64(length) || le_u32(rg_ordinal))`.
//! - SHELF-06 — [`FoyerStore::get_or_fetch`] is the single-flight
//!   miss path wired into the HTTP handler.
//!
//! The trait-first layout is deliberate so we can unit-test consumers
//! (the HTTP handler, the admission policy) against an in-memory
//! implementation without linking Foyer.

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use bytes::Bytes;
use foyer::{
    DirectFsDeviceOptions, Engine, EvictionConfig, FifoConfig, HybridCache, HybridCacheBuilder,
    LargeEngineOptions, LfuConfig, LruConfig, S3FifoConfig,
};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;

use crate::compression::CompressionPipeline;

// NOTE: The trait below uses `impl Future` (RPITIT) instead of the
// `async_trait` crate. If we later need `dyn Store`, SHELF-NN will add
// the `async_trait` dep with its own design note and dep justification
// per agents/4-shelfd-builder.md "Quality bar" dep rules.

/// Which of the two Foyer pools owns a key (see ADR-0008).
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Pool {
    /// DRAM only, SIEVE. `metadata.json`, manifest lists, manifests,
    /// Parquet footers, page indexes.
    Metadata,
    /// Hybrid DRAM + NVMe, S3-FIFO. Row-group byte-ranges.
    RowGroup,
}

/// A content-addressed key.
///
/// The bytes are `sha256(etag || le_u64(offset) || le_u64(length) ||
/// le_u32(rg_ordinal))` per SHELF-04. Row-group-ordinal = 0 for
/// non-columnar ranges (manifests, footers) so the same function
/// covers every pool.
///
/// # Multipart ETag caveat
///
/// S3's ETag is an MD5 for single-PUT objects but is `md5(parts)-N`
/// for multipart objects; neither form is a cryptographic hash of the
/// object. We do **not** rely on ETag for integrity — only as an
/// opaque version token that changes whenever S3 observes a new
/// version. SHA-256 over the concatenated inputs is what gives us the
/// content-addressed property inside the cache.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Key(pub [u8; 32]);

impl Key {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lower-case hex rendering (64 chars). Used in HTTP paths and
    /// Prometheus exemplars — never as a security token.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse a lower-case hex string (64 chars) back into a `Key`.
    pub fn from_hex(s: &str) -> crate::Result<Self> {
        if s.len() != 64 {
            return Err(crate::Error::InvalidKey("hex key must be 64 chars"));
        }
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out)
            .map_err(|_| crate::Error::InvalidKey("hex key contains non-hex characters"))?;
        Ok(Key(out))
    }
}

/// The cache interface consumed by `http::serve` and the admission
/// policy.
///
/// Returning `crate::Result<Option<Bytes>>` from `get` follows the
/// agents/4-shelfd-builder.md Pass 2 rule: no `.unwrap()`, every error
/// has a path.
pub trait Store: Send + Sync + Debug + 'static {
    /// Fetch a cached byte range. `None` = cache miss (not an error).
    fn get(
        &self,
        pool: Pool,
        key: &Key,
    ) -> impl std::future::Future<Output = crate::Result<Option<Bytes>>> + Send;

    /// Insert bytes into the pool.
    fn insert(
        &self,
        pool: Pool,
        key: Key,
        bytes: Bytes,
    ) -> impl std::future::Future<Output = crate::Result<()>> + Send;

    /// Explicit eviction (e.g. from `shelfctl evict` or the admin
    /// HTTP surface). Returns `true` iff the key was resident in
    /// `pool` before the call.
    ///
    /// Async because a hybrid pool's disk tier requires a Foyer
    /// `get`-probe to tell a disk-only entry apart from a genuine
    /// miss — calling `remove` is synchronous and idempotent, but
    /// the residency answer used for the 404 / 200 split is not.
    fn evict(&self, pool: Pool, key: &Key) -> impl std::future::Future<Output = bool> + Send;

    // SHELF-23 + SHELF-24 pin-set surface. The pin-set is held
    // separately from the two Foyer caches so that evicting the
    // cached bytes does not silently drop the pin.
    //
    // All of these are synchronous for the same reason as `evict`.

    /// Pin a key in `pool`. Returns `true` iff the entry was resident
    /// in that specific pool (or was already pinned in that same
    /// pool — idempotent). Pinning a key that is already pinned in a
    /// *different* pool returns `false` so operators see the
    /// mismatch instead of silently succeeding.
    fn pin(&self, pool: Pool, key: &Key) -> bool;

    /// Remove a key from the pin-set. Returns `true` iff it was pinned.
    fn unpin(&self, key: &Key) -> bool;

    /// Membership test.
    fn is_pinned(&self, key: &Key) -> bool;

    /// Snapshot of all pinned keys — used by the pin-list loader.
    /// Each key is paired with the pool it is pinned against so a
    /// reconciler can spot pool drift.
    fn pinned_keys(&self) -> Vec<(Pool, Key)>;

    /// Sum of recorded byte lengths for pinned keys.
    fn pinned_bytes(&self) -> u64;

    /// Count of pinned keys.
    fn pinned_count(&self) -> usize;

    /// Bytes used per pool, for `/stats` + HRW capacity weighting.
    fn used_bytes(&self, pool: Pool) -> u64;

    /// Pool capacity in bytes.
    fn capacity_bytes(&self, pool: Pool) -> u64;
}

/// Shared slot for a single in-flight miss fetch.
///
/// Concurrent callers hitting the same `(Pool, Key)` race for the
/// `Mutex<HashMap>` slot exactly once; whichever arrives first creates
/// the `OnceCell` and drives `fetch`, everyone else awaits the same
/// cell. Map entries hold only a `Weak`, so the last `Arc` drop cleans
/// up without a separate reaper task.
type InflightMap = Mutex<
    HashMap<
        (Pool, Key),
        Weak<OnceCell<Result<(Bytes, crate::coop_admission::FetchSource), String>>>,
    >,
>;

/// Internal: one pool worth of Foyer state.
///
/// `Dram` matches the SHELF-17 path (both pools DRAM-only). `Hybrid`
/// wraps a `foyer::HybridCache` for SHELF-18 once the operator
/// configures `pools.rowgroup.nvme_bytes > 0`. The enum is private
/// so the rest of `shelfd` keeps treating pools uniformly via the
/// [`FoyerStore`] surface.
#[derive(Debug)]
enum PoolHandle {
    Dram {
        cache: foyer::Cache<Key, Bytes>,
        /// DRAM budget as configured — Foyer's `Cache::capacity`
        /// applies internal alignment we would rather not surface
        /// on `/stats`.
        dram_capacity: u64,
    },
    Hybrid {
        cache: HybridCache<Key, Bytes>,
        dram_capacity: u64,
        disk_capacity: u64,
    },
}

/// Tier of the pool that served a `get`. Used internally to route
/// the disk hit/miss counter increments — never leaves the module.
#[derive(Debug, Clone, Copy)]
enum Tier {
    Dram,
    Disk,
}

/// Public tier of a cache hit, surfaced via [`ReadOutcome::Hit`] so
/// callers can split latency-by-outcome dashboards into
/// `hit_memory` vs `hit_disk` (SHELF-G1 / Track A1). Mirrors the
/// internal [`Tier`] but is part of the public API surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HitTier {
    /// Served straight from the in-memory tier of the pool.
    Memory,
    /// Served from the NVMe tier of a hybrid pool (DRAM miss + disk hit).
    Disk,
}

impl HitTier {
    /// Stable Prometheus label fragment. Pair with the `pool` label
    /// for `shelf_request_seconds{path,outcome}`.
    pub fn outcome_label(self) -> &'static str {
        match self {
            HitTier::Memory => "hit_memory",
            HitTier::Disk => "hit_disk",
        }
    }
}

impl From<Tier> for HitTier {
    fn from(t: Tier) -> Self {
        match t {
            Tier::Dram => HitTier::Memory,
            Tier::Disk => HitTier::Disk,
        }
    }
}

/// SHELF-A5 — Foyer `EventListener` that bumps
/// `shelf_evictions_total{pool, reason="capacity"}` whenever an entry
/// leaves the in-memory tier.
///
/// Foyer 0.12's `EventListener` exposes a single hook,
/// `on_memory_release`, which fires on every DRAM departure
/// regardless of cause: capacity-driven eviction (the dominant
/// signal in steady state), explicit `cache.remove(...)` from the
/// admin path, and pin-list-replace. We expose them all under
/// `reason="capacity"` because (a) capacity events dwarf the
/// others by orders of magnitude and (b) the existing
/// `reason="admin"` increment in [`FoyerStore::evict_in_pool`]
/// already labels the explicit-remove subset, so operators
/// reading the dashboard can subtract one from the other if
/// they need to.
///
/// Hybrid (NVMe-backed) pools fire this hook when an entry leaves
/// **DRAM** — that includes the spill from L1 → L2. The L2 → origin
/// evictions are not exposed by Foyer 0.12 and stay un-counted; they
/// would need either an `on_storage_release` hook (not yet
/// upstream) or a per-region GC hook (private API). See the
/// follow-up note in `agents/out/A5-eviction-listener.md`.
struct CapacityEvictionListener {
    pool_label: &'static str,
}

impl foyer::EventListener for CapacityEvictionListener {
    type Key = Key;
    type Value = Bytes;

    fn on_memory_release(&self, _key: Key, _value: Bytes) {
        crate::metrics::EVICTIONS_TOTAL
            .with_label_values(&[self.pool_label, "capacity"])
            .inc();
    }
}

impl PoolHandle {
    fn dram_capacity(&self) -> u64 {
        match self {
            PoolHandle::Dram { dram_capacity, .. } | PoolHandle::Hybrid { dram_capacity, .. } => {
                *dram_capacity
            }
        }
    }

    fn disk_capacity(&self) -> u64 {
        match self {
            PoolHandle::Dram { .. } => 0,
            PoolHandle::Hybrid { disk_capacity, .. } => *disk_capacity,
        }
    }

    fn used_bytes(&self) -> u64 {
        match self {
            PoolHandle::Dram { cache, .. } => cache.usage() as u64,
            PoolHandle::Hybrid { cache, .. } => cache.memory().usage() as u64,
        }
    }

    /// SHELF-18 best-effort disk occupancy.
    ///
    /// Foyer 0.12 does not expose a live "bytes on disk" counter on
    /// `HybridCache`; the closest proxy is `DeviceStats.write_bytes`
    /// (monotonic lifetime write volume). Once the on-disk ring has
    /// wrapped the reported value equals or exceeds the configured
    /// capacity, so we clamp with `min`. Operators who need a
    /// precise number reach for `foyer_storage_op_total{op="write"}`
    /// on the Foyer-emitted series; `shelf_disk_bytes_used` is
    /// meant as a "disk has started filling" signal for dashboards.
    fn disk_used_bytes(&self) -> u64 {
        match self {
            PoolHandle::Dram { .. } => 0,
            PoolHandle::Hybrid {
                cache,
                disk_capacity,
                ..
            } => {
                let stats = cache.stats();
                let written = stats.write_bytes.load(Ordering::Relaxed) as u64;
                written.min(*disk_capacity)
            }
        }
    }

    /// Load a value from the pool.
    ///
    /// Returns `(bytes, tier)` on hit. For the DRAM path `tier` is
    /// always [`Tier::Dram`]. For the hybrid path we first consult
    /// the in-memory cache to differentiate a memory hit (no disk
    /// traffic) from a disk hit (storage engine `load` succeeded).
    async fn get(&self, key: &Key) -> crate::Result<Option<(Bytes, Tier)>> {
        match self {
            PoolHandle::Dram { cache, .. } => {
                Ok(cache.get(key).map(|e| (e.value().clone(), Tier::Dram)))
            }
            PoolHandle::Hybrid { cache, .. } => {
                if let Some(entry) = cache.memory().get(key) {
                    return Ok(Some((entry.value().clone(), Tier::Dram)));
                }
                match cache
                    .get(key)
                    .await
                    .map_err(|e| crate::Error::Store(format!("hybrid get: {e}")))?
                {
                    Some(entry) => Ok(Some((entry.value().clone(), Tier::Disk))),
                    None => Ok(None),
                }
            }
        }
    }

    fn insert(&self, key: Key, bytes: Bytes) {
        match self {
            PoolHandle::Dram { cache, .. } => {
                cache.insert(key, bytes);
            }
            PoolHandle::Hybrid { cache, .. } => {
                cache.insert(key, bytes);
            }
        }
    }

    /// Disk-aware residency probe used by the admin eviction path.
    ///
    /// On a hybrid pool, `contains` only checks the memory tier, so a
    /// key that has aged out of DRAM onto NVMe looks absent — yet a
    /// subsequent `get` still returns bytes. `contains_any` falls
    /// back to a Foyer `get` probe when the memory tier misses so
    /// `evict_in_pool` can correctly report residency (and, more
    /// importantly, actually issue a disk-tier remove instead of a
    /// silent no-op).
    async fn contains_any(&self, key: &Key) -> crate::Result<bool> {
        match self {
            PoolHandle::Dram { cache, .. } => Ok(cache.contains(key)),
            PoolHandle::Hybrid { cache, .. } => {
                if cache.memory().contains(key) {
                    return Ok(true);
                }
                // `get` returns `Some` if the key is resident in
                // either tier. A disk hit will transiently promote
                // the entry back into DRAM; the caller (`remove`)
                // drops it from both tiers immediately after, so the
                // promotion is invisible externally.
                let got = cache
                    .get(key)
                    .await
                    .map_err(|e| crate::Error::Store(format!("hybrid probe: {e}")))?;
                Ok(got.is_some())
            }
        }
    }

    fn memory_get_len(&self, key: &Key) -> Option<u64> {
        match self {
            PoolHandle::Dram { cache, .. } => cache.get(key).map(|e| e.value().len() as u64),
            PoolHandle::Hybrid { cache, .. } => {
                cache.memory().get(key).map(|e| e.value().len() as u64)
            }
        }
    }

    /// **B1** — DRAM-only payload accessor used by [`FoyerStore::pin`]
    /// when compression is enabled, so the pin-set can record the
    /// *decoded* byte length rather than the on-disk frame length.
    /// Returns `None` when the entry is not memory-resident; callers
    /// are responsible for fall-back behaviour.
    fn memory_payload_bytes(&self, key: &Key) -> Option<Bytes> {
        match self {
            PoolHandle::Dram { cache, .. } => cache.get(key).map(|e| e.value().clone()),
            PoolHandle::Hybrid { cache, .. } => cache.memory().get(key).map(|e| e.value().clone()),
        }
    }

    fn remove(&self, key: &Key) {
        match self {
            PoolHandle::Dram { cache, .. } => {
                cache.remove(key);
            }
            PoolHandle::Hybrid { cache, .. } => {
                cache.remove(key);
            }
        }
    }
}

/// **B1** — on-disk record of how the rowgroup NVMe ring was last
/// written, persisted at `<nvme_dir>/.shelf-compression.json`. The
/// store boots only when the configured pipeline matches this
/// marker; mismatching configs abort with an operator-actionable
/// error rather than corrupt-read silently.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompressionMarker {
    /// Marker schema version. Bump when fields are added in a
    /// non-backward-compatible way.
    version: u32,
    /// Pipeline descriptor (e.g. `"zstd@3"`). Matches the string
    /// returned by `CompressionPipeline::descriptor`.
    descriptor: String,
    /// Configured minimum size that triggers actual compression
    /// (smaller frames are stored uncompressed but still tag-prefixed).
    min_size_bytes: u64,
}

/// Pool-segmented Foyer cache. `metadata` is always DRAM-only per
/// ADR-0008 / SHELF-17. `rowgroup` is DRAM-only when
/// `pools.rowgroup.nvme_bytes == 0` and a Foyer `HybridCache`
/// otherwise (SHELF-18, ADR-0009).
#[derive(Debug)]
pub struct FoyerStore {
    metadata: PoolHandle,
    rowgroup: PoolHandle,
    /// SHELF-21e — level-based back-pressure for the rowgroup
    /// hybrid pool's LODC submit queue. `Some` only when rowgroup
    /// is wired as a Foyer `HybridCache` (i.e. `nvme_bytes > 0`);
    /// `None` for DRAM-only deployments where the LODC simply
    /// doesn't exist. Held outside `PoolHandle::Hybrid` so the
    /// admission gate can be consulted with a single field access
    /// rather than a match-on-pool every read.
    rowgroup_lodc_bp: Option<crate::lodc_backpressure::LodcBackpressure>,
    /// **B1** — optional zstd pipeline for the rowgroup pool. `Some`
    /// when `cache.pools.rowgroup.compression.enabled` is true. The
    /// pipeline wraps every byte stream that reaches Foyer's
    /// `insert` and unwraps everything that comes back out of
    /// `get`, so compression is invisible to `PoolHandle` and the
    /// `Store` trait.
    rowgroup_compression: Option<CompressionPipeline>,
    /// SHELF-29 — independent-queue admission rate-limiter. Sits
    /// alongside `rowgroup_lodc_bp`; both gates must say admit for
    /// the insert to proceed. `None` when the rowgroup pool is
    /// DRAM-only OR the operator disabled the limiter.
    rowgroup_lodc_admit: Option<crate::admission_limiter::LodcAdmission>,
    inflight: InflightMap,
    /// SHELF-24 allowlist. Held separately from the two Foyer caches
    /// so that (1) eviction of the bytes does not also unpin the key
    /// and (2) the admin surface can refuse pins for keys that are
    /// not yet resident.
    ///
    /// The value tuple is `(pool, recorded_length_bytes)`. Tracking
    /// the pool alongside the key lets `pin` reject idempotent calls
    /// that name a different pool than the original pin — a SHELF-04
    /// key is unique per pool by construction, and pinning the same
    /// key against two pools would be a contract violation, not an
    /// operator convenience.
    pin_set: RwLock<HashMap<Key, (Pool, u64)>>,
    /// **A2 (rc.7)** — SHELF-20 [`crate::membership::DrainSignal`]
    /// shared with `main` and the membership resolver. Defaults to a
    /// fresh signal that is permanently inactive — a fine
    /// approximation for unit tests and dev boots that do not wire
    /// the SIGTERM path. Production wires the real one via
    /// [`FoyerStore::with_drain`].
    ///
    /// Held inline (not behind `Option<_>`) so the admit hot path
    /// reads a single atomic — the same cost as the no-throttle
    /// branch on the existing A1 RSS gauge — instead of paying a
    /// branch on `Option::is_some` followed by the load.
    drain_signal: crate::membership::DrainSignal,
    /// **A2 (rc.7)** — when `true`, an active `drain_signal`
    /// triggers a refused admit and a bump of
    /// [`crate::metrics::ADMIT_REFUSED_TOTAL`]; when `false` (the
    /// rollback escape hatch from `cache.drain.refuse_admits`), the
    /// signal is observed for `/stats` and metrics but does *not*
    /// gate writes. Default `false` so a bare `FoyerStore::open`
    /// from tests does not accidentally engage A2.
    drain_refuse_admits: bool,
    /// **A6 (rc.7)** — cooperative peer-admission probabilistic gate.
    /// Consulted **only** when `get_or_fetch`'s fetcher returns
    /// [`crate::coop_admission::FetchSource::Peer`]; origin fetches
    /// admit unconditionally. Held inline (not behind `Option<_>`) so
    /// the admit hot path reads a single field — the default-
    /// constructed gate (built when no config is wired) has
    /// `enabled = false` and short-circuits inside
    /// `should_admit_peer_bytes` for ~zero overhead. See ADR-0037.
    coop_gate: crate::coop_admission::CoopAdmissionGate,
    /// **B3 (rc.7)** — intermediate-table opt-out admission gate.
    /// Consulted on every admit chain run AFTER the A2 drain check
    /// but BEFORE the SHELF-25 / SHELF-21e / SHELF-29 / A6 chain so
    /// it short-circuits the more expensive W-TinyLFU + LODC + rate
    /// work. Held behind `Arc<_>` so the same gate instance can be
    /// cheaply cloned to background-refresh tasks. The default-
    /// constructed gate has `enabled = false` and is a strict no-op
    /// at the admit site, so non-opt-in deployments pay nothing.
    /// See ADR-0038.
    transient_gate: std::sync::Arc<crate::transient_admission::TransientGate>,
}

impl FoyerStore {
    /// Open the Foyer pools from the daemon config.
    ///
    /// `metadata` is always DRAM-only. `rowgroup` is DRAM-only when
    /// `pools.rowgroup.nvme_bytes == 0` and a Foyer `HybridCache`
    /// with a `DirectFsDevice` disk engine otherwise (SHELF-18,
    /// ADR-0009). The in-memory eviction algorithm on the hybrid
    /// pool is `S3FifoConfig::default()` so the ADR-0009 admission
    /// story (small → main promotion before any disk write) is
    /// honoured by construction.
    ///
    /// Fails fast with `Error::Store("pool.rowgroup NVMe init …")`
    /// on any disk-engine error — operators should see the failure,
    /// not a silent fall-back to DRAM.
    pub async fn open(config: &crate::config::PoolsConfig) -> crate::Result<Self> {
        let metadata_capacity = config.metadata.dram_bytes;
        let rowgroup_capacity = config.rowgroup.dram_bytes;
        if metadata_capacity == 0 {
            return Err(crate::Error::Store(
                "pools.metadata.dram_bytes must be > 0".into(),
            ));
        }
        if rowgroup_capacity == 0 {
            return Err(crate::Error::Store(
                "pools.rowgroup.dram_bytes must be > 0".into(),
            ));
        }

        let metadata_cache = foyer::CacheBuilder::new(metadata_capacity as usize)
            .with_weighter(|_k: &Key, v: &Bytes| v.len())
            .with_event_listener(Arc::new(CapacityEvictionListener {
                pool_label: pool_label(Pool::Metadata),
            }))
            .build();
        let metadata = PoolHandle::Dram {
            cache: metadata_cache,
            dram_capacity: metadata_capacity,
        };

        // **B1** — validate compression config + reconcile with the
        // on-disk marker file (hybrid pool only). Run before any
        // Foyer pool is constructed so a misconfiguration aborts
        // boot loud and early rather than corrupting bytes silently
        // on the first read.
        config.rowgroup.compression.validate()?;
        let rowgroup_compression = if config.rowgroup.compression.enabled {
            let pipeline = CompressionPipeline::new(
                pool_label(Pool::RowGroup),
                config.rowgroup.compression.level,
                config.rowgroup.compression.min_size_bytes,
            );
            pipeline.pre_touch_metrics();
            Some(pipeline)
        } else {
            None
        };
        if config.rowgroup.nvme_bytes > 0 {
            Self::ensure_compression_marker(
                &config.rowgroup.nvme_dir,
                rowgroup_compression.as_ref(),
            )?;
        }

        let rowgroup = if config.rowgroup.nvme_bytes == 0 {
            let cache = foyer::CacheBuilder::new(rowgroup_capacity as usize)
                .with_weighter(|_k: &Key, v: &Bytes| v.len())
                .with_event_listener(Arc::new(CapacityEvictionListener {
                    pool_label: pool_label(Pool::RowGroup),
                }))
                .build();
            PoolHandle::Dram {
                cache,
                dram_capacity: rowgroup_capacity,
            }
        } else {
            Self::build_hybrid_rowgroup(&config.rowgroup).await?
        };

        // SHELF-21e — wire the LODC back-pressure controller. Only
        // meaningful for hybrid rowgroup; DRAM-only pools have no
        // submit queue to bound. Pre-touch the three `shelf_lodc_*`
        // metric children on the `rowgroup` label so dashboards see
        // the series even on a freshly booted, idle pod (same
        // pattern the rest of `open()` uses for the other vec
        // metrics).
        let rowgroup_lodc_bp = if config.rowgroup.nvme_bytes == 0 {
            None
        } else {
            let bp = crate::lodc_backpressure::LodcBackpressure::from_disk_cache_config(
                &config.rowgroup.disk_cache,
                pool_label(Pool::RowGroup),
            );
            // SHELF-29 — the drops counter gained a `reason` label, so
            // pre-touch each child the gate can ever bump. This keeps
            // dashboards green on a freshly booted, idle pod.
            crate::metrics::LODC_DROPS_TOTAL
                .with_label_values(&[pool_label(Pool::RowGroup), "submit_queue_overflow"])
                .inc_by(0);
            crate::metrics::LODC_DROPS_TOTAL
                .with_label_values(&[pool_label(Pool::RowGroup), "rate_limit"])
                .inc_by(0);
            crate::metrics::LODC_INFLIGHT_BYTES
                .with_label_values(&[pool_label(Pool::RowGroup)])
                .set(0);
            crate::metrics::LODC_QUEUE_DEPTH
                .with_label_values(&[pool_label(Pool::RowGroup)])
                .set(0);
            Some(bp)
        };

        // SHELF-29 — wire the independent-queue admission rate-limiter
        // alongside the level gate above. Returns `None` when the
        // operator has explicitly disabled the limiter, which we want
        // for unit-test pools and ops emergency rollback. Pre-touch
        // the new gauges (drops counter is already pre-touched above)
        // so the admin UI sees a non-empty series at boot.
        let rowgroup_lodc_admit = crate::admission_limiter::LodcAdmission::from_config(
            &config.rowgroup.disk_cache.admission,
            pool_label(Pool::RowGroup),
        );
        if let Some(ref limiter) = rowgroup_lodc_admit {
            crate::metrics::LODC_ADMIT_TOKENS_AVAILABLE
                .with_label_values(&[pool_label(Pool::RowGroup)])
                .set(limiter.max_burst_bytes_for_init() as i64);
            crate::metrics::LODC_ADMIT_BURST_CAPACITY
                .with_label_values(&[pool_label(Pool::RowGroup)])
                .set(limiter.max_burst_bytes_for_init() as i64);
            // **A1 (rc.7)** — spawn the RSS poller. Held by an
            // `Arc<RssThrottle>` clone the limiter and the poller
            // both share; the poller self-exits via `Weak::upgrade`
            // failure when `FoyerStore` is dropped (process exit).
            // Polling cadence comes from the operator config so
            // tuning does not require a code change.
            if let Some(throttle) = limiter.rss_throttle() {
                let interval = std::time::Duration::from_secs(
                    config
                        .rowgroup
                        .disk_cache
                        .admission
                        .rss_throttle
                        .rss_poll_interval_secs
                        .max(1),
                );
                tracing::info!(
                    pool = pool_label(Pool::RowGroup),
                    rss_target_bytes = config.rowgroup.disk_cache.admission.rss_throttle.rss_target_bytes,
                    rss_poll_interval = ?interval,
                    low_watermark = config.rowgroup.disk_cache.admission.rss_throttle.low_watermark,
                    high_watermark = config.rowgroup.disk_cache.admission.rss_throttle.high_watermark,
                    "A1 RSS-aware admission throttle enabled",
                );
                crate::admission_limiter::RssThrottle::spawn_poller(throttle, interval);
            }
        } else {
            // Pre-touch the A1 gauges at FULL even when the byte-
            // rate limiter is disabled so dashboards never show
            // "no data" on a freshly deployed pod.
            crate::metrics::LODC_RSS_THROTTLE_MULTIPLIER
                .with_label_values(&[pool_label(Pool::RowGroup)])
                .set(10_000);
            crate::metrics::LODC_RSS_PRESSURE_SECONDS_TOTAL
                .with_label_values(&[pool_label(Pool::RowGroup)])
                .inc_by(0);
        }

        // Pre-touch every metric family that is otherwise only emitted
        // after a real hit/miss/admit/evict has fired. The post-cutover
        // 2026-04-27 snapshot caught only 6 of the 21 declared families
        // showing up in mimir-data because the `prometheus` crate prunes
        // `*Vec` collectors with zero observed children at scrape time.
        // Touching them with `inc_by(0)` / `set(0)` guarantees an
        // initial child row so dashboards never have to special-case
        // "metric not yet present" vs "value is genuinely zero".
        for pool in [Pool::Metadata, Pool::RowGroup] {
            let label = pool_label(pool);
            crate::metrics::ADMISSIONS_TOTAL
                .with_label_values(&[label, "admit"])
                .inc_by(0);
            crate::metrics::ADMISSIONS_TOTAL
                .with_label_values(&[label, "reject_size"])
                .inc_by(0);
            // SHELF-21e — the LODC back-pressure label is only
            // meaningful for `rowgroup`, but pre-touching for both
            // pools keeps the dashboard panel symmetric.
            crate::metrics::ADMISSIONS_TOTAL
                .with_label_values(&[label, "reject_lodc"])
                .inc_by(0);
            // SHELF-29 — pre-touch the rate-limiter rejection child
            // for the same reason as the other reject labels.
            crate::metrics::ADMISSIONS_TOTAL
                .with_label_values(&[label, "reject_rate"])
                .inc_by(0);
            // **A2 (rc.7)** — pre-touch the drain-rejection child so
            // dashboards see a non-empty series on a healthy pod.
            crate::metrics::ADMISSIONS_TOTAL
                .with_label_values(&[label, "reject_drain"])
                .inc_by(0);
            // **A6 (rc.7)** — pre-touch the cooperative-rejection
            // child for the same reason. Stays flat on a stock OSS
            // deploy (gate is default-off) but the series has to
            // exist for the dashboard to render.
            crate::metrics::ADMISSIONS_TOTAL
                .with_label_values(&[label, "reject_coop"])
                .inc_by(0);
            crate::metrics::EVICTIONS_TOTAL
                .with_label_values(&[label, "capacity"])
                .inc_by(0);
            crate::metrics::INFLIGHT_SINGLEFLIGHT
                .with_label_values(&[label])
                .set(0);
            crate::metrics::ENGINE_RESETS_TOTAL
                .with_label_values(&[label, "pool_open_retry"])
                .inc_by(0);
            crate::metrics::ROLLING_HIT_RATIO_BPS
                .with_label_values(&[label])
                .set(0);
        }

        // **A2 (rc.7)** — pre-touch the drain-aware admission series
        // so dashboards see a non-empty value on a freshly booted,
        // non-draining pod (same pattern the rest of `open()` uses).
        crate::metrics::ADMIT_REFUSED_TOTAL
            .with_label_values(&["draining"])
            .inc_by(0);
        crate::metrics::DRAIN_ACTIVE.set(0);

        // **A6 (rc.7)** — pre-touch the cooperative-admission counters
        // so a freshly booted, idle pod publishes the documented label
        // set as zeros. Same pre-touch discipline the rest of `open()`
        // uses for the A1/A2/SHELF-21e/SHELF-29 series.
        for label in [pool_label(Pool::Metadata), pool_label(Pool::RowGroup)] {
            crate::metrics::COOP_PEER_ADMITS_TOTAL
                .with_label_values(&[label])
                .inc_by(0);
            crate::metrics::COOP_PEER_DROPS_TOTAL
                .with_label_values(&[label])
                .inc_by(0);
            crate::metrics::COOP_PRIMARY_FORCE_ADMITS_TOTAL
                .with_label_values(&[label])
                .inc_by(0);
        }

        // **B3 (rc.7)** — pre-touch the transient-gate counters so a
        // freshly booted, idle pod publishes them as zeros. The
        // `other` label is the s3_shim sentinel for non-Iceberg
        // paths; pre-touching it keeps dashboards green even on a
        // cluster whose first request hits a non-Iceberg key.
        crate::metrics::TRANSIENT_REFUSALS_TOTAL
            .with_label_values(&["other"])
            .inc_by(0);
        crate::metrics::TRANSIENT_REFRESH_ERRORS_TOTAL
            .with_label_values(&["other"])
            .inc_by(0);
        crate::metrics::TRANSIENT_DECISIONS_CACHED.set(0);
        crate::metrics::ADMISSIONS_TOTAL
            .with_label_values(&[pool_label(Pool::RowGroup), "reject_transient"])
            .inc_by(0);

        Ok(Self {
            metadata,
            rowgroup,
            rowgroup_lodc_bp,
            rowgroup_lodc_admit,
            rowgroup_compression,
            inflight: Mutex::new(HashMap::new()),
            pin_set: RwLock::new(HashMap::new()),
            drain_signal: crate::membership::DrainSignal::new(),
            drain_refuse_admits: false,
            coop_gate: crate::coop_admission::CoopAdmissionGate::new(
                crate::coop_admission::CoopAdmissionConfig::default(),
            ),
            transient_gate: std::sync::Arc::new(crate::transient_admission::TransientGate::new(
                crate::transient_admission::TransientAdmissionConfig::default(),
            )),
        })
    }

    /// **A2 (rc.7)** — wire the SHELF-20
    /// [`crate::membership::DrainSignal`] into the rowgroup admit
    /// gate. When `refuse_admits` is `true` and the signal flips
    /// active (SIGTERM), `get_or_fetch` short-circuits before the
    /// SHELF-21e / SHELF-29 / A1 gates and bumps the new
    /// `shelf_admit_refused_total{reason="draining"}` counter.
    ///
    /// `refuse_admits = false` is the operator escape hatch
    /// (`cache.drain.refuse_admits = false`): the signal still
    /// flows out via `/stats` so peers' membership resolvers drop
    /// us from their HRW rings, but the local admit gate keeps
    /// behaving as it did before A2. Pinned (memory-resident) keys
    /// are unaffected either way — pin replay during drain stays
    /// observably the same.
    ///
    /// Builder shape (consumes `self` and returns the modified
    /// store) mirrors [`crate::http::ServerState::with_drain_signal`]
    /// so the call-site reads as a one-liner in `main.rs`.
    pub fn with_drain(
        mut self,
        signal: crate::membership::DrainSignal,
        refuse_admits: bool,
    ) -> Self {
        self.drain_signal = signal;
        self.drain_refuse_admits = refuse_admits;
        self
    }

    /// **A2 (rc.7)** — `true` when this store is currently configured
    /// to refuse admits on drain *and* the local pod's drain bit is
    /// flipped. Hot-path test: two atomic loads, one `&&`. Exposed
    /// for tests and for `/stats` instrumentation that wants to
    /// surface the *effective* gate state (vs the raw signal).
    pub fn drain_refuses_admits(&self) -> bool {
        self.drain_refuse_admits && self.drain_signal.is_active()
    }

    /// **A6 (rc.7)** — wire the operator-configured cooperative
    /// peer-admission gate into the rowgroup admit site. Builder
    /// shape mirrors [`FoyerStore::with_drain`] so `main.rs` reads
    /// as a one-liner. Called ONCE during boot; replacing the gate
    /// at runtime is not supported (the RNG state would reset on
    /// every call).
    ///
    /// The default-constructed gate (built in `open`) has
    /// `enabled = false` and is a strict no-op at the admit site,
    /// so this builder is only required when the operator opts in
    /// via `cache.coopAdmission.enabled = true`. See ADR-0037.
    pub fn with_coop_admission(mut self, gate: crate::coop_admission::CoopAdmissionGate) -> Self {
        self.coop_gate = gate;
        self
    }

    /// **A6 (rc.7)** — `true` when the cooperative peer-admission
    /// gate's master switch is flipped on. Exposed for `/stats` and
    /// test introspection; the admit hot path consults the gate
    /// directly via `should_admit_peer_bytes`.
    pub fn coop_admission_enabled(&self) -> bool {
        self.coop_gate.is_enabled()
    }

    /// **B3 (rc.7)** — wire the operator-configured intermediate-table
    /// admit gate. Builder mirrors [`FoyerStore::with_coop_admission`]
    /// so `main.rs` reads as a one-liner. Called ONCE during boot.
    /// The default-constructed gate (built in `open`) has
    /// `enabled = false` and is a strict no-op at the admit site, so
    /// this builder is only required when the operator opts in via
    /// `cache.transientAdmission.enabled = true`. See ADR-0038.
    pub fn with_transient_admission(
        mut self,
        gate: std::sync::Arc<crate::transient_admission::TransientGate>,
    ) -> Self {
        self.transient_gate = gate;
        self
    }

    /// **B3 (rc.7)** — `true` when the transient-admission gate's
    /// master switch is flipped on. Exposed for `/stats` and tests.
    pub fn transient_admission_enabled(&self) -> bool {
        self.transient_gate.is_enabled()
    }

    /// **B3 (rc.7)** — clone the live gate handle. Useful for
    /// background tasks (e.g. a Prometheus updater) that want to
    /// publish [`crate::transient_admission::TransientGate::decisions_cached`]
    /// without holding the entire `FoyerStore`.
    pub fn transient_gate(&self) -> std::sync::Arc<crate::transient_admission::TransientGate> {
        self.transient_gate.clone()
    }

    /// **B1** — reconcile the configured compression mode with the
    /// `.shelf-compression.json` marker file in `nvme_dir`. The
    /// marker is the only authoritative record of how the bytes
    /// already on the NVMe ring were encoded; flipping mode without
    /// wiping the dir would corrupt every read because the encoder
    /// header byte (`0x00` / `0x5A`) is indistinguishable from
    /// arbitrary Parquet content.
    ///
    /// Rules:
    /// - Empty `nvme_dir` (no Foyer region files yet) — write a
    ///   marker that matches the current config (or no marker if
    ///   compression is off).
    /// - Non-empty `nvme_dir` + marker present + matches config — ok.
    /// - Non-empty `nvme_dir` + marker present + mismatches config —
    ///   abort with a clear "wipe NVMe to switch compression mode"
    ///   error.
    /// - Non-empty `nvme_dir` + marker missing + compression off —
    ///   ok (legacy state, the ring was never compressed).
    /// - Non-empty `nvme_dir` + marker missing + compression on —
    ///   abort: there is pre-existing uncompressed data the new
    ///   decoder cannot tell apart from a header byte.
    fn ensure_compression_marker(
        nvme_dir: &std::path::Path,
        pipeline: Option<&CompressionPipeline>,
    ) -> crate::Result<()> {
        let marker_path = nvme_dir.join(".shelf-compression.json");
        let dir_exists = nvme_dir.exists();
        let dir_has_payload = if dir_exists {
            std::fs::read_dir(nvme_dir)
                .map(|rd| {
                    rd.filter_map(Result::ok)
                        .any(|entry| entry.file_name() != ".shelf-compression.json")
                })
                .unwrap_or(false)
        } else {
            false
        };

        let existing_marker: Option<CompressionMarker> = match std::fs::read(&marker_path) {
            Ok(bytes) => Some(serde_json::from_slice(&bytes).map_err(|e| {
                crate::Error::Store(format!(
                    "pool.rowgroup compression marker `{}` is malformed: {e}; \
                     either restore a known-good marker or wipe the NVMe ring",
                    marker_path.display()
                ))
            })?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(crate::Error::Store(format!(
                    "pool.rowgroup compression marker `{}`: read failed: {e}",
                    marker_path.display()
                )));
            }
        };

        match (existing_marker.as_ref(), pipeline) {
            (Some(marker), Some(pipe)) if marker.descriptor == pipe.descriptor() => Ok(()),
            (Some(marker), Some(pipe)) => Err(crate::Error::Store(format!(
                "pool.rowgroup compression marker mismatch: NVMe ring was written with `{}` \
                 but config is `{}`. Wipe `{}` and restart to switch compression mode \
                 (the on-disk frames are not self-describing).",
                marker.descriptor,
                pipe.descriptor(),
                nvme_dir.display(),
            ))),
            (Some(marker), None) => Err(crate::Error::Store(format!(
                "pool.rowgroup compression is disabled but the NVMe ring at `{}` was \
                 written with `{}`. Wipe the directory and restart, or re-enable \
                 compression with the same descriptor.",
                nvme_dir.display(),
                marker.descriptor,
            ))),
            (None, Some(pipe)) if dir_has_payload => Err(crate::Error::Store(format!(
                "pool.rowgroup compression is enabled (`{}`) but the NVMe ring at `{}` \
                 contains pre-existing uncompressed data. Wipe the directory and restart \
                 — flipping compression on a populated ring corrupts subsequent reads.",
                pipe.descriptor(),
                nvme_dir.display(),
            ))),
            (None, Some(pipe)) => {
                if !dir_exists {
                    std::fs::create_dir_all(nvme_dir).map_err(|e| {
                        crate::Error::Store(format!(
                            "pool.rowgroup nvme_dir `{}`: create failed: {e}",
                            nvme_dir.display()
                        ))
                    })?;
                }
                let marker = CompressionMarker {
                    version: 1,
                    descriptor: pipe.descriptor(),
                    min_size_bytes: pipe.min_size_bytes() as u64,
                };
                let bytes = serde_json::to_vec_pretty(&marker).map_err(|e| {
                    crate::Error::Store(format!("compression marker serialise: {e}"))
                })?;
                std::fs::write(&marker_path, bytes).map_err(|e| {
                    crate::Error::Store(format!(
                        "pool.rowgroup compression marker `{}`: write failed: {e}",
                        marker_path.display()
                    ))
                })?;
                Ok(())
            }
            (None, None) => Ok(()),
        }
    }

    /// SHELF-21e — Foyer's monotonic "bytes committed to NVMe"
    /// counter for the rowgroup hybrid pool, used by the LODC
    /// back-pressure controller to compute `inflight = admitted −
    /// committed`. Returns `0` for DRAM-only pools (no NVMe, no LODC).
    fn rowgroup_committed_bytes(&self) -> u64 {
        match &self.rowgroup {
            PoolHandle::Dram { .. } => 0,
            PoolHandle::Hybrid { cache, .. } => {
                cache.stats().write_bytes.load(Ordering::Relaxed) as u64
            }
        }
    }

    /// Build the `rowgroup` pool as a Foyer `HybridCache` backed by
    /// `nvme_dir`. Kept as its own method so the `open` path reads
    /// linearly; the error wrapping funnels every possible failure
    /// (missing dir, zero-after-alignment capacity, device IO) into
    /// a single `Error::Store("pool.rowgroup NVMe init failed: …")`
    /// for ops.
    async fn build_hybrid_rowgroup(
        cfg: &crate::config::RowGroupPoolConfig,
    ) -> crate::Result<PoolHandle> {
        cfg.validate_nvme()
            .map_err(|e| crate::Error::Store(format!("pool.rowgroup NVMe init failed: {e}")))?;
        std::fs::create_dir_all(&cfg.nvme_dir).map_err(|e| {
            crate::Error::Store(format!(
                "pool.rowgroup NVMe init failed: create `{}`: {e}",
                cfg.nvme_dir.display()
            ))
        })?;

        let dram_capacity = cfg.dram_bytes as usize;
        let disk_capacity = cfg.nvme_bytes as usize;
        // `DirectFsDeviceOptions::with_file_size` must be <= capacity.
        // Aim for ~4 regions so the reclaim loop has multiple ranges
        // to work with, cap at 64 MiB so very large pools do not get
        // multi-GiB region files, and — critically — never exceed
        // `disk_capacity`. The previous `clamp(1 MiB, 64 MiB)` could
        // return 1 MiB when `disk_capacity` was smaller, which failed
        // Foyer's device sizing invariant on small/test pools.
        let file_size = (disk_capacity / 4)
            .clamp(1 << 20, 64 * 1024 * 1024)
            .min(disk_capacity.max(1));

        let device = DirectFsDeviceOptions::new(&cfg.nvme_dir)
            .with_capacity(disk_capacity)
            .with_file_size(file_size);

        // SHELF-E1b — ADR-0009 originally pinned this to S3-FIFO for
        // scan resistance. The default is now LRU because S3-FIFO's
        // "small queue → main queue → disk" promotion path keeps
        // one-shot reads (Metabase admin, ad-hoc BI) off NVMe entirely:
        // items expire from the small queue before they earn promotion,
        // so `shelf_disk_bytes_used` stays at zero. Operators can opt
        // back into S3-FIFO via `cache.pools.rowgroup.evictionPolicy`.
        let eviction: EvictionConfig = match cfg.eviction_policy {
            crate::config::EvictionPolicy::S3Fifo => S3FifoConfig::default().into(),
            crate::config::EvictionPolicy::Lru => LruConfig::default().into(),
            crate::config::EvictionPolicy::Lfu => LfuConfig::default().into(),
            crate::config::EvictionPolicy::Fifo => FifoConfig::default().into(),
        };
        // SHELF — Foyer LODC tunables (post-mortem 2026-04-27 shelf-1
        // OOMKilled). Foyer 0.12 ships with `flushers=1` and a 16 MiB
        // buffer pool, which serialises every region write to NVMe and
        // overflows the submit queue under shelfd's 256-inflight × 32
        // MiB rowgroup workload. The chart's
        // `cache.pools.rowgroup.diskCache.*` block lets operators raise
        // these without recompiling. See
        // `shelfd/docs/runbooks/2026-04-shelf-1-oom.md`.
        let mut large_opts = LargeEngineOptions::new();
        if let Some(flushers) = cfg.disk_cache.flushers {
            large_opts = large_opts.with_flushers(flushers);
        }
        if let Some(bytes) = cfg.disk_cache.buffer_pool_size_bytes {
            large_opts = large_opts.with_buffer_pool_size(bytes as usize);
        }
        if let Some(bytes) = cfg.disk_cache.submit_queue_size_threshold_bytes {
            large_opts = large_opts.with_submit_queue_size_threshold(bytes as usize);
        }
        // SHELF-21e — back-pressure now lives in
        // [`crate::lodc_backpressure::LodcBackpressure`] (a level-based,
        // shelfd-side gate at the `get_or_fetch` admission seam),
        // NOT in a Foyer admission picker. The previous Foyer-side
        // `RateLimitPicker` (preview-8) added latency to every write
        // even when the queue was empty because the token bucket
        // fills purely on time, not on observed drain rate; reverted
        // in preview-9 / helm rev-22. The LODC submit-queue *size*
        // is still bounded by Foyer's
        // `submit_queue_size_threshold` configured in
        // `LargeEngineOptions` above; the new gate adds a soft
        // watermark in front of that hard cap so the drop event is
        // observable via `shelf_lodc_drops_total{pool}`.
        let cache: HybridCache<Key, Bytes> = HybridCacheBuilder::new()
            .with_name("shelfd.rowgroup")
            .with_event_listener(Arc::new(CapacityEvictionListener {
                pool_label: pool_label(Pool::RowGroup),
            }))
            .memory(dram_capacity)
            .with_weighter(|_k: &Key, v: &Bytes| v.len())
            .with_eviction_config(eviction)
            .storage(Engine::Large)
            .with_device_options(device)
            .with_large_object_disk_cache_options(large_opts)
            .build()
            .await
            .map_err(|e| crate::Error::Store(format!("pool.rowgroup NVMe init failed: {e}")))?;

        // Pre-touch the disk-tier counters/gauges so Prometheus
        // emits a child row even before the first hit/miss. This
        // keeps dashboards green on a freshly-booted hybrid pool
        // that has not yet served any traffic.
        let label = pool_label(Pool::RowGroup);
        crate::metrics::DISK_HITS_TOTAL
            .with_label_values(&[label])
            .inc_by(0);
        crate::metrics::DISK_MISSES_TOTAL
            .with_label_values(&[label])
            .inc_by(0);
        crate::metrics::DISK_BYTES_USED
            .with_label_values(&[label])
            .set(0);
        crate::metrics::DISK_BYTES_CAPACITY
            .with_label_values(&[label])
            .set(cfg.nvme_bytes as i64);

        Ok(PoolHandle::Hybrid {
            cache,
            dram_capacity: cfg.dram_bytes,
            disk_capacity: cfg.nvme_bytes,
        })
    }

    fn handle_for(&self, pool: Pool) -> &PoolHandle {
        match pool {
            Pool::Metadata => &self.metadata,
            Pool::RowGroup => &self.rowgroup,
        }
    }

    /// Read-through miss path with single-flight deduplication.
    ///
    /// Contract:
    /// 1. If `(pool, key)` is cached, return those bytes. No admission
    ///    check, no `fetch` call.
    /// 2. Otherwise, dedupe concurrent callers so `fetch` runs exactly
    ///    once; all callers receive a clone of the same `Bytes`.
    /// 3. After fetch, consult `admission`. On `Admit` the bytes are
    ///    inserted into the pool before being returned; on `Reject`
    ///    the bytes are returned but not cached.
    ///
    /// Errors from `fetch` propagate to every concurrent caller.
    ///
    /// **B3 (rc.7)**: this entry point passes the sentinel
    /// `"other"` for the transient-gate's table label so the gate
    /// is a strict no-op for callers that do not already know the
    /// originating Iceberg `schema.table`. Use
    /// [`FoyerStore::get_or_fetch_for_table`] to opt in to the
    /// gate from a call site that has the raw S3 key (and therefore
    /// can compute the label via [`crate::s3_shim::table_label`]).
    pub async fn get_or_fetch<A, F>(
        &self,
        pool: Pool,
        key: Key,
        admission: &A,
        fetch: F,
    ) -> crate::Result<ReadOutcome>
    where
        A: crate::admission::AdmissionPolicy + ?Sized,
        F: Future<Output = crate::Result<(Bytes, crate::coop_admission::FetchSource)>> + Send,
    {
        self.get_or_fetch_for_table(pool, key, "other", admission, fetch)
            .await
    }

    /// **B3 (rc.7)** — opt-in variant of [`FoyerStore::get_or_fetch`]
    /// that lets the caller hand in the originating Iceberg
    /// `schema.table` label. The label is consulted by the
    /// transient-table admission gate (see ADR-0038); the rest of
    /// the admit chain (drain → policy → LODC → rate → coop) is
    /// unchanged. Pass `"other"` (or call [`FoyerStore::get_or_fetch`]
    /// directly) when the originating path is not Iceberg or when
    /// the label cannot be derived without extra work.
    pub async fn get_or_fetch_for_table<A, F>(
        &self,
        pool: Pool,
        key: Key,
        table_label: &str,
        admission: &A,
        fetch: F,
    ) -> crate::Result<ReadOutcome>
    where
        A: crate::admission::AdmissionPolicy + ?Sized,
        F: Future<Output = crate::Result<(Bytes, crate::coop_admission::FetchSource)>> + Send,
    {
        // SHELF-G1 / Track A1: capture the tier (DRAM vs NVMe) so the
        // returned `ReadOutcome::Hit` keeps `hit_memory` / `hit_disk`
        // splittable downstream. Going through `Store::get` would
        // strip the bit before the shim can observe it. We replicate
        // the disk hit/miss counter dance from `<Self as Store>::get`
        // here so the operator-facing counters stay consistent.
        match self.handle_for(pool).get(&key).await? {
            Some((bytes, tier)) => {
                if matches!(tier, Tier::Disk) {
                    crate::metrics::DISK_HITS_TOTAL
                        .with_label_values(&[pool_label(pool)])
                        .inc();
                }
                let payload = self.decode_for_read(pool, bytes)?;
                return Ok(ReadOutcome::Hit(payload, tier.into()));
            }
            None => {
                if matches!(self.handle_for(pool), PoolHandle::Hybrid { .. }) {
                    crate::metrics::DISK_MISSES_TOTAL
                        .with_label_values(&[pool_label(pool)])
                        .inc();
                }
            }
        }

        // Track E8 — in-flight single-flight gauge. Decremented in the
        // RAII guard dropped at the end of this scope so the counter is
        // symmetric even if `fetch.await` panics.
        let pool_label = match pool {
            Pool::Metadata => "metadata",
            Pool::RowGroup => "rowgroup",
        };
        crate::metrics::INFLIGHT_SINGLEFLIGHT
            .with_label_values(&[pool_label])
            .inc();
        struct InflightGuard(&'static str);
        impl Drop for InflightGuard {
            fn drop(&mut self) {
                crate::metrics::INFLIGHT_SINGLEFLIGHT
                    .with_label_values(&[self.0])
                    .dec();
            }
        }
        let _inflight_guard = InflightGuard(pool_label);

        let cell = self.acquire_inflight_cell(pool, &key);

        // SHELF-08: differentiate leader from follower for trace-level
        // fan-in analysis. The leader is the caller whose closure
        // `get_or_init` actually runs; followers observe the cell was
        // already initialized before they called `get_or_init` (there
        // is still a small race where `initialized()` lies, so we
        // additionally record the leader role from inside the closure
        // and compare).
        let was_initialized = cell.initialized();
        let role_seen_by_closure = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let role_flag = role_seen_by_closure.clone();
        let slot = cell
            .get_or_init(|| async move {
                role_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                tracing::debug!(
                    target: "shelfd::singleflight",
                    role = "leader",
                    "shelfd.singleflight"
                );
                fetch.await.map_err(|e| e.to_string())
            })
            .await;
        if !role_seen_by_closure.load(std::sync::atomic::Ordering::Relaxed) || was_initialized {
            tracing::debug!(
                target: "shelfd::singleflight",
                role = "follower",
                "shelfd.singleflight"
            );
        }

        let (bytes, source) = match slot.clone() {
            Ok((b, src)) => (b, src),
            // S2: the leader's original `crate::Error` variant was
            // stringified at the `OnceCell<Result<(Bytes, FetchSource),
            // String>>` boundary, so we can no longer recover it.
            // Surface as `Error::Singleflight` rather than
            // `Error::Origin` so the `shelfd_error_total{component}`
            // series doesn't mis-attribute Foyer / membership /
            // admission failures to the S3 origin.
            Err(e) => return Err(crate::Error::Singleflight(e)),
        };

        let ctx = crate::admission::AdmissionContext {
            pool,
            key: &key,
            size_bytes: bytes.len() as u64,
            // SHELF-24: pin-set is the single source of truth for
            // the admission bypass flag.
            pinned: self.is_pinned(&key),
        };

        // **A2 (rc.7)** — drain-aware admission, evaluated before the
        // policy / level / rate gates so a draining pod does not pay
        // for a Foyer insert it is about to lose.
        //
        // Cost: two atomic loads (the `bool` and the `AtomicBool`
        // inside [`crate::membership::DrainSignal`]). Same shape as
        // the existing A1 multiplier check — the hot path stays
        // branch-predictable when neither signal is active.
        //
        // We bypass the gate when `pinned == true`: pin-set entries
        // are operator-blessed, must round-trip through
        // [`FoyerStore::insert`] for the pin-list reload to behave,
        // and a pin during drain is rare-enough-to-ignore traffic
        // (the pod is going away in <60s; a pin replay racing the
        // signal will simply land on a still-warm peer next round).
        if !ctx.pinned && self.drain_refuses_admits() {
            crate::metrics::ADMIT_REFUSED_TOTAL
                .with_label_values(&["draining"])
                .inc();
            crate::metrics::ADMISSIONS_TOTAL
                .with_label_values(&[pool_label, "reject_drain"])
                .inc();
            return Ok(ReadOutcome::Miss(bytes));
        }

        // **B3 (rc.7)** — intermediate-table opt-out. Consulted AFTER
        // the A2 drain check (drain wins because the pod is going
        // away) but BEFORE the SHELF-25 / SHELF-21e / SHELF-29 / A6
        // chain so a refusal short-circuits the more expensive
        // W-TinyLFU + LODC + rate-limiter work. The gate is a strict
        // no-op when:
        //   - `cache.transientAdmission.enabled = false` (default), or
        //   - the table label is the `"other"` sentinel (non-Iceberg
        //     path, or caller used `get_or_fetch` instead of
        //     `get_or_fetch_for_table`), or
        //   - no override or refreshed metadata flags the table.
        // Pinned keys bypass the gate (operator-blessed; same
        // invariant the cooperative gate honours).
        // See `transient_admission.rs` + ADR-0038.
        if !ctx.pinned
            && self.transient_gate.decide(table_label)
                == crate::transient_admission::TableAdmission::RefuseTransient
        {
            crate::metrics::TRANSIENT_REFUSALS_TOTAL
                .with_label_values(&[table_label])
                .inc();
            crate::metrics::ADMISSIONS_TOTAL
                .with_label_values(&[pool_label, "reject_transient"])
                .inc();
            return Ok(ReadOutcome::Miss(bytes));
        }

        // Track E8 — categorise the admission outcome. The
        // `AdmissionDecision` enum only exposes Admit / Reject, so
        // "why rejected" is reconstructed here from the context: if
        // the payload is over the size threshold and not pinned,
        // report `reject_size`; otherwise `reject_other` (ML model,
        // future reasons). This is approximate but cheap; the policy
        // module could return a richer enum later without breaking
        // the metric name.
        let decision = admission.decide(&ctx);
        let policy_admit = decision == crate::admission::AdmissionDecision::Admit;

        // SHELF-21e — second admission gate: even when the
        // size-threshold policy says admit, drop the insert if the
        // hybrid pool's LODC submit queue is backed up. Only
        // applies to the rowgroup hybrid pool; metadata is DRAM
        // only and has no LODC. Non-blocking: two atomic loads.
        let lodc_admit = if policy_admit && pool == Pool::RowGroup {
            match &self.rowgroup_lodc_bp {
                Some(bp) => bp.should_admit(bytes.len() as u64, self.rowgroup_committed_bytes()),
                None => true,
            }
        } else {
            true
        };

        // SHELF-29 — third admission gate: independent-queue token-
        // bucket rate-limiter. Sized in bytes-per-second; bounds the
        // rate of admissions feeding Foyer's submit queue independent
        // of the in-flight level. Same non-blocking guarantee as the
        // SHELF-21e gate (atomic CAS, no await, no Mutex). Only the
        // rowgroup pool has the limiter; metadata is DRAM-only and
        // has no NVMe drain budget to defend.
        let rate_admit = if policy_admit && lodc_admit && pool == Pool::RowGroup {
            match &self.rowgroup_lodc_admit {
                Some(limiter) => limiter.try_admit(bytes.len() as u64),
                None => true,
            }
        } else {
            true
        };

        // **A6 (rc.7)** — cooperative peer-admission gate. Consulted
        // ONLY for `FetchSource::Peer`; origin admits unchanged. The
        // gate sits **after** the pressure-aware chain (drain / policy
        // / LODC / rate-limiter) so back-pressure rejections still
        // dominate the `shelf_admissions_total{decision=...}` rollup.
        // Pinned keys bypass the cooperative gate as well — operator-
        // blessed entries always admit regardless of source.
        //
        // By construction the only path that produces `Peer` here is
        // `peer_or_origin_fetch` (SHELF-23), which already short-
        // circuits to `Origin` when this pod is the HRW primary. So
        // when `source == Peer` we know `key_primary_is_self == false`
        // and pass that directly. The gate's
        // `should_admit_peer_bytes` keeps the `key_primary_is_self`
        // parameter as a documented invariant — see `coop_admission.rs`.
        let coop_admit = match source {
            crate::coop_admission::FetchSource::Origin => true,
            crate::coop_admission::FetchSource::Peer => {
                if ctx.pinned {
                    // Operator-blessed; pinned keys are exempt from
                    // every probabilistic gate, A6 included.
                    true
                } else {
                    let admitted = self.coop_gate.should_admit_peer_bytes(false);
                    if admitted {
                        crate::metrics::COOP_PEER_ADMITS_TOTAL
                            .with_label_values(&[pool_label])
                            .inc();
                    } else {
                        crate::metrics::COOP_PEER_DROPS_TOTAL
                            .with_label_values(&[pool_label])
                            .inc();
                    }
                    admitted
                }
            }
        };

        let admit = policy_admit && lodc_admit && rate_admit && coop_admit;
        let decision_label = if admit {
            "admit"
        } else if !lodc_admit {
            // SHELF-21e — the policy said admit but the level gate
            // dropped the insert. Distinct from `reject_size` /
            // `reject_rate` so dashboards can tell "policy rejected"
            // apart from "back-pressure dropped" apart from
            // "rate-limited"; the latter two are ops signals that
            // NVMe drain is falling behind ingress.
            "reject_lodc"
        } else if !rate_admit {
            // SHELF-29 — the policy and the level gate both said
            // admit but the rate-limiter dropped the insert. Kept
            // distinct from `reject_lodc` so the
            // `shelf_admissions_total` panel can show the *new*
            // gate's blast radius vs the level gate's.
            "reject_rate"
        } else if !coop_admit {
            // **A6 (rc.7)** — the upstream chain (policy / LODC /
            // rate) all said admit, but the cooperative gate dropped
            // the secondary copy because the local pod is not the
            // HRW primary and the operator-configured replication
            // factor said "trust the primary". Distinct from the
            // pressure-aware reject labels so dashboards can graph
            // "saved by cooperative gate" independently from "saved
            // by NVMe pressure".
            "reject_coop"
        } else if ctx.pinned {
            "reject_other"
        } else {
            // `reject_size` captures the dominant path today per
            // ADR-0003; when LightGBM lands (c-lightgbm-escape-hatch)
            // we'll split this further.
            "reject_size"
        };
        crate::metrics::ADMISSIONS_TOTAL
            .with_label_values(&[pool_label, decision_label])
            .inc();
        if admit {
            self.insert(pool, key, bytes.clone()).await?;
        }
        Ok(ReadOutcome::Miss(bytes))
    }

    fn acquire_inflight_cell(
        &self,
        pool: Pool,
        key: &Key,
    ) -> Arc<OnceCell<Result<(Bytes, crate::coop_admission::FetchSource), String>>> {
        let mut guard = self.inflight.lock();
        if let Some(weak) = guard.get(&(pool, key.clone())) {
            if let Some(a) = weak.upgrade() {
                return a;
            }
        }
        let a: Arc<OnceCell<Result<(Bytes, crate::coop_admission::FetchSource), String>>> =
            Arc::new(OnceCell::new());
        guard.insert((pool, key.clone()), Arc::downgrade(&a));
        a
    }

    // SHELF-23 + SHELF-24 pin-set surface. See design note
    // `shelfd/docs/design-notes/SHELF-23-24-admin-surface-and-pinlist.md`.
    //
    // The pin-set is a `HashMap<Key, u64>` where the value is the
    // resident byte length recorded at `pin()` time. Storing the
    // length inline makes [`FoyerStore::pinned_bytes`] a simple sum
    // over the map values — no extra cache lookups on `/stats` or
    // `POST /admin/reload`.

    /// Pin a key against a specific pool. Returns `true` iff the key
    /// is resident in `pool` (or was already pinned **in that same
    /// pool** — idempotent). Pinning a key that is already pinned in
    /// a *different* pool returns `false` so the admin handler can
    /// 404 the caller: a SHELF-04 key is unique per pool by
    /// construction, so a cross-pool pin request indicates a
    /// mis-typed request, not an operator convenience.
    pub fn pin(&self, pool: Pool, key: &Key) -> bool {
        // Idempotent only for the same (pool, key). The existing
        // byte count is trusted — re-inserting the same key with a
        // potentially different payload would be an ADR-0003
        // violation anyway.
        if let Some((existing_pool, _)) = self.pin_set.read().get(key) {
            return *existing_pool == pool;
        }
        // SHELF-18: on a hybrid pool we only honour `pin()` when the
        // key is already memory-resident — a disk-only pin would
        // require async I/O which the admin surface does not do.
        // Operators can always re-fetch to warm the memory tier and
        // then pin.
        //
        // **B1**: when compression is enabled the *stored* length is
        // the encoded frame length, but `pinned_bytes()` is a budget
        // signal that should reflect what the user sees. Decode the
        // memory-resident frame so the pin-set tracks the decoded
        // payload length.
        let len = match (
            self.compression_for(pool),
            self.handle_for(pool).memory_payload_bytes(key),
        ) {
            (Some(pipeline), Some(stored)) => match pipeline.decode_from_store(&stored) {
                Ok(decoded) => decoded.len() as u64,
                Err(_) => stored.len() as u64,
            },
            (None, _) => match self.handle_for(pool).memory_get_len(key) {
                Some(len) => len,
                None => return false,
            },
            (Some(_), None) => return false,
        };
        self.pin_set.write().insert(key.clone(), (pool, len));
        true
    }

    /// Remove a key from the pin-set. Never touches the caches.
    /// Returns `true` iff the key was pinned.
    pub fn unpin(&self, key: &Key) -> bool {
        self.pin_set.write().remove(key).is_some()
    }

    /// Hot-path membership test used on every read-miss admission.
    pub fn is_pinned(&self, key: &Key) -> bool {
        self.pin_set.read().contains_key(key)
    }

    /// Snapshot used by the pin-list loader when diffing. Each key
    /// is paired with the pool it is pinned against so the reloader
    /// can spot pool drift without a second lookup.
    pub fn pinned_keys(&self) -> Vec<(Pool, Key)> {
        self.pin_set
            .read()
            .iter()
            .map(|(k, (p, _))| (*p, k.clone()))
            .collect()
    }

    /// Distinct pinned key count regardless of residency.
    pub fn pinned_count(&self) -> usize {
        self.pin_set.read().len()
    }

    /// Sum of the byte lengths recorded when each pinned key was
    /// installed. O(N) over the pin-set; no cache lookups.
    pub fn pinned_bytes(&self) -> u64 {
        self.pin_set.read().values().map(|(_, len)| *len).sum()
    }

    /// **B1** — pipeline lookup keyed on `Pool`. Today only
    /// `Pool::RowGroup` can carry a pipeline; the metadata pool
    /// keeps its legacy compile-time `zstd_metadata` feature gate
    /// for now. Returning `None` short-circuits the encode/decode
    /// boundary in the hot path.
    fn compression_for(&self, pool: Pool) -> Option<&CompressionPipeline> {
        match pool {
            Pool::RowGroup => self.rowgroup_compression.as_ref(),
            Pool::Metadata => None,
        }
    }

    /// **B1** — pre-store transform. Returns the bytes Foyer should
    /// hold; identical to `bytes` when no pipeline is configured.
    fn encode_for_store(&self, pool: Pool, bytes: Bytes) -> crate::Result<Bytes> {
        match self.compression_for(pool) {
            Some(pipeline) => pipeline
                .encode_for_store(&bytes)
                .map_err(|e| crate::Error::Store(format!("compression encode: {e}"))),
            None => Ok(bytes),
        }
    }

    /// **B1** — post-load transform. Returns the original payload
    /// bytes; identical to `bytes` when no pipeline is configured.
    fn decode_for_read(&self, pool: Pool, bytes: Bytes) -> crate::Result<Bytes> {
        match self.compression_for(pool) {
            Some(pipeline) => pipeline
                .decode_from_store(&bytes)
                .map_err(|e| crate::Error::Store(format!("compression decode: {e}"))),
            None => Ok(bytes),
        }
    }

    /// Evict a key from `pool`. Preserves the pin-set so a subsequent
    /// re-fetch still goes through admission with `ctx.pinned = true`.
    ///
    /// Returns `true` iff the key was resident in the pool (either
    /// DRAM or — for hybrid pools — NVMe) before the call. For a
    /// hybrid pool, `remove` drops the disk-tier copy as well, so
    /// subsequent reads cannot resurrect the bytes from NVMe.
    ///
    /// The previous implementation only consulted the memory tier,
    /// so a disk-resident entry produced a spurious 404 from the
    /// admin handler and — worse — left the disk copy in place,
    /// still servable on the next `GET /cache/...`.
    pub async fn evict_in_pool(&self, pool: Pool, key: &Key) -> bool {
        let pool_label = match pool {
            Pool::Metadata => "metadata",
            Pool::RowGroup => "rowgroup",
        };
        // Track E8 — admin-triggered eviction. Capacity evictions
        // are now also counted via [`CapacityEvictionListener`]
        // under `reason="capacity"` (SHELF-A5). The two labels
        // overlap by exactly the bytes torn down by an explicit
        // admin call (handle.remove also fires `on_memory_release`),
        // which is acceptable: admin evictions are rare and the
        // dashboard treats `capacity` as the dominant ops signal.
        crate::metrics::EVICTIONS_TOTAL
            .with_label_values(&[pool_label, "admin"])
            .inc();
        let handle = self.handle_for(pool);
        let had = match handle.contains_any(key).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    pool = ?pool,
                    key_hex = %key.to_hex(),
                    error = %e,
                    "hybrid residency probe failed during evict; issuing remove anyway",
                );
                // Bias towards a best-effort purge: we cannot prove
                // the key is there, but we can always issue the
                // idempotent Foyer remove and let the caller retry.
                false
            }
        };
        if had {
            handle.remove(key);
        }
        had
    }

    /// SHELF-D7 — residency probe used by the batch `/cache/contains`
    /// endpoint and by peer-failover (E6). Returns `true` when the key
    /// is resident in the named pool's DRAM **or** NVMe tier.
    ///
    /// This is a fast-path probe: DRAM residency is O(1); only on a
    /// DRAM miss do we fall back to a hybrid `get` probe. Callers that
    /// cannot afford the disk roundtrip (e.g. Trino's split-planning
    /// hot path) should call this against a remote peer, not against
    /// their own `shelfd`, and rely on the HRW ring to route to the
    /// owner replica.
    pub async fn contains(&self, pool: Pool, key: &Key) -> bool {
        self.handle_for(pool)
            .contains_any(key)
            .await
            .unwrap_or(false)
    }

    /// SHELF-18 — bytes currently held on the NVMe tier of `pool`.
    /// Always `0` for `Pool::Metadata` (DRAM-only per ADR-0008).
    pub fn disk_bytes_used(&self, pool: Pool) -> u64 {
        self.handle_for(pool).disk_used_bytes()
    }

    /// SHELF-18 — configured NVMe capacity for `pool` (0 when the
    /// pool runs DRAM-only).
    pub fn disk_bytes_capacity(&self, pool: Pool) -> u64 {
        self.handle_for(pool).disk_capacity()
    }
}

/// Whether a [`FoyerStore::get_or_fetch`] returned the bytes straight
/// from a warm pool or after an origin fetch.
///
/// `Hit` carries the [`HitTier`] (memory vs disk) so the shim and
/// native data plane can emit `shelf_request_seconds{outcome}` with
/// the same `hit_memory` / `hit_disk` / `miss` cardinality the
/// dashboard expects (Track A1 / SHELF-G1 — without this split the
/// p95 latency panel collapses memory hits, NVMe hits, and full
/// origin misses into one number, which is exactly the signal the
/// cache is supposed to differentiate).
#[derive(Debug)]
pub enum ReadOutcome {
    Hit(Bytes, HitTier),
    Miss(Bytes),
}

impl ReadOutcome {
    pub fn into_bytes(self) -> Bytes {
        match self {
            ReadOutcome::Hit(b, _) | ReadOutcome::Miss(b) => b,
        }
    }

    pub fn is_hit(&self) -> bool {
        matches!(self, ReadOutcome::Hit(_, _))
    }

    /// Stable label for `shelf_request_seconds{outcome}`. `Hit`
    /// splits into `hit_memory` / `hit_disk`; `Miss` is the
    /// origin-fetch path.
    pub fn outcome_label(&self) -> &'static str {
        match self {
            ReadOutcome::Hit(_, tier) => tier.outcome_label(),
            ReadOutcome::Miss(_) => "miss",
        }
    }
}

impl Store for FoyerStore {
    async fn get(&self, pool: Pool, key: &Key) -> crate::Result<Option<Bytes>> {
        // SHELF-18: a `None` on a hybrid pool means both DRAM and
        // the NVMe tier missed; we record the miss unconditionally
        // so dashboards see disk-miss pressure even for pools that
        // would otherwise never report. DRAM-only pools do not
        // bump either counter (hence the guard on `Tier::Disk`).
        match self.handle_for(pool).get(key).await? {
            Some((bytes, Tier::Dram)) => Ok(Some(self.decode_for_read(pool, bytes)?)),
            Some((bytes, Tier::Disk)) => {
                crate::metrics::DISK_HITS_TOTAL
                    .with_label_values(&[pool_label(pool)])
                    .inc();
                Ok(Some(self.decode_for_read(pool, bytes)?))
            }
            None => {
                if matches!(self.handle_for(pool), PoolHandle::Hybrid { .. }) {
                    crate::metrics::DISK_MISSES_TOTAL
                        .with_label_values(&[pool_label(pool)])
                        .inc();
                }
                Ok(None)
            }
        }
    }

    async fn insert(&self, pool: Pool, key: Key, bytes: Bytes) -> crate::Result<()> {
        let to_store = self.encode_for_store(pool, bytes)?;
        self.handle_for(pool).insert(key, to_store);
        Ok(())
    }

    async fn evict(&self, pool: Pool, key: &Key) -> bool {
        // Forwards to the inherent method so the pool-targeting logic
        // lives in one place.
        FoyerStore::evict_in_pool(self, pool, key).await
    }

    fn pin(&self, pool: Pool, key: &Key) -> bool {
        FoyerStore::pin(self, pool, key)
    }

    fn unpin(&self, key: &Key) -> bool {
        FoyerStore::unpin(self, key)
    }

    fn is_pinned(&self, key: &Key) -> bool {
        FoyerStore::is_pinned(self, key)
    }

    fn pinned_keys(&self) -> Vec<(Pool, Key)> {
        FoyerStore::pinned_keys(self)
    }

    fn pinned_bytes(&self) -> u64 {
        FoyerStore::pinned_bytes(self)
    }

    fn pinned_count(&self) -> usize {
        FoyerStore::pinned_count(self)
    }

    fn used_bytes(&self, pool: Pool) -> u64 {
        self.handle_for(pool).used_bytes()
    }

    fn capacity_bytes(&self, pool: Pool) -> u64 {
        // SHELF-18 contract: `capacity_bytes` reports the DRAM budget
        // only. The NVMe tier is exposed separately via
        // [`FoyerStore::disk_bytes_capacity`] so ops dashboards can
        // graph the two tiers independently without an extra join on
        // `/stats`. HRW weighting (SHELF-20) stays anchored on DRAM
        // for cache-sizing purposes.
        self.handle_for(pool).dram_capacity()
    }
}

/// Prometheus label for a pool. Kept as a separate fn so the HTTP
/// and store layers emit the same string.
pub(crate) fn pool_label(pool: Pool) -> &'static str {
    match pool {
        Pool::Metadata => "metadata",
        Pool::RowGroup => "rowgroup",
    }
}

/// Derive a content-addressed `Key` from the SHELF-04 tuple.
///
/// `Key = sha256(etag || offset.to_le_bytes() || length.to_le_bytes()
///                || rg_ordinal.to_le_bytes())`.
///
/// Length-0 reads are rejected: the cache never stores empty ranges
/// and rejecting them early is the cheapest place to catch a
/// malformed plugin request. ETag is passed through as opaque bytes —
/// see the `Key` type docs for the multipart caveat.
///
/// The Java side derives the same key from the same tuple; the
/// golden-vector test in this file and in `KeyTest` share a fixed
/// set of inputs to pin the invariant.
pub fn key_from_tuple(
    etag: &[u8],
    offset: u64,
    length: u64,
    rg_ordinal: u32,
) -> crate::Result<Key> {
    if etag.is_empty() {
        return Err(crate::Error::InvalidKey("etag must be non-empty"));
    }
    if length == 0 {
        return Err(crate::Error::InvalidKey("length must be > 0"));
    }

    let mut hasher = Sha256::new();
    hasher.update(etag);
    hasher.update(offset.to_le_bytes());
    hasher.update(length.to_le_bytes());
    hasher.update(rg_ordinal.to_le_bytes());
    let digest = hasher.finalize();

    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    Ok(Key(out))
}

#[cfg(test)]
mod key_tests {
    use super::*;

    /// Golden-vector **inputs** shared with
    /// `clients/trino/src/test/java/io/shelf/client/KeyTest`.
    ///
    /// Format: `(etag_utf8, offset, length, rg_ordinal)`.
    ///
    /// The fixture file
    /// `tests/fixtures/shelf04_golden_vectors.txt` holds the expected
    /// hex for each tuple and is loaded by both the Rust and the Java
    /// tests. That file is **the source of truth** for the algorithm:
    /// if you change it, you have changed the on-disk cache layout and
    /// must write an ADR. Regeneration: `cargo run -p shelfctl --
    /// debug-golden`. See ADR-0011 for the formal invariant.
    const GOLDEN_INPUTS: &[(&str, u64, u64, u32)] = &[
        // -- SHELF-04 baseline --
        // Representative Iceberg manifest read (offset 0, ordinal 0).
        ("\"9f8e6e48a1f7e2c3b5d41234567890ab\"", 0, 8_192, 0),
        // Parquet footer slice (non-zero offset, ordinal 0).
        (
            "\"aa11bb22cc33dd44ee55ff6677889900\"",
            536_854_528,
            65_536,
            0,
        ),
        // Row-group 3 of the same file (same etag/offset/length,
        // different ordinal) — proves ordinal participates.
        (
            "\"aa11bb22cc33dd44ee55ff6677889900\"",
            536_854_528,
            65_536,
            3,
        ),
        // Multipart ETag (has `-N` suffix); treated as opaque bytes.
        ("\"d41d8cd98f00b204e9800998ecf8427e-7\"", 1, 1, 42),
        // -- SHELF-16: row-group ordinal variants --
        // Same (etag, offset, length), three distinct rg ordinals.
        // If the key function ever drops the ordinal input, these three
        // rows collapse to one hash and the fixture parity test fails
        // on both sides simultaneously.
        ("\"rg-ordinal-sweep\"", 4_096, 131_072, 0),
        ("\"rg-ordinal-sweep\"", 4_096, 131_072, 1),
        ("\"rg-ordinal-sweep\"", 4_096, 131_072, 7),
        // Offset = u64::MAX / 2 — exercises the upper half of the LE
        // u64 encoding; ordinal 0 vs 255 also flips a full u32 byte.
        ("\"big-offset\"", u64::MAX / 2, 16, 0),
        ("\"big-offset\"", u64::MAX / 2, 16, 255),
        // Length = 1 byte; ordinal = u16 ceiling.
        ("\"single-byte\"", 0, 1, 65_535),
        // Length = 16 MiB; ordinal = 4_096 (row-group count scale).
        ("\"row-group-xl\"", 0, 16 * 1024 * 1024, 4_096),
        // Multipart ETag form with ordinals 0 and 2.
        ("\"\"-multipart\"", 0, 4_096, 0),
        ("\"\"-multipart\"", 0, 4_096, 2),
        // ASCII-only 8-byte ETag (no surrounding quotes — 8 bytes),
        // every ordinal in 0..=3 to pin the hot-path ordinals.
        ("shelf16b", 2_048, 8_192, 0),
        ("shelf16b", 2_048, 8_192, 1),
        ("shelf16b", 2_048, 8_192, 2),
        ("shelf16b", 2_048, 8_192, 3),
    ];

    #[test]
    fn golden_vectors_match_fixture() {
        let fixture = include_str!("../tests/fixtures/shelf04_golden_vectors.txt");
        let expected: Vec<&str> = fixture
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();
        assert_eq!(
            expected.len(),
            GOLDEN_INPUTS.len(),
            "fixture must have one line per golden input"
        );
        for ((etag, off, len, ord), want) in GOLDEN_INPUTS.iter().zip(expected) {
            let got = key_from_tuple(etag.as_bytes(), *off, *len, *ord)
                .expect("valid golden vector must hash")
                .to_hex();
            assert_eq!(
                got, want,
                "golden vector mismatch for etag={etag} off={off} len={len} ord={ord}"
            );
        }
    }

    #[test]
    fn ordinal_changes_key() {
        let a = key_from_tuple(b"etag", 0, 1, 0).unwrap();
        let b = key_from_tuple(b"etag", 0, 1, 1).unwrap();
        assert_ne!(a, b, "rg_ordinal must be part of the hash input");
    }

    #[test]
    fn offset_and_length_change_key() {
        let base = key_from_tuple(b"etag", 0, 1, 0).unwrap();
        let shifted = key_from_tuple(b"etag", 1, 1, 0).unwrap();
        let longer = key_from_tuple(b"etag", 0, 2, 0).unwrap();
        assert_ne!(base, shifted);
        assert_ne!(base, longer);
    }

    #[test]
    fn etag_changes_key() {
        let a = key_from_tuple(b"etag-a", 0, 1, 0).unwrap();
        let b = key_from_tuple(b"etag-b", 0, 1, 0).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn rejects_empty_etag() {
        let err = key_from_tuple(b"", 0, 1, 0).unwrap_err();
        assert!(matches!(err, crate::Error::InvalidKey(_)));
    }

    #[test]
    fn rejects_zero_length() {
        let err = key_from_tuple(b"etag", 0, 0, 0).unwrap_err();
        assert!(matches!(err, crate::Error::InvalidKey(_)));
    }

    #[test]
    fn hex_roundtrip() {
        let k = key_from_tuple(b"etag", 123, 456, 7).unwrap();
        let hex = k.to_hex();
        let parsed = Key::from_hex(&hex).unwrap();
        assert_eq!(k, parsed);
    }

    #[test]
    fn from_hex_rejects_wrong_length() {
        assert!(Key::from_hex("abc").is_err());
        assert!(Key::from_hex(&"a".repeat(63)).is_err());
        assert!(Key::from_hex(&"a".repeat(65)).is_err());
    }

    #[test]
    fn from_hex_rejects_non_hex() {
        assert!(Key::from_hex(&"z".repeat(64)).is_err());
    }

    #[test]
    fn roundtrip_produces_same_digest() {
        // The hash is deterministic across calls: the "roundtrip"
        // property required by the SHELF-04 acceptance list.
        for (etag, off, len, ord) in GOLDEN_INPUTS {
            let a = key_from_tuple(etag.as_bytes(), *off, *len, *ord).unwrap();
            let b = key_from_tuple(etag.as_bytes(), *off, *len, *ord).unwrap();
            assert_eq!(a, b);
        }
    }
}

#[cfg(test)]
mod store_tests {
    // **A6 (rc.7) test stability** — drain (A2) and cooperative-admission
    // tests in this module use a `parking_lot::Mutex<()>` (see
    // `COUNTER_TEST_LOCK`) to serialise reads of shared global metric
    // counters. The guard is held across `await` deliberately: the
    // test body runs entirely on the current tokio runtime, the
    // awaited `get_or_fetch` calls do not yield to other tests in
    // this module because each `#[tokio::test]` brings its own
    // runtime, and the counters' "must be 0 between baseline and
    // final read" invariant would otherwise be racy. Clippy lints
    // `await_holding_lock` unconditionally for `parking_lot::Mutex`,
    // hence the module-level allow.
    #![allow(clippy::await_holding_lock)]
    use super::*;
    use crate::admission::{AdmissionContext, AdmissionDecision, AdmissionPolicy};
    use crate::config::{MetadataPoolConfig, PoolsConfig, RowGroupPoolConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn test_pools() -> PoolsConfig {
        PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 1 << 20,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 1 << 20,
                nvme_dir: std::path::PathBuf::from("/tmp/unused"),
                nvme_bytes: 0,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
                compression: crate::config::CompressionConfig::default(),
            },
        }
    }

    async fn new_store() -> FoyerStore {
        FoyerStore::open(&test_pools()).await.expect("open")
    }

    fn k(seed: u8) -> Key {
        key_from_tuple(&[seed; 4], 0, 1, 0).unwrap()
    }

    /// Admit-everything policy for happy-path store tests; the
    /// SHELF-25 logic is covered separately in `admission::tests`.
    #[derive(Debug)]
    struct AlwaysAdmit;
    impl AdmissionPolicy for AlwaysAdmit {
        fn decide(&self, _ctx: &AdmissionContext<'_>) -> AdmissionDecision {
            AdmissionDecision::Admit
        }
    }

    #[derive(Debug)]
    struct NeverAdmit;
    impl AdmissionPolicy for NeverAdmit {
        fn decide(&self, _ctx: &AdmissionContext<'_>) -> AdmissionDecision {
            AdmissionDecision::Reject
        }
    }

    #[tokio::test]
    async fn insert_then_get_is_hit() {
        let store = new_store().await;
        let key = k(1);
        store
            .insert(Pool::RowGroup, key.clone(), Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let got = store.get(Pool::RowGroup, &key).await.unwrap();
        assert_eq!(got.as_deref(), Some(&b"hello"[..]));
    }

    #[tokio::test]
    async fn evict_removes_entry() {
        let store = new_store().await;
        let key = k(2);
        store
            .insert(Pool::Metadata, key.clone(), Bytes::from_static(b"x"))
            .await
            .unwrap();
        assert!(store.evict(Pool::Metadata, &key).await);
        assert!(store.get(Pool::Metadata, &key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pools_are_isolated() {
        let store = new_store().await;
        let key = k(3);
        store
            .insert(Pool::Metadata, key.clone(), Bytes::from_static(b"m"))
            .await
            .unwrap();
        assert!(store.get(Pool::RowGroup, &key).await.unwrap().is_none());
        assert_eq!(
            store.get(Pool::Metadata, &key).await.unwrap().as_deref(),
            Some(&b"m"[..]),
        );
    }

    #[tokio::test]
    async fn get_or_fetch_miss_admits_and_caches() {
        let store = new_store().await;
        let key = k(4);
        let outcome = store
            .get_or_fetch(Pool::RowGroup, key.clone(), &AlwaysAdmit, async {
                Ok((
                    Bytes::from_static(b"abc"),
                    crate::coop_admission::FetchSource::Origin,
                ))
            })
            .await
            .unwrap();
        assert!(matches!(outcome, ReadOutcome::Miss(_)));
        // Second get is a straight hit.
        let hit = store.get(Pool::RowGroup, &key).await.unwrap();
        assert_eq!(hit.as_deref(), Some(&b"abc"[..]));
    }

    #[tokio::test]
    async fn get_or_fetch_reject_does_not_cache() {
        let store = new_store().await;
        let key = k(5);
        let outcome = store
            .get_or_fetch(Pool::RowGroup, key.clone(), &NeverAdmit, async {
                Ok((
                    Bytes::from_static(b"xyz"),
                    crate::coop_admission::FetchSource::Origin,
                ))
            })
            .await
            .unwrap();
        assert!(matches!(outcome, ReadOutcome::Miss(_)));
        assert!(
            store.get(Pool::RowGroup, &key).await.unwrap().is_none(),
            "reject must not insert into the pool"
        );
    }

    /// SHELF-06 acceptance: 100 concurrent miss requests for the same
    /// cold key fan in to exactly ONE fetch invocation.
    #[tokio::test]
    async fn single_flight_coalesces_concurrent_misses() {
        let store = Arc::new(new_store().await);
        let key = k(6);
        let fetch_count = Arc::new(AtomicUsize::new(0));

        let mut joins = Vec::with_capacity(100);
        for _ in 0..100 {
            let store = store.clone();
            let key = key.clone();
            let fetch_count = fetch_count.clone();
            joins.push(tokio::spawn(async move {
                store
                    .get_or_fetch(Pool::RowGroup, key, &AlwaysAdmit, async move {
                        fetch_count.fetch_add(1, Ordering::SeqCst);
                        // Give siblings time to queue on the OnceCell.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        Ok((
                            Bytes::from_static(b"coalesced"),
                            crate::coop_admission::FetchSource::Origin,
                        ))
                    })
                    .await
            }));
        }

        for j in joins {
            let outcome = j.await.unwrap().unwrap();
            assert_eq!(outcome.into_bytes(), Bytes::from_static(b"coalesced"));
        }
        assert_eq!(
            fetch_count.load(Ordering::SeqCst),
            1,
            "single-flight must collapse 100 concurrent misses into 1 fetch"
        );
    }

    #[tokio::test]
    async fn used_bytes_reflects_insertions() {
        let store = new_store().await;
        assert_eq!(store.used_bytes(Pool::RowGroup), 0);
        store
            .insert(Pool::RowGroup, k(7), Bytes::from_static(&[0u8; 1024]))
            .await
            .unwrap();
        assert!(store.used_bytes(Pool::RowGroup) >= 1024);
        assert_eq!(store.capacity_bytes(Pool::RowGroup), 1 << 20);
    }

    /// SHELF-17 pool-isolation guarantee (ADR-0008).
    ///
    /// Two Foyer instances are constructed by [`FoyerStore::open`]
    /// (one per pool); eviction is therefore physically scoped to a
    /// single `foyer::Cache`. No amount of pressure on
    /// [`Pool::RowGroup`] can touch a byte in [`Pool::Metadata`]. In
    /// production the same invariant scales: a 50 GB ad-hoc scan
    /// fills the rowgroup pool's NVMe/DRAM capacity, not the 5 GiB
    /// metadata budget sitting in a separate cache instance.
    ///
    /// The test sizes look generous (8 MiB metadata, 1 MiB rowgroup)
    /// because `foyer::CacheBuilder` defaults to 8 shards and the
    /// capacity budget is divided across them. To keep the test
    /// focused on *cross-pool* isolation (not intra-pool per-shard
    /// eviction), we size the metadata pool so that even if every
    /// seeded entry hashes to a single shard the entry still fits.
    /// Rowgroup is then blasted with > 16x its capacity.
    #[tokio::test]
    async fn pool_isolation_under_rowgroup_pressure() {
        // Metadata pool: 8 MiB total → 1 MiB per shard (8 shards, Foyer
        // default), which comfortably holds the 16 * 8 KiB = 128 KiB
        // of seeded manifest-shaped entries even in the pathological
        // "all hash to one shard" case.
        //
        // Rowgroup pool: 1 MiB total. We will insert 2048 * 8 KiB =
        // 16 MiB worth of entries into it — 16x its capacity — to
        // establish the "50 GB ad-hoc scan" analogue at unit-test
        // scale. After the scan, every metadata entry must still be
        // retrievable byte-identical; rowgroup entries may or may not
        // remain (we don't assert on eviction of that pool).
        let pools = PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 8 * 1024 * 1024,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 1024 * 1024,
                nvme_dir: std::path::PathBuf::from("/tmp/unused"),
                nvme_bytes: 0,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
                compression: crate::config::CompressionConfig::default(),
            },
        };
        let store = FoyerStore::open(&pools).await.expect("open");

        // Seed the metadata pool with 16 * 8 KiB = 128 KiB of distinct
        // manifest-shaped entries.
        let mut md_keys = Vec::new();
        for seed in 0..16u8 {
            let key = key_from_tuple(&[seed; 4], 0, 8192, 0).unwrap();
            let payload = Bytes::from(vec![seed; 8192]);
            store
                .insert(Pool::Metadata, key.clone(), payload)
                .await
                .unwrap();
            md_keys.push((key, seed));
        }

        // Blast the rowgroup pool with far more than its capacity
        // (2048 * 8 KiB = 16 MiB > 16x of 1 MiB).
        for seed in 0..2048u16 {
            let key = key_from_tuple(&[(seed >> 8) as u8, seed as u8, 0, 0], 0, 8192, 1).unwrap();
            let payload = Bytes::from(vec![(seed & 0xff) as u8; 8192]);
            store.insert(Pool::RowGroup, key, payload).await.unwrap();
        }

        // The metadata pool is physically independent, so every seeded
        // entry survives, byte-identical.
        for (key, seed) in &md_keys {
            let got = store.get(Pool::Metadata, key).await.unwrap();
            assert!(
                got.is_some(),
                "metadata entry for seed {seed} was evicted by rowgroup pressure"
            );
            let bytes = got.unwrap();
            assert_eq!(bytes.len(), 8192);
            assert!(
                bytes.iter().all(|b| *b == *seed),
                "metadata payload tampered for seed {seed}"
            );
        }
    }

    /// Smaller variant of [`pool_isolation_under_rowgroup_pressure`]:
    /// observe `used_bytes` on the metadata pool before and after
    /// blasting rowgroup. The number must never go down — that would
    /// imply a cross-pool eviction, which is forbidden by ADR-0008.
    #[tokio::test]
    async fn rowgroup_pressure_does_not_shrink_metadata_used_bytes() {
        // Same sharding caveat as the isolation test: metadata is
        // sized so that intra-pool shard eviction does not confound
        // the cross-pool invariant we want to observe.
        let pools = PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 4 * 1024 * 1024,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 512 * 1024,
                nvme_dir: std::path::PathBuf::from("/tmp/unused"),
                nvme_bytes: 0,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
                compression: crate::config::CompressionConfig::default(),
            },
        };
        let store = FoyerStore::open(&pools).await.expect("open");

        // Seed metadata with 8 * 8 KiB = 64 KiB so there is headroom
        // but also a measurable `used_bytes` value to anchor on.
        for seed in 0..8u8 {
            let key = key_from_tuple(&[seed; 4], 0, 8192, 0).unwrap();
            let payload = Bytes::from(vec![seed; 8192]);
            store.insert(Pool::Metadata, key, payload).await.unwrap();
        }

        let before = store.used_bytes(Pool::Metadata);
        assert!(
            before > 0,
            "metadata used_bytes should be > 0 after seeding, got {before}"
        );

        // Overrun rowgroup capacity by > 16x (1024 * 8 KiB = 8 MiB
        // into a 512 KiB pool).
        for seed in 0..1024u16 {
            let key = key_from_tuple(&[(seed >> 8) as u8, seed as u8, 0, 0], 0, 8192, 1).unwrap();
            let payload = Bytes::from(vec![(seed & 0xff) as u8; 8192]);
            store.insert(Pool::RowGroup, key, payload).await.unwrap();
        }

        let after = store.used_bytes(Pool::Metadata);
        assert!(
            after >= before,
            "metadata used_bytes shrank under rowgroup pressure: before={before}, after={after}"
        );
    }

    #[tokio::test]
    async fn pin_missing_entry_returns_false() {
        let store = new_store().await;
        let key = k(40);
        assert!(!store.pin(Pool::RowGroup, &key));
        assert!(!store.pin(Pool::Metadata, &key));
        assert!(!store.is_pinned(&key));
    }

    #[tokio::test]
    async fn pin_then_unpin_roundtrip() {
        let store = new_store().await;
        let key = k(41);
        store
            .insert(Pool::RowGroup, key.clone(), Bytes::from_static(b"abcd"))
            .await
            .unwrap();
        assert!(store.pin(Pool::RowGroup, &key));
        assert!(store.is_pinned(&key));
        assert_eq!(store.pinned_count(), 1);
        assert_eq!(store.pinned_bytes(), 4);
        // Idempotent: pinning again still returns true without growing
        // the set.
        assert!(store.pin(Pool::RowGroup, &key));
        assert_eq!(store.pinned_count(), 1);
        assert!(store.unpin(&key));
        assert!(!store.is_pinned(&key));
        assert_eq!(store.pinned_count(), 0);
        assert_eq!(store.pinned_bytes(), 0);
        // Unpinning a key that is not pinned returns false.
        assert!(!store.unpin(&key));
    }

    #[tokio::test]
    async fn evict_preserves_pin_set() {
        let store = new_store().await;
        let key = k(42);
        store
            .insert(Pool::RowGroup, key.clone(), Bytes::from_static(&[0u8; 64]))
            .await
            .unwrap();
        assert!(store.pin(Pool::RowGroup, &key));
        assert!(store.evict(Pool::RowGroup, &key).await);
        assert!(store.is_pinned(&key), "pin-set must outlive eviction");
        assert_eq!(store.pinned_count(), 1);
        // The recorded byte length at pin-time is unchanged by the
        // subsequent eviction — the pin-set is the SOURCE of truth
        // for `pinned_bytes`, not the live cache.
        assert_eq!(store.pinned_bytes(), 64);
    }

    #[tokio::test]
    async fn pinned_bytes_and_count_reflect_pins() {
        let store = new_store().await;
        let k_meta = k(43);
        let k_rg = k(44);
        store
            .insert(
                Pool::Metadata,
                k_meta.clone(),
                Bytes::from_static(&[0u8; 10]),
            )
            .await
            .unwrap();
        store
            .insert(Pool::RowGroup, k_rg.clone(), Bytes::from_static(&[0u8; 32]))
            .await
            .unwrap();
        assert!(store.pin(Pool::Metadata, &k_meta));
        assert!(store.pin(Pool::RowGroup, &k_rg));
        assert_eq!(store.pinned_count(), 2);
        assert_eq!(store.pinned_bytes(), 42);
    }

    #[tokio::test]
    async fn evict_missing_returns_false() {
        let store = new_store().await;
        let key = k(45);
        assert!(!store.evict(Pool::RowGroup, &key).await);
        assert!(!store.evict(Pool::Metadata, &key).await);
    }

    // --- SHELF-18 hybrid-pool unit tests ---
    //
    // These exercise the `PoolHandle::Hybrid` branch at unit scope.
    // Integration tests against the HTTP surface live in
    // `shelfd/tests/it_hybrid_pool.rs`.

    fn hybrid_pools(nvme_dir: std::path::PathBuf) -> PoolsConfig {
        PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 1 << 20,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 1 << 20,
                nvme_dir,
                nvme_bytes: 64 * 1024 * 1024,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
                compression: crate::config::CompressionConfig::default(),
            },
        }
    }

    #[tokio::test]
    async fn zero_nvme_bytes_stays_dram_only() {
        // Regression guard for the "SHELF-17 behaviour is unchanged
        // when NVMe is off" contract. `test_pools()` builds with
        // `nvme_bytes = 0`, so capacity_bytes reports the DRAM
        // budget, disk capacity is 0, and no temp dir is ever
        // consulted even though `nvme_dir` is a nonsense path.
        let store = new_store().await;
        assert_eq!(store.capacity_bytes(Pool::RowGroup), 1 << 20);
        assert_eq!(store.disk_bytes_capacity(Pool::RowGroup), 0);
        assert_eq!(store.disk_bytes_used(Pool::RowGroup), 0);
    }

    #[tokio::test]
    async fn hybrid_pool_uses_tempdir_under_nvme_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pools = hybrid_pools(dir.path().to_path_buf());
        let store = FoyerStore::open(&pools).await.expect("open hybrid");

        // We report DRAM capacity on `capacity_bytes`; NVMe shows
        // up on `disk_bytes_capacity`. Either shape was acceptable
        // per SHELF-18 spec; the DRAM-only reporting keeps HRW
        // (SHELF-20) weighting stable.
        assert_eq!(store.capacity_bytes(Pool::RowGroup), 1 << 20);
        assert_eq!(store.disk_bytes_capacity(Pool::RowGroup), 64 * 1024 * 1024);
        // A hybrid pool that has never been written to reports zero
        // disk bytes used.
        assert_eq!(store.disk_bytes_used(Pool::RowGroup), 0);

        // The directory must contain Foyer's on-disk layout after
        // open (at minimum a region file). This is a cheap sanity
        // check on "NVMe init really happened".
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .expect("readdir")
            .filter_map(Result::ok)
            .collect();
        assert!(
            !entries.is_empty(),
            "hybrid pool open must populate nvme_dir"
        );
    }

    #[tokio::test]
    async fn hybrid_pool_insert_then_get_is_hit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pools = hybrid_pools(dir.path().to_path_buf());
        let store = FoyerStore::open(&pools).await.expect("open");
        let key = k(60);
        store
            .insert(
                Pool::RowGroup,
                key.clone(),
                Bytes::from_static(b"hybrid-hit"),
            )
            .await
            .unwrap();
        let got = store.get(Pool::RowGroup, &key).await.unwrap();
        assert_eq!(got.as_deref(), Some(&b"hybrid-hit"[..]));
    }

    #[tokio::test]
    async fn hybrid_pool_disk_miss_bumps_counter() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pools = hybrid_pools(dir.path().to_path_buf());
        let store = FoyerStore::open(&pools).await.expect("open");
        let baseline = crate::metrics::DISK_MISSES_TOTAL
            .with_label_values(&["rowgroup"])
            .get();
        let key = k(61);
        assert!(store.get(Pool::RowGroup, &key).await.unwrap().is_none());
        let now = crate::metrics::DISK_MISSES_TOTAL
            .with_label_values(&["rowgroup"])
            .get();
        assert_eq!(
            now - baseline,
            1,
            "a miss on a hybrid pool must increment shelf_disk_misses_total"
        );
    }

    /// Regression: `evict` on a hybrid pool must reach the disk tier.
    ///
    /// Before the fix, `evict_in_pool` only consulted memory and
    /// short-circuited when the key had aged onto NVMe, returning
    /// `false` *and* skipping the Foyer `remove`. A subsequent
    /// `get` resurrected the bytes from disk — so operator eviction
    /// was a silent no-op for the dominant steady-state case on a
    /// hybrid pool.
    #[tokio::test]
    async fn hybrid_pool_evict_after_memory_eviction_still_removes_from_disk() {
        // Tiny DRAM budget so the second insert forces the first
        // entry out of memory onto NVMe; large disk budget so the
        // demoted entry definitely lands on the disk ring.
        let dir = tempfile::tempdir().expect("tempdir");
        let pools = PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 1 << 20,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 16 * 1024,
                nvme_dir: dir.path().to_path_buf(),
                nvme_bytes: 64 * 1024 * 1024,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
                compression: crate::config::CompressionConfig::default(),
            },
        };
        let store = FoyerStore::open(&pools).await.expect("open");

        let victim = k(70);

        store
            .insert(
                Pool::RowGroup,
                victim.clone(),
                Bytes::from(vec![0xAA; 8192]),
            )
            .await
            .unwrap();
        // Insert a filler so `victim` is demoted out of DRAM.
        store
            .insert(Pool::RowGroup, k(71), Bytes::from(vec![0xBB; 8192]))
            .await
            .unwrap();

        // Both keys together exceed the DRAM budget so Foyer demotes
        // the older one to the NVMe tier; confirm it is still
        // reachable via a regular `get` before we evict.
        let warm = store
            .get(Pool::RowGroup, &victim)
            .await
            .unwrap()
            .expect("victim must still be present (DRAM+NVMe)");
        assert_eq!(warm.len(), 8192);

        // `evict` now reports true for a disk-only key and actually
        // removes it from both tiers.
        assert!(
            store.evict(Pool::RowGroup, &victim).await,
            "evict must report residency for a disk-tier entry"
        );
        assert!(
            store.get(Pool::RowGroup, &victim).await.unwrap().is_none(),
            "evict must purge the disk copy, not only memory",
        );
    }

    /// Regression: pinning a key under one pool then a different
    /// pool must not silently succeed. The old `pin()` short-circuited
    /// on "key already in pin_set" without checking the pool, so an
    /// operator could pin the same key against both pools and only
    /// the first one actually did anything.
    #[tokio::test]
    async fn pin_rejects_wrong_pool_after_idempotent_same_pool_pin() {
        let store = new_store().await;
        let key = k(72);
        store
            .insert(Pool::RowGroup, key.clone(), Bytes::from_static(b"xyz"))
            .await
            .unwrap();

        // First pin succeeds; second is idempotent (same pool → ok).
        assert!(store.pin(Pool::RowGroup, &key));
        assert!(store.pin(Pool::RowGroup, &key));

        // Third pin names the *other* pool. The key is not resident
        // in metadata, so the contract says `false`. Prior behaviour
        // was a false-positive `true`.
        assert!(
            !store.pin(Pool::Metadata, &key),
            "wrong-pool pin on an already-pinned key must return false"
        );

        // The pin-set still reflects the original pool only.
        assert_eq!(store.pinned_count(), 1);
        let entries = store.pinned_keys();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, Pool::RowGroup);
        assert_eq!(&entries[0].1, &key);
    }

    #[tokio::test]
    async fn dram_only_miss_does_not_bump_disk_counter() {
        let store = new_store().await; // nvme_bytes=0
        let baseline = crate::metrics::DISK_MISSES_TOTAL
            .with_label_values(&["rowgroup"])
            .get();
        let key = k(62);
        assert!(store.get(Pool::RowGroup, &key).await.unwrap().is_none());
        let now = crate::metrics::DISK_MISSES_TOTAL
            .with_label_values(&["rowgroup"])
            .get();
        assert_eq!(
            now, baseline,
            "DRAM-only pools must not bump the disk-miss counter"
        );
    }

    /// SHELF-A5 — capacity evictions emitted by Foyer's
    /// `EventListener` must increment
    /// `shelf_evictions_total{pool, reason="capacity"}`. We construct
    /// a 4 KiB DRAM-only rowgroup pool, then insert 16 × 1 KiB
    /// entries with distinct keys; Foyer's eviction policy will
    /// release at least a handful of older ones from memory.
    #[tokio::test]
    async fn capacity_evictions_increment_counter() {
        let cfg = PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 1 << 20,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 4 * 1024,
                nvme_dir: std::path::PathBuf::from("/tmp/unused"),
                nvme_bytes: 0,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
                compression: crate::config::CompressionConfig::default(),
            },
        };
        let store = FoyerStore::open(&cfg).await.expect("open small pool");

        let baseline = crate::metrics::EVICTIONS_TOTAL
            .with_label_values(&["rowgroup", "capacity"])
            .get();

        // 16 × 1 KiB = 16 KiB into a 4 KiB pool ⇒ at least 12 evictions.
        let payload = Bytes::from(vec![0u8; 1024]);
        for seed in 0u8..16 {
            let key = k(seed);
            store
                .insert(Pool::RowGroup, key, payload.clone())
                .await
                .expect("insert");
        }

        // Foyer's release callback fires synchronously inside `insert`,
        // but a small amount of work is dispatched onto its background
        // thread; yield once so the counter is settled before we read.
        tokio::task::yield_now().await;

        let now = crate::metrics::EVICTIONS_TOTAL
            .with_label_values(&["rowgroup", "capacity"])
            .get();
        assert!(
            now > baseline,
            "capacity evictions must climb past baseline ({baseline}); got {now}"
        );
    }

    /// Companion check: an explicit `evict` still bumps the
    /// `reason="admin"` line. Two label values can both move at the
    /// same time — see the doc comment on
    /// [`FoyerStore::evict_in_pool`] — but the admin counter must
    /// always be the one to advance for an explicit call.
    #[tokio::test]
    async fn admin_eviction_increments_admin_counter() {
        let store = new_store().await;
        let key = k(63);
        store
            .insert(Pool::RowGroup, key.clone(), Bytes::from_static(b"x"))
            .await
            .unwrap();

        // EVICTIONS_TOTAL is a process-global Prometheus counter,
        // so other tests running in parallel may also bump it. We
        // therefore assert "moved forward" rather than an exact
        // delta of 1.
        let baseline_admin = crate::metrics::EVICTIONS_TOTAL
            .with_label_values(&["rowgroup", "admin"])
            .get();
        assert!(store.evict(Pool::RowGroup, &key).await);
        let now_admin = crate::metrics::EVICTIONS_TOTAL
            .with_label_values(&["rowgroup", "admin"])
            .get();
        assert!(
            now_admin > baseline_admin,
            "explicit evict must bump reason=admin (baseline {baseline_admin}, now {now_admin})",
        );
    }

    // -----------------------------------------------------------------
    // **B1** — rowgroup zstd compression coverage.
    //
    // Every test below exercises a `compression.enabled = true`
    // RowGroupPoolConfig: we want byte-identity round-trips, the
    // pin-set's recorded length to be the *decoded* size, and
    // marker-file aborts to fire on mode flips.
    // -----------------------------------------------------------------

    fn compressible_payload(byte_len: usize) -> Bytes {
        // Repeating short JSON-ish chunk — compresses well under
        // zstd-3, mirrors the row-group dictionary-encoded string
        // distribution well enough for unit-test purposes.
        let unit = br#"{"row":"abcdefghijklmnopqrstuvwxyz","val":1234567890}"#;
        let mut out = Vec::with_capacity(byte_len + unit.len());
        while out.len() < byte_len {
            out.extend_from_slice(unit);
        }
        out.truncate(byte_len);
        Bytes::from(out)
    }

    fn compressed_dram_pool() -> PoolsConfig {
        PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 1 << 20,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 8 * 1024 * 1024,
                nvme_dir: std::path::PathBuf::from("/tmp/unused"),
                nvme_bytes: 0,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
                compression: crate::config::CompressionConfig {
                    enabled: true,
                    ..Default::default()
                },
            },
        }
    }

    #[tokio::test]
    async fn compression_round_trip_is_byte_identical() {
        let store = FoyerStore::open(&compressed_dram_pool())
            .await
            .expect("open compressed DRAM pool");
        let key = k(80);
        let payload = compressible_payload(64 * 1024);
        store
            .insert(Pool::RowGroup, key.clone(), payload.clone())
            .await
            .unwrap();
        let got = store.get(Pool::RowGroup, &key).await.unwrap();
        assert_eq!(
            got.as_deref(),
            Some(&payload[..]),
            "compression encode/decode must be byte-identical",
        );
    }

    #[tokio::test]
    async fn compression_used_bytes_reflects_encoded_frame() {
        // The Foyer pool's `usage()` reports stored bytes, which are
        // post-encode. A highly compressible 64 KiB JSON-ish payload
        // should land at well under half its original size on zstd-3.
        let store = FoyerStore::open(&compressed_dram_pool())
            .await
            .expect("open compressed DRAM pool");
        let payload = compressible_payload(64 * 1024);
        store
            .insert(Pool::RowGroup, k(81), payload.clone())
            .await
            .unwrap();
        let used = store.used_bytes(Pool::RowGroup);
        assert!(used > 0, "used_bytes must reflect at least the header byte");
        assert!(
            (used as usize) < payload.len() / 2,
            "compression must shrink storage on highly redundant input: used={used}, payload={}",
            payload.len()
        );
    }

    #[tokio::test]
    async fn compression_pin_records_decoded_length() {
        let store = FoyerStore::open(&compressed_dram_pool())
            .await
            .expect("open compressed DRAM pool");
        let key = k(82);
        let payload = compressible_payload(64 * 1024);
        store
            .insert(Pool::RowGroup, key.clone(), payload.clone())
            .await
            .unwrap();
        assert!(store.pin(Pool::RowGroup, &key));
        assert_eq!(
            store.pinned_bytes(),
            payload.len() as u64,
            "pin-set must record the decoded payload length, not the encoded frame",
        );
    }

    #[tokio::test]
    async fn compression_outcomes_metric_advances_on_round_trip() {
        let store = FoyerStore::open(&compressed_dram_pool())
            .await
            .expect("open compressed DRAM pool");
        let baseline_in = crate::metrics::COMPRESS_BYTES_IN_TOTAL
            .with_label_values(&["rowgroup"])
            .get();
        let baseline_compressed = crate::metrics::COMPRESS_OUTCOMES_TOTAL
            .with_label_values(&["rowgroup", "compressed"])
            .get();
        let baseline_decompressed = crate::metrics::COMPRESS_OUTCOMES_TOTAL
            .with_label_values(&["rowgroup", "decompressed_ok"])
            .get();

        let payload = compressible_payload(8 * 1024);
        store
            .insert(Pool::RowGroup, k(83), payload.clone())
            .await
            .unwrap();
        assert!(store.get(Pool::RowGroup, &k(83)).await.unwrap().is_some());

        let now_in = crate::metrics::COMPRESS_BYTES_IN_TOTAL
            .with_label_values(&["rowgroup"])
            .get();
        let now_compressed = crate::metrics::COMPRESS_OUTCOMES_TOTAL
            .with_label_values(&["rowgroup", "compressed"])
            .get();
        let now_decompressed = crate::metrics::COMPRESS_OUTCOMES_TOTAL
            .with_label_values(&["rowgroup", "decompressed_ok"])
            .get();

        assert!(
            now_in - baseline_in >= payload.len() as u64,
            "shelf_compress_bytes_in_total must advance by at least the payload length",
        );
        assert!(
            now_compressed > baseline_compressed,
            "shelf_compress_outcomes_total{{outcome=compressed}} must advance",
        );
        assert!(
            now_decompressed > baseline_decompressed,
            "shelf_compress_outcomes_total{{outcome=decompressed_ok}} must advance",
        );
    }

    #[tokio::test]
    async fn marker_written_on_first_open_with_compression() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pools = PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 1 << 20,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 1 << 20,
                nvme_dir: dir.path().to_path_buf(),
                nvme_bytes: 64 * 1024 * 1024,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
                compression: crate::config::CompressionConfig {
                    enabled: true,
                    ..Default::default()
                },
            },
        };
        let _store = FoyerStore::open(&pools)
            .await
            .expect("open hybrid + compression");
        let marker_path = dir.path().join(".shelf-compression.json");
        let bytes = std::fs::read(&marker_path).expect("marker file must exist after open");
        let parsed: serde_json::Value =
            serde_json::from_slice(&bytes).expect("marker is valid JSON");
        assert_eq!(parsed["version"], 1);
        assert_eq!(parsed["descriptor"], "zstd@3");
    }

    #[tokio::test]
    async fn marker_mismatch_aborts_open() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Pre-populate the directory with a mismatched marker AND a
        // dummy region file so `dir_has_payload` is `true`. This
        // simulates the "operator flipped compression mode against a
        // populated NVMe ring" failure mode.
        std::fs::write(
            dir.path().join(".shelf-compression.json"),
            br#"{"version":1,"descriptor":"zstd@3","min_size_bytes":256}"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("region-0.bin"), b"foyer-payload").unwrap();
        let pools = PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 1 << 20,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 1 << 20,
                nvme_dir: dir.path().to_path_buf(),
                nvme_bytes: 64 * 1024 * 1024,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
                // Compression OFF in config but marker says zstd@3 —
                // expected to fail loudly.
                compression: crate::config::CompressionConfig::default(),
            },
        };
        let err = FoyerStore::open(&pools)
            .await
            .expect_err("mismatched compression marker must abort open");
        let message = format!("{err}");
        assert!(
            message.contains("compression marker") || message.contains("compression is disabled"),
            "expected marker-mismatch diagnostic, got: {message}"
        );
    }

    #[tokio::test]
    async fn marker_missing_with_payload_and_compression_on_aborts() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Pre-existing payload, no marker — operator flipping
        // compression on against an unmarked populated ring.
        std::fs::write(dir.path().join("region-0.bin"), b"foyer-payload").unwrap();
        let pools = PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 1 << 20,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 1 << 20,
                nvme_dir: dir.path().to_path_buf(),
                nvme_bytes: 64 * 1024 * 1024,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
                compression: crate::config::CompressionConfig {
                    enabled: true,
                    ..Default::default()
                },
            },
        };
        let err = FoyerStore::open(&pools)
            .await
            .expect_err("compression-on against unmarked populated ring must abort");
        let message = format!("{err}");
        assert!(
            message.contains("pre-existing uncompressed data"),
            "expected ring-already-populated diagnostic, got: {message}"
        );
    }

    // ---------------------------------------------------------------
    // A2 (rc.7) — SIGTERM-only drain-aware admission tests.
    //
    // These cover the spec list from `agents/out/adr/0027-rc7-drain-
    // aware-admission.md`:
    //   1. drain_inactive_admits_normally
    //   2. drain_active_refuses_all_admits
    //   3. drain_active_reads_succeed
    //   4. drain_signal_flips_during_admit_loop
    //   5. drain_disabled_via_config_no_refuse
    //
    // We use a unique per-test pool label only where the test reads
    // back a metric child; otherwise we rely on the `inserted into
    // Foyer or not` observation, which is a strictly local check
    // and does not race other tests in the binary.
    // ---------------------------------------------------------------

    /// **A6 (rc.7)** test-stability mutex — drain (A2) and cooperative
    /// admission (A6) tests both read shared module-level
    /// `IntCounterVec` counters and assert exact deltas. With the
    /// number of `#[tokio::test]` cases in this module growing
    /// (12 drain/coop tests as of A6) cargo's parallel runner can
    /// race two same-counter tests against each other and produce
    /// a transient delta of 0 → ≥1 in the "must not move" arm.
    /// Serialising the counter-delta tests through a single
    /// `parking_lot::Mutex` resolves it without changing
    /// production code. Acquired at the *top* of each affected
    /// test; the counters' baselines are taken inside the
    /// critical section.
    static COUNTER_TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    fn drained_admit_refused_count() -> u64 {
        crate::metrics::ADMIT_REFUSED_TOTAL
            .with_label_values(&["draining"])
            .get()
    }

    /// Spec 1 — a healthy, non-draining store admits as normal under
    /// the always-admit policy. The drain gate is the cheap up-front
    /// check; any regression that mistakenly engages it on a healthy
    /// pod would make every test in this module fail, so the
    /// happy-path coverage here doubles as the primary canary.
    #[tokio::test]
    async fn drain_inactive_admits_normally() {
        let signal = crate::membership::DrainSignal::new();
        let store = FoyerStore::open(&test_pools())
            .await
            .expect("open")
            .with_drain(signal, true);
        let key = k(120);
        let outcome = store
            .get_or_fetch(Pool::RowGroup, key.clone(), &AlwaysAdmit, async {
                Ok((
                    Bytes::from_static(b"healthy"),
                    crate::coop_admission::FetchSource::Origin,
                ))
            })
            .await
            .expect("get_or_fetch");
        assert!(matches!(outcome, ReadOutcome::Miss(_)));
        // Insert side-effect: the key is now resident.
        let resident = store
            .get(Pool::RowGroup, &key)
            .await
            .expect("get")
            .expect("resident");
        assert_eq!(resident.as_ref(), b"healthy");
    }

    /// Spec 2 — a draining store refuses every admit, the byte
    /// payload still flows back to the caller (read keeps working
    /// against the origin), and Foyer never ends up holding the
    /// bytes for any subsequent read.
    #[tokio::test]
    async fn drain_active_refuses_all_admits() {
        let _guard = COUNTER_TEST_LOCK.lock();
        let signal = crate::membership::DrainSignal::new();
        signal.begin();
        let store = FoyerStore::open(&test_pools())
            .await
            .expect("open")
            .with_drain(signal.clone(), true);
        assert!(store.drain_refuses_admits());

        let baseline = drained_admit_refused_count();
        let key = k(121);
        let outcome = store
            .get_or_fetch(Pool::RowGroup, key.clone(), &AlwaysAdmit, async {
                Ok((
                    Bytes::from_static(b"refused"),
                    crate::coop_admission::FetchSource::Origin,
                ))
            })
            .await
            .expect("get_or_fetch");
        // Caller still receives the bytes — a cache miss, not an error.
        assert!(matches!(outcome, ReadOutcome::Miss(_)));
        assert_eq!(outcome.into_bytes(), Bytes::from_static(b"refused"));
        // Foyer must not have absorbed the insert.
        assert!(
            store
                .get(Pool::RowGroup, &key)
                .await
                .expect("get")
                .is_none(),
            "draining pod must not cache the refused admit",
        );
        // Counter ticks exactly once for the refused admit.
        assert_eq!(
            drained_admit_refused_count() - baseline,
            1,
            "shelf_admit_refused_total{{reason=\"draining\"}} must tick once per refused admit",
        );
    }

    /// Spec 3 — drain *only* gates writes/admits. Reads continue
    /// serving from cache for the grace window so peers get a
    /// chance to reroute without the local pod going dark on the
    /// data plane mid-drain. We pre-populate a key, flip drain on,
    /// and assert the read still hits.
    #[tokio::test]
    async fn drain_active_reads_succeed() {
        let signal = crate::membership::DrainSignal::new();
        let store = FoyerStore::open(&test_pools())
            .await
            .expect("open")
            .with_drain(signal.clone(), true);

        let key = k(122);
        // Warm the cache *before* drain flips so we know the
        // residence is from a healthy admit, not a racing one.
        store
            .insert(Pool::RowGroup, key.clone(), Bytes::from_static(b"warmed"))
            .await
            .expect("insert");

        signal.begin();
        assert!(store.drain_refuses_admits());

        let resident = store
            .get(Pool::RowGroup, &key)
            .await
            .expect("get")
            .expect("read must still serve from cache during drain");
        assert_eq!(resident.as_ref(), b"warmed");
    }

    /// Spec 4 — race / flip-mid-loop sanity. The drain bit is an
    /// `AtomicBool`: `try_admit`-style gates that read it once per
    /// call must observe the flip on the next iteration cleanly,
    /// never producing torn state. We model that by running a
    /// burst of admits, flipping mid-burst, and asserting the
    /// pre-flip admits inserted while the post-flip ones did not.
    #[tokio::test]
    async fn drain_signal_flips_during_admit_loop() {
        let _guard = COUNTER_TEST_LOCK.lock();
        let signal = crate::membership::DrainSignal::new();
        let store = FoyerStore::open(&test_pools())
            .await
            .expect("open")
            .with_drain(signal.clone(), true);

        let pre_baseline = drained_admit_refused_count();
        for ord in 0..3u8 {
            let key = k(140 + ord);
            store
                .get_or_fetch(Pool::RowGroup, key.clone(), &AlwaysAdmit, async {
                    Ok((
                        Bytes::from_static(b"pre-drain"),
                        crate::coop_admission::FetchSource::Origin,
                    ))
                })
                .await
                .expect("pre-drain admit");
            assert!(
                store
                    .get(Pool::RowGroup, &key)
                    .await
                    .expect("get")
                    .is_some(),
                "pre-drain admit (ord={ord}) must be cached"
            );
        }
        assert_eq!(
            drained_admit_refused_count() - pre_baseline,
            0,
            "no drain refusals before signal flips"
        );

        signal.begin();

        let post_baseline = drained_admit_refused_count();
        for ord in 0..3u8 {
            let key = k(150 + ord);
            store
                .get_or_fetch(Pool::RowGroup, key.clone(), &AlwaysAdmit, async {
                    Ok((
                        Bytes::from_static(b"post-drain"),
                        crate::coop_admission::FetchSource::Origin,
                    ))
                })
                .await
                .expect("post-drain admit");
            assert!(
                store
                    .get(Pool::RowGroup, &key)
                    .await
                    .expect("get")
                    .is_none(),
                "post-drain admit (ord={ord}) must NOT be cached"
            );
        }
        assert_eq!(
            drained_admit_refused_count() - post_baseline,
            3,
            "every post-flip admit must bump the refused counter exactly once",
        );
    }

    /// Spec 5 — `cache.drain.refuse_admits = false` is the
    /// operator escape hatch. Even with the signal active, admits
    /// must continue to flow into Foyer; the dedicated counter
    /// stays flat. The signal is still observable via
    /// [`crate::membership::DrainSignal::is_active`] so `/stats`
    /// keeps advertising drain (peers still rotate us out of
    /// their rings); this test asserts only the local admit-gate
    /// behaviour.
    #[tokio::test]
    async fn drain_disabled_via_config_no_refuse() {
        let _guard = COUNTER_TEST_LOCK.lock();
        let signal = crate::membership::DrainSignal::new();
        signal.begin();
        let store = FoyerStore::open(&test_pools())
            .await
            .expect("open")
            .with_drain(signal.clone(), false);

        // Effective gate: signal is active but disabled by config,
        // so the helper reports `false` and the admit flows.
        assert!(signal.is_active(), "raw signal still flipped");
        assert!(
            !store.drain_refuses_admits(),
            "config opt-out must short-circuit the gate"
        );

        let baseline = drained_admit_refused_count();
        let key = k(170);
        store
            .get_or_fetch(Pool::RowGroup, key.clone(), &AlwaysAdmit, async {
                Ok((
                    Bytes::from_static(b"escape-hatch"),
                    crate::coop_admission::FetchSource::Origin,
                ))
            })
            .await
            .expect("get_or_fetch");
        assert!(
            store
                .get(Pool::RowGroup, &key)
                .await
                .expect("get")
                .is_some(),
            "escape-hatch admit must still cache",
        );
        assert_eq!(
            drained_admit_refused_count(),
            baseline,
            "refused counter must not move while refuse_admits=false"
        );
    }

    // ---------------------------------------------------------------
    // **A6 (rc.7)** — cooperative peer admission integration tests.
    //
    // These exercise the gate at the `get_or_fetch` admit seam — the
    // unit-level probability assertions live in
    // `crate::coop_admission::tests`. Together they pin the two
    // invariants that matter for the operator-facing dashboards:
    //   * `FetchSource::Origin` admits unconditionally regardless of
    //     gate state — the counter `shelf_coop_peer_drops_total` must
    //     never tick on origin bytes.
    //   * `FetchSource::Peer` is the only path that can charge the new
    //     counters; with `replication_factor = 1` (default-ish) every
    //     peer byte still admits and the existing pre-A6 behaviour is
    //     preserved bit-for-bit.
    // ---------------------------------------------------------------

    fn coop_peer_admits_count() -> u64 {
        crate::metrics::COOP_PEER_ADMITS_TOTAL
            .with_label_values(&["rowgroup"])
            .get()
    }

    fn coop_peer_drops_count() -> u64 {
        crate::metrics::COOP_PEER_DROPS_TOTAL
            .with_label_values(&["rowgroup"])
            .get()
    }

    fn coop_reject_label_count() -> u64 {
        crate::metrics::ADMISSIONS_TOTAL
            .with_label_values(&["rowgroup", "reject_coop"])
            .get()
    }

    /// Origin-sourced bytes always admit, regardless of gate state.
    /// Even with `enabled = true` and `replication_factor = u32::MAX`
    /// (every peer admit dropped), origin admits flow through. This
    /// is the primary correctness invariant: A6 must be invisible to
    /// the read path on origin fetches.
    #[tokio::test]
    async fn coop_admission_origin_always_admits() {
        let store = FoyerStore::open(&test_pools())
            .await
            .expect("open")
            .with_coop_admission(crate::coop_admission::CoopAdmissionGate::with_seed(
                crate::coop_admission::CoopAdmissionConfig {
                    enabled: true,
                    replication_factor: u32::MAX,
                },
                0xC0_DE_C0_DE,
            ));
        let key = k(200);
        let outcome = store
            .get_or_fetch(Pool::RowGroup, key.clone(), &AlwaysAdmit, async {
                Ok((
                    Bytes::from_static(b"origin-bytes"),
                    crate::coop_admission::FetchSource::Origin,
                ))
            })
            .await
            .expect("get_or_fetch");
        assert!(matches!(outcome, ReadOutcome::Miss(_)));
        assert!(
            store
                .get(Pool::RowGroup, &key)
                .await
                .expect("get")
                .is_some(),
            "origin admit must cache regardless of gate state"
        );
    }

    /// `replication_factor = 1` ⇒ probability 1.0 ⇒ every peer-sourced
    /// admit flows. The dropped-counter must stay flat; the
    /// admit-counter must tick once per peer admit.
    #[tokio::test]
    async fn coop_admission_peer_factor_1_admits_every_byte() {
        let _guard = COUNTER_TEST_LOCK.lock();
        let admits_baseline = coop_peer_admits_count();
        let drops_baseline = coop_peer_drops_count();
        let store = FoyerStore::open(&test_pools())
            .await
            .expect("open")
            .with_coop_admission(crate::coop_admission::CoopAdmissionGate::with_seed(
                crate::coop_admission::CoopAdmissionConfig {
                    enabled: true,
                    replication_factor: 1,
                },
                0xAA_BB_CC_DD,
            ));
        let key = k(201);
        store
            .get_or_fetch(Pool::RowGroup, key.clone(), &AlwaysAdmit, async {
                Ok((
                    Bytes::from_static(b"peer-bytes"),
                    crate::coop_admission::FetchSource::Peer,
                ))
            })
            .await
            .expect("get_or_fetch");
        assert!(
            store
                .get(Pool::RowGroup, &key)
                .await
                .expect("get")
                .is_some(),
            "factor=1 must admit every peer byte"
        );
        assert_eq!(coop_peer_admits_count() - admits_baseline, 1);
        assert_eq!(coop_peer_drops_count() - drops_baseline, 0);
    }

    /// `replication_factor` large enough to drop every peer admit.
    /// We pin the seed and `u32::MAX` so the modulo always lands
    /// off-zero on the first draw; the drop counter must tick and
    /// the bytes must NOT cache. The bytes themselves still flow
    /// back to the caller (the read path is untouched).
    #[tokio::test]
    async fn coop_admission_peer_dropped_does_not_cache() {
        let _guard = COUNTER_TEST_LOCK.lock();
        let admits_baseline = coop_peer_admits_count();
        let drops_baseline = coop_peer_drops_count();
        let reject_baseline = coop_reject_label_count();
        let store = FoyerStore::open(&test_pools())
            .await
            .expect("open")
            .with_coop_admission(crate::coop_admission::CoopAdmissionGate::with_seed(
                crate::coop_admission::CoopAdmissionConfig {
                    enabled: true,
                    replication_factor: u32::MAX,
                },
                // Seed chosen so the first draw % u32::MAX != 0 with
                // overwhelming probability — the gate drops the admit.
                0xDEAD_F00D,
            ));
        let key = k(202);
        let outcome = store
            .get_or_fetch(Pool::RowGroup, key.clone(), &AlwaysAdmit, async {
                Ok((
                    Bytes::from_static(b"peer-dropped"),
                    crate::coop_admission::FetchSource::Peer,
                ))
            })
            .await
            .expect("get_or_fetch");
        // Caller still receives the bytes — A6 only gates the admit.
        assert_eq!(outcome.into_bytes(), Bytes::from_static(b"peer-dropped"));
        // Foyer must NOT have absorbed the insert.
        assert!(
            store
                .get(Pool::RowGroup, &key)
                .await
                .expect("get")
                .is_none(),
            "dropped peer admit must not be cached"
        );
        // Counter parity: drop ticked once, admit-counter flat,
        // `reject_coop` decision label ticked once.
        assert_eq!(
            coop_peer_drops_count() - drops_baseline,
            1,
            "shelf_coop_peer_drops_total must tick on dropped peer admit"
        );
        assert_eq!(
            coop_peer_admits_count() - admits_baseline,
            0,
            "shelf_coop_peer_admits_total must NOT tick on dropped peer admit"
        );
        assert_eq!(
            coop_reject_label_count() - reject_baseline,
            1,
            "shelf_admissions_total{{decision=reject_coop}} must tick"
        );
    }

    /// Disabled gate: `enabled = false` ⇒ peer bytes admit
    /// unconditionally and neither A6 counter ticks. This covers the
    /// OSS default — a freshly deployed pod with the chart's
    /// `cache.coopAdmission.enabled = false` must look identical to
    /// pre-A6 from the Foyer admit perspective.
    #[tokio::test]
    async fn coop_admission_disabled_admits_peer_bytes() {
        let _guard = COUNTER_TEST_LOCK.lock();
        let admits_baseline = coop_peer_admits_count();
        let drops_baseline = coop_peer_drops_count();
        let store = FoyerStore::open(&test_pools())
            .await
            .expect("open")
            .with_coop_admission(crate::coop_admission::CoopAdmissionGate::with_seed(
                crate::coop_admission::CoopAdmissionConfig {
                    enabled: false,
                    replication_factor: u32::MAX,
                },
                0x42,
            ));
        let key = k(203);
        store
            .get_or_fetch(Pool::RowGroup, key.clone(), &AlwaysAdmit, async {
                Ok((
                    Bytes::from_static(b"oss-default"),
                    crate::coop_admission::FetchSource::Peer,
                ))
            })
            .await
            .expect("get_or_fetch");
        assert!(
            store
                .get(Pool::RowGroup, &key)
                .await
                .expect("get")
                .is_some(),
            "disabled gate must admit peer bytes"
        );
        // Disabled gate short-circuits BEFORE the counter bumps in
        // `get_or_fetch` (the gate returns `true` from
        // `should_admit_peer_bytes`); but the admit-counter still
        // ticks (the gate said admit, so `coop_admit = true`).
        assert_eq!(coop_peer_admits_count() - admits_baseline, 1);
        assert_eq!(coop_peer_drops_count() - drops_baseline, 0);
    }
}
