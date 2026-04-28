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
type InflightMap = Mutex<HashMap<(Pool, Key), Weak<OnceCell<Result<Bytes, String>>>>>;

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
            crate::metrics::LODC_DROPS_TOTAL
                .with_label_values(&[pool_label(Pool::RowGroup)])
                .inc_by(0);
            crate::metrics::LODC_INFLIGHT_BYTES
                .with_label_values(&[pool_label(Pool::RowGroup)])
                .set(0);
            crate::metrics::LODC_QUEUE_DEPTH
                .with_label_values(&[pool_label(Pool::RowGroup)])
                .set(0);
            Some(bp)
        };

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

        Ok(Self {
            metadata,
            rowgroup,
            rowgroup_lodc_bp,
            inflight: Mutex::new(HashMap::new()),
            pin_set: RwLock::new(HashMap::new()),
        })
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
    pub async fn get_or_fetch<A, F>(
        &self,
        pool: Pool,
        key: Key,
        admission: &A,
        fetch: F,
    ) -> crate::Result<ReadOutcome>
    where
        A: crate::admission::AdmissionPolicy + ?Sized,
        F: Future<Output = crate::Result<Bytes>> + Send,
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
                return Ok(ReadOutcome::Hit(bytes, tier.into()));
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

        let bytes = match slot.clone() {
            Ok(b) => b,
            // S2: the leader's original `crate::Error` variant was
            // stringified at the `OnceCell<Result<Bytes, String>>`
            // boundary, so we can no longer recover it. Surface as
            // `Error::Singleflight` rather than `Error::Origin` so
            // the `shelfd_error_total{component}` series doesn't
            // mis-attribute Foyer / membership / admission failures
            // to the S3 origin.
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

        let admit = policy_admit && lodc_admit;
        let decision_label = if admit {
            "admit"
        } else if !lodc_admit {
            // SHELF-21e — the policy said admit but back-pressure
            // dropped the insert. Keep this label distinct from
            // `reject_size` / `reject_other` so dashboards can
            // tell "policy rejected" apart from "back-pressure
            // dropped"; the latter is an ops signal that NVMe
            // drain is falling behind ingress.
            "reject_lodc"
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

    fn acquire_inflight_cell(&self, pool: Pool, key: &Key) -> Arc<OnceCell<Result<Bytes, String>>> {
        let mut guard = self.inflight.lock();
        if let Some(weak) = guard.get(&(pool, key.clone())) {
            if let Some(a) = weak.upgrade() {
                return a;
            }
        }
        let a: Arc<OnceCell<Result<Bytes, String>>> = Arc::new(OnceCell::new());
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
        let len = match self.handle_for(pool).memory_get_len(key) {
            Some(len) => len,
            None => return false,
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
            Some((bytes, Tier::Dram)) => Ok(Some(bytes)),
            Some((bytes, Tier::Disk)) => {
                crate::metrics::DISK_HITS_TOTAL
                    .with_label_values(&[pool_label(pool)])
                    .inc();
                Ok(Some(bytes))
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
        self.handle_for(pool).insert(key, bytes);
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
                Ok(Bytes::from_static(b"abc"))
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
                Ok(Bytes::from_static(b"xyz"))
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
                        Ok(Bytes::from_static(b"coalesced"))
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
}
