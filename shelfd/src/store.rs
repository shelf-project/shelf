//! Foyer-backed `Store` — the core caching surface.
//!
//! Ticket ownership:
//! - SHELF-03 — DRAM-only `pool.metadata` with SIEVE (Foyer built-in).
//! - SHELF-17 — separate DRAM pool for Iceberg manifests / Parquet
//!   footers / page indexes. ADR-0008 mandates exactly two pools in v1.
//! - SHELF-18 — hybrid DRAM + NVMe `pool.rowgroup` with S3-FIFO
//!   eviction per ADR-0009. **Deferred in phase-0 — rowgroup is DRAM
//!   only; NVMe tier lands once we have a PVC-backed test loop.**
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
use std::sync::{Arc, Weak};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
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
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
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
    /// `pool` before the call. Synchronous: Foyer's `remove` and the
    /// pin-set `RwLock` are both in-memory and lock-based; avoiding
    /// `async` here keeps the admin handlers and tests from having
    /// to `await` trivially-resolvable calls.
    fn evict(&self, pool: Pool, key: &Key) -> bool;

    // SHELF-23 + SHELF-24 pin-set surface. The pin-set is held
    // separately from the two Foyer caches so that evicting the
    // cached bytes does not silently drop the pin.
    //
    // All of these are synchronous for the same reason as `evict`.

    /// Pin a key in `pool`. Returns `true` iff the entry was resident
    /// (or already pinned — idempotent).
    fn pin(&self, pool: Pool, key: &Key) -> bool;

    /// Remove a key from the pin-set. Returns `true` iff it was pinned.
    fn unpin(&self, key: &Key) -> bool;

    /// Membership test.
    fn is_pinned(&self, key: &Key) -> bool;

    /// Snapshot of all pinned keys — used by the pin-list loader.
    fn pinned_keys(&self) -> Vec<Key>;

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

/// Pool-segmented Foyer cache. Phase-0 holds both pools as DRAM-only
/// `foyer::Cache<Key, Bytes>`; SHELF-18 will swap `rowgroup` for
/// `HybridCache` once the PVC-backed test loop exists.
#[derive(Debug)]
pub struct FoyerStore {
    metadata: foyer::Cache<Key, Bytes>,
    rowgroup: foyer::Cache<Key, Bytes>,
    metadata_capacity: u64,
    rowgroup_capacity: u64,
    inflight: InflightMap,
    /// SHELF-24 allowlist. Held separately from the two Foyer caches
    /// so that (1) eviction of the bytes does not also unpin the key
    /// and (2) the admin surface can refuse pins for keys that are
    /// not yet resident.
    pin_set: RwLock<HashMap<Key, u64>>,
}

impl FoyerStore {
    /// Open the Foyer pools from the daemon config.
    ///
    /// Phase-0: both pools are DRAM-only `foyer::Cache`. The weighter
    /// charges each entry its byte length so eviction honours the
    /// byte budget rather than entry count. NVMe hybrid-tier wiring
    /// for `rowgroup` lands with SHELF-18.
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

        let metadata = foyer::CacheBuilder::new(metadata_capacity as usize)
            .with_weighter(|_k: &Key, v: &Bytes| v.len())
            .build();
        let rowgroup = foyer::CacheBuilder::new(rowgroup_capacity as usize)
            .with_weighter(|_k: &Key, v: &Bytes| v.len())
            .build();

        Ok(Self {
            metadata,
            rowgroup,
            metadata_capacity,
            rowgroup_capacity,
            inflight: Mutex::new(HashMap::new()),
            pin_set: RwLock::new(HashMap::new()),
        })
    }

    fn cache_for(&self, pool: Pool) -> &foyer::Cache<Key, Bytes> {
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
        if let Some(bytes) = self.get(pool, &key).await? {
            return Ok(ReadOutcome::Hit(bytes));
        }

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
            Err(e) => return Err(crate::Error::Origin(e)),
        };

        let ctx = crate::admission::AdmissionContext {
            pool,
            key: &key,
            size_bytes: bytes.len() as u64,
            // SHELF-24: pin-set is the single source of truth for
            // the admission bypass flag.
            pinned: self.is_pinned(&key),
        };
        if admission.decide(&ctx) == crate::admission::AdmissionDecision::Admit {
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
    /// is resident in `pool` (or was already pinned — idempotent).
    /// `false` signals 404 to the admin handler so operators see
    /// typos rather than silent no-ops.
    pub fn pin(&self, pool: Pool, key: &Key) -> bool {
        // Idempotent: pinning an already-pinned key returns true
        // without re-reading the cache. The existing byte count is
        // trusted — re-inserting the same key with a potentially
        // different payload would be an ADR-0003 violation anyway.
        if self.pin_set.read().contains_key(key) {
            return true;
        }
        let len = match self.cache_for(pool).get(key) {
            Some(entry) => entry.value().len() as u64,
            None => return false,
        };
        self.pin_set.write().insert(key.clone(), len);
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

    /// Snapshot used by the pin-list loader when diffing.
    pub fn pinned_keys(&self) -> Vec<Key> {
        self.pin_set.read().keys().cloned().collect()
    }

    /// Distinct pinned key count regardless of residency.
    pub fn pinned_count(&self) -> usize {
        self.pin_set.read().len()
    }

    /// Sum of the byte lengths recorded when each pinned key was
    /// installed. O(N) over the pin-set; no cache lookups.
    pub fn pinned_bytes(&self) -> u64 {
        self.pin_set.read().values().copied().sum()
    }

    /// Evict a key from `pool`. Preserves the pin-set so a subsequent
    /// re-fetch still goes through admission with `ctx.pinned = true`.
    /// Returns `true` iff the key was resident in `pool`.
    pub fn evict_in_pool(&self, pool: Pool, key: &Key) -> bool {
        let cache = self.cache_for(pool);
        let had = cache.get(key).is_some();
        if had {
            cache.remove(key);
        }
        had
    }
}

/// Whether a [`FoyerStore::get_or_fetch`] returned the bytes straight
/// from a warm pool or after an origin fetch.
#[derive(Debug)]
pub enum ReadOutcome {
    Hit(Bytes),
    Miss(Bytes),
}

impl ReadOutcome {
    pub fn into_bytes(self) -> Bytes {
        match self {
            ReadOutcome::Hit(b) | ReadOutcome::Miss(b) => b,
        }
    }

    pub fn is_hit(&self) -> bool {
        matches!(self, ReadOutcome::Hit(_))
    }
}

impl Store for FoyerStore {
    async fn get(&self, pool: Pool, key: &Key) -> crate::Result<Option<Bytes>> {
        let cache = self.cache_for(pool);
        Ok(cache.get(key).map(|entry| entry.value().clone()))
    }

    async fn insert(&self, pool: Pool, key: Key, bytes: Bytes) -> crate::Result<()> {
        let cache = self.cache_for(pool);
        cache.insert(key, bytes);
        Ok(())
    }

    fn evict(&self, pool: Pool, key: &Key) -> bool {
        // Forwards to the inherent method so the pool-targeting logic
        // lives in one place.
        FoyerStore::evict_in_pool(self, pool, key)
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

    fn pinned_keys(&self) -> Vec<Key> {
        FoyerStore::pinned_keys(self)
    }

    fn pinned_bytes(&self) -> u64 {
        FoyerStore::pinned_bytes(self)
    }

    fn pinned_count(&self) -> usize {
        FoyerStore::pinned_count(self)
    }

    fn used_bytes(&self, pool: Pool) -> u64 {
        self.cache_for(pool).usage() as u64
    }

    fn capacity_bytes(&self, pool: Pool) -> u64 {
        match pool {
            Pool::Metadata => self.metadata_capacity,
            Pool::RowGroup => self.rowgroup_capacity,
        }
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
        assert!(store.evict(Pool::Metadata, &key));
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
        assert!(store.evict(Pool::RowGroup, &key));
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
        assert!(!store.evict(Pool::RowGroup, &key));
        assert!(!store.evict(Pool::Metadata, &key));
    }
}
