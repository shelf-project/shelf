//! Foyer-backed `Store` — the core caching surface.
//!
//! Ticket ownership:
//! - SHELF-03 — DRAM-only `pool.metadata` with SIEVE (Foyer built-in).
//! - SHELF-17 — separate DRAM pool for Iceberg manifests / Parquet
//!   footers / page indexes. ADR-0008 mandates exactly two pools in v1.
//! - SHELF-18 — hybrid DRAM + NVMe `pool.rowgroup` with S3-FIFO
//!   eviction per ADR-0009.
//! - SHELF-04 — content-addressed keys:
//!   `sha256(etag_bytes || le_u64(offset) || le_u64(length) || le_u32(rg_ordinal))`.
//!
//! The trait-first layout is deliberate so we can unit-test consumers
//! (the HTTP handler, the admission policy) against an in-memory
//! implementation without linking Foyer.

use std::fmt::Debug;

use bytes::Bytes;

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
/// The bytes are `sha256(etag || offset || length || rg_ordinal)` per
/// SHELF-04. Row-group-ordinal = 0 for non-columnar ranges (manifests,
/// footers) so the same function covers every pool.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Key(pub [u8; 32]);

impl Key {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
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

    /// Explicit eviction (e.g. from `shelfctl evict`).
    fn evict(
        &self,
        pool: Pool,
        key: &Key,
    ) -> impl std::future::Future<Output = crate::Result<()>> + Send;

    /// Bytes used per pool, for `/stats` + HRW capacity weighting.
    fn used_bytes(&self, pool: Pool) -> u64;

    /// Pool capacity in bytes.
    fn capacity_bytes(&self, pool: Pool) -> u64;
}

/// Foyer-backed `Store` implementation. The type is public but
/// intentionally opaque; all construction goes through `open()`.
#[derive(Debug)]
pub struct FoyerStore {
    // Fields elided until SHELF-03/17/18 land. The final shape will be
    // roughly:
    //   metadata: foyer::Cache<Key, Bytes>,
    //   rowgroup: foyer::HybridCache<Key, Bytes>,
    //   metrics:  Arc<crate::metrics::StoreMetrics>,
    _private: (),
}

impl FoyerStore {
    /// Open the Foyer pools from the daemon config.
    pub async fn open(_config: &crate::config::PoolsConfig) -> crate::Result<Self> {
        todo!(
            "SHELF-03 + SHELF-17 + SHELF-18: store: wire two Foyer pools \
             (metadata: DRAM+SIEVE; rowgroup: DRAM+NVMe+S3-FIFO); see \
             03-plan.md §4 SHELF-03/17/18 and \
             agents/out/adr/0008-two-pools-in-v1.md + 0009-foyer-s3-fifo-over-gl-cache-custom.md"
        )
    }
}

impl Store for FoyerStore {
    async fn get(&self, _pool: Pool, _key: &Key) -> crate::Result<Option<Bytes>> {
        todo!(
            "SHELF-06: store: Foyer get; see 03-plan.md §4 SHELF-06 \
             and adr/0009"
        )
    }

    async fn insert(&self, _pool: Pool, _key: Key, _bytes: Bytes) -> crate::Result<()> {
        todo!(
            "SHELF-06: store: Foyer insert with admission check; see \
             03-plan.md §4 SHELF-06 + SHELF-25"
        )
    }

    async fn evict(&self, _pool: Pool, _key: &Key) -> crate::Result<()> {
        todo!("SHELF-23: store: explicit eviction path for shelfctl evict; see 03-plan.md §4 SHELF-23")
    }

    fn used_bytes(&self, _pool: Pool) -> u64 {
        todo!("SHELF-08: store: expose used_bytes for Prometheus + /stats; see 03-plan.md §4 SHELF-08/SHELF-20")
    }

    fn capacity_bytes(&self, _pool: Pool) -> u64 {
        todo!("SHELF-08: store: expose capacity_bytes for Prometheus + /stats; see 03-plan.md §4 SHELF-08/SHELF-20")
    }
}

/// Derive a content-addressed `Key` from the SHELF-04 tuple.
///
/// Body lands in SHELF-04 together with the Java golden-vector test.
pub fn key_from_tuple(
    _etag: &[u8],
    _offset: u64,
    _length: u64,
    _rg_ordinal: u32,
) -> Key {
    todo!(
        "SHELF-04: store: implement sha256(etag || le_u64(offset) || \
         le_u64(length) || le_u32(rg_ordinal)); cross-check with \
         clients/trino KeyTest#roundtrip; see 03-plan.md §4 SHELF-04"
    )
}
