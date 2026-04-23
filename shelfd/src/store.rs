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
use sha2::{Digest, Sha256};

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
    ];

    #[test]
    fn golden_vectors_match_fixture() {
        let fixture =
            include_str!("../tests/fixtures/shelf04_golden_vectors.txt");
        let expected: Vec<&str> = fixture
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();
        assert_eq!(
            expected.len(),
            GOLDEN_INPUTS.len(),
            "fixture must have one line per golden input"
        );
        for ((etag, off, len, ord), want) in GOLDEN_INPUTS.iter().zip(expected)
        {
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
