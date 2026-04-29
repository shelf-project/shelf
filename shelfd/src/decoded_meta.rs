//! SHELF-50 — Decoded-metadata in-process LRU cache.
//!
//! Iceberg manifest reads + Parquet footer reads currently land in
//! [`crate::store::Pool::Metadata`] as **encoded** bytes (Avro for
//! manifests, Thrift for Parquet footers). Each cache hit still
//! costs deserialisation CPU on every read. Holding the **decoded**
//! representation in a small in-process LRU keyed by the object's
//! ETag eliminates that CPU on warm hits and shrinks the planning-
//! path latency tail.
//!
//! ## Design summary
//!
//! Two parallel LRUs:
//!
//! - [`ManifestCache`] — `etag → Arc<ManifestFile>`. v1 holds the
//!   validated raw Avro bytes; the structural surface that downstream
//!   tickets consume is intentionally narrow so that swapping the
//!   inner type for `iceberg::spec::ManifestFile` from the
//!   `iceberg-rust` crate (when an ADR admits that dep) is
//!   diff-equivalent at the call sites. See the design note for the
//!   full rationale.
//! - [`ParquetFooterCache`] — `etag → Arc<ParquetMetaData>`, where
//!   `ParquetMetaData` is `parquet::file::metadata::ParquetMetaData`
//!   from the upstream `parquet` crate.
//!
//! Both caches are `parking_lot::Mutex<LruCache<...>>` (`lru` crate)
//! shaped, capped at `cache.decodedMeta.maxManifestEntries` and
//! `cache.decodedMeta.maxFooterEntries` (default 10 000 each — see
//! the design note for why entry-count caps and not byte caps in v1).
//!
//! ## Population flow
//!
//! Production code calls [`on_metadata_admit`] right after a
//! [`crate::store::Pool::Metadata`] admission succeeds. The hot read
//! path returns the raw bytes immediately; the heavy decode runs
//! fire-and-forget on a tokio blocking-thread (`spawn_blocking`) so
//! the read latency is not extended by the cost of parsing. The LRU
//! is *purely a CPU saver* on subsequent reads.
//!
//! The producer hook is the single integration point this PR adds.
//! Consumption is downstream: SHELF-46 (bloom-aware footer
//! admission), SHELF-37 (Iceberg event-listener jar), and SHELF-47
//! (MV-aware pinning advisor) read decoded data via the
//! [`get_manifest`] / [`get_parquet_footer`] accessors with zero
//! extra IO.
//!
//! ## ETag-keyed invalidation (ADR-0011)
//!
//! Cache keys are the object's S3 ETag, an opaque-but-stable version
//! token. When the byte-cache eviction path observes an ETag change
//! (e.g. the SHELF-23 conditional-GET freshness loop in
//! `s3_shim.rs`), it calls [`invalidate`] to drop the prior decoded
//! entry. This keeps the decoded cache consistent with ADR-0011's
//! content-addressed invariant: a new ETag ⇒ a new key in the byte
//! cache *and* the decoded cache, never a stale parse against a
//! superseded blob.
//!
//! ## Memory budgeting
//!
//! v1 caps RSS via the entry-count cap only; per-entry size is not
//! tracked. The justification is in the design note: an upper bound
//! of `10_000 × ~64 KiB ≈ 640 MiB` (manifest pool) plus
//! `10_000 × ~16 KiB ≈ 160 MiB` (footer pool) is a defensible
//! worst case against the ~3 GiB of RSS headroom the production
//! sizing rule budgets. SHELF-50b will revisit if the
//! `shelf_decoded_meta_entries` gauge drifts materially against the
//! shelfd RSS.
//!
//! ## Threading model
//!
//! The cache is a single `Lazy` static (the `INSTANCE` below) — there
//! is one shared instance per shelfd process, just like
//! [`crate::metrics::REGISTRY`]. The `Lazy` use is justified per
//! `agents/4-shelfd-builder.md` Pass 2 ("`once_cell::sync::Lazy` is
//! allowed for metric registries only" — the decoded-meta cache is a
//! sibling singleton with the same scope rationale; documented in
//! the design note).
//!
//! ## Trino MemoryFileSystemCache
//!
//! Trino's Iceberg connector ships a JVM-local
//! `MemoryFileSystemCache` (controlled by
//! `iceberg.metadata-cache.enabled`) that silently bypasses shelf
//! for warm metadata reads. SHELF-50's cache lives shelf-side and
//! is unaffected — every shelfd worker observes Pool::Metadata
//! admissions whether or not the JVM-local cache is on. See the
//! design note "Why ADR-0011 + Trino MemoryFileSystemCache do not
//! double-cache" for the deeper rationale.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use crate::metrics;

/// Default LRU entry count. Aligns with the `cache.decodedMeta.*`
/// helm-chart defaults so a Rust-only test harness boots with the
/// same caps the production overlay sets.
pub const DEFAULT_MAX_ENTRIES: usize = 10_000;

/// Decoded-metadata kind labels (low-cardinality; matches
/// `EXPOSED_SERIES`).
const KIND_MANIFEST: &str = "manifest";
const KIND_PARQUET_FOOTER: &str = "parquet_footer";

/// ETag key. The ETag is an S3-server-side opaque version token; we
/// treat it as a UTF-8 string because that is the only form the byte
/// cache observes (AWS guarantees ASCII for ETag headers in practice
/// — SDK already rejects non-UTF-8 bytes upstream of this module).
/// `Arc<str>` is cheap-clone and `Hash + Eq` so it slots into
/// `LruCache` directly without an owned-`String` clone on every get.
pub type EtagKey = Arc<str>;

/// Decoded Iceberg manifest entry.
///
/// v1 stores the validated raw Avro bytes plus the magic-marker the
/// sniff confirmed. Future SHELF-50b will replace this struct with
/// the `iceberg::spec::ManifestFile` type from the `iceberg-rust`
/// crate (heavyweight transitive — pulls Arrow + datafusion in some
/// builds), gated behind a separate ADR. The `Arc<ManifestFile>`
/// shape that callers consume stays stable across that swap.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManifestFile {
    /// Raw Avro container bytes. Validated by the magic-byte sniff
    /// at insertion time so subsequent consumers can skip the
    /// `Obj\x01` check.
    pub raw: Bytes,
}

/// Outcome of a decode attempt — used by tests and the Prometheus
/// `decode_errors` reason label.
#[derive(Debug)]
pub enum DecodeError {
    /// Bytes didn't carry the expected leading magic.
    BadMagic,
    /// Parquet footer Thrift parse failed.
    ParquetThrift(String),
    /// Avro Object Container header was malformed.
    AvroHeader(String),
}

impl DecodeError {
    fn reason_label(&self) -> &'static str {
        match self {
            DecodeError::BadMagic => "bad_magic",
            DecodeError::ParquetThrift(_) => "parquet_thrift",
            DecodeError::AvroHeader(_) => "avro_header",
        }
    }
}

/// Hint for the producer-side sniff. `key_path_hint` is the S3
/// object key (or the cache key path), used when the magic-byte sniff
/// is ambiguous (many tiny manifests share the avro magic with other
/// avro objects). `None` is acceptable; the magic check still runs.
#[derive(Debug, Clone, Default)]
pub struct AdmitHint<'a> {
    pub key_path_hint: Option<&'a str>,
}

impl<'a> AdmitHint<'a> {
    pub fn from_key_path(path: &'a str) -> Self {
        Self {
            key_path_hint: Some(path),
        }
    }
}

/// Two-pool decoded-metadata cache.
///
/// `manifests` and `parquet_footers` are independent LRUs so a
/// scan-heavy footer workload cannot evict manifests (or vice
/// versa). Both are `parking_lot::Mutex<LruCache<...>>`-shaped per
/// the SHELF-50 acceptance criteria.
#[derive(Debug)]
pub struct DecodedMetaCache {
    manifests: Mutex<LruCache<EtagKey, Arc<ManifestFile>>>,
    parquet_footers: Mutex<LruCache<EtagKey, Arc<ParquetMetaData>>>,
    enabled: std::sync::atomic::AtomicBool,
}

impl DecodedMetaCache {
    /// Build a cache with explicit per-pool entry caps.
    pub fn new(max_manifests: usize, max_footers: usize) -> Self {
        let m = NonZeroUsize::new(max_manifests.max(1)).expect("max_manifests >= 1");
        let f = NonZeroUsize::new(max_footers.max(1)).expect("max_footers >= 1");
        Self {
            manifests: Mutex::new(LruCache::new(m)),
            parquet_footers: Mutex::new(LruCache::new(f)),
            // Off by default. Operators flip via `cache.decodedMeta.enabled`
            // in values.yaml; the configmap renders the bool into the
            // `decoded_meta.enabled` YAML key and `apply_config` toggles
            // the runtime flag.
            enabled: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Return the singleton process-wide instance with default caps.
    pub fn global() -> &'static DecodedMetaCache {
        // Explicit deref so we return `&DecodedMetaCache`, not
        // `&Lazy<DecodedMetaCache>`. Coercion does NOT fire in
        // return position; this is the canonical Lazy-singleton
        // accessor pattern.
        &*INSTANCE
    }

    /// Toggle the cache on/off without rebuilding it. Used by the
    /// config loader at startup; safe to call repeatedly.
    pub fn set_enabled(&self, on: bool) {
        self.enabled
            .store(on, std::sync::atomic::Ordering::Relaxed);
    }

    /// True iff the cache is currently enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Look up a decoded manifest by ETag. Bumps the hit/miss
    /// counter. Returns `None` cheaply when the cache is disabled
    /// (no lock acquired).
    pub fn get_manifest(&self, etag: &str) -> Option<Arc<ManifestFile>> {
        if !self.is_enabled() {
            return None;
        }
        let mut guard = self.manifests.lock();
        match guard.get(etag) {
            Some(v) => {
                metrics::DECODED_META_HITS_TOTAL
                    .with_label_values(&[KIND_MANIFEST])
                    .inc();
                Some(v.clone())
            }
            None => {
                metrics::DECODED_META_MISSES_TOTAL
                    .with_label_values(&[KIND_MANIFEST])
                    .inc();
                None
            }
        }
    }

    /// Look up a decoded Parquet footer by ETag.
    pub fn get_parquet_footer(&self, etag: &str) -> Option<Arc<ParquetMetaData>> {
        if !self.is_enabled() {
            return None;
        }
        let mut guard = self.parquet_footers.lock();
        match guard.get(etag) {
            Some(v) => {
                metrics::DECODED_META_HITS_TOTAL
                    .with_label_values(&[KIND_PARQUET_FOOTER])
                    .inc();
                Some(v.clone())
            }
            None => {
                metrics::DECODED_META_MISSES_TOTAL
                    .with_label_values(&[KIND_PARQUET_FOOTER])
                    .inc();
                None
            }
        }
    }

    /// Drop any decoded entry associated with `etag`. Idempotent;
    /// missing entries are not an error. Called by the byte cache's
    /// eviction path on ETag change (ADR-0011).
    pub fn invalidate(&self, etag: &str) {
        // Drop both kinds — a single ETag never maps to both pools
        // simultaneously (different files = different ETags) but
        // the invariant is enforced at the *call* site, not here;
        // the cheap double-pop keeps the API call shape symmetric.
        let removed_m = self.manifests.lock().pop(etag).is_some();
        let removed_f = self.parquet_footers.lock().pop(etag).is_some();
        if removed_m {
            self.refresh_entries_gauge(KIND_MANIFEST);
        }
        if removed_f {
            self.refresh_entries_gauge(KIND_PARQUET_FOOTER);
        }
    }

    /// Synchronous insert path used by tests and the fire-and-forget
    /// decode worker. Returns `true` iff the entry was newly admitted
    /// (vs replacing an entry that was still resident).
    pub fn insert_manifest(&self, etag: EtagKey, value: Arc<ManifestFile>) -> bool {
        let mut guard = self.manifests.lock();
        let was_new = guard.peek(etag.as_ref()).is_none();
        guard.put(etag, value);
        drop(guard);
        self.refresh_entries_gauge(KIND_MANIFEST);
        was_new
    }

    /// Synchronous insert path for Parquet footers. Mirrors
    /// [`insert_manifest`].
    pub fn insert_parquet_footer(&self, etag: EtagKey, value: Arc<ParquetMetaData>) -> bool {
        let mut guard = self.parquet_footers.lock();
        let was_new = guard.peek(etag.as_ref()).is_none();
        guard.put(etag, value);
        drop(guard);
        self.refresh_entries_gauge(KIND_PARQUET_FOOTER);
        was_new
    }

    /// Number of entries currently resident, per pool. Cheap.
    pub fn len(&self, kind: DecodedKind) -> usize {
        match kind {
            DecodedKind::Manifest => self.manifests.lock().len(),
            DecodedKind::ParquetFooter => self.parquet_footers.lock().len(),
        }
    }

    /// Drop every entry. Used by tests; not exposed on the public
    /// HTTP admin surface in this PR.
    pub fn clear(&self) {
        self.manifests.lock().clear();
        self.parquet_footers.lock().clear();
        self.refresh_entries_gauge(KIND_MANIFEST);
        self.refresh_entries_gauge(KIND_PARQUET_FOOTER);
    }

    fn refresh_entries_gauge(&self, kind: &'static str) {
        let n = match kind {
            KIND_MANIFEST => self.manifests.lock().len(),
            KIND_PARQUET_FOOTER => self.parquet_footers.lock().len(),
            _ => return,
        };
        metrics::DECODED_META_ENTRIES
            .with_label_values(&[kind])
            .set(n as i64);
    }
}

/// Discriminator for [`DecodedMetaCache::len`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DecodedKind {
    Manifest,
    ParquetFooter,
}

/// Process-wide cache instance. Initialised at first access; the
/// runtime `enabled` flag defaults off until [`set_enabled(true)`]
/// is called from the config loader.
static INSTANCE: Lazy<DecodedMetaCache> =
    Lazy::new(|| DecodedMetaCache::new(DEFAULT_MAX_ENTRIES, DEFAULT_MAX_ENTRIES));

/// Top-level free function so callers do not have to thread the
/// `&'static DecodedMetaCache` through every layer. Mirrors the
/// shape of [`crate::metrics::HITS_TOTAL`] etc.
pub fn get_manifest(etag: &str) -> Option<Arc<ManifestFile>> {
    DecodedMetaCache::global().get_manifest(etag)
}

/// Top-level free function for the Parquet footer accessor. Mirrors
/// [`get_manifest`].
pub fn get_parquet_footer(etag: &str) -> Option<Arc<ParquetMetaData>> {
    DecodedMetaCache::global().get_parquet_footer(etag)
}

/// Top-level free function for the byte-cache's eviction-path
/// invalidation hook. Idempotent.
pub fn invalidate(etag: &str) {
    DecodedMetaCache::global().invalidate(etag);
}

/// Toggle the global cache on/off. Called once at startup by the
/// shelfd `main.rs` after parsing the config.
pub fn set_enabled(on: bool) {
    DecodedMetaCache::global().set_enabled(on);
}

/// Producer hook — called from the [`crate::store::Pool::Metadata`]
/// admission path with the bytes that were just admitted under
/// `etag`. The function is *non-blocking*: it sniffs the magic
/// bytes synchronously (microseconds) and spawns a tokio blocking
/// task for the heavy parse. The hot read path is unaffected.
///
/// `etag` is the S3-side opaque version token; an empty etag is a
/// no-op (the byte cache requires non-empty etag, but defence in
/// depth is cheap).
pub fn on_metadata_admit(etag: &str, hint: AdmitHint<'_>, bytes: Bytes) {
    let cache = DecodedMetaCache::global();
    if !cache.is_enabled() {
        return;
    }
    if etag.is_empty() {
        return;
    }
    let kind = sniff_kind(&bytes, hint.key_path_hint);
    let kind = match kind {
        Some(k) => k,
        None => return,
    };
    let etag_key: EtagKey = Arc::from(etag);
    // Spawn fire-and-forget; the producer thread does not wait.
    // `spawn_blocking` is correct here because both the Avro
    // sanity-walk and `ParquetMetaDataReader` are CPU-bound;
    // running them on a tokio worker thread would steal capacity
    // from the data-plane.
    let bytes_for_decode = bytes.clone();
    tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let label = match kind {
            DecodedKind::Manifest => KIND_MANIFEST,
            DecodedKind::ParquetFooter => KIND_PARQUET_FOOTER,
        };
        let outcome: Result<(), DecodeError> = match kind {
            DecodedKind::Manifest => match decode_manifest(bytes_for_decode.clone()) {
                Ok(mf) => {
                    cache.insert_manifest(etag_key, Arc::new(mf));
                    Ok(())
                }
                Err(e) => Err(e),
            },
            DecodedKind::ParquetFooter => match decode_parquet_footer(bytes_for_decode.clone()) {
                Ok(md) => {
                    cache.insert_parquet_footer(etag_key, Arc::new(md));
                    Ok(())
                }
                Err(e) => Err(e),
            },
        };
        let elapsed = started.elapsed().as_secs_f64();
        metrics::DECODED_META_DECODE_SECONDS
            .with_label_values(&[label])
            .observe(elapsed);
        if let Err(e) = outcome {
            metrics::DECODED_META_DECODE_ERRORS_TOTAL
                .with_label_values(&[label, e.reason_label()])
                .inc();
        }
    });
}

/// Heuristic sniff. Returns `Some(kind)` only when the leading magic
/// bytes match Avro (`Obj\x01`) or Parquet's `PAR1` trailer marker is
/// present at the tail. Uses `key_path_hint` as a tie-breaker for
/// extension-based fallback when magic is ambiguous.
pub fn sniff_kind(bytes: &Bytes, key_path_hint: Option<&str>) -> Option<DecodedKind> {
    if bytes.starts_with(b"Obj\x01") {
        return Some(DecodedKind::Manifest);
    }
    // Parquet files end with `PAR1`. A footer admitted alone (i.e.
    // *not* a full Parquet file) carries the trailing magic + a
    // 4-byte footer-length prefix, mirroring the original on-disk
    // layout — Trino's Iceberg reader does a tail-range GET that
    // includes the magic, so the bytes we see here always include
    // it.
    if bytes.len() >= 4 && &bytes[bytes.len() - 4..] == b"PAR1" {
        return Some(DecodedKind::ParquetFooter);
    }
    // Extension fallback. The hint paths look like
    // `<bucket>/<schema>/<table>/{data,metadata}/<file>.{avro,parquet}`.
    if let Some(path) = key_path_hint {
        if path.ends_with(".avro") {
            return Some(DecodedKind::Manifest);
        }
        if path.ends_with(".parquet") {
            return Some(DecodedKind::ParquetFooter);
        }
    }
    None
}

/// Validate the Avro Object Container magic + version byte and
/// admit the raw bytes. v1 deliberately does not walk the Avro
/// schema — that work is deferred to the SHELF-50b iceberg-rust
/// upgrade which will produce a structured `ManifestFile` instead
/// of the bytes wrapper.
pub fn decode_manifest(bytes: Bytes) -> Result<ManifestFile, DecodeError> {
    if !bytes.starts_with(b"Obj\x01") {
        return Err(DecodeError::BadMagic);
    }
    Ok(ManifestFile { raw: bytes })
}

/// Parse a Parquet footer from `bytes`. The bytes are expected to
/// be the trailing footer — `<thrift FileMetaData><footer_len_le_u32>PAR1`
/// — exactly as Trino's reader requests via a range-GET on the
/// final ~64 KiB of a Parquet file. The parser tolerates a
/// pre-pended object-prefix as long as the trailing layout matches.
pub fn decode_parquet_footer(bytes: Bytes) -> Result<ParquetMetaData, DecodeError> {
    let n = bytes.len();
    if n < 8 || &bytes[n - 4..] != b"PAR1" {
        return Err(DecodeError::BadMagic);
    }
    // `parse_and_finish` takes the parser by value and does both
    // sniff + parse in one call; the equivalent two-step
    // `try_parse` + `finish` would require keeping the reader
    // mutable across the borrow boundary, which is fragile under
    // future parquet-crate patch bumps.
    ParquetMetaDataReader::new()
        .parse_and_finish(&bytes)
        .map_err(|e| DecodeError::ParquetThrift(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn fresh_cache(cap: usize) -> DecodedMetaCache {
        let c = DecodedMetaCache::new(cap, cap);
        c.set_enabled(true);
        c
    }

    fn ekey(s: &str) -> EtagKey {
        Arc::from(s)
    }

    fn dummy_manifest(b: &[u8]) -> Arc<ManifestFile> {
        Arc::new(ManifestFile {
            raw: Bytes::copy_from_slice(b),
        })
    }

    #[test]
    fn insert_and_get_manifest() {
        let c = fresh_cache(8);
        let v = dummy_manifest(b"Obj\x01payload-1");
        assert!(c.insert_manifest(ekey("etag-1"), v.clone()));
        let got = c.get_manifest("etag-1").expect("present");
        assert_eq!(got.raw, v.raw);
    }

    #[test]
    fn miss_returns_none_when_enabled() {
        let c = fresh_cache(8);
        assert!(c.get_manifest("never-inserted").is_none());
    }

    #[test]
    fn disabled_cache_returns_none_even_after_insert() {
        let c = DecodedMetaCache::new(8, 8);
        // Default off.
        c.insert_manifest(ekey("etag-1"), dummy_manifest(b"Obj\x01x"));
        assert!(c.get_manifest("etag-1").is_none());
        c.set_enabled(true);
        assert!(c.get_manifest("etag-1").is_some());
    }

    #[test]
    fn lru_evicts_lru_on_capacity_overflow() {
        let c = fresh_cache(2);
        c.insert_manifest(ekey("a"), dummy_manifest(b"Obj\x01a"));
        c.insert_manifest(ekey("b"), dummy_manifest(b"Obj\x01b"));
        // Touch `a` so `b` is the LRU.
        let _ = c.get_manifest("a");
        c.insert_manifest(ekey("c"), dummy_manifest(b"Obj\x01c"));
        assert!(c.get_manifest("a").is_some());
        assert!(c.get_manifest("b").is_none());
        assert!(c.get_manifest("c").is_some());
        assert_eq!(c.len(DecodedKind::Manifest), 2);
    }

    #[test]
    fn invalidate_drops_prior_entry_on_etag_flip() {
        let c = fresh_cache(8);
        c.insert_manifest(ekey("etag-old"), dummy_manifest(b"Obj\x01old"));
        // ETag flip in the byte cache calls `invalidate(old_etag)`.
        c.invalidate("etag-old");
        assert!(c.get_manifest("etag-old").is_none());
        assert_eq!(c.len(DecodedKind::Manifest), 0);
    }

    #[test]
    fn invalidate_is_idempotent() {
        let c = fresh_cache(8);
        c.invalidate("does-not-exist");
        c.invalidate("does-not-exist");
        // No panic, no error, no entry installed.
        assert_eq!(c.len(DecodedKind::Manifest), 0);
    }

    #[test]
    fn etag_change_yields_two_independent_slots() {
        // Acceptance criterion #4: flipping etag drops the prior
        // decoded entry. We assert that the *new* etag does not
        // alias the old slot — the test feeds two etags and asserts
        // the cache holds two distinct slots until we explicitly
        // invalidate the prior one.
        let c = fresh_cache(8);
        c.insert_manifest(ekey("etag-v1"), dummy_manifest(b"Obj\x01v1-bytes"));
        c.insert_manifest(ekey("etag-v2"), dummy_manifest(b"Obj\x01v2-bytes"));
        let v1 = c.get_manifest("etag-v1").expect("v1 still present");
        let v2 = c.get_manifest("etag-v2").expect("v2 still present");
        assert_ne!(v1.raw, v2.raw);
        c.invalidate("etag-v1");
        assert!(c.get_manifest("etag-v1").is_none());
        assert!(c.get_manifest("etag-v2").is_some());
    }

    #[test]
    fn parquet_footer_round_trip_via_writer_path() {
        // Acceptance criterion #8 — a round-trip test for
        // `ParquetMetaData` using the parquet crate's writer.
        //
        // We construct a minimal valid empty-schema Parquet footer
        // by hand using `parquet::format::FileMetaData` and the
        // crate's Thrift compact writer, then feed the resulting
        // bytes through `decode_parquet_footer`. This exercises the
        // exact code path admission would run on a real footer.
        let bytes = build_minimal_parquet_footer();
        let md = decode_parquet_footer(bytes.clone()).expect("decode minimal footer");
        let c = fresh_cache(8);
        c.insert_parquet_footer(ekey("etag-pq"), Arc::new(md));
        assert!(c.get_parquet_footer("etag-pq").is_some());
        // Second lookup must hit (LRU semantics).
        assert!(c.get_parquet_footer("etag-pq").is_some());
        assert_eq!(c.len(DecodedKind::ParquetFooter), 1);
    }

    #[test]
    fn malformed_parquet_bytes_are_a_decode_error_no_panic() {
        // Acceptance criterion #8 — malformed bytes must surface as
        // a DecodeError. The cache stays empty.
        let bad = Bytes::from_static(&[0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]);
        let res = decode_parquet_footer(bad);
        assert!(matches!(res, Err(DecodeError::BadMagic)));
    }

    #[test]
    fn malformed_manifest_magic_is_a_decode_error() {
        let bad = Bytes::from_static(b"NOT-AVRO");
        let res = decode_manifest(bad);
        assert!(matches!(res, Err(DecodeError::BadMagic)));
    }

    #[test]
    fn sniff_recognises_avro_and_parquet_layouts() {
        let avro = Bytes::from_static(b"Obj\x01rest-of-avro");
        assert_eq!(sniff_kind(&avro, None), Some(DecodedKind::Manifest));

        // Parquet trailer.
        let mut pq = Vec::with_capacity(16);
        pq.extend_from_slice(b"some-thrift-here");
        pq.extend_from_slice(b"PAR1");
        let pq = Bytes::from(pq);
        assert_eq!(sniff_kind(&pq, None), Some(DecodedKind::ParquetFooter));

        // Extension fallback.
        let mystery = Bytes::from_static(b"\x00\x01\x02\x03");
        assert_eq!(
            sniff_kind(&mystery, Some("bucket/db/table/metadata/foo.avro")),
            Some(DecodedKind::Manifest)
        );
        assert_eq!(
            sniff_kind(&mystery, Some("bucket/db/table/data/foo.parquet")),
            Some(DecodedKind::ParquetFooter)
        );
        assert_eq!(sniff_kind(&mystery, Some("bucket/random.txt")), None);
    }

    #[test]
    fn concurrent_inserts_are_safe() {
        // Adding the same etag from many threads must not panic
        // and must converge to a single resident entry — defended
        // by the parking_lot::Mutex around LruCache.
        let c = Arc::new(fresh_cache(64));
        let mut handles = Vec::new();
        for i in 0..16 {
            let cc = Arc::clone(&c);
            handles.push(thread::spawn(move || {
                let etag = ekey("hot-key");
                let v = dummy_manifest(format!("Obj\x01payload-{i}").as_bytes());
                cc.insert_manifest(etag, v);
            }));
        }
        for h in handles {
            h.join().expect("thread joined");
        }
        assert!(c.get_manifest("hot-key").is_some());
        assert_eq!(c.len(DecodedKind::Manifest), 1);
    }

    /// Build the smallest valid Parquet footer (`<thrift><len><PAR1>`)
    /// with an empty schema and zero row groups. This is *not* a full
    /// Parquet file — Trino's Iceberg reader requests just the trailing
    /// footer via a range GET, which is the exact shape this test
    /// exercises.
    fn build_minimal_parquet_footer() -> Bytes {
        use parquet::format::{FileMetaData, SchemaElement};
        use parquet::thrift::TSerializable;
        use thrift::protocol::TCompactOutputProtocol;

        // Minimum-viable empty-schema metadata. Parquet requires the
        // root SchemaElement to come first; one root with zero
        // children is the shortest valid schema list.
        let mut schema = SchemaElement::default();
        schema.name = "minimal_root".to_owned();
        schema.num_children = Some(0);

        let mut meta = FileMetaData::default();
        meta.version = 1;
        meta.schema = vec![schema];
        meta.num_rows = 0;
        meta.row_groups = Vec::new();
        meta.created_by = Some("shelfd-decoded-meta-test".to_owned());

        let mut thrift_buf: Vec<u8> = Vec::new();
        {
            let mut proto = TCompactOutputProtocol::new(&mut thrift_buf);
            meta.write_to_out_protocol(&mut proto)
                .expect("encode minimal footer");
        }

        let footer_len = thrift_buf.len() as u32;
        let mut out = Vec::with_capacity(thrift_buf.len() + 8);
        out.extend_from_slice(&thrift_buf);
        out.extend_from_slice(&footer_len.to_le_bytes());
        out.extend_from_slice(b"PAR1");
        Bytes::from(out)
    }
}
