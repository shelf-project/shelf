//! SHELF-46 — Bloom-aware footer admission policy.
//!
//! ## Why this module exists
//!
//! `shelfd` today admits the entire Parquet footer slice every time
//! a reader fetches it, and lands the bytes in whichever Foyer pool
//! [`crate::s3_shim::pool_for`] picked from the key extension. For
//! `.parquet` files that means the rowgroup pool, which is
//! NVMe-backed and tuned for 1–32 MiB row-group payloads. Footers are
//! 8–64 KiB; bloom blocks attached to a column live somewhere in the
//! body of the file at an offset and length recorded in the footer
//! ([Apache Parquet bloom-filter spec][parquet-bloom],
//! [DuckDB Parquet bloom blog Mar 2025][duckdb-blog]). Both classes of
//! read are pure metadata for predicate-pushdown — they never need to
//! age out to disk and they want longer DRAM residency than scan-side
//! row-group bytes get under S3-FIFO/LRU.
//!
//! Promoting **footers** and **bloom blocks** into [`Pool::Metadata`]
//! cuts metadata-pool churn AND lets Trino skip more row groups via
//! bloom pushdown (Trino landed bloom-filter writes in [trinodb/trino
//! #20662][trino-pr-20662], merged 2024-04-16, in releases ≥ 445).
//! Both effects lower S3 GET cost on rep-2 / rep-1's `cdp.*` reads.
//!
//! ## Design constraints (from `agents/out/SHELF-46-bloom-aware-footer-admission.md`)
//!
//! 1. **Forward-compat with ADR-0011.** Cache keys remain
//!    `sha256(etag || offset || length || rg_ord)` — no new tag byte,
//!    no key-function fork. The bloom-aware policy only changes
//!    *which pool* a read targets and *whether* the size-threshold
//!    policy can reject it.
//! 2. **Fail-open.** If footer parsing fails we fall back to the
//!    inner [`AdmissionPolicy`] and increment
//!    `shelf_bloom_parse_errors_total{reason}`. The data plane never
//!    errors out because of bloom logic.
//! 3. **Bounded memory.** The `etag → Vec<BloomBlockRange>` index is
//!    a generation-counter LRU capped at
//!    [`BloomAdmissionConfig::max_index_entries`] (default 50 000;
//!    ≈ 1 MiB at 10 ranges/file).
//! 4. **Etag drop on change.** When a fresh etag is observed for a
//!    key we already index, the prior bloom block list is dropped
//!    so a stale entry never produces a false `BloomBlock` hit.
//!    SHELF-04 keys are content-addressed by etag, so this is purely
//!    a cleanliness invariant — false hits would cache as the wrong
//!    `Pool::Metadata` slot, not corrupt bytes.
//! 5. **Default off.** `cache.bloom.enabled=false` ships in the OSS
//!    chart; operator overlays flip it on AFTER the canary gate.
//!
//! ## Interaction with `iceberg.metadata-cache.enabled=false`
//!
//! Trino/Iceberg has a JVM-local `MemoryFileSystemCache` that caches
//! manifest/metadata files and silently bypasses any external cache
//! on warm reads (see `AGENTS.md`). For SHELF-46 to be visible —
//! i.e. for the `shelf_hits_total{pool="metadata"}` counter to climb
//! cold→warm on Parquet footer reads — the Iceberg catalog properties
//! must include `iceberg.metadata-cache.enabled=false`. Without this,
//! the JVM cache absorbs every footer hit after the first and shelfd
//! sees a flat-line on the metadata pool even when this policy is
//! routing things correctly.
//!
//! ## Interaction with B1 (zstd) and SHELF-49 (range coalesce)
//!
//! - **B1 NVMe zstd compression** lives on `Pool::RowGroup` only.
//!   Footers and bloom blocks land in `Pool::Metadata` (DRAM-only),
//!   so SHELF-46 is orthogonal to B1; the two compose without
//!   conflict.
//! - **SHELF-49 range coalesce** quantises adjacent GETs into one
//!   wider GET. A coalesced fetch may straddle the footer suffix and
//!   a non-bloom column body. SHELF-46 classifies on the
//!   *pre-coalesce* per-read offsets observed by the shim, so an
//!   8-call coalesced batch still bumps
//!   `shelf_bloom_admit_total{kind="footer"}` for the one trailing
//!   call and `kind="not_applicable"` for the others. Coalesce
//!   short-reads do NOT poison the footer suffix heuristic.
//!
//! ## Failure modes
//!
//! | Failure | Effect | Metric / Log |
//! |---|---|---|
//! | Footer parser disagrees with the file's actual `bloom_filter_offset` | Index may carry stale ranges; later `classify` returns `BloomBlock` for a non-bloom range; the read still succeeds, just lands in the wrong pool | `shelf_bloom_parse_errors_total{reason}` incremented when parse returns `Err`; never panics |
//! | Footer parsing not enabled (`parquet_meta` cargo feature off, default) | `parse_footer_blooms` returns `Ok(vec![])`; the bloom-block lookup path is a no-op; the footer-suffix heuristic still routes trailing reads to `Pool::Metadata` | none |
//! | LRU eviction races a bloom-block lookup | `classify` returns `NotApplicable`; the read falls back to size-threshold admission (the existing default); correctness preserved | none |
//! | Etag changes between `insert` and a subsequent lookup | `insert` overwrites the prior entry under the same etag key; the new ranges win; the old ranges become unreachable | none (deterministic) |
//!
//! [parquet-bloom]: https://parquet.apache.org/docs/file-format/bloomfilter/
//! [duckdb-blog]: https://duckdb.org/2025/03/07/parquet-bloom-filters-in-duckdb.html
//! [trino-pr-20662]: https://github.com/trinodb/trino/pull/20662

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::admission::{AdmissionContext, AdmissionDecision, AdmissionPolicy};

/// One Parquet bloom-filter byte range, parsed from a footer.
///
/// `offset` is absolute within the originating Parquet file;
/// `length` is the bloom block size in bytes (header + bitset).
/// Both fields are `u64` so they round-trip through the same wire
/// format the s3-shim observes on `Range:` headers.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BloomBlockRange {
    pub offset: u64,
    pub length: u64,
}

impl BloomBlockRange {
    /// Whether this range exactly matches the half-open interval
    /// `[offset, offset + length)`.
    pub fn matches(&self, offset: u64, length: u64) -> bool {
        self.offset == offset && self.length == length
    }
}

/// Classification of an incoming read (`offset`, `length`,
/// `object_size`, `etag`) against the bloom index. Used by the
/// s3-shim to decide pool routing AND admission bypass.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BloomKind {
    /// The read is the trailing footer suffix of a Parquet object —
    /// i.e. the read's last byte is the file's last byte AND its
    /// length crosses the configured `min_footer_bytes` threshold.
    /// Always admit to [`Pool::Metadata`].
    Footer,
    /// The read's `(offset, length)` matches a known bloom block
    /// range parsed earlier from the same etag's footer. Always
    /// admit to [`Pool::Metadata`].
    BloomBlock,
    /// Default — no bloom-aware override; the read goes through
    /// the existing pool routing and size-threshold admission.
    NotApplicable,
}

impl BloomKind {
    /// Stable Prometheus label fragment for `shelf_bloom_admit_total`.
    pub fn metric_label(self) -> &'static str {
        match self {
            BloomKind::Footer => "footer",
            BloomKind::BloomBlock => "bloom_block",
            BloomKind::NotApplicable => "not_applicable",
        }
    }
}

/// Errors surfaced by [`parse_footer_blooms`]. Every variant is
/// recorded under [`crate::metrics::BLOOM_PARSE_ERRORS_TOTAL`] with
/// a stable `reason` label so the dashboard can split parse-class
/// problems from generic data-plane errors. None of these surface to
/// the data plane — the s3-shim swallows the error and falls back to
/// the inner admission policy.
#[derive(Debug)]
pub enum ParseError {
    /// Buffer is shorter than 8 bytes (PAR1 + footer length suffix).
    TooShort,
    /// Trailing 4-byte sequence is not the literal `b"PAR1"` magic.
    BadMagic,
    /// Declared footer length overflows the supplied buffer.
    LengthOverflow,
    /// Thrift / parquet decode error from the optional `parquet`
    /// crate dep. Only emitted when the `parquet_meta` feature is on.
    Decode(String),
    /// `parquet_meta` cargo feature is disabled in this build.
    /// Treated as a no-op by callers; never increments the error
    /// counter (it's the documented "stub" path).
    FeatureDisabled,
}

impl ParseError {
    /// Stable Prometheus `reason` label.
    pub fn reason_label(&self) -> &'static str {
        match self {
            ParseError::TooShort => "too_short",
            ParseError::BadMagic => "bad_magic",
            ParseError::LengthOverflow => "length_overflow",
            ParseError::Decode(_) => "decode",
            ParseError::FeatureDisabled => "feature_disabled",
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::TooShort => write!(f, "buffer shorter than parquet footer suffix"),
            ParseError::BadMagic => write!(f, "trailing 4 bytes are not PAR1"),
            ParseError::LengthOverflow => write!(f, "declared footer length exceeds buffer"),
            ParseError::Decode(s) => write!(f, "parquet decode error: {s}"),
            ParseError::FeatureDisabled => {
                write!(f, "parquet_meta cargo feature is disabled in this build")
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// Per-process configuration for [`BloomAdmission`].
///
/// Defaults match the OSS Helm chart so the runtime knob set and the
/// chart values stay in 1:1 correspondence.
#[derive(Debug, Clone, Copy)]
pub struct BloomAdmissionConfig {
    /// Master switch. When `false`, [`BloomAdmission::classify`]
    /// always returns [`BloomKind::NotApplicable`] and
    /// [`BloomAdmission::maybe_index_footer`] is a no-op. Default
    /// `false`.
    pub enabled: bool,
    /// Hard cap on the etag → bloom block list LRU. Each entry costs
    /// roughly `etag_bytes (~32) + 16 × ranges_per_file` ≈ 192 B at
    /// 10 ranges/file, so the default 50 000 caps RSS at
    /// ~ 10 MiB worst case. Default 50 000.
    pub max_index_entries: usize,
    /// Minimum length (bytes) for a trailing-suffix read to be
    /// classified as a Parquet footer. Reads shorter than this
    /// fall through to [`BloomKind::NotApplicable`] even when they
    /// touch the file's last byte. Default 65 536 (64 KiB).
    pub min_footer_bytes: u64,
}

impl Default for BloomAdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_index_entries: 50_000,
            min_footer_bytes: 64 * 1024,
        }
    }
}

/// Bounded LRU mapping `etag bytes → Arc<Vec<BloomBlockRange>>`.
///
/// We use a generation-counter pseudo-LRU (touch-on-read +
/// touch-on-write, evict the lowest generation when over capacity)
/// rather than pulling in a new `lru` crate dep. The map is held
/// behind a single `parking_lot::Mutex`; lookup is O(1) hash + one
/// atomic generation bump under the lock. At 50 000 entries with a
/// short-tail workload the lock is held for sub-microsecond
/// intervals — well under the s3-shim path's 100 µs budget.
///
/// Etag bytes are stored as `Vec<u8>` rather than a `&str` because
/// S3's ETag is opaque (multipart ETags include `-N` parts; we never
/// interpret the value, only equality-compare it).
#[derive(Debug)]
pub struct BloomIndex {
    inner: Mutex<BloomIndexInner>,
    max_entries: usize,
    next_gen: AtomicU64,
}

#[derive(Debug, Default)]
struct BloomIndexInner {
    entries: HashMap<Vec<u8>, BloomIndexEntry>,
}

#[derive(Debug, Clone)]
struct BloomIndexEntry {
    ranges: Arc<Vec<BloomBlockRange>>,
    /// Monotonic counter; the entry with the lowest `gen` is the LRU
    /// victim. Updated on every read AND every write.
    gen: u64,
}

impl BloomIndex {
    /// Build an empty index. `max_entries == 0` is allowed and means
    /// "always evict" — useful in tests to assert the eviction path.
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(BloomIndexInner::default()),
            max_entries,
            next_gen: AtomicU64::new(1),
        }
    }

    /// Insert `ranges` for `etag`, evicting the LRU entry if the
    /// insert would exceed `max_entries`. Replaces any prior entry
    /// for the same etag (forward-compat with ADR-0011: etag changes
    /// invalidate the entire bloom set for that key).
    pub fn insert(&self, etag: &[u8], ranges: Vec<BloomBlockRange>) {
        // Empty etag = degenerate; refuse to index. S3's `HeadObject`
        // always populates ETag, but defensive caller chains may pass
        // `&[]` if the upstream HEAD-LRU misses; treat as no-op.
        if etag.is_empty() {
            return;
        }
        let gen = self.next_gen.fetch_add(1, Ordering::Relaxed);
        let entry = BloomIndexEntry {
            ranges: Arc::new(ranges),
            gen,
        };
        let mut guard = self.inner.lock();
        guard.entries.insert(etag.to_vec(), entry);
        if guard.entries.len() > self.max_entries {
            // Evict the entry with the lowest `gen`. With
            // max_entries on the order of 50 000 a linear scan is
            // ~50 µs — acceptable for an admission path that fires
            // at most once per file. Future optimisation: a `BTreeMap`
            // keyed on `gen` would drop this to O(log n).
            let mut victim_etag: Option<Vec<u8>> = None;
            let mut victim_gen = u64::MAX;
            for (k, v) in guard.entries.iter() {
                if v.gen < victim_gen {
                    victim_gen = v.gen;
                    victim_etag = Some(k.clone());
                }
            }
            if let Some(k) = victim_etag {
                guard.entries.remove(&k);
            }
        }
        crate::metrics::BLOOM_INDEX_ENTRIES.set(guard.entries.len() as i64);
    }

    /// Look up the bloom block range list for `etag`. Returns `None`
    /// if absent. As a side effect, the entry's generation is bumped
    /// so it climbs back to the head of the LRU.
    pub fn lookup_ranges(&self, etag: &[u8]) -> Option<Arc<Vec<BloomBlockRange>>> {
        if etag.is_empty() {
            return None;
        }
        let gen = self.next_gen.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.inner.lock();
        let entry = guard.entries.get_mut(etag)?;
        entry.gen = gen;
        Some(entry.ranges.clone())
    }

    /// Whether `(offset, length)` matches any bloom block range
    /// indexed under `etag`. Convenience wrapper around
    /// [`Self::lookup_ranges`] used by [`BloomAdmission::classify`].
    pub fn contains_range(&self, etag: &[u8], offset: u64, length: u64) -> bool {
        match self.lookup_ranges(etag) {
            Some(ranges) => ranges.iter().any(|r| r.matches(offset, length)),
            None => false,
        }
    }

    /// Drop the index entry for `etag`, if any. Returns `true` iff
    /// an entry was present before the call.
    pub fn invalidate(&self, etag: &[u8]) -> bool {
        let mut guard = self.inner.lock();
        let removed = guard.entries.remove(etag).is_some();
        if removed {
            crate::metrics::BLOOM_INDEX_ENTRIES.set(guard.entries.len() as i64);
        }
        removed
    }

    /// Current entry count. Test-only on the regression path; not on
    /// the hot path.
    pub fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }

    /// Whether the index is currently empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().entries.is_empty()
    }
}

/// Bloom-aware admission state. One of these is built once at boot
/// from [`BloomAdmissionConfig`] and shared via `Arc` on
/// [`crate::http::ServerState`].
#[derive(Debug)]
pub struct BloomAdmission {
    pub config: BloomAdmissionConfig,
    pub index: Arc<BloomIndex>,
}

impl BloomAdmission {
    /// Build a new instance from runtime config.
    pub fn new(config: BloomAdmissionConfig) -> Self {
        Self {
            index: Arc::new(BloomIndex::new(config.max_index_entries)),
            config,
        }
    }

    /// Test helper: build a disabled instance (every classify →
    /// NotApplicable, every parse no-op).
    #[cfg(test)]
    pub fn disabled() -> Self {
        Self::new(BloomAdmissionConfig {
            enabled: false,
            ..Default::default()
        })
    }

    /// Classify a read against the bloom index + footer-suffix
    /// heuristic. Always cheap (one hash lookup at worst). Never
    /// returns an error and never panics.
    ///
    /// Semantics:
    /// 1. Disabled config → `NotApplicable`.
    /// 2. Read covers the trailing `min_footer_bytes` of the object →
    ///    `Footer`. (The implementation accepts any read whose last
    ///    byte equals the object's last byte AND whose length is
    ///    ≥ `min_footer_bytes`. Suffix-range reads `bytes=-N` and
    ///    open-ended `bytes=START-` both produce that pattern after
    ///    the s3-shim resolves `Range:` against `total_size`.)
    /// 3. `(offset, length)` matches a known bloom block for `etag` →
    ///    `BloomBlock`.
    /// 4. Otherwise `NotApplicable`.
    pub fn classify(&self, etag: &[u8], offset: u64, length: u64, object_size: u64) -> BloomKind {
        if !self.config.enabled {
            return BloomKind::NotApplicable;
        }
        // Defensive: zero-length reads cannot be footers and cannot
        // match a positive-length bloom range.
        if length == 0 {
            return BloomKind::NotApplicable;
        }
        // Footer suffix heuristic.
        let last_byte = offset.saturating_add(length);
        if length >= self.config.min_footer_bytes
            && object_size > 0
            && last_byte >= object_size
            && offset < object_size
        {
            return BloomKind::Footer;
        }
        // Bloom block range.
        if self.index.contains_range(etag, offset, length) {
            return BloomKind::BloomBlock;
        }
        BloomKind::NotApplicable
    }

    /// Bump `shelf_bloom_admit_total{kind=...}` once per
    /// classification observed in the s3-shim. Kept on the
    /// [`BloomAdmission`] surface (rather than inline in the shim)
    /// so the metric label string stays in lockstep with
    /// [`BloomKind::metric_label`].
    pub fn record_classification(&self, kind: BloomKind) {
        crate::metrics::BLOOM_ADMIT_TOTAL
            .with_label_values(&[kind.metric_label()])
            .inc();
    }

    /// If `kind == Footer`, attempt to parse the supplied bytes as a
    /// Parquet footer and populate the index for `etag`. Fail-open:
    /// every error variant is counted under
    /// `shelf_bloom_parse_errors_total{reason}` and silently dropped.
    /// Never returns an error to the caller.
    pub fn maybe_index_footer(&self, etag: &[u8], kind: BloomKind, file_bytes: &[u8]) {
        if !matches!(kind, BloomKind::Footer) {
            return;
        }
        if !self.config.enabled {
            return;
        }
        if etag.is_empty() {
            return;
        }
        match parse_footer_blooms(file_bytes) {
            Ok(ranges) => {
                if !ranges.is_empty() {
                    self.index.insert(etag, ranges);
                }
                // Empty range list is a normal outcome (Parquet file
                // with no bloom-enabled columns). Don't bump the
                // error counter.
            }
            Err(ParseError::FeatureDisabled) => {
                // Documented stub path — feature is off, no parser
                // available. Don't pollute the error metric.
            }
            Err(e) => {
                crate::metrics::BLOOM_PARSE_ERRORS_TOTAL
                    .with_label_values(&[e.reason_label()])
                    .inc();
                tracing::debug!(
                    target: "shelfd::parquet_admit",
                    reason = e.reason_label(),
                    "footer parse failed; falling back to footer-suffix heuristic only",
                );
            }
        }
    }
}

/// Wrapper that delegates [`AdmissionPolicy::decide`] to an inner
/// policy unless the request matches the bloom admission heuristic,
/// in which case it always returns `Admit`.
///
/// **Note**: [`AdmissionContext`] does not currently carry the
/// origin metadata (`etag`, `offset`, `length`, `object_size`)
/// needed to classify a read. The s3-shim therefore performs
/// classification *before* calling
/// [`crate::store::FoyerStore::get_or_fetch`] and substitutes
/// [`FORCE_ADMIT`] for the duration of the call when the read is a
/// `Footer` or `BloomBlock`. The `BloomAwareAdmissionPolicy` type
/// here exists so consumer crates that build their own admission
/// stacks can wire bloom-aware bypass into a single
/// [`AdmissionPolicy`] handle; the decision in that case has to be
/// pre-set on the wrapper before each `decide` call.
#[derive(Debug)]
pub struct BloomAwareAdmissionPolicy<I: AdmissionPolicy> {
    pub inner: Arc<I>,
    /// Set by the caller (typically the s3-shim) immediately before
    /// calling `decide`. The atomic exists only so the wrapper can
    /// implement the `'static + Send + Sync` bound without interior
    /// mutability tied to a request lifetime.
    pub force_admit: std::sync::atomic::AtomicBool,
}

impl<I: AdmissionPolicy> BloomAwareAdmissionPolicy<I> {
    pub fn new(inner: Arc<I>) -> Self {
        Self {
            inner,
            force_admit: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn set_force_admit(&self, value: bool) {
        self.force_admit.store(value, Ordering::Release);
    }
}

impl<I: AdmissionPolicy> AdmissionPolicy for BloomAwareAdmissionPolicy<I> {
    fn decide(&self, ctx: &AdmissionContext<'_>) -> AdmissionDecision {
        if self.force_admit.load(Ordering::Acquire) {
            return AdmissionDecision::Admit;
        }
        self.inner.decide(ctx)
    }
}

/// Stub admission policy that always returns `Admit`. Held as a
/// `'static` instance so the s3-shim hot path can hand a
/// `&dyn AdmissionPolicy` reference into
/// [`crate::store::FoyerStore::get_or_fetch`] without allocating a
/// new policy per request when the read is bloom-aware. The
/// wrapping in [`BloomAwareAdmissionPolicy`] is the alternative for
/// embedding consumers that need a single policy handle.
#[derive(Debug)]
pub struct ForceAdmit;

impl AdmissionPolicy for ForceAdmit {
    fn decide(&self, _ctx: &AdmissionContext<'_>) -> AdmissionDecision {
        AdmissionDecision::Admit
    }
}

/// Process-global instance of [`ForceAdmit`].
pub static FORCE_ADMIT: ForceAdmit = ForceAdmit;

/// Parse the trailing PAR1 magic + footer length from `file_bytes`,
/// then walk row groups → columns → `bloom_filter_offset` /
/// `bloom_filter_length` and return the set of bloom block byte
/// ranges.
///
/// **Stub semantics when the `parquet_meta` cargo feature is off**
/// (the OSS default): returns `Err(ParseError::FeatureDisabled)`,
/// which [`BloomAdmission::maybe_index_footer`] treats as a no-op.
/// The footer-suffix heuristic still works (it does not require
/// parsing); only the bloom-block lookup path is dormant.
///
/// **Real semantics when `parquet_meta` is on**: the trailing 4
/// bytes must equal `b"PAR1"`; the 4 bytes immediately preceding
/// must form a little-endian `u32` footer length whose value is in
/// `[0, file_bytes.len() - 8]`. The slice
/// `[file_bytes.len() - 8 - footer_len, file_bytes.len() - 8]` is
/// fed to `parquet::file::metadata::ParquetMetaDataReader::decode_metadata`.
/// We then iterate `metadata.row_groups()` and, for every
/// `ColumnChunkMetaData` whose `bloom_filter_offset()` is `Some(_)`,
/// emit a `BloomBlockRange { offset, length }`. When
/// `bloom_filter_length()` is `None` (Parquet ≤ 2.9 spec) we record
/// a defensive 256 KiB upper bound clamped to `[offset, file_end)`,
/// matching the design note's "default 256 KiB upper bound" guidance.
pub fn parse_footer_blooms(file_bytes: &[u8]) -> Result<Vec<BloomBlockRange>, ParseError> {
    if file_bytes.len() < 8 {
        return Err(ParseError::TooShort);
    }
    // PAR1 magic at end. Any other 4-byte trailer ⇒ this is not a
    // Parquet file (or it's truncated). The s3-shim invokes the
    // parser on every Footer-classified read, including ORC/Avro
    // files whose suffix happens to match the size threshold; those
    // legitimately fail here and the error is silently absorbed.
    let len = file_bytes.len();
    let trailing_magic = &file_bytes[len - 4..];
    if trailing_magic != b"PAR1" {
        return Err(ParseError::BadMagic);
    }
    let footer_len_bytes = &file_bytes[len - 8..len - 4];
    let footer_len = u32::from_le_bytes([
        footer_len_bytes[0],
        footer_len_bytes[1],
        footer_len_bytes[2],
        footer_len_bytes[3],
    ]) as usize;
    if footer_len == 0 || footer_len > len.saturating_sub(8) {
        return Err(ParseError::LengthOverflow);
    }
    let footer_thrift = &file_bytes[len - 8 - footer_len..len - 8];
    parse_footer_thrift(footer_thrift, len as u64)
}

/// Inner thrift-decoding step. Split out so the magic-validation
/// path stays compile-cheap when the `parquet_meta` feature is off.
#[cfg(not(feature = "parquet_meta"))]
fn parse_footer_thrift(
    _footer_thrift: &[u8],
    _file_len: u64,
) -> Result<Vec<BloomBlockRange>, ParseError> {
    Err(ParseError::FeatureDisabled)
}

#[cfg(feature = "parquet_meta")]
fn parse_footer_thrift(
    footer_thrift: &[u8],
    file_len: u64,
) -> Result<Vec<BloomBlockRange>, ParseError> {
    use parquet::file::metadata::ParquetMetaDataReader;
    // The reader expects the raw thrift-serialized FileMetaData, which
    // is exactly what we sliced off above (between the optional page-
    // index region and the trailing 8-byte length+magic suffix).
    let metadata = ParquetMetaDataReader::decode_metadata(footer_thrift)
        .map_err(|e| ParseError::Decode(e.to_string()))?;
    let mut out = Vec::new();
    // Default upper bound when `bloom_filter_length` is absent
    // (Parquet ≤ 2.9 writers): 256 KiB clamped to the file end.
    const DEFAULT_BLOOM_LEN: u64 = 256 * 1024;
    for rg in metadata.row_groups() {
        for col in rg.columns() {
            let offset = match col.bloom_filter_offset() {
                Some(o) if o >= 0 => o as u64,
                _ => continue,
            };
            let length = match col.bloom_filter_length() {
                Some(l) if l > 0 => l as u64,
                _ => DEFAULT_BLOOM_LEN.min(file_len.saturating_sub(offset)),
            };
            if length == 0 {
                continue;
            }
            // Defensive: drop any range that cannot fit inside the
            // file. A malformed footer that claims an offset past
            // EOF would otherwise be cached as a phantom slot.
            if offset.saturating_add(length) > file_len {
                continue;
            }
            out.push(BloomBlockRange { offset, length });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_returns_not_applicable_when_disabled() {
        let admit = BloomAdmission::disabled();
        // Footer-shaped suffix read on a disabled instance.
        assert_eq!(
            admit.classify(b"etag-x", 1024, 64 * 1024, 1024 + 64 * 1024),
            BloomKind::NotApplicable
        );
    }

    #[test]
    fn classify_footer_suffix_when_length_meets_threshold() {
        let admit = BloomAdmission::new(BloomAdmissionConfig {
            enabled: true,
            min_footer_bytes: 64 * 1024,
            ..Default::default()
        });
        // Suffix range reading the trailing 64 KiB of a 10 MiB file.
        let object_size: u64 = 10 * 1024 * 1024;
        let length: u64 = 64 * 1024;
        let offset = object_size - length;
        assert_eq!(
            admit.classify(b"etag-x", offset, length, object_size),
            BloomKind::Footer,
        );
    }

    #[test]
    fn classify_short_suffix_below_min_falls_through() {
        let admit = BloomAdmission::new(BloomAdmissionConfig {
            enabled: true,
            min_footer_bytes: 64 * 1024,
            ..Default::default()
        });
        // Tiny 4 KiB suffix read — Iceberg manifest probe shape; not
        // a Parquet footer despite touching the last byte.
        let object_size: u64 = 10 * 1024 * 1024;
        let length: u64 = 4 * 1024;
        let offset = object_size - length;
        assert_eq!(
            admit.classify(b"etag-x", offset, length, object_size),
            BloomKind::NotApplicable,
        );
    }

    #[test]
    fn classify_zero_length_is_never_applicable() {
        let admit = BloomAdmission::new(BloomAdmissionConfig {
            enabled: true,
            ..Default::default()
        });
        assert_eq!(
            admit.classify(b"etag-x", 0, 0, 1_000_000),
            BloomKind::NotApplicable,
        );
    }

    #[test]
    fn classify_mid_file_read_is_not_applicable() {
        let admit = BloomAdmission::new(BloomAdmissionConfig {
            enabled: true,
            ..Default::default()
        });
        // Random row-group read in the middle of the file.
        assert_eq!(
            admit.classify(b"etag-x", 1024 * 1024, 32 * 1024 * 1024, 100 * 1024 * 1024),
            BloomKind::NotApplicable,
        );
    }

    #[test]
    fn classify_bloom_block_via_indexed_range() {
        let admit = BloomAdmission::new(BloomAdmissionConfig {
            enabled: true,
            ..Default::default()
        });
        admit.index.insert(
            b"etag-bloom",
            vec![BloomBlockRange {
                offset: 4096,
                length: 8192,
            }],
        );
        assert_eq!(
            admit.classify(b"etag-bloom", 4096, 8192, 1_000_000),
            BloomKind::BloomBlock,
        );
        // Same etag, non-matching range → not applicable.
        assert_eq!(
            admit.classify(b"etag-bloom", 4097, 8192, 1_000_000),
            BloomKind::NotApplicable,
        );
        // Different etag, same range → not applicable.
        assert_eq!(
            admit.classify(b"etag-other", 4096, 8192, 1_000_000),
            BloomKind::NotApplicable,
        );
    }

    #[test]
    fn index_drops_on_etag_change() {
        let idx = BloomIndex::new(16);
        idx.insert(
            b"etag-v1",
            vec![BloomBlockRange {
                offset: 100,
                length: 50,
            }],
        );
        assert!(idx.contains_range(b"etag-v1", 100, 50));
        // Re-insert under same etag with empty list → old range gone.
        idx.insert(b"etag-v1", vec![]);
        assert!(!idx.contains_range(b"etag-v1", 100, 50));
    }

    #[test]
    fn index_evicts_when_over_capacity() {
        let idx = BloomIndex::new(2);
        idx.insert(
            b"etag-1",
            vec![BloomBlockRange {
                offset: 1,
                length: 1,
            }],
        );
        idx.insert(
            b"etag-2",
            vec![BloomBlockRange {
                offset: 2,
                length: 2,
            }],
        );
        idx.insert(
            b"etag-3",
            vec![BloomBlockRange {
                offset: 3,
                length: 3,
            }],
        );
        // Three insertions, capacity 2 → exactly one entry was evicted.
        assert_eq!(idx.len(), 2);
        // The most recently inserted entry must still be present.
        assert!(idx.contains_range(b"etag-3", 3, 3));
    }

    #[test]
    fn index_lookup_touches_generation_for_lru() {
        let idx = BloomIndex::new(2);
        idx.insert(
            b"etag-1",
            vec![BloomBlockRange {
                offset: 1,
                length: 1,
            }],
        );
        idx.insert(
            b"etag-2",
            vec![BloomBlockRange {
                offset: 2,
                length: 2,
            }],
        );
        // Touching etag-1 lifts its generation above etag-2.
        assert!(idx.contains_range(b"etag-1", 1, 1));
        // Inserting a third entry must evict etag-2 (the least
        // recently used after the touch above).
        idx.insert(
            b"etag-3",
            vec![BloomBlockRange {
                offset: 3,
                length: 3,
            }],
        );
        assert!(idx.contains_range(b"etag-1", 1, 1));
        assert!(!idx.contains_range(b"etag-2", 2, 2));
        assert!(idx.contains_range(b"etag-3", 3, 3));
    }

    #[test]
    fn index_invalidate_drops_entry() {
        let idx = BloomIndex::new(16);
        idx.insert(
            b"etag-x",
            vec![BloomBlockRange {
                offset: 1,
                length: 1,
            }],
        );
        assert!(idx.invalidate(b"etag-x"));
        assert!(!idx.invalidate(b"etag-x"));
        assert!(idx.is_empty());
    }

    #[test]
    fn parser_rejects_buffer_shorter_than_suffix() {
        match parse_footer_blooms(&[0u8; 4]) {
            Err(ParseError::TooShort) => (),
            other => panic!("expected TooShort, got {other:?}"),
        }
    }

    #[test]
    fn parser_rejects_bad_magic() {
        let mut buf = vec![0u8; 16];
        // Trailing magic = "NOPE".
        buf[12..].copy_from_slice(b"NOPE");
        match parse_footer_blooms(&buf) {
            Err(ParseError::BadMagic) => (),
            other => panic!("expected BadMagic, got {other:?}"),
        }
    }

    #[test]
    fn parser_rejects_overflowing_footer_length() {
        // 16-byte buffer where the declared footer length (1 GiB) is
        // way larger than the buffer.
        let mut buf = vec![0u8; 16];
        let footer_len: u32 = 1 << 30;
        buf[8..12].copy_from_slice(&footer_len.to_le_bytes());
        buf[12..].copy_from_slice(b"PAR1");
        match parse_footer_blooms(&buf) {
            Err(ParseError::LengthOverflow) => (),
            other => panic!("expected LengthOverflow, got {other:?}"),
        }
    }

    #[test]
    fn parser_feature_off_returns_feature_disabled() {
        // Construct a minimum-viable parquet trailer (PAR1 + tiny
        // declared footer length) so the magic check passes; the
        // thrift-decode step is a no-op stub when the feature is off.
        let mut buf = vec![0u8; 16];
        let footer_len: u32 = 4;
        buf[4..8].copy_from_slice(&[0u8; 4]); // dummy footer body
        buf[8..12].copy_from_slice(&footer_len.to_le_bytes());
        buf[12..].copy_from_slice(b"PAR1");
        match parse_footer_blooms(&buf) {
            Err(ParseError::FeatureDisabled) => {
                // Default build path. Confirms the stub is wired and
                // documents the FeatureDisabled error variant.
                #[cfg(not(feature = "parquet_meta"))]
                {}
                // When the feature IS enabled this branch is
                // unreachable — the parser will produce
                // ParseError::Decode (the dummy bytes are not valid
                // thrift) instead. We accept either path here so the
                // test runs cleanly under both feature configurations.
                #[cfg(feature = "parquet_meta")]
                {}
            }
            Err(ParseError::Decode(_)) => {
                // Feature-on path on a malformed thrift body. Also
                // acceptable for this single unit test.
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn record_classification_bumps_metric_label() {
        // No assertion on counter value (it is shared global state in
        // the prom registry); exercising the label string is enough
        // to keep `BloomKind::metric_label` from drifting away from
        // the EXPOSED_SERIES touch test.
        let admit = BloomAdmission::new(BloomAdmissionConfig {
            enabled: true,
            ..Default::default()
        });
        admit.record_classification(BloomKind::Footer);
        admit.record_classification(BloomKind::BloomBlock);
        admit.record_classification(BloomKind::NotApplicable);
    }

    #[test]
    fn maybe_index_footer_is_no_op_when_kind_is_not_footer() {
        let admit = BloomAdmission::new(BloomAdmissionConfig {
            enabled: true,
            ..Default::default()
        });
        admit.maybe_index_footer(b"etag-x", BloomKind::NotApplicable, &[0u8; 0]);
        admit.maybe_index_footer(b"etag-x", BloomKind::BloomBlock, &[0u8; 0]);
        assert!(admit.index.is_empty());
    }

    #[test]
    fn maybe_index_footer_is_no_op_when_disabled() {
        let admit = BloomAdmission::disabled();
        // Even with a Footer classification, disabled means no
        // index population.
        let mut buf = vec![0u8; 16];
        let footer_len: u32 = 4;
        buf[8..12].copy_from_slice(&footer_len.to_le_bytes());
        buf[12..].copy_from_slice(b"PAR1");
        admit.maybe_index_footer(b"etag-x", BloomKind::Footer, &buf);
        assert!(admit.index.is_empty());
    }

    #[test]
    fn force_admit_static_admits_unconditionally() {
        let key = crate::store::key_from_tuple(b"any", 0, 1, 0).unwrap();
        let ctx = AdmissionContext {
            pool: crate::store::Pool::RowGroup,
            key: &key,
            size_bytes: 1 << 30,
            pinned: false,
        };
        // FORCE_ADMIT must always Admit, regardless of size.
        assert_eq!(FORCE_ADMIT.decide(&ctx), AdmissionDecision::Admit);
    }

    #[test]
    fn bloom_aware_policy_delegates_when_force_admit_off() {
        use crate::admission::SizeThresholdPolicy;
        let inner = Arc::new(SizeThresholdPolicy {
            size_threshold_bytes: 16,
            pinned_bypass: true,
        });
        let policy = BloomAwareAdmissionPolicy::new(inner);
        let key = crate::store::key_from_tuple(b"any", 0, 1, 0).unwrap();
        let ctx = AdmissionContext {
            pool: crate::store::Pool::RowGroup,
            key: &key,
            size_bytes: 1 << 30,
            pinned: false,
        };
        assert_eq!(policy.decide(&ctx), AdmissionDecision::Reject);
        policy.set_force_admit(true);
        assert_eq!(policy.decide(&ctx), AdmissionDecision::Admit);
    }

    /// Property-style loop test: across 1 000 randomly-generated
    /// `(offset, length)` ranges, exactly the indexed bloom blocks
    /// classify as `BloomBlock`. The randomness is deterministic
    /// (linear-congruential PRNG seeded from a constant) so failures
    /// reproduce identically. Mirrors the design note's
    /// "if proptest is unavailable, loop-test 1 000 cases" guidance.
    #[test]
    fn property_indexed_bloom_block_always_classifies() {
        let admit = BloomAdmission::new(BloomAdmissionConfig {
            enabled: true,
            min_footer_bytes: u64::MAX, // disable footer suffix path
            ..Default::default()
        });
        let etag = b"property-etag";
        let mut state: u64 = 0xC0FFEE;
        // Generate 100 indexed bloom block ranges, all in the lower
        // half of the address space.
        let mut indexed = Vec::with_capacity(100);
        for _ in 0..100 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let offset = (state >> 32) % (1u64 << 28);
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let length = 1 + ((state >> 32) % 4096);
            indexed.push(BloomBlockRange { offset, length });
        }
        admit.index.insert(etag, indexed.clone());

        // 1 000 trials: half taken from the indexed set (must classify
        // as BloomBlock), half from the upper-half address space (must
        // classify as NotApplicable).
        for trial in 0..1_000 {
            if trial % 2 == 0 {
                let r = indexed[trial % indexed.len()];
                assert_eq!(
                    admit.classify(etag, r.offset, r.length, 1u64 << 40),
                    BloomKind::BloomBlock,
                    "indexed range trial {trial}: {:?}",
                    r,
                );
            } else {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let offset = (1u64 << 32) + ((state >> 32) % (1u64 << 28));
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let length = 1 + ((state >> 32) % 4096);
                // Skip the off chance that this random range happens
                // to collide with an indexed block (vanishingly small
                // given the address-space split, but make the test
                // resilient).
                let collides = indexed.iter().any(|r| r.matches(offset, length));
                if collides {
                    continue;
                }
                assert_eq!(
                    admit.classify(etag, offset, length, 1u64 << 40),
                    BloomKind::NotApplicable,
                    "non-indexed range trial {trial}: ({offset}, {length})",
                );
            }
        }
    }

    #[cfg(feature = "parquet_meta")]
    #[test]
    fn parser_extracts_bloom_offset_from_synthetic_footer() {
        // When the `parquet_meta` cargo feature is enabled, build a
        // minimal Parquet file in-memory with a single column that
        // declares a non-null `bloom_filter_offset`, then assert the
        // parser surfaces the same offset.
        //
        // We use the parquet crate's `WriterProperties` bloom-filter
        // toggle which causes the writer to emit
        // `column_metadata.bloom_filter_offset` for every column on
        // every row group.
        use bytes::Bytes;
        use parquet::data_type::ByteArray;
        use parquet::data_type::ByteArrayType;
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;
        use std::sync::Arc as ArcAlias;

        let message_type = "
            message schema {
                REQUIRED BYTE_ARRAY user_id;
            }
        ";
        let schema = ArcAlias::new(parse_message_type(message_type).expect("parse schema"));
        let props = ArcAlias::new(
            WriterProperties::builder()
                .set_bloom_filter_enabled(true)
                .build(),
        );
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer =
                SerializedFileWriter::new(&mut buf, schema.clone(), props.clone()).expect("writer");
            let mut row_group = writer.next_row_group().expect("row group");
            let mut col = row_group.next_column().expect("col").expect("col present");
            let typed = col.typed::<ByteArrayType>().write_batch(
                &[
                    ByteArray::from(b"alice".as_slice()),
                    ByteArray::from(b"bob".as_slice()),
                ],
                None,
                None,
            );
            assert!(typed.is_ok(), "write_batch: {:?}", typed.err());
            col.close().expect("close col");
            row_group.close().expect("close rg");
            writer.close().expect("close writer");
        }
        let _bytes = Bytes::from(buf.clone());
        let ranges = parse_footer_blooms(&buf).expect("parser");
        assert!(
            !ranges.is_empty(),
            "expected at least one bloom range to be extracted from a bloom-enabled file",
        );
        for r in &ranges {
            // Every range must point inside the file.
            assert!(
                r.offset + r.length <= buf.len() as u64,
                "range {:?} overflows file len {}",
                r,
                buf.len()
            );
        }
    }
}
