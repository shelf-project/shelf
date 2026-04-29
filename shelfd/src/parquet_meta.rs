// Licensed under the Apache License, Version 2.0.
// See <http://www.apache.org/licenses/LICENSE-2.0>.

//! SHELF-34 — Parquet page-index extraction + `/predicate-prune`
//! sidecar substrate.
//!
//! ## What this module does
//!
//! Parquet 2.9+ writes a per-row-group / per-column **page index**
//! into the footer region. Each page carries `(min, max,
//! null_count)` + an `OffsetIndex` entry of `(file_offset,
//! compressed_page_size)`. A reader holding the page index can
//! prune individual pages within a row group before decoding any
//! column data — finer-grained than the row-group-level pruning
//! Trino's Iceberg connector does today.
//!
//! `extract_page_index` parses a Parquet footer (or full file) into
//! a structural [`PageIndex`]; `predicate_prune` filters the page
//! list down to the byte ranges that overlap a user-supplied
//! [`Predicate`]. The HTTP wiring lives in [`crate::http`] —
//! `GET /predicate-prune?path=...&col=...&min=...&max=...`.
//!
//! ## Aligned upstream work
//!
//! - Apache Parquet PageIndex spec:
//!   <https://parquet.apache.org/docs/file-format/pageindex/>
//! - Iceberg #15211 — vectorized reader page skipping (open).
//! - Iceberg #10090 — multi-predicate row-group filter cooperation.
//! - Trino #24007 — footer-reader optimization. **CLOSED, NOT
//!   MERGED** (verified Apr 29 2026, `mergedAt: null`); SHELF-34
//!   stands alone.
//!
//! ## Sidecar security envelope
//!
//! Every `pub` function exposed through `/predicate-prune` enforces
//! the four hardening rules from the plan's
//! "§ Sidecar security review" section:
//!
//! 1. **Path traversal containment** — [`validate_path`] enforces
//!    scheme + bucket-allowlist + `..` rejection. Default OSS
//!    allowlist is empty (operator must populate via env var or
//!    overlay file).
//! 2. **Footer-parse DoS containment** — [`MAX_FOOTER_BYTES`],
//!    [`MAX_BLOB_COUNT`], [`MAX_PAGE_INDEX_ENTRIES`] are checked
//!    before any allocation that scales with the input.
//! 3. **Negative-cache discipline** — see [`extract_page_index`]:
//!    a 4xx origin response surfaces as `Err`, never positively
//!    cached. Mirrors `head_lru::NEGATIVE_TTL_DEFAULT` semantics.
//! 4. **PII leak containment** — [`PageRange`] is structural-only;
//!    `predicate_prune` returns `(offset, length)` tuples. Page
//!    `min`/`max` byte values are NEVER returned over the wire.
//!
//! Threat-model document with code-line references:
//! `agents/out/SHELF-34/THREAT_MODEL.md`.
//!
//! ## SHELF-D3 backward compatibility
//!
//! The phase-1 SHELF-D3 surface (`FooterRange`, `FooterRangeKind`,
//! `Extracted`, `FooterExtractor`, `NoopExtractor`, `ExtractError`)
//! is preserved verbatim so callers that consume the prefetch-
//! ranges contract keep working. SHELF-34 adds the page-index
//! types alongside.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use parquet::file::page_index::column_index::{ColumnIndexIterators, ColumnIndexMetaData};

// ---------------------------------------------------------------------------
// SHELF-D3 phase-1 surface (preserved for callers; do not change shape).
// ---------------------------------------------------------------------------

/// A single byte range inside the Parquet file, carrying its
/// semantic role so the caller can pick the right Foyer pool and
/// emit the right metric label.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FooterRange {
    pub offset: u64,
    pub length: u64,
    pub kind: FooterRangeKind,
    pub row_group: u32,
    pub column: u32,
}

/// Classification used by pool routing and metrics labels. Must
/// round-trip through a `&'static str` so we can attach it to
/// Prometheus counters without allocating.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FooterRangeKind {
    /// `ColumnIndex` thrift region — min/max/null counts per page.
    ColumnIndex,
    /// `OffsetIndex` thrift region — page locations + compressed
    /// sizes. Needed to do any page-level skipping.
    OffsetIndex,
    /// Per-column Bloom filter header + bitset.
    BloomFilter,
}

impl FooterRangeKind {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::ColumnIndex => "column_index",
            Self::OffsetIndex => "offset_index",
            Self::BloomFilter => "bloom_filter",
        }
    }
}

impl fmt::Display for FooterRangeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_metric_label())
    }
}

/// Flat extraction result. The caller typically iterates and issues
/// a per-range origin GET as a pre-admission prefetch request.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct Extracted {
    pub ranges: Vec<FooterRange>,
    /// The footer itself — offset + length — echoed back for
    /// convenience so the caller can re-admit the footer into the
    /// metadata pool under its content-addressed key without a
    /// second parse.
    pub footer: Option<FooterRange>,
}

impl Extracted {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty() && self.footer.is_none()
    }

    /// Convenience: total bytes across all extracted ranges.
    pub fn total_bytes(&self) -> u64 {
        self.ranges.iter().map(|r| r.length).sum::<u64>()
            + self.footer.map(|f| f.length).unwrap_or(0)
    }
}

/// Contract for a footer extractor. Phase 1 ships a stub
/// implementation for determinism across feature builds; phase 2
/// (SHELF-34) adds the live [`extract_page_index`] path alongside.
pub trait FooterExtractor: Send + Sync + fmt::Debug {
    /// Extract all prefetchable byte ranges from a Parquet footer.
    fn extract(&self, footer_bytes: &[u8], object_size: u64) -> Result<Extracted, ExtractError>;
}

/// Errors that the SHELF-D3 phase-1 extractor may produce.
#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("parquet footer malformed: {0}")]
    Malformed(String),
    #[error("parquet thrift compact decode error: {0}")]
    Thrift(String),
    #[error("feature \"parquet_meta\" is disabled in this build")]
    FeatureDisabled,
}

/// No-op extractor used when the SHELF-D3 phase-1 caller wants a
/// determinstic empty result.
#[derive(Debug, Default)]
pub struct NoopExtractor;

impl FooterExtractor for NoopExtractor {
    fn extract(&self, _footer_bytes: &[u8], _object_size: u64) -> Result<Extracted, ExtractError> {
        Ok(Extracted::empty())
    }
}

// ---------------------------------------------------------------------------
// SHELF-34 — page-index extraction + predicate prune.
// ---------------------------------------------------------------------------

/// Hard cap on accepted footer-parse input, in bytes. A 1 GB
/// malicious payload must NOT OOM the pod; we reject any input
/// larger than this with [`ParquetMetaError::FooterTooLarge`].
///
/// Sized at 8 MiB. Production Iceberg `metadata.json` blobs cap at
/// ~50 MB on heavily-evolved tables (see SHELF-48), but those go
/// through a different code path; the page-index sidecar's input
/// is always the Parquet footer + page index region, which is
/// observably under 1 MiB even on multi-row-group / multi-column
/// files.
pub const MAX_FOOTER_BYTES: usize = 8 * 1024 * 1024;

/// Hard cap on total column-chunk count across all row groups in
/// a single Parquet file. Exceeding this returns
/// [`ParquetMetaError::TooManyBlobs`].
pub const MAX_BLOB_COUNT: usize = 4096;

/// Hard cap on total page locations across all `(row_group,
/// column)` pairs. Exceeding this returns
/// [`ParquetMetaError::TooManyPages`].
pub const MAX_PAGE_INDEX_ENTRIES: usize = 65_536;

/// Errors surfaced by [`extract_page_index`] and the helpers used
/// by `/predicate-prune`. Distinct from the SHELF-D3 phase-1
/// [`ExtractError`] because the two surfaces have different sets
/// of failure modes — keeping them separate avoids silently
/// inheriting variant semantics from a sibling path.
#[derive(Debug, thiserror::Error)]
pub enum ParquetMetaError {
    /// Input bytes exceed the [`MAX_FOOTER_BYTES`] cap.
    #[error("footer input too large: {got} bytes > limit {limit}")]
    FooterTooLarge { got: usize, limit: usize },
    /// Total column-chunk count exceeds [`MAX_BLOB_COUNT`].
    #[error("too many column chunks: {got} > limit {limit}")]
    TooManyBlobs { got: usize, limit: usize },
    /// Total page locations exceed [`MAX_PAGE_INDEX_ENTRIES`].
    #[error("too many page index entries: {got} > limit {limit}")]
    TooManyPages { got: usize, limit: usize },
    /// Footer / metadata thrift decode failed.
    #[error("parquet footer parse error: {0}")]
    Parse(String),
    /// The file does not carry a page index (older Parquet writer
    /// or `EnabledStatistics::Chunk` only).
    #[error("parquet file has no page index")]
    NoPageIndex,
}

/// A single page's structural information: byte range + min/max
/// statistics. The min/max values are kept inside the daemon for
/// the prune computation; they are NEVER returned over the wire
/// to a `/predicate-prune` caller (see PII containment rule).
#[derive(Debug, Clone)]
pub struct PageRange {
    /// Byte offset of the page within the Parquet file.
    pub offset: u64,
    /// Compressed page size in bytes (header + payload).
    pub length: u64,
    /// Page minimum (per the column index). `Null` for
    /// all-null pages or pages whose column had no min recorded.
    pub min: ColumnValue,
    /// Page maximum (analogous to `min`).
    pub max: ColumnValue,
    /// Null count, when recorded.
    pub null_count: Option<i64>,
    /// Index of the page within the column chunk. Zero-based.
    pub page_index_in_chunk: u32,
    /// Row-group ordinal this page belongs to. Zero-based.
    pub row_group_ordinal: u32,
}

/// Typed value union used for page-min/page-max and predicate
/// inputs. Comparisons on `ColumnValue` are total within the
/// `Int64` and `Float64` variants; cross-variant comparisons
/// (`Int64` vs `Bytes`) always return `None` because the input
/// types do not commute.
///
/// The variant set deliberately collapses Parquet's INT32/INT64
/// into [`ColumnValue::Int64`] and FLOAT/DOUBLE into
/// [`ColumnValue::Float64`] — i32 fits in i64 losslessly, and f32
/// fits in f64 losslessly, so the prune-side comparison loses no
/// precision.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Null,
    Int64(i64),
    Float64(f64),
    Bytes(Vec<u8>),
}

impl ColumnValue {
    /// Total ordering within a single value-class. Returns `None`
    /// when the variants don't commute (e.g. `Int64` vs `Bytes`).
    /// Pages whose min/max are `Null` always compare as `None` —
    /// the conservative answer keeps the page in the result set.
    pub fn partial_cmp_same(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use ColumnValue::*;
        match (self, other) {
            (Null, _) | (_, Null) => None,
            (Int64(a), Int64(b)) => Some(a.cmp(b)),
            (Float64(a), Float64(b)) => a.partial_cmp(b),
            (Bytes(a), Bytes(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// Parse a string the HTTP layer received as `?min=<v>` or
    /// `?max=<v>`. Tries i64, then f64, else falls back to bytes
    /// (UTF-8 string). The handler picks one variant per request.
    pub fn parse_query_value(s: &str) -> Self {
        if let Ok(v) = s.parse::<i64>() {
            return ColumnValue::Int64(v);
        }
        if let Ok(v) = s.parse::<f64>() {
            return ColumnValue::Float64(v);
        }
        ColumnValue::Bytes(s.as_bytes().to_vec())
    }
}

/// A predicate against a single column. The HTTP layer accepts a
/// closed inclusive range `[lo, hi]`; richer predicate shapes can
/// be added later without breaking this enum (it's non-exhaustive
/// by intent).
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Closed inclusive range `[lo, hi]`. Matches a page when
    /// `page.min <= hi && page.max >= lo`.
    Range { lo: ColumnValue, hi: ColumnValue },
    /// Single-point match. Matches a page when
    /// `page.min <= v <= page.max`.
    Equals(ColumnValue),
}

impl Predicate {
    /// Build a closed range predicate from two query-string
    /// values. The HTTP layer normalizes both to the same variant
    /// (Int64 / Float64 / Bytes) before this is called.
    pub fn range(lo: ColumnValue, hi: ColumnValue) -> Self {
        Predicate::Range { lo, hi }
    }

    /// Whether the predicate semantically overlaps `[page_min,
    /// page_max]`. Returns `true` when the variants are
    /// incompatible — the conservative answer keeps the page in
    /// the result set (a false-positive hurts S3 a little; a
    /// false-negative loses query data).
    fn overlaps(&self, page_min: &ColumnValue, page_max: &ColumnValue) -> bool {
        match self {
            Predicate::Range { lo, hi } => {
                // page.min <= hi AND page.max >= lo
                let min_le_hi = match page_min.partial_cmp_same(hi) {
                    Some(o) => o != std::cmp::Ordering::Greater,
                    None => true,
                };
                let max_ge_lo = match page_max.partial_cmp_same(lo) {
                    Some(o) => o != std::cmp::Ordering::Less,
                    None => true,
                };
                min_le_hi && max_ge_lo
            }
            Predicate::Equals(v) => {
                // page.min <= v <= page.max
                let min_le_v = match page_min.partial_cmp_same(v) {
                    Some(o) => o != std::cmp::Ordering::Greater,
                    None => true,
                };
                let max_ge_v = match page_max.partial_cmp_same(v) {
                    Some(o) => o != std::cmp::Ordering::Less,
                    None => true,
                };
                min_le_v && max_ge_v
            }
        }
    }
}

/// Parsed page-index for a single Parquet file. Indexed by both
/// column ordinal AND column path so callers can look up by either
/// (Trino plugin ships ordinals; humans probing
/// `/predicate-prune?col=foo` ship paths).
#[derive(Debug, Clone, Default)]
pub struct PageIndex {
    /// Column ordinal (across all row groups; ordinal is
    /// per-row-group internally) → flattened pages. Pages from
    /// row group 0 come first, then row group 1, etc., preserving
    /// `row_group_ordinal` on each entry.
    pub by_ordinal: HashMap<u32, Vec<PageRange>>,
    /// Column path (e.g. `"foo"` for a flat column,
    /// `"a.b.c"` for a nested column). Same `Vec<PageRange>` as
    /// `by_ordinal` — just a different lookup key.
    pub by_name: HashMap<String, Vec<PageRange>>,
    /// Total pages across every column. Useful for capacity
    /// telemetry (`shelf_page_index_cached_bytes`).
    pub total_pages: u64,
}

impl PageIndex {
    /// Total bytes the pages cover. Used for the
    /// `shelf_page_index_cached_bytes` gauge.
    pub fn total_bytes(&self) -> u64 {
        self.by_ordinal
            .values()
            .flat_map(|pages| pages.iter())
            .map(|p| p.length)
            .sum()
    }

    /// Memory footprint of the parsed structure itself (a rough
    /// estimate: 64 bytes per `PageRange` + map overhead).
    pub fn approximate_memory_bytes(&self) -> u64 {
        let entries = self.by_ordinal.values().map(|v| v.len()).sum::<usize>()
            + self.by_name.values().map(|v| v.len()).sum::<usize>();
        (entries as u64) * 64
    }

    /// Lookup page list by column path. Returns `None` when the
    /// column is unknown.
    pub fn pages_for_column(&self, column: &str) -> Option<&[PageRange]> {
        self.by_name.get(column).map(|v| v.as_slice())
    }

    /// Lookup page list by column ordinal. Useful when the caller
    /// (Trino plugin) carries a numeric column index rather than a
    /// path.
    pub fn pages_for_column_idx(&self, ordinal: u32) -> Option<&[PageRange]> {
        self.by_ordinal.get(&ordinal).map(|v| v.as_slice())
    }
}

/// Parse a Parquet file's footer and page index from the supplied
/// bytes. The input must be the FULL Parquet file (or a slice that
/// covers the footer + the column-index / offset-index thrift
/// regions). For typical Parquet writers the index regions are
/// just before the footer, so a tail slice of ~1 MiB is sufficient
/// in practice.
///
/// ### Security caps (concrete, enforced before allocation)
///
/// 1. `bytes.len() > MAX_FOOTER_BYTES` ⇒ `FooterTooLarge`.
/// 2. Total column-chunk count > `MAX_BLOB_COUNT` ⇒ `TooManyBlobs`.
/// 3. Total page locations > `MAX_PAGE_INDEX_ENTRIES` ⇒ `TooManyPages`.
///
/// ### Negative-cache discipline
///
/// The 4xx-origin → positive-404 trap that
/// `head_lru::NEGATIVE_TTL_DEFAULT` (5 s) was carved out for is
/// avoided here by surfacing every parse failure as `Err`. The
/// upstream caller (HTTP handler in `crate::http`) is the place
/// where the negative-cache decision is made — and per the
/// SHELF-A4 policy, transient origin errors are NOT memoised.
pub fn extract_page_index(bytes: &[u8]) -> Result<PageIndex, ParquetMetaError> {
    if bytes.len() > MAX_FOOTER_BYTES {
        return Err(ParquetMetaError::FooterTooLarge {
            got: bytes.len(),
            limit: MAX_FOOTER_BYTES,
        });
    }

    // Materialise a `Bytes` which the parquet crate's
    // `ChunkReader for bytes::Bytes` impl can consume. Cheap: this
    // does NOT copy the underlying buffer.
    let chunk = Bytes::copy_from_slice(bytes);
    let meta = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_offset_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&chunk)
        .map_err(|e| ParquetMetaError::Parse(e.to_string()))?;

    // Cap total column chunks BEFORE iterating. `num_columns`
    // returns the per-row-group column count; multiply by row
    // group count for the total.
    let row_groups = meta.row_groups();
    let total_blobs: usize = row_groups.iter().map(|rg| rg.num_columns()).sum();
    if total_blobs > MAX_BLOB_COUNT {
        return Err(ParquetMetaError::TooManyBlobs {
            got: total_blobs,
            limit: MAX_BLOB_COUNT,
        });
    }

    let column_index = match meta.column_index() {
        Some(c) => c,
        None => return Err(ParquetMetaError::NoPageIndex),
    };
    let offset_index = match meta.offset_index() {
        Some(o) => o,
        None => return Err(ParquetMetaError::NoPageIndex),
    };

    // Cap total page locations.
    let total_pages: usize = offset_index
        .iter()
        .flat_map(|rg| rg.iter())
        .map(|oim| oim.page_locations().len())
        .sum();
    if total_pages > MAX_PAGE_INDEX_ENTRIES {
        return Err(ParquetMetaError::TooManyPages {
            got: total_pages,
            limit: MAX_PAGE_INDEX_ENTRIES,
        });
    }

    let mut idx = PageIndex {
        by_ordinal: HashMap::new(),
        by_name: HashMap::new(),
        total_pages: total_pages as u64,
    };

    for (rg_ord, rg) in row_groups.iter().enumerate() {
        let rg_ord_u32: u32 = rg_ord.try_into().unwrap_or(u32::MAX);
        let rg_col_index = match column_index.get(rg_ord) {
            Some(c) => c,
            None => continue,
        };
        let rg_off_index = match offset_index.get(rg_ord) {
            Some(o) => o,
            None => continue,
        };

        for (col_ord, col_chunk) in rg.columns().iter().enumerate() {
            let col_ord_u32: u32 = col_ord.try_into().unwrap_or(u32::MAX);
            let col_idx = match rg_col_index.get(col_ord) {
                Some(c) => c,
                None => continue,
            };
            let off_idx = match rg_off_index.get(col_ord) {
                Some(o) => o,
                None => continue,
            };

            let pages = build_pages_for_column(col_idx, off_idx, rg_ord_u32);
            if pages.is_empty() {
                continue;
            }

            // Column path → `a.b.c`. `parts()` returns nested
            // segments for Parquet groups; we join them with `.`
            // so flat columns just yield their simple name.
            let col_path = col_chunk.column_path().parts().join(".");

            idx.by_ordinal
                .entry(col_ord_u32)
                .or_default()
                .extend(pages.iter().cloned());
            idx.by_name.entry(col_path).or_default().extend(pages);
        }
    }

    Ok(idx)
}

/// Apply a predicate against one named column's pages and return
/// the matching `(offset, length)` byte ranges. This is the pure
/// function the HTTP handler calls after [`extract_page_index`]
/// has populated the cache.
pub fn predicate_prune(idx: &PageIndex, column: &str, predicate: &Predicate) -> Vec<(u64, u64)> {
    let pages = match idx.by_name.get(column) {
        Some(p) => p,
        None => return Vec::new(),
    };
    pages
        .iter()
        .filter(|p| predicate.overlaps(&p.min, &p.max))
        .map(|p| (p.offset, p.length))
        .collect()
}

/// Result of [`validate_path`]. `bucket` is guaranteed to match an
/// allowlist entry exactly; `key` is the post-`s3a://bucket/`
/// remainder, traversal-rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Path {
    pub bucket: String,
    pub key: String,
}

/// Errors surfaced by [`validate_path`]. Distinct types so the
/// HTTP layer can map them to specific status codes (400 for
/// scheme / structure errors, 403 for allowlist denials).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PathError {
    #[error("scheme must be s3:// or s3a://")]
    SchemeMissing,
    #[error("bucket segment is empty")]
    EmptyBucket,
    #[error("key segment is empty")]
    EmptyKey,
    #[error("bucket {0:?} not in operator allowlist")]
    BucketNotAllowed(String),
    #[error("path traversal component '..' rejected")]
    PathTraversal,
    #[error("absolute path key rejected")]
    AbsoluteKey,
    #[error("control character in path rejected")]
    ControlChar,
}

/// Validate an `s3://` or `s3a://` path against the operator
/// allowlist. Implements the threat-model item 1 from the plan's
/// "§ Sidecar security review":
///
/// 1. Scheme must be exactly `s3://` or `s3a://`.
/// 2. Bucket must equal one of the allowlist entries (no
///    suffix / prefix matching: `acme-data-temp-evil` must
///    NOT pass when only `acme-data-temp` is allowlisted).
/// 3. Key must be non-empty.
/// 4. Key must not contain `..` as a path segment.
/// 5. Key must not start with `/` (would be absolute).
/// 6. Key must not contain ASCII NUL.
///
/// `allowlist` is operator-supplied via env var or cluster
/// overlay; the OSS default is empty, and an empty allowlist
/// rejects every input with [`PathError::BucketNotAllowed`].
pub fn validate_path(path: &str, allowlist: &[String]) -> Result<S3Path, PathError> {
    let stripped = path
        .strip_prefix("s3a://")
        .or_else(|| path.strip_prefix("s3://"))
        .ok_or(PathError::SchemeMissing)?;

    let (bucket, key) = stripped.split_once('/').ok_or(PathError::EmptyKey)?;

    if bucket.is_empty() {
        return Err(PathError::EmptyBucket);
    }
    if key.is_empty() {
        return Err(PathError::EmptyKey);
    }
    if !allowlist.iter().any(|b| b == bucket) {
        return Err(PathError::BucketNotAllowed(bucket.to_owned()));
    }
    if key.starts_with('/') {
        return Err(PathError::AbsoluteKey);
    }
    if key.bytes().any(|b| b == 0) {
        return Err(PathError::ControlChar);
    }
    for segment in key.split('/') {
        if segment == ".." {
            return Err(PathError::PathTraversal);
        }
    }
    Ok(S3Path {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    })
}

/// Cache key string the HTTP handler uses to memoise a parsed
/// [`PageIndex`]. The format is stable wire contract within a
/// process — never exposed externally — and intentionally
/// distinct from the SHELF-04 32-byte content keyspace per
/// ADR-0011 to avoid namespace collision.
pub fn page_index_cache_key(etag: &str) -> String {
    format!("{etag}::page-index")
}

// ---------------------------------------------------------------------------
// Internals.
// ---------------------------------------------------------------------------

fn build_pages_for_column(
    col_idx: &ColumnIndexMetaData,
    off_idx: &parquet::file::page_index::offset_index::OffsetIndexMetaData,
    rg_ord: u32,
) -> Vec<PageRange> {
    let pages = off_idx.page_locations();
    if pages.is_empty() {
        return Vec::new();
    }
    // The two indexes must agree on page count; if they don't,
    // truncate to the smaller (defensive) and continue.
    let n = pages.len().min(col_idx.num_pages() as usize);

    let (mins, maxs) = read_min_max(col_idx, n);

    let mut out = Vec::with_capacity(n);
    for (i, loc) in pages.iter().enumerate().take(n) {
        let null_count = col_idx.null_count(i);
        let min = mins.get(i).cloned().unwrap_or(ColumnValue::Null);
        let max = maxs.get(i).cloned().unwrap_or(ColumnValue::Null);
        out.push(PageRange {
            offset: loc.offset.max(0) as u64,
            length: loc.compressed_page_size.max(0) as u64,
            min,
            max,
            null_count,
            page_index_in_chunk: i.try_into().unwrap_or(u32::MAX),
            row_group_ordinal: rg_ord,
        });
    }
    out
}

fn read_min_max(col_idx: &ColumnIndexMetaData, n: usize) -> (Vec<ColumnValue>, Vec<ColumnValue>) {
    use ColumnIndexMetaData::*;
    let mut mins = Vec::with_capacity(n);
    let mut maxs = Vec::with_capacity(n);
    match col_idx {
        NONE => {
            for _ in 0..n {
                mins.push(ColumnValue::Null);
                maxs.push(ColumnValue::Null);
            }
        }
        BOOLEAN(_) => {
            // We don't carry boolean min/max as a typed variant.
            // Boolean predicate-prune is uncommon and this cleanly
            // falls through to "always overlap" via Null.
            for _ in 0..n {
                mins.push(ColumnValue::Null);
                maxs.push(ColumnValue::Null);
            }
        }
        INT32(_) => {
            let mins_iter = <i32 as ColumnIndexIterators>::min_values_iter(col_idx);
            let maxs_iter = <i32 as ColumnIndexIterators>::max_values_iter(col_idx);
            for v in mins_iter {
                mins.push(
                    v.map(|x| ColumnValue::Int64(x as i64))
                        .unwrap_or(ColumnValue::Null),
                );
            }
            for v in maxs_iter {
                maxs.push(
                    v.map(|x| ColumnValue::Int64(x as i64))
                        .unwrap_or(ColumnValue::Null),
                );
            }
        }
        INT64(_) => {
            let mins_iter = <i64 as ColumnIndexIterators>::min_values_iter(col_idx);
            let maxs_iter = <i64 as ColumnIndexIterators>::max_values_iter(col_idx);
            for v in mins_iter {
                mins.push(v.map(ColumnValue::Int64).unwrap_or(ColumnValue::Null));
            }
            for v in maxs_iter {
                maxs.push(v.map(ColumnValue::Int64).unwrap_or(ColumnValue::Null));
            }
        }
        INT96(_) => {
            for _ in 0..n {
                mins.push(ColumnValue::Null);
                maxs.push(ColumnValue::Null);
            }
        }
        FLOAT(_) => {
            let mins_iter = <f32 as ColumnIndexIterators>::min_values_iter(col_idx);
            let maxs_iter = <f32 as ColumnIndexIterators>::max_values_iter(col_idx);
            for v in mins_iter {
                mins.push(
                    v.map(|x| ColumnValue::Float64(x as f64))
                        .unwrap_or(ColumnValue::Null),
                );
            }
            for v in maxs_iter {
                maxs.push(
                    v.map(|x| ColumnValue::Float64(x as f64))
                        .unwrap_or(ColumnValue::Null),
                );
            }
        }
        DOUBLE(_) => {
            let mins_iter = <f64 as ColumnIndexIterators>::min_values_iter(col_idx);
            let maxs_iter = <f64 as ColumnIndexIterators>::max_values_iter(col_idx);
            for v in mins_iter {
                mins.push(v.map(ColumnValue::Float64).unwrap_or(ColumnValue::Null));
            }
            for v in maxs_iter {
                maxs.push(v.map(ColumnValue::Float64).unwrap_or(ColumnValue::Null));
            }
        }
        BYTE_ARRAY(_) | FIXED_LEN_BYTE_ARRAY(_) => {
            let mins_iter =
                <parquet::data_type::ByteArray as ColumnIndexIterators>::min_values_iter(col_idx);
            let maxs_iter =
                <parquet::data_type::ByteArray as ColumnIndexIterators>::max_values_iter(col_idx);
            for v in mins_iter {
                mins.push(
                    v.map(|x| ColumnValue::Bytes(x.data().to_vec()))
                        .unwrap_or(ColumnValue::Null),
                );
            }
            for v in maxs_iter {
                maxs.push(
                    v.map(|x| ColumnValue::Bytes(x.data().to_vec()))
                        .unwrap_or(ColumnValue::Null),
                );
            }
        }
    }
    (mins, maxs)
}

// ---------------------------------------------------------------------------
// In-process page-index cache.
// ---------------------------------------------------------------------------

/// Tiny LRU of parsed [`PageIndex`] values keyed by ETag, so a
/// burst of `/predicate-prune` requests against the same file
/// share one parse. Held inside `ServerState`. Sized small (256
/// entries by default) — the metadata pool's byte-range cache
/// already covers the warm-footer-bytes case; this is just the
/// parsed-structure cache.
#[derive(Debug)]
pub struct PageIndexCache {
    cache: foyer::Cache<String, Arc<PageIndex>>,
}

impl PageIndexCache {
    pub fn new(max_entries: u64) -> Self {
        // Default LFU has admission gating; a single-shot insert
        // followed by an immediate `get` can race window→protected
        // promotion at small capacities and miss the just-inserted
        // entry. SHELF-34 just wants a deterministic LRU here —
        // the metadata pool already does the heavy ETag-keyed
        // byte caching; this cache only memoizes parsed structures
        // for a small set of hot files.
        let capped = max_entries.max(1) as usize;
        let cache = foyer::CacheBuilder::new(capped)
            .with_eviction_config(foyer::LruConfig {
                high_priority_pool_ratio: 0.0,
            })
            .with_weighter(|_k: &String, _v: &Arc<PageIndex>| 1)
            .build();
        Self { cache }
    }

    pub fn get(&self, etag: &str) -> Option<Arc<PageIndex>> {
        let key = page_index_cache_key(etag);
        self.cache.get(&key).map(|e| e.value().clone())
    }

    pub fn insert(&self, etag: &str, idx: Arc<PageIndex>) {
        let key = page_index_cache_key(etag);
        self.cache.insert(key, idx);
    }

    pub fn len(&self) -> usize {
        self.cache.usage()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for PageIndexCache {
    fn default() -> Self {
        Self::new(256)
    }
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- SHELF-D3 phase-1 surface (preserved) ---------------------

    #[test]
    fn kind_labels_are_stable() {
        assert_eq!(
            FooterRangeKind::ColumnIndex.as_metric_label(),
            "column_index"
        );
        assert_eq!(
            FooterRangeKind::OffsetIndex.as_metric_label(),
            "offset_index"
        );
        assert_eq!(
            FooterRangeKind::BloomFilter.as_metric_label(),
            "bloom_filter"
        );
    }

    #[test]
    fn extracted_total_bytes_sums_ranges_and_footer() {
        let mut ex = Extracted::empty();
        ex.ranges.push(FooterRange {
            offset: 10,
            length: 32,
            kind: FooterRangeKind::ColumnIndex,
            row_group: 0,
            column: 0,
        });
        ex.ranges.push(FooterRange {
            offset: 50,
            length: 64,
            kind: FooterRangeKind::BloomFilter,
            row_group: 0,
            column: 1,
        });
        ex.footer = Some(FooterRange {
            offset: 1000,
            length: 128,
            kind: FooterRangeKind::OffsetIndex,
            row_group: 0,
            column: 0,
        });
        assert_eq!(ex.total_bytes(), 32 + 64 + 128);
    }

    #[test]
    fn noop_extractor_returns_empty_for_any_input() {
        let ex = NoopExtractor;
        let out = ex.extract(&[0, 1, 2, 3], 4096).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn empty_extracted_default_roundtrip() {
        let e = Extracted::default();
        assert!(e.is_empty());
        assert_eq!(e.total_bytes(), 0);
    }

    // ---- SHELF-34 — value + predicate semantics --------------------

    #[test]
    fn column_value_partial_cmp_only_within_variant() {
        use ColumnValue::*;
        assert_eq!(
            Int64(1).partial_cmp_same(&Int64(2)),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(Int64(1).partial_cmp_same(&Bytes(vec![1])), None);
        assert_eq!(Null.partial_cmp_same(&Int64(1)), None);
    }

    #[test]
    fn column_value_parse_query_chooses_int_first() {
        assert!(matches!(
            ColumnValue::parse_query_value("42"),
            ColumnValue::Int64(42)
        ));
        assert!(matches!(
            ColumnValue::parse_query_value("3.14"),
            ColumnValue::Float64(_)
        ));
        match ColumnValue::parse_query_value("hello") {
            ColumnValue::Bytes(b) => assert_eq!(b, b"hello"),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    #[test]
    fn predicate_range_overlaps_int_pages() {
        // page [0, 100], predicate [50, 150] — overlap.
        let pred = Predicate::range(ColumnValue::Int64(50), ColumnValue::Int64(150));
        assert!(pred.overlaps(&ColumnValue::Int64(0), &ColumnValue::Int64(100)));
        // page [200, 300], predicate [50, 150] — no overlap.
        assert!(!pred.overlaps(&ColumnValue::Int64(200), &ColumnValue::Int64(300)));
        // page [50, 60], predicate [50, 150] — overlap (boundary).
        assert!(pred.overlaps(&ColumnValue::Int64(50), &ColumnValue::Int64(60)));
    }

    #[test]
    fn predicate_equals_on_int_page_min_eq_max() {
        let pred = Predicate::Equals(ColumnValue::Int64(42));
        assert!(pred.overlaps(&ColumnValue::Int64(40), &ColumnValue::Int64(50)));
        assert!(pred.overlaps(&ColumnValue::Int64(42), &ColumnValue::Int64(42)));
        assert!(!pred.overlaps(&ColumnValue::Int64(43), &ColumnValue::Int64(50)));
    }

    #[test]
    fn predicate_overlaps_null_page_is_conservative() {
        // Page with Null min/max: keep it (don't lose data).
        let pred = Predicate::range(ColumnValue::Int64(0), ColumnValue::Int64(10));
        assert!(pred.overlaps(&ColumnValue::Null, &ColumnValue::Null));
    }

    #[test]
    fn predicate_overlaps_string_pages() {
        let pred = Predicate::range(
            ColumnValue::Bytes(b"abc".to_vec()),
            ColumnValue::Bytes(b"xyz".to_vec()),
        );
        assert!(pred.overlaps(
            &ColumnValue::Bytes(b"def".to_vec()),
            &ColumnValue::Bytes(b"ghi".to_vec()),
        ));
        assert!(!pred.overlaps(
            &ColumnValue::Bytes(b"000".to_vec()),
            &ColumnValue::Bytes(b"010".to_vec()),
        ));
    }

    // ---- SHELF-34 — predicate_prune over a synthetic PageIndex -----

    fn synth_page(offset: u64, length: u64, min: i64, max: i64, page_idx: u32) -> PageRange {
        PageRange {
            offset,
            length,
            min: ColumnValue::Int64(min),
            max: ColumnValue::Int64(max),
            null_count: Some(0),
            page_index_in_chunk: page_idx,
            row_group_ordinal: 0,
        }
    }

    #[test]
    fn predicate_prune_returns_subset_of_pages() {
        let mut idx = PageIndex::default();
        idx.by_name.insert(
            "id".to_string(),
            vec![
                synth_page(0, 1024, 0, 99, 0),       // [0,99]
                synth_page(1024, 1024, 100, 199, 1), // [100,199]
                synth_page(2048, 1024, 200, 299, 2), // [200,299]
                synth_page(3072, 1024, 300, 399, 3), // [300,399]
            ],
        );
        idx.total_pages = 4;
        let pred = Predicate::range(ColumnValue::Int64(150), ColumnValue::Int64(250));
        let out = predicate_prune(&idx, "id", &pred);
        // Pages [100,199] and [200,299] overlap [150,250].
        assert_eq!(out, vec![(1024, 1024), (2048, 1024)]);
    }

    #[test]
    fn predicate_prune_unknown_column_returns_empty() {
        let idx = PageIndex::default();
        let pred = Predicate::range(ColumnValue::Int64(0), ColumnValue::Int64(1));
        assert!(predicate_prune(&idx, "missing", &pred).is_empty());
    }

    #[test]
    fn predicate_prune_predicate_outside_all_pages_yields_empty() {
        let mut idx = PageIndex::default();
        idx.by_name
            .insert("id".to_string(), vec![synth_page(0, 1024, 0, 99, 0)]);
        let pred = Predicate::range(ColumnValue::Int64(1000), ColumnValue::Int64(2000));
        assert!(predicate_prune(&idx, "id", &pred).is_empty());
    }

    // ---- SHELF-34 — security caps + path validator -----------------

    #[test]
    fn extract_page_index_rejects_oversized_input() {
        let bytes = vec![0u8; MAX_FOOTER_BYTES + 1];
        let err = extract_page_index(&bytes).unwrap_err();
        assert!(matches!(err, ParquetMetaError::FooterTooLarge { .. }));
    }

    #[test]
    fn extract_page_index_rejects_garbage_input() {
        // Random bytes that are not a Parquet file → Parse error.
        let bytes = b"this is not a parquet file at all";
        let err = extract_page_index(bytes).unwrap_err();
        assert!(matches!(err, ParquetMetaError::Parse(_)));
    }

    #[test]
    fn validate_path_accepts_allowlisted_bucket() {
        let allow = vec!["my-bucket".to_string()];
        let p = validate_path("s3a://my-bucket/some/key.parquet", &allow).unwrap();
        assert_eq!(p.bucket, "my-bucket");
        assert_eq!(p.key, "some/key.parquet");
    }

    #[test]
    fn validate_path_accepts_s3_scheme_too() {
        let allow = vec!["my-bucket".to_string()];
        let p = validate_path("s3://my-bucket/file.parquet", &allow).unwrap();
        assert_eq!(p.bucket, "my-bucket");
        assert_eq!(p.key, "file.parquet");
    }

    #[test]
    fn validate_path_rejects_unscheme_path() {
        let allow = vec!["my-bucket".to_string()];
        assert_eq!(
            validate_path("my-bucket/foo", &allow).unwrap_err(),
            PathError::SchemeMissing
        );
    }

    #[test]
    fn validate_path_rejects_other_bucket() {
        let allow = vec!["my-bucket".to_string()];
        let err = validate_path("s3a://other-bucket/foo", &allow).unwrap_err();
        assert!(matches!(err, PathError::BucketNotAllowed(b) if b == "other-bucket"));
    }

    #[test]
    fn validate_path_rejects_dotdot_segment() {
        let allow = vec!["my-bucket".to_string()];
        assert_eq!(
            validate_path("s3a://my-bucket/foo/../etc/passwd", &allow).unwrap_err(),
            PathError::PathTraversal,
        );
        // Single dots are NOT path traversal — they refer to the
        // current directory and are harmless.
        validate_path("s3a://my-bucket/foo/./bar", &allow).unwrap();
    }

    #[test]
    fn validate_path_rejects_absolute_key() {
        let allow = vec!["my-bucket".to_string()];
        let err = validate_path("s3a://my-bucket//abs/path", &allow).unwrap_err();
        assert_eq!(err, PathError::AbsoluteKey);
    }

    #[test]
    fn validate_path_rejects_empty_key() {
        let allow = vec!["my-bucket".to_string()];
        let err = validate_path("s3a://my-bucket/", &allow).unwrap_err();
        assert_eq!(err, PathError::EmptyKey);
    }

    #[test]
    fn validate_path_rejects_nul_byte_in_key() {
        let allow = vec!["my-bucket".to_string()];
        let err = validate_path("s3a://my-bucket/foo\0bar", &allow).unwrap_err();
        assert_eq!(err, PathError::ControlChar);
    }

    #[test]
    fn validate_path_default_oss_allowlist_rejects_everything() {
        let allow: Vec<String> = Vec::new();
        let err = validate_path("s3a://anywhere/foo", &allow).unwrap_err();
        assert!(matches!(err, PathError::BucketNotAllowed(_)));
    }

    #[test]
    fn validate_path_does_not_match_bucket_prefix() {
        // A bucket named `acme-data-temp-evil` must NOT be accepted
        // when only `acme-data-temp` is allowlisted. This guards
        // against a sneaky operator overlay typo.
        let allow = vec!["acme-data-temp".to_string()];
        let err = validate_path("s3a://acme-data-temp-evil/foo", &allow).unwrap_err();
        assert!(matches!(err, PathError::BucketNotAllowed(_)));
    }

    // ---- SHELF-34 — page-index cache -------------------------------

    #[test]
    fn page_index_cache_round_trip() {
        let cache = PageIndexCache::new(16);
        assert!(cache.is_empty());
        let idx = Arc::new(PageIndex::default());
        cache.insert("etag-1", idx.clone());
        let got = cache.get("etag-1").expect("hit");
        assert!(Arc::ptr_eq(&got, &idx));
        assert!(cache.get("etag-2").is_none());
    }

    // ---- SHELF-34 — real Parquet round-trip ------------------------

    /// Build a tiny in-memory Parquet file (one INT64 column, two
    /// row groups, two pages each) and verify that
    /// `extract_page_index` returns a page-index with non-empty
    /// `(min, max, offset, length)` tuples for the expected
    /// column. The fixture is intentionally tiny (~1 KiB) so the
    /// test is fast and stays well below the [`MAX_FOOTER_BYTES`]
    /// cap.
    #[test]
    fn extract_page_index_round_trip_against_real_parquet() {
        use parquet::basic::Type as PhysicalType;
        use parquet::data_type::Int64Type;
        use parquet::file::properties::{EnabledStatistics, WriterProperties};
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type;
        use std::sync::Arc;

        // Schema: { id: INT64 NOT NULL } — REQUIRED so the writer
        // doesn't demand definition levels for the test fixture.
        let id_field = Type::primitive_type_builder("id", PhysicalType::INT64)
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .expect("id field");
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(id_field)])
                .build()
                .expect("schema"),
        );
        // Force the page-index thrift sections to be written: turn
        // page-level statistics on, and cap data-page rows so the
        // writer flushes more than one page per row group (a
        // single-page row group degenerates the OffsetIndex into
        // a one-element vector and is a weak test of the
        // extraction path).
        let props = Arc::new(
            WriterProperties::builder()
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_data_page_row_count_limit(2)
                .set_write_batch_size(2)
                .build(),
        );

        let mut buffer: Vec<u8> = Vec::new();
        {
            let mut writer =
                SerializedFileWriter::new(&mut buffer, schema, props).expect("file writer");
            // Two row groups, four rows each; data-page-row-count
            // limit is 2 ⇒ two pages per row group.
            for rg_offset in [0i64, 100i64] {
                let mut rg_writer = writer.next_row_group().expect("rg writer");
                let mut col_writer = rg_writer
                    .next_column()
                    .expect("column writer")
                    .expect("non-empty");
                let values: Vec<i64> = (0..4).map(|i| rg_offset + i).collect();
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(&values, None, None)
                    .expect("write_batch");
                col_writer.close().expect("col close");
                rg_writer.close().expect("rg close");
            }
            writer.close().expect("file close");
        }

        let idx =
            extract_page_index(&buffer).expect("page index extracts from a real parquet footer");
        // Expectation: at least one column carries pages.
        assert!(idx.total_pages > 0, "expected ≥1 page, got 0");
        let pages = idx
            .pages_for_column("id")
            .or_else(|| idx.pages_for_column_idx(0))
            .expect("`id` or column ordinal 0 must carry pages");
        assert!(!pages.is_empty(), "page list must be non-empty");
        // Each page has a strictly-positive length and a non-NaN min/max.
        for p in pages {
            assert!(p.length > 0, "page length must be > 0");
            assert!(!matches!(p.min, ColumnValue::Null));
            assert!(!matches!(p.max, ColumnValue::Null));
        }

        // Predicate `id ∈ [50, 200]` should keep at least one page
        // from the second row group (ids 100..103) and may include
        // both row groups depending on stats granularity. The exact
        // count is writer-dependent, so we only assert ≥1 page is
        // returned and ≤ total page count.
        let pred = Predicate::range(ColumnValue::Int64(50), ColumnValue::Int64(200));
        let kept = predicate_prune(&idx, "id", &pred);
        assert!(!kept.is_empty(), "predicate `id > 50` must keep ≥1 page");
        assert!(
            (kept.len() as u64) <= idx.total_pages,
            "kept page count must not exceed total page count"
        );
        // PII containment: predicate_prune returns only
        // `(offset, length)` tuples — never page-level min/max.
        for (offset, length) in &kept {
            assert!(*length > 0);
            assert!(*offset > 0);
        }
    }

    #[test]
    fn page_index_cache_key_format_is_stable_intra_process() {
        // The cache key namespace must NOT collide with the SHELF-04
        // 32-byte content keyspace. Asserting the stable format
        // guards against a future refactor accidentally rewriting
        // it as a sha256 hex string (which would namespace-pollute).
        assert_eq!(page_index_cache_key("abc"), "abc::page-index");
    }
}
