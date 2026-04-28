//! Parquet footer metadata extraction for Pool::Metadata pre-pinning
//! (Track D3).
//!
//! When the shim admits a Parquet footer into `Pool::Metadata`, the
//! footer carries the exact byte ranges of each row group's:
//!
//! - **PageIndex** (Parquet 2.9+ `ColumnIndex` + `OffsetIndex` —
//!   min/max/null counts per data page, enabling Trino's predicate
//!   push-down to skip pages without decoding any Parquet at all).
//! - **Bloom filters** — optional per-column filters stored directly
//!   in the file, queried by Trino's `DynamicFilterService`.
//!
//! Extracting those ranges from the footer **at the moment of
//! admission** means subsequent predicate-aware reads find them
//! already in Pool::Metadata; without this step, every query pays a
//! separate GET per (file, row_group, column) just to read a
//! sub-kilobyte PageIndex.
//!
//! ## Scope
//!
//! This module defines the data model and the extractor contract. A
//! full `parquet` crate integration lives behind an opt-in feature
//! flag because pulling in the `parquet` transitive tree adds ~4 MB
//! of compile output and ~60s of CI time; the rollout in
//! `docs/rollout-v1.md` gates on the replay harness showing a
//! material win.
//!
//! Once enabled, the wiring is:
//!
//! ```text
//!   shim GET footer → admission (Pool::Metadata)
//!                               │
//!                               ▼
//!         parquet_meta::extract_footer_ranges(bytes, object_size)
//!                               │
//!                               ├── ColumnChunkRange  (Pool::Metadata)
//!                               ├── OffsetIndexRange  (Pool::Metadata)
//!                               └── BloomFilterRange  (Pool::Metadata)
//!                               │
//!                               ▼ prewarm via /cache/contains batch probe
//!                        shelfd origin fetch (per-range GET)
//! ```
//!
//! The no-op default implementation returns `Ok(Extracted::empty())`
//! so the rest of the codebase compiles and tests against a stable
//! API while the `parquet` feature is off.

use std::fmt;

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

    /// Convenience: total bytes across all extracted ranges. Used
    /// for capacity-planning telemetry — high ratios of
    /// `total_bytes / row_group_count` are a red flag that the
    /// writer emitted too-small row groups.
    pub fn total_bytes(&self) -> u64 {
        self.ranges.iter().map(|r| r.length).sum::<u64>()
            + self.footer.map(|f| f.length).unwrap_or(0)
    }
}

/// Contract for a footer extractor. Phase 1 ships a stub
/// implementation for determinism across feature builds; phase 2
/// ships `ParquetFooterExtractor` behind the `parquet_meta` feature
/// using the upstream `parquet` crate.
pub trait FooterExtractor: Send + Sync + fmt::Debug {
    /// Extract all prefetchable byte ranges from a Parquet footer.
    ///
    /// `footer_bytes` is the Thrift-serialised `FileMetaData`
    /// payload (i.e. the bytes between the file's leading magic and
    /// the trailing length-prefix), not the enclosing file.
    /// `object_size` is the Parquet object's total content length.
    fn extract(&self, footer_bytes: &[u8], object_size: u64) -> Result<Extracted, ExtractError>;
}

/// Errors that the extractor may produce. All variants are
/// observable via the `shelf_footer_extract_failures_total{reason}`
/// metric that the wiring layer emits; nothing here panics.
#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("parquet footer malformed: {0}")]
    Malformed(String),
    #[error("parquet thrift compact decode error: {0}")]
    Thrift(String),
    #[error("feature \"parquet_meta\" is disabled in this build")]
    FeatureDisabled,
}

/// No-op extractor used when the `parquet_meta` feature is off. It
/// returns `Extracted::empty()` for every input so the caller can
/// keep the same call site whether or not the feature is on.
///
/// Track D3 phase 2 will replace this with `ParquetFooterExtractor`
/// backed by the `parquet::file::metadata::ParquetMetaDataReader` —
/// feature-gated behind `parquet_meta` in Cargo.toml.
#[derive(Debug, Default)]
pub struct NoopExtractor;

impl FooterExtractor for NoopExtractor {
    fn extract(&self, _footer_bytes: &[u8], _object_size: u64) -> Result<Extracted, ExtractError> {
        Ok(Extracted::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
