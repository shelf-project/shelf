//! Iceberg manifest input.
//!
//! Walks the manifest list / manifest files for a given Iceberg
//! table to produce the `DataFile` records the recommenders need.
//! The Phase-1 scaffold defines the trait + value types; the real
//! `iceberg-rust` integration lands under SHELF-53.

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Subset of an Iceberg manifest's `DataFile` entry that the
/// advisor consumes. Iceberg's wire schema has many more fields
/// (column-level lower/upper bounds, null counts, NaN counts,
/// split offsets, sort-order id, …) — we add them as new
/// recommenders learn to use them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    /// Physical path (e.g. `s3://bucket/.../00000-...-data.parquet`).
    pub path: String,

    /// File size in bytes. Drives the `OptimizeRecommender`'s
    /// "small-file ratio" calculation.
    pub file_size_bytes: u64,

    /// Total record count. Useful for distinguishing
    /// "small-and-empty" vs "small-but-dense" files.
    pub record_count: u64,

    /// Partition spec id the file was written under. Out-of-spec
    /// files are a strong signal for an `OPTIMIZE` rewrite.
    pub spec_id: i32,
}

/// Reader contract for an Iceberg table's data files.
///
/// `list_files` is expected to return the *current snapshot's*
/// data files only — the advisor never reasons about historical
/// snapshots. Implementations should honour Iceberg's
/// `current-snapshot-id` rather than picking a snapshot themselves.
pub trait IcebergManifestReader: Send + Sync {
    /// List every `DataFile` in the current snapshot of `table`,
    /// where `table` is a fully-qualified `catalog.schema.table`.
    fn list_files(&self, table: &str) -> Result<Vec<DataFile>>;
}
