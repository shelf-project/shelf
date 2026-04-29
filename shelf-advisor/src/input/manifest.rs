//! Iceberg manifest input.
//!
//! Walks the manifest list / manifest files for a given Iceberg
//! table to produce the [`DataFile`] records the recommenders
//! consume. The production path is an `iceberg-rust`-backed walker
//! against the catalog's `metadata.json`; the in-tree implementation
//! shipped with SHELF-53 is the JSON-fixture reader used by the
//! `dry-run` CLI mode and every integration test.
//!
//! Why fixture-first: shipping the production reader requires either
//! pulling `iceberg-rust` into the workspace dep graph or shelling
//! out to `pyiceberg` from the binary. Both decisions belong to a
//! follow-up ticket (the user override on SHELF-53 forbids new
//! heavy deps in this PR). The trait + fixture path lets SHELF-65
//! (MV-aware pinning) and SHELF-52 (bloom-write advisor) land
//! their recommenders in this same crate without waiting on the
//! reader-backend decision.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Subset of an Iceberg manifest's `DataFile` entry that the
/// advisor consumes. Iceberg's wire schema has many more fields
/// (column-level lower / upper bounds, null counts, NaN counts,
/// split offsets, sort-order id, …) — extend this struct as new
/// recommenders learn to use them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    /// Physical path (e.g. `s3://bucket/.../00000-...-data.parquet`).
    pub path: String,

    /// File size in bytes. Drives the `OptimizeRecommender`'s
    /// "small-file ratio" calculation.
    pub file_size_bytes: u64,

    /// Total record count. Useful for distinguishing
    /// "small-and-empty" vs "small-but-dense" files, and for the
    /// per-record cost denominator in future recommenders.
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
    /// List every [`DataFile`] in the current snapshot of `table`,
    /// where `table` is a fully-qualified `catalog.schema.table`.
    fn list_files(&self, table: &str) -> Result<Vec<DataFile>>;
}

/// Implementation of [`IcebergManifestReader`] that serves rows
/// straight from a `{ "<catalog.schema.table>": [DataFile, …] }`
/// JSON map. Used by the `dry-run` CLI mode and by the integration
/// tests.
pub struct FixtureManifestReader {
    by_table: HashMap<String, Vec<DataFile>>,
}

impl FixtureManifestReader {
    pub fn new(by_table: HashMap<String, Vec<DataFile>>) -> Self {
        Self { by_table }
    }

    /// Load the table → files map from a JSON file on disk.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = std::fs::read(path.as_ref())?;
        let by_table: HashMap<String, Vec<DataFile>> = serde_json::from_slice(&bytes)?;
        Ok(Self::new(by_table))
    }

    /// Empty-fixture convenience for tests that don't exercise the
    /// `OptimizeRecommender`.
    pub fn empty() -> Self {
        Self::new(HashMap::new())
    }

    /// Iterate over every `(table, files)` pair the fixture knows
    /// about. `OptimizeRecommender` walks this surface so it can
    /// emit one recommendation per table without first having a
    /// curated list of tables to ask about.
    pub fn iter_tables(&self) -> impl Iterator<Item = (&String, &Vec<DataFile>)> {
        self.by_table.iter()
    }

    /// Number of distinct tables held. Surfaced into the
    /// envelope's `inputs.tables_scanned`.
    pub fn table_count(&self) -> usize {
        self.by_table.len()
    }
}

impl IcebergManifestReader for FixtureManifestReader {
    fn list_files(&self, table: &str) -> Result<Vec<DataFile>> {
        Ok(self.by_table.get(table).cloned().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_returns_empty_for_unknown_table() {
        let r = FixtureManifestReader::empty();
        let files = r.list_files("demo.no.such.table").unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn fixture_returns_known_table_files() {
        let mut m = HashMap::new();
        m.insert(
            "demo.events.purchases".to_string(),
            vec![DataFile {
                path: "s3://example/0.parquet".into(),
                file_size_bytes: 64,
                record_count: 1,
                spec_id: 0,
            }],
        );
        let r = FixtureManifestReader::new(m);
        let files = r.list_files("demo.events.purchases").unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].file_size_bytes, 64);
    }
}
