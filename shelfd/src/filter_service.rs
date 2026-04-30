//! SHELF-G4 — `ShelfFilterService` Rust-native implementation.
//!
//! This module ships the *logic* half of Track G's predicate
//! pushdown: given a probe request, it consults the three
//! available signal sources — Parquet native indexes (G1/D3),
//! Shelf-learned side blooms (G2), and the G3 sort-order tag —
//! and returns the row-group refs that might match.
//!
//! The gRPC transport (see `proto/shelf_filter.proto`) is left
//! behind a future `grpc` feature; for now the same logic is
//! reachable as JSON at `POST /filter/probe`, which is what the
//! G5 Trino plugin will call in the first cut.
//!
//! # Fail-open contract
//!
//! If shelf has no data for the probed `(file, column)` the
//! service returns `ProbeOutcome::FailOpen`. Callers interpret
//! that as "assume every row group matches" and do the full
//! scan. `fail_open` is a first-class signal, not an error.
//!
//! # Latency budget
//!
//! 5 ms per probe on shelfd CPU. We intentionally keep the
//! implementation allocation-light: signal lookups go through
//! `Arc<dyn …>` providers so the same call path works for unit
//! tests (in-memory) and production (Foyer-backed).

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::table_props::TableTag;

/// The JSON body accepted by `POST /filter/probe`. The wire
/// shape mirrors the proto one-for-one so a future gRPC server
/// re-uses the same types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeRequest {
    pub table_fqn: String,
    pub column: String,
    pub predicate: Predicate,
    #[serde(default)]
    pub manifest_files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Predicate {
    Equal {
        value: Vec<u8>,
    },
    Range {
        min_inclusive: Vec<u8>,
        max_inclusive: Vec<u8>,
    },
    InList {
        values: Vec<Vec<u8>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RowGroupRef {
    pub file_etag: String,
    pub row_group_ordinal: u32,
}

/// Outcome of a `ShelfFilterService::probe` call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResponse {
    pub maybe_match: Vec<RowGroupRef>,
    pub fail_open: bool,
}

/// Signal sources the service consults. Each one is `None` in
/// tests that don't exercise that signal; production wires all
/// three.
#[derive(Clone)]
pub struct Signals {
    pub native_index: Option<Arc<dyn NativeIndex>>,
    pub side_bloom: Option<Arc<dyn SideBloom>>,
    pub table_tag: Option<Arc<dyn TableTagProvider>>,
}

impl fmt::Debug for Signals {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signals")
            .field("native_index", &self.native_index.as_ref().map(|_| "dyn"))
            .field("side_bloom", &self.side_bloom.as_ref().map(|_| "dyn"))
            .field("table_tag", &self.table_tag.as_ref().map(|_| "dyn"))
            .finish()
    }
}

/// Row-group min/max signal, backed by the Parquet page index
/// (G1/D3) once the metadata pool has it, or by Iceberg manifest
/// min/max as a fallback.
pub trait NativeIndex: Send + Sync {
    /// Return every row group in `manifest_files` whose min/max
    /// on `column` is compatible with `predicate`. Returning
    /// `None` means "no data for this column/file" — the caller
    /// escalates to the next signal.
    fn maybe_match(
        &self,
        table_fqn: &str,
        column: &str,
        predicate: &Predicate,
        manifest_files: &[String],
    ) -> Option<Vec<RowGroupRef>>;
}

/// G2 side-bloom signal. Returns the row groups whose bloom
/// admits at least one probe value. Only meaningful for `Equal`
/// and `InList`.
pub trait SideBloom: Send + Sync {
    fn maybe_match(
        &self,
        table_fqn: &str,
        column: &str,
        predicate: &Predicate,
        manifest_files: &[String],
    ) -> Option<Vec<RowGroupRef>>;
}

/// G3 table tag. The service uses it to decide whether to short
/// circuit straight to `NativeIndex` (clustered column) or go
/// through `SideBloom` (unclustered, bloom is the main tool).
pub trait TableTagProvider: Send + Sync {
    fn tag(&self, table_fqn: &str) -> Option<TableTag>;
}

/// The service itself — stateless over its signal providers, so
/// tests can instantiate it in one line.
#[derive(Debug, Clone)]
pub struct ShelfFilterService {
    signals: Signals,
}

impl ShelfFilterService {
    pub fn new(signals: Signals) -> Self {
        Self { signals }
    }

    pub fn probe(&self, req: &ProbeRequest) -> ProbeResponse {
        let clustered = self
            .signals
            .table_tag
            .as_ref()
            .and_then(|t| t.tag(&req.table_fqn))
            .map(|tag| tag.clustered_columns.contains(&req.column))
            .unwrap_or(false);

        // Path 1: clustered column — prefer native index (cheap,
        // exact on min/max).
        if clustered {
            if let Some(idx) = self.signals.native_index.as_ref() {
                if let Some(rows) = idx.maybe_match(
                    &req.table_fqn,
                    &req.column,
                    &req.predicate,
                    &req.manifest_files,
                ) {
                    return ProbeResponse {
                        maybe_match: rows,
                        fail_open: false,
                    };
                }
            }
        }

        // Path 2: equality / in-list through G2 side blooms.
        if matches!(
            req.predicate,
            Predicate::Equal { .. } | Predicate::InList { .. }
        ) {
            if let Some(bloom) = self.signals.side_bloom.as_ref() {
                if let Some(rows) = bloom.maybe_match(
                    &req.table_fqn,
                    &req.column,
                    &req.predicate,
                    &req.manifest_files,
                ) {
                    return ProbeResponse {
                        maybe_match: rows,
                        fail_open: false,
                    };
                }
            }
        }

        // Path 3: range predicate but column not clustered; the
        // native index still helps if we have a page-index cache
        // for this file.
        if let Some(idx) = self.signals.native_index.as_ref() {
            if let Some(rows) = idx.maybe_match(
                &req.table_fqn,
                &req.column,
                &req.predicate,
                &req.manifest_files,
            ) {
                return ProbeResponse {
                    maybe_match: rows,
                    fail_open: false,
                };
            }
        }

        // Nothing to say — fail open.
        ProbeResponse {
            maybe_match: Vec::new(),
            fail_open: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    use super::*;
    use crate::table_props::TableTag;

    /// Simple provider that returns a fixed set of row groups
    /// and records the number of times it was called. Good
    /// enough to assert routing logic between the three paths.
    #[derive(Default)]
    struct FixedIndex {
        rows: Vec<RowGroupRef>,
        calls: Mutex<usize>,
    }

    impl NativeIndex for FixedIndex {
        fn maybe_match(
            &self,
            _table: &str,
            _col: &str,
            _pred: &Predicate,
            _manifests: &[String],
        ) -> Option<Vec<RowGroupRef>> {
            *self.calls.lock().unwrap() += 1;
            Some(self.rows.clone())
        }
    }

    #[derive(Default)]
    struct NoSignal;
    impl NativeIndex for NoSignal {
        fn maybe_match(
            &self,
            _: &str,
            _: &str,
            _: &Predicate,
            _: &[String],
        ) -> Option<Vec<RowGroupRef>> {
            None
        }
    }
    impl SideBloom for NoSignal {
        fn maybe_match(
            &self,
            _: &str,
            _: &str,
            _: &Predicate,
            _: &[String],
        ) -> Option<Vec<RowGroupRef>> {
            None
        }
    }

    struct FixedTag(TableTag);
    impl TableTagProvider for FixedTag {
        fn tag(&self, _: &str) -> Option<TableTag> {
            Some(self.0.clone())
        }
    }

    fn req(col: &str, pred: Predicate) -> ProbeRequest {
        ProbeRequest {
            table_fqn: "iceberg.analytics.events".into(),
            column: col.into(),
            predicate: pred,
            manifest_files: vec![],
        }
    }

    #[test]
    fn fails_open_when_no_signals() {
        let svc = ShelfFilterService::new(Signals {
            native_index: None,
            side_bloom: None,
            table_tag: None,
        });
        let out = svc.probe(&req(
            "user_id",
            Predicate::Equal {
                value: b"42".to_vec(),
            },
        ));
        assert!(out.fail_open);
        assert!(out.maybe_match.is_empty());
    }

    #[test]
    fn clustered_column_hits_native_index_first() {
        let idx = Arc::new(FixedIndex {
            rows: vec![RowGroupRef {
                file_etag: "etag".into(),
                row_group_ordinal: 0,
            }],
            calls: Mutex::new(0),
        });
        let mut cols = BTreeSet::new();
        cols.insert("user_id".into());
        let svc = ShelfFilterService::new(Signals {
            native_index: Some(idx.clone()),
            side_bloom: Some(Arc::new(NoSignal)),
            table_tag: Some(Arc::new(FixedTag(TableTag {
                clustered_columns: cols,
                has_z_order: false,
                hash_distributed: false,
            }))),
        });
        let out = svc.probe(&req(
            "user_id",
            Predicate::Equal {
                value: b"42".to_vec(),
            },
        ));
        assert!(!out.fail_open);
        assert_eq!(out.maybe_match.len(), 1);
        assert_eq!(*idx.calls.lock().unwrap(), 1);
    }

    #[test]
    fn unclustered_equality_escalates_to_bloom() {
        struct BloomHit;
        impl SideBloom for BloomHit {
            fn maybe_match(
                &self,
                _: &str,
                _: &str,
                _: &Predicate,
                _: &[String],
            ) -> Option<Vec<RowGroupRef>> {
                Some(vec![RowGroupRef {
                    file_etag: "etag".into(),
                    row_group_ordinal: 7,
                }])
            }
        }
        let svc = ShelfFilterService::new(Signals {
            native_index: Some(Arc::new(NoSignal)),
            side_bloom: Some(Arc::new(BloomHit)),
            table_tag: None,
        });
        let out = svc.probe(&req(
            "raw_blob",
            Predicate::Equal {
                value: b"x".to_vec(),
            },
        ));
        assert!(!out.fail_open);
        assert_eq!(out.maybe_match[0].row_group_ordinal, 7);
    }

    #[test]
    fn range_predicate_skips_bloom_path() {
        // Bloom is useless on ranges; service should go straight
        // through to native index.
        let idx = Arc::new(FixedIndex::default());
        let svc = ShelfFilterService::new(Signals {
            native_index: Some(idx.clone()),
            side_bloom: Some(Arc::new(NoSignal)),
            table_tag: None,
        });
        let out = svc.probe(&req(
            "event_ts",
            Predicate::Range {
                min_inclusive: b"a".to_vec(),
                max_inclusive: b"z".to_vec(),
            },
        ));
        assert!(!out.fail_open);
        assert_eq!(*idx.calls.lock().unwrap(), 1);
    }
}
