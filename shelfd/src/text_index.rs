//! SHELF-G6 — text-index acceleration scaffolding.
//!
//! BLUEPRINT's Track G gap vs Warp Speed is `LIKE` / prefix /
//! suffix queries on text columns. Warp Speed ships Lucene per
//! row group. The shelf equivalent is either:
//!
//! 1. **Tantivy-backed index in shelfd.** Pure Rust, no JVM.
//!    Matches the rest of shelfd's operational story but adds
//!    ~8 MiB of binary bloat per allowlisted column.
//! 2. **Lucene sidecar.** Runs alongside shelfd, consulted over
//!    loopback HTTP. Keeps shelfd binary unchanged but forces a
//!    second process in the deployment chart.
//!
//! This module ships the **wire-level primitives** so the rest
//! of Track G (the `/textindex/probe` endpoint, the Trino plugin
//! translator) can land without locking us into a choice. The
//! fast-path `contains(pattern)` is stubbed with a simple
//! in-memory `HashMap` of keyword → row groups; the real index
//! lands with ADR-0010 once benchmark evidence picks between the
//! two options.
//!
//! # Pattern surface
//!
//! We commit to three pattern shapes:
//! - `Exact(term)` — case-sensitive equality.
//! - `Prefix(term)` — `LIKE 'term%'`.
//! - `Suffix(term)` — `LIKE '%term'`.
//!
//! Arbitrary `LIKE '%foo%bar%'` is out of scope for v1; the
//! Trino translator falls back to "keep all splits" on patterns
//! outside these three.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Pattern shape the probe accepts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TextPattern {
    Exact { term: String },
    Prefix { term: String },
    Suffix { term: String },
}

/// Per-column stub index. The production index will be
/// Tantivy-backed (ADR-0010); this scaffold exists so the HTTP
/// surface can be exercised from unit tests today.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KeywordIndex {
    /// term → row-group ordinals that contain the term at least
    /// once.
    postings: BTreeMap<String, BTreeSet<u32>>,
}

impl KeywordIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that row group `ordinal` contains `term`.
    pub fn insert(&mut self, term: impl Into<String>, ordinal: u32) {
        self.postings
            .entry(term.into())
            .or_default()
            .insert(ordinal);
    }

    /// Match a pattern against the index. Returns the row groups
    /// that *might* contain a matching value; in the stub
    /// implementation the answer is exact because we store full
    /// terms, but the surface is written as "maybe" so the
    /// Tantivy path can remain a drop-in replacement (n-gram
    /// queries over `Suffix` are approximate in practice).
    pub fn maybe_match(&self, pattern: &TextPattern) -> BTreeSet<u32> {
        match pattern {
            TextPattern::Exact { term } => self.postings.get(term).cloned().unwrap_or_default(),
            TextPattern::Prefix { term } => self
                .postings
                .range(term.clone()..)
                .take_while(|(k, _)| k.starts_with(term))
                .flat_map(|(_, v)| v.iter().copied())
                .collect(),
            TextPattern::Suffix { term } => {
                // Linear scan — fine for the stub, not for
                // production. Tantivy's reverse-n-gram field
                // replaces this when ADR-0010 lands.
                self.postings
                    .iter()
                    .filter(|(k, _)| k.ends_with(term))
                    .flat_map(|(_, v)| v.iter().copied())
                    .collect()
            }
        }
    }

    /// Rough memory footprint. Used by the admission policy
    /// when sizing `Pool::TextIndex`.
    pub fn footprint_bytes(&self) -> usize {
        let mut n = 0usize;
        for (k, v) in &self.postings {
            n += k.len() + v.len() * std::mem::size_of::<u32>();
        }
        n
    }
}

/// HTTP body for `POST /textindex/probe`. Mirrors the request
/// shape the Java Trino plugin will send.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextProbeRequest {
    pub table_fqn: String,
    pub column: String,
    pub pattern: TextPattern,
}

/// HTTP body returned by `POST /textindex/probe`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextProbeResponse {
    pub row_group_ordinals: Vec<u32>,
    /// `true` when shelf has no index for this (table, column);
    /// the caller must fall back to the full scan.
    pub fail_open: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_index() -> KeywordIndex {
        let mut idx = KeywordIndex::new();
        idx.insert("apple", 0);
        idx.insert("applet", 0);
        idx.insert("banana", 1);
        idx.insert("pineapple", 2);
        idx
    }

    #[test]
    fn exact_match_hits_single_row_group() {
        let idx = sample_index();
        let rows = idx.maybe_match(&TextPattern::Exact {
            term: "apple".into(),
        });
        assert_eq!(rows.into_iter().collect::<Vec<_>>(), vec![0u32]);
    }

    #[test]
    fn prefix_matches_multiple_terms() {
        let idx = sample_index();
        let rows = idx.maybe_match(&TextPattern::Prefix { term: "app".into() });
        assert!(rows.contains(&0));
        assert!(!rows.contains(&1));
        assert!(!rows.contains(&2));
    }

    #[test]
    fn suffix_matches_pineapple() {
        let idx = sample_index();
        let rows = idx.maybe_match(&TextPattern::Suffix {
            term: "apple".into(),
        });
        assert!(rows.contains(&0));
        assert!(rows.contains(&2));
    }

    #[test]
    fn empty_index_returns_empty_set() {
        let idx = KeywordIndex::new();
        let rows = idx.maybe_match(&TextPattern::Exact { term: "x".into() });
        assert!(rows.is_empty());
    }

    #[test]
    fn footprint_grows_with_data() {
        let mut idx = KeywordIndex::new();
        let before = idx.footprint_bytes();
        idx.insert("alpha", 0);
        assert!(idx.footprint_bytes() > before);
    }
}
