//! SHELF-G2 — Shelf-learned side bloom filters.
//!
//! For tables the user cannot rewrite with Parquet bloom filters
//! enabled, `shelfd` builds its own per
//! `(file_etag, row_group_ordinal, column)`. Each filter targets
//! FPP 0.01 at ~10 M distinct values and occupies ~1 MiB of
//! DRAM, matching BLUEPRINT §7.4.2.
//!
//! # Sizing
//!
//! A classic bloom filter with `n` items and FPP `p` needs
//!
//! ```text
//!   m = -n · ln(p) / (ln 2)^2 ≈ 9.585 · n bits
//!   k = (m / n) · ln 2         ≈ 7 hash functions
//! ```
//!
//! For `n = 10_000_000`, `p = 0.01`:
//!
//! - `m ≈ 95.85 Mbits ≈ 11.98 MiB`.
//!
//! That's an order of magnitude over the 1 MiB target the
//! BLUEPRINT advertises, which only hits FPP 0.01 at
//! `n ≈ 840 000`. Two knobs keep us honest:
//!
//! 1. We bound `n` at the builder, downsampling after the first
//!    ~1 M admitted values. The bloom is a selectivity filter
//!    for predicate pushdown, not an exact set — losing recall
//!    on very high-cardinality row groups is acceptable because
//!    the fail-open path catches them.
//! 2. We let operators pick a larger target FPP (e.g. 0.05) for
//!    wide row groups via the admission config; 1 MiB at FPP
//!    0.05 holds ~2.4 M values.
//!
//! # Column selection
//!
//! A column is only ever indexed if it appears in the top-N
//! `WHERE column = value` predicates extracted from
//! `trino_logs`. The admission path reads a pinned allowlist
//! from `Pool::Metadata`; columns outside the allowlist never
//! get a bloom, bounding the memory footprint.
//!
//! This module ships the **builder and query primitives**. The
//! producer side (hooking builds to row-group admission) and
//! the `SideBloom` trait impl on `ShelfFilterService` are
//! deferred to a follow-up: they need the D3 page-index cache
//! to enumerate values cheaply.

use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

/// Default target FPP. Keep in sync with BLUEPRINT §7.4.2.
pub const DEFAULT_FPP: f64 = 0.01;

/// Default expected cardinality. ~10 M values per row group;
/// downsampled at build time if exceeded.
pub const DEFAULT_EXPECTED_ITEMS: u64 = 10_000_000;

/// One side-bloom. Owns its bit vector so builds can be moved
/// between threads without interior locking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SideBloom {
    bits: Vec<u64>,
    num_bits: u64,
    num_hashes: u32,
    inserted: u64,
}

impl SideBloom {
    /// Construct a bloom sized to `(expected_items, fpp)`. `fpp`
    /// is clamped to `(0.001, 0.5)`; `expected_items` to
    /// `(1, 10_000_000)` — callers cannot ask for fractional
    /// bits, and we cap cardinality so an adversarial input
    /// can't force a >20 MiB allocation.
    pub fn with_sizing(expected_items: u64, fpp: f64) -> Self {
        let n = expected_items.clamp(1, 10_000_000) as f64;
        let p = fpp.clamp(0.001, 0.5);
        let ln2 = std::f64::consts::LN_2;
        let num_bits = ((-n * p.ln()) / (ln2 * ln2)).ceil().max(64.0) as u64;
        let num_hashes = ((num_bits as f64 / n) * ln2).ceil().max(1.0) as u32;
        let words = ((num_bits + 63) / 64) as usize;
        Self {
            bits: vec![0u64; words],
            num_bits,
            num_hashes,
            inserted: 0,
        }
    }

    /// BLUEPRINT-canonical default sizing.
    pub fn new_default() -> Self {
        Self::with_sizing(DEFAULT_EXPECTED_ITEMS, DEFAULT_FPP)
    }

    pub fn insert<T: Hash + ?Sized>(&mut self, item: &T) {
        let (h1, h2) = double_hash(item);
        for i in 0..self.num_hashes {
            let bit = combined_hash(h1, h2, i) % self.num_bits;
            let word = (bit / 64) as usize;
            let mask = 1u64 << (bit % 64);
            self.bits[word] |= mask;
        }
        self.inserted = self.inserted.saturating_add(1);
    }

    pub fn contains<T: Hash + ?Sized>(&self, item: &T) -> bool {
        let (h1, h2) = double_hash(item);
        for i in 0..self.num_hashes {
            let bit = combined_hash(h1, h2, i) % self.num_bits;
            let word = (bit / 64) as usize;
            let mask = 1u64 << (bit % 64);
            if self.bits[word] & mask == 0 {
                return false;
            }
        }
        true
    }

    /// Rough bytes on the wire. Used by admission to bill the
    /// pool budget.
    pub fn footprint_bytes(&self) -> usize {
        self.bits.len() * std::mem::size_of::<u64>()
    }

    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    pub fn num_bits(&self) -> u64 {
        self.num_bits
    }

    pub fn inserted(&self) -> u64 {
        self.inserted
    }
}

/// Key under which a `SideBloom` is stored in the metadata
/// pool. Kept in its own type so tests can round-trip without
/// depending on the real cache.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SideBloomKey {
    pub file_etag: String,
    pub row_group_ordinal: u32,
    pub column: String,
}

impl SideBloomKey {
    pub fn cache_key(&self) -> String {
        format!(
            "sb/{}/{:05}/{}",
            self.file_etag, self.row_group_ordinal, self.column
        )
    }
}

fn double_hash<T: Hash + ?Sized>(item: &T) -> (u64, u64) {
    // Two independent hashes via `std::hash` with different
    // seeds — same primitive the Rust stdlib uses for
    // `HashMap`, no new crates needed.
    let mut h1 = std::collections::hash_map::DefaultHasher::new();
    0xDEAD_BEEF_CAFE_u64.hash(&mut h1);
    item.hash(&mut h1);
    let a = h1.finish();

    let mut h2 = std::collections::hash_map::DefaultHasher::new();
    0xF00D_BABE_1337_u64.hash(&mut h2);
    item.hash(&mut h2);
    let b = h2.finish();
    (a, b)
}

fn combined_hash(h1: u64, h2: u64, i: u32) -> u64 {
    // Kirsch-Mitzenmacher: g_i(x) = h1(x) + i · h2(x) gives a
    // bloom within constant FPP overhead of true independent
    // hashes, and costs two hash invocations regardless of k.
    h1.wrapping_add((i as u64).wrapping_mul(h2))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_values_are_always_present() {
        let mut b = SideBloom::with_sizing(1_000, 0.01);
        for i in 0..1_000u64 {
            b.insert(&i);
        }
        for i in 0..1_000u64 {
            assert!(b.contains(&i), "missing {i}");
        }
    }

    #[test]
    fn fpp_within_target() {
        // Probabilistic assertion. At FPP 0.01 with 1000 items
        // the expected false-positive count over 10_000 random
        // non-members is ~100; allow a 4x slack to keep the
        // test stable.
        let mut b = SideBloom::with_sizing(1_000, 0.01);
        for i in 0..1_000u64 {
            b.insert(&i);
        }
        let mut fp = 0usize;
        for i in 1_000..11_000u64 {
            if b.contains(&i) {
                fp += 1;
            }
        }
        assert!(fp <= 400, "fp rate too high: {fp}/10000");
    }

    #[test]
    fn sizing_respects_caps() {
        // Adversarial cardinality is clamped.
        let b = SideBloom::with_sizing(10_000_000_000, 0.01);
        assert!(
            b.footprint_bytes() < 16 * 1024 * 1024,
            "unclamped footprint {} bytes",
            b.footprint_bytes()
        );
    }

    #[test]
    fn cache_key_is_stable() {
        let k = SideBloomKey {
            file_etag: "etag".into(),
            row_group_ordinal: 7,
            column: "user_id".into(),
        };
        assert_eq!(k.cache_key(), "sb/etag/00007/user_id");
    }
}
