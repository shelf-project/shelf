//! SHELF-33 — W-TinyLFU admission gate.
//!
//! ## Why this exists
//!
//! Iceberg / Trino read patterns include a heavy "one-hit-wonder"
//! tail: manifest scans, predicate pushdown probes, time-travel
//! reads, and dbt-incremental tests each touch a row group exactly
//! once and never again. Today these one-hit-wonder bytes are
//! admitted into Foyer's DRAM tier under the size-threshold policy
//! (ADR-0003, [`crate::admission::SizeThresholdPolicy`]) — they
//! pay full price (DRAM byte + NVMe spill cost on eviction) and
//! return zero hit-ratio.
//!
//! W-TinyLFU is Caffeine's frequency-aware admission filter
//! ([Einziger 2017, arXiv 1512.00727](https://arxiv.org/abs/1512.00727)).
//! It pairs a tiny 4-bit Count-Min Sketch with a Bloom
//! "doorkeeper": the doorkeeper absorbs items being seen for the
//! first time, the sketch tracks frequency for items past the
//! doorkeeper, and admission is gated on `estimated_freq >=
//! admit_threshold`. Caffeine's published benchmarks land within
//! ~1 % of Belady's algorithm on web traces.
//!
//! ## Plan citation
//!
//! This module implements lever 6 in
//! `agents/out/03-plan.md` (algorithmic optimization roadmap)
//! ("W-TinyLFU admission layer in front of Foyer", SHELF-33, P1).
//! Composability with the existing size-threshold policy is the
//! design choice of ADR-0020: W-TinyLFU is the *outer* gate;
//! oversized objects are still rejected by [`SizeThresholdPolicy`]
//! before frequency is consulted, so the 1 GiB cliff from ADR-0003
//! does not regress.
//!
//! ## Hot-path cost
//!
//! Per `decide` call:
//!
//! - one Bloom doorkeeper lookup + set (k=2 hashes, ~10 ns).
//! - `depth` Count-Min Sketch reads (depth=4 atomic loads, ~10 ns).
//! - `depth` 4-bit increments via CAS loop (~25 ns under contention).
//!
//! No allocations on the hot path; all backing storage is
//! pre-allocated `AtomicU64` slabs sized once at construction.
//! Decay is serialised behind a `parking_lot::Mutex` and amortised
//! across `window_size` observations; the typical decay cost is one
//! pass over the sketch every `window_size` admissions, which for
//! `capacity_hint = 1 MiB` means ≈ once per 10 M admits.
//!
//! ## Why CAS, not RwLock
//!
//! The hot path is read-heavy and write-frequent (every admission
//! attempt records the key). A `RwLock` would serialise readers
//! through a single writer when decay runs; CAS on `AtomicU64`
//! cells lets readers proceed without coordination, paying only
//! contended-cell retries. Decay grabs a `parking_lot::Mutex` for
//! the duration of the pass over the sketch; the read path checks
//! the same mutex with `try_lock` and skips the decay-write tick
//! if a peer already holds it (so two callers crossing the window
//! threshold simultaneously don't double-halve).
//!
//! ## What this module does NOT do
//!
//! - **Window cache / main cache split**: Caffeine's W-TinyLFU has a
//!   small "window cache" in front of the main cache to give every
//!   new item a brief warm-up before frequency is consulted. The
//!   plan does not ask for that — Foyer already holds the bytes;
//!   we are only adding an admission gate. Without the window cache
//!   the failure mode is "bursty new keys never admit" rather than
//!   "DRAM polluted by one-hit-wonders"; the doorkeeper alleviates
//!   the first case (any second visit promotes through the
//!   doorkeeper into the sketch). If replay (SHELF-35) shows the
//!   bursty failure mode dominating, we can revisit.
//! - **Cache-hit observation**: the policy is only consulted at
//!   `FoyerStore::get_or_fetch` admission time, which is post-miss.
//!   Cache hits never reach `decide`. This undercounts true
//!   frequency for hot keys but is consistent with TinyLFU's
//!   "admission filter" framing — only candidates for insertion
//!   are recorded. Adding an `observe()` hook on hits is a future
//!   ticket if the replay shows we need it.
//! - **Per-pool sketches**: a single sketch covers both metadata
//!   and rowgroup pools. Caffeine sizes its sketch by capacity;
//!   we size by `capacity_hint`, treating it as the union of both
//!   pools. Splitting per-pool is a future optimisation if cross-
//!   pool key contamination ever shows up in metrics.
//!
//! ## Scope restriction (F3, deep-research 2026-04-30)
//!
//! This policy applies ONLY to the DRAM metadata pool. It MUST NOT
//! be wired on the rowgroup pool while the rowgroup pool runs
//! S3-FIFO eviction. W-TinyLFU's doorkeeper and S3-FIFO's small
//! queue are redundant — both filter one-hit-wonders — so stacking
//! them yields near-zero additional lift but doubles admission-path
//! CPU. The cluster-side cutover MR that swaps
//! `SizeThresholdPolicy::from_config(...)` for
//! `WTinyLfuPolicy::new(...)` in `main.rs` must gate the policy on
//! `AdmissionContext::pool == Pool::Metadata`. The
//! `Pool::RowGroup` unit tests below exercise the algorithm in
//! isolation; they are not production wiring.
//!
//! ## References
//!
//! - [Einziger, Friedman, Manes — TinyLFU: A Highly Efficient Cache
//!   Admission Policy (arXiv 1512.00727)](https://arxiv.org/abs/1512.00727)
//! - [Caffeine TinyLFU implementation](https://github.com/ben-manes/caffeine/wiki/Efficiency)
//! - `agents/out/adr/0003-size-threshold-admission-over-onnx-mlp.md`
//!   — superseded as the *only* admission policy; size threshold
//!   stays as the inner gate.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::admission::{AdmissionContext, AdmissionDecision, AdmissionPolicy};

/// Default Count-Min Sketch depth (number of independent rows).
/// Caffeine uses 4; the FPP is `(e/depth)^depth` ≈ 0.4 % at depth=4
/// which is more than enough to gate cache admission.
pub const DEFAULT_DEPTH: usize = 4;

/// Default frequency threshold to admit. Items must be observed
/// at least this many times in the current window before they
/// pass the gate. `2` matches Caffeine's default (one observation
/// to fill the doorkeeper, one to lift the sketch counter to 1).
pub const DEFAULT_ADMIT_THRESHOLD: u8 = 2;

/// Maximum representable count per cell. 4-bit cells = 0..=15.
const CMS_CELL_MAX: u8 = 15;

/// 4-bit cells per `AtomicU64`. Each `u64` holds 16 cells.
const CELLS_PER_WORD: usize = 16;

/// 4 bits per cell.
const CELL_BITS: u32 = 4;

/// Mask for one 4-bit cell.
const CELL_MASK: u64 = 0xF;

/// Bloom doorkeeper hash count (Kirsch-Mitzenmacher with k=2).
const DOORKEEPER_HASHES: u32 = 2;

/// Configuration for [`WTinyLfuPolicy`].
#[derive(Debug, Clone, PartialEq)]
pub struct WTinyLfuConfig {
    /// Hint at the working-set size in items. Used to size the
    /// sketch and doorkeeper. 1 M items ≈ 1 MiB sketch + 0.25 MiB
    /// doorkeeper at default settings.
    pub capacity_hint: usize,
    /// Number of independent rows in the Count-Min Sketch. 4 is
    /// Caffeine's default and yields ~0.4 % FPP.
    pub depth: usize,
    /// Admit threshold. Items must reach `freq >= admit_threshold`
    /// in the current window before they pass.
    pub admit_threshold: u8,
    /// Window size in observations. After this many observations
    /// the sketch + doorkeeper halve / clear. Caffeine uses ~10 ×
    /// capacity; we take 8 × so the window rolls slightly faster
    /// under bursty traffic.
    pub window_size: u64,
}

impl Default for WTinyLfuConfig {
    fn default() -> Self {
        Self::with_capacity(1_000_000)
    }
}

impl WTinyLfuConfig {
    /// Build a config sized for the given working-set hint.
    pub fn with_capacity(capacity_hint: usize) -> Self {
        let capacity_hint = capacity_hint.max(64);
        Self {
            capacity_hint,
            depth: DEFAULT_DEPTH,
            admit_threshold: DEFAULT_ADMIT_THRESHOLD,
            window_size: capacity_hint.saturating_mul(8) as u64,
        }
    }
}

/// Composition policy: how the W-TinyLFU gate interacts with an
/// inner [`AdmissionPolicy`]. The plan calls W-TinyLFU "in front
/// of Foyer", which the workspace memory clarifies as "outer gate
/// over the existing size-threshold". The default is `AndAfter`
/// — apply size threshold first, then frequency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Composition {
    /// Inner policy decides first. Only `Admit` survivors then go
    /// through the frequency gate. Used when wrapping
    /// [`SizeThresholdPolicy`] so the 1 GiB cliff from ADR-0003 is
    /// preserved.
    AndAfter,
    /// Frequency gate decides first. Used when the inner policy is
    /// the always-admit no-op and we want pure W-TinyLFU.
    AndBefore,
    /// No inner policy — W-TinyLFU alone.
    Standalone,
}

/// W-TinyLFU admission policy.
///
/// `Inner` is the policy applied around the frequency gate per
/// [`Composition`]. Use [`SizeThresholdPolicy`] for the standard
/// production wiring or [`crate::admission::PinList`]-aware policies
/// for richer composition.
///
/// [`SizeThresholdPolicy`]: crate::admission::SizeThresholdPolicy
#[derive(Debug)]
pub struct WTinyLfuPolicy<Inner: AdmissionPolicy> {
    inner: Inner,
    composition: Composition,
    sketch: CountMinSketch4Bit,
    doorkeeper: AtomicBloom,
    admit_threshold: u8,
    window_size: u64,
    sample_counter: AtomicU64,
    decay_lock: Mutex<()>,
}

impl<Inner: AdmissionPolicy> WTinyLfuPolicy<Inner> {
    /// Build a new W-TinyLFU policy wrapping `inner`.
    pub fn new(inner: Inner, composition: Composition, cfg: &WTinyLfuConfig) -> Self {
        let depth = cfg.depth.max(1);
        let width = sketch_width(cfg.capacity_hint, depth);
        let sketch = CountMinSketch4Bit::with_size(width, depth);
        let doorkeeper = AtomicBloom::with_capacity(cfg.capacity_hint);
        Self {
            inner,
            composition,
            sketch,
            doorkeeper,
            admit_threshold: cfg.admit_threshold.min(CMS_CELL_MAX),
            window_size: cfg.window_size.max(1),
            sample_counter: AtomicU64::new(0),
            decay_lock: Mutex::new(()),
        }
    }

    /// Estimated frequency for `key` under the current window.
    /// Public for instrumentation only — the hot path uses the
    /// internal `decide` flow.
    pub fn estimated_frequency(&self, key: &[u8]) -> u8 {
        self.sketch.estimate(key)
    }

    /// Returns whether `key` is in the current-window doorkeeper.
    /// Public for instrumentation / tests.
    pub fn doorkeeper_contains(&self, key: &[u8]) -> bool {
        self.doorkeeper.contains(key)
    }

    /// Number of admissions observed since boot. Wraps on overflow,
    /// which is fine — used only for decay arithmetic.
    pub fn samples_total(&self) -> u64 {
        self.sample_counter.load(Ordering::Relaxed)
    }

    /// Observe a key: bumps the sample counter, sets the
    /// doorkeeper, increments the sketch, and runs decay if the
    /// window threshold is crossed. Returns the post-increment
    /// estimated frequency.
    fn observe(&self, key: &[u8]) -> u8 {
        let prev = self.sample_counter.fetch_add(1, Ordering::Relaxed);
        let crossed = (prev + 1) % self.window_size == 0;

        // Doorkeeper: first-seen items only set bits, no sketch
        // increment. Second visit goes into the sketch.
        let already_seen = self.doorkeeper.test_and_set(key);
        if already_seen {
            self.sketch.increment(key);
        }

        if crossed {
            self.try_decay();
        }

        // For the freshly-set doorkeeper bit, frequency stays 0.
        // For sketch-tracked items, return the estimate.
        if already_seen {
            self.sketch.estimate(key)
        } else {
            0
        }
    }

    /// Halve every cell in the sketch and clear the doorkeeper.
    /// Serialised — at most one decay runs at a time, and a
    /// concurrent caller crossing the threshold while decay is
    /// running silently skips its tick (the next caller will
    /// trigger a fresh decay if the next window is reached).
    fn try_decay(&self) {
        let _guard = match self.decay_lock.try_lock() {
            Some(g) => g,
            None => return,
        };
        self.sketch.halve();
        self.doorkeeper.clear();
        crate::metrics::WTINYLFU_DECAYS_TOTAL
            .with_label_values(&["both"])
            .inc();
    }

    /// Standalone-style decision: returns Admit when frequency >=
    /// threshold, Reject otherwise. Always observes the key first.
    pub fn decide_freq(&self, key: &[u8]) -> AdmissionDecision {
        let freq = self.observe(key);
        if freq >= self.admit_threshold {
            AdmissionDecision::Admit
        } else {
            AdmissionDecision::Reject
        }
    }
}

impl<Inner: AdmissionPolicy> AdmissionPolicy for WTinyLfuPolicy<Inner> {
    fn decide(&self, ctx: &AdmissionContext<'_>) -> AdmissionDecision {
        let key_bytes = ctx.key.as_bytes().as_slice();

        let inner_decision = self.inner.decide(ctx);
        let freq_decision = self.decide_freq(key_bytes);

        // SHELF-24: the pin-list flag has higher precedence than
        // frequency. A pinned key always admits if the inner
        // policy admits, regardless of frequency. This keeps the
        // pin-list workflow (operator-curated hot tables) from
        // being silently overridden by the frequency gate.
        let pinned_admit = ctx.pinned;

        let decision = match self.composition {
            Composition::AndAfter => {
                if inner_decision == AdmissionDecision::Reject {
                    AdmissionDecision::Reject
                } else if pinned_admit {
                    AdmissionDecision::Admit
                } else {
                    freq_decision
                }
            }
            Composition::AndBefore => {
                if pinned_admit {
                    inner_decision
                } else if freq_decision == AdmissionDecision::Reject {
                    AdmissionDecision::Reject
                } else {
                    inner_decision
                }
            }
            Composition::Standalone => {
                if pinned_admit {
                    AdmissionDecision::Admit
                } else {
                    freq_decision
                }
            }
        };

        let outcome_label: &'static str = match (inner_decision, freq_decision, decision) {
            (_, _, AdmissionDecision::Admit) => "admit",
            (AdmissionDecision::Reject, _, AdmissionDecision::Reject) => "reject_inner",
            (AdmissionDecision::Admit, AdmissionDecision::Reject, AdmissionDecision::Reject) => {
                "reject_freq"
            }
            _ => "reject_other",
        };
        crate::metrics::WTINYLFU_DECISIONS_TOTAL
            .with_label_values(&[outcome_label])
            .inc();

        decision
    }
}

// ---------- Count-Min Sketch (4-bit) ----------

/// 4-bit Count-Min Sketch packed into `AtomicU64` slabs.
///
/// `width` is rounded up to the next power of two so the modulo
/// becomes a mask. Each row of `width` 4-bit cells lives in
/// `width / 16` `AtomicU64`s. Increments use a CAS loop that
/// saturates at 15.
#[derive(Debug)]
struct CountMinSketch4Bit {
    rows: Vec<Vec<AtomicU64>>,
    width_mask: u64,
}

impl CountMinSketch4Bit {
    fn with_size(width: usize, depth: usize) -> Self {
        let width = width.next_power_of_two().max(CELLS_PER_WORD);
        let depth = depth.max(1);
        let words = width / CELLS_PER_WORD;
        let rows = (0..depth)
            .map(|_| (0..words).map(|_| AtomicU64::new(0)).collect())
            .collect();
        Self {
            rows,
            width_mask: (width - 1) as u64,
        }
    }

    fn cell_index(&self, key: &[u8], row: usize) -> (usize, u32) {
        let h = hash_for_row(key, row) & self.width_mask;
        let word = (h as usize) / CELLS_PER_WORD;
        let shift = ((h as usize) % CELLS_PER_WORD) as u32 * CELL_BITS;
        (word, shift)
    }

    fn estimate(&self, key: &[u8]) -> u8 {
        let mut min_count: u8 = CMS_CELL_MAX;
        for (row, cells) in self.rows.iter().enumerate() {
            let (word_idx, shift) = self.cell_index(key, row);
            let word = cells[word_idx].load(Ordering::Relaxed);
            let count = ((word >> shift) & CELL_MASK) as u8;
            if count < min_count {
                min_count = count;
            }
        }
        min_count
    }

    fn increment(&self, key: &[u8]) {
        for (row, cells) in self.rows.iter().enumerate() {
            let (word_idx, shift) = self.cell_index(key, row);
            let cell = &cells[word_idx];
            loop {
                let cur = cell.load(Ordering::Relaxed);
                let count = ((cur >> shift) & CELL_MASK) as u8;
                if count >= CMS_CELL_MAX {
                    break;
                }
                let cleared = cur & !(CELL_MASK << shift);
                let next = cleared | ((u64::from(count + 1)) << shift);
                if cell
                    .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }
    }

    /// Conservative halving of every cell. Used at window
    /// roll-over. Each 4-bit cell is independently right-shifted
    /// by 1.
    fn halve(&self) {
        const HALVE_MASK: u64 = 0x7777_7777_7777_7777;
        for row in &self.rows {
            for cell in row {
                loop {
                    let cur = cell.load(Ordering::Relaxed);
                    let next = (cur >> 1) & HALVE_MASK;
                    if cell
                        .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn footprint_bytes(&self) -> usize {
        self.rows.len()
            * self.rows.first().map(|r| r.len()).unwrap_or(0)
            * std::mem::size_of::<u64>()
    }
}

fn sketch_width(capacity_hint: usize, depth: usize) -> usize {
    let depth = depth.max(1);
    let target_cells = capacity_hint.saturating_mul(8); // ~8 cells / item
    let per_row = target_cells / depth;
    per_row.max(64).next_power_of_two()
}

// ---------- Atomic Bloom doorkeeper ----------

/// Concurrent Bloom filter using `AtomicU64` words. `test_and_set`
/// returns whether all bits were already set before the call (i.e.
/// the key was probably already present). `clear` zeroes every
/// word and is called once per window roll-over.
#[derive(Debug)]
struct AtomicBloom {
    bits: Vec<AtomicU64>,
    num_bits: u64,
}

impl AtomicBloom {
    fn with_capacity(capacity_hint: usize) -> Self {
        // Size to ~10 bits / item ⇒ FPP ≈ 1 % at k=2.
        let n = capacity_hint.max(64) as u64;
        let num_bits = (n.saturating_mul(10)).max(64);
        let words = ((num_bits + 63) / 64) as usize;
        let bits = (0..words).map(|_| AtomicU64::new(0)).collect();
        Self { bits, num_bits }
    }

    fn test_and_set(&self, key: &[u8]) -> bool {
        let (h1, h2) = double_hash(key);
        let mut all_set = true;
        for i in 0..DOORKEEPER_HASHES {
            let bit = combined_hash(h1, h2, i) % self.num_bits;
            let word_idx = (bit / 64) as usize;
            let mask = 1u64 << (bit % 64);
            let prev = self.bits[word_idx].fetch_or(mask, Ordering::AcqRel);
            if (prev & mask) == 0 {
                all_set = false;
            }
        }
        all_set
    }

    fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = double_hash(key);
        for i in 0..DOORKEEPER_HASHES {
            let bit = combined_hash(h1, h2, i) % self.num_bits;
            let word_idx = (bit / 64) as usize;
            let mask = 1u64 << (bit % 64);
            if self.bits[word_idx].load(Ordering::Relaxed) & mask == 0 {
                return false;
            }
        }
        true
    }

    fn clear(&self) {
        for w in &self.bits {
            w.store(0, Ordering::Release);
        }
    }

    #[cfg(test)]
    fn footprint_bytes(&self) -> usize {
        self.bits.len() * std::mem::size_of::<u64>()
    }
}

fn double_hash(key: &[u8]) -> (u64, u64) {
    // Same Kirsch-Mitzenmacher pattern as `side_bloom::double_hash`,
    // but operating on a byte slice rather than a generic `Hash`
    // type. Keeps allocator pressure to zero.
    let mut h1 = DefaultHasher::new();
    0xDEAD_BEEF_CAFE_u64.hash(&mut h1);
    key.hash(&mut h1);
    let a = h1.finish();
    let mut h2 = DefaultHasher::new();
    0xF00D_BABE_1337_u64.hash(&mut h2);
    key.hash(&mut h2);
    let b = h2.finish();
    (a, b)
}

fn combined_hash(h1: u64, h2: u64, i: u32) -> u64 {
    // g_i(x) = h1 + i · h2 — same primitive as `side_bloom`.
    h1.wrapping_add((i as u64).wrapping_mul(h2))
}

fn hash_for_row(key: &[u8], row: usize) -> u64 {
    let (h1, h2) = double_hash(key);
    combined_hash(h1, h2, (row + 1) as u32)
}

// ---------- Constructor helper ----------

/// Build a [`WTinyLfuPolicy`] from the existing size-threshold
/// admission config. The frequency-gate sizing comes from
/// [`WTinyLfuConfig::with_capacity`] using a `capacity_hint`
/// approximated from the rowgroup pool's DRAM budget. This is
/// intentionally cheap to compute and stays inside the public
/// API surface for tests.
pub fn from_size_threshold(
    inner: crate::admission::SizeThresholdPolicy,
    capacity_hint_bytes: u64,
    avg_item_bytes: u64,
) -> Arc<WTinyLfuPolicy<crate::admission::SizeThresholdPolicy>> {
    let cap = (capacity_hint_bytes / avg_item_bytes.max(4096)).max(1024) as usize;
    let cfg = WTinyLfuConfig::with_capacity(cap);
    Arc::new(WTinyLfuPolicy::new(inner, Composition::AndAfter, &cfg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::{AdmissionContext, AdmissionDecision, SizeThresholdPolicy};
    use crate::store::{key_from_tuple, Pool};

    fn fresh_policy(threshold: u8, window: u64) -> WTinyLfuPolicy<SizeThresholdPolicy> {
        let inner = SizeThresholdPolicy {
            size_threshold_bytes: 1 << 30,
            pinned_bypass: true,
        };
        let cfg = WTinyLfuConfig {
            capacity_hint: 1024,
            depth: DEFAULT_DEPTH,
            admit_threshold: threshold,
            window_size: window,
        };
        WTinyLfuPolicy::new(inner, Composition::AndAfter, &cfg)
    }

    fn ctx_for(key: &crate::store::Key, size: u64, pinned: bool) -> AdmissionContext<'_> {
        AdmissionContext {
            pool: Pool::RowGroup,
            key,
            size_bytes: size,
            pinned,
        }
    }

    #[test]
    fn rare_item_is_rejected_before_threshold() {
        let policy = fresh_policy(2, 10_000);
        let key = key_from_tuple(b"rare", 0, 1, 0).expect("key");
        let ctx = ctx_for(&key, 1024, false);
        assert_eq!(policy.decide(&ctx), AdmissionDecision::Reject);
    }

    #[test]
    fn frequent_item_admits_after_threshold() {
        let policy = fresh_policy(2, 10_000);
        let key = key_from_tuple(b"hot", 0, 1, 0).expect("key");
        let ctx = ctx_for(&key, 1024, false);
        // Doorkeeper: first observation just sets the bloom bit.
        assert_eq!(policy.decide(&ctx), AdmissionDecision::Reject);
        // Second observation increments sketch to 1 — still below 2.
        assert_eq!(policy.decide(&ctx), AdmissionDecision::Reject);
        // Third observation lifts sketch to 2 — admits.
        assert_eq!(policy.decide(&ctx), AdmissionDecision::Admit);
    }

    #[test]
    fn pinned_bypasses_frequency_gate() {
        let policy = fresh_policy(8, 10_000);
        let key = key_from_tuple(b"pin", 0, 1, 0).expect("key");
        let ctx = ctx_for(&key, 1024, true);
        // First observation, never seen before — sketch says 0 — but
        // pinned bypass forces Admit because inner policy admits.
        assert_eq!(policy.decide(&ctx), AdmissionDecision::Admit);
    }

    #[test]
    fn inner_reject_short_circuits_frequency() {
        // Inner = size-threshold of 32 bytes; payload is 1 KiB.
        let inner = SizeThresholdPolicy {
            size_threshold_bytes: 32,
            pinned_bypass: false,
        };
        let cfg = WTinyLfuConfig::with_capacity(1024);
        let policy = WTinyLfuPolicy::new(inner, Composition::AndAfter, &cfg);
        let key = key_from_tuple(b"big", 0, 1, 0).expect("key");
        let ctx = ctx_for(&key, 1024, false);
        for _ in 0..100 {
            assert_eq!(policy.decide(&ctx), AdmissionDecision::Reject);
        }
        // The frequency gate never had a chance to admit.
    }

    #[test]
    fn standalone_composition_ignores_inner() {
        let inner = NeverAdmit;
        let cfg = WTinyLfuConfig {
            capacity_hint: 256,
            depth: DEFAULT_DEPTH,
            admit_threshold: 2,
            window_size: 10_000,
        };
        let policy = WTinyLfuPolicy::new(inner, Composition::Standalone, &cfg);
        let key = key_from_tuple(b"std", 0, 1, 0).expect("key");
        let ctx = ctx_for(&key, 1024, false);
        for _ in 0..3 {
            policy.decide(&ctx);
        }
        // Standalone: inner Reject is ignored, frequency >= 2 admits.
        assert_eq!(policy.decide(&ctx), AdmissionDecision::Admit);
    }

    #[test]
    fn doorkeeper_clears_on_window_roll_over() {
        let policy = fresh_policy(2, 8);
        let key = key_from_tuple(b"win", 0, 1, 0).expect("key");
        // Push the sample counter past one window.
        for _ in 0..16 {
            policy.observe(b"noise-key");
        }
        // Bloom should have been cleared at least once.
        assert!(!policy.doorkeeper_contains(b"win"));
        // First observation of `win` is now the post-decay first
        // visit, which only sets the doorkeeper bit and returns
        // frequency 0 — i.e. the gate rejects.
        let ctx = ctx_for(&key, 1024, false);
        assert_eq!(policy.decide(&ctx), AdmissionDecision::Reject);
    }

    #[test]
    fn sketch_estimate_saturates_at_max() {
        let policy = fresh_policy(2, 1_000_000);
        let key = b"sat".as_slice();
        // Lift past the doorkeeper.
        policy.observe(key);
        // Now hammer the sketch directly. Should saturate at 15.
        for _ in 0..1024 {
            policy.sketch.increment(key);
        }
        assert_eq!(policy.estimated_frequency(key), CMS_CELL_MAX);
    }

    #[test]
    fn sketch_halve_makes_progress() {
        let policy = fresh_policy(2, 1_000_000);
        // Saturate via repeated direct increments.
        let key = b"halve";
        policy.observe(key);
        for _ in 0..32 {
            policy.sketch.increment(key);
        }
        assert!(policy.estimated_frequency(key) >= 8);
        policy.sketch.halve();
        let post = policy.estimated_frequency(key);
        assert!((4..=8).contains(&post), "halved estimate = {post}");
    }

    #[test]
    fn observe_returns_frequency_after_doorkeeper_promotion() {
        let policy = fresh_policy(2, 10_000);
        let key = b"freq";
        assert_eq!(policy.observe(key), 0); // doorkeeper-set, no sketch yet
        assert_eq!(policy.observe(key), 1); // first sketch increment
        assert_eq!(policy.observe(key), 2); // crosses threshold
    }

    #[test]
    fn capacity_hint_zero_does_not_panic() {
        let cfg = WTinyLfuConfig::with_capacity(0);
        assert!(cfg.capacity_hint >= 64);
        let inner = AlwaysAdmit;
        let _policy = WTinyLfuPolicy::new(inner, Composition::Standalone, &cfg);
    }

    #[test]
    fn footprint_bytes_within_budget() {
        // 1 M items ≈ 1 MiB sketch + 1.25 MiB doorkeeper at default
        // settings (10 bits / item).
        let cfg = WTinyLfuConfig::with_capacity(1_000_000);
        let inner = AlwaysAdmit;
        let policy = WTinyLfuPolicy::new(inner, Composition::Standalone, &cfg);
        let total = policy.sketch.footprint_bytes() + policy.doorkeeper.footprint_bytes();
        // Hard cap: 8 MiB. Production sizing is 4–6 MiB; the slack
        // catches accidental over-allocation regressions.
        assert!(
            total < 8 * 1024 * 1024,
            "W-TinyLFU footprint {} bytes exceeds 8 MiB",
            total
        );
    }

    #[test]
    fn samples_total_increments_monotonically() {
        let policy = fresh_policy(2, 1_000_000);
        assert_eq!(policy.samples_total(), 0);
        policy.observe(b"a");
        policy.observe(b"b");
        assert_eq!(policy.samples_total(), 2);
    }

    #[test]
    fn distinct_keys_do_not_collide_grossly() {
        // 10 000 distinct keys, observe each once, then verify
        // doorkeeper FPP is below 5 % on a fresh probe set.
        let cfg = WTinyLfuConfig::with_capacity(20_000);
        let inner = AlwaysAdmit;
        let policy = WTinyLfuPolicy::new(inner, Composition::Standalone, &cfg);
        for i in 0..10_000u64 {
            policy.observe(format!("hot-{i}").as_bytes());
        }
        let mut fp = 0;
        for i in 0..10_000u64 {
            if policy.doorkeeper_contains(format!("cold-{i}").as_bytes()) {
                fp += 1;
            }
        }
        assert!(fp < 500, "doorkeeper FPP too high: {fp}/10000");
    }

    #[test]
    fn concurrent_observations_do_not_corrupt() {
        use std::sync::Arc;
        use std::thread;
        let policy = Arc::new(fresh_policy(2, 1_000_000));
        let mut handles = vec![];
        for t in 0..8 {
            let p = policy.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1_000u64 {
                    p.observe(format!("k{t}-{i}").as_bytes());
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // 8 threads × 1000 observations = 8000 samples.
        assert_eq!(policy.samples_total(), 8_000);
    }

    // ---- helper inner policies for tests ----

    #[derive(Debug)]
    struct AlwaysAdmit;
    impl AdmissionPolicy for AlwaysAdmit {
        fn decide(&self, _ctx: &AdmissionContext<'_>) -> AdmissionDecision {
            AdmissionDecision::Admit
        }
    }

    #[derive(Debug)]
    struct NeverAdmit;
    impl AdmissionPolicy for NeverAdmit {
        fn decide(&self, _ctx: &AdmissionContext<'_>) -> AdmissionDecision {
            AdmissionDecision::Reject
        }
    }
}
