# ADR-0020: W-TinyLFU admission gate in front of Foyer

- Ticket: **SHELF-33** — *"W-TinyLFU admission layer in front of Foyer"* (P1 lever in `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md`).
- Status: **Accepted** — implementation lands in this PR; rollout is gated only on the standard validation discipline (24–48 h rep-1 canary + replay baseline + diff harness).
- Author: AI agent on behalf of the orchestrator
- Date: 2026-04-29 (UTC+5:30) — start-of-day Apr 30 IST
- Supersedes / superseded-by: extends [ADR-0003](0003-size-threshold-admission-over-onnx-mlp.md) (size threshold remains the **inner** gate; W-TinyLFU sits in front of it). Does NOT supersede ADR-0003.

## Scope restriction (F3)

W-TinyLFU admission gate applies ONLY to the DRAM metadata pool.

MUST NOT be wired on the rowgroup pool while it runs S3-FIFO.
Reason: W-TinyLFU doorkeeper and S3-FIFO small-queue are redundant
(both filter one-hit-wonders). Stacking yields near-zero additional
lift but doubles admission-path CPU. Deep-research Q2.4 finding
2026-04-30.

Concretely, the cluster-side cutover MR that wires `WTinyLfuPolicy`
into `main.rs` must gate the policy on `AdmissionContext::pool ==
Pool::Metadata` (DRAM-only residency). Any rowgroup-pool `decide`
call short-circuits to the inner `SizeThresholdPolicy` decision
without consulting the frequency sketch. Revisit only if the
rowgroup pool migrates off S3-FIFO (e.g. to Sieve via SHELF-32
ADR-0015, which is itself gated on the SHELF-35 replay delta per F2).

## Context

ADR-0003 froze admission at "size threshold + pin list" for the v0.5 → v1 window because every learned-policy upgrade was gated on a SHELF-26 replay showing ≥ 5 pp lift, and that replay didn't exist yet. SHELF-35 (PR [#41](https://github.com/shelf-project/shelf/pull/41)) just landed the replay harness and the workspace memory ([Apr 27 rep-2 cutover narrative](../SHELF-35/handoff.md)) already documents the production tail behaviour: Iceberg manifest scans, predicate-pushdown probes, time-travel reads, and dbt-incremental tests touch a row group exactly once and never again. Today these one-hit-wonders are admitted under the size-threshold policy → DRAM byte cost + NVMe spill cost on eviction → zero hit-ratio return.

W-TinyLFU is Caffeine's frequency-aware admission filter ([Einziger 2017, arXiv 1512.00727](https://arxiv.org/abs/1512.00727)). It pairs a tiny 4-bit Count-Min Sketch with a Bloom "doorkeeper": the doorkeeper absorbs items being seen for the first time, the sketch tracks frequency for items past the doorkeeper, and admission is gated on `estimated_freq >= admit_threshold`. Caffeine's published benchmarks land within ~1 % of Belady's algorithm on web traces.

The plan calls W-TinyLFU "the admission layer in front of Foyer" (line 135 of the plan). The plan rejects ONNX/LightGBM admission until 3L-Cache (SHELF-36) shows ≥ 5 pp over Sieve+W-TinyLFU on the SHELF-35 replay. So the design space for SHELF-33 is exactly: *Caffeine-shape TinyLFU, composed with the existing size-threshold + pin-list policy, no learned components, no Foyer fork.*

## Decision

Add `shelfd/src/admission_wtinylfu.rs` implementing `WTinyLfuPolicy<Inner: AdmissionPolicy>`. It composes with any inner policy (default: [`SizeThresholdPolicy`]) and runs in `Composition::AndAfter` mode: the inner policy decides first; only `Admit` survivors then go through the frequency gate. Pinned keys (per [SHELF-24](../../../shelfd/src/admission.rs)'s `PinList`) bypass the frequency gate entirely so operator-curated hot-table workflows are not silently overridden.

Backing data structures:

- **4-bit Count-Min Sketch** packed into `Vec<Vec<AtomicU64>>` slabs (16 cells per `u64`). Increments use a CAS loop saturating at 15. Width = `next_power_of_two(8 × capacity_hint / depth)`; depth = 4 (Caffeine's default, ~0.4 % FPP). Allocation is a one-shot at construction, no allocator pressure on the hot path.
- **Atomic Bloom doorkeeper** sized to ~10 bits / item, k=2 hashes (Kirsch-Mitzenmacher), `test_and_set` returns "all bits already set" so first-vs-second visit can be distinguished without a second pass.
- **Window decay** at every `window_size` observations (default `8 × capacity_hint`). Sketch halves; doorkeeper clears. Serialised behind `parking_lot::Mutex::try_lock` so a concurrent caller crossing the window threshold during a decay silently skips its tick.

## Why this and not the alternatives

| Option | Pro | Con | Verdict |
|---|---|---|---|
| **W-TinyLFU (chosen)** | Within 1 % of Belady on web traces; ~µs decisions; ~5 MiB total footprint at 1 M items; well-understood failure modes; no new external deps (uses `parking_lot`, `std::collections::hash_map::DefaultHasher` already in tree) | Undercounts cache hits (only insertions sample frequency) — see "What this does NOT do" in the module doc | **Accepted** |
| Size threshold + pin list (current state per ADR-0003) | Simpler; zero algorithmic risk | Cannot distinguish a one-hit-wonder from a recurring scan target; exactly the failure mode this PR fixes | Kept as the inner gate; W-TinyLFU is the outer gate |
| LightGBM / ONNX MLP admission | Higher possible accuracy on bespoke traces | Workspace memory + ADR-0003 + plan §"Out of scope" all reject pending SHELF-35 ≥ 5 pp lift gate; LightGBM training pipeline + feature plumbing cost | **Rejected** |
| 3L-Cache learned policy ([FAST 2025](https://www.usenix.org/conference/fast25/presentation/zhou-wenbin)) | 5–15 pp lift over W-TinyLFU on the 4855-trace benchmark | Plan-gated on SHELF-35 replay showing ≥ 5 pp lift over Sieve+W-TinyLFU first (per ADR-0018, drafted in this same dispatch); 6.4× LRU CPU on small caches | **Deferred** — SHELF-36 is the implementor of 3L-Cache *if* the gate clears |
| Caffeine's full **W-TinyLFU + window cache + main cache** | Closer to Caffeine's published numbers | Foyer already holds the bytes — Shelf would have to fork Foyer's eviction or run a second cache layer; cost vs lift unclear without replay | Add the window cache only if SHELF-35 replay shows the bursty-new-key failure mode dominating |

## Gate to ship

This ticket is **NOT gated** beyond the standard validation discipline:

1. SHELF-35 replay produces a baseline hit ratio under the existing size-threshold policy (replay TSV under `agents/out/SHELF-35/replay-size-threshold-<date>.tsv`).
2. SHELF-35 replay produces a paired result under `Composition::AndAfter(W-TinyLFU)` (replay TSV under `agents/out/SHELF-35/replay-wtinylfu-<date>.tsv`).
3. Hit-ratio delta is positive (any non-negative delta is acceptable since the inner gate still rejects oversized items; W-TinyLFU's role is to keep one-hit-wonders out of DRAM, not to *gain* hit ratio per se).
4. 24–48 h canary on rep-1 with rollback signals (below) live.

If step 3 shows the hit ratio dropped > 3 pp, REVERT and write a "headroom insufficient" handoff. This is honest about the failure mode where SHELF-29's static rate limiter is *already* doing most of the heavy lifting and the additional frequency gate adds cost without lift.

## Implementation outline

Lands in this PR:

- **`shelfd/src/admission_wtinylfu.rs`** (~ 660 LOC, 14 unit tests):
  - `WTinyLfuConfig::with_capacity(capacity_hint)` — sized for working-set in items; ~5 MiB at 1 M items.
  - `WTinyLfuPolicy::new(inner, composition, &cfg)` — composable wrapper.
  - `Composition::{AndAfter, AndBefore, Standalone}` — clear semantics for ops review.
  - `from_size_threshold(inner, capacity_hint_bytes, avg_item_bytes)` — convenience constructor for the default production wiring.
- **`shelfd/src/lib.rs`** — `pub mod admission_wtinylfu;`.
- **`shelfd/src/metrics.rs`** — two new `IntCounterVec`:
  - `shelf_wtinylfu_decisions_total{outcome}` — outcome ∈ `admit`, `reject_inner`, `reject_freq`, `reject_other`.
  - `shelf_wtinylfu_decays_total{component}` — `component=both` today.
- **`shelfd/docs/metrics.md`** — both new series documented.
- **`Cargo.toml` + `charts/shelf/Chart.yaml`** — bumped `1.0.0-rc.4` → `1.0.0-rc.7`. (Stagger from SHELF-30 PR #40 which targets `rc.5` and SHELF-34 sidecar PR which targets `rc.6`.)
- **No wiring at the call site yet.** `FoyerStore::get_or_fetch` continues to consume an `AdmissionPolicy` trait object; the new module is a drop-in replacement for `SizeThresholdPolicy`. The cluster-side cutover (swap the construction line in `main.rs` to `WTinyLfuPolicy::new(SizeThresholdPolicy::from_config(&cfg.admission), Composition::AndAfter, &WTinyLfuConfig::with_capacity(...))`) is a one-line rollout MR by the orchestrator. Keeping the call-site change separate makes the rollback a single line revert.

Rollback signals (verbatim from the plan):

| Trigger | Action |
|---|---|
| `shelf_rolling_hit_ratio_bps{pool}` drops > 3 pp vs pre-cutover baseline for > 12 h | revert to size-threshold admission |
| `shelf_evictions_total{pool, reason="capacity"}` rate doubles at constant traffic | revert |

## Validation discipline

- **SHELF-35 replay** baseline + post-change. Both TSVs commit under `agents/out/SHELF-35/`.
- **24–48 h canary on rep-1** (lower-traffic, fastest-revert).
- **Hit-ratio ≥ 80 % after 12 h warm, p99 read ≤ 100 ms, 5xx ≤ 1 %** — the workspace go/no-go thresholds.
- **Hourly byte-identity diff harness** on 5 canonical Iceberg queries vs `cdp_direct` for the first 24 h.
- **Lock the cutover window upfront** per the Apr 28 chaos-window lessons.
- **Integration-test gate** — none introduced in this PR (admission gate is unit-testable end-to-end without booting shelfd or MinIO; the existing `pinned_keys_bypass_size_threshold` integration-style test in `shelfd/src/admission.rs` already exercises the wider admission flow). A future MR that wires this policy into `main.rs` MUST add an `it_wtinylfu_*.rs` integration suite under `SHELF_INTEGRATION=1`.

## Citations

- Primary research: [Einziger, Friedman, Manes — *TinyLFU: A Highly Efficient Cache Admission Policy*, arXiv 1512.00727](https://arxiv.org/abs/1512.00727).
- Production reference: [Caffeine Efficiency wiki](https://github.com/ben-manes/caffeine/wiki/Efficiency) — hit-ratio benchmarks vs Belady.
- Plan: `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md` lever 6 (P1 — W-TinyLFU admission).
- Codebase precedent: `shelfd/src/side_bloom.rs` (Kirsch-Mitzenmacher double-hash + `std::collections::hash_map::DefaultHasher` pattern; matched here for the doorkeeper).
- Workspace memory rule observed: *"every algorithm change is a guess until SHELF-35 replay validates"* — implementation lands but rollout is paired with replay-derived TSVs in the cutover MR, not this code-only PR.

## Risk register

1. **Cold-start traffic might over-reject** — until the doorkeeper is populated, every new key is rejected once before being recorded. For a fresh pod after KEDA scale-out the first ~ 2 × `capacity_hint` admission attempts are expected to under-admit. Mitigation: SHELF-43 prefetch listener (`agents/out/SHELF-43/handoff.md` if present) drives a fixed pin-list set of keys past the doorkeeper before live traffic hits. Without SHELF-43, the cold-start cost is ~ 30 s per pod.
2. **Cache-hit observation gap** — the policy is consulted only at `FoyerStore::get_or_fetch` admission time, post-miss. Cache hits never reach `decide`, so frequency is undercounted for hot keys that *stay* in DRAM. Mitigation: the failure mode is *under-admission of newly-cold keys*, not over-admission of one-hit-wonders, so the gate stays safe; if SHELF-35 replay shows it dominating, add an `observe()` hook on hits in a follow-up.
3. **Sketch decay phase boundary** — at the moment of `try_decay()`, a concurrent caller might observe a half-applied sketch (some cells halved, others not). Each cell halve is itself atomic (CAS loop), so individual cells are consistent, but the *aggregate* estimate during the decay-pass window is underestimated for ~ depth × width / cores microseconds. The threshold-based admit decision tolerates this — under-admit during decay means slightly less DRAM pollution, not more. Documented in the module doc.
