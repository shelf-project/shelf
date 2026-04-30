# ADR 0015: SHELF-32 Sieve eviction on the rowgroup pool

- Ticket: SHELF-32 — Sieve eviction on rowgroup pool (REQUIRES Foyer bump 0.12.2 → ≥ 0.18; PR #22 to 0.22.3 is the vehicle)
- Status: **Proposed (gated)** — ships only after Dependabot PR [shelf-project/shelf#22](https://github.com/shelf-project/shelf/pull/22) (Foyer 0.12.2 → 0.22.3) merges and post-merge `cargo test -p shelfd --lib` is green.
- Priority: **P2-conditional** (re-tiered from P0 on 2026-04-30 per F2 deep-research finding).
- Author: AI agent on behalf of orchestrator
- Date: 2026-04-30 (UTC+5:30)
- Supersedes: ADR-0009 (foyer-s3-fifo-over-gl-cache-custom) for the rowgroup pool only; the metadata pool inherits no change.
- Superseded-by: none

## Context

The rowgroup pool currently runs plain LRU. S3-FIFO was tried in preview-3
and reverted because the small-queue admission gate trapped everything in
DRAM and NVMe never engaged (workspace memory). The pinned Foyer version
in `Cargo.toml` is **0.12.2**, whose `EvictionConfig` enum exposes only
`Fifo` / `Lru` / `Lfu` / `S3Fifo` (verified at
`foyer-memory-0.12.2/src/cache.rs:251`). **Sieve was added upstream in
Foyer 0.18.0 (2025-07-13)**; therefore Sieve is *not a config flip in the
current tree* — it requires the Foyer bump first.

Sieve's published lift over LRU on production traces (5–15 pp on rowgroup
class workloads, [Sieve, NSDI 2024](https://www.usenix.org/system/files/nsdi24-zhang-yazhuo.pdf)
— up to 63 % lower miss ratio than ARC, 2× the throughput of optimized
LRU on lock-free hits, landed in five third-party libraries in 12–21 LOC)
is the largest hit-ratio lever in the P0 set that touches a single config
line. Plan §"P0 — Eviction policy upgrade" (lines 147–157).

## Decision

After PR #22 merges, set the rowgroup pool's `EvictionConfig` to
`Sieve(SieveConfig::default())` in `shelfd/src/store.rs` (the
`crate::config::EvictionPolicy::Sieve` arm of the existing match in
`build_rowgroup_pool`). The metadata pool stays LRU; SIEVE on metadata
adds noise without lift because Trino's coord-side `MemoryFileSystemCache`
already shadows the warm metadata path (workspace memory + ADR-0012).

## Why this and not the alternatives

| Option | Pro | Con | Why this / not |
|---|---|---|---|
| **Sieve on rowgroup pool (chosen)** | 5–15 pp lift on published traces; 12–21 LOC integration footprint in third-party caches; dodges S3-FIFO's small-queue gate that drove preview-3 revert; FIFO-class data structure → lock-free hit path | Requires Foyer 0.12.2 → ≥ 0.18 bump (PR #22) | Highest-lift / lowest-LOC lever conditional on a Dependabot PR that is already open. |
| Keep S3-FIFO | Already in Foyer 0.12.2; "FIFO is all you need" published precedent | Preview-3 revert evidence: small-queue gate kept everything in DRAM, NVMe never engaged — same workload class | Rejected; we have direct production evidence this admission shape misbehaves on Iceberg row-group reads. |
| Keep LRU | Default since preview-4; stable; zero risk | Sieve papers + every published large-block-cache benchmark show 5–15 pp headroom over LRU; LRU is the floor, not the ceiling | Acceptable status quo, but leaves measurable lift on the table once the bump is in. |
| W-TinyLFU on rowgroup pool (SHELF-33) | Caffeine-class admission gate; complementary to eviction | Different layer (admission, not eviction); already separately tracked | Not exclusive; SHELF-32 + SHELF-33 stack (admission gate in front of Sieve eviction). |
| 3L-Cache learned (SHELF-36) | Best published byte-miss ratio on FAST 2025 traces | 6.4× LRU CPU on small caches; complex; gated on SHELF-35 ≥ 5 pp | Out of scope for SHELF-32; tracked separately under ADR-0018. |

## Gate

Requires SHELF-35 replay showing ≥ 5 pp hit-ratio lift over tuned-S3-FIFO
(SHELF-67) baseline before P0 promotion. F2 finding (deep-research
2026-04-30).

Concretely: the Belady replay harness (SHELF-35) must re-play a 7-day
slice of `cdp.trino_logs.trino_queries` against two simulators —
tuned-S3-FIFO with SHELF-67's `small_queue_capacity_ratio = 10 %` /
`promotion_threshold = 2` on the rowgroup pool, and Sieve with the
`SieveConfig::default()` envelope from Foyer 0.18+. Sieve must win
by ≥ 5 pp *measured* hit ratio on the cached-byte trace for the same
working-set budget, not the published 5–15 pp envelope. If the
measured delta is < 5 pp, Sieve stays at P2-conditional and this ADR
goes back in the P2 backlog; Foyer 0.22.3 still ships on its own
merits (CVE / API surface), but the rowgroup pool stays on tuned
S3-FIFO.

## Gate to ship

PR [shelf-project/shelf#22](https://github.com/shelf-project/shelf/pull/22)
(Foyer 0.12.2 → 0.22.3) is **merged to `main`** AND post-merge
`cargo test -p shelfd --lib` plus `SHELF_INTEGRATION=1 cargo test -p
shelfd --tests` are green on `origin/main`. No additional production
soak gate beyond the standard 24–48 h rep-1 canary defined in
`Validation discipline` below — the published-lift evidence does not
require a per-replica replay number to *open* the PR, only to *roll
out beyond rep-1*.

## Implementation outline

Files modified (target ≤ 60 LOC excluding tests):

- `shelfd/src/store.rs` — extend the `let eviction: EvictionConfig =
  match cfg.eviction_policy { ... }` block in `build_rowgroup_pool`
  (currently lines ~655–660) with a `Sieve =>
  SieveConfig::default().into()` arm. Import `SieveConfig` from `foyer`.
- `shelfd/src/config.rs` — add `Sieve` to the `EvictionPolicy` enum
  (~line 191), keep `default()` at `Lru` so the on-disk shape continues
  to deserialize without the field present (SHELF-E1b precedent),
  extend the existing `rowgroup_eviction_policy_accepts_all_known_variants`
  test to cover `("sieve", EvictionPolicy::Sieve)`.
- `charts/shelf/values.yaml` (and per-env overlays) — opt-in flip
  `cache.pools.rowgroup.evictionPolicy: sieve` in a follow-up MR after
  rep-1 canary clears.
- No changes to admission, metadata pool, peer routing, or shim.

Rollback signals (verbatim from plan lines 154–157):

| Trigger | Action |
|---|---|
| `shelf_rolling_hit_ratio_bps{pool="rowgroup"}` drops > 5 pp vs SHELF-35 baseline for > 12 h | revert to LRU |
| Any p99 regression in `shelf_request_seconds` > 50 % vs baseline for > 10 min | revert |

Revert path is a single `EvictionPolicy` value flip in the per-env
values overlay (no image rebuild).

## Validation discipline

- **SHELF-35 replay**: run the Belady harness with `EvictionPolicy::Sieve`
  vs `EvictionPolicy::Lru` on the same 30-day `cdp.trino_logs.trino_queries`
  trace; freeze tsv to `agents/out/SHELF-35-replay-sieve-<date>.tsv`.
- **24–48 h canary on rep-1**: lower-traffic, fastest-revert; gate at
  hit-ratio ≥ 80 % after 12 h warm, p99 read ≤ 100 ms, 5xx ≤ 1 %
  (workspace rollout convention).
- **Hourly byte-identity diff harness** on 5 canonical Iceberg queries
  vs `cdp_direct` for the first 24 h.
- **Integration-test gate**: `SHELF_INTEGRATION=1 cargo test -p shelfd
  --tests` (without it, integration suites silently exit in 0.00 s
  pretending to pass — SHELF-09 trap).

## Citations

- [Sieve, NSDI 2024 (Zhang et al.)](https://www.usenix.org/system/files/nsdi24-zhang-yazhuo.pdf)
- [Foyer 0.18.0 release notes (2025-07-13)](https://github.com/foyer-rs/foyer/releases/tag/foyer-v0.18.0) — `EvictionConfig::Sieve` added.
- [shelf-project/shelf#22](https://github.com/shelf-project/shelf/pull/22) — Dependabot Foyer 0.12.2 → 0.22.3 bump, the prerequisite vehicle.
- Plan: `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md` lines 147–157 (P0 lever 2), §Verification corrections lines 502–503.
- Workspace memory: "Eviction: rowgroup pool is plain LRU (S3-FIFO was tried in preview-3 and reverted because the small-queue gate kept everything in DRAM, NVMe never engaged)."
- Supersedes: `agents/out/adr/0009-foyer-s3-fifo-over-gl-cache-custom.md` for the NVMe rowgroup pool.

## Risk register

1. **Foyer API stability across 0.12.2 → 0.22.3.** The bump spans 11
   minor versions; `EvictionConfig`, `HybridCache`, `LargeEngineOptions`
   API surfaces have all moved upstream. Mitigation: PR #22 is the
   carrier — it must compile clean on its own merits before SHELF-32
   touches `store.rs`. Treat the post-merge `cargo test -p shelfd --lib`
   green as the minimum gate; rebase SHELF-32 on top of the merged PR.
2. **Preview-3 LRU revert lessons.** S3-FIFO's small-queue gate kept
   everything in DRAM. Sieve's data structure is FIFO-class with a
   single visited-bit; it does not have an analogous gating bug, but
   the canary on rep-1 must explicitly verify `shelf_disk_bytes_used`
   on the rowgroup pool rises within 30 min of warm traffic — a 0
   reading is the same fingerprint as the preview-3 trap.
3. **Replay-validation precondition.** The plan's ≥ 5 pp lift is a
   *replay-derived* number, not a wall-time observation; if SHELF-35's
   tsv shows < 1 pp lift on our trace, do NOT ship — freeze the lever
   and document "headroom insufficient" in the handoff. Sieve's
   published lift is on web-CDN and block-storage traces; analytical
   lakehouse traffic (our case) may be flatter.
