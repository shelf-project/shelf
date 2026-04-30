# ADR 0018: SHELF-36 3L-Cache learned admission/eviction

- Ticket: SHELF-36 — 3L-Cache learned admission/eviction (gated by SHELF-35 ≥ 5 pp lift over Sieve+W-TinyLFU)
- Status: **Proposed (gated)** — ships only if the SHELF-35 replay tsv shows ≥ 5 pp hit-ratio headroom over Sieve (SHELF-32) + W-TinyLFU (SHELF-33). If lift is < 5 pp, the lever is frozen and documented as "headroom insufficient".
- Author: AI agent on behalf of orchestrator
- Date: 2026-04-30 (UTC+5:30)
- Supersedes: none until the gate clears; would supersede SHELF-33 W-TinyLFU as the primary admission gate (eviction stays Sieve from SHELF-32; 3L-Cache replaces admission, not eviction).
- Superseded-by: none

## Context

[3L-Cache (FAST 2025, Zhou et al.)](https://www.usenix.org/conference/fast25/presentation/zhou-wenbin)
reports best byte-miss ratio across 4855 traces with **60.9 % lower CPU
than HALP** and **94.9 % lower CPU than LRB** — only 6.4× LRU CPU on
small caches, 3.4× on large. OSS implementation at
[optiq-lab/3L-Cache](https://github.com/optiq-lab/3L-Cache). The paper
positions the algorithm specifically for large-block analytical caches
where the working set is much bigger than DRAM — the same shape as the
Shelf rowgroup pool.

Workspace memory locks the upgrade gate at **≥ 5 pp lift over
Sieve+W-TinyLFU**: "the only sanctioned upgrade path is a LightGBM gate
conditional on a SHELF-26 replay showing ≥ 5 pp hit-rate lift over the
size-threshold baseline — do NOT re-propose ONNX as the admission
model". 3L-Cache supersedes LightGBM as the candidate per plan §
Verification corrections (lines 472–479) — same gate value, better
algorithm.

Plan §"P2 — 3L-Cache learned admission/eviction" (lines 250–258)
explicitly conditions this lever on the SHELF-35 replay; the workspace
memory rule against ONNX rejection is preserved (3L-Cache does NOT use
neural-network inference at admission time — it uses lightweight feature
trees, which is why the CPU bound is publishable).

## Decision

When the gate clears, port the 3L-Cache OSS C++ implementation into a
new `shelfd/src/wlearned.rs` module (Rust port) behind a Cargo feature
flag `learned_admission`. Replaces the W-TinyLFU admission gate
(SHELF-33), NOT the Sieve eviction policy (SHELF-32) — eviction stays
Sieve. ONNX/LightGBM are explicitly NOT re-proposed (workspace memory
ADR convention).

3L-Cache's CPU overhead is **measured, not assumed**. The Cargo feature
flag is the rollback path: a build with `--no-default-features` or
without `--features learned_admission` falls back to W-TinyLFU
admission with no runtime config change. The rowgroup pool's eviction
remains Sieve regardless.

## Why this and not the alternatives

| Option | Pro | Con | Why this / not |
|---|---|---|---|
| **3L-Cache learned admission (chosen, gated)** | Best byte-miss ratio across 4855 published traces; 60.9 % lower CPU than HALP, 94.9 % lower than LRB; OSS reference impl | 6.4× LRU CPU on small caches (must measure on our trace before commitment); OSS port quality TBD | Only worth shipping if the SHELF-35 number says so. The gate makes this evidence-driven. |
| Stay on Sieve (SHELF-32) + W-TinyLFU (SHELF-33) | Already in the P0/P1 plan; replay-validated by the time SHELF-36 is even considered; far simpler | Leaves any SHELF-35-measured headroom on the table | Acceptable status quo; this is exactly what we ship if the SHELF-35 lift is < 5 pp. |
| HALP / LRB (deep-learning admission) | High accuracy on traces designed for them | 3L-Cache's FAST 2025 paper reports 60.9 % / 94.9 % lower CPU than these on the SAME traces | Rejected — strictly dominated by 3L-Cache. |
| ONNX MLP admission | High expressivity | 100 µs+ inference per admission decision under load; workspace-memory rule explicitly rejects ONNX as a re-proposal | Rejected per workspace-memory ADR convention. |
| LightGBM admission | Lower CPU than ONNX MLP; the original SHELF-26 candidate | 3L-Cache's published lift on FAST 2025 supersedes LightGBM on the same trace class; same ≥ 5 pp gate value | Documented as the prior candidate; superseded by 3L-Cache. |

## Gate to ship

**SHELF-35 replay tsv shows ≥ 5 pp hit-ratio headroom over Sieve
(SHELF-32) + W-TinyLFU (SHELF-33) on our 30-day
`cdp.trino_logs.trino_queries` trace.** Both the baseline (Sieve +
W-TinyLFU) and the candidate (3L-Cache + Sieve) must run on the same
working-set size, the same DRAM:NVMe ratio, and the same query stream
order. Frozen tsvs at:

- `agents/out/SHELF-35-replay-sieve-wtinylfu-<date>.tsv` (baseline)
- `agents/out/SHELF-35-replay-3l-cache-<date>.tsv` (candidate)

Acceptance: `hit_ratio_3l_cache - hit_ratio_sieve_wtinylfu ≥ 0.05` AND
`bytes_origin_3l_cache / bytes_origin_sieve_wtinylfu ≤ 0.95` (≥ 5 %
origin-byte reduction). If either threshold is missed, **do NOT ship
— document as "headroom insufficient" in the SHELF-35 handoff**, leave
SHELF-32+33 in place. This is the workspace-memory ADR convention
applied verbatim.

## Implementation outline

Files modified (target ≤ 1 200 LOC under feature flag + tests):

- `shelfd/src/wlearned.rs` — new module. Rust port of the optiq-lab
  3L-Cache C++ reference; admission decision returns
  `AdmissionDecision::{Admit, Reject}` plus a per-pool counter
  `shelf_admission_3l_decisions_total{decision}`. Module is gated
  behind Cargo feature `learned_admission`.
- `shelfd/src/admission.rs` — extend the existing admission entry
  point with a `LearnedAdmission` arm that delegates to `wlearned`
  when the feature is on; falls back to W-TinyLFU otherwise.
- `shelfd/src/config.rs` — add `admission.policy: {size_threshold,
  wtinylfu, learned_3l}` (default `wtinylfu` once SHELF-33 ships).
- `shelfd/Cargo.toml` — add `learned_admission` feature gating the
  new module + any heavyweight deps (e.g. `nalgebra` if the port
  needs linear algebra primitives).
- `shelfd/src/metrics.rs` — add `shelf_admission_3l_cpu_seconds_total`
  histogram so the < 5 % CPU-overhead invariant is observable in
  production.
- No changes to S3 shim, peer race, store eviction (stays Sieve), or
  origin path.

Rollback signals (verbatim from plan lines 255–258):

| Trigger | Action |
|---|---|
| `shelf_rolling_hit_ratio_bps{pool}` drops > 3 pp vs SHELF-32+33 baseline for > 12 h | revert |
| 3L-Cache CPU overhead > 5 % vs SHELF-32+33 baseline | revert |

Revert is a single config flip `admission.policy: wtinylfu` — feature
flag stays compiled in for fast re-enable; no image rebuild required
unless the entire feature is being stripped.

## Validation discipline

- **SHELF-35 replay**: required for gate clearance. Both baseline and
  candidate tsvs frozen under `agents/out/`.
- **24–48 h canary on rep-1**: hit-ratio ≥ 80 % after 12 h warm, p99
  read ≤ 100 ms, 5xx ≤ 1 %.
- **CPU-overhead observation**: track
  `histogram_quantile(0.99, rate(shelf_admission_3l_cpu_seconds_total[5m]))`
  on rep-1 for the canary window; > 5 ms p99 admission decision = revert.
- **Hourly byte-identity diff harness** on 5 canonical Iceberg queries
  vs `cdp_direct` for the first 24 h (admission policy changes do not
  affect bytes returned; diff = bug).
- **Integration-test gate**: `SHELF_INTEGRATION=1 cargo test -p shelfd
  --features learned_admission --tests`.
- **Per-replica soak**: rep-1 first; rep-2 only after rep-1 stays
  green for 7 days.

## Citations

- [3L-Cache, FAST 2025 (Zhou et al.)](https://www.usenix.org/conference/fast25/presentation/zhou-wenbin)
- [optiq-lab/3L-Cache OSS implementation](https://github.com/optiq-lab/3L-Cache) — C++ reference port basis.
- Plan: `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md` lines 250–258 (P2 lever 10), §Verification corrections lines 472–474.
- Workspace memory: SHELF-26 gate locked at ≥ 5 pp lift over Sieve+W-TinyLFU; ONNX explicitly rejected; LightGBM as the prior candidate superseded by 3L-Cache.
- Existing ADR-0003 (size-threshold admission over ONNX MLP) — SHELF-36 layers on top; the size-threshold remains as the fail-safe under the feature flag.

## Risk register

1. **6.4× LRU CPU on small caches.** The FAST 2025 paper measures CPU
   on the *full* admission decision; on small caches (working set
   fits in DRAM) 3L-Cache pays the full feature-tree cost on every
   admission. Mitigation: the SHELF-35 replay must include both a
   "small-cache" and "large-cache" sweep; if our production cache
   shape is closer to small than large, the CPU budget can outweigh
   the hit-ratio lift. The < 5 % CPU-overhead rollback signal is
   exactly that backstop.
2. **Feature-flag rollback path is mandatory.** A learned admission
   policy that misfires under a traffic pattern not in the replay
   trace is a production hit-ratio cliff. Mitigation: compiling
   3L-Cache behind `--features learned_admission` keeps the
   binary's W-TinyLFU path intact; the `admission.policy: wtinylfu`
   config flip reverts in seconds without a build.
3. **OSS port quality.** The optiq-lab implementation is research
   code; a Rust port may surface unsoundness that did not matter in
   the C++ academic harness (e.g. unchecked integer overflow on
   sketch counters under multi-week production traffic). Mitigation:
   port + integration tests must include a 24-hour soak on the
   replay harness with `panic = "abort"` to surface any latent
   panics before rep-1 deploy. Treat the OSS port as a *port*, not
   a vendor — own the Rust source under `shelfd/src/wlearned.rs`.
