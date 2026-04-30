# ADR 0016: SHELF-31 Vegas / AIMD adaptive concurrency replacing the static SHELF-29 limiter

- Ticket: SHELF-31 — replace the SHELF-29 static rate limiter with Vegas/AIMD adaptive concurrency
- Status: **Proposed (gated)** — ships only after ≥ 7 days of clean SHELF-29 soak data PLUS a SHELF-35 replay tsv quantifying the static-vs-Vegas gap.
- Author: AI agent on behalf of orchestrator
- Date: 2026-04-30 (UTC+5:30)
- Supersedes: none yet (would supersede SHELF-29's static-rate token bucket inside `shelfd/src/admission_limiter.rs` once shipped).
- Superseded-by: none

## Context

SHELF-29 (the static-rate, independent-queue token bucket at
`shelfd/src/admission_limiter.rs`, shipped via [shelf-project/shelf#39](https://github.com/shelf-project/shelf/pull/39)
on 2026-04-29 ~21:41 IST) bounds admission with operator-tuned
`refill_bytes_per_sec` + `max_burst_bytes` knobs and exposes
`shelf_lodc_drops_total{reason="rate_limit"}` for back-pressure
attribution. It is a band-aid by design — workspace-memory line: "the
chronic `[lodc] submit queue overflow, new entry ignored` floods +
OOMKill chain are the band-aid case the limiter targets". The static
shape requires per-replica retuning when traffic shape changes (rep-0
vs rep-1 vs rep-2 carry materially different loads — workspace rollout
convention) and cannot reclaim concurrency under low load.

[Vegas (Brakmo & Peterson, 1995)](https://sites.cs.ucsb.edu/~almeroth/classes/W04.290F/vegas.pdf)
estimates queue depth from `(observed_latency / min_latency)` and ramps
concurrency up under low load, down on latency degradation —
mathematically the same Little's-Law shape as a hybrid-cache write
queue with non-trivial NVMe latency. Netflix's
[concurrency-limits](https://github.com/Netflix/concurrency-limits)
library is the reference implementation; Rust ports include the
`tower-acc` and `congestion-limiter` crates. AIMD is the simpler
fallback if Vegas oscillates under bursty NVMe-write contention.

Plan §"P1 — Adaptive concurrency" (lines 189–200) demotes this lever
from P0 to P1-conditional explicitly because *replacing 2-hour-old
production code is exactly the kind of thrash workspace memory warns
against*.

## Decision

When the gate clears, replace `shelfd/src/admission_limiter.rs`'s fixed
`refill_bytes_per_sec` / `max_burst_bytes` knobs with a Vegas controller
that adjusts the in-flight byte ceiling from observed
`shelf_origin_request_seconds` p50 + p99. AIMD becomes the simpler
fallback if Vegas oscillates beyond a documented threshold (concurrency
window swings > ±25 % within a 60 s window). The static-rate path
remains compiled in behind a config flag `admission_limiter.mode:
{static, aimd, vegas}` so a one-line revert is possible without an
image rollback.

## Why this and not the alternatives

| Option | Pro | Con | Why this / not |
|---|---|---|---|
| **Vegas adaptive (chosen, gated)** | Provably reclaims concurrency under low load and shrinks under latency rise; mature literature (30-year track record); Rust crate `congestion-limiter` ports the Netflix algorithm | Oscillation risk under bursty NVMe writes; 2-hour-old static-rate code is the floor it has to beat in a SHELF-35 replay | Highest payoff once SHELF-29 hits its static-rate ceiling (visible as `rate_limit` drops sustained against constant traffic). |
| AIMD (simpler fallback) | Trivial Rust impl; survives oscillation that Vegas falls into; no min-RTT estimation needed | Slower convergence than Vegas under non-bursty load | Documented as the in-tree fallback; the same `admission_limiter.mode` config slot. |
| Keep static SHELF-29 | Stable; in production; eliminated the OOMKill class within 90 min of smoke | Fragile per-replica retuning; cannot reclaim concurrency under low load; chronic `submit queue overflow` flood resurfaces the moment load steps up | Acceptable status quo while the gate is open. |
| TCP-Cubic / BBR-class | High-throughput precedent at internet scale | Massive over-engineering for a per-pod in-process bucket; tuning surface enormous | Rejected. |
| Manual operator-tuned cap (current) | Zero code | Workspace-memory rule against "1 size fits all" sizing across replicas | This is exactly what SHELF-31 replaces. |

## Gate to ship

1. **≥ 7 days of clean SHELF-29 soak**: no spikes in
   `rate(shelf_lodc_drops_total{reason="rate_limit"}[5m])` above the
   steady-state baseline established in the SHELF-29 ship handoff;
   zero OOMKills on any shelf pod over the window.
2. **SHELF-35 replay quantified gap**: tsv at
   `agents/out/SHELF-35-replay-vegas-vs-static-<date>.tsv` shows the
   static path leaving ≥ 5 % of admissible bytes unadmitted at peak
   load OR the Vegas path lifting p99 hit_disk by ≥ 100 ms vs static.
   If the gap is < 5 %, document as "headroom insufficient" and
   freeze the lever — the workspace-memory rule against thrashing
   2-hour-old code dominates.

The gate is conjunctive: both clauses must hold. If only soak is clean
but the replay shows no headroom, do NOT ship.

## Implementation outline

Files modified (target ≤ 250 LOC + tests):

- `shelfd/src/admission_limiter.rs` — add `enum LimiterMode { Static,
  Aimd, Vegas }`, factor the existing static path behind
  `LimiterMode::Static`, add a `VegasController` struct holding
  `min_rtt_us: u64`, `target_queue_depth: f64`, `concurrency_window:
  AtomicU64`, with a periodic update tick driven by the same probe
  point that already updates `shelf_lodc_admit_tokens_available`.
- `shelfd/src/config.rs` — add `admission_limiter.mode` (default
  `static` so an upgrade is opt-in), keep all existing static-rate
  knobs alive for fallback.
- `shelfd/src/metrics.rs` — add `shelf_admission_limiter_concurrency_window`
  (gauge) and `shelf_admission_limiter_mode` (info-style gauge with
  `mode` label) for ops visibility.
- New tests: `vegas_controller_ramps_up_under_low_load`,
  `vegas_controller_shrinks_on_latency_rise`,
  `aimd_fallback_is_stable_under_oscillation`.
- No changes to S3 shim, peer race, store, or admission policy.

Rollback signals (verbatim from plan lines 196–200):

| Trigger | Action |
|---|---|
| `rate(shelf_lodc_drops_total{reason="rate_limit"}[5m])` > 2 × steady-state baseline for > 10 min | revert to static SHELF-29 |
| `histogram_quantile(0.99, rate(shelf_request_seconds_bucket{outcome="hit_disk"}[5m]))` > 5 s for > 5 min | revert |
| Any pod RSS > 24 GiB sustained > 5 min | revert |

Revert is a single config flip `admission_limiter.mode: static`; no
image rebuild required.

## Validation discipline

- **SHELF-35 replay**: required for gate clearance — produce tsv at
  `agents/out/SHELF-35-replay-vegas-vs-static-<date>.tsv` covering
  static / AIMD / Vegas on the same 30-day trace.
- **24–48 h canary on rep-1**: hit-ratio ≥ 80 % after 12 h warm, p99
  read ≤ 100 ms, 5xx ≤ 1 %.
- **Hourly byte-identity diff harness** on 5 canonical Iceberg queries
  vs `cdp_direct` for the first 24 h (admission limiter changes do
  not affect bytes returned, so any diff is a code-bug signal).
- **Integration-test gate**: `SHELF_INTEGRATION=1 cargo test -p shelfd
  --tests admission_limiter` with the new Vegas/AIMD test cases.
- **Per-replica soak**: rep-1 first; rep-2 only after rep-1 stays
  green for 7 days (workspace rollout convention).

## Citations

- [Vegas: Brakmo & Peterson, "TCP Vegas: New Techniques for Congestion Detection and Avoidance" (1994)](https://sites.cs.ucsb.edu/~almeroth/classes/W04.290F/vegas.pdf)
- [Netflix concurrency-limits](https://github.com/Netflix/concurrency-limits) — reference Java implementation.
- [`congestion-limiter` Rust crate](https://docs.rs/congestion-limiter) and [`tower-acc`](https://docs.rs/tower-acc) — Rust ports of the Netflix algorithm.
- Plan: `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md` lines 189–200 (P1 lever 5), §Verification corrections lines 478–479 (Vegas applies to write queues — Little's Law framing).
- Workspace memory: "the chronic `[lodc] submit queue overflow, new entry ignored` floods + OOMKill chain are the band-aid case the limiter targets" — SHELF-31 is the principled-controller successor to that band-aid.
- Workspace memory rule against thrashing 2-hour-old production code; encoded as the conjunctive gate above.

## Risk register

1. **Vegas oscillation under bursty NVMe-write contention.** Vegas
   tends to oscillate when the queue's service-time distribution is
   bimodal (DRAM-hit fast path vs NVMe-write slow path). Mitigation:
   bound the concurrency window's per-tick swing to ±15 % and fall
   back to AIMD if 60 s peak-to-peak swing exceeds 25 % twice in a
   10-min window. This fall-back is wired in-process, not a manual
   ops action.
2. **Replacing 2-hour-old code is exactly the kind of thrash the
   workspace-memory rule warns against.** Mitigation: the conjunctive
   gate (≥ 7 d clean soak + SHELF-35 quantified gap) makes this lever
   evidence-driven, not speculative. If the SHELF-35 tsv shows < 5 %
   headroom, the lever is frozen and SHELF-29 is left in place.
3. **AIMD as the simpler-fallback to Vegas.** AIMD is intentionally
   in scope so the lever is not "Vegas or revert"; under sustained
   oscillation the in-process flag flips to AIMD without an image
   rebuild. This bounds the worst-case complexity exposure to "AIMD
   over a static cap", which is well-understood Linux TCP territory.
