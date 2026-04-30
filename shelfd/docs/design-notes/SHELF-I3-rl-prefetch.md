# I3 — RL-based prefetch policy (design spike)

**Status:** research spike. Paper, not a v1 shipment.
**Owning ticket:** `i3-rl-prefetch`.
**Related:** `BLUEPRINT §7` honest-residual, ADR-0003 (admission),
Tier 5 gate in the mission plan.

## What Warp Speed does and why we can do better

Warp Speed's warmup is a heuristic: it watches for `SELECT *` on
a table, treats that as a signal to index everything, and hopes
subsequent queries reuse the index. The heuristic is cheap but
has two failure modes we've observed in `trino_logs`:

1. Dashboards that never issue `SELECT *` (most production BI
   tools don't) never trigger warmup.
2. One-shot exploratory `SELECT *` from an analyst at 2am
   triggers a warmup for data that no subsequent query actually
   touches — the index is pure waste.

A learned policy doesn't need a proxy signal: it watches the
jsonPlan fingerprints (E7) directly and prefetches the rowgroups
that the cost model says will be hit within the next window.

## Problem shape

This is a **contextual decision problem**, not a pure bandit:
each prefetch has downstream consequences on cache residency and
pool pressure. PPO is the standard choice; the state/action space
keeps it tractable.

| | Shape |
|---|---|
| State | [last-K jsonPlan fingerprints, pool residency fraction, pool pressure ratio, time-of-day bucket, pending admission queue depth] |
| Action | For an incoming fingerprint, which (file_etag, rg_ordinal) tuples to prefetch |
| Reward | ΔWall-clock attributable to the prefetch = p50(with) − p50(without), measured over a 5-minute window on matched fingerprint pairs |
| Policy | PPO with a Gaussian latent over a factored action space; action head is softmax over candidate rowgroups |

`last-K` is capped at 64 to keep the state vector small; beyond
that, marginal information per slot drops below the noise floor.

## Why this is a paper, not a feature

Four honest reasons:

1. **Reward attribution is noisy.** Observed wall-clock savings
   depend on concurrent query load, network weather, and AWS
   neighbour noise. Low signal-to-noise ratio eats into how fast
   the policy can learn anything that beats a hand-tuned
   heuristic.
2. **Training data is small.** Production `trino_logs` at
   example's scale is ~10⁶ queries/month; even with replay,
   PPO wants more samples than that to converge reliably.
3. **Operational complexity.** Shipping a Rust inference path
   (`ort` for ONNX or `lightgbm-rust` for GBDT) is do-able, but
   the rollback story when the model misbehaves at 3 AM is
   unclear. The LightGBM admission work (Tier 5, ADR-0003) gives
   us the muscle memory for this deploy shape; we should finish
   that before adding a second ML path.
4. **Baseline is strong.** Shelf already prefetches via:
   - D2 HMS-poller pin list refresh (snapshot-aware).
   - D3 page-index + bloom-filter byte-range extraction.
   - B4 `shelf-replay prewarm` on 7-day SplitCompletedEvent.
   - H3 MV auto-pin on HMS events.
   Adding an RL policy on top of this stack needs a ≥ 5 pp
   wall-clock win in replay *after* the above have run, which
   the honest residual in §7 says might not exist.

## Minimum experiment to decide

Before anyone writes Rust:

1. **Offline replay baseline.** Run the F1 replay harness with
   only the current (heuristic) prefetch pipeline. Record every
   cold miss and compute the theoretical upper bound — what
   would p50 have been if every cold miss were a hit?
2. **Oracle policy cap.** Replay again with an oracle that
   prefetches every needed rowgroup one minute before the query.
   That's the performance ceiling.
3. **Gap analysis.** If the gap between heuristic and oracle is
   < 10 % of warm wall-clock, no ML can close it; close I3 as
   unviable.
4. **If gap ≥ 10 %**, train a single-step GBDT on the replay as
   a supervised baseline (rowgroup-level "will be used within 5
   min?"). If GBDT recovers > half the gap, stop there —
   supervised beats RL for this problem at our scale. Only if
   GBDT plateaus does PPO become worth the engineering cost.

## Data and infra

- **Replay source:** `shelf-replay` binary (SHELF-26), already
  consumes `SplitCompletedEvent` byte ranges from trino_logs.
- **Training host:** a single `g5.2xlarge` offline; no streaming
  RL in v1 (too risky).
- **Model format:** ONNX via `tract` or `ort` for inference so
  the shelfd binary stays Rust-only. The E7 telemetry substrate
  (fingerprint + tenant counters) is already on `shelfd`'s
  `/metrics`; pipe it into the state vector directly.
- **Feature flag:** `--rl-prefetch=shadow|enforce|off`. Default
  off; shadow mode logs the policy's recommended actions without
  acting so we can compare against reality before rollout.

## Failure modes to design against up-front

- **Catastrophic forgetting.** Traffic drift (new dbt project
  lands with a different column set) can cause the policy to
  prefetch the wrong things for hours. Mitigation: a cheap
  offline sanity check (compare new policy's Top-N predicted
  rowgroups against the prior policy's; if overlap < 50 %, hold
  rollout for review).
- **Runaway prefetch.** Bug amplifies prefetches, blows the DRAM
  budget, evicts legitimately hot entries. Mitigation: hard cap
  on prefetch bytes/second per shelfd replica, wired *below* the
  policy's action path.
- **Correlated failure.** All replicas run the same model; a bad
  update hits every replica simultaneously. Mitigation: canary
  a single replica (we have 4); rollback key is a single env
  var.

## Exit criteria to ship

- Offline-replay wall-clock improvement ≥ 10 pp vs heuristic.
- Online shadow run for ≥ 2 weeks with ≤ 1 % byte-budget
  overshoot and ≤ 0.5 % false-evict rate.
- Full rollback story documented + tested on staging.

Until all three are met, I3 stays a research note.
