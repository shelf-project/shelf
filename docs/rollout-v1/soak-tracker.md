# 14-day post-rollout soak — v0.5 criteria tracker

**Start**: T+0 = moment rep-3 is declared green at its T+24h checkpoint.
**End**: T+14 days.
**Owner**: shelf-oncall (daily eval) · shelf-core (weekly sign-off).
**Status**: begins once rep-3 cutover completes; this file is the live
operational record.

## Purpose

The compressed-canary plan traded the v0.5 gate's 7-day observation window
for a 14-day **post-rollout** soak on cumulative four-replica traffic. This
tracker is what converts the trade-off from "we hope it's fine" into "we
have written evidence, checked daily". If any v0.5 criterion fails for a
24 h window during the soak, we **pre-commit to rolling one replica back**
to S3-direct and root-causing before re-adding (plan §6).

## Daily table (oncall fills in)

Legend: ✅ green · 🟡 yellow (within 10 % of threshold, watch) · ❌ red (rollback-eligible).

| day | date | hit rate (target ≥ 71 %) | `GOLD_DBT` ok (≥ 99.9 %) | p95 latency (≤ 120 % Alluxio baseline) | shelf pages (= 0) | correctness diff (= 0 rows) | pod-roll-friendliness | overall | oncall |
| --- | ---- | ------------------------ | ------------------------- | -------------------------------------- | ----------------- | --------------------------- | --------------------- | ------- | ------ |
| 1   |      |                          |                           |                                        |                   |                             |                       |         |        |
| 2   |      |                          |                           |                                        |                   |                             |                       |         |        |
| 3   |      |                          |                           |                                        |                   |                             |                       |         |        |
| 4   |      |                          |                           |                                        |                   |                             |                       |         |        |
| 5   |      |                          |                           |                                        |                   |                             |                       |         |        |
| 6   |      |                          |                           |                                        |                   |                             |                       |         |        |
| 7   |      |                          |                           |                                        |                   |                             |                       |         |        |
| 8   |      |                          |                           |                                        |                   |                             |                       |         |        |
| 9   |      |                          |                           |                                        |                   |                             |                       |         |        |
| 10  |      |                          |                           |                                        |                   |                             |                       |         |        |
| 11  |      |                          |                           |                                        |                   |                             |                       |         |        |
| 12  |      |                          |                           |                                        |                   |                             |                       |         |        |
| 13  |      |                          |                           |                                        |                   |                             |                       |         |        |
| 14  |      |                          |                           |                                        |                   |                             |                       |         |        |

**Pod-roll-friendliness column**: once per week, roll one shelfd pod during
business hours; the column captures whether cumulative hit rate stays
≥ 80 % of Alluxio baseline during the 10-min roll. This substitutes for
the weekly chaos drill that `runbook.md` §2 required under the original
v0.5 gate.

## Slow-growing pathologies to watch

The 14-day soak exists specifically to catch things a 48 h canary cannot:

1. **NVMe fragmentation** — watch `shelf_nvme_used_bytes` vs
   `shelf_nvme_logical_bytes`; if the ratio drifts > 1.3× over the soak,
   Foyer has a fragmentation problem (reclaim + compact should fix, but
   needs measurement).
2. **Pinlist decay** — `shelf_pinned_bytes` trending downward week-over-week
   means the pin-list loader is no longer getting fresh inputs.
3. **Working-set drift** — `sum(rate(shelf_miss_bytes_total[7d]))` should
   be roughly flat. If it's climbing, the working set is outpacing the
   NVMe size that the capacity plan reserved — this is the signal that
   drives the "bump `storage.size` to 640 GiB" decision.
4. **Replica asymmetry** — per-replica hit ratio (dashboard `$replica=All`
   with split-by) should stay within 10 pp across the four replicas. A
   single replica falling behind means its prewarm didn't take or its
   workload drifted.

## Rollback protocol during soak

If any row ends red:

1. Identify the **smallest** red-contributing replica via the `$replica`
   split on each panel.
2. Revert that replica's `iceberg.properties` PR (one-line revert).
3. Roll that replica's Trino pods.
4. Enter a **72h freeze** on further shelf changes.
5. Root-cause and document in an ADR (`shelf/agents/out/adr/00XX-soak-day-N-incident.md`).
6. Re-canary that replica before re-adding to the four-replica soak; reset the
   soak counter to day 0 on re-add.

## End-of-soak gate

If days 1-14 are all green (no red rows): proceed to
[v0.5-promote.md](v0.5-promote.md) and tag `v0.5`.

If any day was red and recovered: the soak counter reset; extend to whichever
later day reaches 14 consecutive green days.

## References

- v0.5 criteria source of truth: [runbook.md §1](../runbook.md).
- Alluxio baseline (the 71 % / latency target): E12 output in
  `shelf/agents/out/experiments/E12-alluxio-baseline.md`.
- Pre-commitment rationale: plan §6.
