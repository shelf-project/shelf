# Locked-window test template

A locked window is a fixed wall-clock interval during which **no
unplanned events** are permitted on the system under test. Every
window has its PASS/FAIL criteria written down before the window
opens. Any unplanned event during the window invalidates the result —
do not interpret around contamination.

This template covers Stage 2 (90-min A/B re-run), any Stage-2 re-run
needed if the first window invalidates, and any future locked-window
A/B (e.g. SHELF-23 peer-fetch validation, SHELF-24 fallback
validation, picker-on-vs-off comparisons).

## How to use

1. Copy this file to `shelf/docs/rollout-v1/locked-window-<topic>-<date>.md`.
2. Fill in the **Window metadata** and **Pre-window freeze checklist**
   sections **before** T-0. Pin the doc in `#data-platform`.
3. Run the window. Take no actions other than passive observation
   (queries against `cdp.trino_logs.trino_queries`, dashboard reads).
4. After T+end, fill in the **Post-window report** section. Record
   PASS / FAIL verdict with measured numbers, not narrative.
5. If invalidated mid-window: fill in the **Invalidation record**,
   reschedule. Do not re-use the partial data.

---

## Window metadata

| Field | Value |
|---|---|
| Topic | `<e.g. SHELF-22 + picker-OFF clean baseline>` |
| Plan reference | `<plan name + stage>` |
| Start (UTC, IST) | `<2026-MM-DD HH:MM UTC / HH:MM IST>` |
| End (UTC, IST) | `<2026-MM-DD HH:MM UTC / HH:MM IST>` |
| Duration | `<min>` |
| Replicas under test | `<rep-N, rep-M>` |
| Replicas as control | `<rep-X, rep-Y>` |
| Image tag | `<shelfd:0.1.0-preview-N>` |
| Helm revision | `<n; helm -n shelf history shelf>` |
| Trino version | `<480>` |
| Owner | `<oncall handle>` |

## Pre-window freeze checklist

Tick before T-0. Any "no" = window does not open.

| # | Item | Confirmed |
|---|------|-----------|
| F1 | No helm upgrades in `shelf` ns scheduled for the window | [ ] |
| F2 | No helm upgrades in `trino-db` ns scheduled for the window | [ ] |
| F3 | No coord restarts scheduled (no `kubectl rollout restart` planned) | [ ] |
| F4 | No image swaps of shelfd / trino during the window | [ ] |
| F5 | No concurrent MRs that touch values for any rep under test | [ ] |
| F6 | shelfd configmap stable; no `kubectl patch cm` planned | [ ] |
| F7 | No infra maintenance (Karpenter node drain, VPC change) scheduled | [ ] |
| F8 | Stage 0a picker is OFF (or matches the documented picker state for this window) | [ ] |
| F9 | Pin-list pre-warm done (if applicable for this window) | [ ] |
| F10 | Slack `#data-platform` pinned with start/end + freeze notice | [ ] |
| F11 | Calendar invite sent to data-platform + shelf oncalls | [ ] |
| F12 | Baseline numbers (24h-prior same wall-clock window) captured before T-0 | [ ] |

## PASS / FAIL criteria

Write the **exact** thresholds before the window opens. These are the
only measurements that count. Add or remove rows as the window's
purpose requires; the thresholds below are the Stage-2 defaults.

| # | Metric | Threshold | Hard fail |
|---|--------|-----------|-----------|
| C1 | hit-ratio (replicas under test, last 30 min of window) | ≥ 70 % | < 50 % |
| C2 | `hit_disk` p99 latency | ≤ 1 s | > 5 s |
| C3 | `ICEBERG_CANNOT_OPEN_SPLIT` count (replicas under test, full window) | 0 | ≥ 1 |
| C4 | `ICEBERG_INVALID_METADATA` count | ≤ baseline + 10 % | > baseline + 50 % |
| C5 | P95 wall_time (replicas under test) | ≤ 1.2× baseline | > 2× |
| C6 | P99 wall_time (replicas under test) | ≤ 1.2× baseline | > 2× |
| C7 | shelfd 5xx rate | ≤ 1 % | > 5 % |
| C8 | New failure classes vs baseline | 0 | any |

Each criterion needs a query or panel reference recorded next to it
(commit the SQL with the doc, not just a description).

## Invalidation rule

If **any** of the following occur during the window, mark
**INVALIDATED**, fill in the §Invalidation record, and reschedule:

- Helm upgrade against `shelf` or `trino-db`
- Coord or worker restart on a replica under test
- Image swap of any pod under test
- Merge to `cicd-v2` that touches a values file under test
- Manual `kubectl delete pod` against a pod under test
- Infra event impacting the test path (Karpenter drain, NLB flap, AZ
  failure, S3 region throttling)
- Any operator action that breaks the F1–F12 checklist mid-window

Do not "interpret around" contamination. The 04:15-06:00 UTC window
on 2026-04-28 is the cautionary tale — 7 helm revs + 2 coord restarts
+ 1 image swap mid-flight produced a misleading 130x P95 explosion
that v1 of the plan misread as a steady-state shelf characteristic.
A 90-min freeze is cheaper than a wrong conclusion.

---

## Post-window report

Filled in after T+end.

### Verdict

`PASS` / `FAIL` / `INVALIDATED`.

### Measured values

| # | Metric | Threshold | Measured | Verdict |
|---|--------|-----------|----------|---------|
| C1 | hit-ratio | ≥ 70 % | `<x %>` | PASS / FAIL |
| C2 | `hit_disk` p99 | ≤ 1 s | `<x s>` | PASS / FAIL |
| C3 | `ICEBERG_CANNOT_OPEN_SPLIT` | 0 | `<n>` | PASS / FAIL |
| C4 | `ICEBERG_INVALID_METADATA` | ≤ baseline + 10 % | `<n vs baseline m>` | PASS / FAIL |
| C5 | P95 wall_time | ≤ 1.2× baseline | `<x ms vs baseline y ms>` | PASS / FAIL |
| C6 | P99 wall_time | ≤ 1.2× baseline | `<x ms vs baseline y ms>` | PASS / FAIL |
| C7 | shelfd 5xx rate | ≤ 1 % | `<x %>` | PASS / FAIL |
| C8 | New failure classes | 0 | `<list>` | PASS / FAIL |

### Follow-ups

- `<concrete next action 1>`
- `<concrete next action 2>`

### Invalidation record (only if INVALIDATED)

| Field | Value |
|---|---|
| Time of invalidating event (UTC) | `<HH:MM>` |
| Event | `<helm upgrade / restart / image swap / ...>` |
| Owner | `<who triggered>` |
| Cause | `<why>` |
| Reschedule window | `<new start / end>` |

Attach the next locked-window doc (`locked-window-<topic>-<new-date>.md`)
that supersedes this one, and link both directions.
