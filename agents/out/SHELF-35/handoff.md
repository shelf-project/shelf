# SHELF-35 hand-off — Belady oracle replay harness

| Field                          | Value                                          |
|--------------------------------|------------------------------------------------|
| Ticket                         | SHELF-35                                       |
| Plan                           | `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md` § P0 lever 3 |
| Branch                         | `shelf-35-belady-replay`                       |
| Worktree                       | `/private/tmp/shelf-35-replay`                 |
| Off-branch base                | `origin/main` @ `1aeb17d`                      |
| ADR                            | n/a (SHELF-35 is **not** in {30, 32, 33, 36, 37, 38, 46, 47}; per-ticket template exempts it from the public ADR rule) |
| Cargo workspace version        | unchanged (Python-only ticket — no shelfd touch) |
| Helm chart version             | unchanged                                      |
| `python -m unittest -v tools.replay.tests` | **14 tests, all passing** in 0.011 s |
| Synthetic CLI smoke (5000 q, 80 tables, 3 capacities × 4 policies) | 12 TSV rows produced, Belady strictly dominates LRU/FIFO/S3-FIFO at every capacity below saturation; all policies converge at saturation (working set fits). Output: `/tmp/shelf35-smoke.tsv` |
| Open follow-ups                | • SHELF-35b: file-level granularity by joining the trace against Iceberg `$files` snapshots — current v1 is `(query, table)` because `cdp.trino_logs.trino_queries.inputs_json` doesn't record per-split paths (Trino removed `SplitCompletedEvent` in PR #26436, ADR-0005).<br>• SHELF-35c: Sieve / W-TinyLFU / 3L-Cache policies (each warrants its own ADR — explicitly out of scope for v1).<br>• SHELF-35d: per-query latency model joining the trace with `shelf_request_seconds` histograms.<br>• SHELF-35e: 4-pod ring simulation with HRW + peer-fetch race (SHELF-23 effects).<br>• Operator action: run `tools/replay/sql/extract_trace_30d.sql` against rep-3, export the CSV, replay against rep-1 + rep-2 capacity sweeps, file the resulting TSVs under `agents/out/SHELF-35/replay-<algo>-<date>.tsv`. |

## Files added

```
tools/replay/__init__.py                       (new — public API)
tools/replay/trace.py                          (new — Access dataclass, load_synthetic, load_from_trino_csv, write_csv)
tools/replay/policies.py                       (new — LRU, FIFO, S3FIFO, BeladyMin, build_policy)
tools/replay/simulator.py                      (new — simulate(), SimStats, write_tsv_row)
tools/replay/main.py                           (new — CLI: --synthetic | --trace, --capacity-mb (repeat), --policies, --output)
tools/replay/sql/extract_trace_30d.sql         (new — operator-runs SQL for the 30d production trace)
tools/replay/tests/__init__.py                 (new)
tools/replay/tests/test_simulator.py           (new — 14 unit tests, no network/cluster dep)
tools/replay/README.md                         (new — usage, schema, validation discipline, v1 limits)
agents/out/SHELF-35/handoff.md                 (new — this file)
```

No shelfd Rust code touched. No Cargo / Helm version bump. No image
build. No cluster cutover. No smoke watch on cluster.

## What this unblocks

The plan locks five P1+ tickets behind SHELF-35:

| Ticket | Gate satisfied by SHELF-35 |
|---|---|
| SHELF-31 (Vegas / AIMD limiter) | "≥ 7d clean SHELF-29 soak + SHELF-35 quantified gap" |
| SHELF-32 (Sieve eviction) | "Replay validation" (Sieve goes through the harness against the same trace as LRU + S3-FIFO) |
| SHELF-33 (W-TinyLFU admission) | "Replay-validated ≥ 5 pp lift" |
| SHELF-36 (3L-Cache learned policy) | "SHELF-35 ≥ 5 pp lift gate" — explicit |
| SHELF-37 (HRW bounded-load) | "Replay must confirm the lift before shipping" |

Without SHELF-35 each of those is a guess. With SHELF-35 + the
operator-extracted 30-day production trace, every algorithm change is
measurable against the same trace + Belady-MIN upper bound.

## v1 limits (intentional, documented)

1. **Granularity is `(query, table)`**. `cdp.trino_logs.trino_queries.inputs_json`
   has one entry per `(catalog, schema, table)` with `physicalInputBytes`,
   not per split / row group. Per-split paths are not retrievable
   after-the-fact since `SplitCompletedEvent` was removed in
   [Trino #26436 (merged 2025-08-19)](https://github.com/trinodb/trino/pull/26436)
   per ADR-0005.
2. **No latency model**. The TSV reports counts and bytes; p50/p99
   latency is SHELF-35d (joins with `shelf_request_seconds`).
3. **No multi-pod simulation**. The cache is one pod. The 4-pod
   shelf StatefulSet's HRW + peer-fetch effects (SHELF-23) are
   SHELF-35e.
4. **Sieve / W-TinyLFU / 3L-Cache are deferred to SHELF-35c**. v1 is
   the *infrastructure*, not every algorithm. Adding a new policy is
   a 50-LOC subclass of the existing protocol once the gate-validation
   case is made.

## Validation discipline (per the plan)

- **Replay output must reproduce the live cluster's last-7-day hit
  ratio within ±2 pp.** If not, discard the run; do NOT use as a
  baseline. The harness can't tell whether the trace is stale —
  this check is the operator's responsibility.
- **Same trace + same seed (synthetic) ⇒ byte-identical TSV.** Two
  operators getting different TSVs indicates one ran a different
  policy or capacity; double-check before drawing conclusions.
- **TSV outputs land under `agents/out/SHELF-35/`** per the plan's
  "Output frozen per algorithm" rule. Don't overwrite — append a
  date suffix.
