# SHELF-35 — Belady oracle replay harness

Drives algorithm-comparison experiments over Trino query traces. The
output is the empirical headroom number every other ticket cites:

> "Sieve+W-TinyLFU is X percentage points from Belady-optimal on a
> 30-day cdp.trino_logs.trino_queries trace."

Without this, every change to the admission / eviction policy
(SHELF-26 / SHELF-31 / SHELF-32 / SHELF-33 / SHELF-36) is a guess.

## Quick start

```bash
# Synthetic smoke (no Trino dep, deterministic):
python -m tools.replay.main \
    --synthetic \
    --capacity-mb 14000 \
    --policies lru,fifo,s3fifo,belady \
    --output /tmp/replay-smoke.tsv

# Real production trace (operator runs the SQL first):
python -m tools.replay.main \
    --trace /tmp/trace_30d.csv \
    --capacity-mb 1000 --capacity-mb 5000 --capacity-mb 14000 \
    --policies lru,fifo,s3fifo,belady \
    --output agents/out/SHELF-35/replay-$(date +%F).tsv
```

`--capacity-mb` is repeatable — one TSV row per `(policy, capacity)`.

## Trace extraction

The operator runs `tools/replay/sql/extract_trace_30d.sql` against
**rep-3** (or any direct-S3 replica that won't self-affect during a
shelf cutover) and exports the result as CSV. The CSV columns are:

| Column        | Type    | Meaning |
|---|---|---|
| `timestamp_ms` | bigint  | IST-converted millisecond timestamp. `query_date` in `cdp.trino_logs.trino_queries` is UTC; the SQL converts to IST. |
| `object_id`    | varchar | `<catalog>.<schema>.<table>` — the cache key the simulator uses. |
| `size_bytes`   | bigint  | `physicalInputBytes` for that `(query, table)` — bytes the query actually read after predicate pushdown. |
| `query_id`     | varchar | Trino query id; useful for joining back against the per-query latency histogram. |

Why `(query, table)` granularity:

- `cdp.trino_logs.trino_queries.inputs_json` records one entry per
  `(catalog, schema, table)` with a `physicalInputBytes` field.
- Per-split paths are **not** recorded —
  Trino removed `SplitCompletedEvent` in
  [PR #26436 (merged 2025-08-19)](https://github.com/trinodb/trino/pull/26436)
  per ADR-0005. SHELF-35b will lift this to file granularity by
  joining against Iceberg `$files` snapshots; v1 stops at the table
  granularity that the audit log supports honestly.

## Output schema

One TSV row per `(policy, capacity)`:

```
policy  capacity_bytes  accesses  hits  misses  bytes_requested
bytes_hit  bytes_miss  bypassed  evictions  bytes_evicted
hit_ratio  byte_hit_ratio  byte_miss_ratio
```

`hit_ratio` / `byte_hit_ratio` / `byte_miss_ratio` are formatted to 6
decimals so two TSVs differing in the 7th decimal still diff cleanly
under `diff -u`. Operators check the per-day TSV into
`agents/out/SHELF-35/` so a future agent can diff against an older
run.

## Policies

| Name | Source | Why included |
|---|---|---|
| `lru` | textbook | baseline upper bound for non-frequency-aware policies |
| `fifo` | textbook | sanity floor — if `s3fifo` doesn't beat `fifo`, the trace is degenerate |
| `s3fifo` | [Yang+ SOSP 2023](https://www.usenix.org/system/files/nsdi24-zhang-yazhuo.pdf) | Foyer's pre-bump default; reproduces the rep-1 v0.1 production trade-off |
| `belady` | [Belady 1966](https://doi.org/10.1147/sj.52.0078) | future-optimal oracle. Establishes the upper bound every other policy is measured against. |

Sieve, W-TinyLFU, 3L-Cache are deferred to SHELF-35b — each warrants
its own ADR and its own per-policy TSV.

## Running tests

```bash
cd shelf
python -m unittest -v tools.replay.tests.test_simulator
```

11 unit tests, no network / cluster dependency.

## Validation discipline

Per the plan's discipline rules:

- Replay output must reproduce the live cluster's last-7-day hit
  ratio within ±2 pp, otherwise discard the run; do NOT use as a
  baseline.
- Same trace + same seed (synthetic) ⇒ byte-identical TSV. The
  simulator is deterministic. If two operators get different TSVs,
  one of them ran a different policy or capacity; double-check.
- TSV outputs land under `agents/out/SHELF-35/` per the plan's
  "Output frozen per algorithm" rule. Don't overwrite — append a
  date suffix.

## Limitations (v1)

1. **Granularity is `(query, table)`**, not file or row group. Per
   the workspace memory + ADR-0005 reasoning above, this is the only
   granularity `cdp.trino_logs.trino_queries` honestly supports
   today. SHELF-35b will lift this.
2. **No latency model**. The TSV reports counts and bytes; p50/p99
   latency is a follow-up that joins the trace with
   `shelf_request_seconds` histograms from Prom.
3. **No multi-pod simulation**. The cache is one pod. The 4-pod
   shelf StatefulSet's HRW + peer-fetch effects (SHELF-23) are out
   of scope for v1; SHELF-35b will simulate the 4-pod ring with
   peer-fetch races.
