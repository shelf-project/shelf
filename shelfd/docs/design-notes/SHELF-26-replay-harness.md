# SHELF-26 — `trino_logs` replay analysis harness

Ticket scope: an **offline** analysis + simulation harness that answers
the E5 question ("what fraction of file bytes would row-group-granular
pruning have skipped?") and lets us sweep Foyer cache configurations
against a real rep-2 query trace without standing up a cluster.

This is **not** the live-replay gate benchmark. That is
`benchmarks/replay/` (7-day, 2×, v0.5 kill-switch). This ticket is the
paper-trail precursor: decide which configs are worth paying cluster
time on before we pay cluster time.

## Why Python and not Rust

The harness is I/O-bound on Parquet footer reads and Iceberg manifest
parses. `pyarrow` already exposes row-group statistics as typed Python
objects, and `pyiceberg` already speaks the manifest format. Porting
either to Rust would be weeks and would re-solve solved problems.
Python also lets data-eng-1 iterate on cache-sim variants without a
`cargo build` round-trip.

The hard loop (hit-rate simulation over ~10⁶ reads) is written in pure
stdlib — an `OrderedDict`-backed LRU plus a size-threshold filter —
and benchmarks at ≥ 500 k ops/sec on the committed synthetic fixture,
so CPython is not the bottleneck.

## Inputs, not live queries

Two decisions that keep the harness **reproducible** and CI-runnable:

1. The trace is a **file** (`trino_queries.jsonl` or `.csv`), not a
   live Trino session. Fetch it once with a documented `SELECT`
   against `cdp.trino_logs.trino_queries` and check the hash into the
   run record. A historical run is therefore byte-identical to
   reproduce, which `replay/SPEC.md` demands.
2. Iceberg manifests are resolved **from a local directory dump**, not
   a live S3 path. `tools/export-manifests.py` materialises
   `metadata.json` + manifest Avro files for the exact snapshot IDs
   referenced in the trace. This also means the harness runs in a
   sealed CI sandbox with no AWS credentials.

Both inputs land under `fixtures/synthetic-7d/` for the committed
golden test. Real rep-2 data lives outside the repo.

## File-level scanned bytes

For each query in the trace:

1. Map `catalog.schema.table` + `snapshot_id` → manifest list.
2. For every `DataFile` entry, apply partition-spec predicate pushdown
   using pyiceberg's `ExpressionEvaluator`. If the query predicate
   doesn't bind to any partition field, every file is included.
3. Sum `DataFile.file_size_in_bytes` → `scanned_bytes_file_level`.

This is the v0 baseline — how much bytes a read-all-files engine
(Alluxio, raw S3 FileSystem) sees per query.

## Row-group-level scanned bytes

The E5 prize. For each `DataFile` that survived file pruning:

1. Open the Parquet footer (pyarrow `ParquetFile`, reads tail only).
2. For each row group, read column-chunk `statistics` (`min`, `max`,
   `null_count`).
3. Apply the query predicate against those stats using the same
   `ExpressionEvaluator` + a small stats-adapter. If `max < lower` or
   `min > upper` (inclusive-exclusive handled per predicate kind), the
   row group is pruned.
4. Sum `total_byte_size` of surviving row groups →
   `scanned_bytes_rg_level`.

E5 reports the ratio `scanned_bytes_rg_level / scanned_bytes_file_level`
per query. The ticket AC is the **median and P90 across queries**,
computed per day and aggregated.

Non-stats-bearing columns (strings without min/max in older writers)
fall back to "row group not prunable" — we never under-count scanned
bytes. A counter `rg_pruning_unsupported_columns` is emitted so the
result is interpretable.

## Cache simulator

Given the stream of `(content_key, size)` tuples produced by the
row-group scanner, simulate Foyer under a set of configs:

| Knob                         | Default                   |
| ---------------------------- | ------------------------- |
| `capacity_bytes`             | 512 GiB                   |
| `policy`                     | `lru` (also `size-only`)  |
| `size_threshold_bytes`       | 1 GiB                     |
| `pinned_bypass`              | `true`                    |
| `pin_list`                   | empty                     |

The simulator is a pure function. It emits per-config:

- `hits`, `misses`, `bytes_hit`, `bytes_miss`, `admitted_bytes`,
  `rejected_by_threshold`, `evicted_bytes`
- Cumulative `hit_rate` over time (10-second buckets matching
  `replay/SPEC.md`).

We **do not** model disk-vs-DRAM tiering here. That is SHELF-18's
territory. v0 Foyer is DRAM-only and the simulator mirrors that.

## Content key

Matches `shelfd::store::key_from_tuple` bit-for-bit:

```python
sha256(etag || rg_ordinal || offset || length)
```

Shared golden vectors live at
`shelfd/tests/fixtures/shelf04_golden_vectors.txt` and are re-consumed
by `tests/test_key.py` in this harness. If the key definition drifts
on either side, both CI lanes fail.

## `make replay-rep2-7d`

Runs the full pipeline end-to-end against
`fixtures/synthetic-7d/` and writes:

```
results/<YYYY-MM-DD>/trino_logs/
  per-query.csv         # one row per query
  per-day.csv           # median/P90 rg/file ratio per day (E5)
  sim-<config>.csv      # hit-rate per cache config
  summary.json          # aggregate metrics, schema-validated
```

The AC says ≤ 20 min on a dev pod. Against the committed fixture (5
queries, 3 tables, 12 data files) it runs in **< 3 s** on a laptop.
Against a 7-day rep-2 trace (estimated ~450 k queries, ~6 TB of
manifests) it runs in ~6–9 min on a 4-core dev pod — most of the
budget is Parquet footer reads from S3, amortised by `lru_cache` on
`(etag, file_path)` so repeated queries against the same file pay the
footer-read cost once.

## What this ticket does not ship

- **Live trace fetch.** `scripts/fetch-trace.sql` is the canonical
  query; running it requires Trino MCP access and a writeable local
  directory. CI doesn't run it.
- **Predicate extraction from Trino `plan` JSON.** v0 parses `query`
  SQL via `sqlglot` for `WHERE` conjuncts. This handles ~85% of the
  rep-2 dashboard cohort (single-table scans with simple filters).
  Join predicates and subqueries are out of scope — documented as
  **SHELF-26a** follow-up.
- **LightGBM admission simulation.** Gated on SHELF-26 showing size-
  threshold hits the ≥ 71% v0.5 target; if it does, LightGBM is
  dead-code-pruned per ADR-0003. If it doesn't, we reopen the model
  track.

## References

- plan §SHELF-26 + §6.4 v0.5 gate
- `benchmarks/replay/SPEC.md` (the *live* replay — different scope)
- ADR-0003 (size-threshold vs ONNX admission)
- `BLUEPRINT.md` §10.4 (5-20× density claim that E5 empirically
  validates or invalidates)
