# `trino_logs` replay analysis harness (SHELF-26)

> Offline analysis + cache simulator that consumes a dump of
> `your_query_log_table` and Iceberg manifest snapshots, and
> produces publishable CSV + JSON measuring:
>
> 1. **E5** — median and P90 ratio of row-group-scanned bytes vs
>    file-scanned bytes, per query and per day.
> 2. **Cache hit-rate** under a matrix of Foyer-equivalent
>    configurations (capacity × policy × admission threshold × pin
>    list).
>
> Design note: `shelf/shelfd/docs/design-notes/SHELF-26-replay-harness.md`.
> This is **not** the live v0.5-gate benchmark — that lives at
> `shelf/benchmarks/replay/SPEC.md`.

## Install (local dev)

```bash
cd shelf/benchmarks/trino_logs
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
```

## Quick-start against the committed synthetic fixture

```bash
shelf-replay analyze \
  --trace fixtures/synthetic-7d/trace.jsonl \
  --manifest-dir fixtures/synthetic-7d/manifests \
  --out results/synthetic-7d/

shelf-replay simulate \
  --trace fixtures/synthetic-7d/trace.jsonl \
  --manifest-dir fixtures/synthetic-7d/manifests \
  --configs fixtures/synthetic-7d/sim-configs.yaml \
  --out results/synthetic-7d/
```

Output:

```
results/synthetic-7d/
  per-query.csv        # one row per trace entry
  per-day.csv          # E5: median + P90 rg/file ratio per day
  sim-<config>.csv     # hit-rate curve per config
  summary.json         # aggregate metrics (schema-validated)
```

`make replay-rep2-7d` runs the same pipeline end-to-end and asserts
the synthetic fixture produces its golden numbers.

## Running against real rep-2 data

1. **Fetch the trace.** From a Trino replica with access to
   `your_query_log_table`:

   ```sql
   -- scripts/fetch-trace.sql
   SELECT
     query_id,
     query_date,
     query,
     plan,
     catalog,
     schema,
     "user",
     wall_time_millis,
     physical_input_bytes,
     error_code
   FROM your_query_log_table
   WHERE query_date >= from_iso8601_timestamp('2026-04-16T00:00:00Z')
     AND query_date <  from_iso8601_timestamp('2026-04-23T00:00:00Z')
     AND server_address = '<rep-2 coordinator IP>'
     AND query_type IN ('SELECT')
     AND error_code IS NULL
   ORDER BY query_date ASC
   ```

   Save as `trace.jsonl` (one JSON object per line). Record the query
   hash + Trino `queryId` of the export into `trace.meta.json` so the
   run is byte-identical to reproduce.

2. **Export manifests.** For every distinct
   `(catalog, schema, table, snapshot_id)` the trace references, copy
   the Iceberg `metadata.json` + its manifest-list + manifest Avro
   files into a local directory:

   ```bash
   python scripts/export-manifests.py \
     --trace trace.jsonl \
     --out /tmp/rep2-manifests/
   ```

   The script uses `pyiceberg` with whatever AWS credentials are in
   scope — IRSA or a local `AWS_PROFILE` both work.

3. **Run the harness.** Same CLI as above, pointing at the real
   inputs:

   ```bash
   shelf-replay analyze --trace trace.jsonl --manifest-dir /tmp/rep2-manifests/ --out results/rep2-7d/
   shelf-replay simulate --trace trace.jsonl --manifest-dir /tmp/rep2-manifests/ --out results/rep2-7d/
   ```

   Wall-clock on a 4-core dev pod for a 7-day window: ~6–9 min.

## What the harness does not do

- It does **not** issue live queries against Trino.
- It does **not** read S3 at simulation time — the manifest + footer
  cache is pre-exported once and re-used on every sweep.
- It does **not** cover join predicates or subqueries — those fall
  through as `predicate_extraction=none` and are counted at file
  granularity only (conservative: never under-counts scanned bytes).
  Tracked as SHELF-26a.
