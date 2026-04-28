# Iceberg MV auto-rewrite verification (H4)

Track H's delta vs Firebolt hinges on Trino 468+ recognising an
Iceberg materialized view and rewriting a matching query to hit
it instead of re-scanning the base table. Because that
auto-rewrite is an optimizer behaviour (not a plugin hook), we
verify it with an end-to-end test that

1. seeds a small Iceberg base table,
2. creates an MV over a TPC-DS-style aggregation,
3. runs the same aggregation against the base table, and
4. asserts `EXPLAIN` references the MV and that the elapsed
   wall-clock drops by at least 5x.

## Prerequisites

- Trino 468 or newer reachable via `TRINO_HOST` (default
  `http://localhost:8080`).
- The `iceberg` catalog has `iceberg.materialized-views.enabled=true`
  (already set for shelf's dev + prod configs; see
  `infra/trino/dev/cdp_shelf.properties`).
- A scratch schema the harness owns end-to-end; the default is
  `iceberg.shelf_mv_rewrite_test`. The harness will `CREATE SCHEMA
  IF NOT EXISTS` on first run and `DROP SCHEMA CASCADE` on
  teardown.

## Running the test

```bash
cd benchmarks/mv-rewrite
./run.sh
```

The script exits non-zero if

- `EXPLAIN` of the user query does not reference the MV name, or
- the elapsed delta is less than 5x, or
- teardown fails.

Wire this into the nightly SF100 CI job (F4 sibling workflow —
`tpcds-regression.yml`) once the Track H gate opens. Until then
it runs on demand to catch regressions in Trino version bumps.

## Why a shell harness

Every hop (Trino, the dbt pipeline that publishes MVs, shelfd's
pin-list refresher on `CREATE MATERIALIZED VIEW`) is already
addressable via HTTP. Keeping the test shell-first avoids
pulling JDBC + Trino test jars into every developer laptop.
