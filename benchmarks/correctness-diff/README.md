# shelf/benchmarks/correctness-diff

Row-level correctness diff between a Shelf-backed Trino catalog
(`iceberg`) and an S3-direct baseline catalog (`iceberg_direct`).
Runs a fixed set of 5 canonical Iceberg queries, canonicalises each
result set (sorted tuples → SHA-256), and fails the process with
exit-code 1 if any result diverges row-for-row.

This is the substitute for SHELF-13's shadow-mirror in the
compressed-canary rollout (see
[`docs/rollout-v1.md`](../../docs/rollout-v1.md) §3). The
shadow-mirror we're skipping caught byte-level mismatches on *every*
production query; this harness catches them on *5 canonical queries*
every hour during each replica's canary window. The gap is
documented as a residual risk in the rollout plan.

## Query inventory

Five `.sql.tmpl` files under [`queries/`](queries/):

| # | name                           | dimension exercised                              |
| - | ------------------------------ | ------------------------------------------------ |
| 1 | `01-count-recent.sql.tmpl`     | manifest-list reads + partition pruning          |
| 2 | `02-group-by-user.sql.tmpl`    | row-group reads + shuffle + aggregation          |
| 3 | `03-join-two-tables.sql.tmpl`  | row-group reads on both join sides               |
| 4 | `04-large-scan-limit.sql.tmpl` | whole-file byte-range reads + ORDER BY + LIMIT   |
| 5 | `05-predicate-pushdown.sql.tmpl` | file-skipping + Parquet page-index skipping    |

The five were picked with data-eng review (see
[rollout plan §3](../../docs/rollout-v1.md)) to cover the query
shapes that between them touch every Shelf code path in the read
critical path.

## How the diff works

```
┌─────────┐      SQL        ┌──────────────┐
│ runner  │────────────────▶│ iceberg_     │──▶ S3 direct
│         │                 │ direct       │
│         │◀─── rows  ──────│ (catalog A)  │
│         │                 └──────────────┘
│  canon  │                 ┌──────────────┐
│  + hash │      SQL        │   iceberg    │──▶ shelfd ──▶ S3
│         │────────────────▶│ (catalog B)  │
│         │                 │              │
│         │◀─── rows  ──────└──────────────┘
└─────────┘
    │
    ├─ equal hashes + equal row counts ⇒ match (exit 0)
    └─ otherwise ⇒ diff_preview.json + exit 1
```

Canonicalisation uses `sorted(str(col) for col in row) → SHA-256`;
the unit-separator `\x1f` joins columns and the record-separator
`\x1e` joins rows so any column value containing them is definitionally
a Trino-side payload-encoding bug and *should* trigger a diff.

## Local smoke test

```bash
# From the shelf/benchmarks/smoke directory, have docker-compose
# running with both catalogs configured (see the `Dual-catalog
# smoke config` section below).
cd shelf/benchmarks/correctness-diff
make install                       # creates .venv and installs -e .[dev]
make test                          # pytest (no live Trino needed)
make run CONFIG=config.smoke.yaml  # hits the smoke compose stack
```

For the smoke compose stack the `catalog_a`/`catalog_b` both point
at the same MinIO-backed warehouse — the run is expected to diverge
on *zero* queries. This is the harness self-check.

### Dual-catalog smoke config

To run the harness locally you need a second catalog
`iceberg_direct` alongside the existing `iceberg` catalog. Add
`shelf/benchmarks/smoke/config/trino/etc/catalog/iceberg_direct.properties`
as a byte-identical copy of `iceberg.properties` *except* the
`s3.endpoint` line — point `iceberg_direct` at MinIO
(`http://minio:9000`) and leave `iceberg` at `http://shelfd:9092`.
Restart Trino; both catalogs now exist.

This "dual catalog" pattern is exactly what production uses during
each replica's canary window; the only difference is the endpoint
values.

## Production deployment

The compressed-canary rollout runs this harness as a
Kubernetes `CronJob` scoped to one replica's Trino Gateway pool
during that replica's canary window. A full example manifest lives
at [`k8s/cronjob.example.yaml`](k8s/cronjob.example.yaml).

Cron schedule: `0 * * * *` (hourly, on the hour). The rollout
runbook treats:

- Any single exit-1 during the canary window ⇒ **immediate rollback**
  of that replica (per rollout plan §5).
- Three consecutive exit-3s ⇒ same. Exit-3 means Trino itself is
  unreachable; three in a row implies a broken coordinator, not
  transient network flap.
- Exit-2 ⇒ harness configuration bug; does NOT auto-rollback (the
  cache is not implicated), but does page oncall-shelf.

## Configuration

See [`config.example.yaml`](config.example.yaml) for the full
schema. Per-replica overlays live next to the production CronJob
manifest and differ only on the `replica` field and on any
replica-specific filter bindings (e.g. `partition_value` tuned for
data freshness).

## Files

| path                                   | purpose                                      |
| -------------------------------------- | -------------------------------------------- |
| `pyproject.toml`                       | editable-install metadata                    |
| `Makefile`                             | `venv` / `install` / `test` / `run` targets  |
| `config.example.yaml`                  | per-replica configuration template           |
| `queries/*.sql.tmpl`                   | 5 canonical query templates                  |
| `src/correctness_diff/runner.py`       | core diff algorithm (unit-testable)          |
| `src/correctness_diff/cli.py`          | `shelf-correctness-diff` entry point         |
| `tests/test_runner.py`                 | unit tests (no live Trino required)          |
| `k8s/cronjob.example.yaml`             | production CronJob template                  |
