# ClickHouse + Shelf S3 shim — runnable example

A 5-minute walkthrough that shows ClickHouse 24.12 reading an Apache
Iceberg table through Shelf's signature-agnostic S3 read shim.

```
                          shelfd:9092 (S3 GET/HEAD)
                          ┌──────────────┐
ClickHouse  ── iceberg() ─┤  Shelf shim  ├──── http://minio:9000 ──► MinIO (warehouse/)
                          └──────────────┘
                                 ▲
                                 │ on miss
                                 ▼
                            DRAM/NVMe cache
```

The example ships a tiny Iceberg table (`demo.events`, ~30k rows across
30 days) and runs the same `count()/avg()` query twice — first against an
empty Shelf cache, then against a warm one — so you can see hit counters
move and the shelf delivering reads instead of MinIO.

## Layout

| File                              | Purpose |
| --------------------------------- | ------- |
| `docker-compose.yml`              | MinIO, iceberg-rest, seed (one-shot), shelfd, ClickHouse |
| `init/seed.sh`                    | pip-installs `pyiceberg[pyarrow,s3fs]` and runs the writer |
| `init/seed_iceberg.py`            | Writes `demo.events` (30k rows, schema `id, ts, date, value`) |
| `config/shelfd/shelfd.yaml`       | DRAM-only single-pod shelfd config; origin = `s3://warehouse/` on MinIO |
| `config/clickhouse/00-shelf-example.xml` | Disables the Iceberg metadata cache so warm reads still hit shelfd |
| `bench.sql`                       | The two queries the example runs |
| `run.sh`                          | Cold + warm orchestration; prints a summary |

## Prerequisites

- Docker + Docker Compose (a recent build supporting the long-form
  `depends_on` schema).
- ~3 GiB free RAM and ~2 GiB free disk for MinIO + image layers.
- Host ports 8123, 8381, 9300, 9301, 9009, 9390, 9392 free. (The example
  intentionally uses non-default host ports so it does not collide with
  other Shelf compose stacks running on the same machine.)
- First run builds `shelfd` from the production Dockerfile at the repo
  root (`shelfd/Dockerfile`); allow ~5–10 min on a cold cache. Subsequent
  runs are seconds.

## Run it

```bash
cd shelf/examples/clickhouse
bash run.sh
```

`run.sh` brings the stack up, waits for the seed step to finish, runs
`SELECT 1` to warm the ClickHouse process, then issues the bench query
twice — cold (empty cache) and warm (cache populated by the cold run).

Sample output:

```
============================================================
ClickHouse + Shelf S3 shim — cold vs warm
============================================================
Bench query result (cold):  1000 500.1234
Bench query result (warm):  1000 500.1234

Wall clock (ClickHouse exec)
  cold:   612 ms
  warm:   154 ms   (speedup 3.97x)

Shelf cache deltas (sum across all pools)
  cold pass:  hits +0       misses +14
  warm pass:  hits +14      misses +0
============================================================
```

The cold pass should produce only misses (Shelf hasn't seen any of the
manifests / Parquet ranges yet); the warm pass should produce only hits
(or near-zero misses if ClickHouse's own readahead asks for a slightly
larger range than the cold pass did).

## Tear down

```bash
docker compose down -v
```

`-v` removes the MinIO data volume so a re-run starts clean.

## How ClickHouse talks to Shelf

The bench query is:

```sql
SELECT count(), avg(value)
FROM iceberg('http://shelfd:9092/warehouse/demo/events', 'dummy', 'dummy')
WHERE date = '2024-01-15';
```

Three things make this work:

1. **`iceberg()` is an alias for `icebergS3()`** in ClickHouse 24.x. It
   takes a `<url>, <access_key>, <secret>` triple, walks Iceberg
   metadata starting from the table directory, and reads Parquet data
   files via the same S3 client.
2. **Path-style addressing is auto-detected.** ClickHouse only switches
   to virtual-hosted style when the URL host matches the AWS pattern
   `<bucket>.s3.<region>.amazonaws.com`. `shelfd` is not such a host, so
   `http://shelfd:9092/warehouse/demo/events` is parsed as
   `bucket=warehouse`, `key=demo/events/...` — exactly what the shim
   expects. There is no `s3_use_path_style` query setting in 24.x; the
   `<use_path_style_url>` XML element only applies to
   `<storage_configuration>` disks, which we are not using.
3. **The credentials are placeholders.** Shelf's S3 shim is
   signature-agnostic — it ignores the `Authorization` header and uses
   whatever credentials it has in its own environment to talk to the
   origin. `'dummy', 'dummy'` is a deliberate signal that ClickHouse
   never authenticates against Shelf.

## Verifying the wiring

Three lightweight checks, each useful on its own:

```bash
# 1. The Shelf shim accepts ClickHouse's HEAD request directly.
curl -s -I http://127.0.0.1:9392/warehouse/demo/events/metadata/ | head -1

# 2. Hit counters per pool.
curl -s http://127.0.0.1:9390/metrics | grep -E 'shelf_(hits|misses)_total'

# 3. MinIO shows the seeded table.
docker exec shelf-ch-minio mc ls --recursive local/warehouse/demo/events/ | head
```

## Notes on ClickHouse version pinning

We pin `clickhouse/clickhouse-server:24.12.4.49`. The minimum version
that makes this example interesting is roughly:

| Capability                       | Minimum ClickHouse |
| -------------------------------- | ------------------ |
| `iceberg()` table function       | 23.3               |
| Iceberg time travel              | 24.1               |
| `use_iceberg_partition_pruning`  | 24.x               |
| Iceberg schema evolution         | 24.12              |

If you only need the bench query, anything from 24.1 onwards will work;
the file just hard-codes a recent confirmed-on-Docker-Hub patch tag.

## What this example is not

- It is **not** a benchmark. The dataset is a few MiB and the query
  finishes in tens of milliseconds; both wall-clock numbers are
  dominated by query overhead, not S3 throughput. Run a real workload
  (TPC-DS, your prod queries) for sizing.
- It uses `iceberg-rest` for seeding only. ClickHouse never talks to the
  catalog — it walks the metadata files in S3 directly. If you wire
  ClickHouse to a catalog elsewhere, point `s3_endpoint` at shelfd and
  the same shim wiring applies.
- It does not exercise Shelf's NVMe tier; `dram_bytes` is 768 MiB, more
  than enough to hold the full table footprint in memory.
