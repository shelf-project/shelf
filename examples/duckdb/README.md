# DuckDB on Shelf

A 5-minute, runnable example of [DuckDB](https://duckdb.org) reading an
Iceberg table through Shelf's S3-compatible shim. Cold reads pull bytes
from MinIO; warm reads hit Foyer DRAM (and NVMe, when configured).

```
              ┌────────────┐  s3://warehouse  ┌────────────┐
   DuckDB ──▶ │  shelfd    │ ──── proxy ────▶ │   MinIO    │
  (httpfs)    │  shim:9092 │ ◀── cache ─────  │ (origin)   │
              └─────┬──────┘                  └────────────┘
                    │ Iceberg metadata fetched the same way
                    ▼
              ┌────────────┐
              │ iceberg-   │
              │ rest:8181  │  (catalog only — no data path)
              └────────────┘
```

## Why this example exists

Shelf is engine-agnostic — anything that speaks S3 can sit in front of
it. Trino is the production driver, but the same shim works for DuckDB,
Spark, Athena, ClickHouse, or `aws s3 cp`. This example is the smallest
possible end-to-end demo: one container for storage, one for the
catalog, one for shelfd, one for the bench.

## Layout

```
examples/duckdb/
├── README.md             # you are here
├── docker-compose.yml    # MinIO + iceberg-rest + shelfd + seed + bench
├── run.sh                # one-shot driver
├── config/shelfd/
│   └── shelfd.yaml       # 256 MiB metadata pool + 512 MiB rowgroup pool
├── init/
│   ├── seed.sh           # entrypoint: pip install + run seed_iceberg.py
│   └── seed_iceberg.py   # writes 1 M-row partitioned events table
└── bench/
    ├── run-bench.sh      # entrypoint: pip install + run bench.py
    └── bench.py          # cold + warm DuckDB query, prints summary
```

## Run it

Prereqs: Docker 24+, ~4 GiB free RAM, ports 9000/9001/8181/9091/9092 free
on `127.0.0.1`. The shelfd image is built from the workspace root on
first run (~3–5 min); subsequent runs use the layer cache.

```bash
cd examples/duckdb
bash run.sh
```

`run.sh` brings up the stack, waits for `shelfd /healthz`, runs the
seed once, then runs the bench. Total time on a warm Docker cache is
under a minute.

## Expected output

Measured on the workspace box (M-series Mac, Docker Desktop, MinIO + shelfd
+ DuckDB all on the loopback network):

```
================================================================
 DuckDB → Shelf → MinIO  (Iceberg events table, 1 M rows)
================================================================
  cold:         791 ms    shelf hits/misses:    0 /   62    origin:   9.64 MiB
  warm:         123 ms    shelf hits/misses:   62 /    0    origin:   0.00 MiB
  speedup:     6.4x
  $-saved:     $0.000872   (62 GETs + 9.64 MiB egress avoided)
================================================================

 sample result rows:
   (datetime.date(2026, 4, 1), 33470, 24303, 501.76, 16793912.34)
   (datetime.date(2026, 4, 2), 33214, 24265, 500.3,  16617074.24)
   ...
```

Numbers will vary (~0.7–2.5 s cold, 80–250 ms warm depending on disk
speed and CPU), but the shape is stable: the warm run should report
**hits ≫ 0**, **misses == 0**, **origin bytes == 0**, and several-x
lower latency than the cold run.

> The bench disables DuckDB's `enable_external_file_cache` so we
> measure shelfd's cache, not DuckDB's. With it on, the warm run
> finishes in ~25 ms but never even calls shelfd.

## What just happened

1. **Cold query**: DuckDB's Iceberg extension walks the catalog —
   metadata JSON → manifest list → manifest files → Parquet footers
   → row groups. Every byte is a new GET to `shelfd:9092`, which finds
   nothing in Foyer and proxies the request to MinIO. Each response
   body is admitted to the metadata or rowgroup pool on the way back.
2. **Warm query**: DuckDB issues the same GETs. Shelf's content-
   addressed keys (`sha256(etag || offset || length || rg_ordinal)`,
   per ADR-0011) are stable across the two runs, so every byte is
   served from DRAM with zero MinIO traffic.
3. The bench script reads `shelf_hits_total` / `shelf_misses_total` /
   `shelf_origin_request_bytes_total` from `/metrics` to attribute
   the savings.

## Knobs

Environment variables on `docker-compose.yml`:

| Variable          | Default       | Service     | Effect                                                                         |
| ----------------- | ------------- | ----------- | ------------------------------------------------------------------------------ |
| `EVENTS_ROWS`     | `1000000`     | seed        | Total rows in `default.events`.                                                |
| `EVENTS_DAYS`     | `30`          | seed        | Date-partition cardinality. Each day → 1 Parquet file.                         |
| `DUCKDB_VERSION`  | `1.3.1`       | bench       | DuckDB pip wheel pinned at runtime.                                            |
| `KEEP_UP=1`       | unset         | host        | If set, `run.sh` leaves the stack running so you can iterate on `bench.py`.    |

Iterate the bench query without rebuilding shelfd:

```bash
KEEP_UP=1 bash run.sh
$EDITOR bench/bench.py
docker compose --profile bench run --rm bench
docker compose --profile bench down -v   # final cleanup
```

## Manual mode (run DuckDB on your laptop)

If you'd rather skip the bench container and use a local `duckdb` CLI
or notebook, the same pattern works — just point S3 at the host-mapped
shim port:

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;

CREATE OR REPLACE SECRET shelf_s3 (
    TYPE S3,
    KEY_ID 'dummy',
    SECRET 'dummy',
    REGION 'us-east-1',
    ENDPOINT '127.0.0.1:9092',
    URL_STYLE 'path',
    USE_SSL false
);

ATTACH '' AS lake (
    TYPE iceberg,
    ENDPOINT 'http://127.0.0.1:8181',
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none'
);

SELECT event_date, COUNT(*) FROM lake.default.events GROUP BY 1 ORDER BY 1;
```

The shim is signature-agnostic, so the `KEY_ID`/`SECRET` values are
ignored — DuckDB just needs *something* to construct an `Authorization`
header with.

## Cleanup

```bash
docker compose --profile bench down -v
```

This drops the MinIO volume too; the next `bash run.sh` reseeds.

## Troubleshooting

| Symptom                                                                  | Likely cause                                                                                                                                                                            |
| ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bench` reports `WARN: warm was slower than cold`                        | Foyer evicted between runs. Bump `pools.rowgroup.dram_bytes` in `config/shelfd/shelfd.yaml`, or shrink `EVENTS_ROWS`.                                                                   |
| Cold and warm both fast, hits/misses both 0                              | DuckDB's local httpfs cache is masking shelfd. Make sure you opened a fresh in-memory connection (the bench does this automatically).                                                   |
| `IO Error: HTTP GET error on '...': 416 Requested Range Not Satisfiable` | shelf shim version mismatch. The shim must accept suffix (`bytes=-100`) and open-ended (`bytes=0-`) ranges. Rebuild from the workspace tip (`docker compose build shelfd`).             |
| `shelfd` build OOMs on a 4 GiB Mac                                       | Cargo's release build needs ~3 GiB peak. Increase Docker Desktop's memory limit, or set `--profile build` with `cargo build --release -p shelfd` once on the host and `image:` instead. |

## See also

- `BLUEPRINT.md` — Shelf architecture and design rationale.
- `clients/trino/` — same idea, JDBC/Trino-shaped, with a richer
  smoke harness at `benchmarks/smoke/`.
- `docs/architecture.md` — cache key derivation (ADR-0011) and the
  Iceberg-snapshot-safety argument.
