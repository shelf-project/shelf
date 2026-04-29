# StarRocks on Shelf

A self-contained Docker Compose example that points
[StarRocks](https://www.starrocks.io/) at an Iceberg table living in MinIO,
with **Shelf's S3-compat shim sitting in the byte path**. It demonstrates
that any S3-API SQL engine — not just Trino — can use Shelf as a
transparent read cache, by changing one property: `aws.s3.endpoint`.

```
   StarRocks BE  ── Iceberg REST ──▶  iceberg-rest    (catalog state in sqlite)
                 ── S3 GET/HEAD ───▶  shelfd:9092   ── miss ──▶  minio:9000
                                          │
                                          └── hit: Foyer DRAM / NVMe
```

The shim is **signature-agnostic** by design — it ignores the SigV4
`Authorization` header — so the StarRocks-side credentials are dummy
strings. Real credentials live on the shelfd container itself, where
they're used to talk to the actual origin (MinIO here, S3 in production).

---

## What's in the box

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | minio, iceberg-rest (apache/iceberg-rest-fixture:1.9.2, sqlite state — see [Caveats](#caveats-and-known-pitfalls)), shelfd (built locally), starrocks (allin1-ubuntu:3.2.16), seed (one-shot PyIceberg writer). All host ports re-mapped to a unique high range (87xx / 97xx) so this example coexists with the sister duckdb / daft / spark / clickhouse examples. |
| `config/shelfd/shelfd.yaml` | DRAM-only shelfd config; origin → `minio:9000`, S3 shim binds `0.0.0.0:9092` |
| `init/seed.sh`, `init/seed_iceberg.py` | One-shot PyIceberg writer that creates a 1 M-row, 30-partition `default.events` Iceberg table |
| `init/create-catalog.sql` | StarRocks `CREATE EXTERNAL CATALOG iceberg_demo` with `aws.s3.endpoint=http://shelfd:9092` |
| `bench.sql` | Two scan-and-aggregate queries against `iceberg_demo.default.events` |
| `run.sh` | End-to-end orchestrator: build → up → seed → register catalog → cold bench → warm bench → diff metrics |

---

## Prerequisites

- Docker 24+ with Compose v2.
- ~6 GB free disk for the StarRocks image (5 GB pulled, ~1 GB resident).
- ~10 minutes the first time (Rust release build of shelfd is the long pole).

The StarRocks `allin1-ubuntu:3.2.16` image is published `linux/amd64`
only. On Apple Silicon, Docker Desktop emulates via QEMU — first boot is
60–120 s and steady-state queries are 2–3× slower than native. Fine for
this demo, **not** representative of production performance.

---

## Host port map

| Service | Host port | Container port |
| --- | --- | --- |
| MinIO API | 9700 | 9000 |
| MinIO console | 9701 | 9001 |
| Iceberg REST | 8781 | 8181 |
| shelfd `/metrics`, `/admin` | 9791 | 9090 |
| shelfd S3 shim | 9792 | 9092 |
| StarRocks FE HTTP | 8730 | 8030 |
| StarRocks MySQL | 9730 | 9030 |
| StarRocks BE HTTP | 8740 | 8040 |

(The Postgres state DB is not published — it's only reachable from the
compose network.)

## Quick start

```bash
cd shelf/examples/starrocks
bash run.sh
```

That runs eight steps end-to-end and prints something like:

```
=== Shelf cache effect ===
                       hits         misses       wall-secs
cold (run 1)           0            93           4.21
warm (run 2)           87           6            1.18

Cumulative origin_bytes  : 41873920
Cumulative cache_bytes   : 39214592
```

The exact numbers vary by host, but the shape is consistent: the cold run
is dominated by misses, the warm run is dominated by hits, and wall-time
drops materially.

To shut down and free the MinIO + Postgres volumes:

```bash
bash run.sh --cleanup
```

---

## How the wiring works

### 1. Seed: PyIceberg → MinIO directly

`init/seed_iceberg.py` is a one-shot Python container that talks to
`iceberg-rest:8181` for the catalog and to `minio:9000` for the storage.
It does **not** go through shelfd — that path only matters for the
*read* benchmark we're demonstrating. Result: a partitioned Parquet
Iceberg table at `s3://warehouse/default.db/events/...`.

### 2. Catalog registration

```sql
CREATE EXTERNAL CATALOG iceberg_demo PROPERTIES (
  "type"                              = "iceberg",
  "iceberg.catalog.type"              = "rest",
  "iceberg.catalog.uri"               = "http://iceberg-rest:8181",
  "iceberg.catalog.warehouse"         = "warehouse",
  "aws.s3.endpoint"                   = "http://shelfd:9092",
  "aws.s3.enable_path_style_access"   = "true",
  "aws.s3.access_key"                 = "dummy",
  "aws.s3.secret_key"                 = "dummy",
  "aws.s3.region"                     = "us-east-1",
  "client.factory"                    = "com.starrocks.connector.iceberg.IcebergAwsClientFactory"
);
```

The single line that turns Shelf on is `aws.s3.endpoint=http://shelfd:9092`.
Property names are verified against the StarRocks 3.2 docs:

- [Iceberg catalog reference](https://docs.starrocks.io/docs/3.2/data_source/catalog/iceberg_catalog)
- [Iceberg lakehouse tutorial](https://docs.starrocks.io/docs/3.2/data_source/icebergtutorial/) — uses the same property set against MinIO directly; we just swap the endpoint.

`client.factory` matters: without it, StarRocks's S3 client falls back
to the default credentials chain (instance profile / env vars) and the
`aws.s3.access_key` / `aws.s3.secret_key` properties are ignored.

### 3. Read path

When StarRocks queries `iceberg_demo.default.events`:

1. FE asks `iceberg-rest:8181` for the table's metadata pointer (snapshot path).
2. BE downloads `s3://warehouse/.../metadata/v1.metadata.json`, then the
   manifest list (`.avro`), then per-partition manifests, then per-file
   Parquet footers, then per-row-group Parquet pages.
3. Every one of those byte ranges goes to `http://shelfd:9092/warehouse/...`
   — the shim parses the bucket out of the path-style URL and either
   serves from Foyer (DRAM/NVMe) or proxies to `minio:9000`.

### 4. Verifying the cache is actually doing work

While `run.sh` is running (or after), peek at shelfd's metrics:

```bash
curl -s http://127.0.0.1:9791/metrics | grep -E '^shelf_(hits|misses|origin)_total'
```

Or open the MinIO console at <http://127.0.0.1:9701>
(user `minioadmin` / pass `minioadmin`) to see only the cold-run requests
hit the bucket access log, while the warm-run reads stay inside shelfd.

---

## Caveats and known pitfalls

- **Reads only.** This example exercises the SELECT path. Writing to an
  Iceberg table from StarRocks (`INSERT INTO iceberg_demo...`) goes
  through `aws.s3.endpoint` for *every* verb (PUT, multipart). Shelf's
  shim handles small PUT/DELETE since SHELF-21, but multipart upload
  isn't yet supported in this example's preview build — see the upstream
  shelf docs. There's also a known StarRocks 3.4.0 multipart-with-MinIO
  bug ([#56178](https://github.com/StarRocks/starrocks/issues/56178))
  that's orthogonal to Shelf.
- **StarRocks Iceberg metadata cache.** StarRocks has its own JVM-local
  Iceberg metadata cache (default 512 MB). On the warm run, *some* of
  the apparent speedup is from that cache rather than from Shelf — both
  contribute, and that's fine for a demo. To isolate Shelf's
  contribution, set `enable_iceberg_metadata_cache=false` in
  `fe.conf` (out of scope here).
- **`apache/iceberg-rest-fixture` is a reference image, not a production
  catalog server.** It's the same image the StarRocks docs Iceberg
  tutorial uses. For production, swap in Lakekeeper, Polaris, or your
  own.
- **Catalog state is sqlite, not Postgres.** The reference image does
  not bundle a Postgres JDBC driver, so wiring it to an external
  Postgres requires either a custom image (rebuild with the driver) or
  a JAR bind-mount onto `/usr/lib/iceberg-rest/` — both out of scope
  for a 5-minute walkthrough. Sqlite is fine here: catalog state is a
  few rows of namespace + table-pointer metadata, and the demo's
  *whole point* is the byte read path through shelfd, not catalog-state
  durability.
- **Image is linux/amd64 only.** `starrocks/allin1-ubuntu:3.2.16` is
  emulated on ARM. The `platform: linux/amd64` field in the compose
  file is explicit so this is at worst slow, never silently wrong.
- **DRAM-only Foyer.** `shelfd.yaml` configures a 256 MiB metadata pool
  + 512 MiB rowgroup pool. The 1 M-row dataset (~40 MB on disk) fits
  comfortably; you'll see `shelf_disk_bytes_used = 0` because Foyer
  never spills. That's expected for this size, not a bug.

---

## Cleanup

```bash
bash run.sh --cleanup
```

Drops the MinIO Docker volume (`shelf-starrocks-example_minio-data`)
so re-runs start clean. The iceberg-rest sqlite is in-container, so
just stopping and restarting the stack is enough.

---

## License

Apache-2.0, same as the rest of the repo. See `../../LICENSE`.
