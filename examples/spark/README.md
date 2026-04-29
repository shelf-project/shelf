# Spark on Shelf — 5-minute walkthrough

This example shows **Apache Spark 3.5** reading an **Iceberg** table through
[Shelf](../../README.md)'s S3-compatibility shim. Every byte Spark fetches
goes via `shelfd:9092`; the second run of the same query is materially
faster because Shelf served the Parquet bytes from its Foyer DRAM cache
instead of round-tripping to MinIO.

```
  ┌────────────┐    s3.endpoint = http://shelfd:9092
  │   Spark    │──────────────────────────────────────┐
  │  (Iceberg  │  Iceberg metadata: http://iceberg-rest:8181
  │   S3FileIO)│                                       ▼
  └────┬───────┘                              ┌─────────────────┐
       │                                      │   iceberg-rest  │
       │  GET /warehouse/demo/events/...      │  (catalog only) │
       ▼                                      └─────────────────┘
  ┌────────────┐  miss → GET /warehouse/...   ┌─────────────────┐
  │   shelfd   │─────────────────────────────▶│      MinIO      │
  │ (S3 shim:  │◀─────────────────────────────│   (S3 origin)   │
  │  Foyer)    │  bytes (and ETag) cached     └─────────────────┘
  └────────────┘
```

## What you get

- A 1 M-row identity-partitioned Iceberg table (`lake.demo.events`,
  30 daily partition files) seeded into MinIO via PyIceberg + the
  [Iceberg REST catalog reference image](https://hub.docker.com/r/tabulario/iceberg-rest).
- A PySpark bench that runs the same aggregation twice: cold (Shelf
  empty → fetches from MinIO and populates Foyer DRAM) and warm
  (every byte served from Foyer). Both runs use a single SparkSession
  so JVM codegen is amortised across runs.
- A one-line summary: `cold=… | warm=… | speedup=… | $-saved=…`.

## Prerequisites

- Docker Desktop / Docker Engine ≥ 24 with Compose v2.
- ~6 GB of free RAM (Spark JVM + Foyer DRAM tier + MinIO buffer pool).
- Network egress to Docker Hub (`tabulario/spark-iceberg`,
  `tabulario/iceberg-rest`, `minio/minio`).

The example builds `shelfd` from this checkout's source via the
[smoke-harness Dockerfile](../../benchmarks/smoke/Dockerfile.shelfd) so
you don't have to wait on any GHCR image to be flipped public during
the OSS bootstrap window. To pin a published image instead:

```bash
SHELFD_IMAGE=ghcr.io/shelf-project/shelfd:0.1.0-preview-9 bash run.sh
```

(`0.1.0-preview-9` is the `appVersion` in
[`charts/shelf/Chart.yaml`](../../charts/shelf/Chart.yaml).)

## Run it

```bash
cd shelf/examples/spark
bash run.sh
```

You should see something like (real numbers from a `m1` MacBook,
Docker Desktop 28.3, on `2026-04-28`):

```
[run] bringing up MinIO + iceberg-rest + shelfd + seed...
[seed] generating 1,000,000 rows across 30 days...
[seed] wrote 1,000,000 rows to demo.events (identity-partitioned on event_date)
[run] waiting for shelfd /healthz...
[run] shelfd is healthy.
[run] running bench (cold + warm)...
[bench] building SparkSession (this can take ~10–15 s on a cold JVM)...
[bench] running warmup (JVM + Iceberg client bootstrap)...
[bench] forcing a cold cache...
[bench] cold cache assumed empty (fresh shelfd boot — there is no v0.5 'clear pool' admin endpoint)
[bench] cold run...
[bench] warm run...

========================================================================
  Spark → Shelf → MinIO   (Iceberg `lake.demo.events`, 1 M rows)
========================================================================
  cold:          2.11 s    shelf hits/misses:     1 /    45    origin:   4.03 MiB
  warm:          409 ms    shelf hits/misses:    46 /     0    origin:   0.00 MiB
  speedup:     5.2x
  $-saved:     $0.000373   (45 GETs + 4.03 MiB egress avoided)
========================================================================

cold=2.11s | warm=0.41s | speedup=5.2x | $-saved=$0.0004
```

Exact numbers vary (driver memory, host disk, MinIO throughput, JVM
warm state), but the **shape** is stable: warm has zero misses, zero
origin bytes, and runs ~4–10× faster than cold. The dollar-savings
are tiny here because the demo dataset is tiny (~30 MiB total
Parquet); on a production-shape table — multi-TB Iceberg + thousands
of partitions — a 70–85% hit ratio compounds into real money.

Tear down when you're done:

```bash
docker compose down -v
```

## Spark configuration that matters

Both Iceberg's own AWS S3 client (`S3FileIO`) **and** Hadoop's S3A
filesystem are wired at `shelfd:9092`. Iceberg uses `S3FileIO` for
data files; the `fs.s3a.*` keys cover any non-Iceberg `s3a://` reads
you might add later (e.g. a `read.parquet("s3a://warehouse/raw/…")`
from an exploratory notebook).

```python
spark = (
    SparkSession.builder
        # Iceberg `lake` catalog → REST endpoint → S3FileIO
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "rest")
        .config("spark.sql.catalog.lake.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lake.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.lake.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")

        # Iceberg AWS S3 properties — point Iceberg's bytes at shelfd
        .config("spark.sql.catalog.lake.s3.endpoint", "http://shelfd:9092")
        .config("spark.sql.catalog.lake.s3.path-style-access", "true")
        .config("spark.sql.catalog.lake.s3.access-key-id", "shelf-demo")
        .config("spark.sql.catalog.lake.s3.secret-access-key", "shelf-demo")
        .config("spark.sql.catalog.lake.client.region", "us-east-1")

        # Hadoop S3A — same target, anonymous credentials (the shim
        # is signature-agnostic, so AnonymousAWSCredentialsProvider
        # is the most honest choice; it sends NO Authorization
        # header at all).
        .config("spark.hadoop.fs.s3a.endpoint", "http://shelfd:9092")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
)
```

### Why `path-style.access=true`

Shelf's shim routes path-style addressing only — `GET
/<bucket>/<key>` — because virtual-hosted-style would require the
shim to reverse-proxy `<bucket>.shelfd:9092` DNS, which is
operationally fragile (and explicitly out of scope in
[`shelfd/src/s3_shim.rs`](../../shelfd/src/s3_shim.rs)). Spark's
default for `fs.s3a` is virtual-hosted-style, so `path.style.access=true`
is **mandatory** for both the Iceberg-AWS client and S3A.

### Why `AnonymousAWSCredentialsProvider` for S3A

The shim deliberately ignores SigV4 — no client needs real
credentials to reach it. Three credential-provider options work:

| Provider                                                       | Wire shape                  | Notes                                                   |
| -------------------------------------------------------------- | --------------------------- | ------------------------------------------------------- |
| `AnonymousAWSCredentialsProvider`                              | no `Authorization` header   | Cleanest; matches the shim's "no auth" contract.        |
| `SimpleAWSCredentialsProvider` with any non-empty key/secret   | SigV4-signed, header ignored | Fine; the shim discards the signature.                  |
| `EnvironmentVariableCredentialsProvider` with dummy env vars   | SigV4-signed, header ignored | Useful when other code in the JVM expects creds in env. |

We use `AnonymousAWSCredentialsProvider` per the
[Hadoop S3A authentication docs][s3a-auth].

[s3a-auth]: https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/tools/hadoop-aws/index.html#Authentication_properties

For the Iceberg `S3FileIO` path we still pass dummy `s3.access-key-id`
/ `s3.secret-access-key` because Iceberg's AWS SDK v2 client
constructs a SigV4 `Authorization` header up front before the shim
gets a chance to ignore it; AnonymousAWSCredentials at the SDK v2
level is harder to wire from `--conf` flags. Iceberg AWS S3
properties are catalogued in the [Iceberg AWS docs][iceberg-aws].

[iceberg-aws]: https://iceberg.apache.org/docs/latest/aws/

## Files in this directory

| Path                                | Purpose                                                                 |
| ----------------------------------- | ----------------------------------------------------------------------- |
| `docker-compose.yml`                | Brings up MinIO, iceberg-rest, shelfd (built locally), seed, bench.     |
| `config/shelfd/shelfd.yaml`         | Single-pod shelfd config: 256 MiB metadata + 512 MiB rowgroup DRAM.     |
| `init/seed.sh` / `seed_iceberg.py`  | One-shot PyIceberg writer that seeds `lake.demo.events`.                |
| `spark-warmup.sql`                  | JVM + Iceberg client warmup before timing.                              |
| `spark-bench.sql`                   | Cold/warm bench query (15-day aggregation).                             |
| `bench/run-bench.sh` / `bench.py`   | PySpark driver: warmup → evict shelfd → cold → warm → summary.          |
| `run.sh`                            | One command end-to-end runner.                                          |

## Manual queries (interactive)

If you want to drive Spark by hand instead of running the bench
harness, leave the stack up after `run.sh` and exec in:

```bash
docker compose run --rm bench bash
# inside the container:
spark-sql \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=rest \
  --conf spark.sql.catalog.lake.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.lake.warehouse=s3://warehouse/ \
  --conf spark.sql.catalog.lake.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.lake.s3.endpoint=http://shelfd:9092 \
  --conf spark.sql.catalog.lake.s3.path-style-access=true \
  --conf spark.sql.catalog.lake.s3.access-key-id=shelf-demo \
  --conf spark.sql.catalog.lake.s3.secret-access-key=shelf-demo \
  --conf spark.sql.catalog.lake.client.region=us-east-1 \
  -f /work/spark-bench.sql
```

`shelfd`'s admin endpoints are exposed on the host:

```bash
curl -s http://127.0.0.1:9590/metrics | grep -E '^shelf_(hits|misses|origin)_total'
curl -X POST 'http://127.0.0.1:9590/admin/evict?pool=rowgroup'
```

## Troubleshooting

- **`docker pull ghcr.io/shelf-project/shelfd:…` says `denied`** — the
  GHCR package is private until the OSS launch flips it public (see
  the launch playbook); the example builds shelfd from this repo by
  default, so you don't need the GHCR image to run it.
- **`UnknownHostException: rest`** during the first `lake.demo.events`
  query — the `tabulario/spark-iceberg` image ships a pre-baked
  `spark-defaults.conf` whose `spark.sql.defaultCatalog` is `demo`
  (pointing at `http://rest:8181`). Spark's analyzer eagerly
  initialises the default catalog on the first query, so an
  unrelated `lake.demo.events` query still tries to fetch
  `http://rest:8181/v1/config`. The bench fixes this with
  `spark.sql.defaultCatalog=lake`; if you write your own driver,
  override the same config or the analyzer will trip the same
  error.
- **`bench` container fails with `OutOfMemoryError`** — bump
  `spark.driver.memory` in `bench/bench.py` (the default 1g is
  enough for the 1 M-row demo, but a larger seed needs more).
- **Warm time ≈ cold time** — Foyer pools may be too small. Check
  `config/shelfd/shelfd.yaml`: `pools.metadata.dram_bytes` and
  `pools.rowgroup.dram_bytes`. The full warm working set for this
  demo is ~30 MiB of Parquet so 256 MiB / 512 MiB is comfortable.
