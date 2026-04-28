# PyIceberg + Shelf — reference example

A 5-minute, self-contained walkthrough that runs PyIceberg against an
Iceberg table on MinIO with **Shelf's S3 shim in front of every read**.

PyIceberg is the Iceberg spec author's own client, so this is the
cleanest possible illustration of how the Shelf shim plugs in: it's a
single catalog property change, no source patches, no plugin install.

## What you get

* **MinIO** — S3-compatible origin, holds the Iceberg warehouse.
* **iceberg-rest** — `tabulario/iceberg-rest:1.6.0`, REST catalog backed by MinIO.
* **shelfd** — Shelf cache daemon, serves the S3 shim on `:9092`, signature-agnostic.
* **seed** — one-shot Python job that creates `demo.events` (~70k rows, 7 daily partitions).
* **runner** — `python:3.11-slim` with `pyiceberg[pyarrow,s3fs]==0.7.1`.

## How the wiring works

PyIceberg 0.7+ reads through PyArrow's `S3FileSystem`. When the catalog
is loaded with `s3.endpoint`, `s3.access-key-id`, `s3.secret-access-key`,
and `s3.region`, PyIceberg forwards every one of those into
`pyarrow.fs.S3FileSystem(...)` inside
`pyiceberg.io.pyarrow.PyArrowFileIO._initialize_s3_fs`. Set `s3.endpoint`
to shelfd and every manifest, metadata, and Parquet read transits the
shim.

Property surface:

| Property                        | Value                       | Why                                                                                         |
|---------------------------------|-----------------------------|---------------------------------------------------------------------------------------------|
| `s3.endpoint`                   | `http://shelfd:9092`        | Routes all S3 traffic through the Shelf shim.                                               |
| `s3.force-virtual-addressing`   | `"false"`                   | Path-style is mandatory; the shim does not resolve bucket-as-DNS-host.                      |
| `s3.access-key-id`              | any non-empty string        | Shim ignores SigV4; pass anything.                                                          |
| `s3.secret-access-key`          | any non-empty string        | Same.                                                                                       |
| `s3.region`                     | `us-east-1`                 | Required by PyArrow; arbitrary value is fine since the shim doesn't care.                   |

> **Naming gotcha:** Java Iceberg uses `s3.path-style-access`. PyIceberg
> uses `s3.force-virtual-addressing` (note: the *inverse*). PyIceberg
> *silently ignores* `s3.path-style-access` — verified against
> [`pyiceberg/io/pyarrow.py` `_initialize_s3_fs`](https://github.com/apache/iceberg-python/blob/main/pyiceberg/io/pyarrow.py).
> The default is already path-style, so the misnamed property "works"
> by accident in many setups; we set the correct one to be explicit.

The seed job points at MinIO directly (`s3.endpoint=http://minio:9000`)
because we want writes to land at the origin without going through
Shelf's read path. The bench job points at `http://shelfd:9092` — that's
the whole demo.

## Run it

```bash
cd shelf/examples/pyiceberg
bash run.sh
```

First run builds the shelfd image from source (~3–5 min cold). Subsequent
runs reuse the layer cache and finish in ~30 s plus the cold/warm bench.

Skip the local build by exporting a published image:

```bash
SHELFD_IMAGE=ghcr.io/<owner>/shelfd:<tag> bash run.sh
```

Keep the stack running for ad-hoc poking (`docker compose exec runner …`,
`curl 127.0.0.1:27090/metrics`, MinIO console at <http://127.0.0.1:27201>):

```bash
KEEP_UP=1 bash run.sh
docker compose -f shelf/examples/pyiceberg/docker-compose.yml down -v
```

## Expected output

A real run on 2026-04-28 (Apple silicon, shelfd built locally):

```
[bench] table=demo.events filter="date = '2024-01-15'" endpoint=http://shelfd:9092
[cold] rows= 10000  cols=5  elapsed=   56.9 ms  shelf_hits+=0   shelf_misses+=10
[warm] rows= 10000  cols=5  elapsed=   16.8 ms  shelf_hits+=10  shelf_misses+=0
[bench] summary: cold=56.9 ms  warm=16.8 ms  speedup=3.39x  hit_delta_cold=0  hit_delta_warm=10
```

A healthy run shows:

* `rows=10000` on both runs (the `2024-01-15` slice is 1/7 of the seeded data).
* Warm run faster than cold (3–5× on this footprint).
* `shelf_misses+` materially higher on the cold run, `shelf_hits+` materially higher on the warm run.

See [`VALIDATION_NOTES.md`](./VALIDATION_NOTES.md) for environment details and the second confirming run.

## What this example does *not* do

* No NVMe — the cache is DRAM-only (256 MiB metadata pool + 256 MiB rowgroup pool).
* No multi-pod membership, no peer fetch, no admission tuning.
* No correctness diff against direct-S3. PyIceberg writes Parquet with
  ETags, Shelf's keys are content-addressed by ETag, so a re-seed
  produces a fresh keyspace automatically; there's no stale-cache failure
  mode to defend against in this scope.

For a more realistic stack (multi-pod, peer fetch, NVMe spill, real
catalog), see `shelf/benchmarks/smoke/` and `shelf/charts/shelf/`.

## Files

| File                   | Purpose                                              |
|------------------------|------------------------------------------------------|
| `docker-compose.yml`   | Defines the 6 services (minio, minio-setup, iceberg-rest, shelfd, seed, runner). |
| `shelfd.yaml`          | shelfd config: DRAM-only, listens on `:9090` (data) and `:9092` (shim). |
| `requirements.txt`     | `pyiceberg[pyarrow,s3fs]==0.7.1`, pandas, requests.  |
| `init/seed.py`         | Creates `demo.events` (partitioned by `date`) via PyIceberg, writes through MinIO directly. |
| `bench.py`             | Loads the catalog with `s3.endpoint=http://shelfd:9092`, scans `date='2024-01-15'` twice, scrapes `shelf_hits_total` / `shelf_misses_total` deltas. |
| `run.sh`               | Orchestrator: `compose config -q`, `up -d --wait`, `exec runner python bench.py`, teardown. |
| `VALIDATION_NOTES.md`  | Captured output from a working end-to-end run on 2026-04-28.        |
