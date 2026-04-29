# Validation evidence

`bash run.sh` produced real numbers on a clean run on 2026-04-28. Two
verification gates the spec asked for:

## 1. `docker compose config -q`

```
$ cd shelf/examples/pyiceberg
$ docker compose config -q && echo OK
OK
```

## 2. `bash run.sh` end-to-end

Working tree: `shelf-23-peer-fetch` branch, shelfd built from local source
(`v0.1.0-preview-9`), `shelf/examples/pyiceberg/Dockerfile` reused via
`benchmarks/smoke/Dockerfile.shelfd` build context.

Two consecutive cold runs (separate `down -v` between them) produced
matching numbers — the cache effect is reproducible, not a one-off.

### Run A (manual `compose exec` after `KEEP_UP=1 bash run.sh`)

```
[bench] table=demo.events filter="date = '2024-01-15'" endpoint=http://shelfd:9092
[cold] rows= 10000  cols=5  elapsed=   64.3 ms  shelf_hits+=0  shelf_misses+=10
[warm] rows= 10000  cols=5  elapsed=   19.0 ms  shelf_hits+=10  shelf_misses+=0
[bench] summary: cold=64.3 ms  warm=19.0 ms  speedup=3.39x  hit_delta_cold=0  hit_delta_warm=10
```

### Run B (full `bash run.sh` with auto-teardown)

```
[run] starting stack (will build shelfd from source on first run only)
 ... (compose graph healthy, seed exited 0, runner healthy)
[run] running bench.py inside the runner container
[bench] table=demo.events filter="date = '2024-01-15'" endpoint=http://shelfd:9092
[cold] rows= 10000  cols=5  elapsed=   56.9 ms  shelf_hits+=0  shelf_misses+=10
[warm] rows= 10000  cols=5  elapsed=   16.8 ms  shelf_hits+=10  shelf_misses+=0
[bench] summary: cold=56.9 ms  warm=16.8 ms  speedup=3.39x  hit_delta_cold=0  hit_delta_warm=10
[run] done
[run] tearing down stack
```

## What the numbers prove

* Row count is identical cold vs warm (`10000` rows on both runs) — the
  same logical query, served twice. The shim is not silently dropping or
  re-ordering bytes.
* Cold: **10 misses, 0 hits**. Every Parquet/Iceberg metadata read goes
  through Shelf's S3 shim on `:9092`, lands in Foyer's metadata + rowgroup
  pools, and the underlying data is fetched once from MinIO.
* Warm: **0 misses, 10 hits**. Every read in the second scan is served
  from Foyer DRAM.
* Path-style works without any explicit `s3.path-style-access` setting —
  PyIceberg's PyArrow-backed `S3FileIO` defaults to path-style addressing
  (verified against `pyiceberg/io/pyarrow.py::_initialize_s3_fs` on main
  and 0.7.1).

## Environment

| Component        | Version                                                    |
|------------------|------------------------------------------------------------|
| docker / compose | Docker 28.3.0                                              |
| shelfd           | `v0.1.0-preview-9`, built locally (release profile, arm64) |
| MinIO            | `RELEASE.2024-12-13T22-19-12Z`                             |
| iceberg-rest     | `tabulario/iceberg-rest:1.6.0`                             |
| Python           | `python:3.11-slim`                                         |
| pyiceberg        | `0.7.1[pyarrow,s3fs]`                                      |
| host             | macOS 24.6.0, Apple silicon                                |

## Reproducing

```bash
cd shelf/examples/pyiceberg
bash run.sh
```

First run rebuilds shelfd from source (~3–5 min on a cold cargo cache).
Subsequent runs reuse the local image and finish in ~30 s including
seed.
