# Polars + Shelf

A 5-minute, runnable example of Polars reading an Iceberg table
through Shelf's S3-compatible read shim. Everything runs in Docker
on your laptop — no cluster, no real S3.

```
                ┌──────────────┐
   bench.py ──► │ shelfd :9092 │ ──► MinIO :9000
   (Polars +    │  S3 shim     │     (Iceberg
    PyIceberg)  │  (SHELF-22)  │      warehouse)
                └──────────────┘
                       │
                       ▼
                 DRAM + NVMe cache
                 (Foyer hybrid)
```

The first run is **cold** — shelfd forwards every Parquet byte-range
to MinIO. The second run is **warm** — Foyer answers from DRAM /
local NVMe and the shim never touches MinIO.

## What you'll see

```
[bench] cold:    1238.4 ms   (40 groups)
[bench] warm:     142.7 ms   (40 groups)

shelfd cold→warm speedup: 8.68x
summary: cold: 1.24s | warm: 143ms
```

Numbers vary with laptop spec, Docker file-system flavour, and
network I/O on the cold pull. The point isn't the absolute
latency — it's that the second run skips the origin entirely and
returns the same rows.

## Prerequisites

- Docker Desktop ≥ 4.30 (or any Compose v2 setup)
- ~3 GB free disk for the runner image build + MinIO data
- Outbound network for the first build (MinIO, distroless, pip)

## Run

```bash
cd shelf/examples/polars
bash run.sh
```

`run.sh` does the following:

1. `docker compose up -d minio minio-setup shelfd` — boots MinIO,
   creates bucket `warehouse`, starts shelfd.
2. Waits for `http://127.0.0.1:9090/healthz`.
3. Seeds an Iceberg table `demo.events` (~200 000 rows) into
   `s3://warehouse/demo/events/` via PyIceberg's SqlCatalog.
4. Runs `bench.py` twice (cold + warm) and prints a summary.

When you're done:

```bash
docker compose down -v
```

## How the storage_options dict works

Polars' `pl.scan_iceberg(...)` accepts a `storage_options` dict that
is forwarded to PyIceberg's FileIO layer. The keys come from the
[PyIceberg FileIO docs][pyiceberg-fileio], **not** the Polars /
`object_store` keys you'd use with `pl.scan_parquet`.

```python
storage_options = {
    "s3.endpoint":          "http://shelfd:9092",
    "s3.access-key-id":     "minioadmin",       # dummy works too —
    "s3.secret-access-key": "minioadmin",       # the shim ignores
    "s3.region":            "us-east-1",        # SigV4 auth headers.
}
pl.scan_iceberg(metadata_path, storage_options=storage_options)
```

A few things worth knowing:

| Key | Why it's set the way it is |
| --- | --- |
| `s3.endpoint` | Points at the SHELF-22 read shim (`shelfd:9092`). Every metadata + data file read flows through here. |
| `s3.access-key-id` / `s3.secret-access-key` | Required by the AWS SDK for SigV4 signing. The shim is signature-agnostic, so the values themselves don't matter — they just have to exist. |
| `s3.region` | Same — required by the SDK; not interpreted by the shim. |
| `s3.force-virtual-addressing` | Not set. Defaults to `False`, which means PyIceberg's pyarrow-based FileIO uses **path-style** addressing whenever `s3.endpoint` is set. The shim only speaks path-style. |

`bench.py` also passes `reader_override="pyiceberg"` so PyIceberg's
`PyArrowFileIO` does the I/O. The default `native` reader uses
Polars' Rust `object_store` binding, which expects a different (and
internally-translated) key scheme. Pinning to `pyiceberg` keeps this
example honest about exactly which keys the shim sees on the wire.

## Files

| File | What it does |
| --- | --- |
| `docker-compose.yml` | MinIO + shelfd + a profile-gated `runner` Python container. |
| `config/shelfd.yaml` | Single-pod shelfd config — DRAM 256 MiB, NVMe 512 MiB, pin list disabled. |
| `Dockerfile.runner` | `python:3.11-slim` + `polars` + `pyiceberg[pyarrow,sql-sqlite,s3fs]`. |
| `init/seed.py` | Generates 200 k synthetic event rows, writes them as Iceberg via PyIceberg's `SqlCatalog`, talks to MinIO directly (cold path is bench-only). |
| `bench.py` | Loads the metadata path written by `seed.py`, runs the lazy chain twice through shelfd, prints `cold: Xs \| warm: Yms`. |
| `run.sh` | Orchestrates compose-up → seed → bench → summary. |

## shelfd image

`docker-compose.yml` builds shelfd from the repo's production
`shelfd/Dockerfile` by default. The first build takes ~5–10 minutes
(Rust release build + distroless runtime); subsequent builds are
~30 seconds with warm layer cache.

The published OSS image lives at:

```
ghcr.io/shelf-project/shelfd:0.1.0-preview-9      # appVersion in charts/shelf/Chart.yaml
```

Once that image is public you can skip the local build with:

```bash
SHELFD_IMAGE=ghcr.io/shelf-project/shelfd:0.1.0-preview-9 bash run.sh
```

## Tweaks

- Bigger seed → bigger cold/warm delta. Set `SEED_ROWS=2000000`
  before `bash run.sh`.
- Drop the warm cache between runs:
  `docker compose restart shelfd`.
- Inspect cache state at any point:
  `curl -s http://127.0.0.1:29090/stats | jq`
  or `curl -s http://127.0.0.1:29090/metrics | grep shelf_`.
  (Host ports are 29000/29001 for MinIO, 29090/29092 for shelfd
  data-plane / shim; bumped off the defaults so this example
  can run alongside other Shelf example stacks on the same host.)

## Troubleshooting

- **`shelfd did not become ready in 60s`** — first build is slow;
  re-run `bash run.sh` once `docker compose build shelfd` finishes.
  Tail logs with `docker compose logs -f shelfd`.
- **`pyarrow.lib.ArrowIOError: ... NoSuchBucket`** — bucket
  `warehouse` wasn't created. The `minio-setup` one-shot service
  must complete successfully; `docker compose logs minio-setup`
  shows whether it did.
- **Cold run is slower than expected** — first cold run also pays
  Docker network setup + bucket-list latency. The cold→warm ratio
  is the meaningful number, not the absolute cold time.

[pyiceberg-fileio]: https://py.iceberg.apache.org/configuration/#fileio
