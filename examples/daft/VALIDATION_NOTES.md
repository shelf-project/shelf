# Validation notes

## What I ran

```
cd shelf/examples/daft
docker compose -f docker-compose.yml config --quiet   # passed
docker compose build runner                            # ~60s, daft 0.7.5 + pyiceberg 0.10.0 wheels installed
docker compose build shelfd                            # ~5m, Rust release build of shelfd v0.1.0-preview-9
docker compose up -d minio iceberg-rest shelfd
docker compose run --rm seed                           # 50_000 rows of default.orders
docker compose run --rm runner                         # bench.py — cold + warm
```

## Live result (Apr 2026, MacBook arm64, Docker Desktop 28.3)

```
=== Daft + Shelf example results ===
run    elapsed (s)    shelf hits   shelf misses
cold   0.527          0            3
warm   0.060          3            0

warm/cold speedup: 8.83x
```

The `shelf_hits_total` / `shelf_misses_total` deltas are scraped from
shelfd's `/metrics` Prometheus endpoint immediately before and after
each run. 3 misses on the cold run → 3 hits on the warm run is the
expected pattern: shelfd is caching the byte ranges Daft asked for
(metadata.json, manifest, Parquet footer) and serving them out of
Foyer DRAM the second time.

## What broke during validation

1. **`S3Config.verify_ssl` is gone in Daft 0.7.x.**
   Original draft used `verify_ssl=False` per the user prompt; the
   live 0.7.5 binary panics with
   `not implemented: Setting S3Config.verify_ssl is no longer supported`.
   See [Eventual-Inc/Daft#4530](https://github.com/Eventual-Inc/Daft/issues/4530)
   — the field went away with the rustls/AWS-LC switch. Fix: drop it.
   `use_ssl=False` is enough because no TLS is negotiated to begin
   with.

2. **PyIceberg + iceberg-rest happy path.** No surprises — the same
   pattern as `benchmarks/smoke/seed/seed_iceberg.py` works for Daft.

## What I did NOT change

- Anything outside `shelf/examples/daft/`. Confirmed via
  `git status` from the repo root before tearing down.

## Tear-down

```
docker compose -f shelf/examples/daft/docker-compose.yml down -v
```
