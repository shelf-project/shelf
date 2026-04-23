# `fs-cache/` — Trino `fs.cache` baseline

Trino's built-in worker-local filesystem cache, tuned per our rep-0
production values (see `shelf/infra/trino/fs-cache-values.yaml` in
the parent repo).

## Key Trino properties

```properties
# configs/fs-cache/trino-catalog-iceberg.properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
iceberg.metadata-cache.enabled=false   # mutually exclusive with fs.cache
fs.cache.enabled=true
fs.cache.directories=/mnt/trino-cache
fs.cache.max-sizes=500GB
```

## Volume requirement

- `hostPath` (NOT `emptyDir`) — Phase −1 migration is a prerequisite,
  per plan §3 Phase −1. The bench harness enforces this via a
  `volumeClaimTemplate` backed by `local-nvme` StorageClass.

## Metrics of interest (feed result JSON)

- `trino_fscache_hits_total`
- `trino_fscache_misses_total`
- `trino_fscache_bytes_admitted_total`

## Known gotcha

`fs.cache` is *per-worker*, so a 2 → 20 scale-up in `cold-start/` means
18 freshly-empty caches. That is the effect the cold-start benchmark
is designed to expose.

## TODO_SHELF-26

- Align cache eviction policy (SIEVE / LRU) with what rep-0 actually
  uses in prod, so we are comparing like for like.
