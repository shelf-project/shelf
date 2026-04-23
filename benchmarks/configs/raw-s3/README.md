# `raw-s3/` — no cache, the lower bound

Baseline: Trino reads Iceberg files directly from S3 with no caching
layer. Establishes the "zero" on every benchmark.

## Trino config (sketch)

```properties
# configs/raw-s3/trino-catalog-iceberg.properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
iceberg.metadata-cache.enabled=false
fs.cache.enabled=false
# no plugin; no shelf; no alluxio
```

## Notes

- `iceberg.metadata-cache.enabled=false` is deliberate — we want the
  raw-S3 floor, not a Trino-internal partial cache.
- HMS is reachable on `hms.hms.svc.cluster.local:9083` per `bootstrap.sh`.
- Expect p99s on TPC-DS @ 1 TB in the multi-second range; this is a
  *floor*, not a target.

## TODO_SHELF-26

- Pin AWS SDK request IDs into result records for post-hoc
  correlation with CloudWatch `AllRequests` metrics.
- Record S3 bytes-scanned via Trino `QueryStatistics`.
