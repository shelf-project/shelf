# `shelf/` — the Shelf backend config

Values, Trino plugin config, and driver overlay for running Shelf as
the cache backend. This is the *contract* we ship; the actual Helm
chart lives under `shelf/charts/shelf/` (see SHELF-09, SHELF-21).

## Files that will live here (scaffold)

| File                    | Purpose                                               |
| ----------------------- | ----------------------------------------------------- |
| `shelf-values.yaml`     | Helm values for the 3-pod StatefulSet.                |
| `trino-values.yaml`     | Trino Helm values wiring the `shelf-trino-plugin`.    |
| `trino-catalog-iceberg.properties` | Iceberg catalog with `fs.shelf.enabled=true`. |
| `minio-values.yaml`     | Optional in-cluster MinIO if no external bucket.      |
| `tpcds-loader-job.yaml` | Kubernetes Job that generates the TPC-DS fixture.     |
| `driver.yaml`           | Bench driver Pod spec.                                |

All placeholders today. Files land as SHELF-09 / SHELF-21 / SHELF-26
ship.

## Required Trino properties (sketch)

```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
iceberg.metadata-cache.enabled=false
fs.cache.enabled=false

fs.shelf.enabled=true
fs.shelf.endpoint=http://shelf.shelf.svc.cluster.local:9090
fs.shelf.footer.prefetch.kib=64
fs.shelf.admission.size_threshold_mib=1024
fs.shelf.circuit_breaker.failure_threshold=5
fs.shelf.circuit_breaker.open_window_ms=10000
```

## Shelf Helm values (sketch)

```yaml
replicaCount: 3
image:
  repository: ghcr.io/penpencil-oss/shelfd
  tag: scaffold
  pullPolicy: IfNotPresent

resources:
  requests: { cpu: "4", memory: "32Gi" }
  limits:   { cpu: "8", memory: "64Gi" }

persistence:
  enabled: true
  storageClass: local-nvme
  size: 500Gi

nodeSelector:
  role: shelf

tolerations:
  - key: role
    operator: Equal
    value: shelf
    effect: NoSchedule

pools:
  metadata:
    type: dram
    size: 5Gi
    policy: frozen-hot
  rowgroup:
    type: hybrid
    dramSize: 16Gi
    nvmeSize: 500Gi
    policy: s3-fifo   # ADR-0009

hashRing:
  mode: hrw          # ADR-0002
  serviceDns: shelf.shelf.svc.cluster.local
  refreshSeconds: 5

admission:
  sizeThresholdMiB: 1024   # ADR-0003
  pinListSource: s3://shelf-config/pin_list.json
```

## Reproducibility note

The Shelf image tag and the plugin JAR checksum are recorded in every
result record's `config` field (see each benchmark's `schema.json`).
Bumping either without bumping `release_tag` in `RESULTS.md` is a bug.

## TODO_SHELF-09 / SHELF-21 / SHELF-27

- Concrete `shelf-values.yaml` with NVMe StorageClass parameters.
- Plugin jar versioning aligned with Trino image builds.
- Grafana dashboard JSON imported automatically (sidecar).
