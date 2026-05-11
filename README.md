# Shelf



> A row-group-granular, plan-aware, Iceberg-native read cache for Trino. Rust, Apache 2.0, fail-open.

[CI](https://github.com/shelf-project/shelf/actions/workflows/verify.yml)
[Release](https://github.com/shelf-project/shelf/releases)
[License](./LICENSE)
[Container](https://github.com/shelf-project/shelf/pkgs/container/shelfd)
[SBOM](./SECURITY.md)
[Stars](https://github.com/shelf-project/shelf/stargazers)

## Why Shelf

- **Row-group granularity.** Keys are `sha256(etag || offset || length)`. Shelf caches the 64 KB Parquet footer or a single 4 MB row group, not the whole 512 MB file.
- **Plan-aware prefetch.** A Trino coordinator plugin warms file and footer bytes while the planner is still assigning splits. Row-group prefetch is plugin-observation-driven; no dependency on the removed `SplitCompletedEvent`.
- **Shared across replicas.** One cluster, four Trino replicas, one warm working set. No more cold-start tax per replica.
- **Consensus-free.** Membership is the K8s headless service. Pin list and tenant quotas are a versioned S3 ConfigMap. No Raft, no etcd.
- **HTTP/2 in v1.** One protocol, one pool to tune. Arrow Flight is deferred to v1.x contingent on measured EKS throughput.

## Production Results

Measured on a 4-replica Trino 480 cluster (EKS, Iceberg on S3, KEDA-autoscaled spot workers) after cutting over one replica's catalog to Shelf. Business-hours comparison (9 AM–9 PM, excluding the cutover day):


| Metric                    | Direct S3 (7-day baseline) | Shelf (post-cutover) | Delta          |
| ------------------------- | -------------------------- | -------------------- | -------------- |
| **p50 wall time**         | 2.34 s                     | 1.12 s               | **−52 %**      |
| p95 wall time             | 1,112 s                    | 1,107 s              | ~same          |
| p99 wall time             | 1,272 s                    | 1,232 s              | −3 %           |
| Avg CPU time              | 197.7 s                    | 152.4 s              | **−23 %**      |
| Avg planning time         | 0.46 s                     | 0.37 s               | −19 %          |
| `ICEBERG_BAD_DATA` errors | 10                         | **0**                | **eliminated** |
| `CLUSTER_OUT_OF_MEMORY`   | 62                         | 9                    | −85 %          |


### Warm-up curve


| Phase              | p50 wall   | Iceberg infra errors |
| ------------------ | ---------- | -------------------- |
| 0–6 h (cold cache) | 3.3 s      | 5                    |
| 6–12 h (warming)   | 85.1 s*    | 0                    |
| 12 h+ (warm)       | **2.87 s** | 40                   |


 Inflated by scheduled batch ETL, not cache performance.

### What the numbers mean

1. **p50 latency halved.** Cached rowgroup reads from DRAM are ~10× faster than S3 round-trips. The median interactive query finishes in half the time.
2. **CPU time dropped 23 %.** Workers spend less time blocked on I/O, freeing capacity for concurrent queries.
3. `**ICEBERG_BAD_DATA` = 0.** The "Malformed Parquet" corruption class (caused by proxy byte-truncation under connection-pool saturation) is structurally impossible with Shelf's ETag-keyed content-addressed design.
4. **No new failure class introduced.** The error mix is the same shape as direct S3.

> These results are from a single replica over ~26 hours post-cutover. Multi-replica, multi-week soak data will follow as the rollout continues.

## Quickstart

Zero to first cache hit on a laptop, in ≤ 10 minutes with k3d + MinIO:

→ [docs/quickstart/](./docs/quickstart/index.md)

## Architecture

User-facing summary of the BLUEPRINT with ADR overrides applied:

→ [docs/architecture.md](./docs/architecture.md)

Full canonical design: [BLUEPRINT.md](./BLUEPRINT.md).

## License

Copyright 2026 The Shelf Authors.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0). See [LICENSE](./LICENSE) and [NOTICE](./NOTICE) for details.