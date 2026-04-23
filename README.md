# Shelf

> A row-group-granular, plan-aware, Iceberg-native read cache for Trino. Rust, Apache 2.0, fail-open.

[![CI](https://img.shields.io/badge/CI-pending-lightgrey)](./docs/quickstart/index.md)
[![Release](https://img.shields.io/badge/release-v0.0.1--pre-blue)](./docs/changelog.md)
[![License](https://img.shields.io/badge/license-Apache--2.0-green)](./LICENSE)
[![SBOM](https://img.shields.io/badge/SBOM-pending-lightgrey)](./SECURITY.md)
[![Stars](https://img.shields.io/badge/stars-0-lightgrey)](./README.md)

## Why Shelf

- **Row-group granularity.** Keys are `sha256(etag || offset || length)`. Shelf caches the 64 KB Parquet footer or a single 4 MB row group, not the whole 512 MB file.
- **Plan-aware prefetch.** A Trino coordinator plugin warms file and footer bytes while the planner is still assigning splits. Row-group prefetch is plugin-observation-driven; no dependency on the removed `SplitCompletedEvent`.
- **Shared across replicas.** One cluster, four Trino replicas, one warm working set. No more cold-start tax per replica.
- **Consensus-free.** Membership is the K8s headless service. Pin list and tenant quotas are a versioned S3 ConfigMap. No Raft, no etcd.
- **HTTP/2 in v1.** One protocol, one pool to tune. Arrow Flight is deferred to v1.x contingent on measured EKS throughput.

## Quickstart

Zero to first cache hit on a laptop, in ≤ 10 minutes with k3d + MinIO:

→ [docs/quickstart/](./docs/quickstart/index.md)

## Architecture

User-facing summary of the BLUEPRINT with ADR overrides applied:

→ [docs/architecture.md](./docs/architecture.md)

Full canonical design: [BLUEPRINT.md](./BLUEPRINT.md).

## License

Copyright 2026 The Shelf Authors.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. You may obtain a copy of the License at <http://www.apache.org/licenses/LICENSE-2.0>. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE) for details.
