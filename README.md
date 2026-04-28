# Shelf

<p align="center">
  <img src="docs/brand/shelf-logo.png" alt="Shelf" width="320" />
</p>

> A row-group-granular, plan-aware, Iceberg-native read cache for Trino. Rust, Apache 2.0, fail-open.

[![CI](https://github.com/shelf-project/shelf/actions/workflows/verify.yml/badge.svg?branch=main)](https://github.com/shelf-project/shelf/actions/workflows/verify.yml)
[![Release](https://img.shields.io/github/v/release/shelf-project/shelf?include_prereleases&sort=semver)](https://github.com/shelf-project/shelf/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-green)](./LICENSE)
[![Container](https://ghcr-badge.egpl.dev/shelf-project/shelfd/latest_tag?trim=major&label=ghcr.io%2Fshelf-project%2Fshelfd)](https://github.com/shelf-project/shelf/pkgs/container/shelfd)
[![SBOM](https://img.shields.io/badge/SBOM-syft%20%2B%20cosign-blue)](./SECURITY.md)
[![Stars](https://img.shields.io/github/stars/shelf-project/shelf?style=flat)](https://github.com/shelf-project/shelf/stargazers)

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
