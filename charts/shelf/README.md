# shelf — Helm chart

Scaffolding chart for the Shelf cache (BLUEPRINT §9, plan Phase 5).
**v0.1.0 — not production-tuned.** Resource, NVMe, and pool sizes are
placeholders until Phase 0 benchmarks E3 / E10 land.

## Prerequisites

- Kubernetes ≥ 1.27 (we rely on `policy/v1` PDB and the headless
  service membership flow in ADR-0001).
- `kube-prometheus-stack` installed in the cluster (for the
  `ServiceMonitor` CRD). Disable with `serviceMonitor.enabled=false`
  if not applicable.
- `local-nvme` StorageClass on rep-2 nodes, or fall back to
  `ebs-gp3-wffc` in the staging overlay.
- IRSA-ready IAM role for S3 read-only access to the origin bucket.

## Install

```bash
helm upgrade --install shelf charts/shelf \
  --namespace shelf --create-namespace \
  -f charts/shelf/values-prod.yaml
```

Dev (kind cluster with MinIO):

```bash
helm upgrade --install shelf charts/shelf \
  --namespace shelf --create-namespace \
  -f charts/shelf/values-dev.yaml
```

## Upgrade

```bash
helm diff upgrade shelf charts/shelf -f charts/shelf/values-prod.yaml
helm upgrade shelf charts/shelf -f charts/shelf/values-prod.yaml
```

Rolling upgrade strategy is `OrderedReady` — one pod at a time so
per-pod circuit breakers on Trino workers absorb the disruption.

## Uninstall

```bash
helm uninstall shelf --namespace shelf
kubectl -n shelf delete pvc -l app.kubernetes.io/instance=shelf
```

PVCs are preserved by default across `helm uninstall` (StatefulSet
behaviour). Delete explicitly if reclaiming NVMe.

## Lint / validate

```bash
helm lint charts/shelf -f charts/shelf/values-dev.yaml
helm lint charts/shelf -f charts/shelf/values-staging.yaml
helm lint charts/shelf -f charts/shelf/values-prod.yaml
helm lint charts/shelf -f charts/shelf/ci/lint-values.yaml --strict
```

CI strict-lints `ci/lint-values.yaml` on every PR and merges each
`charts/shelf/examples/*.yaml` on top for the latency-first NVMe overlay
(`values-latency-priority.yaml`).

## Configuration quick-reference

The full matrix of values lives in `values.yaml` with inline citations.
Highlights:

| Key                                    | Default              | Citation             |
| -------------------------------------- | -------------------- | -------------------- |
| `replicaCount`                         | 3                    | plan §3 Phase 1      |
| `service.dataPort`                     | 9090                 | ADR-0004 HTTP/2 only |
| `cache.pools.metadata.sizeBytes`       | 5 GiB                | ADR-0008             |
| `cache.pools.rowgroup.dramSizeBytes`   | 11 GiB               | values.yaml          |
| `cache.pools.rowgroup.nvmeSizeBytes`   | 60 GiB               | ADR-0042 (K1); use `examples/values-latency-priority.yaml` for 500Gi |
| `cache.admission.sizeThresholdMiB`     | 1024                 | ADR-0003             |
| `cache.admission.model.enabled`        | false                | ADR-0003             |
| `storage.storageClassName`             | local-nvme           | plan §4 SHELF-18     |
| `podDisruptionBudget.maxUnavailable`   | 1                    | agents/8-operator    |

## Runbooks + dashboards

- Dashboards: `observability/dashboards/`
- Alerts:     `observability/alerts/`
- Runbooks:   `runbooks/`
- SLO doc:    `docs/SLO.md`
- Capacity:   `docs/capacity.md`
- On-call:    `docs/oncall.md`

## Intentional omissions

- **No Raft sidecar, port, or ConfigMap.** ADR-0001 removed embedded
  Raft; membership is the headless service, pin list is an S3 JSON.
- **No Arrow Flight port.** ADR-0004: HTTP/2 only in v1.
- **No ONNX inference container.** ADR-0003: size-threshold admission
  in v1; `cache.admission.model.enabled=false`.
- **No `shelf-result-cache` Deployment.** ADR-0006 punts result caching
  to the existing Redis-Gateway plugin.
