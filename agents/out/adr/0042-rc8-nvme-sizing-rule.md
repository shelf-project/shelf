# ADR-0042: OSS NVMe sizing rule + skew-aware autoscaler (rc.8 K1 + K2)

- **Status**: Accepted (rc.8)
- **Date**: 2026-05-02
- **Tickets**: K1 (NVMe default shrink), K2 (skew-aware autoscaler)
- **Supersedes / amends**: ADR-0008 (original two-pool sizing)

## Context

The OSS Helm chart's NVMe default — `storage.size: 240Gi` +
`cache.pools.rowgroup.nvmeSizeBytes: 257698037760` — was copied from
the penpencil production StatefulSet layout at launch (rc.4 /
ADR-0008). That layout was sized to the cluster's then-worst-case
hot working set (rep-0 traffic projected at ~8.7× rep-2) rather
than the median OSS deployment.

### Benchmark evidence (May 1 2026, TPC-H SF100, 4-hour run)

Per-pod NVMe utilisation at steady state on a fresh `shelf-bench`
StatefulSet (6 pods × 240 GiB PVCs, same image as production):

| Pod              | disk_used      | capacity   | utilisation |
| ---------------- | -------------- | ---------- | ----------- |
| shelf-bench-0    | 18.69 GiB      | 240 GiB    | **7.79%**   |
| shelf-bench-1    | 18.77 GiB      | 240 GiB    | **7.82%**   |
| shelf-bench-2    | 18.67 GiB      | 240 GiB    | 7.78%       |
| shelf-bench-3    |  4.79 GiB      | 240 GiB    | 2.00%       |
| shelf-bench-4    |  4.79 GiB      | 240 GiB    | 2.00%       |
| shelf-bench-5    |  4.79 GiB      | 240 GiB    | 2.00%       |
| **Cluster**      | **70.50 GiB**  | 1440 GiB   | **4.90%**   |

The peak pod reached 7.82% of its 240 GiB budget. The ~20 GB
working set fit almost entirely in the 11 GiB DRAM rowgroup pool;
NVMe only absorbed the DRAM overflow.

Raw data:

- `benchmarks/results/2026-05-01/4hr/COMPREHENSIVE-RESULTS.md` (§"Diagnostic")
- `benchmarks/results/2026-05-01/4hr/shelfd-metrics/shelf-bench-{0..5}-stats.json`

### Cost impact

At AWS ap-south-1 list price for `gp3` ($0.0924 / GB-month, no
additional IOPS / throughput provisioning), a 6-pod OSS deployment
on 240 GiB PVCs carries **6 × 240 × $0.0924 ≈ $133/month** on the
shelf-pool storage line. Shrinking to 60 GiB drops that to
**6 × 60 × $0.0924 ≈ $33/month** — a ~**$100/month ( ~75% )** cut
per 6-pod deployment, scaling linearly with pool size.

Production (penpencil, rep-2 / rep-1) already opts into 240 GiB
via the existing chart default; after K1 flips the OSS default
to 60 GiB, production overlays must explicitly pin
`storage.size: 240Gi` + `cache.pools.rowgroup.nvmeSizeBytes:
257698037760` — see the "Migration" section below.

## Decision

**K1**: Ship the OSS chart default at **60 GiB NVMe** per pod
(`storage.size: 60Gi`, `cache.pools.rowgroup.nvmeSizeBytes:
64424509440`). Operators with large hot working sets (>30 GB per
pod — high-traffic BI dashboards, multi-TB Iceberg scans) must
opt in to higher NVMe via overlay values.

**K2** (paired under the same ADR — both target shelf-pool
right-sizing): ship `shelf_pod_load_skew_ratio` and a sample
KEDA `ScaledObject` that scales shelf replicas on the skew
metric rather than on raw CPU / memory. HRW imbalance means a
single high-traffic key family can concentrate on one pod
(observed 2026-04-27: shelf-2 saturated while peers idled);
scaling on skew adds pods where they actually reduce
tail-latency, and avoids the cost of over-provisioning every
pod to accommodate the worst-case skew.

K1 and K2 are sibling levers because both address "is the
default shelf-pool footprint right-sized for the median OSS
deployment?" Bundling them in one ADR ensures the sizing-rule
story stays coherent when operators read the chart.

## Consequences

### Positive

- **~75% gp3 EBS cost reduction** on the shelf-pool storage line
  for median OSS deployments (~$100/month per 6-pod cluster at
  ap-south-1 list).
- **Lower cold-start time**: Foyer NVMe recovery scans the hybrid
  pool at boot; a 60 GiB pool recovers in ~16 s vs ~64 s on a
  240 GiB pool (linear in disk size — see the `probes.startup`
  comment in `values.yaml`).
- **Smaller blast radius for node-replace events**: an evicted
  shelf pod re-warms from cold faster on a 60 GiB pool.

### Neutral

- **DRAM rowgroup pool unchanged**: the 11 GiB DRAM default
  (SHELF-21f) is where the hot working set actually lives on
  median workloads; NVMe is only overflow. Shrinking NVMe does
  not reduce the DRAM hit rate.
- **RSS budget unchanged**: the 27.3 GiB node-allocatable budget
  (`metadata 5 + DRAM 11 + inflight 4 + submit-queue 1 + runtime
  ~3`) is unaffected — NVMe backs disk-resident Foyer entries, not
  process RSS.

### Negative / opt-in required

- **Operators with >30 GB per-pod hot working sets will regress
  hit ratio** on a stock install. The `values.yaml` comment
  block and README table both direct those operators to override
  via overlay.
- **PVC downsize is operator-driven** (see Migration). An
  existing 240 GiB PVC does not auto-shrink on `helm upgrade`
  because StatefulSet `volumeClaimTemplates` is immutable after
  StatefulSet creation.

## Migration

### Fresh installs

`helm install` picks up the new 60 GiB default automatically.
No action required.

### Existing OSS installs (accept the new default)

`kubectl` cannot resize a StatefulSet PVC downward through the
StatefulSet controller. Operators who want the cost saving
must:

1. `helm upgrade` (chart values now default to 60 GiB, but the
   existing PVCs stay at 240 GiB — the StatefulSet spec diff is
   silently dropped by the API server on PVC downsize attempts).
2. Drain one pod at a time: `kubectl -n <ns> delete pod shelf-N
   --wait=true` and simultaneously `kubectl -n <ns> delete pvc
   nvme-shelf-N` (orphan-delete the StatefulSet with
   `--cascade=orphan` first if the controller won't let you
   delete the pvc under the sts).
3. On StatefulSet pod re-creation, the new PVC is provisioned at
   60 GiB from the updated template.
4. Re-warm via the pin-list replay tool (`tools/gen_pin_list.py`)
   or wait for natural warmup (~1–4h depending on traffic).

One-liner dry-run to preview the storage class and size that
would be provisioned on a fresh pod:

```bash
helm template <release> charts/shelf --kube-version 1.30.0 \
  | yq 'select(.kind == "StatefulSet") .spec.volumeClaimTemplates[0].spec.resources.requests.storage'
```

### Existing production (penpencil) installs (keep 240 GiB)

Overlay values file must now pin the previous default explicitly:

```yaml
storage:
  size: 240Gi
cache:
  pools:
    rowgroup:
      nvmeSizeBytes: 257698037760  # 240 GiB
```

This is already documented in the operator-private overlay path
maintained outside the OSS repo.

## Alternatives considered

1. **Keep 240 GiB default + emit a runtime warning if utilisation
   < 30%.** Rejected — adds a scrape-and-decide loop inside
   shelfd for a static-config problem, and creates operator
   churn (noisy warning logs for deployments where 240 GiB is
   correct because operator chose it intentionally).

2. **Auto-resize the Foyer hybrid pool based on observed
   utilisation.** Rejected — Foyer's NVMe device is fixed at
   process start (`store.rs:~340`, `LargeEngineOptions`). A
   resize would require a shelfd restart + Foyer re-open, which
   is identical to a StatefulSet pod recycle from an operator's
   perspective. Not worth the code complexity.

3. **Ship two presets (`small` = 60 GiB, `large` = 240 GiB) as
   a Helm sub-chart or as a `preset` value.** Rejected for rc.8
   as scope creep; will reconsider in rc.9 if operators report
   the 60-vs-240 decision is hard.

4. **Size by NodePool instance store** (auto-detect the EC2
   instance-store size on Karpenter-provisioned nodes). Rejected
   — the OSS chart must remain cloud-agnostic; this belongs in a
   companion operator (out of scope).

## References

- Bench evidence: `benchmarks/results/2026-05-01/4hr/COMPREHENSIVE-RESULTS.md`
- Per-pod stats: `benchmarks/results/2026-05-01/4hr/shelfd-metrics/shelf-bench-{0..5}-stats.json`
- Plan: `rc.8_roadmap.plan.md` §K1 + §K2
- Chart values: `charts/shelf/values.yaml` (storage.size, cache.pools.rowgroup.nvmeSizeBytes)
- Chart StatefulSet template: `charts/shelf/templates/statefulset.yaml` (volumeClaimTemplates)
- Prior sizing ADR: `agents/out/adr/0008-*-two-pool-sizing.md`
- K2 skew-autoscaler metric + ScaledObject example (ships in K2 PR)
