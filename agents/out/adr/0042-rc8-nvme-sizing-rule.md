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
right-sizing): ship `shelf_pod_load_qps`, `shelf_pod_load_skew_ratio_bps`,
and `shelf_pod_load_probe_errors_total` from every pod, plus a
sample KEDA `ScaledObject` that scales shelf replicas on the skew
metric rather than on raw CPU / memory. HRW imbalance means a
single high-traffic key family can concentrate on one pod
(observed 2026-04-27: shelf-2 saturated while peers idled);
scaling on skew adds pods where they actually reduce
tail-latency, and avoids the cost of over-provisioning every
pod to accommodate the worst-case skew. The skew gauge is
expressed in **basis points** (× 100) so chart values overlays
do not trip the YAML scientific-notation Helm landmine that
already bit `shelf_rolling_hit_ratio_bps`. See the
"K2 — implementation details" section below for the wire shape,
hot-path discipline, and rollback levers.

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

## K2 — implementation details

### Metric shape

Three series exposed on every shelf pod's `/metrics`:

| Metric                                  | Type        | Unit               | Cardinality |
|-----------------------------------------|-------------|--------------------|-------------|
| `shelf_pod_load_qps`                    | `IntGauge`  | requests / second  | per-pod (Prometheus stamps `pod` external label) |
| `shelf_pod_load_skew_ratio_bps`         | `IntGauge`  | basis points (× 100) | per-pod   |
| `shelf_pod_load_probe_errors_total`     | `IntCounter`| count              | per-pod   |

`100 bps = 1.0` (perfect balance) is the lower bound;
`>= 150 bps` (1.5×) is the K2 scale-up threshold the example
KEDA `ScaledObject` targets; `>= 200 bps` (2×) is the page-class
imbalance.

### Hot-path discipline

The s3-shim accept handlers (`handle_get_object`,
`handle_head_object`) call `state.pod_load.record_request()` at
the very top of the function — after route dispatch but before
range parsing, cache lookup, or origin GET. The call is a single
`AtomicU64::fetch_add(Relaxed)` when the gate is enabled, and a
check-then-return when disabled (`cache.podLoad.enabled=false`).
There is **no lock** on the hot path; the rolling-window
`VecDeque` and the peer probes live entirely in the background
loop, which runs at `cache.podLoad.aggregationInterval` (default
30 s). The `concurrent_record_request_safe` test pins the
lock-free invariant by spawning 1 000 threads × 100 calls and
asserting all 100 000 increments land.

### Wire shape: opt-in `pod_load` block on `/stats`

The in-cluster aggregator probes peers with
`GET /stats?include=pod_load`, which adds an opt-in
`pod_load: { qps, window_secs }` block to the JSON payload. The
default `/stats` shape (no `?include=`) stays byte-identical with
pre-K2 — Agent 5's HRW weighting, `shelfctl stats`, the
`cap-ready` gate, and the membership resolver all use the
no-include path. Wire compatibility is enforced by the
`stats_payload_has_contract_keys` test (asserts `pod_load` is
absent from the default payload) and the new
`stats_payload_includes_pod_load_when_requested` test (asserts
the `qps` + `window_secs` fields round-trip cleanly).

### Skew formula

`compute_skew_bps(qps_values)` =
`max(qps_values) * 100 / median(qps_values)`, with the
**lower-median** convention `qps_values[(len-1)/2]` after
ascending sort. Edge cases:

- Empty input ⇒ `100 bps` (cannot compute; report balanced so
  the autoscaler does not react to noise on a fresh cluster).
- Median == 0 ⇒ `100 bps` (avoid divide-by-zero; same "no
  signal" semantics).
- Two pods `[80, 40]` ⇒ `200 bps` (1 pod doing 2× the other's
  work). Linear-interpolation median would report `133 bps`
  and under-state the imbalance — the lower-median is the
  simplest convention that makes the binary case correct
  without the n-pod case becoming pathological.

### KEDA wiring

`charts/shelf/examples/keda-scaledobject-skew-aware.yaml` is a
drop-in template (NOT chart-default; the chart does not depend
on KEDA). Two triggers:

| Trigger                                    | Threshold | Why |
|--------------------------------------------|-----------|-----|
| `max(shelf_pod_load_skew_ratio_bps)`       | `150`     | Catches HRW hot-key fan-out (workspace memory: `mbuser_admin` regime) |
| `avg(shelf_pod_load_qps)`                  | `800`     | Optional baseline; matches per-pod throughput envelope from rep-1 cutover |

Operators substitute `serverAddress`, `namespace`, and replica
bounds for their cluster.

### Composition with the rc.7 admit chain

K2 is **additive**. It does NOT modify A1
(`admission_limiter`), A2 (drain), A3 (`rewarm_poller`), A4
(`cost`), A6 (`coop_admission`), or B3 (`transient_admission`).
The only hot-path touch is the single `record_request()` call
at the top of the two s3-shim accept handlers; everything else
lives in the background `PodLoadAggregator::run` loop and the
additive `?include=pod_load` JSON block. Each gate's blast
radius stays observable independently via the existing
counters.

### Rollback

Three escape hatches in increasing severity:

1. **Disable autoscaler only** — delete or scale-out-pin the
   `ScaledObject` resource. Shelf keeps publishing K2 metrics
   for observability.
2. **Disable metric publication** — flip
   `cache.podLoad.enabled=false` and `kubectl rollout restart
   sts/shelf-pool`. The aggregator stops, gauges stop
   publishing, the autoscaler falls back to its second trigger
   (the example uses aggregate `shelf_pod_load_qps` as a
   baseline fallback).
3. **Pin replica count** — `kubectl scale sts shelf-pool
   --replicas=N`. Independent of K2; reverts the cluster to a
   pre-K2 fixed-size pool.

### Validation evidence (in-tree)

- 12 unit tests in `shelfd/src/pod_load.rs`
  (`disabled_records_no_op`,
  `rolling_window_evicts_old_samples`,
  `rolling_window_count_undefined_with_one_sample`,
  `single_pod_skew_is_one_bps_100`,
  `two_pods_balanced_skew_is_100`,
  `two_pods_skewed_2_to_1_skew_is_200`,
  `three_pods_two_hot_one_cold_uses_lower_median`,
  `empty_input_reports_balanced`,
  `zero_median_reports_balanced`,
  `peer_probe_timeout_falls_back_to_local`,
  `concurrent_record_request_safe`,
  `aggregation_interval_respected`).
- `shelfd::http::tests::stats_payload_includes_pod_load_when_requested`
  pins the wire shape of the `?include=pod_load` JSON block.
- `shelfd::metrics::tests::registry_exposes_documented_series`
  proves the three K2 series register with the global registry
  on every pod (whether the gate is enabled or not).
- KEDA example offline-validates with
  `python3 -c 'import yaml; d = yaml.safe_load(open(...))'`
  asserting `apiVersion`, `kind`, `scaleTargetRef`, and
  trigger structure. `kubectl apply --dry-run=client` requires
  cluster API discovery for the CRD (not available in this
  agent's network namespace); operators run it post-install.

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
- K2 module: `shelfd/src/pod_load.rs` (aggregator + trait-injected peer prober)
- K2 metrics: `shelfd/src/metrics.rs` (`POD_LOAD_QPS`, `POD_LOAD_SKEW_RATIO_BPS`, `POD_LOAD_PROBE_ERRORS_TOTAL`)
- K2 wire shape: `shelfd/src/control.rs::PodLoadStats`, `shelfd/src/http.rs::handlers::stats` (`?include=pod_load`)
- K2 hot-path call site: `shelfd/src/s3_shim.rs::handle_get_object` + `handle_head_object`
- K2 KEDA example: `charts/shelf/examples/keda-scaledobject-skew-aware.yaml`
- K2 Helm wiring: `charts/shelf/values.yaml` (`cache.podLoad`) + `charts/shelf/templates/configmap-shelfd.yaml` (`pod_load:` block)
