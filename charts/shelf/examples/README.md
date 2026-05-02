# Examples

Reference configs for common shelf integration patterns. None of these
are wired into the chart by default; they are drop-in templates the
operator can copy into their own values overlay.

## `trino-catalog-recipe.yaml`

Recommended Trino Iceberg catalog block when fronting tables through
`shelf-pool`. Per-property rationale is inline in the YAML, including
a link to the relevant `@Config(...)` annotation in Trino 480 source.

The five-property tuning block is **cost-neutral** by construction
(no extra EC2 spend, no new infra) and **performance-up**:

| Property                                  | What it fixes                                                       |
|-------------------------------------------|---------------------------------------------------------------------|
| `iceberg.metadata-cache.enabled=false`    | Unshadows the shelf metadata pool (in-JVM cache otherwise pegs hit ratio at ~0%) |
| `iceberg.dynamic-filtering.wait-timeout=3s` | Caps DF stall on the ~50% JOIN cohort                              |
| `s3.max-connections=256`                  | Sized to a 6-pod shelf-pool (~1536 cluster-wide cap)                |
| `parquet.small-file-threshold=8MB`        | Merges small footer reads into single GET                           |
| `s3.tcp-keep-alive=true`                  | Survives idle TCP drop by kube-proxy / LB                           |

### When to revisit

Update this example when any of the following hold:

- Shelf metadata pool warm hit rate sustains above 80% (you may be able
  to drop `iceberg.metadata-cache.enabled=false` once the shim layer is
  uncontested).
- Phase 4 lever flips land (decoded-metadata cache, range coalescing,
  bloom-aware admission, NVMe compression).
- Your shelf-pool size or per-pod connection budget changes
  (re-derive `s3.max-connections`).

### Related

- `charts/shelf/values.yaml` — `cache.abTag` block: SHELF-42 A/B
  tagging knob for attributing hit/miss across query cohorts. Pair with
  this recipe when measuring the impact of a single property flip.

## `keda-scaledobject-skew-aware.yaml`

K2 (rc.8) drop-in `KEDA ScaledObject` for HRW-skew-aware autoscaling
of the `shelf-pool` StatefulSet. Targets `max(shelf_pod_load_skew_ratio_bps)
> 150` (= ratio > 1.5×) — i.e. scale up when one shelf pod is doing
significantly more work than the cluster median.

| Trigger                           | Threshold | Why                                                                |
|-----------------------------------|-----------|--------------------------------------------------------------------|
| `max(shelf_pod_load_skew_ratio_bps)` | `150`  | Catches HRW hot-key fan-out (workspace memory: rep-2 `mbuser_admin` regime) |
| `avg(shelf_pod_load_qps)`         | `800`     | Optional baseline; matches per-pod throughput envelope from rep-1 cutover |

Adjust the `serverAddress`, `namespace`, and replica bounds for your
cluster. Requires KEDA installed in-cluster. The chart does NOT depend
on KEDA — this is a paste-in template.

### When to revisit

- Phase 4 lever flips land (decoded-metadata cache, range coalescing,
  bloom-aware admission) — the per-pod throughput envelope shifts and
  the `qps` baseline trigger may want re-tuning.
- Your cluster's hot-key distribution evens out (e.g. by sharding a
  Metabase user pool by `user_id` prefix) — the skew threshold can
  rise without losing autoscaling responsiveness.

### Related

- `agents/out/adr/0042-rc8-shelf-pool-rightsizing.md` — design + rationale
  for K1 (NVMe shrink) + K2 (skew autoscaler) together.
- `shelfd/src/pod_load.rs` — the in-process aggregator that publishes
  `shelf_pod_load_qps` + `shelf_pod_load_skew_ratio_bps`.
