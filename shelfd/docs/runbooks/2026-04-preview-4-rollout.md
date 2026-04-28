# `shelfd:0.1.0-preview-4` rollout — `data-platform-cluster:alluxio`

## Status

| Pod        | Image                                | Restarts | OOM history   | Notes                                  |
| ---------- | ------------------------------------ | -------- | ------------- | -------------------------------------- |
| `shelf-0`  | `0.1.0-preview-4`                    | 0        | —             | healthy                                |
| `shelf-1`  | `0.1.0-preview-4`                    | 2        | 2026-04-27 14:30:14 UTC, exit 137 | OOM, see runbook   |
| `shelf-2`  | `0.1.0-preview-4`                    | 0        | —             | healthy                                |

`shelf-1`'s OOM is a Foyer LODC submit-queue saturation failure mode,
not a `preview-4` regression. RCA + fix in
[`2026-04-shelf-1-oom.md`](./2026-04-shelf-1-oom.md). Fix is coded on
the same `rep2-shelf-integration` branch and ships in `preview-5`.

## What `preview-4` actually carries

In addition to the SHELF-20 features called out in `values-prod.yaml`'s
image-tag comment block (DNS membership resolver, lameduck drain,
`/admin/ring` endpoint), the `rep2-shelf-integration` branch piles on
the SHELF-A1/A5/A6/G-4/G-8/G-9/G-10/G-11/A7 metric+observability wave:

| Track | Surface added                                                          |
| ----- | ---------------------------------------------------------------------- |
| A1    | `shelf_request_seconds{outcome,tier}` wired in the S3 shim hot path    |
| A5    | `shelf_evictions_total{reason="capacity"}` via Foyer EventListener     |
| A6    | `shelf_engine_resets_total` counter                                    |
| G-4   | `shelf_hits_by_table_total` / `shelf_misses_by_table_total`            |
| G-8/9 | `shelf_origin_bytes_total`, `shelf_inflight_singleflight`              |
| G-10  | `shelf_engine_resets_total` (alert: ≥ 1 in 5 min)                      |
| G-11  | `shelf_warm_threshold_crossed_seconds` time-to-warm SLI sampler        |
| A7    | `tools/gen_pin_list.py` SQL fixed; `--top-5-prod` emergency fallback   |

Grafana dashboard `shelf-overview` was extended with 20 panels that
read these series. Mimir-data outage on the day of rollout caused a
brief "No data" cosmetic; not related to the OOM.

## What `preview-5` will add

1. The OOM fix from `2026-04-shelf-1-oom.md`:
   - `RowGroupDiskCacheConfig` in shelfd config (Foyer LODC tunables)
   - `cache.pools.rowgroup.diskCache.{flushers, bufferPoolSizeBytes,
     submitQueueSizeThresholdBytes}` plumbed through the chart
   - `origin.pool.maxConnections` 256 → 128 in both prod and alluxio overlays
   - `values-alluxio.yaml` rowgroup DRAM 20 → 14 GiB to fit under the
     real (~27 GiB) NodePool allocatable
2. Sizing comments rewritten so the next operator sees the *node*
   allocatable as the ceiling, not the (unreachable) 32 GiB container
   limit.

No code-path behaviour changes outside the LODC tuning hooks; existing
config YAML continues to parse unchanged because every new field is
`#[serde(default)]`-backed.

## Rollout step (when image is ready)

```bash
# 1. Render & diff
cd /Users/aamir/trino/shelf
helm template charts/shelf -f charts/shelf/values-prod.yaml \
  --kube-version 1.30.0 > /tmp/prod-after.yaml
# expect diff = max_inflight 256→128 + new disk_cache block, nothing else

# 2. Upgrade
helm upgrade shelf ./charts/shelf -n alluxio \
  -f charts/shelf/values-alluxio.yaml \
  --set image.tag=0.1.0-preview-5
kubectl -n alluxio rollout status statefulset/shelf

# 3. Watch
kubectl -n alluxio logs -f shelf-1 | rg -i 'lodc|submit queue' &
# Grafana → Shelf — Cache, Disk and Pods → "Pod RSS"
#   - steady-state ≤ 22 GiB / pod
#   - burst peaks  ≤ 25 GiB / pod
#   - shelf_evictions_total{reason="capacity"} climbs cleanly
```

24 h clean soak before declaring `preview-5` healthy and re-tagging as
`v0.5.0`.
