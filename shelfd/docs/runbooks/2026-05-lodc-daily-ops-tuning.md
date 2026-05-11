# LODC + NVMe tuning (daily ops loop — May 2026)

## Preconditions

- Kubernetes context points at the cluster running shelf (`infra use <cluster>` if your wrapper requires it).
- Grafana: `shelf_lodc_drops_total{reason="submit_queue_overflow"}` and `shelf_disk_bytes_capacity`.

## Fix 0 — ConfigMap + rolling restart

Patch merges **Foyer LODC** knobs and picks up **NVMe size** changes only after pods restart.

### 1. Merge patch (example — align with live `shelf-shelfd` / Helm)

```bash
kubectl -n alluxio patch cm shelf-shelfd --type merge -p '
data:
  shelfd.yaml: |
    # ... preserve entire existing shelfd.yaml content ...
    pools:
      rowgroup:
        disk_cache:
          flushers: 4
          buffer_pool_size_bytes: 268435456
          submit_queue_size_threshold_bytes: 1073741824
          admission:
            enabled: true
            target_bytes_per_sec: 104857600
            max_burst_bytes: 268435456
'

```

**Prefer Helm** so drift is tracked: set `cache.pools.rowgroup.diskCache` + `lodcAdmission` in your values and `helm upgrade`, then restart if the chart does not roll the StatefulSet automatically.

### 2. Rolling restart

```bash
kubectl -n alluxio rollout restart sts/shelf
kubectl -n alluxio rollout status sts/shelf --timeout=600s
```

### 3. Gates (within ~30 min)

| Signal | Target |
|--------|--------|
| `shelf_disk_bytes_capacity` | ~536e9 B per pod after 500 GiB PVC + config |
| `rate(shelf_lodc_drops_total{reason="submit_queue_overflow"}[5m])` | Collapse vs 500+/s sustained pre-fix |
| Pod RSS | Below your node allocatable minus headroom |

## Default code change (shelfd ≥ this commit)

- **SHELF-29** default refill: `100 MiB/s` (`target_bytes_per_sec`) instead of 200 MiB/s, matching ~80 % of a 125 MiB/s gp3 baseline.
- Chart values emit `lodcAdmission` into `pools.rowgroup.disk_cache.admission` when set.

## See also

- `agents/out/SHELF-29-independent-queue-rate-limiter.md`
- Workspace `analyze-replica-impact` skill for post-change synthesis
