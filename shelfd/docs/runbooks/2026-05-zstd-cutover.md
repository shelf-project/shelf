# Runbook: Enable zstd NVMe Compression (Tier 2 item 6)

**Date**: 2026-05
**Owner**: Shelf Operations
**Risk Level**: Medium (one-way format break)

## Overview

This runbook enables zstd compression on the rowgroup pool's NVMe tier.
Compression typically achieves 2–4× reduction on Parquet row groups,
extending the effective NVMe cache capacity without hardware changes.

**CRITICAL**: This is a one-way format break. The NVMe cache cannot be
read by a shelfd binary with different compression settings. You MUST
wipe all PVCs.

## Prerequisites

1. **Confirm sufficient cluster capacity**: shelf-pool must handle the
   traffic load while pods are cycling (one pod down at a time via
   `OrderedReady` rolling restart).

2. **Confirm Helm values**: Ensure your overlay sets:
   ```yaml
   cache:
     pools:
       rowgroup:
         compression:
           enabled: true
           level: 3    # zstd level 1-22; 3 is a good latency/ratio trade-off
           minSizeBytes: 65536  # skip compression for <64 KB payloads
   ```

3. **Notify on-call**: The entire shelf-pool will cold-restart; expect
   ~2 hours of elevated miss rate while the cache rewarms.

## Procedure

### Step 1: Scale down the StatefulSet

```bash
kubectl -n alluxio scale sts/shelf-pool --replicas=0
kubectl -n alluxio wait --for=delete pod -l app.kubernetes.io/name=shelf --timeout=5m
```

### Step 2: Delete the PVCs

```bash
# List PVCs to confirm
kubectl -n alluxio get pvc -l app.kubernetes.io/name=shelf

# Delete all shelf PVCs
kubectl -n alluxio delete pvc -l app.kubernetes.io/name=shelf

# Verify deletion
kubectl -n alluxio get pvc -l app.kubernetes.io/name=shelf
```

### Step 3: Apply the updated Helm values

```bash
helm upgrade shelf charts/shelf \
  -n alluxio \
  -f charts/shelf/values-prod.yaml \
  --set cache.pools.rowgroup.compression.enabled=true \
  --set cache.pools.rowgroup.compression.level=3
```

### Step 4: Scale up the StatefulSet

```bash
kubectl -n alluxio scale sts/shelf-pool --replicas=6
kubectl -n alluxio rollout status sts/shelf-pool --timeout=10m
```

### Step 5: Verify compression is active

```bash
# Check the marker file on any pod
kubectl -n alluxio exec shelf-pool-0 -- cat /data/.shelf-compression.json

# Expected output:
# {"compression_enabled":true,"level":3}
```

### Step 6: Monitor rewarm

Watch the dashboard **Shelf — Cache, Disk and Pods** (uid `shelf-overview`)
for:

- `shelf_rolling_hit_ratio_bps{pool="rowgroup"}` climbing back toward 80%+
- `shelf_disk_bytes_used` growing (now compressed size, not raw)
- Zero `shelf_lodc_drops_total{reason="submit_queue_overflow"}`

## Rollback

If compression causes issues:

1. Scale down: `kubectl -n alluxio scale sts/shelf-pool --replicas=0`
2. Delete PVCs: `kubectl -n alluxio delete pvc -l app.kubernetes.io/name=shelf`
3. Helm upgrade with `compression.enabled=false`
4. Scale up: `kubectl -n alluxio scale sts/shelf-pool --replicas=6`

## Post-cutover

1. Observe for 24h: hit ratio should return to pre-cutover levels
2. Document the effective compression ratio in your cluster's AGENTS.md
3. Consider adjusting `level` (1–22) based on CPU overhead vs ratio trade-off

## References

- `TODO-fix-shelf-performance.md` §3 Tier 2 item 6
- `charts/shelf/values.yaml` — `cache.pools.rowgroup.compression` block
- `shelfd/src/store.rs` — Foyer compression integration
