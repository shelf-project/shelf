# Runbook: ShelfNvmeUsageHigh

**Alert:** `ShelfNvmeUsageHigh`
**Severity:** warn
**Dashboard:** `${SHELF_DASHBOARD_BASE}/d/shelf-overview`

## Symptom

A Shelf pod's NVMe volume is > 85% full for 15 minutes. Foyer's S3-FIFO
eviction (ADR-0009) is running but not keeping up with admission.

## Impact

- At 90% Foyer's built-in admission controller starts refusing new
  inserts. Existing keys still serve — hit rate holds on already-cached
  data.
- If the pod crosses 95% and the kubelet's `DiskPressure` condition
  fires, the pod is evicted; HRW reshuffles and a second pod may then
  tip into the same state. This is the failure mode from the Alluxio
  `NodeHasDiskPressure` incident on 2026-04-20 — we want to interrupt
  the feedback loop **before** kubelet intervenes.

## Diagnosis

```bash
# 1. Which pod, and by how much?
kubectl -n shelf exec shelf-0 -- shelfctl stats --by-pod | grep -E 'pod|nvme'

# 2. Who is admitting? Look for a recent large-object admission storm.
kubectl -n shelf logs shelf-0 -c shelfd --since=30m | grep '"admit"' | head -20

# 3. Is the pin list forcing a heavy-pinned bypass of the size threshold?
kubectl -n shelf exec shelf-0 -- shelfctl stats --pinned-bytes
```

## Mitigation

1. **Tighten the size threshold temporarily.** Set
   `cache.admission.sizeThresholdMiB=512` via `helm upgrade` — refuses
   more admits, keeps existing data warm. Revert after peak load.
2. **Unpin low-value pins.** If `pinned_bytes` is a large fraction of
   NVMe, review the pin list for tables that should have rolled off
   (see `unpin-table.md`).
3. **Horizontal scale-up by 1 pod.** See `scale-up.md`. Spreads bytes
   across one more node's NVMe without moving any existing data
   involuntarily.

## Escalation

- No page expected — this is a `warn` alert.
- If the same pod crosses 92% after mitigation, page the primary
  on-call and treat as `ShelfPodRestarting` risk.

## Post-incident actions

- [ ] Re-evaluate `docs/capacity.md` for the working-set estimate; if
      the actual is > 1.5× planned, schedule a permanent scale-up.
- [ ] If the pin list was oversized, add a byte-budget guardrail in the
      trainer PR template.
- [ ] Check whether Foyer's S3-FIFO eviction kept up — if not, file an
      issue against the Foyer version pinned in `Cargo.toml`.
