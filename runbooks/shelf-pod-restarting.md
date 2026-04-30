# Runbook: ShelfPodRestarting

**Alert:** `ShelfPodRestarting`
**Severity:** page
**Dashboard:** `${SHELF_DASHBOARD_BASE}/d/shelf-overview`

## Symptom

A `shelfd` container has restarted ≥ 3 times in the last 15 minutes.
Crashloop territory.

## Impact

The affected pod is intermittently absent from the HRW ring. Plugin
circuit breakers open and route traffic to S3 for the keys hashing to
this pod. Hit rate drops proportionally to `1/replicas`.

## Diagnosis

```bash
# 1. Why did it die? (OOM, panic, probe failure)
kubectl -n shelf describe pod shelf-0 | tail -40
kubectl -n shelf logs shelf-0 -c shelfd --previous | tail -80

# 2. Is the NVMe PVC healthy? Foyer may be refusing to mount/replay.
kubectl -n shelf get pvc -l app.kubernetes.io/instance=shelf
kubectl -n shelf describe pvc nvme-shelf-0 | tail -20

# 3. Is it a config regression? Diff last two ConfigMap / Helm revisions.
helm -n shelf history shelf
helm -n shelf get values shelf --revision $(helm -n shelf history shelf -o json | jq -r '.[-1].revision')
```

## Mitigation

1. **If OOMKilled:** bump `resources.limits.memory` on the
   StatefulSet and `helm upgrade`. The default limit is a placeholder
   (see `values.yaml` comment). Don't just raise requests without
   updating the capacity doc.
2. **If Foyer panics on startup reading NVMe:** delete the PVC for that
   pod (`kubectl -n shelf delete pvc nvme-shelf-<ordinal>` then the
   pod). The pod comes back with an empty NVMe and rewarms — hit rate
   recovers in minutes.
3. **If config regression:** `helm -n shelf rollback shelf` to the
   last known-good revision.

## Escalation

- Page primary on-call.
- If > 2 pods are restarting simultaneously → secondary + eng-lead;
  treat as a whole-cluster failure and follow `regional-outage.md`.

## Post-incident actions

- [ ] Capture the crash `tracing` log into the incident ticket.
- [ ] If PVC replay caused the crash, file a bug against the pinned
      Foyer version (risk R-06 / R-14 in plan §5).
- [ ] If an OOM, run the E3 benchmark on staging to re-derive a
      resource baseline; update `docs/capacity.md`.
