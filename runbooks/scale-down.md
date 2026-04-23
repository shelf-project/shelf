# Runbook: scale-down

**Scenario:** Safely drain and remove a `shelfd` pod.

## Symptom

On-call decides to scale down because:
- A node is being retired (Karpenter node rotation).
- Capacity is comfortably over-provisioned.
- A pod has gone bad and we want it out of the StatefulSet entirely
  (rare — usually a pod restart is enough).

## Impact

Keys owned by the removed pod re-hash. `1/N` of the hot keys miss on
their new owner and re-fetch from S3. With circuit breakers and
fallthrough, users see no error.

## Diagnosis

```bash
# 1. Verify the remaining pods have NVMe headroom to absorb the delta.
kubectl -n shelf exec shelf-0 -- shelfctl stats --by-pod

# 2. Which pod is being drained? Default: highest ordinal.
kubectl -n shelf get pods -l app.kubernetes.io/name=shelf -o name

# 3. Any in-flight pin operations we should finish first?
kubectl -n shelf exec shelf-0 -- shelfctl stats --pending
```

## Mitigation

1. **Set the pod unready first** so Trino workers rehash away from it
   *before* we tear it down (prevents a 5-failure storm on breakers):
   ```bash
   kubectl -n shelf exec shelf-N -c shelfd -- \
     curl -X POST http://localhost:9093/admin/drain
   ```
   (`/admin/drain` marks the pod NotReady for 30 s, giving plugin
   DNS resolvers time to pick up the new ring.)
2. **Scale down via Helm:**
   ```bash
   helm upgrade shelf charts/shelf -n shelf \
     -f charts/shelf/values-prod.yaml \
     --set replicaCount=N
   ```
   StatefulSet removes the highest-ordinal pod. The PVC is retained by
   default (see `charts/shelf/README.md` uninstall section).
3. **Delete the PVC** only after a 24-hour safety window, and only if
   you're sure you won't need to scale back up quickly.

## Escalation

- Routine scale-down: no escalation.
- Scale-down to a single pod (v0.1 shape) during an incident: must be
  approved by eng-lead.

## Post-incident actions

- [ ] Log the drain + scale-down sequence in the change log.
- [ ] If the ring took > 1 min to converge, compare with plan §2 E7.
- [ ] If PVC reclamation is intentional, remove the PVC and confirm
      NVMe-backed StorageClass releases the node volume.
