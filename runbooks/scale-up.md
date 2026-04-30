# Runbook: scale-up

**Scenario:** Add one or more `shelfd` pods and watch the HRW ring
rebalance cleanly.

## Symptom

On-call decides to scale up because either:
- NVMe usage is above 80% on a majority of pods (warning zone).
- Hit rate is good but tail latency is drifting under sustained load.
- A new replica (rep-0 / rep-1 / rep-3) is being onboarded per plan §3
  Phase 6.

## Impact

Short-term: HRW re-hashes a fraction of keys to the new pod. Those
keys miss on the new pod and re-fetch from S3. Fraction ≈ `1 / N+1`.
For N=3 → 25% of keys re-fetch. This is expected and measured in plan
§2 E7.

## Diagnosis

```bash
# 1. Current replica count + NVMe pressure.
kubectl -n shelf get sts shelf -o jsonpath='{.spec.replicas}'
kubectl -n shelf exec shelf-0 -- shelfctl stats --by-pod | grep nvme

# 2. Do we have capacity on the nodeSelector pool?
kubectl get nodes -l workload=shelf -o wide

# 3. Current ring view.
kubectl -n shelf exec shelf-0 -- shelfctl ring
```

## Mitigation

1. **Bump replicas by one** via Helm (not `kubectl scale`, so Helm
   state stays consistent):
   ```bash
   helm upgrade shelf charts/shelf -n shelf \
     -f charts/shelf/values-prod.yaml \
     --set replicaCount=6
   ```
   The PDB keeps concurrent disruption to 1; StatefulSet ordinals
   ensure the new pod is `shelf-<N>`.
2. **Watch the rebalance.** HRW is stateless — the plugin re-resolves
   the headless service within 5 s of DNS TTL expiry and routes new
   requests accordingly. `shelfctl ring` on any pod shows the new
   weights within 30 s.
3. **Confirm hit-rate recovery.** Expect a brief hit-rate dip
   (~`1/N+1`), recovering as the new pod ingests traffic. If the dip
   exceeds 10% for more than 10 min, something is off — check
   `shelf-fall-through-surge.md`.

## Escalation

- Scale-up during normal hours: no escalation.
- Scale-up during an incident: primary on-call drives; secondary
  observes the rebalance.

## Post-incident actions

- [ ] Update `docs/capacity.md` with the new working-set headroom.
- [ ] If rebalance took > 2 min to converge, capture the E7-style
      metric and compare with plan §2 E7 baseline.
- [ ] If the new pod is slow to warm, consider adding a pre-warm pin
      list pass via `shelfctl pin`.
