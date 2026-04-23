# Runbook: ShelfFallThroughSurge

**Alert:** `ShelfFallThroughSurge`
**Severity:** page
**Dashboard:** https://grafana.penpencil.internal/d/shelf-overview

## Symptom

More than 20% of plugin requests are falling through to S3 over a
rolling 5-minute window. This is the BLUEPRINT §9.5 fail-open path
firing at scale.

## Impact

Users still get correct answers — plugin never surfaces a Shelf error.
But p95 / p99 query latency drift toward raw-S3 values, S3 GB-month +
request cost rises sharply, and a 20% fall-through often correlates
with an underlying pod-level failure that the hit-rate alert has not
yet detected.

## Diagnosis

```bash
# 1. Which Shelf pod is the plugin avoiding? (circuit breakers open)
kubectl -n trino-db exec -it deploy/trino-coordinator -- \
  curl -s http://localhost:8080/v1/jmx/mbean/io.trino.shelf:name=CircuitBreakers | jq

# 2. What does shelfd itself report about connectivity?
for p in $(kubectl -n shelf get pod -l app.kubernetes.io/name=shelf -o name); do
  echo "== $p =="
  kubectl -n shelf exec $p -c shelfd -- shelfctl stats --fallthrough
done

# 3. Anything unusual in the NetworkPolicy or CNI logs?
kubectl -n shelf describe networkpolicy shelf | tail -20
kubectl -n kube-system logs -l k8s-app=aws-node --tail=100 | grep -i denied || true
```

## Mitigation

1. **Identify + restart the sick pod.** If a single pod has the circuit
   breaker open against it from most workers, `kubectl -n shelf delete
   pod shelf-<ordinal>`. HRW rebalances; fallthrough should subside in
   under 1 minute.
2. **Widen the NetworkPolicy only if you have evidence** the CNI is
   dropping legitimate traffic. Apply `charts/shelf/values-prod.yaml`
   with `networkPolicy.enabled=false` via `helm upgrade`; revert as
   soon as the surge passes.
3. **Flip to Alluxio hot-standby** if more than 2 pods are unreachable
   and the cluster is > 50m into the incident. Follow `regional-outage.md`.

## Escalation

- Primary on-call for first 15m.
- If fallthrough rate > 50% for 5m continuous → secondary + network-SRE
  (CNI owner).
- If fallthrough coincides with an AZ event → incident commander.

## Post-incident actions

- [ ] Save `shelfctl stats --fallthrough` output to the incident ticket.
- [ ] Correlate pod-restart events with breaker-open transitions; a
      mismatch means the probe or liveness check is too lax.
- [ ] If CNI policy was the cause: add an explicit CI test that
      `helm template` + kubeconform catches the regression.
