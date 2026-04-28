# Runbook: circuit-breaker-open

**Alert:** `ShelfCircuitBreakerOpen`
**Severity:** page
**Dashboard:** https://grafana.example.internal/d/shelf-overview

## Symptom

At least one per-pod circuit breaker on the plugin side has been OPEN
for 5 minutes. Per BLUEPRINT §9.5 the breaker opens after 5 consecutive
failures, stays open 10s (or longer after successive double-timer
trips), and short-circuits all keys hashing to the target pod to S3.

## Impact

- All keys owned by the target pod are served from S3, not Shelf.
- Hit rate for that slice of keys is zero.
- No user-visible error; queries take S3 latency for the affected
  fraction. This is correct behaviour — but 5 minutes of open is
  longer than transient churn.

## Diagnosis

```bash
# 1. Which breakers are open? From coordinator JMX.
kubectl -n trino-db exec -it deploy/trino-coordinator -- \
  curl -s http://localhost:8080/v1/jmx/mbean/io.trino.shelf:name=CircuitBreakers | jq

# 2. Is the target shelfd pod reachable at all? Three-way check.
TARGET=shelf-1
kubectl -n shelf get pod $TARGET
kubectl -n shelf exec shelf-0 -- curl -fsS http://$TARGET.shelf:9093/healthz || echo "UNREACHABLE"
kubectl -n trino-db exec deploy/trino-worker -- curl -fsS http://$TARGET.shelf.shelf.svc.cluster.local:9090/healthz || echo "UNREACHABLE FROM WORKER"

# 3. What errors is the plugin seeing? Look at worker logs.
kubectl -n trino-db logs deploy/trino-worker --since=15m \
  | grep -E 'ShelfFileSystem|CircuitBreaker' | tail -40
```

## Mitigation

1. **Reset the sick pod** if diagnosis (1)+(2) show it's the pod, not
   the network: `kubectl -n shelf delete pod $TARGET`. Breakers
   auto-close on first-probe success after the pod comes up and
   responds.
2. **Tighten the NetworkPolicy blast-radius**: if diagnosis (2) shows
   the worker cannot reach the pod but `shelf-0` can, the problem is
   network policy, not Shelf. Inspect `networkpolicy.yaml` selector
   labels; likely the Trino worker pod labels drifted from
   `values.yaml` `trino.workerPodLabels`.
3. **Bypass Shelf for the affected catalog** if the breaker is flapping
   on multiple pods and root cause needs > 15 min: set
   `fs.shelf.enabled=false` in the Trino catalog; follow
   `regional-outage.md` fail-back procedure.

## Escalation

- Page primary on-call.
- If > 2 breakers open simultaneously → secondary on-call.
- If breakers open on all pods → incident commander + eng-lead.

## Post-incident actions

- [ ] Save the JMX dump and plugin logs to the incident ticket.
- [ ] If breaker thresholds were too tight, reconsider the default (5
      failures / 10 s open). Any change goes through a BLUEPRINT §9.5
      amendment.
- [ ] If network policy was the cause, add a labels-drift alert.
