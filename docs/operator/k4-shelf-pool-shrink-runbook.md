# K4 — shelf-pool 6 → 4 controlled rollout

> Operator-only. Conditional on K2 (skew autoscaler, PR #112) + 7+ days
> of healthy production traffic on the current 6-pod shelf-pool.

## Pre-flight gates (all must be GREEN)

| Metric | Healthy threshold | How to check |
|---|---|---|
| `shelf_pod_load_skew_ratio_bps` | < 150 (= ratio < 1.5) sustained for 24h | `max_over_time(shelf_pod_load_skew_ratio_bps[24h])` < 150 |
| Hit ratio (rowgroup pool) | ≥ 80% sustained | dashboard row 2 |
| Net $ saved (24h) | ≥ 0 (no regression) | dashboard row 1 |
| Pod restart count (24h) | ≤ 1 | `kube_pod_container_status_restarts_total` |
| `shelf_pod_load_probe_errors_total` rate | ≤ 0.01/s | catches K2 aggregator partition |

If any gate is RED — STOP. K2 has not soaked enough; defer K4.

## Rollout procedure (one pod at a time, 24h soak between)

For each pod-to-drop in [shelf-N, shelf-N-1] (drop highest-numbered first):

1. **Drain** — set drain mode active via the SHELF-A2 mechanism:
   ```
   kubectl exec -n alluxio shelf-N -- /bin/sh -c 'kill -TERM 1' || true
   ```
   Wait 30s for graceful drain (A2 SIGTERM handler refuses new admits).

2. **Scale down**:
   ```
   kubectl scale -n alluxio sts shelf --replicas=$((CURRENT - 1))
   ```

3. **Soak 24h**, observing:
   - `shelf_pod_load_skew_ratio_bps` MUST stay < 150 (peer-fetch picks up dropped pod's range)
   - p95 read wall MUST NOT regress > 20% vs pre-drop baseline
   - `shelf_peer_fetch_errors_total` rate MUST stay ≤ 0.01/s
   - hit ratio MUST stay ≥ 80% (acceptable: 5pp drop short-term while peer-fetch warms)

4. **Abort gates** — auto-abort + restore replicas if ANY of:
   - hit ratio drops > 5pp from pre-drop baseline
   - p95 wall regression > 20%
   - peer-fetch error rate > 1% (1 in 100 fetches)
   - skew ratio > 200 bps (2.0x imbalance — peer-fetch overwhelmed)

   Abort:
   ```
   kubectl scale -n alluxio sts shelf --replicas=$((CURRENT))  # restore
   ```

5. **Promote**: if 24h soak GREEN, drop next pod (back to step 1).

## Final state validation

After both drops complete + 24h soak on the 4-pod state:
- hit ratio ≥ 80% sustained
- p95 wall within 10% of original 6-pod baseline
- skew ratio < 150 bps sustained
- pre-drop monthly $ amortized cost reduced by ~33% (4 vs 6 m5a.4xlarge nodes)

## Restore (if 4-pod state proves unhealthy)

```
kubectl scale -n alluxio sts shelf --replicas=6
```

Pods rejoin the HRW ring; K2 resolver picks them up within 30s.

## References
- K2 ADR-0042 §"K2 — implementation details"
- K2 KEDA example: charts/shelf/examples/keda-scaledobject-skew-aware.yaml
- A2 (SIGTERM drain) ADR-0027
- SHELF-23 peer-fetch (workspace memory)
