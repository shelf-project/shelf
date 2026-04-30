# cutover-rep2 — trino-replica-2 cdp → shelf-pool

**Plan**: `shelf zero-downtime + capacity` (a2fa5fe7), Stage 5.2.
**Branch**: `shelf-cutover-rep2` in `deployments-repo` (local; pushed by
Conductor A at cutover time).
**MR**: `<TBD-after-push>`.
**Soak**: **30 min**.
**Why second**: rep-2 is currently on the per-pod
`shelf-2.shelf.alluxio.svc.cluster.local:9092` pin (commit `649c0732dc`,
MR !17852). This cutover only changes the routing model — same shelfd
binary, same workload, same IRSA — so it's the cheapest place to
validate cluster-svc behavior on a real coord.

The diff is a single line: cdp catalog `s3.endpoint` flips from
`http://shelf-2.shelf.alluxio.svc.cluster.local:9092` to
`http://shelf-pool.shelf.svc.cluster.local:9092`.

## Pre-cutover checklist (T-1h)

| # | Check | Command / source | Expected |
|---|-------|------------------|----------|
| 1 | Image tag locked | `kubectl -n shelf get sts shelf-pool -o jsonpath='{.spec.template.spec.containers[0].image}'` | `shelfd:0.1.0-preview-N` matches Stage 1 helm rev |
| 2 | Helm revision locked | `helm -n shelf history shelf` | latest rev = chart drop-in (Stage 1); no upgrade <2h before T-0 |
| 3 | rep-3 still green | re-run rep-3's monitoring queries (cutover-rep3.md §Live monitoring) | P1-P5 still PASS |
| 4 | Concurrent MRs check | `git -C /Users/aamir/ranger/deployments-repo log origin/cicd-v2 --oneline --since='2h ago' -- values-files/data-platform-cluster/trino-replica-2-values.yaml` | only this MR pending |
| 5 | shelf-pool endpoints | `kubectl -n shelf get endpointslice -l kubernetes.io/service-name=shelf-pool -o yaml \| grep -c 'ready: true'` | ≥ 3 |
| 6 | SHELF-23 peer-fetch live | preview tag includes SHELF-23 fix; `shelf_peer_fetch_total{outcome="hit"}` rising on dashboard since rep-3 cutover | non-zero |
| 7 | Smoke harness PASS | `python shelf/tools/smoke_harness.py --endpoint http://shelf-pool.shelf.svc.cluster.local:9092 --replica rep-2` | exit 0; 5/5 byte-identical |
| 8 | Pin-list pre-warm | see §Pin-list pre-warm | `success_ratio ≥ 0.98` |
| 9 | Stage 0a picker is OFF | `kubectl -n shelf get cm shelf-shelfd -o yaml \| grep admission_bytes_per_sec` | unset |
| 10 | Operator quiet window | NOT 09:00-11:00 IST | confirmed |

## Pin-list pre-warm

> **Tool dependency**: `shelf/tools/replay_pinlist.py` is Agent D's
> deliverable; check `git -C /Users/aamir/trino log shelf/tools/` to
> confirm landed.

Note: shelf-pool's NVMe is already partly warm with rep-3's working set
from Stage 5.1. rep-2's pre-warm will compete for capacity — expect mild
eviction of rep-3 entries. Verify on the `shelf-overview` dashboard that
rep-3's hit-ratio does not drop more than 5 percentage points during
rep-2's pre-warm; if it does, shelf-pool is under-provisioned for the
combined working set (see Stage 4 over-provision plan).

```bash
python /Users/aamir/trino/shelf/tools/replay_pinlist.py \
    --replica rep-2 \
    --endpoint http://shelf-pool.shelf.svc.cluster.local:9092 \
    --lookback 24h \
    --concurrency 64 \
  | tee /tmp/prewarm-rep-2-$(date -u +%Y%m%dT%H%M).json
```

Acceptance: `success_ratio ≥ 0.98`, `p99_latency_ms ≤ 150`.

## T-0 — cutover

1. Conductor A pushes `shelf-cutover-rep2`, opens MR (`<TBD-after-push>`),
   merges to `cicd-v2`.
2. ArgoCD reconciles `trino-replica-2` Helm release.
3. Roll the coordinator:
   ```bash
   kubectl -n trino-db rollout restart deployment/trino-replica-2-coordinator
   kubectl -n trino-db rollout status   deployment/trino-replica-2-coordinator --timeout=5m
   ```

## Live monitoring (during soak)

```bash
COORD_IP=$(kubectl -n trino-db get pods -l release=trino-replica-2,app.kubernetes.io/component=coordinator \
    -o jsonpath='{.items[0].status.podIP}')
echo "rep-2 coord IP: $COORD_IP"
```

### Failed query rate (1-min step)

```sql
SELECT
    date_trunc('minute', query_date) AS minute,
    count_if(query_state = 'FAILED') AS failed,
    count(*) AS total,
    round(100.0 * count_if(query_state = 'FAILED') / nullif(count(*), 0), 2) AS failed_pct
FROM cdp.trino_logs.trino_queries
WHERE query_date >= timestamp '<T-0 UTC>'
  AND server_address = '<COORD_IP>'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 60;
```

### P95 / P99 wall time

```sql
SELECT
    approx_percentile(wall_time_millis, 0.5)  AS p50_ms,
    approx_percentile(wall_time_millis, 0.95) AS p95_ms,
    approx_percentile(wall_time_millis, 0.99) AS p99_ms,
    count(*) AS samples
FROM cdp.trino_logs.trino_queries
WHERE query_date >= timestamp '<T-0 UTC>'
  AND server_address = '<COORD_IP>'
  AND query_state = 'FINISHED';
```

### Error-code histogram

```sql
SELECT error_code, count(*) AS hits
FROM cdp.trino_logs.trino_queries
WHERE query_date >= timestamp '<T-0 UTC>'
  AND server_address = '<COORD_IP>'
  AND query_state = 'FAILED'
GROUP BY 1
ORDER BY hits DESC;
```

## PASS criteria (at T+30 min)

| # | Criterion | Threshold | Hard fail |
|---|-----------|-----------|-----------|
| P1 | P95 wall_time | ≤ 1.2× baseline | > 2× |
| P2 | P99 wall_time | ≤ 1.2× baseline | > 2× |
| P3 | New failure classes | 0 | any |
| P4 | `ICEBERG_CANNOT_OPEN_SPLIT` count | 0 | ≥ 1 |
| P5 | `ICEBERG_INVALID_METADATA` count | ≤ baseline + 10 % | > baseline + 10 % |
| P6 | hit-ratio (rep-2 traffic) | ≥ 70 % at T+30 min | < 50 % |
| P7 | shelfd 5xx rate | ≤ 1 % | > 5 % |
| P8 | rep-3 hit-ratio (carry-over) | does not drop > 5 pp during rep-2 soak | drops > 10 pp |

P8 is the cluster-svc-specific check: rep-2 joining must not steal cache
capacity from rep-3 catastrophically.

## Rollback procedure

ETA: 3-5 min.

1. `git revert` the merge commit; push to `cicd-v2`.
2. ArgoCD reconciles `trino-replica-2`.
3. Roll the coord:
   ```bash
   kubectl -n trino-db rollout restart deployment/trino-replica-2-coordinator
   kubectl -n trino-db rollout status   deployment/trino-replica-2-coordinator --timeout=5m
   ```
4. Verify with smoke harness — but note that the rep-2 rollback restores
   the **per-pod shelf-2 pin**, not direct-S3:
   ```bash
   python /Users/aamir/trino/shelf/tools/smoke_harness.py \
       --endpoint http://shelf-2.shelf.alluxio.svc.cluster.local:9092 \
       --replica rep-2
   ```
   Exit 0 confirms the per-pod path is healthy.

## Post-cutover validation (T+30 min after soak)

Re-run the three monitoring queries on the next 30 min. Append actual
numbers to `cutover-rep2-results.md`.
