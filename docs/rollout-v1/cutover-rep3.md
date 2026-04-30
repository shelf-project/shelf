# cutover-rep3 — trino-replica-3 cdp → shelf-pool

**Plan**: `shelf zero-downtime + capacity` (a2fa5fe7), Stage 5.1.
**Branch**: `shelf-cutover-rep3` in `deployments-repo` (local; pushed by
Conductor A at cutover time).
**MR**: `<TBD-after-push>` (Conductor A opens; updates this field in the
runbook).
**Soak**: **30 min**.
**Why first**: greenfield (rep-3 has never been on shelf), lowest blast
radius — origin S3 today, no per-pod-pin baggage to undo.

The diff is a single line: cdp catalog `s3.endpoint` flips from
`https://s3.ap-south-1.amazonaws.com` to
`http://shelf-pool.shelf.svc.cluster.local:9092`. Every other property,
HMS URI, IRSA wiring, partition-filter rule stays identical.

## Pre-cutover checklist (T-1h)

| # | Check | Command / source | Expected |
|---|-------|------------------|----------|
| 1 | Image tag locked | `kubectl -n shelf get sts shelf-pool -o jsonpath='{.spec.template.spec.containers[0].image}'` | `shelfd:0.1.0-preview-N` matches the tag in [Stage 1 helm rev record](../../shelfd/docs/runbooks/) |
| 2 | Helm revision locked | `helm -n shelf history shelf` | latest revision matches the chart drop-in commit (Stage 1); no upgrade <2h before T-0 |
| 3 | Concurrent MRs check | `git -C /Users/aamir/ranger/deployments-repo log origin/cicd-v2 --oneline --since='2h ago' -- values-files/data-platform-cluster/trino-replica-3-values.yaml` | only this MR pending |
| 4 | shelf-pool endpoints | `kubectl -n shelf get endpointslice -l kubernetes.io/service-name=shelf-pool -o yaml \| grep -c 'ready: true'` | ≥ 3 (3 pods Ready, all in EndpointSlice with `ready: true`) |
| 5 | SHELF-23 peer-fetch live | `kubectl -n shelf logs shelf-0 \| grep -c 'race_peer_or_origin'` for >0 in the last hour, OR confirm preview tag includes SHELF-23 fix | non-zero log lines or tag ≥ preview-N where N covers SHELF-23 |
| 6 | Smoke harness PASS | `python shelf/tools/smoke_harness.py --endpoint http://shelf-pool.shelf.svc.cluster.local:9092 --replica rep-3` (covers PUT/GET/HEAD/DELETE/multipart/list/bulk-delete) | exit 0; all 5 canonical queries byte-identical between cdp-direct and cdp-shelf catalogs |
| 7 | Pin-list pre-warm | see §Pin-list pre-warm | `success_ratio ≥ 0.98` |
| 8 | Stage 0a picker is OFF | `kubectl -n shelf get cm shelf-shelfd -o yaml \| grep admission_bytes_per_sec` | unset (or commented out / null) |
| 9 | Operator quiet window | calendar / Slack #data-platform | NOT 09:00-11:00 IST (peak per g1) |

If any row is red, **reschedule**. Do not interpret around contamination
(g2 — locked window discipline).

## Pin-list pre-warm

Run before T-0 to flip cold-start cost from O(window) to O(seconds).

> **Tool dependency**: `shelf/tools/replay_pinlist.py` is Agent D's
> deliverable. If the file is missing at the time you read this runbook,
> see when committed via `git -C /Users/aamir/trino log shelf/tools/`;
> until then, fall back to manual `aws s3 cp` of the top hot tables
> from `cdp.trino_logs.trino_queries` (Stage 3a fallback in plan).

```bash
python /Users/aamir/trino/shelf/tools/replay_pinlist.py \
    --replica rep-3 \
    --endpoint http://shelf-pool.shelf.svc.cluster.local:9092 \
    --lookback 24h \
    --concurrency 64 \
  | tee /tmp/prewarm-rep-3-$(date -u +%Y%m%dT%H%M).json
```

Acceptance: `success_ratio ≥ 0.98` AND `p99_latency_ms ≤ 150`.

## T-0 — cutover

1. Conductor A pushes `shelf-cutover-rep3` and opens MR (`<TBD-after-push>`).
2. Merge to `cicd-v2`. ArgoCD reconciles `trino-replica-3` Helm release.
3. Roll the coordinator to pick up the new `s3.endpoint` (workers re-resolve
   on next split):
   ```bash
   kubectl -n trino-db rollout restart deployment/trino-replica-3-coordinator
   kubectl -n trino-db rollout status   deployment/trino-replica-3-coordinator --timeout=5m
   ```
4. Stamp T-0 in `agent-e-status.md` as the cutover moment.

## Live monitoring (during soak)

All times relative to T-0. Run these against `cdp.trino_logs.trino_queries`
(MySQL ingest lags ~30 min — for the first 30 min of soak, also check
shelfd `:9090/metrics` `shelf_hits_total` / `shelf_misses_total` and the
`shelf-overview` Grafana dashboard for real-time signal).

`server_address` filter: resolve rep-3's coord pod IP first.

```bash
COORD_IP=$(kubectl -n trino-db get pods -l release=trino-replica-3,app.kubernetes.io/component=coordinator \
    -o jsonpath='{.items[0].status.podIP}')
echo "rep-3 coord IP: $COORD_IP"
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

### Error-code histogram (catches new failure classes)

```sql
SELECT error_code, count(*) AS hits
FROM cdp.trino_logs.trino_queries
WHERE query_date >= timestamp '<T-0 UTC>'
  AND server_address = '<COORD_IP>'
  AND query_state = 'FAILED'
GROUP BY 1
ORDER BY hits DESC;
```

Compare against the same 30-min window 24h ago on the same coord IP.

## PASS criteria (at T+30 min)

| # | Criterion | Threshold | Hard fail |
|---|-----------|-----------|-----------|
| P1 | P95 wall_time | ≤ 1.2× baseline (24h-prior same window) | > 2× |
| P2 | P99 wall_time | ≤ 1.2× baseline | > 2× |
| P3 | New failure classes | 0 (no `error_code` value present in soak that was absent in baseline) | any |
| P4 | `ICEBERG_CANNOT_OPEN_SPLIT` count | 0 | ≥ 1 |
| P5 | `ICEBERG_INVALID_METADATA` count | ≤ baseline + 10 % | > baseline + 10 % |
| P6 | hit-ratio (rep-3 traffic) | ≥ 70 % at T+30 min | < 50 % |
| P7 | shelfd 5xx rate | ≤ 1 % | > 5 % |

If P1–P5 are green: **proceed to rep-2** (cutover-rep2.md). If any hard-fail
condition triggers during the soak, immediately roll back (next section).

## Rollback procedure

ETA: 3-5 min from decision to ArgoCD-reconciled-revert.

1. Conductor A reverts the merge in `deployments-repo`:
   ```bash
   git -C /Users/aamir/ranger/deployments-repo checkout cicd-v2
   git revert <merge-commit-sha>
   git push origin cicd-v2
   ```
   (alternatively, ArgoCD UI revert if MR was a fast-forward and revert
   button is available — same end state).
2. ArgoCD reconciles `trino-replica-3` (3-5 min).
3. Force coord restart so the in-memory S3 client picks up the new
   endpoint immediately:
   ```bash
   kubectl -n trino-db rollout restart deployment/trino-replica-3-coordinator
   kubectl -n trino-db rollout status   deployment/trino-replica-3-coordinator --timeout=5m
   ```
4. Verify by re-running the smoke harness against the **direct-S3**
   endpoint (post-rollback state):
   ```bash
   python /Users/aamir/trino/shelf/tools/smoke_harness.py \
       --endpoint https://s3.ap-south-1.amazonaws.com \
       --replica rep-3
   ```
   Exit 0 confirms direct-S3 path is healthy. Queries in flight during
   the coord restart will fail and need retry by the caller.

## Post-cutover validation (T+30 min after soak ends)

Re-run the three monitoring queries above on the **next 30 min** after the
soak window closes. Expected: criteria P1–P7 still green; no late-arriving
new failure classes.

Append the actual numbers (P50/P95/P99 wall_time, hit-ratio, failed_pct,
error_code histogram diff vs baseline) to
`shelf/docs/rollout-v1/cutover-rep3-results.md` (create from this template;
same shape as Stage 2 locked-window report).

If any post-soak number regresses past PASS, the rollback procedure above
is still valid — ArgoCD reconcile is idempotent.
