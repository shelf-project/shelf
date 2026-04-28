# cutover-rep1 — trino-replica-1 cdp → shelf-pool

**Plan**: `shelf zero-downtime + capacity` (a2fa5fe7), Stage 5.3.
**Branch**: `shelf-cutover-rep1` in `deployments-repo` (local; pushed by
Conductor A at cutover time).
**MR**: `<TBD-after-push>`.
**Soak**: **60 min** (dbt batch jobs, longer per-job; need a wider
window to capture the long tail).
**Why third**: rep-1 is currently on direct S3 (cutover landed via
MR !17873 / commit `d458f7dda2`, then reverted as `bbfe279d30` because
preview-4 was read-only and 405'd Iceberg writes — that gap is closed
by SHELF-21b in preview-6).

The diff is a single line: cdp catalog `s3.endpoint` flips from
`https://s3.ap-south-1.amazonaws.com` to
`http://shelf-pool.shelf.svc.cluster.local:9092`.

## Pre-cutover checklist (T-1h)

| # | Check | Command / source | Expected |
|---|-------|------------------|----------|
| 1 | Image tag locked | `kubectl -n shelf get sts shelf-pool -o jsonpath='{.spec.template.spec.containers[0].image}'` | `shelfd:0.1.0-preview-N` matches Stage 1 helm rev |
| 2 | Helm revision locked | `helm -n shelf history shelf` | latest rev = chart drop-in (Stage 1); no upgrade <2h before T-0 |
| 3 | rep-3 + rep-2 still green | re-run their monitoring queries | P1-P5 still PASS on both |
| 4 | Concurrent MRs check | `git -C /Users/aamir/ranger/deployments-repo log origin/cicd-v2 --oneline --since='2h ago' -- values-files/data-platform-cluster/trino-replica-1-values.yaml` | only this MR pending |
| 5 | shelf-pool endpoints | `kubectl -n shelf get endpointslice -l kubernetes.io/service-name=shelf-pool -o yaml \| grep -c 'ready: true'` | ≥ 3 |
| 6 | Image covers SHELF-21b | preview tag ≥ preview-6 (write-path verb coverage) | confirmed |
| 7 | SHELF-23 peer-fetch live | `shelf_peer_fetch_total{outcome="hit"}` rising on dashboard | non-zero |
| 8 | Smoke harness PASS — **including write-path** | `python shelf/tools/smoke_harness.py --endpoint http://shelf-pool.shelf.svc.cluster.local:9092 --replica rep-1 --include-writes` (must exercise PUT/multipart/DELETE because rep-1's prior cutover broke on writes) | exit 0; 5/5 byte-identical; PUT/DELETE 2xx |
| 9 | dbt schedule check | `airflow dags list-runs --dag-id <rep-1-dbt-batch>` — confirm no critical dbt run starts within the 60-min soak window | no overlap |
| 10 | Pin-list pre-warm | see §Pin-list pre-warm | `success_ratio ≥ 0.98` |
| 11 | Stage 0a picker is OFF | `kubectl -n shelf get cm shelf-shelfd -o yaml \| grep admission_bytes_per_sec` | unset |
| 12 | Operator quiet window | NOT 09:00-11:00 IST | confirmed |

> **Why row 8 has `--include-writes`.** rep-1 take-1 (MR !17873) failed
> exactly because the shim was read-only and Iceberg INSERTs 405'd —
> the cutover MR diff looked clean (single-line `s3.endpoint` swap)
> but the verb-set mismatch only surfaced once writes flowed. Running
> the write-path smoke against the candidate endpoint **before** merge
> catches that class in <30 s.

## Pin-list pre-warm

```bash
python /Users/aamir/trino/shelf/tools/replay_pinlist.py \
    --replica rep-1 \
    --endpoint http://shelf-pool.shelf.svc.cluster.local:9092 \
    --lookback 24h \
    --concurrency 64 \
  | tee /tmp/prewarm-rep-1-$(date -u +%Y%m%dT%H%M).json
```

Acceptance: `success_ratio ≥ 0.98`, `p99_latency_ms ≤ 150`.

rep-1's hot tables include the dbt batch outputs (e.g.
`ai_chat_spam.silver_chat_text_output_log`,
`lsq_pw.silver_prospect_activity_extension_base`). The pin-list replay
should naturally surface these from the previous 24h `trino_queries`
window.

## T-0 — cutover

1. Conductor A pushes `shelf-cutover-rep1`, opens MR (`<TBD-after-push>`),
   merges to `cicd-v2`.
2. ArgoCD reconciles `trino-replica-1`.
3. Roll the coord (and workers, since the dbt batch path involves write
   commits routed through the worker fleet):
   ```bash
   kubectl -n trino-db rollout restart deployment/trino-replica-1-coordinator
   kubectl -n trino-db rollout restart deployment/trino-replica-1-worker
   kubectl -n trino-db rollout status   deployment/trino-replica-1-coordinator --timeout=5m
   kubectl -n trino-db rollout status   deployment/trino-replica-1-worker      --timeout=10m
   ```

## Live monitoring (during 60-min soak)

```bash
COORD_IP=$(kubectl -n trino-db get pods -l release=trino-replica-1,app.kubernetes.io/component=coordinator \
    -o jsonpath='{.items[0].status.podIP}')
echo "rep-1 coord IP: $COORD_IP"
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

### Error-code histogram (write-path emphasis)

```sql
SELECT error_code, query_type, count(*) AS hits
FROM cdp.trino_logs.trino_queries
WHERE query_date >= timestamp '<T-0 UTC>'
  AND server_address = '<COORD_IP>'
  AND query_state = 'FAILED'
GROUP BY 1, 2
ORDER BY hits DESC;
```

Watch specifically for `HIVE_WRITER_CLOSE_ERROR` / 405 / `S3Exception
... Status Code: 405, Request ID: null` (the rep-1 take-1 fingerprint).

### Iceberg-maintenance smoke (T+5 min, T+30 min, T+55 min)

A no-op INSERT through rep-1's coord catches write-path regressions
without waiting for the dbt batch:

```sql
-- via trino-cli pointed at trino-data-replica-1.penpencil.co
INSERT INTO cdp.admin.iceberg_maintenance_log (event_ts, source, note)
VALUES (current_timestamp, 'shelf-cutover-rep1', 'soak-smoke');
```

Three rows expected by T+55 min. Any failure = immediate rollback.

## PASS criteria (at T+60 min)

| # | Criterion | Threshold | Hard fail |
|---|-----------|-----------|-----------|
| P1 | P95 wall_time | ≤ 1.2× baseline | > 2× |
| P2 | P99 wall_time | ≤ 1.2× baseline | > 2× |
| P3 | New failure classes | 0 | any |
| P4 | `ICEBERG_CANNOT_OPEN_SPLIT` count | 0 | ≥ 1 |
| P5 | `ICEBERG_INVALID_METADATA` count | ≤ baseline + 10 % | > baseline + 10 % |
| P6 | hit-ratio (rep-1 traffic) | ≥ 70 % at T+60 min | < 50 % |
| P7 | shelfd 5xx rate | ≤ 1 % | > 5 % |
| P8 | rep-3 + rep-2 hit-ratio carry-over | each does not drop > 5 pp | either drops > 10 pp |
| P9 | INSERT smoke (3 rows) | all 3 succeed | any failure |

## Rollback procedure

ETA: 3-5 min. **Critical**: rep-1 has the dbt write path, so coord
restart is mandatory in the rollback step (not optional).

1. `git revert` the merge commit; push to `cicd-v2`.
2. ArgoCD reconciles `trino-replica-1`.
3. Roll coord + workers:
   ```bash
   kubectl -n trino-db rollout restart deployment/trino-replica-1-coordinator
   kubectl -n trino-db rollout restart deployment/trino-replica-1-worker
   kubectl -n trino-db rollout status   deployment/trino-replica-1-coordinator --timeout=5m
   kubectl -n trino-db rollout status   deployment/trino-replica-1-worker      --timeout=10m
   ```
4. Verify with smoke harness — including write-path:
   ```bash
   python /Users/aamir/trino/shelf/tools/smoke_harness.py \
       --endpoint https://s3.ap-south-1.amazonaws.com \
       --replica rep-1 \
       --include-writes
   ```

## Post-cutover validation (T+30 min after soak)

Re-run the three monitoring queries on the next 30 min after T+60.
Specifically, verify the next dbt batch run (Airflow DAG) completes with
no shelf-attributable failures. Append actual numbers to
`cutover-rep1-results.md`.
