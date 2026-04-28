# cutover-rep0 — trino-replica-0 cdp → shelf-pool

**Plan**: `shelf zero-downtime + capacity` (a2fa5fe7), Stage 5.4.
**Branch**: `shelf-cutover-rep0` in `deployments-repo` (local; pushed by
Conductor A at cutover time).
**MR**: `<TBD-after-push>`.
**Soak**: **90 min** (high-concurrency case; longest tail).
**Why last**: rep-0 is the high-concurrency replica — the v1 plan named
it as the "rep-0 worst-case" (1 TiB+ batch scans, the working set is
the largest of the four). Going last lets rep-3/2/1 pre-warm shelf-pool
before rep-0 piles its load on top. The 1.2x baseline / 2x hard-fail
gate is named explicitly in the plan for this stage and is the hardest
to meet of the four.

The diff is a single line: cdp catalog `s3.endpoint` flips from
`https://s3.ap-south-1.amazonaws.com` to
`http://shelf-pool.shelf.svc.cluster.local:9092`.

> **History note.** rep-0 had a per-pod cutover (commit `4174d15fe3` on
> branch `feat/trino-replica-0-cdp-shelf`) prepared but never merged to
> `cicd-v2`. The cluster-svc cutover here supersedes that branch.

## Pre-cutover checklist (T-1h)

| # | Check | Command / source | Expected |
|---|-------|------------------|----------|
| 1 | Image tag locked | `kubectl -n shelf get sts shelf-pool -o jsonpath='{.spec.template.spec.containers[0].image}'` | `shelfd:0.1.0-preview-N` |
| 2 | Helm revision locked | `helm -n shelf history shelf` | latest rev = chart drop-in (Stage 1); no upgrade <2h before T-0 |
| 3 | rep-3 + rep-2 + rep-1 still green | re-run their monitoring queries | P1-P5 PASS on all three |
| 4 | shelf-pool capacity headroom | `kubectl -n shelf top pod -l app.kubernetes.io/name=shelf-pool` | per-pod RSS ≤ 22 GiB; combined NVMe used ≤ 70 % of capacity |
| 5 | Stage 4 over-provision applied | `kubectl -n shelf get sts shelf-pool -o jsonpath='{.spec.replicas}'` | ≥ 4 (per Stage 4: 3→4 replicas, maxConnections 256→512) |
| 6 | Concurrent MRs check | `git -C /Users/aamir/ranger/deployments-repo log origin/cicd-v2 --oneline --since='2h ago' -- values-files/data-platform-cluster/trino-replica-0-values.yaml` | only this MR pending |
| 7 | shelf-pool endpoints | `kubectl -n shelf get endpointslice -l kubernetes.io/service-name=shelf-pool -o yaml \| grep -c 'ready: true'` | matches replica count |
| 8 | SHELF-23 peer-fetch live | `shelf_peer_fetch_total{outcome="hit"}` rising on dashboard | non-zero |
| 9 | Smoke harness PASS | `python shelf/tools/smoke_harness.py --endpoint http://shelf-pool.shelf.svc.cluster.local:9092 --replica rep-0 --include-writes` | exit 0; 5/5 byte-identical |
| 10 | Pin-list pre-warm — **mandatory for rep-0** | see §Pin-list pre-warm | `success_ratio ≥ 0.98` |
| 11 | Stage 0a picker is OFF | `kubectl -n shelf get cm shelf-shelfd -o yaml \| grep admission_bytes_per_sec` | unset |
| 12 | Operator quiet window | NOT 09:00-11:00 IST (peak); rep-0 is the high-concurrency case so a low-traffic window matters most here | confirmed |
| 13 | rep-0 baseline numbers captured | `cdp.trino_logs.trino_queries` baseline window: same wall-clock 24h prior, same coord IP if stable | numbers recorded for P1/P2 comparison |

> **Why row 10 is mandatory.** rep-0 has the largest working set; a
> cold-start cluster-svc cutover would mean every coord-side metadata
> read goes to S3 origin in lockstep — concentrated GET burst that
> looks exactly like the 04:15-06:00 UTC chaos window the v1 plan was
> revised away from. Pin-list replay flips cold-start cost from
> O(window) to O(seconds).

## Pin-list pre-warm

```bash
python /Users/aamir/trino/shelf/tools/replay_pinlist.py \
    --replica rep-0 \
    --endpoint http://shelf-pool.shelf.svc.cluster.local:9092 \
    --lookback 24h \
    --concurrency 64 \
  | tee /tmp/prewarm-rep-0-$(date -u +%Y%m%dT%H%M).json
```

Acceptance: `success_ratio ≥ 0.98`, `p99_latency_ms ≤ 150`. Also check
shelf-pool's combined NVMe utilization climbs but stays ≤ 80 % capacity
during pre-warm — if it hits 80 %, hold cutover and revisit Stage 4
over-provision (the chart's `cache.pools.rowgroup.dramSizeBytes` and
NVMe capacity bounds need to be widened before rep-0 traffic flips).

## T-0 — cutover

1. Conductor A pushes `shelf-cutover-rep0`, opens MR (`<TBD-after-push>`),
   merges to `cicd-v2`.
2. ArgoCD reconciles `trino-replica-0`.
3. Roll coord + workers:
   ```bash
   kubectl -n trino-db rollout restart deployment/trino-replica-0-coordinator
   kubectl -n trino-db rollout restart deployment/trino-replica-0-worker
   kubectl -n trino-db rollout status   deployment/trino-replica-0-coordinator --timeout=5m
   kubectl -n trino-db rollout status   deployment/trino-replica-0-worker      --timeout=10m
   ```

## Live monitoring (during 90-min soak)

```bash
COORD_IP=$(kubectl -n trino-db get pods -l release=trino-replica-0,app.kubernetes.io/component=coordinator \
    -o jsonpath='{.items[0].status.podIP}')
echo "rep-0 coord IP: $COORD_IP"
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
LIMIT 90;
```

### P95 / P99 wall time (the hard gate)

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

Compare against the 24h-prior same wall-clock window on the same coord
IP (or, if the coord pod has rotated, on the rep-0 pool tag).

### Error-code histogram

```sql
SELECT error_code, query_type, count(*) AS hits
FROM cdp.trino_logs.trino_queries
WHERE query_date >= timestamp '<T-0 UTC>'
  AND server_address = '<COORD_IP>'
  AND query_state = 'FAILED'
GROUP BY 1, 2
ORDER BY hits DESC;
```

### Long-tail watch (rep-0-specific)

rep-0's worst case is the 1 TiB+ batch scan. P99 wall_time alone can
hide a single catastrophic outlier, so also pull the top 5 longest
queries during the soak:

```sql
SELECT query_id, wall_time_millis, query_state, error_code,
       physical_input_bytes, output_rows
FROM cdp.trino_logs.trino_queries
WHERE query_date >= timestamp '<T-0 UTC>'
  AND server_address = '<COORD_IP>'
ORDER BY wall_time_millis DESC
LIMIT 5;
```

If any of these are > 2× the baseline equivalent (24h-prior top-5),
trigger immediate rollback regardless of aggregate P99.

## PASS criteria (at T+90 min) — strictest gate of the four cutovers

| # | Criterion | Threshold | Hard fail |
|---|-----------|-----------|-----------|
| P1 | P95 wall_time | **≤ 1.2× baseline** (named explicitly in plan §Stage 5.4) | **> 2×** |
| P2 | P99 wall_time | **≤ 1.2× baseline** | **> 2×** |
| P3 | New failure classes | 0 | any |
| P4 | `ICEBERG_CANNOT_OPEN_SPLIT` count | 0 | ≥ 1 |
| P5 | `ICEBERG_INVALID_METADATA` count | ≤ baseline + 10 % | > baseline + 10 % |
| P6 | hit-ratio (rep-0 traffic) | ≥ 70 % at T+90 min | < 50 % |
| P7 | shelfd 5xx rate | ≤ 1 % | > 5 % |
| P8 | rep-3 + rep-2 + rep-1 carry-over | each hit-ratio does not drop > 5 pp | any drops > 10 pp |
| P9 | Top-5 longest queries | each ≤ 2× the 24h-prior top-5 equivalent | any > 2× |
| P10 | shelf-pool per-pod RSS | ≤ 24 GiB | any pod OOMKilled |

If P1–P5 are green at T+90 and P9/P10 stayed green throughout: rep-0 is
the final replica on shelf-pool. Move to Stage 6 (zero-downtime
verification).

## Rollback procedure

ETA: 3-5 min. Same shape as rep-1's rollback (rep-0 also has the write
path, so coord+worker restart is mandatory).

1. `git revert` the merge commit; push to `cicd-v2`.
2. ArgoCD reconciles `trino-replica-0`.
3. Roll coord + workers:
   ```bash
   kubectl -n trino-db rollout restart deployment/trino-replica-0-coordinator
   kubectl -n trino-db rollout restart deployment/trino-replica-0-worker
   kubectl -n trino-db rollout status   deployment/trino-replica-0-coordinator --timeout=5m
   kubectl -n trino-db rollout status   deployment/trino-replica-0-worker      --timeout=10m
   ```
4. Verify with smoke harness against direct S3:
   ```bash
   python /Users/aamir/trino/shelf/tools/smoke_harness.py \
       --endpoint https://s3.ap-south-1.amazonaws.com \
       --replica rep-0 \
       --include-writes
   ```

## Post-cutover validation (T+30 min after soak)

Re-run the four monitoring queries on the next 30 min after T+90.
Especially the top-5 longest-query query — if a 1 TiB+ batch scan ran
during the soak, its tail can extend past the soak window. Append
actual numbers to `cutover-rep0-results.md`.

After 30 min of post-soak green, the rollout is complete and Stage 6
(deliberate `kubectl delete pod shelf-2` test) becomes the next item in
the plan.
