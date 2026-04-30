# sre-1 workload confirmation — per-replica `monthly_reads` + working set

**Status**: pending — blocks rep-2 cutover at T-24h.
**Owner**: sre-1.
**Requested by**: shelf-core (rollout-v1 pre-req).
**Expected turnaround**: 1 business day.

## Why we need this

The capacity plan in [`../capacity.md`](../capacity.md) §4
extrapolates rep-2's measured 900 TiB/month read volume to 3.6 PiB
across four replicas using a 4× multiplier. This is a placeholder.
Before we size `values-prod.yaml` and commit to rolling out, we need
the **per-replica** breakdown, because:

- If rep-0 alone is 2.5 PiB/month (users more active than rep-2),
  our 5-pod sizing under-provisions. NVMe fills; hit rate collapses
  during cutover; we roll back.
- If rep-3 is 300 TiB/month (dev-heavy), the 4× is over by 3× and
  we're over-provisioned but no correctness risk.
- The asymmetry also determines rollout **order** — smallest-
  workload-first after rep-2 minimises blast radius if a bug shows
  up.

## What we're asking for

One table, one column per replica, rows as below. Aggregate over
a 30-day window (the previous calendar month is fine; the last 30
days rolling is better if cheap). Pull from
`QueryCompletedEvent` — the same table rep-2's 900 TiB figure
came from.

| Metric                                                   | rep-0 | rep-1 | rep-2 | rep-3 | Source table / query hint            |
| -------------------------------------------------------- | ----- | ----- | ----- | ----- | ------------------------------------ |
| `physical_input_bytes` summed, Iceberg catalog only      |       |       | ~900 TiB | |                                   | `sum(physicalInputBytes) where catalog='iceberg'` |
| Query count                                              |       |       |       |       | `count(*) where catalog='iceberg'`   |
| p50 query runtime                                        |       |       |       |       | `approx_percentile(..., 0.5)`        |
| p99 query runtime                                        |       |       |       |       | `approx_percentile(..., 0.99)`       |
| Unique Iceberg tables touched                            |       |       |       |       | `count(distinct table)`              |
| Top 10 tables by bytes read (attach as a separate list)  |       |       |       |       | `group by table order by sum desc limit 10` |

### Reference query shape (copy / paste into Trino on the QCE table)

```sql
SELECT
    replica,
    sum(physical_input_bytes) / 1024.0 / 1024.0 / 1024.0 / 1024.0 AS tib_read_30d,
    count(*) AS query_count_30d,
    approx_percentile(wall_time_millis, 0.5)  AS p50_ms,
    approx_percentile(wall_time_millis, 0.99) AS p99_ms,
    count(distinct(catalog || '.' || schema_ || '.' || table_name)) AS unique_tables
FROM cdp.trino_logs.query_completed_event
WHERE
    catalog = 'iceberg'
    AND query_state = 'FINISHED'
    AND query_date >= current_date - INTERVAL '30' DAY
GROUP BY replica
ORDER BY replica;
```

(`replica` column is the dimension we filter `QueryCompletedEvent`
by in Grafana already — the SRE Grafana dashboard "Trino → per-replica
query load" uses it. If the column name is different in the QCE
schema, substitute.)

### Working-set question (harder — optional, nice-to-have)

The capacity formula needs `W`, the 7-day unique row-group byte-
count. Rep-2's `W=1.2 TiB` is a placeholder from a rough pass.
A true `W` per replica would come from running
[`../../benchmarks/trino_logs/`](../../benchmarks/trino_logs/)'s
`shelf-replay analyze` subcommand over each replica's 7-day trace
+ Iceberg manifests.

If sre-1 has capacity, we'd love this number per replica before
cutover. If not, we'll run it against the per-replica trace sre-1
exports for pre-warming anyway; it's just a longer feedback loop
(a few hours of analyze time) that blocks T-24h pre-warm rather
than capacity sizing.

## How we'll use the answer

- **`monthly_reads` per replica** → replaces the 4× multiplier in
  `docs/capacity.md` §4. If total across four replicas > 4.5 PiB,
  bump `values-prod.yaml:storage.size` from 500 GiB to 640 GiB
  (keeps the 30 % NVMe headroom the capacity formula requires).
- **Query count** → informs the `per_pod_hot` sizing and HRW
  balance expectations.
- **Top 10 tables by bytes** per replica → feeds the pre-warm
  trace prioritisation; we spend the pre-warm budget on the hot
  tables first.
- **p99** → baseline for the `ShelfReadPathP99Degraded` alert
  per replica. If rep-0's current direct-to-S3 p99 is 200 ms, our
  100 ms alert threshold is wrong for rep-0 and we need to scope
  the alert.
- **Unique tables** → sanity check on the pin list budget;
  `docs/capacity.md` §6 caps pinned bytes at 20 % of NVMe per
  pod.

## Delivery

Reply in the shelf-core Slack channel with the filled table + top-
10 CSVs attached. `shelf-core` owns writing those numbers back
into `docs/capacity.md` §4 (the table marked "confirm with sre-1"
is the target replacement point) before kicking off rep-2 cutover.

## Escalation

If you hit blockers with the QCE query (schema change, permission
on the `cdp.trino_logs` catalog, slow query timeout), ping
`shelf-oncall` — we have a pre-baked version that reads a 30-day
rollup partition instead of the raw table and is ~50× faster.
