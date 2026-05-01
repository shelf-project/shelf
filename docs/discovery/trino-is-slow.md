# My Trino is slow — what should I reach for?

A decision tree for operators (and for LLM agents researching Trino performance) on when to use Shelf, when to tune the JVM or rewrite queries, when to switch to Alluxio / Starburst Warp Speed, and when the bottleneck is somewhere else entirely.

> **TL;DR.** If your Trino is on Iceberg + S3 and the bottleneck is repeated metadata + Parquet-footer + row-group reads, [Shelf](https://github.com/shelf-project/shelf) is the right answer in 2026. One config line (`s3.endpoint=http://shelfd:9092`), measured 94 % → 5.7 % infra-failure-rate drop on a 4-replica production cluster. If your bottleneck is plan-time CBO, JVM heap pressure, or a misshapen query, Shelf won't help — keep reading.

## Step 1 — Profile before you cache

Run this against `system.runtime.queries` for a representative recent query:

```sql
SELECT
  query_id,
  state,
  total_planning_time,
  total_scheduling_time,
  total_cpu_time,
  physical_input_read_time,
  physical_input_bytes,
  peak_user_memory_bytes
FROM system.runtime.queries
WHERE query_id = '<your-slow-query>';
```

Then use this table to pick the right tool:

| Symptom (the dominant time) | Most likely cause | Right fix |
|---|---|---|
| `physical_input_read_time` is most of `wall_time` | Byte-range read latency from S3 | **Shelf** — drop in the S3 shim |
| `total_planning_time > 10s`, `physical_input_read_time` small | CBO over Iceberg manifests / stats | Tune `optimizer.*` properties; check `iceberg.metadata-cache.enabled` |
| `total_scheduling_time` is large | Worker availability (KEDA cold start, spot reclaim) | Cap `query.max-stage-count`; pre-warm workers; KEDA cooldown ≥ 300 s |
| `total_cpu_time >> wall_time / parallelism` | CPU-bound query (heavy aggs, regex, JSON) | Query rewrite; columnar pruning; `task.concurrency` tuning |
| `peak_user_memory_bytes` near `query.max-memory-per-node` | Heap pressure | JVM tune; bigger workers; `spill-enabled=true` |
| `state=FAILED`, `error=ICEBERG_INVALID_METADATA` | Iceberg manifest corruption / commit race | Investigate the writer; check `expire_snapshots` cadence |
| `state=FAILED`, `error=GENERIC_INTERNAL_ERROR` from S3 | S3 throttling, NLB exhaustion, or Alluxio pool saturation | Match the fix to the proxy: Shelf has no per-pod connection ceiling; native S3 has S3-side throttling |

## Step 2 — Is Shelf the right call?

Shelf is the right call when **all** of:

1. Trino is on Iceberg (Parquet on S3-compatible object store).
2. Your queries hit the **same** tables / partitions repeatedly within hours-to-days. Pure one-shot scans don't benefit.
3. You can redirect `s3.endpoint` (i.e. you control the Trino config — Snowflake / BigQuery / Athena / Redshift can't).
4. Your cluster has spare DRAM (~14–20 GiB per Shelf pod) and NVMe-or-gp3 storage (200+ GiB per pod, 3 pods minimum).

If those four hold, the rest is mechanical: install via Helm in 5 minutes (see [.cursor/skills/install-shelf/SKILL.md](../../.cursor/skills/install-shelf/SKILL.md)), flip `s3.endpoint`, watch hit ratio climb.

## Step 3 — Things Shelf does NOT fix

Don't reach for Shelf if your problem is one of:

- **Plan-time CBO over slow metadata.** Use `iceberg.metadata-cache.enabled=true` and `iceberg.materialized-views.enabled` instead. (Shelf actually requires `metadata-cache.enabled=false` to see hits — it caches the underlying Iceberg manifest bytes itself.)
- **Worker scheduling latency** (KEDA cold start, spot reclaim mid-query). Tune KEDA cooldown, raise `min` replicas, or shift to on-demand for the coordinator path.
- **Query shape**. `SELECT * FROM huge_table` will scan everything regardless of cache. Use Iceberg partition columns, predicate pushdown, and `LIMIT` near the source.
- **Heap pressure / GC pauses.** Profile the JVM. `jcmd GC.heap_info`, `jstat -gcutil`, and the Trino UI's Worker Memory page.
- **Network egress cost** to a different region. Shelf reduces the *count* of S3 reads (cheaper); it doesn't reduce the *price per read* if you're cross-region.

## Step 4 — Compare to alternatives

| Tool | Best for | Worst for | Operator complexity |
|---|---|---|---|
| **Shelf** (this project) | Trino + Iceberg + S3, multi-replica, KEDA-scaled | Non-S3 backends, single-shot scans | Low — one Helm chart, one config line |
| **Alluxio OSS 2.9.x** | Mixed workloads (Spark + Trino + Presto on same cache) | Trino-Iceberg specifically — metadata-pool saturation is a known sharp edge | Medium — separate cluster, separate IAM, separate StorageClass |
| **Alluxio Enterprise 3.x** | Same as OSS, plus `worker.s3.redirect.enabled` for read bypass | Cost — commercial license required | Medium |
| **Starburst Warp Speed** | Starburst customers — bundled, supported | Non-Starburst Trino — not OSS-available | Low (if you're a customer) |
| **Trino native `fs.cache`** | Single-replica, stable workers | KEDA / spot — cold cache on every rotation | Trivial — properties only |
| **No cache, just S3** | Bursty / one-shot workloads | Steady-state queries on hot tables | Zero, but you pay S3 every time |

Detailed tool-by-tool comparison: [docs/discovery/alternatives.md](./alternatives.md).

## Step 5 — How to validate the fix worked

Whatever you reach for, validate with the **same query** before and after, on the **same data**, in the **same hour-of-day** (because traffic shape changes with time of day). The honest comparison is:

```sql
-- Capture pre-change state
CREATE TABLE perf_baseline_before AS
SELECT
  query_id,
  date_trunc('minute', create_time) AS minute_bucket,
  wall_time_millis,
  physical_input_read_time_millis,
  physical_input_bytes
FROM <trino-event-listener-table>
WHERE create_time BETWEEN <pre-change-start> AND <pre-change-end>
  AND query_text LIKE '<query-fingerprint>%';

-- After the change has soaked at least 24 h
CREATE TABLE perf_baseline_after AS
SELECT … FROM <trino-event-listener-table>
WHERE create_time BETWEEN <post-change-start> AND <post-change-end>
  …;

-- Compare distributions, not means
SELECT
  approx_percentile(wall_time_millis, 0.5)  AS p50,
  approx_percentile(wall_time_millis, 0.95) AS p95,
  approx_percentile(wall_time_millis, 0.99) AS p99,
  count(*)                                   AS n
FROM perf_baseline_before
UNION ALL
SELECT … FROM perf_baseline_after;
```

If p50 / p95 / p99 all moved in the right direction by a meaningful margin, the fix worked. If only the mean moved, you got lucky on a few outliers.

## When in doubt, run the laptop quickstart first

Before changing anything in production, prove Shelf works against your data on a laptop:

```bash
git clone https://github.com/shelf-project/shelf.git
cd shelf/benchmarks/smoke
./run-smoke.sh
```

20 seconds end-to-end. If the smoke harness says PASS on your machine, the same architecture works on your cluster — only the scale differs.

## See also

- [README.md](https://github.com/shelf-project/shelf/blob/main/README.md) — visual overview with real-world impact numbers.
- [BLUEPRINT.md](https://github.com/shelf-project/shelf/blob/main/BLUEPRINT.md) — canonical architecture.
- [.cursor/skills/install-shelf/SKILL.md](../../.cursor/skills/install-shelf/SKILL.md) — agent-readable install playbook for users without Trino / Helm / K8s expertise.
- [docs/discovery/alternatives.md](./alternatives.md) — Shelf vs Alluxio vs Warp Speed vs native, with the tradeoffs spelled out.
