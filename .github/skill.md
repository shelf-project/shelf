---
name: trino-shelf-perf-check
description: Check and compare Trino replica performance before and after Shelf (read-cache) integration. Use this skill whenever the user asks to "check perf", "check today", "how is Shelf doing", "compare replica performance", "check replica1", or any variation of monitoring Trino cluster performance with or without Shelf cache. Also trigger when the user asks about read throughput, cache hit rates, query latency trends, or failure rate analysis on Trino replicas. This skill contains all validated SQL queries, methodology, and interpretation guidance for the Shelf performance analysis workflow.
---

# Trino Shelf performance check

## Overview

This skill runs a standardized performance check on Trino replicas, comparing Shelf (read-cache) vs direct-S3 performance. It uses `cdp.trino_logs.trino_queries` as the primary data source via the Trino MCP tool (`mcp-trino:execute_query`).

## Prerequisites

- **Trino MCP tool** must be connected (`mcp-trino:execute_query`)
- If unavailable, fall back to Grafana Prometheus queries for infra-level metrics (CPU, memory, pod count) but note that query-level performance metrics won't be available
- The Prometheus datasource for container metrics is `ddy2eykq2tfy8a`; the JMX metrics datasource is `ady2f2kp94hs0c` (may be down — verify with an instant query first)

## Key context

- `query_date` column is stored in **UTC**. To convert to IST: `CAST(query_date AS TIMESTAMP) + INTERVAL '5' HOUR + INTERVAL '30' MINUTE`
- Partition column is `query_date` — always filter on it to avoid full scans
- `environment` column identifies replicas: `replica0`, `replica1`, `replica3`
- Shelf was first added to replica1 on **Apr 28, 2026** at 4:45 PM IST, upgraded Apr 30 at 2 PM IST
- Shelf was briefly on replica0 twice (May 1 and May 4-5) but reverted both times
- Pre-Shelf baseline window (validated): **Apr 24-27, 2026** (Thu-Sun)

## Step 1: Check data freshness

Always run this first to confirm how current the logs are.

```sql
SELECT 
    environment,
    CAST(MAX(query_date) AS TIMESTAMP) + INTERVAL '5' HOUR + INTERVAL '30' MINUTE AS max_ist
FROM cdp.trino_logs.trino_queries
WHERE query_date >= TIMESTAMP '{today} 00:00:00'
  AND environment IN ('replica0', 'replica1')
GROUP BY 1
```

Replace `{today}` with the current date in `YYYY-MM-DD` format.

## Step 2: Today's aggregate for both replicas

```sql
SELECT 
    environment,
    COUNT(*) AS queries,
    ROUND(100.0 * SUM(CASE WHEN query_state = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*), 1) AS fail_pct,
    ROUND(AVG(wall_time_millis) / 1000.0, 2) AS avg_wall,
    ROUND(APPROX_PERCENTILE(wall_time_millis, 0.5) / 1000.0, 2) AS p50_wall,
    ROUND(APPROX_PERCENTILE(wall_time_millis, 0.95) / 1000.0, 2) AS p95_wall,
    ROUND(AVG(physical_input_read_time_millis) / 1000.0, 2) AS avg_read,
    ROUND(APPROX_PERCENTILE(physical_input_read_time_millis, 0.5) / 1000.0, 2) AS p50_read,
    ROUND(AVG(physical_input_bytes) / (1024.0*1024*1024), 2) AS avg_scan_gb,
    ROUND(AVG(cpu_time_millis) / 1000.0, 2) AS avg_cpu,
    ROUND(APPROX_PERCENTILE(
        CASE WHEN query_state = 'FINISHED'
             AND physical_input_read_time_millis > 100 
             AND physical_input_bytes > BIGINT '1073741824'
        THEN physical_input_bytes * 1000.0 / physical_input_read_time_millis / (1024*1024) 
        END, 0.5), 2) AS p50_throughput_mbps
FROM cdp.trino_logs.trino_queries
WHERE query_date >= TIMESTAMP '{today} 00:00:00'
  AND environment IN ('replica0', 'replica1')
  AND query_type IN ('SELECT', 'INSERT')
GROUP BY 1
```

## Step 3: Daily trend (last 7 days)

```sql
SELECT CAST(query_date AS DATE) AS d, environment,
    COUNT(*) AS q,
    ROUND(100.0 * SUM(CASE WHEN query_state = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*), 1) AS fp,
    ROUND(APPROX_PERCENTILE(wall_time_millis, 0.5) / 1000.0, 2) AS p50w,
    ROUND(APPROX_PERCENTILE(wall_time_millis, 0.95) / 1000.0, 2) AS p95w,
    ROUND(AVG(physical_input_read_time_millis) / 1000.0, 2) AS avgr,
    ROUND(AVG(cpu_time_millis) / 1000.0, 2) AS cpu,
    ROUND(APPROX_PERCENTILE(
        CASE WHEN query_state = 'FINISHED'
             AND physical_input_read_time_millis > 100 
             AND physical_input_bytes > BIGINT '1073741824'
        THEN physical_input_bytes * 1000.0 / physical_input_read_time_millis / (1024*1024) 
        END, 0.5), 2) AS tp
FROM cdp.trino_logs.trino_queries
WHERE environment IN ('replica0', 'replica1')
  AND query_type IN ('SELECT', 'INSERT')
  AND CAST(query_date AS DATE) BETWEEN CURRENT_DATE - INTERVAL '7' DAY AND CURRENT_DATE
GROUP BY 1, 2 ORDER BY 2, 1
```

## Step 4: Hourly breakdown for today

Run this for the replica being investigated (replace `{replica}` with `replica0` or `replica1`).

```sql
SELECT 
    HOUR(CAST(query_date AS TIMESTAMP) + INTERVAL '5' HOUR + INTERVAL '30' MINUTE) AS hour_ist,
    COUNT(*) AS queries,
    ROUND(100.0 * SUM(CASE WHEN query_state = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*), 1) AS fail_pct,
    ROUND(APPROX_PERCENTILE(wall_time_millis, 0.5) / 1000.0, 2) AS p50_wall,
    ROUND(APPROX_PERCENTILE(wall_time_millis, 0.95) / 1000.0, 2) AS p95_wall,
    ROUND(AVG(physical_input_read_time_millis) / 1000.0, 2) AS avg_read,
    ROUND(AVG(cpu_time_millis) / 1000.0, 2) AS avg_cpu,
    ROUND(APPROX_PERCENTILE(
        CASE WHEN query_state = 'FINISHED'
             AND physical_input_read_time_millis > 100 
             AND physical_input_bytes > BIGINT '1073741824'
        THEN physical_input_bytes * 1000.0 / physical_input_read_time_millis / (1024*1024) 
        END, 0.5), 2) AS p50_tp
FROM cdp.trino_logs.trino_queries
WHERE environment = '{replica}'
  AND query_type IN ('SELECT', 'INSERT')
  AND CAST(query_date AS DATE) = CURRENT_DATE
GROUP BY 1 ORDER BY 1
```

## Step 5: Error breakdown

```sql
SELECT error_code, COUNT(*) AS cnt
FROM cdp.trino_logs.trino_queries
WHERE environment = '{replica}'
  AND query_state = 'FAILED'
  AND CAST(query_date AS DATE) = CURRENT_DATE
GROUP BY 1 ORDER BY cnt DESC LIMIT 10
```

### Error codes to watch for (Shelf-related)

| Error code | Shelf signal | Action |
|---|---|---|
| `ICEBERG_CANNOT_OPEN_SPLIT` | Cache serving stale data file refs after compaction | Cache invalidation issue — check Shelf version |
| `ICEBERG_INVALID_METADATA` | Cache serving stale metadata/manifest files | Same as above |
| `PAGE_TRANSPORT_TIMEOUT` | Shelf latency causing inter-worker data exchange timeouts | Shelf overhead too high — consider revert |
| `SERVER_SHUTTING_DOWN` | If elevated, Shelf may be causing pod instability | Check pod restart frequency |
| `NO_NODES_AVAILABLE` | Not Shelf-specific, KEDA scaling gap | Check KEDA triggers |
| `ADMINISTRATIVELY_KILLED` | Normal — query timeout enforcement | Only concerning if counts spike with Shelf |
| `ICEBERG_MISSING_METADATA` | Not Shelf-related — stale HMS entries for dropped/recreated tables | Separate investigation needed |

## Step 6: Controlled cohort comparison (for Shelf verdict)

This is the rigorous before/after analysis. Use DOW-matched windows and per-user stratification.

### Methodology rules

1. **Primary metric**: p50 read throughput (bytes per ms of read time) — the direct cache effectiveness measure
2. **Queue time must be verified as non-confound**: check `AVG(queued_time_millis)` in both windows — if non-zero, it's a confounding variable
3. **Same users only**: INNER JOIN users present in both windows with ≥100 finished queries
4. **FINISHED queries only** for performance metrics; ALL queries for failure rate
5. **DOW-aligned windows**: compare Thu-Sun to Thu-Sun, weekday to weekday

### The query

```sql
WITH window_a AS (
    SELECT "user",
        COUNT(*) AS total_q,
        SUM(CASE WHEN query_state='FINISHED' THEN 1 ELSE 0 END) AS finished_q,
        ROUND(100.0*SUM(CASE WHEN query_state='FAILED' THEN 1 ELSE 0 END)/COUNT(*),2) AS fail_pct,
        ROUND(APPROX_PERCENTILE(CASE WHEN query_state='FINISHED' THEN wall_time_millis END,0.5)/1000.0,2) AS p50_wall,
        ROUND(APPROX_PERCENTILE(CASE WHEN query_state='FINISHED' THEN wall_time_millis END,0.95)/1000.0,2) AS p95_wall,
        ROUND(AVG(CASE WHEN query_state='FINISHED' THEN physical_input_read_time_millis END)/1000.0,2) AS avg_read,
        ROUND(APPROX_PERCENTILE(
            CASE WHEN query_state='FINISHED' AND physical_input_read_time_millis>100 
                 AND physical_input_bytes>BIGINT '1073741824'
            THEN physical_input_bytes*1000.0/physical_input_read_time_millis/(1024*1024) END,0.5),2) AS p50_tp,
        ROUND(AVG(CASE WHEN query_state='FINISHED' THEN queued_time_millis END)/1000.0,3) AS avg_queue
    FROM cdp.trino_logs.trino_queries
    WHERE environment='replica1' AND query_type IN ('SELECT','INSERT')
      AND CAST(query_date AS DATE) BETWEEN DATE '{window_a_start}' AND DATE '{window_a_end}'
    GROUP BY "user"
    HAVING SUM(CASE WHEN query_state='FINISHED' THEN 1 ELSE 0 END) >= 100
),
window_b AS (
    SELECT "user",
        COUNT(*) AS total_q,
        SUM(CASE WHEN query_state='FINISHED' THEN 1 ELSE 0 END) AS finished_q,
        ROUND(100.0*SUM(CASE WHEN query_state='FAILED' THEN 1 ELSE 0 END)/COUNT(*),2) AS fail_pct,
        ROUND(APPROX_PERCENTILE(CASE WHEN query_state='FINISHED' THEN wall_time_millis END,0.5)/1000.0,2) AS p50_wall,
        ROUND(APPROX_PERCENTILE(CASE WHEN query_state='FINISHED' THEN wall_time_millis END,0.95)/1000.0,2) AS p95_wall,
        ROUND(AVG(CASE WHEN query_state='FINISHED' THEN physical_input_read_time_millis END)/1000.0,2) AS avg_read,
        ROUND(APPROX_PERCENTILE(
            CASE WHEN query_state='FINISHED' AND physical_input_read_time_millis>100 
                 AND physical_input_bytes>BIGINT '1073741824'
            THEN physical_input_bytes*1000.0/physical_input_read_time_millis/(1024*1024) END,0.5),2) AS p50_tp,
        ROUND(AVG(CASE WHEN query_state='FINISHED' THEN queued_time_millis END)/1000.0,3) AS avg_queue
    FROM cdp.trino_logs.trino_queries
    WHERE environment='replica1' AND query_type IN ('SELECT','INSERT')
      AND CAST(query_date AS DATE) BETWEEN DATE '{window_b_start}' AND DATE '{window_b_end}'
    GROUP BY "user"
    HAVING SUM(CASE WHEN query_state='FINISHED' THEN 1 ELSE 0 END) >= 100
)
SELECT 
    a."user",
    a.finished_q AS a_fin, b.finished_q AS b_fin,
    a.fail_pct AS a_fp, b.fail_pct AS b_fp,
    a.p50_wall AS a_p50w, b.p50_wall AS b_p50w,
    a.p95_wall AS a_p95w, b.p95_wall AS b_p95w,
    a.avg_read AS a_avgr, b.avg_read AS b_avgr,
    a.p50_tp AS a_tp, b.p50_tp AS b_tp,
    a.avg_queue AS a_q, b.avg_queue AS b_q
FROM window_a a
INNER JOIN window_b b ON a."user" = b."user"
ORDER BY a.finished_q DESC
```

Replace `{window_a_start/end}` with the pre-Shelf baseline dates (e.g., `2026-04-24` to `2026-04-27`) and `{window_b_start/end}` with the post-Shelf comparison dates.

## Step 7: Step-change detection

Use this to check if a Shelf version upgrade or NVMe expansion produced a measurable effect. Look for a discontinuity in the daily throughput band.

```sql
SELECT CAST(query_date AS DATE) AS d,
    ROUND(APPROX_PERCENTILE(
        CASE WHEN query_state='FINISHED' AND physical_input_read_time_millis>100 
             AND physical_input_bytes>BIGINT '1073741824'
        THEN physical_input_bytes*1000.0/physical_input_read_time_millis/(1024*1024) END,0.25),2) AS p25_tp,
    ROUND(APPROX_PERCENTILE(
        CASE WHEN query_state='FINISHED' AND physical_input_read_time_millis>100 
             AND physical_input_bytes>BIGINT '1073741824'
        THEN physical_input_bytes*1000.0/physical_input_read_time_millis/(1024*1024) END,0.5),2) AS p50_tp,
    ROUND(APPROX_PERCENTILE(
        CASE WHEN query_state='FINISHED' AND physical_input_read_time_millis>100 
             AND physical_input_bytes>BIGINT '1073741824'
        THEN physical_input_bytes*1000.0/physical_input_read_time_millis/(1024*1024) END,0.75),2) AS p75_tp
FROM cdp.trino_logs.trino_queries
WHERE environment='replica1' AND query_type IN ('SELECT','INSERT')
  AND CAST(query_date AS DATE) BETWEEN DATE '{start}' AND DATE '{end}'
GROUP BY 1 ORDER BY 1
```

A real configuration change shows as a **jump** in the band, not a gradual trend.

## Interpretation guide

### Read throughput (p50_throughput_mbps) — THE Shelf metric

| Value | Interpretation |
|---|---|
| >28 MB/s | Baseline-level (direct S3 performance on replica1) |
| 20-28 MB/s | Cache helping but working set exceeds capacity |
| 15-20 MB/s | Significant throughput regression — cache thrashing likely |
| <15 MB/s | Severe — Shelf is actively degrading performance |

### Pre-Shelf baselines (replica1, Apr 24-27 2026)

| Metric | Value |
|---|---|
| p50 wall time | ~40s |
| p95 wall time | ~1,387s |
| Avg read latency | ~597s |
| p50 throughput | ~29 MB/s |
| Avg CPU | ~303s |
| Failure rate | ~29.1% |

### Pre-Shelf baselines (replica0, no Shelf)

| Metric | Value |
|---|---|
| p50 wall time | ~1.5s |
| p95 wall time | ~52s |
| p50 throughput | ~33-37 MB/s |

### What to look for in hourly data

- **Early morning (5-7 AM IST)**: Low load, cache serving from overnight dbt runs. If throughput is high here (~25-30 MB/s) but drops during business hours, it's a cache-capacity problem (working set exceeds cache under load)
- **Peak hours (9 AM - 3 PM IST)**: This is where thrashing shows up. If throughput drops below 15 MB/s during peak, cache eviction is overwhelming
- **Sudden failure rate spikes in single hours**: Check for PAGE_TRANSPORT_TIMEOUT or NO_NODES_AVAILABLE in that hour specifically

### Scorecard for "is Shelf worth continuing"

Score across these 5 dimensions. A functioning cache should improve the majority:

1. **p50 throughput** (primary): Higher = better. Must be above pre-Shelf baseline for Shelf to be justified
2. **Failure rate**: Should not increase with Shelf
3. **p95 wall time**: Shelf should improve tail latency via metadata caching
4. **Avg read latency**: Should decrease if cache is hitting
5. **Queue time**: Must be verified as zero in both windows to eliminate as confound

## Presentation

After collecting the data, present results using the `visualize:show_widget` tool with:
- Metric cards for today's headline numbers with delta from baseline
- Daily trend table for the last 7 days
- Chart.js line chart for throughput trend (with baseline reference line at 29 MB/s)
- Error summary if any Shelf-specific errors detected

Compare against the baselines documented above and flag any regressions.

## Known query patterns and gotchas

- **BIGINT literals**: Use `BIGINT '1073741824'` syntax, not `CAST(1073741824 AS BIGINT)` — avoids integer overflow
- **Throughput filter**: Only compute throughput for queries with `physical_input_read_time_millis > 100 AND physical_input_bytes > 1 GB` — small queries produce noisy throughput values
- **Tardigrade queries**: Identified by `retry_policy IN ('TASK', 'QUERY')`, not by text search on session properties
- **IST conversion**: Always `CAST(query_date AS TIMESTAMP) + INTERVAL '5' HOUR + INTERVAL '30' MINUTE`
- **Query timeouts**: If a query times out on the MCP tool, simplify by reducing the date range or removing columns. The MCP tool routes through replica3 which can itself be under load