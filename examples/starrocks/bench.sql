-- bench.sql — two queries against the seeded `events` Iceberg table.
--
-- Both are full table scans on purpose:
--   Q1: aggregate over every row → exercises every Parquet data file.
--   Q2: group-by over (event_date, event_type) → exercises partition
--       pruning and per-partition row-group reads.
--
-- run.sh executes this script twice — first run is cold (every byte
-- comes from MinIO via shelfd, all misses), second run is warm
-- (manifests + Parquet row groups served from Foyer DRAM/NVMe).
-- Look for `shelf_hits_total` to climb between the two runs.

SET CATALOG iceberg_demo;
USE `default`;

-- Q1: scan-and-count.
SELECT
    COUNT(*)               AS total_rows,
    COUNT(DISTINCT user_id) AS unique_users,
    ROUND(SUM(amount), 2)   AS total_amount
FROM events;

-- Q2: per-partition × per-event-type aggregate, top 20 buckets by row
-- count (descending), then by date for stable ordering.
SELECT
    event_date,
    event_type,
    COUNT(*)               AS n,
    ROUND(SUM(amount), 2)   AS revenue
FROM events
GROUP BY event_date, event_type
ORDER BY event_date ASC, n DESC
LIMIT 20;
