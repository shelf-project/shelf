-- spark-bench.sql — cold-vs-warm benchmark query.
--
-- Aggregate over a 15-day window (~500 K rows) so the read fans out
-- across 15 of the 30 partition data files. Each partition file is a
-- single Parquet row group, which makes the byte-range Shelf serves
-- extremely uniform between runs:
--
--   cold run  → shelfd misses on every footer + row group, fetches
--               from MinIO, populates DRAM tier
--   warm run  → identical byte-range requests, all serve from Foyer
--               DRAM (and ETag-check elided by HEAD-LRU)
--
-- The same query is executed in both runs by bench.py; this file is
-- the canonical text the README + walkthrough refer to.

SELECT
    event_date,
    COUNT(*)                AS rows,
    COUNT(DISTINCT user_id) AS unique_users,
    ROUND(AVG(amount), 2)   AS avg_amount,
    ROUND(SUM(amount), 2)   AS total_amount
FROM lake.demo.events
WHERE event_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-15'
GROUP BY event_date
ORDER BY event_date;
