-- spark-warmup.sql — JVM + Iceberg metadata cache warmup.
--
-- Run BEFORE the cold/warm benchmark so the cold timing measures the
-- shelfd cache effect, not Spark's first-time codegen + Iceberg
-- catalog client bootstrap. The actual "cold" reads happen against
-- shelfd's empty pools after we explicitly evict between runs.
--
-- Notes:
--   * `lake` is the Iceberg REST catalog (configured via
--     spark.sql.catalog.lake.* in run-bench.sh).
--   * `demo` is the namespace created by the seed step.
--   * `events` is the seeded 1 M-row identity-partitioned table.

-- Force the catalog client to resolve the table at least once so
-- the first benchmark query doesn't pay for the namespace lookup.
SHOW TABLES IN lake.demo;

-- Touch a single partition so Spark's planner caches the file list +
-- partition spec resolution for events. We pick a date that the seed
-- guarantees has rows (events_days=30 starting 2026-04-01).
SELECT COUNT(*) AS warmup_rows
FROM lake.demo.events
WHERE event_date = DATE '2026-04-15';
