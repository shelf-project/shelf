-- ClickHouse bench queries against the demo.events Iceberg table, served
-- through the Shelf S3 read shim on shelfd:9092.
--
-- Two queries:
--   1. warmup        — round-trips ClickHouse without touching Iceberg.
--   2. bench         — count + avg over a single date partition. The
--                      WHERE clause is the meaningful workload, since
--                      use_iceberg_partition_pruning=1 skips manifests
--                      whose stats exclude '2024-01-15'.
--
-- The credentials are intentionally 'dummy' / 'dummy'. shelfd's S3 shim is
-- signature-agnostic — it ignores the SigV4 Authorization header and uses
-- its own env-var creds to talk to the origin (MinIO).

SELECT 1 AS warmup FORMAT TabSeparated;

SELECT count() AS rows, avg(value) AS avg_value
FROM iceberg('http://shelfd:9092/warehouse/demo/events', 'dummy', 'dummy')
WHERE date = '2024-01-15'
FORMAT TabSeparated;
