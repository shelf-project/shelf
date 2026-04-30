-- SHELF-35 — extract a 30-day trace from your_query_log_table.
--
-- Operator runs this against rep-3 (or any direct-S3 replica that won't
-- self-affect via a cache cutover) and exports the result as CSV with
-- columns:
--
--    timestamp_ms  bigint
--    object_id     varchar  -- "<catalog>.<schema>.<table>"
--    size_bytes    bigint   -- physicalInputBytes for this (query, table)
--    query_id      varchar
--
-- The CSV is then fed to ``python -m tools.replay.main --trace <path>``.
--
-- Why (query, table) granularity:
--
--   * your_query_log_table records inputs_json with one entry
--     per (catalog, schema, table) the query touched, including a
--     ``physicalInputBytes`` field per table — this is what we cache-
--     simulate against.
--   * Per-split paths are NOT recorded. Trino removed
--     ``SplitCompletedEvent`` upstream (PR #26436, merged 2025-08-19) per
--     ADR-0005, and the listener wired in clients/trino/ today only
--     observes ``queryCompleted``. Until SHELF-35b lands a file-level
--     synthesis on top of an Iceberg ``$files`` join, the (query, table)
--     granularity is the honest ceiling.
--
-- Time window: 30 days, IST-converted from query_date UTC. The IST
-- conversion matters because the workspace's IST-default reporting
-- convention means downstream rollups (per-day, per-hour) line up with
-- ops dashboards. Use ``--days N`` in a wrapper if you need a smaller
-- window.
--
-- Safety: SELECT only. No catalog mutation. ``your_query_log_table``
-- is the audit log; reading it cannot affect any production query.

WITH expanded AS (
  SELECT
    -- query_date is timestamp(6) UTC; +5:30 → IST.
    to_unixtime(q.query_date AT TIME ZONE 'Asia/Kolkata') * 1000 AS timestamp_ms,
    q.query_id,
    -- inputs_json is a JSON-encoded array of {catalog, schema, table,
    -- physicalInputBytes, ...}. We unnest into rows.
    cast(json_parse(q.inputs_json) AS array(json)) AS inputs
  FROM your_query_log_table q
  WHERE q.query_date >= current_timestamp - INTERVAL '30' DAY
    AND q.query_state = 'FINISHED'
    AND q.error_code IS NULL
)
SELECT
  cast(timestamp_ms AS bigint) AS timestamp_ms,
  concat(
    coalesce(json_extract_scalar(input, '$.catalog'), 'unknown'),
    '.',
    coalesce(json_extract_scalar(input, '$.schema'), 'unknown'),
    '.',
    coalesce(json_extract_scalar(input, '$.table'), 'unknown')
  ) AS object_id,
  cast(coalesce(json_extract_scalar(input, '$.physicalInputBytes'), '0') AS bigint) AS size_bytes,
  query_id
FROM expanded, UNNEST(inputs) AS t(input)
WHERE cast(coalesce(json_extract_scalar(input, '$.physicalInputBytes'), '0') AS bigint) > 0
ORDER BY timestamp_ms ASC
;
