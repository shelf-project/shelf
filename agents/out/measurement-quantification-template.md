# Measurement & Quantification Template

Per-ticket quantification template that every cost-reduction PR must
instantiate before claiming a $ saving. Cross-cuts the plan at
`/Users/aamir/.cursor/plans/shelf-cost-reduction-research_97107ffb.plan.md`
§7.

## Purpose

Every Tier-1 / Tier-2 / Tier-3 ticket lands as a draft PR with measurement
gates that this template defines. A PR cannot exit Draft state without
instantiating §A1 + §A2 + §A3 + §A4 with real numbers from the canary
replica. The template exists because "we shipped X and saved Y%" is a
fabrication unless the author can hand a reviewer the four artefacts
below; reviewers must reject any cost-claim PR whose body lacks them.

## §A. Required artefacts per ticket

The four sub-sections below are the per-PR evidence pack. They are
ordered by source-of-truth priority: Trino-side metadata first
(authoritative for query behaviour), S3-side access logs second
(authoritative for $/byte), shelfd-side $-saved counter third
(narrowest, post-SHELF-61), and the A/B arm split last (post-SHELF-62,
the only thing that defends against seasonality).

### A1. `cdp.trino_logs.trino_queries` before/after

The PR author runs the stub queries below against the Trino MCP. Window
is **7 days pre-cutover vs 7 days post-cutover**, partition column is
`query_date` (UTC). All output rendered in **IST** per the workspace
time-reporting convention.

> **Important caveat**: event-listener ingest lag is ~30 min, so
> post-cutover windows must wait at least 30 min after the cutover flip
> before any read. Any "no rows in last N min" is **listener-side**, not
> Trino-side (per `AGENTS.md`).

#### (i) Wall-time percentiles by replica + query type

```sql
SELECT
    coordinator_replica,
    query_type,
    APPROX_PERCENTILE(wall_time_millis, 0.50) AS p50_ms,
    APPROX_PERCENTILE(wall_time_millis, 0.95) AS p95_ms,
    APPROX_PERCENTILE(wall_time_millis, 0.99) AS p99_ms,
    COUNT(*)                                  AS n_queries
FROM cdp.trino_logs.trino_queries
WHERE query_date BETWEEN DATE '<window_start>' AND DATE '<window_end>'
  AND state = 'FINISHED'
GROUP BY coordinator_replica, query_type
ORDER BY coordinator_replica, query_type;
```

Run twice — once for the pre-window, once for the post-window — and
report deltas.

#### (ii) Physical input bytes + read time by table

```sql
SELECT
    table_name,
    SUM(physical_input_bytes)        AS bytes_read,
    SUM(physical_input_read_time_millis) AS read_time_ms,
    COUNT(*)                         AS n_queries
FROM cdp.trino_logs.trino_queries
     CROSS JOIN UNNEST(tables_read) AS t(table_name)
WHERE query_date BETWEEN DATE '<window_start>' AND DATE '<window_end>'
  AND state = 'FINISHED'
GROUP BY table_name
ORDER BY bytes_read DESC
LIMIT 100;
```

#### (iii) Error-class counts

```sql
SELECT
    error_code,
    COUNT(*) AS n
FROM cdp.trino_logs.trino_queries
WHERE query_date BETWEEN DATE '<window_start>' AND DATE '<window_end>'
  AND error_code IS NOT NULL
  AND (   error_code LIKE 'ICEBERG_%'
       OR error_code LIKE 'HIVE_%'
       OR error_code IN ('GENERIC_INTERNAL_ERROR',
                         'USER_CANCELED',
                         'CLUSTER_OUT_OF_MEMORY'))
GROUP BY error_code
ORDER BY n DESC;
```

### A2. Athena `s3_access_logs_db` per-bucket DoD (day-over-day)

S3 server-access logs are the authoritative source for actual GET / HEAD
/ PUT counts and bytes-served-from-origin. Run the stub below against
the four `pw_data_cdp_prod_<bucket>_logs_v2` tables (`gold`, `silver`,
`bronze`, `temp`) using a UNION ALL pattern.

```sql
WITH src AS (
    SELECT 'gold'   AS bucket, requestdatetime, operation, bytessent, key
      FROM s3_access_logs_db.pw_data_cdp_prod_gold_logs_v2
     WHERE operation IN ('REST.GET.OBJECT','REST.HEAD.OBJECT','REST.PUT.OBJECT')
       AND from_iso8601_timestamp(requestdatetime)
           BETWEEN TIMESTAMP '<window_start>' AND TIMESTAMP '<window_end>'
    UNION ALL
    SELECT 'silver' AS bucket, requestdatetime, operation, bytessent, key
      FROM s3_access_logs_db.pw_data_cdp_prod_silver_logs_v2
     WHERE operation IN ('REST.GET.OBJECT','REST.HEAD.OBJECT','REST.PUT.OBJECT')
       AND from_iso8601_timestamp(requestdatetime)
           BETWEEN TIMESTAMP '<window_start>' AND TIMESTAMP '<window_end>'
    UNION ALL
    SELECT 'bronze' AS bucket, requestdatetime, operation, bytessent, key
      FROM s3_access_logs_db.pw_data_cdp_prod_bronze_logs_v2
     WHERE operation IN ('REST.GET.OBJECT','REST.HEAD.OBJECT','REST.PUT.OBJECT')
       AND from_iso8601_timestamp(requestdatetime)
           BETWEEN TIMESTAMP '<window_start>' AND TIMESTAMP '<window_end>'
    UNION ALL
    SELECT 'temp'   AS bucket, requestdatetime, operation, bytessent, key
      FROM s3_access_logs_db.pw_data_cdp_prod_temp_logs_v2
     WHERE operation IN ('REST.GET.OBJECT','REST.HEAD.OBJECT','REST.PUT.OBJECT')
       AND from_iso8601_timestamp(requestdatetime)
           BETWEEN TIMESTAMP '<window_start>' AND TIMESTAMP '<window_end>'
)
SELECT
    bucket,
    split_part(key, '/', 2) AS db,
    split_part(key, '/', 3) AS table_name,
    operation,
    COUNT(*)         AS n_requests,
    SUM(bytessent)   AS bytes_served
FROM src
GROUP BY bucket, split_part(key, '/', 2), split_part(key, '/', 3), operation
ORDER BY bytes_served DESC NULLS LAST;
```

Compare the same query for pre- and post-windows and report the
`bytes_served` and `n_requests` delta per `(db, table_name, operation)`
tuple.

> **Cost-attribution note**: cost rolls up under
> `line_item_product_code='AmazonEKS'` (not `'AmazonEC2'`) for
> namespace-tagged data per `AGENTS.md`. Any `$` figure derived from CUR
> joins must filter on `AmazonEKS`, otherwise the EKS-pod portion of the
> bill is silently dropped.

### A3. SHELF-61 dollars-saved rate

Once **SHELF-61 (PR #68)** lands, every PR cites:

```promql
rate(shelf_s3_dollars_saved_total{tenant=~"$tenant", table=~"$table"}[7d])
```

from the **mimir-data** datasource (UID `ddy2eykq2tfy8a`).

**Required dimensions** in any panel / claim:

- `tenant`
- `table`
- `pool` (`rowgroup` vs `metadata`)
- `arm` (added post-SHELF-62, see §A4)

**Anti-overclaim rule.** The PR must explicitly reject the metric series
if `amortized_dollars_per_hour` is unset — SHELF-61 refuses to register
the counter when that knob is missing, so any non-empty series is
self-attesting only when the knob is set. The PR body must cite the
exact value used (current default `$0.864/hr/pod` for `m6a.4xlarge`
ap-south-1 list per plan §2 / §10 open-item #2). If the PR runs in a
different instance class or region, restate the value.

### A4. SHELF-62 A/B arm split (when applicable)

Once **SHELF-62 (PR #67)** lands, cutover PRs must include the
`shelf_arm IN ('on','off')` deterministic-hash split numbers from a
**non-cutover replica** (i.e. a replica running both arms concurrently)
during the canary window. Without this, every "saved X%" claim is
pre/post-only and vulnerable to seasonality (week-over-week traffic
swings, exam-period spikes, dbt-run schedule shifts).

Required panels in the PR body:

- `bytes_read` per `arm` (rate over 1h)
- `wall_time_millis p99` per `arm`
- `shelf_s3_dollars_saved_total` rate per `arm` (post-SHELF-61)

If only one of the two arms is enabled in the canary window, the PR
must say so explicitly and downgrade the claim from "A/B-validated" to
"pre/post estimate".

## §B. Quantitative gates per tier (PR-exit checklist)

Each Tier defines the minimum delta that must hold for **≥ 12 h on the
canary replica** before merge-to-main. Numbers below are floors, not
targets — exceeding them is fine, missing them blocks merge.

| Tier                                         | Lever class                       | Gate (must hold for ≥ 12 h on canary before merge)                                                                                                              |
| -------------------------------------------- | --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Tier-1** (B1, SHELF-66, SHELF-64, SHELF-50) | Hot-path latency-sensitive        | hit-ratio change ≥ **+5 pp** OR origin-bytes change ≥ **−10%**, **AND** p99 read latency ≤ **100 ms**, **AND** 5xx ≤ **1%**                                     |
| **Tier-2** (SHELF-60, SHELF-61, SHELF-62)     | Measurement                       | $0 direct; gate is "every Tier-1 + Tier-3 PR opened **after** this lands cites the new dimension". Reviewers reject downstream PRs that omit the new artefact.  |
| **Tier-3** (SHELF-63, SHELF-53, SHELF-65, SHELF-52) | Recommender                  | Recommended action shows ≥ **10 pp lift** in offline replay (SHELF-26 harness when available) **OR** ≥ **5 pp** in 7-day live A/B                                |

## §C. Cutover-window governance

Quoting the `AGENTS.md` Apr-28 lesson literally:

> Lock `(start, end, replicas, image-tag, hit-ratio floor, p99 ceiling)`
> upfront; **≤ 1 helm upgrade per session**; **no Trino coord restart
> during the window**; anything else invalidates the A/B and reschedules.

Every cutover PR description must include the lock-tuple as a fenced
YAML block, e.g.:

```yaml
cutover_lock:
  start:           2026-05-01T10:00:00+05:30   # IST
  end:             2026-05-01T22:00:00+05:30
  replicas:        [replica2]                  # canary only
  image_tag:       shelfd:0.5.3-rc4
  hit_ratio_floor: 0.85
  p99_ceiling_ms:  100
  helm_upgrades:   1                           # hard cap
```

Any deviation invalidates the A/B and the window must be rescheduled.

## §D. Common ingest gotchas (must be acknowledged in PR body)

- **`cdp.trino_logs.trino_queries` lag ~30 min.** Post-cutover read
  windows must start ≥ 30 min after the flip. A "rows in last 5 min"
  zero-count is listener-side, not Trino-side.
- **Athena `s3_access_logs_db` is server-access logs.** PUT / HEAD / GET
  counts are present and authoritative for `bytes_served`, but the
  `requester` column may not be populated for cluster-internal
  (in-VPC) traffic — do not filter on `requester` for shelfd-vs-Trino
  attribution; use `key` prefix instead.
- **Grafana template-variable quoting.** `${var:singlequote}` does **not**
  quote `allValue`. For any multi-select panel against SQL datasources
  (Postgres, Trino, Athena), use `${var:regex}` together with
  `regexp_like()` (or `~` on Postgres) — per `AGENTS.md`.

## §E. References

- Plan §7 — canonical methodology:
  `/Users/aamir/.cursor/plans/shelf-cost-reduction-research_97107ffb.plan.md`
- `AGENTS.md` — cost-attribution rules, ingest-lag note,
  Grafana-variable quoting rule, cutover-window discipline.
- ADR-0011 — content-addressed keys, ETag versioning (Iceberg snapshot
  safety; relevant because every measurement window straddles snapshot
  rolls and the cache key must remain stable across them).
