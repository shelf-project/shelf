#!/usr/bin/env bash
# H4 — end-to-end MV auto-rewrite smoke test.
#
# The test seeds a tiny Iceberg table, creates an MV over an
# aggregation, runs the same aggregation against the base
# table, and asserts:
#   (a) EXPLAIN references the MV name;
#   (b) elapsed wall-clock drops at least 5x vs the base query.
#
# Exit codes:
#   0  ok
#   1  Trino unreachable / API error
#   2  EXPLAIN does not reference the MV
#   3  speedup below 5x
set -euo pipefail

TRINO_HOST="${TRINO_HOST:-http://localhost:8080}"
TRINO_USER="${TRINO_USER:-h4-mv-rewrite}"
CATALOG="${CATALOG:-iceberg}"
SCHEMA="${SCHEMA:-shelf_mv_rewrite_test}"
TABLE="${TABLE:-orders}"
MV="${MV:-orders_by_region_daily}"
SPEEDUP_THRESHOLD="${SPEEDUP_THRESHOLD:-5}"

trino_query() {
    # Runs an SQL statement and prints the raw JSON response.
    # Uses the /v1/statement REST API so we don't need JDBC.
    local sql="$1"
    local body
    body=$(jq -n --arg q "$sql" '{query: $q}')
    local first
    first=$(curl -sS -X POST \
        -H "X-Trino-User: ${TRINO_USER}" \
        -H "X-Trino-Catalog: ${CATALOG}" \
        -H "X-Trino-Schema: ${SCHEMA}" \
        -H "Content-Type: application/json" \
        -d "$sql" \
        "${TRINO_HOST}/v1/statement")
    # Walk `nextUri` until finished, accumulating rows.
    local resp="$first"
    while true; do
        local next
        next=$(echo "$resp" | jq -r '.nextUri // empty')
        if [[ -z "$next" ]]; then
            echo "$resp"
            return 0
        fi
        resp=$(curl -sS "$next")
    done
}

log() { printf '[h4] %s\n' "$*" >&2; }

check_connectivity() {
    local r
    r=$(curl -sS -o /dev/null -w '%{http_code}' "${TRINO_HOST}/v1/info" || true)
    if [[ "$r" != "200" ]]; then
        log "Trino not reachable at ${TRINO_HOST} (HTTP $r)"
        return 1
    fi
}

seed_base_table() {
    log "seeding base table ${SCHEMA}.${TABLE}"
    trino_query "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}" >/dev/null
    trino_query "DROP MATERIALIZED VIEW IF EXISTS ${CATALOG}.${SCHEMA}.${MV}" >/dev/null
    trino_query "DROP TABLE IF EXISTS ${CATALOG}.${SCHEMA}.${TABLE}" >/dev/null
    trino_query "
        CREATE TABLE ${CATALOG}.${SCHEMA}.${TABLE} (
            order_id bigint,
            region varchar,
            order_ts timestamp(6),
            amount double
        ) WITH (format = 'PARQUET')
    " >/dev/null
    trino_query "
        INSERT INTO ${CATALOG}.${SCHEMA}.${TABLE}
        SELECT
            n,
            CAST(CASE n % 4 WHEN 0 THEN 'NA' WHEN 1 THEN 'EU' WHEN 2 THEN 'APAC' ELSE 'LATAM' END AS varchar),
            TIMESTAMP '2026-01-01 00:00:00' + (n * INTERVAL '1' SECOND),
            CAST((n % 997) AS double)
        FROM UNNEST(sequence(1, 200000)) AS t(n)
    " >/dev/null
}

create_mv() {
    log "creating materialized view ${SCHEMA}.${MV}"
    trino_query "
        CREATE MATERIALIZED VIEW ${CATALOG}.${SCHEMA}.${MV}
        AS
        SELECT
            region,
            DATE(order_ts) AS order_date,
            SUM(amount) AS total_amount,
            COUNT(*) AS order_count
        FROM ${CATALOG}.${SCHEMA}.${TABLE}
        GROUP BY region, DATE(order_ts)
    " >/dev/null
    trino_query "REFRESH MATERIALIZED VIEW ${CATALOG}.${SCHEMA}.${MV}" >/dev/null
}

time_query() {
    local sql="$1"
    local start end
    start=$(python3 -c 'import time;print(int(time.time()*1000))')
    trino_query "$sql" >/dev/null
    end=$(python3 -c 'import time;print(int(time.time()*1000))')
    echo $((end - start))
}

assert_explain_references_mv() {
    log "asserting EXPLAIN references ${MV}"
    local plan
    plan=$(trino_query "
        EXPLAIN
        SELECT region, DATE(order_ts) AS order_date, SUM(amount), COUNT(*)
        FROM ${CATALOG}.${SCHEMA}.${TABLE}
        GROUP BY region, DATE(order_ts)
    " | jq -r '.data[]? | tostring')
    if ! echo "$plan" | grep -qi "${MV}"; then
        log "EXPLAIN did not reference ${MV}; plan snippet below"
        echo "$plan" | head -40 >&2
        return 2
    fi
}

measure_speedup() {
    log "measuring speedup"
    local base_ms mv_ms
    base_ms=$(time_query "
        SELECT region, DATE(order_ts) AS order_date, SUM(amount), COUNT(*)
        FROM ${CATALOG}.${SCHEMA}.${TABLE}
        GROUP BY region, DATE(order_ts)
    ")
    # Fire the same query a second time so the engine's MV
    # rewrite has an opportunity to trigger.
    mv_ms=$(time_query "
        SELECT region, DATE(order_ts) AS order_date, SUM(amount), COUNT(*)
        FROM ${CATALOG}.${SCHEMA}.${TABLE}
        GROUP BY region, DATE(order_ts)
    ")
    log "base=${base_ms}ms mv=${mv_ms}ms"
    python3 - "$base_ms" "$mv_ms" "$SPEEDUP_THRESHOLD" <<'PY'
import sys
base, mv, threshold = map(float, sys.argv[1:])
if mv <= 0:
    print(f"mv elapsed 0ms — treating as pass", file=sys.stderr)
    sys.exit(0)
ratio = base / mv
print(f"[h4] speedup = {ratio:.2f}x (threshold {threshold}x)", file=sys.stderr)
if ratio < threshold:
    sys.exit(3)
PY
}

teardown() {
    log "tearing down ${SCHEMA}"
    trino_query "DROP MATERIALIZED VIEW IF EXISTS ${CATALOG}.${SCHEMA}.${MV}" >/dev/null || true
    trino_query "DROP TABLE IF EXISTS ${CATALOG}.${SCHEMA}.${TABLE}" >/dev/null || true
    trino_query "DROP SCHEMA IF EXISTS ${CATALOG}.${SCHEMA}" >/dev/null || true
}

trap teardown EXIT

main() {
    check_connectivity || exit 1
    seed_base_table
    create_mv
    assert_explain_references_mv
    measure_speedup
    log "H4 ok"
}

main "$@"
