#!/usr/bin/env bash
# F1 — TPC-DS SF1000 Iceberg generator.
#
# Populates s3://shelf-bench-tpcds/sf1000/ with 24 TPC-DS tables
# materialised as Iceberg (Parquet, ZSTD) against an Iceberg REST
# catalog or an HMS. Source is the `trinodb/trino-tpcds` built-in
# connector — no external `dsdgen` binary or data upload step.
#
# Hardware: the catalog+engine that runs this script should have
# at least 192 GiB of task memory and 4×m6a.4xlarge worth of split
# parallelism; SF1000 generation takes ~2 hours on that footprint.
#
# Idempotency: every CTAS targets an empty Iceberg table. The
# per-table $BENCH_CATALOG.$BENCH_SCHEMA.<t> namespace is dropped
# and recreated so a retried run is a clean run — but the S3
# object store is **not** purged because Iceberg writes go to
# new snapshot ids; leftover files get garbage-collected by a
# periodic Iceberg maintenance cron.
#
# Usage:
#   ./generate_sf1000.sh                 # default SF1000 into the
#                                        # canonical bucket
#   SF=100 ./generate_sf1000.sh          # SF100 smoke (used by
#                                        # the F4 regression gate)
#   BENCH_CATALOG=shelf_bench ./generate_sf1000.sh
#
# Env:
#   TRINO_URL          https://trino.example.com:443 (required)
#   TRINO_USER         principal (default: bench-runner)
#   BENCH_CATALOG      Iceberg catalog name in Trino (default: shelf_bench)
#   BENCH_SCHEMA       schema inside that catalog (default: tpcds_sf${SF})
#   BENCH_LOCATION     s3://shelf-bench-tpcds/sf${SF}/ override
#   SF                 scale factor: 1, 10, 100, 1000 (default: 1000)
#
# The script is intentionally shell, not Python: it is invoked from
# GitHub Actions and from operator laptops with nothing more than
# bash + trino-cli available. Run `./smoke.sh` for a quick SF1
# sanity check before firing the big generator.

set -euo pipefail

: "${TRINO_URL:?TRINO_URL required (e.g. https://trino.cache.svc.cluster.local)}"
TRINO_USER="${TRINO_USER:-bench-runner}"
SF="${SF:-1000}"
BENCH_CATALOG="${BENCH_CATALOG:-shelf_bench}"
BENCH_SCHEMA="${BENCH_SCHEMA:-tpcds_sf${SF}}"
BENCH_LOCATION="${BENCH_LOCATION:-s3://shelf-bench-tpcds/sf${SF}/}"

# Partitioning choices (BLUEPRINT §F1): deliberate, documented, and
# identical across every engine we compare against. Changing any
# entry here changes the competitive story, so land it in its own
# PR with a reviewer sign-off.
declare -A PARTITION_BY=(
  [store_sales]="bucket(ss_customer_sk, 32)"
  [store_returns]="bucket(sr_customer_sk, 32)"
  [catalog_sales]="bucket(cs_bill_customer_sk, 32)"
  [catalog_returns]="bucket(cr_returning_customer_sk, 32)"
  [web_sales]="bucket(ws_bill_customer_sk, 32)"
  [web_returns]="bucket(wr_returning_customer_sk, 32)"
  [inventory]="inv_date_sk"
)

TABLES=(
  call_center catalog_page catalog_returns catalog_sales customer
  customer_address customer_demographics date_dim dbgen_version
  household_demographics income_band inventory item promotion
  reason ship_mode store store_returns store_sales time_dim
  warehouse web_page web_returns web_sales web_site
)

trino_exec() {
  local sql="$1"
  trino --server "$TRINO_URL" --user "$TRINO_USER" \
        --execute "$sql" --output-format=CSV_HEADER
}

echo "=> creating catalog schema ${BENCH_CATALOG}.${BENCH_SCHEMA} at ${BENCH_LOCATION}"
trino_exec "CREATE SCHEMA IF NOT EXISTS ${BENCH_CATALOG}.${BENCH_SCHEMA} WITH (location = '${BENCH_LOCATION}')"

for table in "${TABLES[@]}"; do
  echo "=> table: ${table}"
  partitioning="${PARTITION_BY[$table]:-}"
  partition_clause=""
  if [[ -n "$partitioning" ]]; then
    partition_clause=", partitioning = ARRAY['${partitioning}']"
  fi

  trino_exec "DROP TABLE IF EXISTS ${BENCH_CATALOG}.${BENCH_SCHEMA}.${table}"

  trino_exec "CREATE TABLE ${BENCH_CATALOG}.${BENCH_SCHEMA}.${table}
              WITH (
                format = 'PARQUET',
                format_version = 2
                ${partition_clause}
              )
              AS SELECT * FROM tpcds.sf${SF}.${table}"

  echo "   ${table} loaded"
done

echo "=> ANALYZE every table so Iceberg stats land alongside the data"
for table in "${TABLES[@]}"; do
  trino_exec "ANALYZE ${BENCH_CATALOG}.${BENCH_SCHEMA}.${table}"
done

echo "=> done. Tables:"
trino_exec "SHOW TABLES IN ${BENCH_CATALOG}.${BENCH_SCHEMA}"
