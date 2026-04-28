#!/usr/bin/env bash
# run.sh — end-to-end orchestrator for the StarRocks-on-Shelf example.
#
# Stages:
#   1. docker compose config         (validates docker-compose.yml)
#   2. docker compose build shelfd   (Rust release build, cached layers)
#   3. docker compose up -d --wait   (minio, iceberg-rest, shelfd, starrocks)
#   4. docker compose run seed       (PyIceberg writes the events table)
#   5. CREATE EXTERNAL CATALOG       (StarRocks → Iceberg REST → shelfd → MinIO)
#   6. cold bench                    (snapshot shelfd /metrics, run bench.sql)
#   7. warm bench                    (snapshot again, run bench.sql)
#   8. print hit/miss delta + per-stage timing
#
# Flags:
#   --cleanup    `docker compose down -v` and exit (drops MinIO + PG volumes)
#   --skip-build skip `docker compose build` (use cached shelfd image)
#   --rows N     EVENTS_ROWS override (default 1_000_000)
#
# This script is idempotent: re-run safely. It does *not* touch anything
# outside `examples/starrocks/`.

set -euo pipefail

cd "$(dirname "$0")"

CLEANUP=0
SKIP_BUILD=0
ROWS_OVERRIDE=""
for arg in "$@"; do
    case "$arg" in
        --cleanup)    CLEANUP=1 ;;
        --skip-build) SKIP_BUILD=1 ;;
        --rows=*)     ROWS_OVERRIDE="${arg#--rows=}" ;;
        --help|-h)
            sed -n '2,20p' "$0"
            exit 0
            ;;
        *) echo "[run] unknown flag: $arg" >&2; exit 2 ;;
    esac
done

DC=(docker compose)

if [[ "$CLEANUP" == "1" ]]; then
    echo "[run] tearing down stack and dropping volumes..."
    "${DC[@]}" down -v --remove-orphans
    exit 0
fi

# ---- 1. validate compose ----
echo "[run] step 1/8: validating docker-compose.yml"
"${DC[@]}" config >/dev/null

# ---- 2. build shelfd ----
if [[ "$SKIP_BUILD" == "1" ]]; then
    echo "[run] step 2/8: skipping shelfd build (--skip-build)"
else
    echo "[run] step 2/8: building shelfd (Rust release; first build ~5 min, cached <30 s)"
    "${DC[@]}" build shelfd
fi

# ---- 3. bring up the long-running services ----
echo "[run] step 3/8: starting minio + iceberg-rest + shelfd + starrocks"
"${DC[@]}" up -d --wait minio iceberg-rest shelfd starrocks

# ---- 4. seed the Iceberg table ----
echo "[run] step 4/8: seeding Iceberg table via PyIceberg (one-shot)"
SEED_ENV=()
if [[ -n "$ROWS_OVERRIDE" ]]; then
    SEED_ENV=(-e "EVENTS_ROWS=$ROWS_OVERRIDE")
fi
"${DC[@]}" run --rm "${SEED_ENV[@]}" seed

# ---- 5. create the external catalog in StarRocks ----
echo "[run] step 5/8: registering iceberg_demo external catalog with StarRocks"
"${DC[@]}" exec -T starrocks mysql -uroot -h127.0.0.1 -P9030 \
    < init/create-catalog.sql

# ---- helpers for /metrics scraping ----
SHELF_METRICS_URL="http://127.0.0.1:9791/metrics"

# Sum a Prometheus counter family (one or more labelled series). Returns
# an integer; falls back to 0 when the metric hasn't been registered yet.
metric_sum() {
    local name="$1"
    curl -fsS "$SHELF_METRICS_URL" 2>/dev/null \
        | awk -v n="$name" '
            $0 ~ "^"n"(\\{|[[:space:]])" {
                # last whitespace-separated field is the value
                v = $NF
                # strip non-numeric chars (handles `1.234e+05` too)
                if (v ~ /^[0-9.eE+-]+$/) sum += v
            }
            END { printf "%.0f\n", (sum ? sum : 0) }
        '
}

snapshot_metrics() {
    local label="$1"
    local hits misses bytes_origin bytes_cache
    hits=$(metric_sum "shelf_hits_total")
    misses=$(metric_sum "shelf_misses_total")
    bytes_origin=$(metric_sum "shelf_origin_bytes_total")
    bytes_cache=$(metric_sum "shelf_cache_bytes_served_total")
    printf "%-12s hits=%s misses=%s origin_bytes=%s cache_bytes=%s\n" \
        "$label" "$hits" "$misses" "$bytes_origin" "$bytes_cache"
    # Echo back as space-separated for the caller to parse.
    echo "$hits $misses $bytes_origin $bytes_cache"
}

run_bench() {
    local label="$1"
    local logfile="bench-${label}.log"
    local start end secs
    start=$(date +%s%N)
    "${DC[@]}" exec -T starrocks mysql -uroot -h127.0.0.1 -P9030 -t \
        < bench.sql > "$logfile" 2>&1 || {
            echo "[run] bench ${label} FAILED — last 60 lines of $logfile:" >&2
            tail -60 "$logfile" >&2
            return 1
        }
    end=$(date +%s%N)
    secs=$(awk "BEGIN { printf \"%.2f\", ($end - $start) / 1e9 }")
    echo "[run] $label run wall-time: ${secs}s  (full output in $logfile)"
    echo "$secs"
}

# ---- 6. cold run ----
echo "[run] step 6/8: cold bench (cache empty)"
read -r cold_h_pre cold_m_pre _ _ < <(snapshot_metrics "before-cold")
cold_secs=$(run_bench "cold")
read -r cold_h_post cold_m_post cold_origin_post cold_cache_post < <(snapshot_metrics "after-cold")

# ---- 7. warm run ----
echo "[run] step 7/8: warm bench (cache populated by cold run)"
warm_secs=$(run_bench "warm")
read -r warm_h_post warm_m_post warm_origin_post warm_cache_post < <(snapshot_metrics "after-warm")

# ---- 8. report ----
echo
echo "=== Shelf cache effect ==="
printf "%-22s %-12s %-12s %-12s\n" "" "hits" "misses" "wall-secs"
printf "%-22s %-12s %-12s %-12s\n" "cold (run 1)" \
    "$(( cold_h_post - cold_h_pre ))" \
    "$(( cold_m_post - cold_m_pre ))" \
    "$cold_secs"
printf "%-22s %-12s %-12s %-12s\n" "warm (run 2)" \
    "$(( warm_h_post - cold_h_post ))" \
    "$(( warm_m_post - cold_m_post ))" \
    "$warm_secs"
echo
echo "Cumulative origin_bytes  : $warm_origin_post"
echo "Cumulative cache_bytes   : $warm_cache_post"
echo
echo "[run] done. To stop and drop volumes: bash run.sh --cleanup"
