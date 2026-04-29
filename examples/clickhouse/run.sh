#!/usr/bin/env bash
# Orchestrate a cold-then-warm bench run for the ClickHouse + Shelf example.
#
# Steps:
#   1.  docker compose up -d   (builds shelfd if needed, ~5-10 min first time)
#   2.  Wait for shelfd /healthz, ClickHouse /ping, and the seed step to
#       complete.
#   3.  Run SELECT 1 inside ClickHouse to warm the process.
#   4.  Scrape shelfd /metrics → metrics.0.txt
#   5.  Run bench query (cold) → results/cold.txt + cold elapsed.
#   6.  Scrape shelfd /metrics → metrics.1.txt
#   7.  Run bench query (warm) → results/warm.txt + warm elapsed.
#   8.  Scrape shelfd /metrics → metrics.2.txt
#   9.  Print summary: cold elapsed, warm elapsed, hit/miss deltas, speedup.
#
# Exit 0 on success. Outputs land in ./results/.
#
# To tear down:
#   docker compose down -v

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

RESULTS="$HERE/results"
mkdir -p "$RESULTS"

CH="shelf-ch-clickhouse"
SHELFD_METRICS="http://127.0.0.1:9390/metrics"
SHELFD_HEALTH="http://127.0.0.1:9390/healthz"
CH_PING="http://127.0.0.1:8123/ping"
COMPOSE=${COMPOSE:-"docker compose"}
TIMEOUT_SECS=${TIMEOUT_SECS:-300}   # bumped to cover first-time shelfd build

log()  { printf '[run] %s\n' "$*" >&2; }
fail() { printf '[run][FAIL] %s\n' "$*" >&2; exit 1; }

wait_for_url() {
  local label="$1" url="$2"
  local deadline=$(( $(date +%s) + TIMEOUT_SECS ))
  log "waiting for $label at $url ..."
  while (( $(date +%s) < deadline )); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      log "$label up"
      return 0
    fi
    sleep 2
  done
  fail "$label did not become ready within ${TIMEOUT_SECS}s"
}

wait_for_seed() {
  log "waiting for seed container to exit 0 ..."
  local deadline=$(( $(date +%s) + TIMEOUT_SECS ))
  while (( $(date +%s) < deadline )); do
    local status
    status=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' shelf-ch-seed 2>/dev/null || echo "missing 1")
    case "$status" in
      "exited 0") log "seed completed"; return 0 ;;
      "exited "*) fail "seed exited non-zero ($status); see: docker logs shelf-ch-seed" ;;
    esac
    sleep 2
  done
  fail "seed did not finish within ${TIMEOUT_SECS}s"
}

scrape_metrics() {
  local out="$1"
  curl -fsS "$SHELFD_METRICS" > "$out" \
    || fail "failed to scrape $SHELFD_METRICS"
}

# Sum a counter family across all label sets:
#   sum_counter <metrics-file> <metric-name>
sum_counter() {
  local f="$1" m="$2"
  awk -v m="$m" '
    $0 ~ "^"m"\\b" || $0 ~ "^"m"\\{" {
      v = $NF
      if (v + 0 == v) { sum += v }
    }
    END { printf "%d\n", sum + 0 }
  ' "$f"
}

# Run the bench query and print the wall clock in milliseconds. Also append
# the query result to $1 (for visual inspection).
time_bench() {
  local out="$1"
  local sql="SELECT count() AS rows, avg(value) AS avg_value
             FROM iceberg('http://shelfd:9092/warehouse/demo/events', 'dummy', 'dummy')
             WHERE date = '2024-01-15'
             FORMAT TabSeparated"
  local t0 t1
  t0=$(date +%s%N)
  docker exec -i "$CH" clickhouse-client --query "$sql" > "$out" 2>>"$RESULTS/clickhouse.err"
  t1=$(date +%s%N)
  echo $(( (t1 - t0) / 1000000 ))
}

main() {
  log "bringing up the stack ..."
  $COMPOSE up -d --remove-orphans
  wait_for_seed
  wait_for_url shelfd  "$SHELFD_HEALTH"
  wait_for_url clickhouse "$CH_PING"

  # Warm the ClickHouse process itself (TCP handshake, server-side
  # initialisation, no Iceberg reads).
  log "warmup: SELECT 1"
  docker exec -i "$CH" clickhouse-client --query "SELECT 1 FORMAT TabSeparated" >/dev/null

  scrape_metrics "$RESULTS/metrics.0.txt"

  log "cold bench ..."
  COLD_MS=$(time_bench "$RESULTS/cold.txt")
  scrape_metrics "$RESULTS/metrics.1.txt"

  log "warm bench ..."
  WARM_MS=$(time_bench "$RESULTS/warm.txt")
  scrape_metrics "$RESULTS/metrics.2.txt"

  # Shelf counter deltas across the cold pass and the warm pass.
  local hits_0 hits_1 hits_2 miss_0 miss_1 miss_2
  hits_0=$(sum_counter "$RESULTS/metrics.0.txt" shelf_hits_total)
  hits_1=$(sum_counter "$RESULTS/metrics.1.txt" shelf_hits_total)
  hits_2=$(sum_counter "$RESULTS/metrics.2.txt" shelf_hits_total)
  miss_0=$(sum_counter "$RESULTS/metrics.0.txt" shelf_misses_total)
  miss_1=$(sum_counter "$RESULTS/metrics.1.txt" shelf_misses_total)
  miss_2=$(sum_counter "$RESULTS/metrics.2.txt" shelf_misses_total)

  local cold_hits=$(( hits_1 - hits_0 ))
  local cold_miss=$(( miss_1 - miss_0 ))
  local warm_hits=$(( hits_2 - hits_1 ))
  local warm_miss=$(( miss_2 - miss_1 ))

  local speedup="n/a"
  if (( WARM_MS > 0 )); then
    speedup=$(awk -v c="$COLD_MS" -v w="$WARM_MS" 'BEGIN { printf "%.2fx", c / w }')
  fi

  {
    echo
    echo "============================================================"
    echo "ClickHouse + Shelf S3 shim — cold vs warm"
    echo "============================================================"
    printf "Bench query result (cold):  %s\n" "$(tr -s '[:space:]' ' ' < "$RESULTS/cold.txt")"
    printf "Bench query result (warm):  %s\n" "$(tr -s '[:space:]' ' ' < "$RESULTS/warm.txt")"
    if ! diff -q "$RESULTS/cold.txt" "$RESULTS/warm.txt" >/dev/null 2>&1; then
      echo "WARNING: cold and warm results differ — see results/cold.txt vs results/warm.txt"
    fi
    echo
    printf "Wall clock (ClickHouse exec)\n"
    printf "  cold: %5d ms\n" "$COLD_MS"
    printf "  warm: %5d ms   (speedup %s)\n" "$WARM_MS" "$speedup"
    echo
    printf "Shelf cache deltas (sum across all pools)\n"
    printf "  cold pass:  hits +%-6d  misses +%-6d\n" "$cold_hits" "$cold_miss"
    printf "  warm pass:  hits +%-6d  misses +%-6d\n" "$warm_hits" "$warm_miss"
    echo "============================================================"
  } | tee "$RESULTS/summary.txt"

  if (( warm_hits <= cold_hits )) && (( warm_hits == 0 )); then
    log "WARNING: warm pass produced 0 shelf hits — Shelf may not be on the read path"
    log "         (check ClickHouse iceberg metadata cache — config.d/00-shelf-example.xml disables it)"
  fi
}

main "$@"
