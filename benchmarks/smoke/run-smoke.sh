#!/usr/bin/env bash
# SHELF-12 smoke driver.
#
# Orchestrates:
#   1. docker compose wait-for-healthy (wait-healthy subcommand)
#   2. Run 10 canonical queries against Trino → results/cold/NN.txt
#   3. Scrape shelfd /metrics → results/metrics-after-cold.txt
#   4. Repeat queries → results/warm/NN.txt
#   5. Scrape /metrics → results/metrics-after-warm.txt
#   6. Diff cold vs warm per query (must be identical)
#   7. Assert warm shelf_hits_total > cold shelf_hits_total on at least
#      one of the metadata/rowgroup pools.
#
# Exit 0 on success, non-zero on any assertion failure.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

RESULTS=${RESULTS:-"$HERE/results"}
TRINO_HOST=${TRINO_HOST:-127.0.0.1}
TRINO_PORT=${TRINO_PORT:-8080}
SHELFD_METRICS_URL=${SHELFD_METRICS_URL:-http://127.0.0.1:9091/metrics}
COMPOSE=${COMPOSE:-"docker compose"}
TIMEOUT_SECS=${TIMEOUT_SECS:-90}

log() { printf '[smoke] %s\n' "$*" >&2; }
fail() { printf '[smoke][FAIL] %s\n' "$*" >&2; exit 1; }

wait_healthy() {
  log "waiting up to ${TIMEOUT_SECS}s for services to report healthy"
  local deadline=$(( $(date +%s) + TIMEOUT_SECS ))
  while (( $(date +%s) < deadline )); do
    local bad=0
    while read -r line; do
      # Skip non-service lines.
      [[ "$line" =~ ^([a-zA-Z0-9_-]+)[[:space:]]+([a-zA-Z]+) ]] || continue
      local svc="${BASH_REMATCH[1]}"
      local state="${BASH_REMATCH[2]}"
      case "$svc" in
        shelfd|trino-coordinator|minio)
          if [[ "$state" != "running" ]]; then
            bad=1
          fi ;;
      esac
    done < <($COMPOSE ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}' | tail -n +2)

    # Probe the actual endpoints too.
    if curl -fsS "http://${TRINO_HOST}:${TRINO_PORT}/v1/info" 2>/dev/null | grep -q '"starting":false' \
       && curl -fsS "$SHELFD_METRICS_URL" >/dev/null 2>&1; then
      log "all services healthy"
      return 0
    fi
    sleep 2
  done
  $COMPOSE ps >&2 || true
  fail "services did not become healthy within ${TIMEOUT_SECS}s"
}

run_query_file() {
  local qfile="$1" outfile="$2"
  docker exec -i shelf-smoke-trino /usr/bin/trino \
      --server "http://localhost:8080" \
      --output-format=CSV_HEADER \
      --file "/tmp/queries/$(basename "$qfile")" \
      > "$outfile" 2>&1 \
    || return 1
}

run_all_queries() {
  local phase="$1"
  local dir="$RESULTS/$phase"
  mkdir -p "$dir"
  log "running 10 queries (phase=$phase)"
  for q in seed/queries/*.sql; do
    local n; n="$(basename "$q" .sql)"
    if ! run_query_file "$q" "$dir/${n}.txt"; then
      cat "$dir/${n}.txt" >&2
      fail "query $n failed in phase $phase"
    fi
  done
}

diff_phases() {
  log "diffing cold vs warm outputs"
  local mismatch=0
  for q in seed/queries/*.sql; do
    local n; n="$(basename "$q" .sql)"
    if ! diff -u "$RESULTS/cold/${n}.txt" "$RESULTS/warm/${n}.txt" >/dev/null; then
      log "MISMATCH: $n"
      diff -u "$RESULTS/cold/${n}.txt" "$RESULTS/warm/${n}.txt" >&2 || true
      mismatch=1
    fi
  done
  (( mismatch == 0 )) || fail "cold != warm outputs (see diff above)"
}

scrape_metrics() {
  local target="$1"
  curl -fsS "$SHELFD_METRICS_URL" > "$target" \
    || fail "failed to scrape shelfd metrics from $SHELFD_METRICS_URL"
}

extract_pool_counter() {
  # $1=metrics file, $2=metric, $3=pool label.
  local f="$1" m="$2" pool="$3"
  awk -v m="$m" -v p="$pool" '
    $0 ~ "^" m "\\{.*pool=\"" p "\"" { print $NF; found=1; exit }
    END { if (!found) print 0 }
  ' "$f"
}

assert_hits_rose() {
  log "parsing shelf_hits_total{pool=metadata|rowgroup} cold vs warm"
  local cold="$RESULTS/metrics-after-cold.txt"
  local warm="$RESULTS/metrics-after-warm.txt"
  local md_c md_w rg_c rg_w
  md_c=$(extract_pool_counter "$cold" shelf_hits_total metadata)
  md_w=$(extract_pool_counter "$warm" shelf_hits_total metadata)
  rg_c=$(extract_pool_counter "$cold" shelf_hits_total rowgroup)
  rg_w=$(extract_pool_counter "$warm" shelf_hits_total rowgroup)
  printf '[smoke] shelf_hits_total metadata: cold=%s warm=%s\n' "$md_c" "$md_w"
  printf '[smoke] shelf_hits_total rowgroup: cold=%s warm=%s\n' "$rg_c" "$rg_w"
  # Bash float-free comparison — counters are integers.
  if (( md_w > md_c )) || (( rg_w > rg_c )); then
    log "warm > cold on at least one pool — conformance PASS"
    return 0
  fi
  fail "SHELF-15/SHELF-20 conformance regression: warm hits did not rise above cold (md $md_c→$md_w, rg $rg_c→$rg_w)"
}

# Stage the query files into the coordinator container's /etc/trino/queries
# so the built-in /usr/bin/trino CLI can read them without a host-mount
# bind after startup.
stage_queries() {
  log "staging query files into trino coordinator"
  # /etc/trino is bind-mounted read-only; stash queries under /tmp instead.
  docker exec shelf-smoke-trino mkdir -p /tmp/queries
  for q in seed/queries/*.sql; do
    docker cp "$q" shelf-smoke-trino:/tmp/queries/ >/dev/null
  done
}

main() {
  mkdir -p "$RESULTS/cold" "$RESULTS/warm"

  if [[ "${1:-}" == "wait-healthy" ]]; then
    wait_healthy
    exit 0
  fi

  local start; start=$(date +%s)

  wait_healthy
  stage_queries

  run_all_queries cold
  scrape_metrics "$RESULTS/metrics-after-cold.txt"

  run_all_queries warm
  scrape_metrics "$RESULTS/metrics-after-warm.txt"

  diff_phases
  assert_hits_rose

  local end; end=$(date +%s)
  log "smoke run PASS in $((end-start))s"
}

main "$@"
