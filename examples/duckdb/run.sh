#!/usr/bin/env bash
# End-to-end driver for the DuckDB-on-Shelf example.
#
# Brings up MinIO + iceberg-rest + shelfd, seeds a 1 M-row partitioned
# Iceberg `events` table, then runs the cold-vs-warm bench inside a
# python:3.11-slim container with duckdb installed via pip.
#
# Cleanup:
#   docker compose -f docker-compose.yml --profile bench down -v
#
# Usage:
#   bash run.sh                     # one-shot
#   KEEP_UP=1 bash run.sh           # keep the stack running after the bench

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

COMPOSE=${COMPOSE:-"docker compose"}
TIMEOUT_SECS=${TIMEOUT_SECS:-180}
SHELFD_DATA_HOST_PORT=${SHELFD_DATA_HOST_PORT:-9091}

log() { printf '[run] %s\n' "$*" >&2; }
fail() { printf '[run][FAIL] %s\n' "$*" >&2; exit 1; }

cleanup() {
  if [[ "${KEEP_UP:-0}" == "1" ]]; then
    log "KEEP_UP=1 set; leaving stack running. tear down with:"
    log "  $COMPOSE --profile bench down -v"
    return 0
  fi
  log "tearing down stack..."
  $COMPOSE --profile bench down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "validating compose file..."
$COMPOSE config -q

log "bringing up minio + iceberg-rest + shelfd + seed (this builds shelfd on first run, ~3-5 min)..."
$COMPOSE up -d --build minio minio-setup iceberg-rest shelfd

log "waiting up to ${TIMEOUT_SECS}s for shelfd healthcheck..."
deadline=$(( $(date +%s) + TIMEOUT_SECS ))
while (( $(date +%s) < deadline )); do
  if curl -fsS "http://127.0.0.1:${SHELFD_DATA_HOST_PORT}/healthz" >/dev/null 2>&1; then
    log "shelfd is healthy"
    break
  fi
  sleep 2
done
curl -fsS "http://127.0.0.1:${SHELFD_DATA_HOST_PORT}/healthz" >/dev/null \
  || fail "shelfd did not become healthy within ${TIMEOUT_SECS}s"

log "running iceberg seed (this writes ~1 M rows; first run installs pyiceberg, ~30-60s)..."
$COMPOSE up --abort-on-container-exit --exit-code-from seed seed

log "running bench (cold + warm DuckDB queries via shelfd:9092)..."
$COMPOSE --profile bench run --rm bench

log "done."
