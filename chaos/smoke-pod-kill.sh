#!/usr/bin/env bash
# SHELF-28 green-in-CI smoke variant of the pod-kill chaos drill.
#
# Cluster-mode equivalent: chaos/pod-kill.sh, which deletes a real K8s
# pod and measures cumulative hit rate during the heal window against
# a Prometheus baseline. This smoke variant stops instead of deletes
# (compose has no notion of "re-scheduling"), and asserts that:
#
#   1. While shelfd is down, plugin-side reads fail-open to S3 (the
#      smoke stack's Trino keeps answering SELECTs without errors).
#   2. When shelfd comes back, warm `shelf_hits_total` resumes
#      climbing — the same shape the cluster drill asserts.
#
# Non-goal: measuring cumulative hit rate during the heal window. That
# requires a sustained traffic generator + Prometheus, neither of which
# the compose harness runs.
set -euo pipefail

SMOKE_DIR="${SMOKE_DIR:-benchmarks/smoke}"
cd "$(dirname "$0")/.."

log() { printf '%s [pod-kill-smoke] %s\n' "$(date -u +%FT%TZ)" "$*"; }

cleanup() {
  log "tearing down compose"
  (cd "$SMOKE_DIR" && docker compose down -v >/dev/null 2>&1) || true
}
trap cleanup EXIT

log "booting compose stack"
(cd "$SMOKE_DIR" && docker compose build shelfd >/dev/null)
(cd "$SMOKE_DIR" && docker compose up -d >/dev/null)
(cd "$SMOKE_DIR" && ./run-smoke.sh wait-healthy)

log "priming cache with the canonical smoke loop"
(cd "$SMOKE_DIR" && ./run-smoke.sh)

log "simulating pod kill: stopping shelfd while Trino is still up"
(cd "$SMOKE_DIR" && docker compose stop shelfd)

log "verify Trino still answers SELECTs (fail-open contract — BLUEPRINT §9.5)"
# The fail-open contract says: when shelfd is unreachable, the plugin
# MUST transparently fall through to direct S3/MinIO. A trivial proof
# is that Trino can still service a SELECT that reads from the MinIO-
# backed Iceberg catalog seeded by the smoke stack. A real cluster-
# mode drill asserts this over 10 minutes of sustained traffic; the
# smoke variant asserts a single successful query, which is enough to
# catch a broken fail-open path (it would timeout or 500 instead).
set +e
docker exec shelf-smoke-trino trino --server http://localhost:8080 \
  --execute "SELECT count(*) FROM iceberg.smoke.sales" >/tmp/failopen.out 2>&1
status=$?
set -e
if [[ "$status" -ne 0 ]]; then
  log "FAIL: Trino errored while shelfd was down (fail-open broken)"
  cat /tmp/failopen.out || true
  exit 1
fi
log "fail-open held: Trino returned $(tr -d '[:space:]' </tmp/failopen.out | head -c 40)"

log "bring shelfd back and confirm hits resume"
(cd "$SMOKE_DIR" && docker compose start shelfd)
for i in {1..30}; do
  if curl -fsS http://127.0.0.1:9090/healthz >/dev/null 2>&1; then
    log "shelfd healthy again (took ${i} s)"
    break
  fi
  sleep 1
  if [[ "$i" == "30" ]]; then
    log "FAIL: shelfd did not come back within 30 s"
    exit 1
  fi
done

(cd "$SMOKE_DIR" && ./run-smoke.sh)

log "PASS"
