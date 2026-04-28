#!/usr/bin/env bash
# SHELF-28 green-in-CI smoke variant of the KEDA-rotation chaos drill.
#
# The cluster-mode drill (chaos/pod-kill.sh, plus the Trino-worker half
# rehearsed by ops on rep-2) requires a live 3-pod Shelf StatefulSet
# and Prometheus. This smoke variant proves the assertion *script* is
# still correct against the docker-compose harness in benchmarks/smoke,
# which CI can run in under 60 s on a free ubuntu-22.04 runner.
#
# It does NOT prove the cluster-side invariant; it proves:
#   1. `shelfd` survives a restart without losing the ability to serve.
#   2. The 10 canonical queries still return the expected row counts
#      after the restart (the real KEDA drill asserts the same thing).
#   3. The `shelf_hits_total` counter still climbs on a warm re-run,
#      which is the "cumulative hit rate" shape the cluster drill
#      asserts against Alluxio baseline.
#
# Exit 0 on success, non-zero on any step failure.
#
# Invoked by the top-level Makefile target `make chaos-keda-rotation-smoke`.
set -euo pipefail

SMOKE_DIR="${SMOKE_DIR:-benchmarks/smoke}"
cd "$(dirname "$0")/.."

log() { printf '%s [keda-rotation-smoke] %s\n' "$(date -u +%FT%TZ)" "$*"; }

cleanup() {
  log "tearing down compose"
  (cd "$SMOKE_DIR" && docker compose down -v >/dev/null 2>&1) || true
}
trap cleanup EXIT

log "booting compose stack (this is the same stack smoke.yml uses)"
(cd "$SMOKE_DIR" && docker compose build shelfd >/dev/null)
(cd "$SMOKE_DIR" && docker compose up -d >/dev/null)
(cd "$SMOKE_DIR" && ./run-smoke.sh wait-healthy)

log "running 10-query cold/warm smoke — this populates the cache"
(cd "$SMOKE_DIR" && ./run-smoke.sh)

log "simulating KEDA rotation: restart shelfd (keeps the volume; the"
log "NVMe survival claim is what SHELF-18 cluster-acceptance covers)"
(cd "$SMOKE_DIR" && docker compose restart shelfd)

log "wait for shelfd /healthz to come back"
for i in {1..30}; do
  if curl -fsS http://127.0.0.1:9090/healthz >/dev/null 2>&1; then
    log "shelfd healthy again (took ${i} s)"
    break
  fi
  sleep 1
  if [[ "$i" == "30" ]]; then
    log "FAIL: shelfd did not come back within 30 s"
    (cd "$SMOKE_DIR" && docker compose logs shelfd --tail=80) || true
    exit 1
  fi
done

log "re-running 10-query smoke to prove the assertion math still holds"
(cd "$SMOKE_DIR" && ./run-smoke.sh)

log "PASS"
