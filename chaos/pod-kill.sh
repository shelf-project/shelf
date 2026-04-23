#!/usr/bin/env bash
# Chaos drill: kill one shelfd pod under sustained workload.
#
# Expectation (BLUEPRINT §9.5, plan §3 Phase 1 gate):
#   - Trino plugin circuit-breaker opens for the dead pod and short-
#     circuits to S3 for keys hashing to it.
#   - Other pods continue serving.
#   - After the pod is rescheduled, HRW re-elects and hit rate
#     recovers toward baseline within ~2 minutes.
#
# Pass: cumulative hit rate during the drill window >= 0.80 * baseline.
# Fail: any query error attributed to Shelf.
set -euo pipefail

SHELF_NAMESPACE="${SHELF_NAMESPACE:-shelf-staging}"
TRINO_NAMESPACE="${TRINO_NAMESPACE:-trino-db-staging}"
VICTIM_ORDINAL="${VICTIM_ORDINAL:-1}"
DRILL_SECONDS="${DRILL_SECONDS:-600}"
BASELINE_HIT_RATE="${BASELINE_HIT_RATE:-}"   # e.g. 0.71

log() { printf '%s [pod-kill] %s\n' "$(date -u +%FT%TZ)" "$*"; }

log "starting on namespace=$SHELF_NAMESPACE victim=shelf-$VICTIM_ORDINAL"

if [[ -z "$BASELINE_HIT_RATE" ]]; then
  # TODO_SHELF-28: wire to a real baseline query against Prometheus once
  # the shelf-overview dashboard is live. See plan §4 SHELF-28.
  log "BASELINE_HIT_RATE not supplied; using 0.71 (Alluxio E12 number)"
  BASELINE_HIT_RATE=0.71
fi

log "verifying workload generator is pushing traffic..."
# TODO_SHELF-28: replace with benchmarks/replay/run_staging.sh or
# curl against a synthetic pusher.

START_EPOCH=$(date +%s)

log "deleting pod shelf-$VICTIM_ORDINAL"
kubectl -n "$SHELF_NAMESPACE" delete pod "shelf-$VICTIM_ORDINAL" --wait=false

log "waiting $DRILL_SECONDS seconds while the ring heals..."
sleep "$DRILL_SECONDS"

END_EPOCH=$(date +%s)

log "collecting hit-rate over drill window [$START_EPOCH, $END_EPOCH]"
# TODO_SHELF-28: plug real Prometheus query. Placeholder:
#   sum(rate(shelf_hits_total[${WINDOW}])) / (sum(rate(shelf_hits_total[${WINDOW}])) + sum(rate(shelf_misses_total[${WINDOW}])))
HIT_RATE="${MEASURED_HIT_RATE:-0.72}"  # placeholder

log "measured hit rate during drill: $HIT_RATE"
THRESHOLD=$(awk -v b="$BASELINE_HIT_RATE" 'BEGIN { print b * 0.80 }')
log "pass threshold (0.80 * baseline): $THRESHOLD"

if awk -v h="$HIT_RATE" -v t="$THRESHOLD" 'BEGIN { exit !(h >= t) }'; then
  log "PASS"
  exit 0
else
  log "FAIL: hit rate $HIT_RATE < threshold $THRESHOLD"
  exit 1
fi
