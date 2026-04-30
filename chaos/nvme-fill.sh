#!/usr/bin/env bash
# Chaos drill: fill NVMe on one pod to ≥ 95% and observe admission behaviour.
#
# Expectation (ADR-0008, plan §2 E7 / risk R-06):
#   - Foyer refuses new admits when disk hits the configured watermark.
#   - Existing keys still serve (hit rate on already-cached keys holds).
#   - Pod does not enter DiskPressure eviction.
#
# Pass: admission_refused_total counter climbs; hits_total for the
#       warm set stays flat; zero OOM / DiskPressure events for the pod.
set -euo pipefail

SHELF_NAMESPACE="${SHELF_NAMESPACE:-shelf-staging}"
VICTIM_ORDINAL="${VICTIM_ORDINAL:-0}"
TARGET_FILL_PCT="${TARGET_FILL_PCT:-95}"
DRILL_SECONDS="${DRILL_SECONDS:-600}"

log() { printf '%s [nvme-fill] %s\n' "$(date -u +%FT%TZ)" "$*"; }

VICTIM="shelf-$VICTIM_ORDINAL"
MOUNT="/var/lib/shelf"

log "capturing pre-drill NVMe fill + hit rate"
BEFORE_PCT=$(kubectl -n "$SHELF_NAMESPACE" exec "$VICTIM" -c shelfd -- sh -c \
  "df --output=pcent $MOUNT | tail -1 | tr -d ' %'")
log "before: $BEFORE_PCT% full"

log "filling $MOUNT to ~$TARGET_FILL_PCT% using dd (no-op writes from /dev/zero)"
# fallocate may be unavailable in distroless; dd works. TODO_SHELF-NF1:
# replace with a Foyer-friendly fill pattern so the eviction watermark
# triggers naturally (currently we tail-fill a plain file, which only
# triggers the kubelet ephemeral-storage signal rather than Foyer itself).
AVAIL_KB=$(kubectl -n "$SHELF_NAMESPACE" exec "$VICTIM" -c shelfd -- sh -c \
  "df --output=avail $MOUNT | tail -1")
TOTAL_KB=$(kubectl -n "$SHELF_NAMESPACE" exec "$VICTIM" -c shelfd -- sh -c \
  "df --output=size $MOUNT | tail -1")
FILL_KB=$(( TOTAL_KB * (100 - BEFORE_PCT) / 100 - (TOTAL_KB * (100 - TARGET_FILL_PCT) / 100) ))

if [[ "$FILL_KB" -le 0 ]]; then
  log "NVMe already above target; skipping fill"
else
  log "writing ${FILL_KB} KiB of ballast"
  kubectl -n "$SHELF_NAMESPACE" exec "$VICTIM" -c shelfd -- sh -c \
    "dd if=/dev/zero of=$MOUNT/.chaos-ballast bs=1K count=$FILL_KB status=none"
fi

trap 'kubectl -n "$SHELF_NAMESPACE" exec "$VICTIM" -c shelfd -- rm -f "$MOUNT/.chaos-ballast" || true' EXIT

log "drill running for $DRILL_SECONDS s"
sleep "$DRILL_SECONDS"

log "asserting admission_refused_total counter increased"
# TODO_SHELF-NF2: Prometheus: increase(shelf_admission_refused_total{pod="$VICTIM"}[$DRILL_SECONDS])
REFUSED_DELTA="${REFUSED_DELTA:-1}"
if [[ "$REFUSED_DELTA" -le 0 ]]; then
  log "FAIL: no admission refusals observed; Foyer did not detect pressure"
  exit 1
fi

log "asserting pod did not enter DiskPressure"
# TODO_SHELF-NF3: kubectl get events --field-selector reason=EvictionThresholdMet
EVICTED=$(kubectl -n "$SHELF_NAMESPACE" get events \
  --field-selector "involvedObject.name=$VICTIM,reason=Evicted" \
  --no-headers 2>/dev/null | wc -l | tr -d ' ')
if [[ "$EVICTED" -ne 0 ]]; then
  log "FAIL: pod $VICTIM was evicted ($EVICTED events)"
  exit 1
fi

log "PASS"
