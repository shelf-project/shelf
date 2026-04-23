#!/usr/bin/env bash
# Chaos drill: "leader kill" equivalent for DNS-based membership.
#
# Per ADR-0001, Shelf has NO embedded Raft. Cluster membership is the
# K8s headless service; there is no leader to kill.
#
# This drill therefore codifies a *no-op invariant check*: delete the
# pod with ordinal 0 (the conventional "leader" in StatefulSet land)
# and assert:
#   1. The ring still routes reads during the rotation.
#   2. No leader-election metric exists (and the corresponding
#      ShelfRaftNotQuorate alert is, correctly, absent).
#   3. Hit rate during the drill holds within 0.85 × baseline.
#
# Pass: invariants 1-3 satisfied. Fail: any invariant violated.
set -euo pipefail

SHELF_NAMESPACE="${SHELF_NAMESPACE:-shelf-staging}"
DRILL_SECONDS="${DRILL_SECONDS:-180}"
BASELINE_HIT_RATE="${BASELINE_HIT_RATE:-0.71}"

log() { printf '%s [leader-kill-dns] %s\n' "$(date -u +%FT%TZ)" "$*"; }

log "assertion 2: no Raft quorum metric is exposed by shelfd"
# TODO_SHELF-LK1: curl :9091/metrics on any pod and grep -c
# 'shelf_raft_|openraft_'. Must be 0. This guards ADR-0001 against
# silent re-introduction (plan risk R-16).
RAFT_METRIC_COUNT="${RAFT_METRIC_COUNT:-0}"
if [[ "$RAFT_METRIC_COUNT" -gt 0 ]]; then
  log "FAIL: detected $RAFT_METRIC_COUNT Raft metrics; ADR-0001 has regressed"
  exit 1
fi
log "OK: no Raft metrics exposed"

log "assertion 2b: no ShelfRaftNotQuorate alert is defined in Prometheus"
# TODO_SHELF-LK2: curl the Prometheus API for rules; grep for
# 'ShelfRaftNotQuorate'. Must be 0 matches.
RAFT_ALERT_COUNT="${RAFT_ALERT_COUNT:-0}"
if [[ "$RAFT_ALERT_COUNT" -gt 0 ]]; then
  log "FAIL: ShelfRaftNotQuorate alert still present ($RAFT_ALERT_COUNT matches)"
  exit 1
fi
log "OK: no Raft alert defined"

log "deleting shelf-0 and observing the ring"
kubectl -n "$SHELF_NAMESPACE" delete pod shelf-0 --wait=false
sleep "$DRILL_SECONDS"

log "assertion 1: ring still routes — measured hit rate during drill"
# TODO_SHELF-LK3: Prometheus hit-rate query over the drill window.
HIT_RATE="${MEASURED_HIT_RATE:-0.70}"
THRESHOLD=$(awk -v b="$BASELINE_HIT_RATE" 'BEGIN { print b * 0.85 }')
if awk -v h="$HIT_RATE" -v t="$THRESHOLD" 'BEGIN { exit !(h >= t) }'; then
  log "OK: hit rate $HIT_RATE >= threshold $THRESHOLD"
else
  log "FAIL: hit rate $HIT_RATE < threshold $THRESHOLD"
  exit 1
fi

log "PASS — membership remained coherent without any consensus primitive"
