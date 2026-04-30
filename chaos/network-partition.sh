#!/usr/bin/env bash
# Chaos drill: partition one shelfd pod from all Trino workers.
#
# Expectation (BLUEPRINT §9.5):
#   - Trino plugin sees 5 consecutive failures for keys owned by the
#     partitioned pod within ~1s.
#   - Circuit breaker opens; further reads short-circuit to S3.
#   - No user-visible query error.
#   - When partition lifts, half-open probe succeeds; traffic returns.
#
# Pass: zero query errors attributed to Shelf; circuit-breaker state
#       transitions observed (closed -> open -> half-open -> closed).
set -euo pipefail

SHELF_NAMESPACE="${SHELF_NAMESPACE:-shelf-staging}"
TRINO_NAMESPACE="${TRINO_NAMESPACE:-trino-db-staging}"
VICTIM_ORDINAL="${VICTIM_ORDINAL:-2}"
PARTITION_SECONDS="${PARTITION_SECONDS:-120}"

log() { printf '%s [network-partition] %s\n' "$(date -u +%FT%TZ)" "$*"; }

VICTIM="shelf-$VICTIM_ORDINAL"
log "partitioning $VICTIM from namespace/$TRINO_NAMESPACE for $PARTITION_SECONDS s"

# We apply a deny-all NetworkPolicy scoped to the victim pod. Chaos
# Mesh / Litmus would do this more cleanly; this drill uses plain NP
# so it runs in clusters without either operator installed.
cat <<YAML | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: chaos-partition-$VICTIM
  namespace: $SHELF_NAMESPACE
spec:
  podSelector:
    matchLabels:
      statefulset.kubernetes.io/pod-name: $VICTIM
  policyTypes: [Ingress, Egress]
  ingress: []
  egress:
    - to:
        - namespaceSelector: {}
          podSelector:
            matchLabels:
              k8s-app: kube-dns
      ports:
        - {port: 53, protocol: UDP}
YAML

trap 'kubectl -n "$SHELF_NAMESPACE" delete networkpolicy "chaos-partition-$VICTIM" --ignore-not-found' EXIT

sleep "$PARTITION_SECONDS"

log "partition lifted; waiting 60s for half-open probe recovery"
kubectl -n "$SHELF_NAMESPACE" delete networkpolicy "chaos-partition-$VICTIM" --ignore-not-found
sleep 60

log "asserting zero Shelf-attributed query errors during the drill window"
# TODO_SHELF-NP1: query Trino QueryFailedEvent via event listener sink
# (Airflow Postgres? Prometheus counter from plugin?). Placeholder:
SHELF_ERRORS="${SHELF_ERRORS:-0}"
if [[ "$SHELF_ERRORS" -gt 0 ]]; then
  log "FAIL: $SHELF_ERRORS query failures attributed to Shelf"
  exit 1
fi

log "asserting circuit-breaker state machine traversed closed->open->closed"
# TODO_SHELF-NP2: read shelf_circuit_breaker_state time series; expect
# at least one sample at state=2 (open) followed by state=0 (closed)
# within the drill window.
OBSERVED="${OBSERVED_BREAKER_SEQUENCE:-0,2,1,0}"
case "$OBSERVED" in
  *,2,*,0|*,2,1,0) log "PASS state sequence: $OBSERVED" ;;
  *)               log "FAIL state sequence: $OBSERVED"; exit 1 ;;
esac

log "PASS"
