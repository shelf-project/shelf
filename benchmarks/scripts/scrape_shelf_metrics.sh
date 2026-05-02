#!/usr/bin/env bash
# benchmarks/scripts/scrape_shelf_metrics.sh
#
# Sidecar curl-pod helper for scraping shelfd /metrics + /stats from
# every pod in a shelf StatefulSet. shelfd ships in a distroless image
# with no shell and no wget, so `kubectl exec shelf-X -- wget` fails
# silently and produces zero hits — see the "Metric scrape gap" note in
# benchmarks/results/2026-05-01/SUMMARY.md for the failure trace.
#
# This script side-steps that by spinning up a one-shot curl pod in the
# same namespace, running curl from inside the cluster network (so the
# headless Service DNS resolves the way it does for the data plane),
# and capturing the response body straight to disk on the operator's
# laptop. The pod is torn down on every exit path including SIGINT.
#
# Usage:
#   scrape_shelf_metrics.sh \
#     --namespace      <ns>                       \
#     --service        shelf-bench-pool           \
#     --pod-prefix     shelf-bench                \
#     --pod-count      4                          \
#     --output-dir     /tmp/run-2026-05-02/pre    \
#     --phase          pre                        \
#     [--curl-image    curlimages/curl:8.10.1]    \
#     [--metrics-port  9090]                      \
#     [--no-stats]                                \
#     [--dry-run]
#
# Outputs (one per pod):
#   <output-dir>/<pod-prefix>-<i>-metrics-<phase>.txt    (Prom text)
#   <output-dir>/<pod-prefix>-<i>-stats-<phase>.json     (JSON)
#
# Exit codes:
#   0  every pod scraped, files written.
#   1  one or more pod scrapes failed (per-pod files still written when
#      partial output was captured).
#   2  bad CLI / preflight failure.

set -euo pipefail

NAMESPACE=""
SERVICE=""
POD_PREFIX="shelf-bench"
POD_COUNT=4
OUTPUT_DIR=""
PHASE=""
CURL_IMAGE="curlimages/curl:8.10.1"
METRICS_PORT=9090
WANT_STATS=1
DRY_RUN=0

usage() {
  sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)     NAMESPACE="$2"; shift 2;;
    --service)       SERVICE="$2"; shift 2;;
    --pod-prefix)    POD_PREFIX="$2"; shift 2;;
    --pod-count)     POD_COUNT="$2"; shift 2;;
    --output-dir)    OUTPUT_DIR="$2"; shift 2;;
    --phase)         PHASE="$2"; shift 2;;
    --curl-image)    CURL_IMAGE="$2"; shift 2;;
    --metrics-port)  METRICS_PORT="$2"; shift 2;;
    --no-stats)      WANT_STATS=0; shift;;
    --dry-run)       DRY_RUN=1; shift;;
    -h|--help)       usage; exit 0;;
    *) echo "unknown arg: $1" >&2; usage; exit 2;;
  esac
done

for v in NAMESPACE SERVICE OUTPUT_DIR PHASE; do
  if [[ -z "${!v}" ]]; then
    echo "ERROR: --${v,,} is required" >&2
    exit 2
  fi
done

case "$PHASE" in
  pre|post|prewarm|measurement) : ;;
  *) echo "ERROR: --phase must be pre|post|prewarm|measurement" >&2; exit 2;;
esac

if ! [[ "$POD_COUNT" =~ ^[0-9]+$ ]] || (( POD_COUNT < 1 )); then
  echo "ERROR: --pod-count must be a positive integer" >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR"

CURL_POD="shelf-metrics-scraper-$RANDOM"
SVC_FQDN_BASE="${SERVICE}.${NAMESPACE}.svc.cluster.local"

log() { printf '[scrape] %s\n' "$*" >&2; }

if [[ "$DRY_RUN" -eq 1 ]]; then
  log "DRY-RUN — would create pod ${CURL_POD} in ns=${NAMESPACE}"
  for i in $(seq 0 $((POD_COUNT - 1))); do
    log "  would scrape http://${POD_PREFIX}-${i}.${SVC_FQDN_BASE}:${METRICS_PORT}/metrics"
    log "  would scrape http://${POD_PREFIX}-${i}.${SVC_FQDN_BASE}:${METRICS_PORT}/stats"
  done
  log "DRY-RUN — no kubectl side effects."
  exit 0
fi

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl not found on PATH" >&2
  exit 2
fi

cleanup() {
  local rc=$?
  if kubectl -n "$NAMESPACE" get pod "$CURL_POD" >/dev/null 2>&1; then
    log "tearing down ${CURL_POD}"
    kubectl -n "$NAMESPACE" delete pod "$CURL_POD" \
        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
  fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

log "creating ephemeral curl pod ${CURL_POD} in ns=${NAMESPACE}"
kubectl -n "$NAMESPACE" run "$CURL_POD" \
  --image="$CURL_IMAGE" \
  --restart=Never \
  --command -- sleep 600 >/dev/null

log "waiting for ${CURL_POD} to become Ready (60 s timeout)"
if ! kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/"$CURL_POD" \
        --timeout=60s >/dev/null; then
  echo "ERROR: curl pod did not become Ready in 60 s" >&2
  kubectl -n "$NAMESPACE" describe pod "$CURL_POD" >&2 || true
  exit 1
fi

failures=0
for i in $(seq 0 $((POD_COUNT - 1))); do
  url_metrics="http://${POD_PREFIX}-${i}.${SVC_FQDN_BASE}:${METRICS_PORT}/metrics"
  out_metrics="${OUTPUT_DIR}/${POD_PREFIX}-${i}-metrics-${PHASE}.txt"
  log "scraping ${url_metrics}"
  if ! kubectl -n "$NAMESPACE" exec "$CURL_POD" -- \
        curl --silent --show-error --fail --max-time 15 \
            "$url_metrics" >"$out_metrics"; then
    log "  ! scrape failed for ${POD_PREFIX}-${i}/metrics"
    failures=$((failures + 1))
  fi

  if [[ "$WANT_STATS" -eq 1 ]]; then
    url_stats="http://${POD_PREFIX}-${i}.${SVC_FQDN_BASE}:${METRICS_PORT}/stats"
    out_stats="${OUTPUT_DIR}/${POD_PREFIX}-${i}-stats-${PHASE}.json"
    log "scraping ${url_stats}"
    if ! kubectl -n "$NAMESPACE" exec "$CURL_POD" -- \
          curl --silent --show-error --fail --max-time 10 \
              "$url_stats" >"$out_stats"; then
      log "  ! scrape failed for ${POD_PREFIX}-${i}/stats"
      failures=$((failures + 1))
    fi
  fi
done

if (( failures > 0 )); then
  log "completed with ${failures} scrape failure(s)"
  exit 1
fi

log "all ${POD_COUNT} pods scraped to ${OUTPUT_DIR}"
exit 0
