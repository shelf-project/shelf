#!/usr/bin/env bash
# benchmarks/in-cluster/down.sh
#
# Tear down the Shelf benchmark fixture in the trino-bench namespace.
# Idempotent: safe to re-run on a partially-uninstalled fixture.
#
# Side effects (in order):
#   1. helm uninstall trino-bench  (removes Trino StatefulSet + Service)
#   2. helm uninstall shelf-bench  (removes shelfd StatefulSet + PVCs)
#   3. kubectl delete ns trino-bench (final reaping; ConfigMaps, Secrets,
#      ServiceAccounts, NetworkPolicies, residual PVCs all reaped here)
#
# Optional:
#   ARCHIVE_RESULTS=1   archive ./benchmarks/results/<today> to
#                       s3://${SHELF_BENCH_RESULTS_BUCKET}/<today>/ first
#   KEEP_NAMESPACE=1    skip the `kubectl delete ns` step (useful when
#                       investigating a failed run)

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
NS="trino-bench"

ARCHIVE_RESULTS="${ARCHIVE_RESULTS:-0}"
KEEP_NAMESPACE="${KEEP_NAMESPACE:-0}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl not found on PATH" >&2
  exit 2
fi
if ! command -v helm >/dev/null 2>&1; then
  echo "ERROR: helm not found on PATH" >&2
  exit 2
fi

if ! kubectl get ns "$NS" >/dev/null 2>&1; then
  echo "[down] namespace $NS does not exist; nothing to do."
  exit 0
fi

# 0a. Grafana snapshot — must run BEFORE helm uninstall so the
# dashboard can still pull live metrics from shelf-bench. This is an
# optional step; skipped silently if GRAFANA_URL / GRAFANA_TOKEN are
# not set.
TODAY="$(date -u +%F)"
SRC="$HERE/../results/$TODAY"
mkdir -p "$SRC"

if [[ -n "${GRAFANA_URL:-}" && -n "${GRAFANA_TOKEN:-}" ]]; then
  GRAFANA_DASHBOARD_UID="${GRAFANA_DASHBOARD_UID:-shelf-overview}"
  echo "[down] snapshot Grafana dashboard $GRAFANA_DASHBOARD_UID → $SRC/grafana-snapshot.json"
  if command -v curl >/dev/null 2>&1; then
    curl -sS -H "Authorization: Bearer $GRAFANA_TOKEN" \
      "$GRAFANA_URL/api/dashboards/uid/$GRAFANA_DASHBOARD_UID" \
      -o "$SRC/grafana-snapshot.json" \
      || echo "[down] WARN: Grafana snapshot fetch failed; continuing teardown"
  else
    echo "[down] WARN: curl missing; skipping Grafana snapshot"
  fi
fi

# 0b. Optional results archive — pre-uninstall so any post-mortem
# kubectl describe / logs are still reachable from $SRC if the
# operator wants them.
if [[ "$ARCHIVE_RESULTS" == "1" ]]; then
  if [[ -z "${SHELF_BENCH_RESULTS_BUCKET:-}" ]]; then
    echo "ERROR: ARCHIVE_RESULTS=1 requires SHELF_BENCH_RESULTS_BUCKET" >&2
    exit 2
  fi
  if ! command -v aws >/dev/null 2>&1; then
    echo "ERROR: aws CLI required for ARCHIVE_RESULTS=1" >&2
    exit 2
  fi
  if [[ -d "$SRC" ]]; then
    echo "[down] archiving $SRC -> s3://$SHELF_BENCH_RESULTS_BUCKET/$TODAY/"
    aws s3 sync "$SRC" "s3://$SHELF_BENCH_RESULTS_BUCKET/$TODAY/" \
      --exclude '.DS_Store' --quiet
  else
    echo "[down] no results dir at $SRC; skipping archive"
  fi
fi

# 1. Trino chart
if helm -n "$NS" status trino-bench >/dev/null 2>&1; then
  echo "[down] helm uninstall trino-bench"
  helm -n "$NS" uninstall trino-bench --wait --timeout 5m || \
    echo "[down] WARN: trino-bench uninstall returned non-zero"
fi

# 2. Shelf chart — second so the catalog ConfigMaps are still around
# for any post-mortem grep before the namespace gets reaped.
if helm -n "$NS" status shelf-bench >/dev/null 2>&1; then
  echo "[down] helm uninstall shelf-bench"
  helm -n "$NS" uninstall shelf-bench --wait --timeout 5m || \
    echo "[down] WARN: shelf-bench uninstall returned non-zero"
fi

# 3. Delete the namespace if asked. This reaps any straggler PVCs,
# ConfigMaps, NetworkPolicies, ServiceAccounts.
if [[ "$KEEP_NAMESPACE" == "1" ]]; then
  echo "[down] KEEP_NAMESPACE=1 — leaving $NS in place."
  exit 0
fi

echo "[down] kubectl delete ns $NS  (this reaps residual PVCs / ConfigMaps)"
kubectl delete ns "$NS" --wait=true --timeout=5m

echo "[down] DONE. Cluster is back to its pre-bench state."
