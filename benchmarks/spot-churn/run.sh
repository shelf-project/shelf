#!/usr/bin/env bash
# spot-churn/run.sh — 50% worker kill every 5 min for 1 h.
#
# Scaffolding only.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BENCH="spot-churn"

BACKEND=""
WARMUP_MIN=20
RUN_MIN=60
QPS=10
KILL_PCT=50
KILL_INTERVAL_MIN=5
APPLY=0
RESULTS_DIR=""

usage() {
  cat <<EOF
Usage: $0 --backend=<name> [options]

Required:
  --backend=<name>   One of: raw-s3, fs-cache, alluxio-2-9, alluxio-3-dora, shelf

Options:
  --warmup-min=N           Warm-up minutes before chaos (default: 20).
  --run-min=N              Steady-state run minutes (default: 60).
  --qps=N                  Dashboard-query rate (default: 10).
  --kill-pct=N             Percent of workers to kill per event (default: 50).
  --kill-interval-min=N    Minutes between kill events (default: 5).
  --results-dir=PATH       Override default results dir.
  --dry-run / --apply      Plan only (default) vs execute.

See SPEC.md for authoritative method.
EOF
}

for arg in "$@"; do
  case "$arg" in
    --backend=*)             BACKEND="${arg#*=}";;
    --warmup-min=*)          WARMUP_MIN="${arg#*=}";;
    --run-min=*)             RUN_MIN="${arg#*=}";;
    --qps=*)                 QPS="${arg#*=}";;
    --kill-pct=*)            KILL_PCT="${arg#*=}";;
    --kill-interval-min=*)   KILL_INTERVAL_MIN="${arg#*=}";;
    --results-dir=*)         RESULTS_DIR="${arg#*=}";;
    --dry-run)               APPLY=0;;
    --apply)                 APPLY=1;;
    -h|--help)               usage; exit 0;;
    *) echo "unknown arg: $arg" >&2; usage; exit 2;;
  esac
done

if [[ -z "$BACKEND" ]]; then
  echo "ERROR: --backend=<name> is required" >&2
  usage; exit 2
fi

case "$BACKEND" in
  raw-s3|fs-cache|alluxio-2-9|alluxio-3-dora|shelf) : ;;
  *) echo "ERROR: unknown backend $BACKEND" >&2; exit 2;;
esac

RUN_ID="${SHELF_BENCH_RUN_ID:-01H0000000000000000000SHELF}"
TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
DATE_DIR="$(date -u +"%Y-%m-%d")"
RESULTS_DIR="${RESULTS_DIR:-$HERE/../results/$DATE_DIR/$BACKEND}"
OUT="$RESULTS_DIR/$BENCH-$RUN_ID.json"

COMMIT="${SHELF_BENCH_COMMIT_SHA:-0000000000000000000000000000000000000000}"
TAG="${SHELF_BENCH_RELEASE_TAG:-v0.0-scaffold}"

log() { printf '[spot-churn] %s\n' "$*"; }

log "run_id=$RUN_ID backend=$BACKEND warmup=${WARMUP_MIN}m run=${RUN_MIN}m qps=$QPS kill=${KILL_PCT}%/${KILL_INTERVAL_MIN}m apply=$APPLY"
log "results -> $OUT"

if [[ "$APPLY" -eq 0 ]]; then
  log "DRY-RUN — no cluster side effects."
  log "would: warm-up for ${WARMUP_MIN} min at $QPS QPS"
  log "would: run chaos loop (kill $KILL_PCT% of workers every $KILL_INTERVAL_MIN min) for ${RUN_MIN} min"
  log "would: record 10-s hit-rate samples + chaos-event log"
fi

mkdir -p "$RESULTS_DIR"

cat > "$OUT" <<EOF
{
  "run_id": "$RUN_ID",
  "timestamp": "$TS",
  "commit_sha": "$COMMIT",
  "release_tag": "$TAG",
  "benchmark": "spot-churn",
  "backend": "$BACKEND",
  "config": {
    "config_hash": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
    "trino_image": "trinodb/trino:480",
    "backend_image": "scaffold",
    "plugin_jar_sha256": null
  },
  "cluster_shape": {
    "region": "ap-south-1",
    "k8s_version": "1.30",
    "trino_instance_type": "m6i.2xlarge",
    "trino_worker_count": 8,
    "shelf_instance_type": "i4i.2xlarge",
    "shelf_node_count": 3,
    "driver_instance_type": "m6i.large"
  },
  "samples": [],
  "chaos_events": [],
  "summary": {
    "latency_ns_p50": 0,
    "latency_ns_p95": 0,
    "latency_ns_p99": 0,
    "latency_ns_p999": 0,
    "hit_rate": 0,
    "hit_rate_floor": 0,
    "bytes_read": 0,
    "bytes_admitted": 0,
    "failed_queries_total": 0,
    "dollars_per_query": 0
  }
}
EOF

log "wrote skeleton record to $OUT"
exit 0
