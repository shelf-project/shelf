#!/usr/bin/env bash
# cold-start/run.sh — 2 -> 20 worker scale-up TTFQ measurement.
#
# Scaffolding only.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BENCH="cold-start"

BACKEND=""
CYCLES=3
INITIAL_WORKERS=2
SCALED_WORKERS=20
APPLY=0
RESULTS_DIR=""

usage() {
  cat <<EOF
Usage: $0 --backend=<name> [options]

Required:
  --backend=<name>   One of: raw-s3, fs-cache, alluxio-2-9, alluxio-3-dora, shelf

Options:
  --cycles=N              Scale-up/scale-down cycles (default: 3).
  --initial-workers=N     Starting worker count (default: 2).
  --scaled-workers=N      Peak worker count (default: 20).
  --results-dir=PATH      Override default results dir.
  --dry-run / --apply     Plan only (default) vs execute.

See SPEC.md for authoritative method.
EOF
}

for arg in "$@"; do
  case "$arg" in
    --backend=*)         BACKEND="${arg#*=}";;
    --cycles=*)          CYCLES="${arg#*=}";;
    --initial-workers=*) INITIAL_WORKERS="${arg#*=}";;
    --scaled-workers=*)  SCALED_WORKERS="${arg#*=}";;
    --results-dir=*)     RESULTS_DIR="${arg#*=}";;
    --dry-run)           APPLY=0;;
    --apply)             APPLY=1;;
    -h|--help)           usage; exit 0;;
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

log() { printf '[cold-start] %s\n' "$*"; }

log "run_id=$RUN_ID backend=$BACKEND cycles=$CYCLES init=$INITIAL_WORKERS scaled=$SCALED_WORKERS apply=$APPLY"
log "results -> $OUT"

if [[ "$APPLY" -eq 0 ]]; then
  log "DRY-RUN — no cluster side effects."
  log "would: warm-up 2-worker cluster to ≥80% hit rate"
  log "would: kubectl scale statefulset/trino-worker --replicas=$SCALED_WORKERS"
  log "would: fan-out 20 dashboard queries, record TTFQ per (query, cycle)"
  log "would: scale back down; repeat $CYCLES times"
fi

mkdir -p "$RESULTS_DIR"

cat > "$OUT" <<EOF
{
  "run_id": "$RUN_ID",
  "timestamp": "$TS",
  "commit_sha": "$COMMIT",
  "release_tag": "$TAG",
  "benchmark": "cold-start",
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
    "trino_worker_count_initial": $INITIAL_WORKERS,
    "trino_worker_count_scaled": $SCALED_WORKERS,
    "shelf_instance_type": "i4i.2xlarge",
    "shelf_node_count": 3,
    "driver_instance_type": "m6i.large"
  },
  "queries": [],
  "summary": {
    "latency_ns_p50": 0,
    "latency_ns_p95": 0,
    "latency_ns_p99": 0,
    "latency_ns_p999": 0,
    "hit_rate": 0,
    "bytes_read": 0,
    "bytes_admitted": 0,
    "dollars_per_query": 0,
    "scale_up_latency_seconds": 0
  }
}
EOF

log "wrote skeleton record to $OUT"
exit 0
