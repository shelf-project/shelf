#!/usr/bin/env bash
# replay/run.sh — 7-day rep-2 trino_queries replay.
#
# Scaffolding only: writes a schema-valid skeleton record.
# This is the v0.5 kill-switch benchmark (ADR-0010).

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BENCH="replay"

BACKEND=""
REPLICA="rep-2"
DAYS=7
SPEED="2x"
FROM=""
TO=""
APPLY=0
RESULTS_DIR=""

usage() {
  cat <<EOF
Usage: $0 --backend=<name> [options]

Required:
  --backend=<name>   One of: raw-s3, fs-cache, alluxio-2-9, alluxio-3-dora, shelf

Options:
  --replica=rep-0|rep-1|rep-2|rep-3  Trace source replica (default: rep-2).
  --days=N                 Trace window in days (default: 7).
  --speed=1x|2x|10x        Replay speed (default: 2x). Gate evaluated only at 2x.
  --from="ISO8601"         Explicit start timestamp. Overrides --days.
  --to="ISO8601"           Explicit end timestamp.
  --results-dir=PATH       Override default results dir.
  --dry-run / --apply      Plan only (default) vs execute.

See SPEC.md and ADR-0010 for the gate rules.
EOF
}

for arg in "$@"; do
  case "$arg" in
    --backend=*)     BACKEND="${arg#*=}";;
    --replica=*)     REPLICA="${arg#*=}";;
    --days=*)        DAYS="${arg#*=}";;
    --speed=*)       SPEED="${arg#*=}";;
    --from=*)        FROM="${arg#*=}";;
    --to=*)          TO="${arg#*=}";;
    --results-dir=*) RESULTS_DIR="${arg#*=}";;
    --dry-run)       APPLY=0;;
    --apply)         APPLY=1;;
    -h|--help)       usage; exit 0;;
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

case "$SPEED" in
  1x|2x|10x) : ;;
  *) echo "ERROR: speed must be 1x|2x|10x" >&2; exit 2;;
esac

case "$REPLICA" in
  rep-0|rep-1|rep-2|rep-3) : ;;
  *) echo "ERROR: replica must be rep-{0..3}" >&2; exit 2;;
esac

if [[ -z "$FROM" ]]; then
  FROM="$(date -u -v -"${DAYS}"d +"%Y-%m-%dT00:00:00Z" 2>/dev/null \
           || date -u -d "${DAYS} days ago" +"%Y-%m-%dT00:00:00Z")"
fi
if [[ -z "$TO" ]]; then
  TO="$(date -u +"%Y-%m-%dT00:00:00Z")"
fi

RUN_ID="${SHELF_BENCH_RUN_ID:-01H0000000000000000000SHELF}"
TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
DATE_DIR="$(date -u +"%Y-%m-%d")"
RESULTS_DIR="${RESULTS_DIR:-$HERE/../results/$DATE_DIR/$BACKEND}"
OUT="$RESULTS_DIR/$BENCH-$RUN_ID.json"

COMMIT="${SHELF_BENCH_COMMIT_SHA:-0000000000000000000000000000000000000000}"
TAG="${SHELF_BENCH_RELEASE_TAG:-v0.0-scaffold}"

log() { printf '[replay] %s\n' "$*"; }

log "run_id=$RUN_ID backend=$BACKEND replica=$REPLICA from=$FROM to=$TO speed=$SPEED apply=$APPLY"
log "results -> $OUT"

if [[ "$APPLY" -eq 0 ]]; then
  log "DRY-RUN — no cluster side effects."
  log "would: pull trace snapshot of cdp.trino_logs.trino_queries($REPLICA) in [$FROM, $TO)"
  log "would: reset $BACKEND cache"
  log "would: replay queries at ${SPEED} real-time; sample hit_rate + dbt ok-rate every 10 s"
  log "would: evaluate v0.5 gate against ADR-0010 thresholds"
fi

mkdir -p "$RESULTS_DIR"

VERDICT="pending"
if [[ "$SPEED" != "2x" ]]; then
  VERDICT="n/a"
fi

cat > "$OUT" <<EOF
{
  "run_id": "$RUN_ID",
  "timestamp": "$TS",
  "commit_sha": "$COMMIT",
  "release_tag": "$TAG",
  "benchmark": "replay",
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
    "trino_worker_count": 3,
    "shelf_instance_type": "i4i.2xlarge",
    "shelf_node_count": 3,
    "driver_instance_type": "m6i.large",
    "scale_factor": null,
    "partial": false
  },
  "trace": {
    "source_table": "cdp.trino_logs.trino_queries",
    "snapshot_id": "0000000000000000000",
    "from": "$FROM",
    "to": "$TO",
    "replica": "$REPLICA",
    "query_count": 0,
    "speed": "$SPEED"
  },
  "samples": [],
  "summary": {
    "latency_ns_p50": 0,
    "latency_ns_p95": 0,
    "latency_ns_p99": 0,
    "latency_ns_p999": 0,
    "hit_rate": 0,
    "bytes_read": 0,
    "bytes_admitted": 0,
    "dollars_per_query": 0
  },
  "gate": {
    "hit_rate_7d_cumulative": 0,
    "gold_dbt_ok_rate": 0,
    "latency_ns_p95_vs_alluxio": null,
    "shelf_caused_pages": 0,
    "oncall_surface_ratio": null,
    "verdict": "$VERDICT",
    "failed_metrics": []
  }
}
EOF

log "wrote skeleton record to $OUT"
exit 0
