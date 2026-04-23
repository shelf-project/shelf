#!/usr/bin/env bash
# tpcds/run.sh — TPC-DS @ 1 TB runner.
#
# Scaffolding only: echoes the plan, writes a schema-valid (but empty)
# result record, and exits 0. Real execution lands in SHELF-26.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BENCH="tpcds"

# Defaults
BACKEND=""
SCALE="1tb"
ITERATIONS=3
PROFILE="full"
APPLY=0
RESULTS_DIR=""

usage() {
  cat <<EOF
Usage: $0 --backend=<name> [options]

Required:
  --backend=<name>   One of: raw-s3, fs-cache, alluxio-2-9, alluxio-3-dora, shelf

Options:
  --scale=1tb|100gb|10gb    TPC-DS scale factor (default: 1tb).
  --iterations=N            Warm iterations per query (default: 3).
  --profile=full|smoke      smoke = 3 queries (Q3/Q19/Q42); full = all 99.
  --results-dir=PATH        Override default results dir.
  --dry-run                 Plan only. Default.
  --apply                   Execute against the cluster.

See SPEC.md for authoritative method and metrics.
EOF
}

for arg in "$@"; do
  case "$arg" in
    --backend=*)      BACKEND="${arg#*=}";;
    --scale=*)        SCALE="${arg#*=}";;
    --iterations=*)   ITERATIONS="${arg#*=}";;
    --profile=*)      PROFILE="${arg#*=}";;
    --results-dir=*)  RESULTS_DIR="${arg#*=}";;
    --dry-run)        APPLY=0;;
    --apply)          APPLY=1;;
    -h|--help)        usage; exit 0;;
    *) echo "unknown arg: $arg" >&2; usage; exit 2;;
  esac
done

if [[ -z "$BACKEND" ]]; then
  echo "ERROR: --backend=<name> is required" >&2
  usage
  exit 2
fi

case "$BACKEND" in
  raw-s3|fs-cache|alluxio-2-9|alluxio-3-dora|shelf) : ;;
  *) echo "ERROR: unknown backend $BACKEND" >&2; exit 2;;
esac

# -----------------------------------------------------------------------------
# Produce identifiers for this run.
# -----------------------------------------------------------------------------
RUN_ID="${SHELF_BENCH_RUN_ID:-}"
if [[ -z "$RUN_ID" ]]; then
  # Fallback ULID: 26 uppercase base32 chars. We use a non-cryptographic
  # fallback here; real implementation uses a proper ULID generator.
  RUN_ID="$(date -u +%s | awk '{printf "01H%023dSHELF", $1 % 100000000}' | tr 'a-z' 'A-Z' | head -c 26)"
  RUN_ID="${RUN_ID:-01H0000000000000000000SHELF}"
fi

TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
DATE_DIR="$(date -u +"%Y-%m-%d")"
RESULTS_DIR="${RESULTS_DIR:-$HERE/../results/$DATE_DIR/$BACKEND}"
OUT="$RESULTS_DIR/$BENCH-$RUN_ID.json"

COMMIT="${SHELF_BENCH_COMMIT_SHA:-0000000000000000000000000000000000000000}"
TAG="${SHELF_BENCH_RELEASE_TAG:-v0.0-scaffold}"

log() { printf '[tpcds] %s\n' "$*"; }

log "run_id=$RUN_ID backend=$BACKEND scale=$SCALE profile=$PROFILE iterations=$ITERATIONS apply=$APPLY"
log "results -> $OUT"

if [[ "$APPLY" -eq 0 ]]; then
  log "DRY-RUN — no cluster side effects."
  log "would: apply configs/$BACKEND/, flush cache, run TPC-DS queries, record per-query measurements"
  log "would: validate output against schema.json, upload to results bucket"
fi

# -----------------------------------------------------------------------------
# Write a schema-valid skeleton record. Fields are zero-valued but the
# structure matches schema.json so downstream tooling can be wired up.
# -----------------------------------------------------------------------------
mkdir -p "$RESULTS_DIR"

QUERIES="[]"
if [[ "$PROFILE" = "smoke" ]]; then
  # Smoke profile: Q3, Q19, Q42. Skeleton records only.
  # TODO_SHELF-12 / TODO_SHELF-26: replace with real measurements.
  QUERIES='[
    {"query_id":"q3","iteration":0,"mode":"warm","latency_ns_p50":0,"latency_ns_p95":0,"latency_ns_p99":0,"latency_ns_p999":0,"hit_rate":0,"bytes_read":0,"bytes_admitted":0,"dollars_per_query":0,"failed":false,"failure_reason":null},
    {"query_id":"q19","iteration":0,"mode":"warm","latency_ns_p50":0,"latency_ns_p95":0,"latency_ns_p99":0,"latency_ns_p999":0,"hit_rate":0,"bytes_read":0,"bytes_admitted":0,"dollars_per_query":0,"failed":false,"failure_reason":null},
    {"query_id":"q42","iteration":0,"mode":"warm","latency_ns_p50":0,"latency_ns_p95":0,"latency_ns_p99":0,"latency_ns_p999":0,"hit_rate":0,"bytes_read":0,"bytes_admitted":0,"dollars_per_query":0,"failed":false,"failure_reason":null}
  ]'
fi

cat > "$OUT" <<EOF
{
  "run_id": "$RUN_ID",
  "timestamp": "$TS",
  "commit_sha": "$COMMIT",
  "release_tag": "$TAG",
  "benchmark": "tpcds",
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
    "driver_instance_type": "m6i.large"
  },
  "queries": $QUERIES,
  "summary": {
    "latency_ns_p50": 0,
    "latency_ns_p95": 0,
    "latency_ns_p99": 0,
    "latency_ns_p999": 0,
    "hit_rate": 0,
    "bytes_read": 0,
    "bytes_admitted": 0,
    "dollars_per_query": 0,
    "warm_up_seconds": null
  }
}
EOF

log "wrote skeleton record to $OUT"
log "next: validate with 'python3 -m jsonschema -i $OUT $HERE/schema.json'"
exit 0
