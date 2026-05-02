#!/usr/bin/env bash
# benchmarks/scripts/run_prod_replay.sh
#
# Production-trace replay harness for V1 of the rc8 roadmap.
#
# Wraps the existing SHELF-35 tooling (tools/gen_replay_list.py +
# tools/replay_pinlist.py) with a sidecar metric scraper, end-to-end
# orchestration, and schema-valid output records that satisfy
# benchmarks/replay/schema.json. The output shape mirrors
# benchmarks/results/2026-05-01/SUMMARY.md so the post-run summary.txt
# can be pasted into a release-cycle MR or status update without
# manual reformatting.
#
# Usage (in-cluster shelf-bench fixture, see
# benchmarks/scripts/RUNBOOK.md for the full operator playbook):
#
#   ./run_prod_replay.sh \
#     --window-days     7 \
#     --output-dir      benchmarks/results/$(date -u +%F)/prodreplay \
#     --shelf-endpoint  http://shelf-bench-pool.<ns>.svc.cluster.local:9092 \
#     --raw-endpoint    https://s3.<region>.amazonaws.com \
#     --trino-host      trino-bench-coordinator.<ns>.svc.cluster.local:8080 \
#     --catalog-shelf   bench_iceberg_shelf \
#     --catalog-raw     bench_iceberg \
#     --top-n           200
#
# All endpoint values, namespace names, IAM role ARNs, and S3 bucket
# names are operator-supplied at invocation time. The wrapper carries
# no operator-private identifiers so it can run on any cluster
# off-the-shelf.
#
# Modes:
#   default        -- generate pin-list, run pre-warm + measurement
#                     replay against both backends, scrape metrics,
#                     write summary.txt + four schema-valid records.
#   --dry-run      -- validate args and print the planned commands;
#                     exits 0 without touching the cluster. Used by
#                     test_prod_replay.sh.
#   --skip-scrape  -- skip the sidecar /metrics scrape (useful when
#                     the operator already has a Grafana panel open).

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PY="$HERE/prod_replay.py"

WINDOW_DAYS=7
OUTPUT_DIR=""
SHELF_ENDPOINT=""
RAW_ENDPOINT=""
TRINO_HOST=""
CATALOG_SHELF=""
CATALOG_RAW=""
TOP_N=200
PREWARM_SECS=1800
MEASUREMENT_SECS=7200
NAMESPACE="alluxio"
SERVICE="shelf-bench-pool"
POD_PREFIX="shelf-bench"
POD_COUNT=4
REPLICA="rep-2"
RELEASE_TAG="rc8-v1"
PINLIST_OVERRIDE=""
LOG_LEVEL="INFO"
DRY_RUN=0
SKIP_SCRAPE=0

usage() {
  sed -n '2,38p' "$0" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --window-days)        WINDOW_DAYS="$2"; shift 2;;
    --output-dir)         OUTPUT_DIR="$2"; shift 2;;
    --shelf-endpoint)     SHELF_ENDPOINT="$2"; shift 2;;
    --raw-endpoint)       RAW_ENDPOINT="$2"; shift 2;;
    --trino-host)         TRINO_HOST="$2"; shift 2;;
    --catalog-shelf)      CATALOG_SHELF="$2"; shift 2;;
    --catalog-raw)        CATALOG_RAW="$2"; shift 2;;
    --top-n)              TOP_N="$2"; shift 2;;
    --prewarm-secs)       PREWARM_SECS="$2"; shift 2;;
    --measurement-secs)   MEASUREMENT_SECS="$2"; shift 2;;
    --namespace)          NAMESPACE="$2"; shift 2;;
    --service)            SERVICE="$2"; shift 2;;
    --pod-prefix)         POD_PREFIX="$2"; shift 2;;
    --pod-count)          POD_COUNT="$2"; shift 2;;
    --replica)            REPLICA="$2"; shift 2;;
    --release-tag)        RELEASE_TAG="$2"; shift 2;;
    --pinlist-override)   PINLIST_OVERRIDE="$2"; shift 2;;
    --log-level)          LOG_LEVEL="$2"; shift 2;;
    --dry-run)            DRY_RUN=1; shift;;
    --skip-scrape)        SKIP_SCRAPE=1; shift;;
    -h|--help)            usage; exit 0;;
    *) echo "unknown arg: $1" >&2; usage; exit 2;;
  esac
done

declare -a REQUIRED_VARS=(
  "OUTPUT_DIR=--output-dir"
  "SHELF_ENDPOINT=--shelf-endpoint"
  "RAW_ENDPOINT=--raw-endpoint"
  "TRINO_HOST=--trino-host"
  "CATALOG_SHELF=--catalog-shelf"
  "CATALOG_RAW=--catalog-raw"
)
for entry in "${REQUIRED_VARS[@]}"; do
  vname="${entry%%=*}"
  flag="${entry#*=}"
  vval=$(eval "printf '%s' \"\${${vname}-}\"")
  if [[ -z "$vval" ]]; then
    echo "ERROR: $flag (\$$vname) is required" >&2
    usage
    exit 2
  fi
done

if ! [[ "$WINDOW_DAYS" =~ ^[0-9]+$ ]] || (( WINDOW_DAYS < 1 || WINDOW_DAYS > 30 )); then
  echo "ERROR: --window-days must be 1..30" >&2; exit 2
fi
if ! [[ "$TOP_N" =~ ^[0-9]+$ ]] || (( TOP_N < 1 )); then
  echo "ERROR: --top-n must be a positive integer" >&2; exit 2
fi
if ! [[ "$POD_COUNT" =~ ^[0-9]+$ ]] || (( POD_COUNT < 1 )); then
  echo "ERROR: --pod-count must be a positive integer" >&2; exit 2
fi
if ! [[ "$PREWARM_SECS" =~ ^[0-9]+$ ]] || (( PREWARM_SECS < 0 )); then
  echo "ERROR: --prewarm-secs must be a non-negative integer" >&2; exit 2
fi
if ! [[ "$MEASUREMENT_SECS" =~ ^[0-9]+$ ]] || (( MEASUREMENT_SECS < 60 )); then
  echo "ERROR: --measurement-secs must be >= 60" >&2; exit 2
fi
case "$REPLICA" in
  rep-0|rep-1|rep-2|rep-3) : ;;
  *) echo "ERROR: --replica must be rep-{0..3}" >&2; exit 2;;
esac

if [[ ! -x "$PY" ]] && [[ ! -f "$PY" ]]; then
  echo "ERROR: orchestrator not found at $PY" >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR"

# Build the python invocation. Endpoints are passed verbatim; the
# orchestrator strips the scheme prefix before handing HOST:PORT to
# tools/replay_pinlist.py per its CLI contract.
cmd=(
  "${PYTHON3:-python3}"
  "$PY"
  --output-dir "$OUTPUT_DIR"
  --shelf-endpoint "$SHELF_ENDPOINT"
  --raw-endpoint "$RAW_ENDPOINT"
  --trino-host "$TRINO_HOST"
  --catalog-shelf "$CATALOG_SHELF"
  --catalog-raw "$CATALOG_RAW"
  --replica "$REPLICA"
  --window-days "$WINDOW_DAYS"
  --top-n "$TOP_N"
  --prewarm-secs "$PREWARM_SECS"
  --measurement-secs "$MEASUREMENT_SECS"
  --namespace "$NAMESPACE"
  --service "$SERVICE"
  --pod-prefix "$POD_PREFIX"
  --pod-count "$POD_COUNT"
  --release-tag "$RELEASE_TAG"
  --log-level "$LOG_LEVEL"
)
if [[ -n "$PINLIST_OVERRIDE" ]]; then
  cmd+=(--pinlist-override "$PINLIST_OVERRIDE")
fi
if [[ "$DRY_RUN" -eq 1 ]]; then
  cmd+=(--dry-run)
fi
if [[ "$SKIP_SCRAPE" -eq 1 ]]; then
  cmd+=(--skip-scrape)
fi

echo "[run_prod_replay] launching orchestrator:" >&2
printf '  %q ' "${cmd[@]}" >&2; echo >&2

exec "${cmd[@]}"
