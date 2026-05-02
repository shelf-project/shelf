#!/usr/bin/env bash
# benchmarks/in-cluster/v2/run.sh
#
# One-command driver for the V2 12-hour production-trace replay bench.
# Renders the cluster manifests via envsubst, applies them, waits for
# shelf-bench pods Ready, kicks off the V1 prod-replay harness against
# the bench shim + raw-S3 endpoint, and tears down on every exit path.
#
# Hard requirements (operator MUST export before invocation; see RUN.md):
#   AWS_ACCOUNT_ID         e.g. 123456789012
#   IRSA_ROLE_ARN          full ARN matching SA shelf-bench-sa
#   PROD_HMS_URI           thrift://hive-metastore-host:9083
#   ORIGIN_BUCKET          bench S3 bucket (no s3:// prefix)
#   ORIGIN_REGION          AWS region of that bucket
#   TRINO_HOST             host:port of the bench Trino coord
#
# Optional overrides:
#   BENCH_NAMESPACE        default: trino-bench (matches V1 in-cluster bench)
#   SHELF_IMAGE_TAG        default: 1.0.1 (latest published GA)
#   SHELF_BENCH_NVME_GIB   default: 60   (K1 OSS default per PR #106)
#   SCRAPER_SA             default: shelf-bench-scraper
#   PIN_LIST               default: ./pin-list.json
#   MEASUREMENT_SECS       default: 43200   (12 h)
#   PREWARM_SECS           default: 1800    (30 min)
#   OUTPUT_DIR             default: ../results/$(date -u +%F)/prodreplay
#   DRY_RUN                set to 1 to envsubst + show plan without applying
#
# Exit:
#   0  bench completed cleanly + cleanup applied
#   1  any phase failed (cleanup still attempted)
#   2  preflight CLI/env failure (no cluster mutation)
#
# This script does NOT trigger the actual 12-hour bench by default — it
# only sets MEASUREMENT_SECS to 43200 and hands off to the V1 harness.
# Pass --plan-only to render+validate+exit (no apply, no harness).

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
MANIFESTS_SRC="$HERE/cluster-manifests"
RENDERED_DIR="${RENDERED_DIR:-/tmp/v2-rendered}"

PLAN_ONLY=0
SKIP_HARNESS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --plan-only)    PLAN_ONLY=1; shift;;
    --skip-harness) SKIP_HARNESS=1; shift;;
    -h|--help)
      sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
      exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

log() { printf '[v2] %s\n' "$*" >&2; }
err() { printf '[v2] ERROR: %s\n' "$*" >&2; }

require_env() {
  local var="$1"
  if [[ -z "${!var:-}" ]]; then
    err "$var must be exported (see RUN.md preflight section)"
    exit 2
  fi
}

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    err "$1 not found on PATH"
    exit 2
  fi
}

# Preflight ---------------------------------------------------------------

require_bin kubectl
require_bin envsubst
require_bin python3

require_env AWS_ACCOUNT_ID
require_env IRSA_ROLE_ARN
require_env PROD_HMS_URI
require_env ORIGIN_BUCKET
require_env ORIGIN_REGION
require_env TRINO_HOST

export BENCH_NAMESPACE="${BENCH_NAMESPACE:-trino-bench}"
export SHELF_IMAGE_TAG="${SHELF_IMAGE_TAG:-1.0.1}"
export SHELF_BENCH_NVME_GIB="${SHELF_BENCH_NVME_GIB:-60}"
export SHELF_BENCH_NVME_BYTES="$(( SHELF_BENCH_NVME_GIB * 1024 * 1024 * 1024 ))"
export SCRAPER_SA="${SCRAPER_SA:-shelf-bench-scraper}"

PIN_LIST="${PIN_LIST:-$HERE/pin-list.json}"
MEASUREMENT_SECS="${MEASUREMENT_SECS:-43200}"
PREWARM_SECS="${PREWARM_SECS:-1800}"
OUTPUT_DIR="${OUTPUT_DIR:-$REPO_ROOT/benchmarks/results/$(date -u +%F)/prodreplay}"

# Pin-list is only required when the harness will actually run; --plan-only
# and --skip-harness both skip the pin-list precondition.
if [[ "$PLAN_ONLY" -eq 0 && "$SKIP_HARNESS" -eq 0 ]]; then
  if [[ ! -f "$PIN_LIST" ]]; then
    err "pin-list not found at $PIN_LIST (set PIN_LIST=… or pre-stage it)"
    exit 2
  fi
fi

log "preflight OK"
log "  BENCH_NAMESPACE      = $BENCH_NAMESPACE"
log "  SHELF_IMAGE_TAG      = $SHELF_IMAGE_TAG"
log "  ORIGIN_BUCKET        = $ORIGIN_BUCKET"
log "  ORIGIN_REGION        = $ORIGIN_REGION"
log "  PROD_HMS_URI         = $PROD_HMS_URI"
log "  TRINO_HOST           = $TRINO_HOST"
log "  PIN_LIST             = $PIN_LIST"
log "  MEASUREMENT_SECS     = $MEASUREMENT_SECS"
log "  PREWARM_SECS         = $PREWARM_SECS"
log "  OUTPUT_DIR           = $OUTPUT_DIR"
log "  RENDERED_DIR         = $RENDERED_DIR"

# Render manifests --------------------------------------------------------

mkdir -p "$RENDERED_DIR"
for f in "$MANIFESTS_SRC"/*.yaml; do
  base="$(basename "$f")"
  envsubst < "$f" > "$RENDERED_DIR/$base"
done
log "rendered $(ls "$RENDERED_DIR" | wc -l | tr -d ' ') manifests under $RENDERED_DIR"

# Per-spec yq validation (yq if present, else python yaml)
if command -v yq >/dev/null 2>&1; then
  for f in "$RENDERED_DIR"/*.yaml; do
    yq . "$f" > /dev/null || { err "yq parse failed: $f"; exit 1; }
  done
  log "yq parse: 7/7 manifests OK"
else
  python3 - <<EOF || { err "python yaml parse failed"; exit 1; }
import glob, sys, yaml
for f in sorted(glob.glob("${RENDERED_DIR}/*.yaml")):
    try:
        list(yaml.safe_load_all(open(f)))
    except Exception as e:
        print(f"FAIL {f}: {e}", file=sys.stderr); sys.exit(1)
print(f"python yaml parse: OK on {len(glob.glob('${RENDERED_DIR}/*.yaml'))} files", file=sys.stderr)
EOF
fi

if [[ "$PLAN_ONLY" -eq 1 ]]; then
  log "--plan-only specified; rendered manifests are at $RENDERED_DIR. Exiting."
  exit 0
fi

# Apply + cleanup trap ----------------------------------------------------

cleanup() {
  local rc=$?
  log "cleanup: applying 99-cleanup.yaml (rc=$rc)"
  kubectl delete --ignore-not-found -f "$RENDERED_DIR/99-cleanup.yaml" \
    --grace-period=30 --wait=false 2>&1 | sed 's/^/[v2-cleanup] /' >&2 || true
  exit "$rc"
}
trap cleanup EXIT INT TERM

# Apply in order: namespace, IRSA SA, RBAC, catalogs, services, statefulset.
# 99-cleanup is the delete-only manifest — explicitly skipped here.
for stage in 00-namespace 01-irsa-sa 05-curl-pod-rbac 04-bench-trino-catalogs 03-shelf-bench-service 02-shelf-bench-statefulset; do
  log "kubectl apply $stage.yaml"
  kubectl apply -f "$RENDERED_DIR/${stage}.yaml"
done

log "waiting for shelf-bench StatefulSet pods Ready (10 min timeout)"
if ! kubectl -n "$BENCH_NAMESPACE" rollout status sts/shelf-bench --timeout=10m; then
  err "shelf-bench StatefulSet did not become Ready"
  kubectl -n "$BENCH_NAMESPACE" get pods -l app.kubernetes.io/instance=shelf-bench -o wide >&2 || true
  exit 1
fi
log "shelf-bench is Ready"

if [[ "$SKIP_HARNESS" -eq 1 ]]; then
  log "--skip-harness specified; cluster up. Run the harness manually then re-trigger cleanup."
  trap - EXIT INT TERM
  exit 0
fi

# Hand off to V1 harness --------------------------------------------------

mkdir -p "$OUTPUT_DIR"
log "kicking off V1 prod-replay harness — measurement_secs=$MEASUREMENT_SECS"

"$REPO_ROOT/benchmarks/scripts/run_prod_replay.sh" \
  --output-dir       "$OUTPUT_DIR" \
  --shelf-endpoint   "http://shelf-bench-pool.${BENCH_NAMESPACE}.svc.cluster.local:9092" \
  --raw-endpoint     "https://s3.${ORIGIN_REGION}.amazonaws.com" \
  --trino-host       "$TRINO_HOST" \
  --catalog-shelf    bench_iceberg_shelf \
  --catalog-raw      bench_iceberg \
  --replica          rep-2 \
  --window-days      7 \
  --top-n            200 \
  --prewarm-secs     "$PREWARM_SECS" \
  --measurement-secs "$MEASUREMENT_SECS" \
  --namespace        "$BENCH_NAMESPACE" \
  --service          shelf-bench-pool \
  --pod-prefix       shelf-bench \
  --pod-count        3 \
  --pinlist-override "$PIN_LIST"

log "V2 bench complete; results at $OUTPUT_DIR"
log "summary: $OUTPUT_DIR/summary.txt"
exit 0
