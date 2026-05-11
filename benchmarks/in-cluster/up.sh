#!/usr/bin/env bash
# benchmarks/in-cluster/up.sh
#
# Stand up the Shelf benchmark fixture in the trino-bench namespace.
# Idempotent: re-running on an existing fixture is a no-op `helm upgrade`.
#
# Usage:
#   export BENCH_BUCKET=s3://my-tpcds-bench
#   export BENCH_REGION=us-east-1
#   export HMS_THRIFT_URI=thrift://my-metastore.svc.cluster.local:9083
#   export SHELF_IRSA_ROLE_ARN=arn:aws:iam::123456789012:role/shelf-bench-s3
#   ./up.sh
#
# Optional knobs:
#   TRINO_WORKERS=4               # bench Trino worker count
#   SHELF_REPLICAS=3              # bench shelfd pod count
#   SHELF_CHART_VERSION=1.0.0     # OCI chart tag at ghcr.io/shelf-project/charts/shelf
#   TRINO_CHART_VERSION=0.36.0    # upstream trino/trino chart version
#   DRY_RUN=1                     # render manifests only, do not apply
#
# Side effects:
#   - kubectl apply manifests/namespace.yaml
#   - helm install/upgrade shelf-bench (OSS shelfd chart)
#   - helm install/upgrade trino-bench (upstream Trino chart)
#   - kubectl create secret password-auth on the bench Trino
#
# Tear down with ./down.sh.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
NS="trino-bench"

require_env() {
  local var="$1"
  if [[ -z "${!var:-}" ]]; then
    echo "ERROR: $var must be set" >&2
    exit 2
  fi
}

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: $1 not found on PATH" >&2
    exit 2
  fi
}

require_bin kubectl
require_bin helm
require_bin yq

require_env BENCH_BUCKET
require_env BENCH_REGION
require_env HMS_THRIFT_URI
require_env SHELF_IRSA_ROLE_ARN

TRINO_WORKERS="${TRINO_WORKERS:-4}"
SHELF_REPLICAS="${SHELF_REPLICAS:-3}"
SHELF_CHART_VERSION="${SHELF_CHART_VERSION:-1.0.0}"
TRINO_CHART_VERSION="${TRINO_CHART_VERSION:-0.36.0}"
DRY_RUN="${DRY_RUN:-0}"

KUBECTL="kubectl"
HELM="helm"
if [[ "$DRY_RUN" == "1" ]]; then
  KUBECTL="echo + kubectl"
  HELM="echo + helm"
fi

echo "[up] target namespace: $NS"
echo "[up] origin bucket:    $BENCH_BUCKET (region: $BENCH_REGION)"
echo "[up] hms uri:          $HMS_THRIFT_URI"
echo "[up] shelf replicas:   $SHELF_REPLICAS"
echo "[up] trino workers:    $TRINO_WORKERS"
echo "[up] shelf chart ver:  $SHELF_CHART_VERSION"
echo "[up] trino chart ver:  $TRINO_CHART_VERSION"

# 1. namespace
$KUBECTL apply -f "$HERE/manifests/namespace.yaml"

# 2. catalog ConfigMap — render properties files with substitutions
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

for f in cdp cdp_shelf; do
  envsubst < "$HERE/manifests/catalogs/${f}.properties" > "$TMPDIR/${f}.properties"
done

# 3. shelfd helm release (OCI chart from the v1.0.0 GA release)
$HELM upgrade --install shelf-bench oci://ghcr.io/shelf-project/charts/shelf \
  --version "$SHELF_CHART_VERSION" \
  -n "$NS" \
  -f "$HERE/manifests/shelf-bench-values.yaml" \
  --set replicaCount="$SHELF_REPLICAS" \
  --set "origin.bucket=${BENCH_BUCKET#s3://}" \
  --set "origin.region=${BENCH_REGION}" \
  --set "serviceAccount.annotations.eks\.amazonaws\.com/role-arn=${SHELF_IRSA_ROLE_ARN}" \
  --wait --timeout 5m

echo "[up] shelf-bench Ready. Verifying /healthz..."
if [[ "$DRY_RUN" != "1" ]]; then
  POD="$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=shelf -o jsonpath='{.items[0].metadata.name}')"
  kubectl -n "$NS" exec "$POD" -- wget -qO- http://localhost:9090/healthz || \
    echo "[up] WARN: /healthz probe failed; check shelf-bench logs"
fi

# 4. Trino chart from the upstream repo
$HELM repo add trino https://trinodb.github.io/charts >/dev/null 2>&1 || true
$HELM repo update >/dev/null

# Trino's catalog values are inlined as multi-line strings; pass them via --set-file.
$HELM upgrade --install trino-bench trino/trino \
  --version "$TRINO_CHART_VERSION" \
  -n "$NS" \
  -f "$HERE/manifests/trino-bench-values.yaml" \
  --set-file "catalogs.cdp=$TMPDIR/cdp.properties" \
  --set-file "catalogs.cdp_shelf=$TMPDIR/cdp_shelf.properties" \
  --set "server.workers=$TRINO_WORKERS" \
  --wait --timeout 10m

echo "[up] trino-bench Ready. Coordinator URL:"
echo "     kubectl -n $NS port-forward svc/trino-bench 18080:8080"

# 5. Sanity smoke — list shelf-bench pods + Trino /v1/info
if [[ "$DRY_RUN" != "1" ]]; then
  echo
  echo "[up] shelf-bench pods:"
  kubectl -n "$NS" get pods -l app.kubernetes.io/name=shelf -o wide
  echo
  echo "[up] trino-bench pods:"
  kubectl -n "$NS" get pods -l app.kubernetes.io/name=trino -o wide
fi

cat <<EOF

[up] FIXTURE READY.

Next steps:
  1. Generate the TPC-DS Iceberg fixture in $BENCH_BUCKET:
       benchmarks/tpcds/generator/generate_sf1000.sh   (or smoke.sh for SF1)
  2. Run the bench:
       benchmarks/tpcds/runner/run.py    --engine shelf    --sf 100 --out results/.../shelf.csv
       benchmarks/tpcds/runner/run.py    --engine raw-s3   --sf 100 --out results/.../raw-s3.csv
       benchmarks/cold-start/run.sh      --backend=shelf   --apply
       benchmarks/cold-start/run.sh      --backend=raw-s3  --apply
       benchmarks/replay/run.sh          --backend=shelf   --days=1 --speed=2x --apply
       benchmarks/replay/run.sh          --backend=raw-s3  --days=1 --speed=2x --apply
  3. Cost model:
       python3 benchmarks/tpcds/cost/model.py --run-dir results/$(date +%F)
  4. Tear down:
       benchmarks/in-cluster/down.sh

EOF
