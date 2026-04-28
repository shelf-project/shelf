#!/usr/bin/env bash
# bootstrap.sh — idempotent installer for the Shelf benchmark harness.
#
# Installs, in order, on the cluster provisioned by `env/`:
#   1. MinIO (or a pre-existing S3 bucket) as the Iceberg fixture store.
#   2. Iceberg TPC-DS tables @ 1 TB (scale configurable).
#   3. Trino via Helm (configs/shelf/trino-values.yaml).
#   4. Shelf via Helm (configs/shelf/shelf-values.yaml).
#   5. Alluxio OSS 2.9.5 baseline (configs/alluxio-2-9/).
#   6. Alluxio 3.x DORA baseline (configs/alluxio-3-dora/).
#   7. fs.cache baseline (configs/fs-cache/).
#   8. Benchmark driver pod + load generators.
#
# Idempotency rule: re-running must produce the same state without
# re-uploading fixtures. Every step checks a sentinel ConfigMap before
# doing real work.
#
# Scaffolding note: this script echoes intent only. No real cluster
# side effects in v0.0. Exit code 0 means "plan is coherent", not
# "cluster is ready".

set -euo pipefail

# -----------------------------------------------------------------------------
# Defaults (override via flags or env).
# -----------------------------------------------------------------------------
SCALE="${SHELF_BENCH_SCALE:-1tb}"
FIXTURE_BUCKET="${SHELF_BENCH_FIXTURE_BUCKET:-}"
BACKENDS="${SHELF_BENCH_BACKENDS:-raw-s3,fs-cache,alluxio-2-9,alluxio-3-dora,shelf}"
DRY_RUN="${SHELF_BENCH_DRY_RUN:-1}"
SHELF_IMAGE="${SHELF_BENCH_IMAGE:-ghcr.io/shelf-project/shelfd:scaffold}"
TRINO_IMAGE="${SHELF_BENCH_TRINO_IMAGE:-trinodb/trino:480}"

usage() {
  cat <<EOF
Usage: $0 [--scale=1tb|100gb|10gb] [--backends=a,b,c] [--fixture-bucket=NAME] [--apply]

Installs the bench harness components on the cluster provisioned by
env/. Idempotent; re-run after any partial failure.

Flags:
  --scale           TPC-DS scale factor. default: 1tb.
  --backends        Comma-separated list of backends to install helm charts for.
                    default: raw-s3,fs-cache,alluxio-2-9,alluxio-3-dora,shelf.
  --fixture-bucket  S3 bucket for the TPC-DS Iceberg fixture. If unset, a MinIO
                    cluster is installed in-cluster instead.
  --apply           Actually run the steps. Without this, prints what would run.
EOF
}

for arg in "$@"; do
  case "$arg" in
    --scale=*)           SCALE="${arg#*=}";;
    --backends=*)        BACKENDS="${arg#*=}";;
    --fixture-bucket=*)  FIXTURE_BUCKET="${arg#*=}";;
    --apply)             DRY_RUN=0;;
    -h|--help)           usage; exit 0;;
    *) echo "unknown arg: $arg" >&2; usage; exit 2;;
  esac
done

log() { printf '[bootstrap] %s\n' "$*"; }

run() {
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    log "DRY-RUN: $*"
  else
    log "RUN: $*"
    "$@"
  fi
}

# -----------------------------------------------------------------------------
# Pre-flight: required tools + kubecontext.
# -----------------------------------------------------------------------------
preflight() {
  log "preflight: verifying required tools on PATH"
  local missing=0
  for cmd in kubectl helm aws jq terraform; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      log "MISSING: $cmd"
      missing=1
    fi
  done
  if [[ "$missing" -ne 0 ]]; then
    log "install the missing tools and re-run; see docs/reproducing.md §Prerequisites"
    exit 3
  fi

  log "preflight: current kubecontext is $(kubectl config current-context 2>/dev/null || echo NONE)"
  log "preflight: scale=$SCALE backends=$BACKENDS fixture=${FIXTURE_BUCKET:-<in-cluster minio>}"
}

# -----------------------------------------------------------------------------
# Step 1 — MinIO (or use pre-existing S3 bucket).
# -----------------------------------------------------------------------------
step_fixture_store() {
  log "step 1: fixture store"
  if [[ -n "$FIXTURE_BUCKET" ]]; then
    log "using external bucket: s3://$FIXTURE_BUCKET"
    return 0
  fi
  # TODO_SHELF-26: install MinIO Helm chart into `minio` namespace
  #   - 3 replicas, 1 TiB PVC each
  #   - IRSA for bucket provisioning
  #   - sentinel ConfigMap `minio-bootstrap-done` used for idempotency
  run helm upgrade --install minio bitnami/minio \
    --namespace minio --create-namespace \
    --values configs/shelf/minio-values.yaml
}

# -----------------------------------------------------------------------------
# Step 2 — Iceberg TPC-DS tables.
# -----------------------------------------------------------------------------
step_tpcds_fixture() {
  log "step 2: TPC-DS Iceberg fixture @ $SCALE"
  # TODO_SHELF-26: run `tpcds-kit` generator as a Kubernetes Job that writes
  #   Parquet directly into the fixture store, then commits Iceberg
  #   metadata via a one-shot Trino container.
  #   - sentinel ConfigMap `tpcds-fixture-$SCALE-done`
  #   - fixture hash recorded under s3://$FIXTURE_BUCKET/tpcds/$SCALE/SHA256SUMS
  run kubectl apply -f configs/shelf/tpcds-loader-job.yaml
  run kubectl wait --for=condition=complete job/tpcds-loader-$SCALE --timeout=60m
}

# -----------------------------------------------------------------------------
# Step 3 — Trino.
# -----------------------------------------------------------------------------
step_trino() {
  log "step 3: Trino (image=$TRINO_IMAGE)"
  # TODO_SHELF-10/SHELF-15: plugin jars baked into a custom Trino image;
  # for scaffolding we assume the image already contains them.
  run helm upgrade --install trino trinodb/trino \
    --namespace trino --create-namespace \
    --set image.tag=480 \
    --values configs/shelf/trino-values.yaml
}

# -----------------------------------------------------------------------------
# Step 4 — Shelf.
# -----------------------------------------------------------------------------
step_shelf() {
  if [[ "${BACKENDS}" != *shelf* ]]; then
    log "step 4: shelf skipped (not in --backends)"
    return 0
  fi
  log "step 4: Shelf (image=$SHELF_IMAGE)"
  # TODO_SHELF-21: StatefulSet with 3 pods, NVMe PVCs, headless svc.
  run helm upgrade --install shelf ../charts/shelf \
    --namespace shelf --create-namespace \
    --set image.repository="${SHELF_IMAGE%:*}" \
    --set image.tag="${SHELF_IMAGE##*:}" \
    --values configs/shelf/shelf-values.yaml
}

# -----------------------------------------------------------------------------
# Step 5 — Alluxio OSS 2.9.5 baseline.
# -----------------------------------------------------------------------------
step_alluxio_2_9() {
  if [[ "${BACKENDS}" != *alluxio-2-9* ]]; then
    log "step 5: alluxio-2-9 skipped"
    return 0
  fi
  log "step 5: Alluxio OSS 2.9.5"
  # TODO_SHELF-26: Helm values cloned from our production rep-2 config
  #   with secrets stripped. See configs/alluxio-2-9/README.md.
  run helm upgrade --install alluxio alluxio-charts/alluxio \
    --namespace alluxio-2-9 --create-namespace \
    --version 0.6.x \
    --values configs/alluxio-2-9/values.yaml
}

# -----------------------------------------------------------------------------
# Step 6 — Alluxio 3.x DORA baseline.
# -----------------------------------------------------------------------------
step_alluxio_3_dora() {
  if [[ "${BACKENDS}" != *alluxio-3-dora* ]]; then
    log "step 6: alluxio-3-dora skipped"
    return 0
  fi
  log "step 6: Alluxio 3.x DORA"
  # TODO_SHELF-26: stock Helm chart, tuned per Alluxio public docs
  #   (not our internal values).
  run helm upgrade --install alluxio3 alluxio-charts/alluxio-enterprise \
    --namespace alluxio-3-dora --create-namespace \
    --values configs/alluxio-3-dora/values.yaml
}

# -----------------------------------------------------------------------------
# Step 7 — fs.cache baseline (Trino sidecar).
# -----------------------------------------------------------------------------
step_fs_cache() {
  if [[ "${BACKENDS}" != *fs-cache* ]]; then
    log "step 7: fs-cache skipped"
    return 0
  fi
  log "step 7: Trino fs.cache baseline"
  # fs.cache is a Trino config, not its own Helm release; applied via
  # a ConfigMap overlay onto the Trino deployment.
  run kubectl apply -f configs/fs-cache/overlay.yaml
}

# -----------------------------------------------------------------------------
# Step 8 — Driver pod + load generators.
# -----------------------------------------------------------------------------
step_driver() {
  log "step 8: benchmark driver"
  # TODO_SHELF-26: driver image with tpcds-kit, k6, shelfctl, replay CLI.
  run kubectl apply -f configs/shelf/driver.yaml
  run kubectl wait --for=condition=Ready pod/bench-driver -n bench --timeout=5m
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
main() {
  preflight
  step_fixture_store
  step_tpcds_fixture
  step_trino
  step_shelf
  step_alluxio_2_9
  step_alluxio_3_dora
  step_fs_cache
  step_driver
  log "bootstrap complete (dry_run=$DRY_RUN)."
  log "next: see tpcds/run.sh, cold-start/run.sh, spot-churn/run.sh, replay/run.sh"
}

main "$@"
