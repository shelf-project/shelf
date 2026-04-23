#!/usr/bin/env bash
# cleanup.sh — tear down everything bootstrap.sh installed.
# Leaves env/ alone; run `make env-down` to delete the cluster itself.

set -euo pipefail

DRY_RUN="${SHELF_BENCH_DRY_RUN:-1}"

for arg in "$@"; do
  case "$arg" in
    --apply) DRY_RUN=0;;
    -h|--help)
      cat <<EOF
Usage: $0 [--apply]

Removes Helm releases and Kubernetes resources installed by bootstrap.sh.
Without --apply, prints the plan only.

Leaves env/ (EKS, VPC, results bucket) intact. Run 'make env-down' to
delete the cluster.
EOF
      exit 0;;
    *) echo "unknown arg: $arg" >&2; exit 2;;
  esac
done

log() { printf '[cleanup] %s\n' "$*"; }
run() {
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    log "DRY-RUN: $*"
  else
    log "RUN: $*"
    "$@"
  fi
}

# Uninstall Helm releases (ignore missing).
for release in \
  "shelf:shelf" \
  "alluxio:alluxio-2-9" \
  "alluxio3:alluxio-3-dora" \
  "trino:trino" \
  "minio:minio"
do
  name="${release%%:*}"
  ns="${release##*:}"
  run helm uninstall "$name" --namespace "$ns" --ignore-not-found
done

# Remove overlays and driver.
run kubectl delete -f configs/fs-cache/overlay.yaml --ignore-not-found
run kubectl delete -f configs/shelf/driver.yaml --ignore-not-found
run kubectl delete -f configs/shelf/tpcds-loader-job.yaml --ignore-not-found

# Namespaces, only if empty.
for ns in shelf alluxio-2-9 alluxio-3-dora trino minio bench; do
  run kubectl delete namespace "$ns" --ignore-not-found --wait=false
done

log "cleanup complete (dry_run=$DRY_RUN). Cluster is still up — run 'make env-down' to delete it."
