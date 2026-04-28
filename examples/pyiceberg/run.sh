#!/usr/bin/env bash
#
# Orchestrate the PyIceberg reference example end to end.
#
#   1. docker compose up -d            (minio, shelfd, iceberg-rest, runner)
#   2. wait for the seed job to finish (creates demo.events on MinIO)
#   3. run bench.py inside the runner  (cold + warm scan via shelfd:9092)
#   4. (default) docker compose down -v
#
# Pass `KEEP_UP=1 bash run.sh` to leave the stack running for ad-hoc poking
# (mc admin info, curl shelfd/metrics, etc.).

set -euo pipefail

cd "$(dirname "$0")"

KEEP_UP="${KEEP_UP:-0}"
COMPOSE="docker compose"

cleanup() {
  if [[ "$KEEP_UP" == "1" ]]; then
    echo "[run] KEEP_UP=1 set, leaving stack up. Tear down with: $COMPOSE down -v"
    return
  fi
  echo "[run] tearing down stack"
  $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[run] validating compose file"
$COMPOSE config -q

BUILD_FRESH="${BUILD_FRESH:-0}"
if [[ "$BUILD_FRESH" == "1" ]]; then
  echo "[run] BUILD_FRESH=1 -> forcing shelfd image rebuild"
  $COMPOSE build shelfd
fi

echo "[run] starting stack (will build shelfd from source on first run only)"
# `--wait` blocks until every service is healthy or run-once services exit 0.
# That's stronger than `compose wait seed`, which can race when the seed
# container is reaped between `up` returning and `wait` querying.
$COMPOSE up -d --wait

echo "[run] running bench.py inside the runner container"
$COMPOSE exec -T runner python /work/bench.py

echo "[run] done"
