#!/usr/bin/env bash
# Orchestrates the Daft + Shelf example end to end.
#
#   1. Brings up MinIO, the Iceberg REST catalog, and shelfd.
#   2. Builds the Python runner image (daft + pyiceberg).
#   3. Seeds default.orders into MinIO via PyIceberg.
#   4. Runs bench.py through Daft, twice, with all S3 traffic going
#      through shelfd's signature-agnostic shim on :9092.
#   5. Prints a small results table and leaves the stack running so
#      you can poke at /metrics and /stats.
#
# Tear down with: docker compose -f docker-compose.yml down -v
set -euo pipefail

cd "$(dirname "$0")"

COMPOSE="docker compose -f docker-compose.yml"

echo "==> validating compose file"
$COMPOSE config >/dev/null

echo "==> building images (shelfd + runner)"
$COMPOSE build shelfd runner

echo "==> bringing up minio + iceberg-rest + shelfd"
$COMPOSE up -d minio iceberg-rest shelfd

echo "==> waiting for shelfd /healthz"
for i in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:9090/healthz >/dev/null 2>&1; then
    echo "    shelfd ready"
    break
  fi
  sleep 1
  if [ "$i" = "60" ]; then
    echo "ERROR: shelfd never became healthy" >&2
    $COMPOSE logs shelfd | tail -50 >&2
    exit 1
  fi
done

echo "==> seeding default.orders (~50k rows)"
$COMPOSE run --rm seed

echo "==> running Daft bench (cold + warm)"
$COMPOSE run --rm runner

echo
echo "Stack is still running. Inspect with:"
echo "  curl http://127.0.0.1:9090/stats"
echo "  curl http://127.0.0.1:9090/metrics | grep shelf_"
echo "  curl http://127.0.0.1:9100   # MinIO API (host port 9100)"
echo "Tear down with:"
echo "  docker compose -f $(pwd)/docker-compose.yml down -v"
