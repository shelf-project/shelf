#!/usr/bin/env bash
# Polars + Shelf example orchestrator.
#
#   bash run.sh              # cold→warm benchmark
#   docker compose down -v   # clean up
set -euo pipefail

cd "$(dirname "$0")"

echo "[run] building images (first run is slow — Rust toolchain + distroless)..."
docker compose build shelfd
docker compose --profile tools build runner

echo "[run] starting MinIO + shelfd..."
docker compose up -d minio minio-setup shelfd

echo "[run] waiting for shelfd /healthz..."
ok=0
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:29090/healthz >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 1
done
if [ "$ok" != "1" ]; then
  echo "[run] shelfd did not become ready in 60s; recent logs:" >&2
  docker compose logs --no-color --tail=80 shelfd >&2 || true
  exit 1
fi

echo "[run] seeding Iceberg table demo.events..."
docker compose run --rm runner python init/seed.py

echo
echo "[run] running Polars cold→warm benchmark..."
docker compose run --rm runner python bench.py

echo
echo "[run] done. Tear down with:  docker compose down -v"
