#!/usr/bin/env bash
# Spark-on-Shelf one-shot runner.
#
# Brings the stack up (MinIO + iceberg-rest + shelfd + a one-shot
# seeder), waits for shelfd's healthcheck, runs the bench container
# (PySpark inside `tabulario/spark-iceberg`), and prints a one-line
# summary parsed straight from bench.py's stdout:
#
#     SUMMARY: cold=… | warm=… | speedup=… | $-saved=…
#
# Override the shelfd image with a published tag instead of building
# from local source:
#
#     SHELFD_IMAGE=ghcr.io/shelf-project/shelfd:0.1.0-preview-9 \
#         bash run.sh
#
# Tear down with `docker compose down -v` from this directory.

set -euo pipefail

cd "$(dirname "$0")"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
COMPOSE=(docker compose -f "$COMPOSE_FILE")

echo "[run] bringing up MinIO + iceberg-rest + shelfd + seed..."
"${COMPOSE[@]}" up -d --build minio minio-setup iceberg-rest shelfd
"${COMPOSE[@]}" up --build --no-log-prefix --exit-code-from seed seed

echo "[run] waiting for shelfd /healthz..."
deadline=$(( $(date +%s) + 60 ))
until curl -fsS "http://127.0.0.1:9590/healthz" >/dev/null 2>&1; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "[run] ERROR: shelfd did not become healthy within 60s" >&2
        "${COMPOSE[@]}" logs shelfd | tail -50 >&2
        exit 1
    fi
    sleep 1
done
echo "[run] shelfd is healthy."

echo "[run] running bench (cold + warm)..."
BENCH_LOG="$(mktemp)"
trap 'rm -f "$BENCH_LOG"' EXIT
if ! "${COMPOSE[@]}" --profile bench run --rm --no-TTY bench 2>&1 | tee "$BENCH_LOG"; then
    echo "[run] ERROR: bench container exited non-zero" >&2
    exit 1
fi

echo
echo "============================================================"
SUMMARY="$(grep -E "^SUMMARY: " "$BENCH_LOG" | tail -n 1 || true)"
if [ -z "$SUMMARY" ]; then
    echo "[run] WARN: bench did not emit a SUMMARY line; see full log above" >&2
    exit 1
fi
echo "$SUMMARY" | sed 's/^SUMMARY: //'
echo "============================================================"
echo
echo "Stack is still up. Tear down with:"
echo "    docker compose -f $COMPOSE_FILE down -v"
