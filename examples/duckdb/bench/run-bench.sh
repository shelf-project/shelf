#!/usr/bin/env bash
# bench container entrypoint: install duckdb + requests, run bench.py.

set -euo pipefail

DUCKDB_VERSION="${DUCKDB_VERSION:-1.3.1}"

echo "[bench] installing duckdb==${DUCKDB_VERSION} + requests..."
pip install --quiet --no-cache-dir \
    "duckdb==${DUCKDB_VERSION}" \
    "requests>=2.31,<3"

exec python3 /work/bench.py
