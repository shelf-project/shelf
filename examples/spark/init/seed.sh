#!/usr/bin/env bash
# Seed container entrypoint.
#
# Installs PyIceberg + PyArrow into a one-shot python:3.12-slim, then
# runs seed_iceberg.py to write a partitioned Iceberg `demo.events`
# table into MinIO via the REST catalog.
#
# Kept intentionally separate from the Spark image so the seed never
# pulls Spark's JVM. The bench container is the only one that needs
# Spark.

set -euo pipefail

echo "[seed] installing deps (pyiceberg + pyarrow)..."
pip install --quiet --no-cache-dir \
    'pyiceberg[pyarrow]>=0.9.0,<0.11'

exec python3 /seed/seed_iceberg.py
