#!/usr/bin/env bash
# Seed container entrypoint.
#
# Installs PyIceberg + PyArrow into a one-shot python:3.12-slim,
# then runs seed_iceberg.py to write a partitioned Iceberg `events`
# table into MinIO via the REST catalog.

set -euo pipefail

echo "[seed] installing deps (pyiceberg + pyarrow + boto3)..."
pip install --quiet --no-cache-dir \
    'pyiceberg[pyarrow,s3fs]>=0.9.0,<0.11' \
    'boto3>=1.34,<2'

exec python3 /seed/seed_iceberg.py
