#!/usr/bin/env bash
# Seed container entrypoint.
#
# Installs pyiceberg + writes the demo.events Iceberg table into MinIO via
# the local iceberg-rest catalog. Idempotent: re-running drops and recreates.
set -euo pipefail

echo "[seed] installing deps..."
pip install --quiet --no-cache-dir \
    'pyiceberg[pyarrow,s3fs]>=0.9.0,<0.11' \
    'boto3>=1.34,<2'

exec python3 /seed/seed_iceberg.py
