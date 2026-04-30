#!/usr/bin/env bash
# bench container entrypoint inside the spark-iceberg image.
#
# `tabulario/spark-iceberg:3.5.5_1.8.1` ships PySpark 3.5.5 + the
# iceberg-spark + iceberg-aws-bundle jars on /opt/spark/jars/, so we
# don't need `--packages`. We DO need to:
#
#   1. install `requests` (the bench scrapes shelfd's /metrics +
#      /admin endpoints to compute hit/miss deltas and $-savings)
#   2. exec bench.py through the image's bundled python3 — which is
#      already on PATH and already linked against PySpark.

set -euo pipefail

echo "[bench] installing 'requests' for /metrics scraping..."
pip install --quiet --no-cache-dir 'requests>=2.31,<3'

exec python3 /work/bench.py
