#!/usr/bin/env bash
# F1 — SF1 Iceberg smoke test. Runs the full generator at SF=1
# against whatever `TRINO_URL` resolves to. Intended to be run
# before every SF1000 generation to catch catalog/auth/DNS issues
# in seconds, not hours.
set -euo pipefail
SF=1 "$(dirname "$0")/generate_sf1000.sh"
