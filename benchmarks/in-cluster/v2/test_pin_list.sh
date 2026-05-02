#!/usr/bin/env bash
# benchmarks/in-cluster/v2/test_pin_list.sh
#
# Validates that the pre-staged ./pin-list.json is consumable by
# tools/replay_pinlist.py — runs --dry-run end-to-end and asserts the
# parser exited cleanly. Smoke check; no cluster contact.
#
# Run before merging the V2-prep PR. Must exit 0 for the harness chain
# (replay_pinlist.py → prod_replay.py → run_prod_replay.sh → run.sh) to
# work end-to-end on the operator's machine.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
PIN_LIST="${1:-$HERE/pin-list.json}"

if [[ ! -f "$PIN_LIST" ]]; then
  echo "ERROR: pin-list not found at $PIN_LIST" >&2
  exit 2
fi

echo "[test_pin_list] pin-list = $PIN_LIST"
n_entries=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(len(d) if isinstance(d,list) else -1)' "$PIN_LIST")
if [[ "$n_entries" -lt 1 ]]; then
  echo "ERROR: pin-list is not a non-empty JSON array (got n=$n_entries)" >&2
  exit 1
fi
echo "[test_pin_list] entries = $n_entries"

echo "[test_pin_list] running replay_pinlist.py --dry-run …"
python3 "$REPO_ROOT/tools/replay_pinlist.py" \
  --pinlist "$PIN_LIST" \
  --shelf-endpoint "shelf-bench-pool.trino-bench.svc.cluster.local:9092" \
  --dry-run \
  --log-level INFO

echo "[test_pin_list] PASS"
