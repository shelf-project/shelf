#!/usr/bin/env bash
# benchmarks/scripts/test_prod_replay.sh
#
# Smoke test for the production-trace replay harness.
#
# Runs end-to-end with --dry-run + --pinlist-override against a
# synthetic pin-list. No live cluster, no Trino, no kubectl needed.
# Asserts:
#
#   * scripts pass `bash -n` and `python3 -m py_compile`
#   * synthetic pin-list parses as a valid JSON array of objects with
#     bucket/key/access_count/table fields (the schema replay_pinlist.py
#     consumes)
#   * --dry-run mode exits 0 without invoking subprocesses that need a
#     cluster, while still producing the planned-summary stub
#   * required CLI args are validated (missing arg -> exit 2)
#
# Exit codes: 0 = all assertions passed, 1 = at least one failure.

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
WRAPPER="$HERE/run_prod_replay.sh"
PY="$HERE/prod_replay.py"
SCRAPE="$HERE/scrape_shelf_metrics.sh"

PASS=0
FAIL=0

step() { printf '  ▸ %s\n' "$*"; }
ok()   { printf '    OK   %s\n' "$*"; PASS=$((PASS + 1)); }
bad()  { printf '    FAIL %s\n' "$*"; FAIL=$((FAIL + 1)); }

echo "[smoke] V1 prod-replay harness — dry-run smoke test"
echo "[smoke] HERE=$HERE"

# ---------------------------------------------------------------------------
step "syntax: bash -n on shell scripts"
for f in "$WRAPPER" "$SCRAPE" "$0"; do
  if bash -n "$f"; then
    ok "$(basename "$f")"
  else
    bad "$(basename "$f") failed bash -n"
  fi
done

step "syntax: python3 -m py_compile on prod_replay.py"
if "${PYTHON3:-python3}" -m py_compile "$PY"; then
  ok "prod_replay.py"
else
  bad "prod_replay.py failed py_compile"
fi

# ---------------------------------------------------------------------------
TMPDIR_SMOKE="$(mktemp -d -t rc8-v1-smoke-XXXXXX)"
trap 'rm -rf "$TMPDIR_SMOKE"' EXIT

PINLIST="$TMPDIR_SMOKE/pinlist.json"
cat >"$PINLIST" <<'EOF'
[
  {
    "bucket": "your-data-bucket",
    "key": "warehouse/your_schema/your_table/metadata/00000-abc.metadata.json",
    "size_estimate": 8192,
    "access_count": 42,
    "table": "your_catalog.your_schema.your_table"
  },
  {
    "bucket": "your-data-bucket",
    "key": "warehouse/your_schema/your_table/metadata/snap-12345-1.avro",
    "size_estimate": 65536,
    "access_count": 42,
    "table": "your_catalog.your_schema.your_table"
  }
]
EOF

step "synthetic pin-list parses as a JSON array of records"
if "${PYTHON3:-python3}" - <<PY
import json, sys
with open("$PINLIST") as f:
    data = json.load(f)
assert isinstance(data, list), "top-level must be array"
assert len(data) > 0, "must have entries"
for e in data:
    for k in ("bucket", "key", "access_count", "table"):
        assert k in e, f"missing field {k}"
sys.exit(0)
PY
then
  ok "pin-list schema"
else
  bad "pin-list schema"
fi

# ---------------------------------------------------------------------------
step "--dry-run end-to-end via run_prod_replay.sh wrapper"
OUT="$TMPDIR_SMOKE/out"
if "$WRAPPER" \
      --window-days 7 \
      --output-dir "$OUT" \
      --shelf-endpoint http://shelf-bench-pool.example-ns.svc:9092 \
      --raw-endpoint   https://s3.example-region.amazonaws.com \
      --trino-host     trino-bench-coord.example-ns.svc:8080 \
      --catalog-shelf  bench_iceberg_shelf \
      --catalog-raw    bench_iceberg \
      --replica        rep-2 \
      --top-n          5 \
      --prewarm-secs   60 \
      --measurement-secs 120 \
      --pinlist-override "$PINLIST" \
      --skip-scrape \
      --dry-run \
      >"$TMPDIR_SMOKE/dryrun.log" 2>&1; then
  ok "wrapper --dry-run exited 0"
else
  bad "wrapper --dry-run failed (exit $?); see $TMPDIR_SMOKE/dryrun.log"
fi

step "--dry-run wrote summary.txt stub"
if [[ -f "$OUT/summary.txt" ]] && grep -q "DRY-RUN" "$OUT/summary.txt"; then
  ok "summary.txt stub present"
else
  bad "summary.txt stub missing or wrong content"
fi

step "--dry-run did NOT touch the cluster (no kubectl / no trino logs)"
if grep -qE "kubectl|TRINO_QUERY|HTTP/" "$TMPDIR_SMOKE/dryrun.log"; then
  bad "dry-run log mentions kubectl/Trino/HTTP — should not"
else
  ok "no cluster-touching commands logged"
fi

# ---------------------------------------------------------------------------
step "wrapper rejects missing required args (no --output-dir)"
if "$WRAPPER" \
      --shelf-endpoint http://example-shelf:9092 \
      --raw-endpoint   https://example.example.com \
      --trino-host     trino:8080 \
      --catalog-shelf  a \
      --catalog-raw    b \
      >"$TMPDIR_SMOKE/missing.log" 2>&1; then
  bad "wrapper accepted invocation missing --output-dir"
else
  rc=$?
  if [[ "$rc" -eq 2 ]]; then
    ok "wrapper exit code 2 on missing arg"
  else
    bad "wrong exit code $rc on missing arg (expected 2)"
  fi
fi

step "wrapper rejects unknown --replica"
if "$WRAPPER" \
      --output-dir     "$TMPDIR_SMOKE/out2" \
      --shelf-endpoint http://example-shelf:9092 \
      --raw-endpoint   https://example.example.com \
      --trino-host     trino:8080 \
      --catalog-shelf  a \
      --catalog-raw    b \
      --replica        rep-9 \
      --pinlist-override "$PINLIST" \
      --dry-run \
      >"$TMPDIR_SMOKE/badreplica.log" 2>&1; then
  bad "wrapper accepted --replica rep-9"
else
  rc=$?
  if [[ "$rc" -eq 2 ]]; then
    ok "wrapper rejected --replica rep-9"
  else
    bad "wrong exit code $rc on bad replica (expected 2)"
  fi
fi

step "scrape_shelf_metrics.sh --dry-run does not require kubectl"
if "$SCRAPE" \
      --namespace example-ns \
      --service shelf-bench-pool \
      --pod-prefix shelf-bench \
      --pod-count 4 \
      --output-dir "$TMPDIR_SMOKE/scrape" \
      --phase pre \
      --dry-run \
      >"$TMPDIR_SMOKE/scrape.log" 2>&1; then
  ok "scrape helper --dry-run exit 0"
else
  bad "scrape helper --dry-run failed"
fi

# ---------------------------------------------------------------------------
echo
echo "[smoke] PASS=$PASS  FAIL=$FAIL"
if (( FAIL > 0 )); then
  echo "[smoke] dry-run log path: $TMPDIR_SMOKE/dryrun.log" >&2
  exit 1
fi
exit 0
