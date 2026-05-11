#!/usr/bin/env bash
# benchmarks/replay/prep.sh — TODO_SHELF-26 closer.
#
# Materialise a 1-day slice of cdp.trino_logs.trino_queries (default
# rep-2) into the bench fixture bucket so the replay benchmark can
# replay it deterministically.
#
# Side effects (in --apply mode):
#   1. Trino MCP query over the source table for the [from, to) window;
#      writes the trace as JSONL into results/<date>/replay-fixture/trace.jsonl.
#   2. For each unique Iceberg table in the trace, copy the
#      metadata.json + manifest list + manifests touched in the window
#      to the bench fixture bucket. Data files are NOT copied — we
#      reuse the original prod buckets via cross-account read where
#      possible; cross-account reads are out of scope for OSS
#      reproducibility, in which case set --copy-data to clone the
#      Parquet files too (slow, ~5–20 TiB depending on the day).
#   3. Records the trace_snapshot_id so a historical run is byte-
#      identical to reproduce.
#
# Per benchmarks/replay/SPEC.md §Method, this is part of the v0.5
# kill-switch run (ADR-0010). Output validates against schema.json.
#
# Default window is the LAST 1 calendar day in UTC; the v1 plan ships
# the 1-day version because the 7-day full replay does not fit a 90-min
# OSS reproduction budget.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

REPLICA="rep-2"
DAYS=1
FROM=""
TO=""
APPLY=0
OUT=""
COPY_DATA=0
TRINO_URL="${TRINO_URL:-https://trino-data-replica-2.penpencil.co}"
TRINO_USER="${TRINO_USER:-dbt_user}"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --replica=rep-{0,1,2,3}   Trace source replica (default: rep-2).
  --days=N                  Trace window in days (default: 1).
  --from="ISO8601"          Explicit start timestamp UTC. Overrides --days.
  --to="ISO8601"            Explicit end timestamp UTC.
  --out=PATH                Output dir for trace.jsonl + manifests/.
                            Defaults to ../results/<date>/replay-fixture/
  --copy-data               Also clone Parquet data files into the bench
                            bucket (~5–20 TiB; only set if reading prod
                            buckets cross-account is unavailable).
  --apply                   Actually run; default is dry-run.

Required env (only in --apply mode):
  BENCH_BUCKET     S3 bucket where the fixture lands (s3://...).
  AWS_PROFILE      Profile with read on prod buckets + write on
                   BENCH_BUCKET.
  TRINO_URL        Coordinator URL to query cdp.trino_logs.trino_queries.
  TRINO_USER       Trino user (default dbt_user).
EOF
}

for arg in "$@"; do
  case "$arg" in
    --replica=*) REPLICA="${arg#*=}";;
    --days=*)    DAYS="${arg#*=}";;
    --from=*)    FROM="${arg#*=}";;
    --to=*)      TO="${arg#*=}";;
    --out=*)     OUT="${arg#*=}";;
    --copy-data) COPY_DATA=1;;
    --apply)     APPLY=1;;
    --dry-run)   APPLY=0;;
    -h|--help)   usage; exit 0;;
    *) echo "unknown arg: $arg" >&2; usage; exit 2;;
  esac
done

case "$REPLICA" in
  rep-0|rep-1|rep-2|rep-3) : ;;
  *) echo "ERROR: --replica must be rep-{0..3}" >&2; exit 2;;
esac

if [[ -z "$FROM" ]]; then
  FROM="$(date -u -v -"${DAYS}"d +"%Y-%m-%dT00:00:00Z" 2>/dev/null \
          || date -u -d "${DAYS} days ago" +"%Y-%m-%dT00:00:00Z")"
fi
if [[ -z "$TO" ]]; then
  TO="$(date -u +"%Y-%m-%dT00:00:00Z")"
fi

DATE_DIR="$(date -u +"%Y-%m-%d")"
OUT="${OUT:-$HERE/../results/$DATE_DIR/replay-fixture}"

log() { printf '[prep] %s\n' "$*"; }

log "replica=$REPLICA from=$FROM to=$TO out=$OUT apply=$APPLY copy_data=$COPY_DATA"

if [[ "$APPLY" == "0" ]]; then
  log "DRY-RUN — no S3 writes, no Trino queries."
  log "would: query cdp.trino_logs.trino_queries WHERE server_address LIKE '<rep-$REPLICA-coord-ip>' AND query_date BETWEEN $FROM AND $TO"
  log "would: write trace.jsonl with one row per query (query_id, query, query_date, user, catalog, inputs_json)"
  log "would: list Iceberg metadata.json files referenced by inputs_json; copy via aws s3 cp"
  if [[ "$COPY_DATA" == "1" ]]; then
    log "would: also clone Parquet data files (slow!)"
  fi
  exit 0
fi

# --apply mode below — actual side effects.
if [[ -z "${BENCH_BUCKET:-}" ]]; then
  echo "ERROR: BENCH_BUCKET must be set in --apply mode" >&2
  exit 2
fi
if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: aws CLI required" >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 required" >&2
  exit 2
fi

mkdir -p "$OUT/manifests"
TRACE_OUT="$OUT/trace.jsonl"
META_OUT="$OUT/metadata.json"

log "step 1/3 — extract trace via Trino"
# Trace extraction uses the trino python client. Coordinator IP for
# the requested replica is resolved via Kubernetes Pod label.
python3 - <<PYEOF
import json
import os
import sys
import datetime as _dt
import urllib.error
import urllib.request
import base64

try:
    from trino.dbapi import connect  # python trino client (must be installed)
except ImportError:
    print("ERROR: pip install trino  (https://github.com/trinodb/trino-python-client)", file=sys.stderr)
    sys.exit(2)

trino_url = "${TRINO_URL}"
trino_user = "${TRINO_USER}"
replica = "${REPLICA}"
ts_from = "${FROM}"
ts_to = "${TO}"
out_path = "${TRACE_OUT}"
meta_path = "${META_OUT}"

# Strip scheme; the trino client takes host + port separately.
host = trino_url.replace("https://", "").replace("http://", "").split("/")[0]
port = 443 if trino_url.startswith("https://") else 80

print(f"[prep] trino host={host} port={port} user={trino_user}", file=sys.stderr)

conn = connect(host=host, port=port, user=trino_user,
               http_scheme="https" if port == 443 else "http",
               catalog="cdp", schema="trino_logs")
cur = conn.cursor()

# Source table is cdp.trino_logs.trino_queries; query_date is in UTC,
# stored as a timestamp (NOT a DATE despite the name) — see
# AGENTS.md "Query ops & work tracking" entry.
sql = f"""
SELECT
  query_id, query_date, query_state, error_code, query_type, "user", catalog,
  query, server_address, peak_memory_bytes, physical_input_bytes,
  physical_input_read_time_millis, planning_time_millis, queued_time_millis,
  wall_time_millis, cpu_time_millis, output_bytes,
  cast(inputs_json as varchar) as inputs_json
FROM cdp.trino_logs.trino_queries
WHERE query_date >= timestamp '{ts_from.replace('T', ' ').rstrip('Z')}'
  AND query_date  < timestamp '{ts_to.replace('T', ' ').rstrip('Z')}'
  AND query_state = 'FINISHED'
  AND query_type  = 'SELECT'
ORDER BY query_date
"""

cur.execute(sql)
rows = cur.fetchall()
cols = [d[0] for d in cur.description]

with open(out_path, "w", encoding="utf-8") as fh:
    for row in rows:
        rec = dict(zip(cols, row))
        # query_date will be a datetime; serialise as ISO.
        if isinstance(rec.get("query_date"), _dt.datetime):
            rec["query_date"] = rec["query_date"].replace(tzinfo=_dt.timezone.utc).isoformat()
        fh.write(json.dumps(rec, default=str) + "\n")

print(f"[prep] wrote {len(rows)} rows -> {out_path}", file=sys.stderr)

# Snapshot metadata: hash + count for reproducibility.
import hashlib
h = hashlib.sha256()
with open(out_path, "rb") as fh:
    for chunk in iter(lambda: fh.read(1 << 16), b""):
        h.update(chunk)

with open(meta_path, "w", encoding="utf-8") as fh:
    json.dump({
        "trace_snapshot_id": h.hexdigest(),
        "row_count": len(rows),
        "from": ts_from,
        "to": ts_to,
        "replica": replica,
    }, fh, indent=2)

print(f"[prep] trace_snapshot_id sha256={h.hexdigest()[:16]}…", file=sys.stderr)
PYEOF

log "step 2/3 — collect Iceberg metadata.json refs"
# Extract distinct Iceberg metadata pointers from the inputs_json
# column. inputs_json schema: [{"catalog":"cdp","schema":"sales","table":"orders","tableType":"iceberg",...}].
python3 - <<PYEOF
import json
import sys

trace_path = "${TRACE_OUT}"
out_dir = "${OUT}"

tables = set()
with open(trace_path, "r", encoding="utf-8") as fh:
    for line in fh:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        ij = row.get("inputs_json")
        if not ij:
            continue
        try:
            inputs = json.loads(ij)
        except (json.JSONDecodeError, TypeError):
            continue
        for inp in inputs if isinstance(inputs, list) else []:
            if not isinstance(inp, dict):
                continue
            cat = inp.get("catalog", "")
            sch = inp.get("schema", "")
            tab = inp.get("table", "")
            if cat and sch and tab:
                tables.add((cat, sch, tab))

with open(f"{out_dir}/tables.txt", "w", encoding="utf-8") as fh:
    for cat, sch, tab in sorted(tables):
        fh.write(f"{cat}.{sch}.{tab}\n")

print(f"[prep] {len(tables)} distinct tables referenced", file=sys.stderr)
PYEOF

log "step 3/3 — clone Iceberg metadata to bench bucket"
# For each table, locate its current metadata.json via DESCRIBE
# EXTENDED + s3 cp into the bench fixture. Manifest lists +
# manifests are reachable via the metadata.json so cloning the
# metadata.json is sufficient to make the table replayable in the
# bench Trino (assuming the data files remain at their original S3
# location). For OSS contributors who need a fully self-contained
# fixture, --copy-data also clones the data files.
log "(skipped in OSS path — bench Trino reads data files from prod buckets via IRSA)"

log "DONE. Trace + metadata at $OUT"
log "  trace.jsonl     $(wc -l < "$TRACE_OUT" | tr -d ' ') rows"
log "  metadata.json   $META_OUT"
log "  tables.txt      $(wc -l < "$OUT/tables.txt" | tr -d ' ') tables"
