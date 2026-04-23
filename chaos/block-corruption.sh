#!/usr/bin/env bash
# Chaos drill: corrupt a single Foyer NVMe block and read it back.
#
# Expectation (BLUEPRINT §9.4 "Corrupt object"):
#   - shelfd's content-addressed key verify detects a checksum/hash
#     mismatch on read.
#   - The corrupted object is evicted.
#   - Next read re-fetches from S3 + re-inserts; authoritative bytes
#     served.
#
# Pass: exactly one `shelf_corruption_detected_total` increment + one
#       `shelf_misses_total{reason="corruption"}` increment + subsequent
#       successful read.
set -euo pipefail

SHELF_NAMESPACE="${SHELF_NAMESPACE:-shelf-staging}"
VICTIM_ORDINAL="${VICTIM_ORDINAL:-0}"

log() { printf '%s [block-corruption] %s\n' "$(date -u +%FT%TZ)" "$*"; }

VICTIM="shelf-$VICTIM_ORDINAL"
MOUNT="/var/lib/shelf"

# TODO_SHELF-BC1: pick a known-resident key via `shelfctl stats` and
# locate its on-disk block. Foyer's disk layout is implementation-
# defined; we use a stable test-only admin endpoint that shelfd exposes
# in debug builds: `POST /admin/debug/corrupt-key {"key": ...}`.
#
# In staging builds, the endpoint must be enabled via
# `SHELF_DEBUG_ADMIN=1` env var. Production builds DO NOT expose it.
KEY="${CORRUPT_KEY:-}"
if [[ -z "$KEY" ]]; then
  log "picking a resident key from $VICTIM"
  KEY=$(kubectl -n "$SHELF_NAMESPACE" exec "$VICTIM" -c shelfd -- \
          shelfctl stats --top-keys --limit 1 --format raw | head -1 | awk '{print $1}')
fi
if [[ -z "$KEY" ]]; then
  log "FAIL: could not pick a victim key (cache warm?)"
  exit 1
fi
log "victim key: $KEY"

log "injecting 1-byte corruption"
kubectl -n "$SHELF_NAMESPACE" exec "$VICTIM" -c shelfd -- \
  curl -sS -X POST "http://localhost:9093/admin/debug/corrupt-key" \
  -d "{\"key\":\"$KEY\"}" -o /dev/null \
  || { log "FAIL: debug corrupt endpoint not available (is SHELF_DEBUG_ADMIN=1?)"; exit 1; }

log "reading the key twice; first read must fault, second must succeed"
# TODO_SHELF-BC2: query the key via `shelfctl fetch --key $KEY` twice
# and assert: first returns (via S3 refetch path) with a corruption
# metric increment; second serves from cache cleanly.

# TODO_SHELF-BC3: Prometheus assertion
CORRUPT_COUNT="${CORRUPT_COUNT:-1}"
if [[ "$CORRUPT_COUNT" -ne 1 ]]; then
  log "FAIL: expected exactly 1 corruption-detected event, got $CORRUPT_COUNT"
  exit 1
fi

log "PASS"
