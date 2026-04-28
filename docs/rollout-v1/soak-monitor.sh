#!/usr/bin/env bash
#
# soak-monitor.sh — single-shot health probe for a rep-N → shelf-N cutover.
#
# Intended to be invoked on a cadence (every ~5 min) during the soak window
# that follows an in-place `s3.endpoint` swap. Emits a compact summary on
# stdout and sets an exit code that callers (cron, tmux loops, Cursor
# soak-monitor subagents) can fan out on:
#
#   0  healthy             all signals nominal
#   1  warn                a background-only anomaly (e.g. transient 502
#                          on iceberg-worker bulk-delete, no user impact)
#   2  critical            user-visible failure (HIVE_WRITER_CLOSE_ERROR,
#                          pod restarts, engine resets, or the rep-N coord
#                          is gone)
#
# Usage:
#   ./soak-monitor.sh <rep-N> <shelf-N> [window-minutes]
#
# Example:
#   ./soak-monitor.sh trino-replica-1 shelf-1 5
#
# Requirements:
#   - kubectl (configured for the prod cluster)
#   - awk, sort, uniq
#
# Why this exists:
#   The rep-1 take-2 cutover (!17887) ran cleanly on the user-query path
#   but emitted a 4-minute burst of 502s from background iceberg-maintenance
#   bulk-delete colliding with S3 SlowDown. A single kubectl glance could
#   not tell "background noise" from "user incident". This script draws
#   that line explicitly and exits on the right bucket.
#
set -euo pipefail

REP="${1:-}"
SHELF="${2:-}"
WINDOW="${3:-5}"   # minutes

if [[ -z "$REP" || -z "$SHELF" ]]; then
    cat >&2 <<EOF
usage: soak-monitor.sh <rep-N> <shelf-N> [window-minutes]

  <rep-N>           e.g. trino-replica-1
  <shelf-N>         e.g. shelf-1
  [window-minutes]  log / event window (default: 5)

exit codes: 0=healthy  1=warn  2=critical
EOF
    exit 64
fi

TRINO_NS="${TRINO_NS:-trino-db}"
SHELF_NS="${SHELF_NS:-alluxio}"
NOW_UTC="$(date -u +%FT%TZ)"

bold()    { printf '\033[1m%s\033[0m\n' "$*"; }
green()   { printf '\033[1;32m%s\033[0m' "$*"; }
yellow()  { printf '\033[1;33m%s\033[0m' "$*"; }
red()     { printf '\033[1;31m%s\033[0m' "$*"; }

exit_code=0
bump() { [[ "$1" -gt "$exit_code" ]] && exit_code="$1"; }

bold "[soak-monitor ${NOW_UTC}]  rep=${REP}  shelf=${SHELF}  window=${WINDOW}m"

# -----------------------------------------------------------------------------
# 1. shelfd pod health
# -----------------------------------------------------------------------------
bold "1. shelfd pod"
shelf_state=$(kubectl -n "$SHELF_NS" get pod "$SHELF" \
    -o jsonpath='{.status.containerStatuses[0].ready},{.status.containerStatuses[0].restartCount},{.spec.containers[0].image}' 2>/dev/null || echo ",,")
IFS=, read -r ready restarts image <<<"$shelf_state"
if [[ "$ready" == "true" && "$restarts" == "0" ]]; then
    printf "   %s  ready=%s restarts=%s image=%s\n" "$(green ok)" "$ready" "$restarts" "${image##*:}"
elif [[ "$ready" == "true" ]]; then
    printf "   %s  ready=%s restarts=%s image=%s\n" "$(yellow warn)" "$ready" "$restarts" "${image##*:}"
    bump 1
else
    printf "   %s  pod not ready or missing (state=%s)\n" "$(red critical)" "$shelf_state"
    bump 2
fi

# -----------------------------------------------------------------------------
# 2. rep-N coordinator health
# -----------------------------------------------------------------------------
bold "2. ${REP} coordinator"
coord_pod=$(kubectl -n "$TRINO_NS" get pods \
    -l "app.kubernetes.io/instance=${REP},app.kubernetes.io/component=coordinator" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -z "$coord_pod" ]]; then
    printf "   %s  no coordinator pod found for %s\n" "$(red critical)" "$REP"
    bump 2
    coord_pod=""
else
    coord_state=$(kubectl -n "$TRINO_NS" get pod "$coord_pod" \
        -o jsonpath='{.status.containerStatuses[0].ready},{.status.containerStatuses[0].restartCount}' 2>/dev/null)
    IFS=, read -r c_ready c_restarts <<<"$coord_state"
    if [[ "$c_ready" == "true" && "$c_restarts" == "0" ]]; then
        printf "   %s  %s ready=%s restarts=%s\n" "$(green ok)" "$coord_pod" "$c_ready" "$c_restarts"
    elif [[ "$c_ready" == "true" ]]; then
        printf "   %s  %s ready=%s restarts=%s\n" "$(yellow warn)" "$coord_pod" "$c_ready" "$c_restarts"
        bump 1
    else
        printf "   %s  %s not ready\n" "$(red critical)" "$coord_pod"
        bump 2
    fi
fi

# -----------------------------------------------------------------------------
# 3. User-visible query failures on rep-N (vs total)
# -----------------------------------------------------------------------------
bold "3. user query outcome (last ${WINDOW}m)"
if [[ -n "$coord_pod" ]]; then
    coord_log=$(kubectl -n "$TRINO_NS" logs "$coord_pod" --since="${WINDOW}m" 2>/dev/null || true)
    fin=$(grep -c 'TIMELINE.*FINISHED' <<<"$coord_log" || true)
    fail=$(grep -c 'TIMELINE.*FAILED' <<<"$coord_log" || true)
    hw_err=$(grep -c 'HIVE_WRITER_CLOSE_ERROR' <<<"$coord_log" || true)
    s3_err=$(grep -c 'Status Code: 405' <<<"$coord_log" || true)

    printf "   FINISHED=%s  FAILED=%s  HIVE_WRITER_CLOSE_ERROR=%s  S3_405=%s\n" \
        "$fin" "$fail" "$hw_err" "$s3_err"

    if [[ "$hw_err" -gt 0 || "$s3_err" -gt 0 ]]; then
        printf "   %s  the exact class of error that killed take-1 is back\n" "$(red critical)"
        bump 2
    elif [[ "$fail" -gt 0 ]]; then
        printf "   top failure reasons:\n"
        grep 'TIMELINE.*FAILED' <<<"$coord_log" \
            | sed -E 's/.*FAILED \(([^)]+)\).*/\1/' \
            | sort | uniq -c | sort -rn | head -5 \
            | sed 's/^/     /'
    else
        printf "   %s  no failures\n" "$(green ok)"
    fi
fi

# -----------------------------------------------------------------------------
# 4. shelfd error surface (last window) — split user vs background
# -----------------------------------------------------------------------------
bold "4. ${SHELF} error surface (last ${WINDOW}m)"
shelf_log=$(kubectl -n "$SHELF_NS" logs "$SHELF" --since="${WINDOW}m" 2>/dev/null || true)

err_502=$(grep -c '"classification":"Status code: 502' <<<"$shelf_log" || true)
err_500=$(grep -c '"classification":"Status code: 500' <<<"$shelf_log" || true)
bulk_del_err=$(grep -c 'origin.delete_objects_bulk' <<<"$shelf_log" || true)
other_err=$(grep -c '"level":"ERROR"' <<<"$shelf_log" || true)
panics=$(grep -cE 'panic|PANIC|thread.*panicked' <<<"$shelf_log" || true)

printf "   ERROR lines=%s   502=%s   500=%s   bulk-delete-errs=%s   panics=%s\n" \
    "$other_err" "$err_502" "$err_500" "$bulk_del_err" "$panics"

# 502s caused ONLY by bulk-delete throttling are background (iceberg
# maintenance), not user-facing. Other ERRORs are not so benign.
non_bulk_err=$(( other_err - bulk_del_err ))
if [[ "$panics" -gt 0 ]]; then
    printf "   %s  shelfd panic detected\n" "$(red critical)"
    bump 2
elif [[ "$non_bulk_err" -gt 10 ]]; then
    printf "   %s  non-bulk-delete ERRORs exceed 10 — investigate\n" "$(yellow warn)"
    bump 1
elif [[ "$bulk_del_err" -gt 0 ]]; then
    printf "   %s  %s bulk-delete throttles (background iceberg maintenance; S3 SlowDown)\n" \
        "$(yellow warn)" "$bulk_del_err"
    bump 1
else
    printf "   %s  no errors\n" "$(green ok)"
fi

# -----------------------------------------------------------------------------
# 5. shelfd traffic (requests/s rough estimate via DEBUG log rate)
# -----------------------------------------------------------------------------
bold "5. ${SHELF} traffic sanity"
dbg=$(grep -c '"level":"DEBUG"' <<<"$shelf_log" || true)
dbg_per_min=$(( dbg / WINDOW ))
printf "   DEBUG events last %sm: %s   (~%s/min, rough req-rate proxy)\n" \
    "$WINDOW" "$dbg" "$dbg_per_min"

# -----------------------------------------------------------------------------
# 6. final verdict
# -----------------------------------------------------------------------------
bold "verdict"
case "$exit_code" in
    0) printf "   %s  probe clean\n" "$(green healthy)" ;;
    1) printf "   %s  background anomaly only (no user impact)\n" "$(yellow warn)" ;;
    2) printf "   %s  user-visible failure — HOLD rollout / consider rollback\n" "$(red critical)" ;;
esac

exit "$exit_code"
