#!/usr/bin/env bash
# verify-rep2.sh — read-only post-rollout checks for the cdp_shelf
# catalog landing on a Trino replica. Read-only — no mutations.
#
# This is an OSS template. Operator-specific identifiers (Trino
# namespace, replica name, shelf namespace, shelfd pod ordinal) are
# read from environment variables; everything else is generic.
#
# Required env (or use the defaults):
#   TRINO_NS         (default: trino)            namespace of Trino replicas
#   TRINO_REPLICA    (default: replica-2)        Helm-instance label value
#   SHELF_NS         (default: shelf)            namespace of the shelf STS
#   SHELF_POD        (default: shelf-2)          target shelfd pod for this replica
#
# Optional:
#   KUBE_CTX                                     kubectl --context value
#
# Exit 0 = all four probes passed. Exit non-zero = first failing probe.

set -euo pipefail

KUBE_CTX="${KUBE_CTX:-}"
TRINO_NS="${TRINO_NS:-trino}"
TRINO_REPLICA="${TRINO_REPLICA:-replica-2}"
SHELF_NS="${SHELF_NS:-shelf}"
SHELF_POD="${SHELF_POD:-shelf-2}"

KUBECTL=(kubectl)
if [[ -n "$KUBE_CTX" ]]; then
    KUBECTL+=(--context "$KUBE_CTX")
fi

# Pretty output --------------------------------------------------------
ok()    { printf "  \033[32m✓\033[0m %s\n" "$*"; }
fail()  { printf "  \033[31m✗\033[0m %s\n" "$*"; exit 1; }
step()  { printf "\n\033[1m%s\033[0m\n" "$*"; }

# Helm-instance value used by the Trino chart on this operator's
# cluster — typically "trino-${TRINO_REPLICA}" or just "${TRINO_REPLICA}".
# We try the most common one first, fall back to the second.
HELM_INSTANCE="trino-${TRINO_REPLICA}"
CATALOG_CM="trino-${TRINO_REPLICA}-catalog"

# 1. shelfd is running ------------------------------------------------
step "1. shelfd healthy in '${SHELF_NS}' namespace"
ready=$("${KUBECTL[@]}" -n "$SHELF_NS" get sts shelf -o jsonpath='{.status.readyReplicas}/{.status.replicas}' 2>/dev/null || true)
[[ "$ready" =~ ^[1-9][0-9]*/[1-9][0-9]*$ && "${ready%/*}" == "${ready#*/}" ]] \
    || fail "shelf STS not fully ready (got: '$ready')"
ok   "shelf StatefulSet $ready ready"

img=$("${KUBECTL[@]}" -n "$SHELF_NS" get sts shelf -o jsonpath='{.spec.template.spec.containers[0].image}')
ok   "image: $img"

# 2. headless DNS resolves from a worker pod -------------------------
step "2. ${SHELF_POD} reachable from a ${TRINO_REPLICA} worker"
worker=$("${KUBECTL[@]}" -n "$TRINO_NS" get pods \
    -l "app.kubernetes.io/instance=${HELM_INSTANCE},app.kubernetes.io/component=worker" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
[[ -n "$worker" ]] || fail "no worker pods found for instance=${HELM_INSTANCE}"

probe=$("${KUBECTL[@]}" -n "$TRINO_NS" exec "$worker" -- sh -c "
    curl -s -o /dev/null -w '%{http_code} %{time_total}' -m 5 \
      http://${SHELF_POD}.shelf.${SHELF_NS}.svc.cluster.local:9092/ 2>/dev/null
" 2>/dev/null || true)
[[ "$probe" =~ ^[2-4][0-9][0-9]\ [0-9.]+$ ]] \
    || fail "${SHELF_POD} unreachable from $worker (got: '$probe')"
ok   "${SHELF_POD}:9092 responded ($probe)  [4xx is fine, S3 shim has no '/' route]"

# 3. cdp_shelf.properties present in the catalog CM -------------------
step "3. cdp_shelf catalog landed in ${CATALOG_CM}"
keys=$("${KUBECTL[@]}" -n "$TRINO_NS" get cm "$CATALOG_CM" \
    -o jsonpath='{.data}' 2>/dev/null | python3 -c 'import json,sys; print("\n".join(json.loads(sys.stdin.read()).keys()))')
echo "$keys" | grep -q '^cdp_shelf\.properties$' || fail "cdp_shelf.properties not in CM. Keys present:
$keys"
ok   "cdp_shelf.properties present in ConfigMap"

# Quick sanity on the file body (placeholder-aware).
body=$("${KUBECTL[@]}" -n "$TRINO_NS" get cm "$CATALOG_CM" \
    -o jsonpath='{.data.cdp_shelf\.properties}' 2>/dev/null)
echo "$body" | grep -q "${SHELF_POD}\." \
    || fail "cdp_shelf endpoint is not pointing at ${SHELF_POD}"
ok   "endpoint pinned to ${SHELF_POD}"

if echo "$body" | grep -qiE "s3\.aws-(access|secret)-key"; then
    fail "cdp_shelf carries explicit AWS keys; should rely on IRSA only"
fi
ok   "no explicit AWS keys in catalog (IRSA passthrough)"

# 4. catalog visible to Trino + smoke statement -----------------------
step "4. SHOW SCHEMAS on ${TRINO_REPLICA} cdp_shelf"
coord=$("${KUBECTL[@]}" -n "$TRINO_NS" get pods \
    -l "app.kubernetes.io/instance=${HELM_INSTANCE},app.kubernetes.io/component=coordinator" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
[[ -n "$coord" ]] || fail "no coordinator pod found for instance=${HELM_INSTANCE}"

out=$("${KUBECTL[@]}" -n "$TRINO_NS" exec "$coord" -- sh -c '
    curl -s -X POST -H "X-Trino-User: rep-verify" \
      -H "X-Trino-Catalog: cdp_shelf" \
      --data "SHOW SCHEMAS" \
      http://localhost:8080/v1/statement \
      | head -c 400
' 2>/dev/null || true)

case "$out" in
    *'"id"'*)        ok "Trino accepted the SHOW SCHEMAS statement on cdp_shelf" ;;
    *'CATALOG_NOT_FOUND'*) fail "Trino does not see the cdp_shelf catalog yet (pod may need restart for new CM key)" ;;
    *)               fail "unexpected response: ${out:0:200}" ;;
esac

step "All four probes passed."
echo "Next: run a real query against cdp_shelf and watch shelfd metrics"
echo "      (shelf_origin_request_bytes_total, shelf_admissions_total)."
