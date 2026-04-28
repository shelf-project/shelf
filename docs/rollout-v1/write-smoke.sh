#!/usr/bin/env bash
#
# write-smoke.sh — pre-cutover S3 verb-coverage probe for shelfd.
#
# Run this **before** merging an iceberg.properties s3.endpoint swap
# from real S3 to a shelfd endpoint. Exits 0 only if every verb Trino
# + Iceberg actually issues works through the candidate shim.
#
# Why this exists:
#   On 2026-04-27 rep-1 was cut over to a read-only shim (preview-4);
#   every Iceberg INSERT 405'd because the shim only served GET/HEAD.
#   This script is the cheap pre-merge check that prevents a recurrence.
#
# Usage:
#   ./write-smoke.sh <shelfd-endpoint> <bucket>
#
# Example:
#   ./write-smoke.sh http://shelfd.shelf.svc.cluster.local:9092 \
#                    pw-data-cdp-prod-temp
#
# Requirements:
#   - awscli (any 2.x). Credentials are not used by the shim itself
#     (it forwards via shelfd's bound IAM role) but awscli signs
#     requests, so any non-empty values are fine for the test.
#   - bash 4+; mktemp; curl is NOT required (awscli does the talking).
#
# Verbs probed (mirrors what Trino's S3 filesystem + Iceberg's
# RemoveOrphanFiles emit during a typical write workload):
#
#   1. PUT  /<bucket>/<key>                    — single-shot upload
#   2. GET  /<bucket>/<key>                    — read-back
#   3. DELETE /<bucket>/<key>                  — single-key delete
#   4. POST /<bucket>/<key>?uploads            — InitiateMultipartUpload
#   5. PUT  /<bucket>/<key>?partNumber=&uploadId=  — UploadPart
#   6. POST /<bucket>/<key>?uploadId=          — CompleteMultipartUpload
#   7. GET  /<bucket>?list-type=2              — ListObjectsV2
#   8. POST /<bucket>?delete                   — bulk DeleteObjects

set -euo pipefail

ENDPOINT="${1:-}"
BUCKET="${2:-}"

if [[ -z "$ENDPOINT" || -z "$BUCKET" ]]; then
    cat >&2 <<'EOF'
usage: write-smoke.sh <shelfd-endpoint> <bucket>

  <shelfd-endpoint>  e.g. http://shelfd.shelf.svc.cluster.local:9092
  <bucket>           a bucket the shelfd's IAM role has full RW access to,
                     used only for an ephemeral _smoke/ key prefix.
EOF
    exit 64
fi

# Stamp a UUID-shaped suffix so concurrent runs don't collide.
RUN_ID="$(date +%s)-$$"
PREFIX="_smoke/${RUN_ID}"
KEY_SS="${PREFIX}/single-shot.bin"
KEY_MP="${PREFIX}/multipart.bin"
SCRATCH=$(mktemp -d)
trap 'rm -rf "$SCRATCH"' EXIT

# AWS-CLI defaults — anything non-empty is fine; the shim forwards
# under shelfd's bound IAM role.
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-shelfd-smoke}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-shelfd-smoke}"
export AWS_REGION="${AWS_REGION:-ap-south-1}"

aws_s3() {
    aws --endpoint-url "$ENDPOINT" --no-cli-pager "$@"
}

step() {
    printf '\033[1;36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"
}

ok() {
    printf '\033[1;32m  ✓\033[0m %s\n' "$*"
}

fail() {
    printf '\033[1;31m  ✗\033[0m %s\n' "$*" >&2
    exit 1
}

step "endpoint=${ENDPOINT}  bucket=${BUCKET}  prefix=${PREFIX}"

# 1. Single-shot PUT (small body, exercises the v1 path)
step "PUT  ${KEY_SS}  (single-shot, ~32 KiB)"
head -c 32768 /dev/urandom > "${SCRATCH}/ss.bin"
aws_s3 s3api put-object --bucket "$BUCKET" --key "$KEY_SS" \
    --body "${SCRATCH}/ss.bin" >/dev/null \
    || fail "single-shot PUT failed — shim missing PUT support?"
ok "PUT 200"

# 2. GET read-back
step "GET  ${KEY_SS}"
aws_s3 s3api get-object --bucket "$BUCKET" --key "$KEY_SS" \
    "${SCRATCH}/ss.out" >/dev/null \
    || fail "GET after PUT failed"
cmp -s "${SCRATCH}/ss.bin" "${SCRATCH}/ss.out" \
    || fail "GET body differs from PUT body — cache invalidation broken?"
ok "GET round-trip ok"

# 3. DELETE single key
step "DELETE  ${KEY_SS}"
aws_s3 s3api delete-object --bucket "$BUCKET" --key "$KEY_SS" >/dev/null \
    || fail "DELETE failed"
ok "DELETE 204"

# 4-6. Multipart upload (12 MiB body forces awscli to split into
# at least 2 parts at default thresholds; we explicitly set
# multipart_threshold=5MB to be deterministic).
step "MULTIPART  ${KEY_MP}  (~12 MiB, multi-part)"
head -c $((12 * 1024 * 1024)) /dev/urandom > "${SCRATCH}/mp.bin"
AWS_CLI_FORCE_MULTIPART_OPTS=(
    s3 cp
    --no-progress
    --cli-read-timeout 60
)
aws --endpoint-url "$ENDPOINT" \
    configure set default.s3.multipart_threshold 5MB
aws --endpoint-url "$ENDPOINT" \
    configure set default.s3.multipart_chunksize 5MB
aws_s3 "${AWS_CLI_FORCE_MULTIPART_OPTS[@]}" \
    "${SCRATCH}/mp.bin" "s3://${BUCKET}/${KEY_MP}" >/dev/null \
    || fail "multipart upload failed — initiate / upload-part / complete chain broken?"
ok "InitiateMultipartUpload + UploadPart + CompleteMultipartUpload all ok"

# 7. ListObjectsV2 — must return both seeded keys (the multipart
# completes synchronously, so it's visible immediately).
step "LIST  prefix=${PREFIX}"
LIST_OUT="${SCRATCH}/list.json"
aws_s3 s3api list-objects-v2 --bucket "$BUCKET" --prefix "${PREFIX}/" \
    > "$LIST_OUT" \
    || fail "ListObjectsV2 failed — shim missing list-type=2 support?"
KEY_COUNT=$(grep -c '"Key":' "$LIST_OUT" || true)
if [[ "$KEY_COUNT" -lt 1 ]]; then
    cat "$LIST_OUT" >&2
    fail "ListObjectsV2 returned 0 keys for prefix ${PREFIX}/ — expected ≥ 1"
fi
ok "ListObjectsV2 returned ${KEY_COUNT} keys"

# 8. Bulk DeleteObjects — clean up everything we created.
step "BULK DELETE  prefix=${PREFIX}"
KEYS_JSON=$(jq -c '{Objects: [.Contents[] | {Key: .Key}], Quiet: false}' "$LIST_OUT")
echo "$KEYS_JSON" > "${SCRATCH}/del.json"
aws_s3 s3api delete-objects --bucket "$BUCKET" \
    --delete "file://${SCRATCH}/del.json" >/dev/null \
    || fail "bulk DeleteObjects failed — shim missing ?delete support?"
ok "DeleteObjects 200"

# Final sanity: re-list. Must come back empty.
step "VERIFY  prefix=${PREFIX} is empty"
aws_s3 s3api list-objects-v2 --bucket "$BUCKET" --prefix "${PREFIX}/" \
    > "${SCRATCH}/list2.json"
REMAINING=$(grep -c '"Key":' "${SCRATCH}/list2.json" || true)
if [[ "$REMAINING" -ne 0 ]]; then
    cat "${SCRATCH}/list2.json" >&2
    fail "bulk DeleteObjects reported success but ${REMAINING} keys remain"
fi
ok "post-delete list is empty"

printf '\n\033[1;32mwrite-smoke OK — all 8 verbs pass through %s\033[0m\n' "$ENDPOINT"
