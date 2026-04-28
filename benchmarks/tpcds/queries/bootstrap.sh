#!/usr/bin/env bash
# F2 — fetch the 99 canonical TPC-DS queries from upstream trino.
# Pinned commit; change only with a reviewer sign-off per
# README.md.
#
# The version below is the tip of trinodb/trino @ 2026-04-24 on
# the `master` branch. Re-pinning requires a fresh diff against
# our `queries.yaml` timeout table — upstream occasionally rewrites
# a query (e.g. q14 was split into q14_1 + q14_2 at some point).
set -euo pipefail

PINNED_TAG="${PINNED_TAG:-master}"
BASE="https://raw.githubusercontent.com/trinodb/trino/${PINNED_TAG}/plugin/trino-tpcds/src/main/resources/io/trino/plugin/tpcds"

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

for i in $(seq 1 99); do
  name="q$(printf '%02d' "$i")"
  url="${BASE}/${name}.sql"
  out="${HERE}/${name}.sql"
  if [[ -f "$out" && -n "$(cat "$out")" ]]; then
    continue
  fi
  echo "fetch ${name} ..."
  curl -fsSL "$url" -o "$out" || {
    echo "warn: ${name} not found at ${url} — upstream may have renamed it; leave absent" >&2
    rm -f "$out"
  }
done

echo "queries under ${HERE}:"
ls -1 "${HERE}"/q*.sql 2>/dev/null | wc -l
