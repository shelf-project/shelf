#!/usr/bin/env bash
# Remove the transient `shelfbench` user + access-control rule from
# the dev cluster. Idempotent — safe to re-run.
set -euo pipefail

NS=${NS:-trino}

# 1. Strip shelfbench from password.db
CURRENT=$(kubectl -n "$NS" get secret example-trino-cluster-trino-password-file \
  -o jsonpath='{.data.password\.db}' | base64 -d)
NEW=$(printf '%s\n' "$CURRENT" | grep -v '^shelfbench:' || true)
NEW_B64=$(printf '%s' "$NEW" | base64 | tr -d '\n')
kubectl -n "$NS" patch secret example-trino-cluster-trino-password-file \
  --type='json' \
  -p="[{\"op\":\"replace\",\"path\":\"/data/password.db\",\"value\":\"${NEW_B64}\"}]"

# 2. Strip the four shelfbench rules from the ACL ConfigMap.
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT
kubectl -n "$NS" get cm example-trino-cluster-trino-access-control-volume-coordinator \
  -o json > "$TMPDIR/cm.json"
python3 <<PY
import json
with open("$TMPDIR/cm.json") as f:
    cm = json.load(f)
rules = json.loads(cm["data"]["rules.json"])
for key in ("schemas","tables","functions","procedures","queries"):
    rules[key] = [r for r in rules.get(key, []) if r.get("user") != "shelfbench"]
cm["data"]["rules.json"] = json.dumps(rules, indent=2)
with open("$TMPDIR/cm.new.json","w") as f:
    json.dump(cm, f)
PY
kubectl -n "$NS" replace -f "$TMPDIR/cm.new.json"
kubectl -n "$NS" rollout restart deploy/example-trino-cluster-trino-coordinator
kubectl -n "$NS" rollout status deploy/example-trino-cluster-trino-coordinator --timeout=180s
echo "shelfbench user and ACL rules removed."
