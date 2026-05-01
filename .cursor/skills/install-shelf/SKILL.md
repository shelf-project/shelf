---

## name: install-shelf
description: |
  Install Shelf (the Iceberg-native read cache for Trino) into a user's
  Kubernetes cluster end-to-end. Detects the Trino deployment, installs
  the Helm chart with sensible defaults, flips Trino's `s3.endpoint` to
  point at Shelf, validates the cutover with a byte-identity smoke
  query, and rolls back on signal. Use when the user says "install
  shelf", "make my Trino faster with shelf", "set up shelf for me", or
  "deploy shelf-project on my cluster". Designed for users with **no
  prior Trino / Helm / K8s expertise** — the agent does the work, the
  user only confirms cluster-mutating steps.

# install-shelf — turnkey Shelf install for Trino-on-K8s

This skill walks a coding agent through a complete Shelf install
against a user's existing Trino-on-Kubernetes cluster. Every step has
a built-in safety check: nothing destructive runs without an
explicit OK from the user, and every step has a documented rollback.

## When to use this skill

Use proactively when the user says any of:

- "install shelf" / "install shelf-project" / "deploy shelf"
- "make trino faster" / "trino is slow" / "fix trino on iceberg" / "fix trino on s3"
- "set up shelf on my cluster" / "set up the shelf cache"
- "we're using alluxio, switch us to shelf"
- "shelf cutover" / "flip our s3 endpoint to shelf"

Also use proactively if the user is debugging Trino + Iceberg on S3
performance and the symptoms match Shelf's sweet spot (repeated
manifest / footer reads, KEDA-scaled cold-cache tax, Alluxio metadata
pool saturation).

## Audience

The user may know Trino but not Helm, may know Helm but not Iceberg,
may know K8s but never deployed a StatefulSet with NVMe, or may not
know any of these. Default to **explaining each step in plain English

- one analogy** before running the command. The skill below is the
agent's playbook; the user-facing language is the agent's job.

## Pre-flight (read-only — safe to run automatically)

```bash
# 1. Where is the user's cluster?
kubectl config current-context
kubectl cluster-info | head -3

# 2. Is there a Trino StatefulSet / Deployment already?
kubectl get pods --all-namespaces -l app.kubernetes.io/name=trino       2>/dev/null
kubectl get pods --all-namespaces -l app=trino                          2>/dev/null
kubectl get sts,deploy --all-namespaces | grep -iE 'trino|presto'

# 3. Do they have helm 3.16+?
helm version --short

# 4. Do they have a default StorageClass that supports SSDs / NVMe?
kubectl get storageclass

# 5. (Best-effort) does the cluster have nodes with local NVMe?
#    On EKS that's `i3.*`, `i4i.*`, `m6id.*`, `c6id.*`, or any node
#    with an `instance-store` volume mount. Generic gp3 EBS is fine
#    for getting started.
kubectl get nodes -o json | jq -r '.items[].metadata.labels["node.kubernetes.io/instance-type"]' | sort -u
```

If **no Trino** is found, branch to the laptop quickstart:

```bash
# Laptop / dev path — full Trino + MinIO + Shelf in one docker compose
git clone https://github.com/shelf-project/shelf.git
cd shelf/benchmarks/smoke
./run-smoke.sh
# Trino UI on http://127.0.0.1:8080, shelfd metrics on :9091
```

…and skip the rest of this skill (the smoke harness already wires
everything end-to-end).

## Decision: where does Shelf go?

Shelf runs as a `StatefulSet` of N pods (3 is the default, scale to
match Trino replica count). Each pod owns:

- 1 PVC for NVMe-backed cache (default 200 GiB gp3 if no instance
store; ideally `local-nvme` StorageClass on i3 / i4i / m6id nodes)
- 1 Service (`shelf-pool`) — Trino points its `s3.endpoint` here
- 1 ServiceAccount with IRSA / Workload-Identity for S3 access

**Default namespace**: `shelf` (separate from the Trino namespace, so
Trino's NetworkPolicy doesn't gate Shelf). Override via `--namespace`.

**S3 access**: Shelf reads from S3 on cache misses, so it needs **the
same S3 bucket access Trino currently has**. Re-use the IAM role /
WorkloadIdentity Trino is using if at all possible — copy the
`eks.amazonaws.com/role-arn` annotation from Trino's ServiceAccount
onto Shelf's. The `values-irsa.example.yaml` overlay shows the
pattern.

## Step 1 — Install the chart (cluster-mutating; ASK FIRST)

Show the user:

> *"I'm about to install Shelf as a 3-pod StatefulSet in the `shelf`
> namespace, with 200 GiB gp3 PVCs each. This will reserve ~80 GiB
> RAM and 600 GiB disk across your cluster. OK to proceed? (yes/no)"*

On `yes`:

```bash
helm install shelf oci://ghcr.io/shelf-project/charts/shelf \
  --version 1.0.0 \
  --namespace shelf --create-namespace \
  --set replicaCount=3 \
  --set persistence.size=200Gi \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=<ROLE_ARN_FROM_TRINO_SA> \
  --wait --timeout 5m
```

Verify:

```bash
kubectl -n shelf get sts,svc,pod
# Expected: 3/3 shelf-{0,1,2} pods Running, shelf-pool Service exists
kubectl -n shelf exec shelf-0 -- /shelfctl stats
# Expected: stats JSON with metadata + rowgroup pool info
```

Rollback if anything goes wrong:

```bash
helm uninstall shelf -n shelf
kubectl delete namespace shelf  # if PVCs need cleaning
```

## Step 2 — Smoke test the shim BEFORE flipping Trino (read-only)

Critical — proves the S3 shim works and IRSA is wired correctly,
without touching production traffic:

```bash
# Spawn an ephemeral curl pod in the shelf namespace
kubectl -n shelf run curl-smoke --rm -it --restart=Never \
  --image=curlimages/curl:8.10.1 -- \
  sh -c '
    BUCKET=<USER_ICEBERG_BUCKET>
    KEY=<KNOWN_ICEBERG_METADATA_FILE_KEY>
    # Should succeed — Shelf proxies the GET to S3 on miss, then caches
    curl -sS -o /tmp/via-shelf.bytes -w "%{http_code}\n" \
      "http://shelf-pool.shelf.svc.cluster.local:9092/$BUCKET/$KEY"
    # Should match aws s3api get-object output exactly
    sha256sum /tmp/via-shelf.bytes
  '
```

If the sha256 matches what `aws s3api get-object --bucket $BUCKET --key $KEY /dev/stdout | sha256sum` returns, the shim is healthy.

**Common failures and what they mean:**

- `403 Forbidden` from Shelf → IRSA mis-wired. Fix the
ServiceAccount annotation, restart pods.
- `Connection refused` → NetworkPolicy on the Trino namespace
blocking egress to `shelf` namespace. Add an explicit allow rule.
- `404 Not Found` from S3 — confirm the key actually exists in the
bucket; the shim doesn't synthesize 404s, it forwards them.

## Step 3 — Flip Trino's `s3.endpoint` (production-affecting; ASK FIRST)

Show the user the **exact catalog properties diff** before applying:

```diff
# Trino catalog, e.g. iceberg.properties
 connector.name=iceberg
 hive.metastore.uri=thrift://<your-hms-host>:9083
+# Shelf S3 shim (cache-through to S3 on miss)
+s3.endpoint=http://shelf-pool.shelf.svc.cluster.local:9092
+# Force every metadata read through Shelf (bypass JVM-local memory cache)
+iceberg.metadata-cache.enabled=false
```

Two delivery patterns depending on how the user manages Trino config:

### 3a — ConfigMap-managed Trino (most Helm installs)

```bash
# Find the catalog ConfigMap
kubectl -n <trino-ns> get configmap | grep -E 'catalog|iceberg'
# Edit it — add the two lines above to the iceberg catalog block
kubectl -n <trino-ns> edit configmap <catalog-configmap-name>
# Roll the coordinator + workers so they pick up the new config
kubectl -n <trino-ns> rollout restart sts/<trino-coord> deploy/<trino-worker>
```

### 3b — Direct properties file (legacy installs)

```bash
# On each Trino node:
echo "s3.endpoint=http://shelf-pool.shelf.svc.cluster.local:9092" \
  >> /etc/trino/catalog/iceberg.properties
echo "iceberg.metadata-cache.enabled=false" \
  >> /etc/trino/catalog/iceberg.properties
sudo systemctl restart trino
```

## Step 4 — Validate the cutover (read-only)

Run a representative query through Trino, then verify Shelf actually
served bytes:

```bash
# Query something against an Iceberg table; capture the byte count Trino reports
trino --execute "SELECT COUNT(*) FROM <iceberg_catalog>.<schema>.<table>" \
  --output-format CSV
# Pull Shelf metrics
kubectl -n shelf port-forward svc/shelf-pool 9091:9091 &
PFP=$!
curl -s http://127.0.0.1:9091/metrics \
  | grep -E '^shelf_(hits|misses|s3_dollars_saved)_total'
kill $PFP
```

You're looking for `shelf_misses_total` to climb on first run (cold
cache), then `shelf_hits_total` to climb on subsequent runs (warm
cache). If misses climb but hits stay at zero on warm runs, double-
check `iceberg.metadata-cache.enabled=false` is actually set —
otherwise Trino's JVM-local cache absorbs warm reads and Shelf never
sees them.

## Rollback (any time, takes ~60 seconds)

If anything is wrong after the flip — query failures, latency spike,
unexpected error class — revert the catalog change and roll Trino:

```bash
# Remove the s3.endpoint line from the catalog ConfigMap (or properties file)
kubectl -n <trino-ns> edit configmap <catalog-configmap-name>  # delete the s3.endpoint line
kubectl -n <trino-ns> rollout restart sts/<trino-coord> deploy/<trino-worker>
```

Trino reverts to direct-S3 within a few seconds of the rollout
completing. Shelf can stay running (it costs nothing if no traffic
hits it); uninstall later via `helm uninstall shelf -n shelf` if
you've decided not to keep it.

## Auto-rollback signals

The skill should recommend the user wire these into their alerting
before the cutover, so a regression rolls itself back:


| Trigger                                                | Action      |
| ------------------------------------------------------ | ----------- |
| `ICEBERG_INVALID_METADATA` rate > 0.5 % / 5 min        | Auto-revert |
| `ICEBERG_CANNOT_OPEN_SPLIT` rate > 0.5 % / 5 min       | Auto-revert |
| p99 query wall-time regression > 50 % sustained 10 min | Auto-revert |
| Hit ratio < 30 % sustained 60 min after warm-up        | Investigate |
| Any shelf pod RSS > 24 GiB sustained 5 min OR OOMKill  | Auto-revert |


## Post-install hygiene

- **Run the diff harness for 24 h** — same query through `cdp` (direct
S3 catalog) and `cdp_shelf` (Shelf-fronted catalog) to byte-diff.
- **Pin the top-N tables** for instant warmup on pod restart:
  ```bash
  kubectl -n shelf exec shelf-0 -- /shelfctl pin --from-trino-history --top-n 20
  ```
- **Wire metrics to Grafana**: import the dashboard at
`charts/shelf/grafana/dashboards/shelf-overview.json`. Use the
`shelf-overview` UID for cross-deployment links.

## Reference docs (for the agent to cite when explaining)

- BLUEPRINT.md — full architecture
- docs/quickstart/index.md — laptop-only path (use this when the user
has no cluster)
- docs/runbook.md — every alert + symptom and what to do
- runbooks/*.md — per-alert deep dives (`shelf-pod-restarting`,
`shelf-nvme-usage-high`, `shelf-hit-rate-too-low`, etc.)
- COMPARISON.md — when Shelf vs Alluxio vs Warp Speed vs native cache

## Mistakes to avoid

- **Don't flip `s3.endpoint` and disable Trino's `fs.cache` in the
same change** — first-time manifest fetches then have no warm
fallback and the default 180 s `iterative_optimizer_timeout`
trips. Flip `s3.endpoint` first; remove `fs.cache` later if at all.
- **Don't pre-warm during peak traffic** — pre-warm runs `alluxio fs load --submit` (or the Shelf equivalent) and competes with live
reads for the connection pool. Window pre-warms to 00:00–04:00
local OR to a paused replica.
- **Don't deploy Shelf into the same namespace as Trino unless you
edit the NetworkPolicy** — Trino's default NetworkPolicy may
block ingress from same-namespace pods that aren't its own.
- **Don't assume `local-nvme` StorageClass exists** — only
instance-store EC2 families have it. On generic gp3 EBS, use the
default StorageClass; performance is still much better than direct
S3.
- **Don't skip the smoke test in Step 2** — flipping Trino against an
unhealthy Shelf pool fails 100 % of queries with cryptic
`Connection refused` or `403 Forbidden` errors. The smoke catches
this in 30 seconds.

