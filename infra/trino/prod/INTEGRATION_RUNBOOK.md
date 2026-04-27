# Production rollout — wiring `cdp_shelf` into one Trino replica

A step-by-step, low-risk runbook for landing the Shelf cache as a new
Iceberg catalog on **one Trino replica only** (template — call it
*replica N*). Designed to be reversible in under 3 minutes.

> This document is an OSS template. It deliberately uses placeholders
> (`<HMS_HOST>`, `<S3_BUCKET>`, `<IRSA_ROLE_ARN>`, `<REPLICA_N>`) for
> operator-specific values. The same placeholders bind to env vars in
> the catalog template at `catalog/cdp_shelf-rep2.properties`. Resolved
> values live in your private deployment repo — never in this one.

> **All four replicas at once is NOT covered.** Each replica gets its
> own MR after the previous has been bedded in for ≥ 48 h with no
> incidents.

---

## 0. Pre-conditions

| Pre-condition | How to verify |
|---------------|---------------|
| `shelfd` deployed in your shelf namespace, healthy | `kubectl -n <shelf-ns> get sts shelf` shows N/N ready |
| Headless service for shelfd | `kubectl -n <shelf-ns> get svc shelf` shows ClusterIP `None` and ports 9090–9093 |
| Trino replica's worker can reach a shelfd pod | run `verify-rep2.sh` step 2 from a worker shell |
| Trino replica's coordinator pod has IRSA mounted | `kubectl exec ... -- env \| grep AWS_WEB_IDENTITY_TOKEN_FILE` returns a path |
| shelfd's `SHELF_ORIGIN_BUCKET` matches the bucket the catalog will read from | `kubectl describe sts shelf` env block |

If any pre-condition is unmet, fix it *before* starting the rollout.

---

## 1. What the rollout changes (and what it does not)

**Changes**
- Adds `cdp_shelf.properties` to the chosen replica's `catalogs:`
  values block — a new Iceberg catalog name; nothing existing is
  modified.
- The catalog file is sourced from the
  [`catalog/cdp_shelf-rep2.properties`](catalog/cdp_shelf-rep2.properties)
  template; values are bound from env vars on the coord/worker pods.

**Does not change**
- Existing catalogs (still route as today).
- Workers, coordinator JVM args, or the access-control ConfigMap.
- Any existing query path. Until a user *explicitly* writes
  `FROM cdp_shelf.<schema>.<table>`, no traffic touches shelfd.

This is purely additive. **No existing query gets faster or slower
the moment this lands.**

---

## 2. Pre-flight (run BEFORE the merge)

```bash
# A. Confirm the live state still matches section 0:
KUBE_CTX=<your-context> ./shelf/infra/trino/prod/verify-rep2.sh
#   Expect: steps 1 + 2 pass, step 3 FAILS ("not in CM" — that's correct
#   pre-merge). Step 4 is skipped because step 3 short-circuits.

# B. Confirm there is no existing cdp_shelf catalog on the target replica:
kubectl -n <trino-ns> get cm <replica>-catalog \
  -o jsonpath='{.data.cdp_shelf\.properties}'
#   Expect: empty.

# C. Confirm the target replica's coordinator pod is healthy:
kubectl -n <trino-ns> get deploy <replica>-coordinator
#   Expect: 1/1 ready.
```

If any pre-flight fails, **stop and re-audit**. Do not merge.

---

## 3. Land the change

### 3a. This repo (OSS)

This MR. Reviewer focus: catalog template body, runbook accuracy,
rollback story.

### 3b. Your private deployment repo

A second MR is required there. The exact path depends on how your
chart is laid out (Helm chart values, ArgoCD app values, kustomize
overlay, etc.). The catalog body and the env vars it expects are:

```yaml
# In your replica's catalogs block:
cdp_shelf.properties: |
  # body of catalog/cdp_shelf-rep2.properties from this repo,
  # verbatim. The ${ENV:...} placeholders are resolved at Trino startup.

# In the same replica's env block (or via Secret/ConfigMap):
env:
  - name: HIVE_METASTORE_URI
    valueFrom:
      secretKeyRef:        # or configMapKeyRef — your call
        name: <replica>-shelf-config
        key: hive_metastore_uri
  - name: SHELF_S3_ENDPOINT
    valueFrom:
      configMapKeyRef:
        name: <replica>-shelf-config
        key: shelf_s3_endpoint
  - name: AWS_REGION
    valueFrom:
      configMapKeyRef:
        name: <replica>-shelf-config
        key: aws_region
  - name: ICEBERG_PARTITION_FILTER_SCHEMAS
    valueFrom:
      configMapKeyRef:
        name: <replica>-shelf-config
        key: iceberg_partition_filter_schemas
```

`SHELF_S3_ENDPOINT` is the per-replica pinning point — for replica
ordinal N, set it to `http://shelf-N.shelf.<shelf-ns>.svc.cluster.local:9092`
(or wrap modulo the shelfd pod count). This is the only value that
varies per replica; the others are identical across all coordinator
pods.

After merge, your reconciler (ArgoCD / Flux / Argo Workflows / your
own automation) updates the catalog ConfigMap, which triggers a
rolling restart of the replica's coordinator and workers (each worker
reloads catalogs on a checksum change).

---

## 4. Verify (run AFTER the reconcile has synced)

```bash
KUBE_CTX=<your-context> ./shelf/infra/trino/prod/verify-rep2.sh
```

Expected output: four green checkmarks. The last one issues a
`SHOW SCHEMAS` against `cdp_shelf` via the replica's coordinator HTTP
endpoint and asserts the catalog is loaded.

Then run a tiny smoke query and watch shelfd:

```bash
# In one terminal — tail shelfd metrics
kubectl -n <shelf-ns> port-forward shelf-N 9090:9090 &
watch -n 2 'curl -s localhost:9090/metrics | grep -E "shelf_admissions_total|shelf_origin_request_bytes_total"'

# In another terminal — issue a query against the cached catalog
kubectl -n <trino-ns> exec deploy/<replica>-coordinator -- \
  /usr/lib/trino/bin/trino --catalog cdp_shelf \
  --execute "SELECT count(*) FROM <some_iceberg_table>"
```

What you should see, in order:
1. `shelf_origin_request_bytes_total` increases (cold) by the
   manifest + footer + first row-group byte counts.
2. `shelf_admissions_total{decision="admit"}` increments.
3. Re-run the same query: `shelf_origin_request_bytes_total` does
   **not** increase further; the bytes come from the local pool.

This is the moment Shelf earns its keep.

---

## 5. Rollback

A 60-second rollback is the contract. Two options, in order of
preference:

### 5a. Reconciler revert (preferred)

```bash
# In your private deployment repo:
git revert <chart-values-commit>
git push
# Your reconciler (ArgoCD / Flux) reverses it on the next sync.
# cdp_shelf disappears from the ConfigMap; coordinator + workers do
# a rolling restart and lose the catalog.
```

### 5b. Emergency-only kubectl path (only if your reconciler is down)

```bash
kubectl -n <trino-ns> patch cm <replica>-catalog \
  --type=json -p='[{"op": "remove", "path": "/data/cdp_shelf.properties"}]'
kubectl -n <trino-ns> rollout restart deploy/<replica>-coordinator
kubectl -n <trino-ns> rollout restart deploy/<replica>-worker
```

After 5b, also pause your reconciler's sync on this app or it will
immediately reapply the bad config. Re-enable sync only after the
chart-values revert in 5a is merged.

---

## 6. Blast radius

| If shelfd misbehaves | Effect on the rolled replica |
|----------------------|-----------------|
| shelf-N pod OOMs / crashes | Queries against `cdp_shelf` fail with S3 connect errors. Other catalogs **unaffected**. |
| shelf-N returns 5xx for a key | Trino retries (default S3 retry policy); on continued 5xx, query fails with S3 error. |
| shelfd serves wrong bytes | **Highest-risk failure.** Detected by [`benchmarks/correctness-diff`](../../../benchmarks/correctness-diff/) which runs the same query against the un-cached and cached catalogs and diffs row counts + checksums. Run it on the first day post-rollout. |
| Reconciler applies bad config | Coordinator pod stuck CrashLoopBackOff. Roll back via 5a. |
| All shelfd pods unavailable | Only `cdp_shelf` queries fail; other catalogs continue. |

The existing catalogs continuing to work through any shelfd outage is
the single most important property of this rollout. Verify it
explicitly during pre-flight: `cdp_shelf` is *additive*; the existing
critical path is untouched.

---

## 7. Going to other replicas later

Each is its own MR, on its own day, with the previous replica's
evidence attached. Suggested order:

1. The smallest / lowest-traffic replica first.
2. After 48 h clean: the next smallest.
3. After 48 h clean: the largest.

Order each rollout against shelfd pod headroom — the wrap-around
mapping (replica → `shelf-(N % shelfd_pod_count)`) means later
replicas may share a shelfd pod with an earlier-rolled one. Confirm
DRAM utilisation on the shared pod stays below your eviction
threshold before rolling the second tenant.
