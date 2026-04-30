# shelfd 0.1.0-preview-5 — rollout packet (SHELF-21 v1)

**Status:** image built + pushed, **NOT** deployed.
**Origin incident:** 2026-04-27 14:23 UTC P1 — Trino dbt INSERTs on
rep-1 returned `HIVE_WRITER_CLOSE_ERROR` because the SHELF-22 shim
served only `GET`/`HEAD`, so PUT/DELETE (Iceberg writes + cleanups)
hit axum's default 405. See
`shelfd/docs/runbooks/2026-04-rep1-revert-cdp-endpoint.md` for the
revert that re-pointed rep-1's `cdp.s3.endpoint` at real S3 to
unblock dbt.

This runbook is the **forward** path: ship the structural fix and
re-cutover rep-1 cleanly.

## What's in preview-5 (delta vs preview-4)

| Area | Change |
| --- | --- |
| `shelfd/src/origin.rs` | `Origin` trait + `S3Origin` gain `put_object` (single-shot) and `delete_object` (idempotent — origin 404 surfaces as `Ok(())`). Both wrap timeout + request-id logging + the existing origin metrics (`shelf_origin_request_*{op=put_object\|delete_object}`). |
| `shelfd/src/s3_shim.rs` | Router registers `.put(handle_put_object)` and `.delete(handle_delete_object)` on `/:bucket/*key`. `handle_put_object` buffers the body up to **256 MiB** (above this → `501 NotImplemented` mentioning SHELF-21b). On 2xx PUT we call `head_lru.invalidate(...)` + `head_lru.forget_missing(...)`; on 2xx DELETE we call `head_lru.record_missing(...)` (which already drops any positive entry). |
| `shelfd/src/head_lru.rs` | New `HeadLru::invalidate(bucket, key)` — drops a positive entry **without** poisoning the negative cache. Two new unit tests pin the contract. |
| `shelfd/tests/it_shim_write.rs` | New 5-test suite (round-trip PUT/GET, post-PUT cache flush, DELETE-then-GET → 404, idempotent DELETE on missing key, oversized-body → 501). All green against MinIO. |
| `charts/shelf/values-prod.yaml` | `image.tag: "0.1.0-preview-5"` + comment block describing the new SHELF-21 surface. |

### Crucial design simplification

The original SHELF-21 spec proposed evicting Foyer entries on every
write. **We don't need to.** SHELF-04 keys are content-addressed by
ETag, so a successful PUT changes the object's ETag → next GET
re-HEADs → derives a fresh content-addressed Foyer key → naturally
misses → fetches new bytes from origin. Old entries become
unreachable orphans and age out via S3FIFO/LRU. **Invalidating the
HEAD-LRU alone is sufficient for correctness.**

This eliminates a whole class of "did the eviction race the read"
failure modes and reduces the change surface to two narrow
HEAD-LRU calls per write.

### Out of scope (still SHELF-21b)

- Multipart uploads (`POST ?uploads`, `PUT ?partNumber=`,
  `POST ?uploadId=`, `DELETE ?uploadId=`). **Trino falls back to
  multipart for chunks > 16 MiB**, so any single CTAS that produces
  large output files will still fail through the shim. Until -21b
  ships, leave such workloads pointed at real S3.
- `ListObjectsV2` — needed by Iceberg `RemoveOrphanFiles` walk.
- Bulk `POST /:bucket?delete`.

## Image

```
ghcr.io/shelf-project/data/data-engineering/ranger/shelfd:0.1.0-preview-5
manifest list digest:  sha256:576788232981bcfcdfc60270c7ac7705636743595c0306f2fcf29de3dd7578af
linux/amd64 digest:    sha256:f617a1ac3a82a8dd14396d15cb2df091f66073fd03f8a54fe949757d345905ea
linux/arm64 digest:    sha256:be65ce6b208cc72e6faf1858a0f3a8c0a5abb9e50195b2c7f94a5aaf5b25173d
```

Built locally on 2026-04-27 from a clean dirty-tree compile (no
warnings), pushed to GitLab Container Registry. The CI pipeline is
not the source of truth for this preview tag.

## Pre-flight checks before merging the deployments-repo MR

1. Verify the manifest list is reachable and shows both arches:

   ```bash
   docker buildx imagetools inspect \
     ghcr.io/shelf-project/data/data-engineering/ranger/shelfd:0.1.0-preview-5
   ```

2. Confirm the chart bump compiles:

   ```bash
   cd /path/to/deployments-repo
   helm template charts/shelf -f values/prod.yaml | grep image:
   ```

3. Re-run the SHELF-21 integration suite locally one last time:

   ```bash
   cd shelfd/tests && docker compose up -d minio
   SHELF_INTEGRATION=1 cargo test -p shelfd \
     --test it_shim_write --test it_s3_shim --test it_read_path \
     -- --test-threads=1
   ```

   Expected: `15 passed; 0 failed`.

## deployments-repo MR — draft body

> **Title:** shelfd: bump to 0.1.0-preview-5 (SHELF-21 write-passthrough v1)
>
> **Why:** preview-4 shim returned 405 on PUT/DELETE, blocking every
> Trino write through `s3.endpoint=shelf-N`. preview-5 adds
> single-shot PUT and idempotent DELETE proxied to the configured
> origin, with HEAD-LRU invalidation on success. Multipart and
> ListObjectsV2 deferred to SHELF-21b — large CTAS / RemoveOrphanFiles
> stay on real S3 for now.
>
> **Risk:** isolated to shelfd pods; rep-1 still pointed at real S3
> after !17873 was reverted. Roll preview-5 to **all three shelfd
> pods first**, then re-cutover rep-1's `cdp.s3.endpoint` in a
> follow-up MR (separate change to keep the blast radius readable).
>
> **Validation:**
> - Unit + integration tests green (15/15 against MinIO).
> - Manifest list verified for amd64 + arm64.
> - Smoke-test plan: PUT a 1 KiB blob through the shim, GET it back,
>   DELETE it, GET → 404. See `## Post-deploy smoke test` below.
>
> **Rollback:** revert the values change (one-line `image.tag` →
> `0.1.0-preview-4`) and ArgoCD reconciles in <2 min. No data loss
> path — the shim is stateless wrt writes.

```yaml
# values/prod.yaml diff (one-liner)
- image.tag: "0.1.0-preview-4"
+ image.tag: "0.1.0-preview-5"
```

## One-shot rollout (run from ops box)

```bash
# 1. Merge the deployments-repo MR. ArgoCD reconciles within 2 min.
# 2. Watch rollout (rolling restart, 1 pod at a time):
kubectl -n alluxio rollout status statefulset/shelf --timeout=5m

# 3. Confirm all 3 pods on the new digest:
kubectl -n alluxio get pods -l app.kubernetes.io/name=shelf \
    -o custom-columns='NAME:.metadata.name,IMAGE:.spec.containers[0].image,RESTARTS:.status.containerStatuses[0].restartCount'

# 4. Sanity probe — PUT/GET/DELETE through shelf-2 (still safe to
#    target while rep-1 cdp.s3.endpoint is pointed at real S3):
POD=shelf-2.shelf.cache.svc.cluster.local
kubectl -n alluxio run shelf21-smoke --rm -i --restart=Never --image=curlimages/curl -- \
  sh -c "curl -fsS -X PUT  --data 'shelf-21 smoke' http://${POD}:9092/shelf-it/_smoke/preview5 \
      && curl -fsS         http://${POD}:9092/shelf-it/_smoke/preview5 \
      && curl -fsS -X DELETE http://${POD}:9092/shelf-it/_smoke/preview5 \
      && (curl -sS -o /dev/null -w '%{http_code}\n' http://${POD}:9092/shelf-it/_smoke/preview5 | grep -q 404 && echo 'smoke ok')"

# 5. Read-path regression check — rep-2 mbuser_admin p99
#    (Grafana 'Shelf — read path' dashboard, last 30 m):
#    expect no regression vs preview-4 baseline.
```

## Post-deploy smoke test (Grafana)

After the rollout completes, the read-path dashboard should show:

- `shelf_request_seconds_bucket{path="/s3/get_object"}` p99 unchanged
  (writes don't share the histogram path label, so this is a pure
  read regression check).
- `shelf_s3_shim_response_bytes_total{op="put_object",outcome="ok"}`
  starts incrementing as soon as a write-capable replica is pointed
  at the shim. While rep-1's cdp endpoint is still real S3, this
  series stays flat — that is correct and expected.
- `shelf_origin_request_seconds_bucket{op="put_object",outcome="ok"}`
  exists in the registry but is observed only when traffic flows
  through the new path.

## Re-cutover rep-1 (separate, follow-up MR)

Only after preview-5 has been live on **all three** shelfd pods for
**at least 6 hours with zero pod restarts**:

```yaml
# trino/replica-1 cdp catalog properties — re-apply !17873 equivalent
- s3.endpoint=https://s3.ap-south-1.amazonaws.com
+ s3.endpoint=http://shelf-1.shelf.cache.svc.cluster.local:9092
```

Then run the dbt iceberg-maintain canary against
`cdp.admin.iceberg_maintenance_log`. The same INSERT that produced
the original 405 should now succeed; verify with:

```sql
SELECT MAX(log_date) FROM cdp.admin.iceberg_maintenance_log;
```

The row this query returns must be later than the cutover timestamp.

## Rollback decision tree

| Symptom | Action |
| --- | --- |
| Any shelfd pod crashloops on preview-5. | Revert chart to preview-4 (one-line). |
| `shelf_request_seconds{path="/s3/get_object"}` p99 regresses > 20 % over baseline for ≥ 5 min. | Revert chart to preview-4. (Read path was not modified, so this would be unexpected — investigate before re-rolling.) |
| Re-cutover MR for rep-1 produces 5xx storms on PUT. | Revert just the rep-1 cdp endpoint MR (back to real S3). Leave preview-5 deployed. |
| Need to disable the new write path entirely. | Not possible at runtime — there's no feature flag in v1 (kept the change small on purpose). Roll back to preview-4 if needed. |

## TODOs after rollout

- Land SHELF-21b for multipart + ListObjectsV2; rep-1 won't survive
  large CTAS until then.
- Add a write-path Grafana panel: `rate(shelf_s3_shim_response_bytes_total{op=~"put_object|delete_object"}[5m])`
  with `outcome` as a series.
- Add a 405/501 alert for the shim — fires if any non-2xx-non-404
  shows up in `shelf_request_seconds` for the new path labels.
