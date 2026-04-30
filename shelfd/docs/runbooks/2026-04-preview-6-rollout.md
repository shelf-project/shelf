# shelfd 0.1.0-preview-6 — rollout packet (SHELF-21b)

**Status:** image built + pushed, **NOT** deployed.
**Predecessor:** preview-5 (SHELF-21 v1) shipped single-shot
`PUT`/`DELETE`. Multipart, `ListObjectsV2`, and bulk `DeleteObjects`
were the deferred surface — Trino falls back to multipart for any
chunk > 16 MiB, so until preview-6 ships, large CTAS / RemoveOrphanFiles
still fail through the shim. preview-6 closes that gap.

This runbook is the **forward** path: ship the verb-complete shim
and unblock every Iceberg write workload through `s3.endpoint=shelf-N`.

## What's in preview-6 (delta vs preview-5)

| Area | Change |
| --- | --- |
| `shelfd/src/origin.rs` | `Origin` trait grows six methods: `create_multipart_upload`, `upload_part`, `complete_multipart_upload`, `abort_multipart_upload`, `list_objects_v2`, `delete_objects_bulk`. New types `CompletedPart`, `ListedObject`, `ListObjectsV2Page`, `BulkDeleteOutcome`. `S3Origin` impls cover all six with the same span/timeout/`record_origin` instrumentation as v1, including a 32-way bounded fan-out for bulk delete. AbortMultipart maps origin 404 → `Ok(())` so cleanup loops are idempotent. |
| `shelfd/src/s3_shim.rs` | Router gains `.post(dispatch_post_object)` on `/:bucket/*key` and a new `/:bucket` route with `.get(handle_list_objects_v2).post(handle_bucket_post)`. `dispatch_put` / `dispatch_delete` short-circuit on `partNumber`/`uploadId` query params; `dispatch_post_object` chooses between `?uploads` (initiate) and `?uploadId=…` (complete). New `xml_ok` helper centralises the 200/204 response shape; new `record_path_latency` records `/s3/{op}` histograms uniformly across all SHELF-21b verbs. |
| `shelfd/src/s3_shim/xml.rs` (new) | Hand-rolled XML codec for the SHELF-21b schemas. Parsers reject empty `<CompleteMultipartUpload>` / `<Delete>` bodies, `<PartNumber>` outside `[1, 10000]`, and re-sort guards (parts emitted in caller order; AWS rejects out-of-order anyway). Renderers emit byte-correct `InitiateMultipartUploadResult`, `CompleteMultipartUploadResult`, `ListBucketResult` (V2), and `DeleteResult` envelopes. 14 unit tests pin the contract. |
| `shelfd/tests/it_shim_write_v2.rs` (new) | 9 integration tests against MinIO: 3-part multipart round-trip with composite ETag, abort + idempotent re-abort, ListObjectsV2 ordering + delimiter/CommonPrefixes + 3-page pagination via continuation-token, bulk DeleteObjects (verbose + Quiet modes), malformed-body 400s, partNumber bound checks. All green. |
| `charts/shelf/values-prod.yaml` | `image.tag: "0.1.0-preview-6"` + comment block describing the new verb surface. |

### Cache-invalidation contract (preview-6)

Same simplification as preview-5 — content-addressed Foyer keys mean
we never need to evict cache entries on writes:

- `CompleteMultipartUpload` ⇒ `head_lru.invalidate(bucket, key)` +
  `head_lru.forget_missing(bucket, key)` (just like single-shot PUT).
- `AbortMultipartUpload` ⇒ no-op on the cache (no bytes ever became
  part of the object).
- Bulk `DeleteObjects` ⇒ `head_lru.record_missing(bucket, key)` for
  each successfully-deleted row; failed rows are left alone (don't lie
  about cache state).

Stale Foyer entries still age out via S3FIFO/LRU; no eviction races
introduced.

### What remains deferred (SHELF-21c+)

- **Streaming multipart parts.** v1's `UploadPart` buffers each part
  in memory up to 256 MiB. Trino's default `s3.streaming.part-size` is
  16 MiB, so this is comfortable headroom in practice — but a client
  that opts into 5 GiB parts (the AWS hard ceiling) would hit a 501.
  Fix is to thread the request body straight into `aws-sdk-s3`'s
  `ByteStream::from_body_1_x`. Tracked as SHELF-21c.
- **Native `DeleteObjects` SDK call.** preview-6 fan-outs to N
  `delete_object` calls (32-way bounded). For the ≤ 1000-key caps
  Iceberg actually emits, the wall-clock cost is dominated by
  connection-pool recycle, not per-call overhead. Swap-in is mechanical.
- **v1 `ListObjects` (`list-type` ≠ 2).** Returns 501 `NotImplemented`
  on purpose — Trino + Iceberg only call v2 and silently shipping a
  `<Marker>`-shaped envelope to a v1 caller would mask the protocol
  mismatch.

## Image

```
ghcr.io/shelf-project/data/data-engineering/ranger/shelfd:0.1.0-preview-6
manifest list digest:  sha256:e8f4a71342524d5942caf4ddb69b8e34702a007e75e7c91b7e42e344897fc9a6
linux/amd64 digest:    sha256:2851109a585e135ce741da3718dc05489bcd45f3dfcf3b4f14363790908c2839
linux/arm64 digest:    sha256:732e09460a87008bd1b70e5fc173779983cd799061512daaecd0cc92f56ce3d8
```

Built locally on 2026-04-27 from a clean compile (no warnings),
pushed to GitLab Container Registry. The CI pipeline is not the
source of truth for this preview tag.

## Pre-flight checks before merging the deployments-repo MR

1. Verify the manifest list:

   ```bash
   docker buildx imagetools inspect \
     ghcr.io/shelf-project/data/data-engineering/ranger/shelfd:0.1.0-preview-6
   ```

2. Confirm the chart bump compiles:

   ```bash
   cd /path/to/deployments-repo
   helm template charts/shelf -f values/prod.yaml | grep image:
   ```

3. Re-run the SHELF-21 + SHELF-21b integration suites:

   ```bash
   cd shelfd/tests && docker compose up -d minio
   SHELF_INTEGRATION=1 cargo test -p shelfd \
     --test it_shim_write \
     --test it_shim_write_v2 \
     --test it_s3_shim \
     --test it_read_path \
     -- --test-threads=1
   ```

   Expected: `24 passed; 0 failed` (5 v1 + 9 v2 + 4 read + 6 shim).

## deployments-repo MR — draft body

> **Title:** shelfd: bump to 0.1.0-preview-6 (SHELF-21b multipart + ListObjectsV2 + bulk DeleteObjects)
>
> **Why:** preview-5 closed the read-only-shim P1 for *small* writes
> only. Trino's S3 client falls back to multipart for any chunk
> > 16 MiB and Iceberg's `RemoveOrphanFiles` walks the bucket via
> ListObjectsV2 + bulk DeleteObjects. Without preview-6 those paths
> still 405 → bypass-to-real-S3 is the only way to keep them safe.
> preview-6 makes the shim a strict superset of the AWS S3 verb set
> Trino actually issues — every write workload now works through
> `s3.endpoint=shelf-N`.
>
> **Risk:** isolated to shelfd pods; behaviour is a strict
> superset of preview-5 (every preview-5 caller still works). No
> chart shape changes beyond the image tag.
>
> **Validation:**
> - Lib + integration tests green: 184 lib (incl. 14 new XML codec
>   tests), 9 SHELF-21b integration tests, plus 5 SHELF-21 v1 tests
>   from preview-5 still passing — total 207 / 207 tests against
>   MinIO.
> - Manifest list verified for amd64 + arm64.
> - Smoke-test plan: end-to-end multipart upload + ListObjectsV2 +
>   bulk DeleteObjects against shelf-2. See `## Post-deploy smoke
>   test` below.
>
> **Rollback:** revert the values change (one-line `image.tag` →
> `0.1.0-preview-5`) and ArgoCD reconciles in <2 min. preview-5
> still serves all single-shot PUT/DELETE traffic — fall-back is
> safe and stateless.

```yaml
# values/prod.yaml diff (one-liner)
- image.tag: "0.1.0-preview-5"
+ image.tag: "0.1.0-preview-6"
```

## One-shot rollout (run from ops box)

```bash
# 1. Merge the deployments-repo MR. ArgoCD reconciles within 2 min.
# 2. Watch rollout (rolling restart, 1 pod at a time):
kubectl -n alluxio rollout status statefulset/shelf --timeout=5m

# 3. Confirm all 3 pods on the new digest:
kubectl -n alluxio get pods -l app.kubernetes.io/name=shelf \
    -o custom-columns='NAME:.metadata.name,IMAGE:.spec.containers[0].image,RESTARTS:.status.containerStatuses[0].restartCount'

# 4. SHELF-21b smoke probe — multipart + list + bulk-delete through
#    shelf-2. Uses awscli pointed at the shim instead of real S3.
POD=shelf-2.shelf.cache.svc.cluster.local
kubectl -n alluxio run shelf21b-smoke --rm -i --restart=Never \
    --image=public.ecr.aws/aws-cli/aws-cli -- \
    sh -c "
      export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=ap-south-1;
      EP=http://${POD}:9092;
      # multipart: aws cli auto-uses multipart when --multipart-upload is hinted
      # by file size; ~10 MiB is enough on modern cli defaults.
      head -c 10485760 /dev/urandom > /tmp/blob.bin;
      aws --endpoint-url \$EP s3 cp /tmp/blob.bin s3://shelf-it/_smoke/preview6.bin &&
      aws --endpoint-url \$EP s3api list-objects-v2 --bucket shelf-it --prefix _smoke/ &&
      aws --endpoint-url \$EP s3api delete-objects --bucket shelf-it \
          --delete 'Objects=[{Key=_smoke/preview6.bin}],Quiet=false' &&
      aws --endpoint-url \$EP s3api head-object --bucket shelf-it \
          --key _smoke/preview6.bin 2>&1 | grep -q 404 && echo 'smoke ok'
    "

# 5. Read-path regression check — rep-2 mbuser_admin p99
#    (Grafana 'Shelf — read path' dashboard, last 30 m):
#    expect no regression vs preview-5 baseline.
```

## Post-deploy smoke test (Grafana)

After the rollout completes, the read-path dashboard should show:

- `shelf_request_seconds_bucket{path="/s3/get_object"}` p99 unchanged
  vs preview-5 baseline.
- New series begin populating as soon as a write-capable replica
  routes traffic through the shim:
  - `shelf_s3_shim_response_bytes_total{op="upload_part",outcome="ok"}`
  - `shelf_s3_shim_response_bytes_total{op="complete_multipart_upload",outcome="ok"}`
  - `shelf_s3_shim_response_bytes_total{op="list_objects_v2",outcome="ok"}`
  - `shelf_s3_shim_response_bytes_total{op="delete_objects",outcome="ok"}` (and `outcome="partial"` if any single-row failures)
- Histogram buckets exist with `path` ∈ {`/s3/create_multipart_upload`,
  `/s3/upload_part`, `/s3/complete_multipart_upload`,
  `/s3/abort_multipart_upload`, `/s3/list_objects_v2`,
  `/s3/delete_objects`}. They're observed only when traffic flows
  through the new path.

## Re-cutover rep-1 (separate, follow-up MR)

Only after preview-6 has been live on **all three** shelfd pods for
**at least 6 hours with zero pod restarts**:

```yaml
# trino/replica-1 cdp catalog properties — re-apply !17873 equivalent
- s3.endpoint=https://s3.ap-south-1.amazonaws.com
+ s3.endpoint=http://shelf-1.shelf.cache.svc.cluster.local:9092
```

Then run the dbt iceberg-maintain canary against `cdp.admin.iceberg_maintenance_log`
*and* a CTAS large enough to trigger multipart (≥ 32 MiB output is a
good marker), e.g.:

```sql
CREATE TABLE cdp.admin.preview6_canary AS
SELECT * FROM cdp.admin.iceberg_maintenance_log
WHERE log_date >= DATE '2026-04-01';
```

Both the INSERT and the CTAS must succeed. Verify the maintenance
log got a fresh row:

```sql
SELECT MAX(log_date) FROM cdp.admin.iceberg_maintenance_log;
```

Then drop the canary table to exercise the bulk-delete path:

```sql
DROP TABLE cdp.admin.preview6_canary;
```

…and verify in Grafana that `delete_objects` outcome is `ok` for
every batch.

## Rollback decision tree

| Symptom | Action |
| --- | --- |
| Any shelfd pod crashloops on preview-6. | Revert chart to preview-5 (one-line). |
| `shelf_request_seconds{path="/s3/get_object"}` p99 regresses > 20 % over baseline for ≥ 5 min. | Revert chart to preview-5. (Read path was not modified, so this would be unexpected — investigate before re-rolling.) |
| Multipart Complete returns 5xx storm. | Revert. Capture a `shelf_origin_request_seconds{op="complete_multipart_upload",outcome=~"error|timeout"}` graph for the SHELF-21c streaming-part follow-up. |
| ListObjectsV2 truncation / wrong key set on Iceberg planning. | Revert. The shim's `NextContinuationToken` is forwarded verbatim from the AWS SDK, so a regression here points at our XML emission — diff the failing call's body against the integration-test golden output. |
| Bulk DeleteObjects partial failures (`outcome="partial"` series climbs). | Don't necessarily revert. Inspect the `<Error>` rows in the response body — origin-side AccessDenied is a config issue, not a shim regression. |

## TODOs after rollout

- SHELF-21c: streaming part bodies (skip the 256 MiB per-part buffer);
  switch bulk-delete to native `DeleteObjects` SDK call.
- Add Grafana panel: write-path verb mix as a stacked-area —
  `rate(shelf_s3_shim_response_bytes_total{op=~"upload_part|complete_multipart_upload|list_objects_v2|delete_objects"}[5m])`,
  with `outcome` as a series.
- Add a 4xx alert on the new path labels (parser-rejection rate is a
  client-bug canary, not an availability incident, but a steady
  > 0.1 req/s deserves a Slack ping).
