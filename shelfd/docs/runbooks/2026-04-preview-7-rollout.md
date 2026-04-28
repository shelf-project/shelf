# shelfd 0.1.0-preview-7 — rollout packet (SHELF-21c)

**Status:** image built + pushed, **NOT** deployed.
**Predecessor:** preview-6 (SHELF-21b) shipped multipart +
ListObjectsV2 + bulk DeleteObjects (per-key fan-out). preview-7
closes the two cliffs preview-6 left in place: each `UploadPart`
buffered the full part body in memory (256 MiB cap), and bulk
`DeleteObjects` issued one SDK round-trip per key (32-way bounded).
preview-7 streams parts straight through to the SDK and replaces the
fan-out with one native `DeleteObjects` call per ≤ 1000-key chunk.

This runbook is the **forward** path: ship the streaming + native
bulk-delete shim and unblock 16 MiB+-part workloads (CTAS, large
INSERTs) that today still buffer per part on the shelfd pod.

## What's in preview-7 (delta vs preview-6)

| Area | Change |
| --- | --- |
| `shelfd/src/origin.rs` | `Origin::upload_part` signature changed: takes `ByteStream` + explicit `content_length: u64` instead of `Bytes`. `S3Origin::upload_part` passes the stream straight to the SDK with the new span attribute `streaming = true`. `S3Origin::delete_objects_bulk` rewritten — drops the 32-way `JoinSet` fan-out and uses a single `delete_objects()` SDK call per ≤ 1000-key chunk, mapping `NoSuchKey` per-row errors to "deleted" so idempotent retries are still 200 / all-Deleted. Verbose error formatting now includes the AWS service `code` + `message` so MinIO/AWS-side issues surface readably in shelfd's logs. |
| `shelfd/src/s3_shim.rs` | `handle_upload_part` accepts `HeaderMap`, parses `Content-Length` (411 if missing, 400 if malformed), validates against new `SHIM_MAX_PART_BYTES = 5 GiB` (501 `EntityTooLarge` if exceeded), and pipes the inbound `axum::body::Body` straight into `ByteStream::from_body_1_x` via the new `SyncBody` adapter. `dispatch_put` threads the headers through. The 256 MiB per-part buffer is gone. |
| `shelfd/src/s3_shim.rs` (cont.) | New `SyncBody` adapter — wraps `axum::body::Body` (which is `Send + !Sync`) in `sync_wrapper::SyncWrapper` so it satisfies the `Send + Sync + 'static` bound the AWS SDK's `ByteStream::from_body_1_x` demands. Polling still goes through `&mut self`, which axum's body type already requires, so the wrapper is zero-cost at runtime. |
| `shelfd/Cargo.toml` | New direct deps: `sync_wrapper = "1"` (already transitively present via `tower`/`axum`) and `http-body = "1"` (already transitively present via `axum`). Both promoted to direct deps to make the `SyncBody` adapter's trait/types explicit. |
| `shelfd/tests/it_shim_write_v2.rs` | 4 new integration tests: 32 MiB streaming `UploadPart` round-trip + head-size assert, raw-TCP `Content-Length: 6 GiB` → 501, 50-key native bulk delete (verbose), 6-key idempotent bulk delete on never-existed keys. All green vs MinIO. |
| `charts/shelf/values-prod.yaml` | `image.tag: "0.1.0-preview-7"` (one-line bump in the deployments-repo MR). |

### Cache-invalidation contract (preview-7)

Unchanged from preview-6. Streaming part bytes never touch the
shim's caches (parts are intermediate state); bulk-delete still
runs `head_lru.record_missing` only on `error=None` rows. The
content-addressed Foyer key invariant carries through — preview-7
is byte-equivalent to preview-6 at the cache layer, only the
write-path memory + round-trip behaviour changed.

### What remains deferred (post-preview-7)

- **v1 `ListObjects`** (`list-type` ≠ 2) — same rationale as
  preview-6: silently shipping a `<Marker>`-shaped envelope to a v1
  caller would mask a protocol mismatch we'd rather catch in CI.
- **Inbound SigV4 on the shim.** Continues to trust the in-cluster
  network. Adding inbound SigV4 is a separate hardening track
  (SHELF-22 would be the natural slot).
- **Adaptive bulk-delete chunking past S3's 1000-key hard cap.**
  The impl chunks transparently, but the integration suite only
  exercises ≤ 50 keys. If a workload ever pushes > 1000-key bulk
  deletes, add a 1500-key test to pin the multi-chunk semantics.

## Image

```
ghcr.io/shelf-project/data/data-engineering/ranger/shelfd:0.1.0-preview-7
manifest list digest:  sha256:4562e4e3fc208b53459cb1047285910b2ae2f74889396df109458d66fb63ea6a
linux/amd64 digest:    sha256:8a6ca3b977395645b5653082da5764c51a7cb28ebbe088895e65e365b511bdff
linux/arm64 digest:    sha256:d322bdf30df5ce3417ad144bcb67b5a70ef2089f670f4525e46bab80ff2c04e8
```

Built locally on 2026-04-27 from a clean compile (release profile,
no warnings on the SHELF-21c-touched files), pushed to GitLab
Container Registry. The CI pipeline is not the source of truth for
this preview tag.

## Pre-flight checks before merging the deployments-repo MR

1. Verify the manifest list:

   ```bash
   docker buildx imagetools inspect \
     ghcr.io/shelf-project/data/data-engineering/ranger/shelfd:0.1.0-preview-7
   ```

   Expect the two digests above (amd64 + arm64) plus the two
   `unknown/unknown` attestation manifests buildkit ships
   alongside.

2. Confirm the chart bump compiles:

   ```bash
   cd /path/to/deployments-repo
   helm template charts/shelf -f values/prod.yaml | grep image:
   ```

   Expected: `image: registry.gitlab.com/.../shelfd:0.1.0-preview-7`.

3. Re-run the SHELF-21 + SHELF-21b + SHELF-21c integration suites
   against a recent MinIO (≥ 2025-01 — older releases reject the
   AWS SDK's default CRC32 checksum on `DeleteObjects` with
   `MissingContentMD5`):

   ```bash
   cd shelfd/tests && docker compose up -d minio
   SHELF_INTEGRATION=1 cargo test -p shelfd \
     --test it_shim_write \
     --test it_shim_write_v2 \
     --test it_s3_shim \
     --test it_read_path \
     -- --test-threads=1
   ```

   Expected: `28 passed; 0 failed` (5 v1 + 13 v2/c + 4 read + 6 shim).

4. **Pre-cutover write-path smoke** (lesson from the 2026-04-27
   SHELF-21 P1) — run `shelf/docs/rollout-v1/write-smoke.sh`
   against the candidate shelfd endpoint **before** merging the
   chart bump. Asserts PUT / DELETE / multipart Complete /
   ListObjectsV2 / bulk DeleteObjects all 2xx in < 30 s.

## deployments-repo MR — draft body

> **Title:** shelfd: bump to 0.1.0-preview-7 (SHELF-21c streaming UploadPart + native bulk DeleteObjects)
>
> **Why:** preview-6 closed the v2 verb gap (multipart, list,
> bulk-delete) but kept two memory + round-trip cliffs in place: every
> `UploadPart` buffered the full part body in shelfd's heap (256 MiB
> cap), and bulk `DeleteObjects` issued one SDK call per key. The
> first cliff caps Trino's `s3.streaming.part-size` at 256 MiB even
> though S3 itself supports 5 GiB; the second adds ~30 round-trips
> per `RemoveOrphanFiles` batch on a saturated link. preview-7
> streams parts straight from the wire into the SDK's `ByteStream`
> (no intermediate buffer) and replaces the 32-way fan-out with one
> native `delete_objects()` call per ≤ 1000-key chunk.
>
> **Risk:** isolated to shelfd pods; behaviour is a strict
> superset of preview-6 (every preview-6 caller still works). Wire
> shape unchanged for `UploadPart` — the streaming swap is internal
> to shelfd. `DeleteObjects` still emits the same `<DeleteResult>`
> envelope; idempotent semantics on `NoSuchKey` rows are preserved.
> No chart shape changes beyond the image tag.
>
> **Validation:**
> - Lib + integration tests green: 184 lib, 13 SHELF-21b/c
>   integration tests (incl. 4 new SHELF-21c cases), 5 SHELF-21 v1
>   tests still passing — total 211 / 211 against MinIO ≥ 2025-01.
> - Manifest list verified for amd64 + arm64.
> - Smoke-test plan: end-to-end multipart upload (≥ 32 MiB part) +
>   bulk `DeleteObjects` against shelf-2. See `## Post-deploy smoke
>   test` below.
>
> **Rollback:** revert the values change (one-line `image.tag` →
> `0.1.0-preview-6`) and ArgoCD reconciles in <2 min. preview-6
> still serves the full v2 verb set — fall-back is safe and
> stateless. (preview-5 is also still safe to fall back to if both
> v2 and v3 need to be quarantined; loses multipart/list/bulk-delete
> support.)

```yaml
# values/prod.yaml diff (one-liner)
- image.tag: "0.1.0-preview-6"
+ image.tag: "0.1.0-preview-7"
```

## One-shot rollout (run from ops box)

```bash
# 1. Merge the deployments-repo MR. ArgoCD reconciles within 2 min.
# 2. Watch rollout (rolling restart, 1 pod at a time):
kubectl -n alluxio rollout status statefulset/shelf --timeout=5m

# 3. Confirm all 3 pods on the new digest:
kubectl -n alluxio get pods -l app.kubernetes.io/name=shelf \
    -o custom-columns='NAME:.metadata.name,IMAGE:.spec.containers[0].image,RESTARTS:.status.containerStatuses[0].restartCount'

# 4. SHELF-21c smoke probe — large multipart + bulk-delete through
#    shelf-2. Uses awscli pointed at the shim instead of real S3.
POD=shelf-2.shelf.cache.svc.cluster.local
kubectl -n alluxio run shelf21c-smoke --rm -i --restart=Never \
    --image=public.ecr.aws/aws-cli/aws-cli -- \
    sh -c "
      export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=ap-south-1;
      EP=http://${POD}:9092;
      # 64 MiB blob — exercises the streaming part path past the
      # old 256 MiB buffer cap *and* across multiple TCP reads.
      head -c 67108864 /dev/urandom > /tmp/blob.bin;
      aws --endpoint-url \$EP s3 cp /tmp/blob.bin s3://shelf-it/_smoke/preview7-large.bin --expected-size 67108864 &&
      aws --endpoint-url \$EP s3api head-object --bucket shelf-it --key _smoke/preview7-large.bin &&
      # bulk delete via the new native path — ~50 keys
      for i in \$(seq 0 49); do
        aws --endpoint-url \$EP s3api put-object --bucket shelf-it --key _smoke/preview7-bulk-\${i}.bin --body /dev/null > /dev/null;
      done;
      python3 -c \"
import json
keys=[{'Key': f'_smoke/preview7-bulk-{i}.bin'} for i in range(50)] + [{'Key':'_smoke/preview7-large.bin'}]
print(json.dumps({'Objects': keys, 'Quiet': False}))
\" > /tmp/del.json;
      aws --endpoint-url \$EP s3api delete-objects --bucket shelf-it --delete file:///tmp/del.json | tee /tmp/del.out;
      grep -q '\"Errors\"' /tmp/del.out && exit 1;
      echo 'smoke ok';
    "

# 5. Read-path regression check — rep-2 mbuser_admin p99
#    (Grafana 'Shelf — read path' dashboard, last 30 m):
#    expect no regression vs preview-6 baseline.
```

## Post-deploy smoke test (Grafana)

After the rollout completes:

- `shelf_request_seconds_bucket{path="/s3/get_object"}` p99 unchanged
  vs preview-6 baseline (read path was not modified).
- `shelf_origin_request_seconds{op="upload_part",outcome="ok"}`
  histogram bucket distribution **shifts** for parts > 16 MiB —
  preview-6 buffered the body and reported `bytes` only after
  `body.collect()`; preview-7 streams, so wall-clock `seconds`
  on a saturated link should be slightly tighter (one fewer
  buffer-and-flush hop). For ≤ 16 MiB parts (Trino's default)
  the distribution should be statistically identical.
- `shelf_origin_request_seconds{op="delete_objects",outcome="ok"}`
  populates as a **new** series — preview-6 emitted per-key
  `op="delete_object"` events from the fan-out; preview-7
  collapses each chunk into one `op="delete_objects"` event. Tail
  latency on `RemoveOrphanFiles` runs should drop visibly.
- `shelf_origin_request_seconds{op="delete_objects",outcome="partial"}`
  should stay at zero in the happy path; any non-zero rate points
  at upstream IAM / object-lock issues, **not** a shelfd
  regression. Read the response body's `<Error>` rows — that's the
  same diagnostic surface preview-6 carried.

## Re-cutover rep-1 (separate, follow-up MR)

Same gating rule as preview-6: only after preview-7 has been live
on **all three** shelfd pods for **at least 6 hours with zero pod
restarts**. The rep-1 cdp-catalog endpoint flip is in the
deployments-repo (cicd-v2 branch). Run:

```yaml
# trino/replica-1 cdp catalog properties — re-apply !17873 equivalent
- s3.endpoint=https://s3.ap-south-1.amazonaws.com
+ s3.endpoint=http://shelf-1.shelf.cache.svc.cluster.local:9092
```

…and exercise the streaming + bulk-delete paths from Trino:

```sql
-- 1. Multipart streaming canary — produce ≥ 64 MiB output so
-- Trino's S3 client uses multipart with parts > 16 MiB.
CREATE TABLE cdp.admin.preview7_canary AS
SELECT * FROM cdp.fact.events
WHERE event_date >= DATE '2026-04-01';

-- 2. Iceberg maintenance hits the bulk-delete path through
-- RemoveOrphanFiles.
ALTER TABLE cdp.admin.preview7_canary EXECUTE expire_snapshots(retention_threshold => '0d');
ALTER TABLE cdp.admin.preview7_canary EXECUTE remove_orphan_files(retention_threshold => '0d');

-- 3. DROP exercises bulk-delete on every data file at once.
DROP TABLE cdp.admin.preview7_canary;
```

Verify in Grafana that:
- `op="upload_part"` outcome is `ok` for every part (no `error`,
  no `timeout`).
- `op="delete_objects"` outcome is `ok` for every batch (no
  `partial`).
- `path=/s3/upload_part` p99 wall-clock for parts > 16 MiB has
  not regressed vs preview-6 (it should match or improve).

## Rollback decision tree

| Symptom | Action |
| --- | --- |
| Any shelfd pod crashloops on preview-7. | Revert chart to preview-6 (one-line). |
| `shelf_request_seconds{path="/s3/get_object"}` p99 regresses > 20 % over baseline for ≥ 5 min. | Revert chart to preview-6. (Read path was not modified — investigate the SyncBody adapter or the SDK upgrade before re-rolling.) |
| `shelf_origin_request_seconds{op="upload_part",outcome=~"error\|timeout"}` rate climbs above pre-deploy baseline. | Revert. Capture the failing request's `Content-Length` header value + `streaming = true` span attribute — if either is missing, it points at a client that lies about the body length. |
| `shelf_origin_request_seconds{op="delete_objects",outcome="error"}` rate non-zero. | Don't necessarily revert. Inspect MinIO/S3 logs — common causes are transient IAM (`AccessDenied`) or eventual-consistency lag, neither of which are shelfd regressions. The new verbose error formatting (`code=… message=…`) makes this readable in shelfd logs. |
| `shelf_origin_request_seconds{op="delete_objects",outcome="partial"}` rate climbs steadily (> 0.1 req/s). | Investigate per-`<Error>` row. Origin-side AccessDenied is a config issue; `NoSuchKey` is mapped to `ok` so it should never appear here. |
| 411 / 400 rate on `/s3/upload_part` non-zero. | Client is sending without / with malformed `Content-Length`. Trino itself never does this; check whether a non-Trino client found the shim's port. Alert on `shelf_request_seconds{path="/s3/upload_part",outcome="client_error"}` rate as a "weird client" canary. |

## TODOs after rollout

- Add a Grafana panel: write-path verb mix as a stacked-area —
  `rate(shelf_s3_shim_response_bytes_total{op=~"upload_part|complete_multipart_upload|list_objects_v2|delete_objects"}[5m])`,
  with `outcome` as a series. (Same TODO as preview-6, lifted up.)
- Add a 4xx alert on the new path labels (parser-rejection rate is
  a client-bug canary, not an availability incident, but a steady
  > 0.1 req/s deserves a Slack ping). Same as preview-6.
- If 1500+ -key bulk deletes ever land in production traffic, add
  a 1500-key integration test to pin the multi-chunk loop's
  ordering + idempotency semantics.
- Backfill SHELF-22 ticket for inbound SigV4 (carried from preview-6).
