# SHELF-21 — S3 shim write-passthrough

**Status:** v1 + v2 (SHELF-21b) + v3 (SHELF-21c) implemented. The
shim now serves the full Trino S3 verb set: single-shot PUT,
multipart upload trio (initiate/**streaming**-upload-part/complete) +
abort, ListObjectsV2 (with delimiter + continuation-token paging),
and bulk DeleteObjects (one **native** SDK round-trip per ≤1000-key
chunk, verbose + Quiet modes). See `## What actually shipped`,
`## SHELF-21b — preview-6`, `## SHELF-21c — preview-7`, and
`## SHELF-21e-v2 — preview-8` below.
**Triggered by:** 2026-04-27 14:23 UTC P1 incident on rep-1 — every
cdp write returned HTTP 405 from the shim. See
`shelfd/docs/runbooks/2026-04-rep1-revert-cdp-endpoint.md` for the
full incident packet.

## What actually shipped (v1, 0.1.0-preview-5)

- `PUT /:bucket/*key` — single-shot, body buffered up to 256 MiB
(a 256 MiB+1 byte body returns `501 NotImplemented` with
`EntityTooLarge`/SHELF-21b in the envelope so callers know exactly
why). Trino's S3 filesystem does single-shot PUTs only when the
buffered chunk is below `s3.streaming.part-size` (16 MiB default),
so 256 MiB is generous head-room.
- `DELETE /:bucket/*key` — idempotent. Origin 404 + 204 both surface
as 204 NoContent on the shim; matches AWS spec for DeleteObject.
- Cache invalidation contract: **HEAD-LRU only**. Foyer caches are
*not* explicitly evicted because SHELF-04 keys are content-
addressed via ETag — a subsequent GET re-HEADs origin, observes the
new ETag, and derives a fresh content-addressed Foyer key. Old
entries become unreachable orphans and age out via S3FIFO/LRU. This
is strictly simpler than the spec below proposed and removes a
whole class of "did we evict in time" race conditions.
- Metrics: `shelf_s3_shim_response_bytes_total{op,outcome}` already
in place; new labels `op=put_object|delete_object`,
`outcome=ok|error|oversized`.  Latency is folded into the existing
`shelf_request_seconds{path,outcome}` histogram with new path
labels `/s3/put_object` and `/s3/delete_object`.
- Tests: `shelfd/tests/it_shim_write.rs` (5 cases, all green against
MinIO) plus 2 new unit tests in `head_lru.rs` covering the
invalidate semantics.

## SHELF-21b — preview-6

Verb-complete shim. Builds on v1's HEAD-LRU-only invalidation
contract (`shelfd/docs/runbooks/2026-04-preview-6-rollout.md`):

- `POST /:bucket/*key?uploads` — InitiateMultipartUpload. Pure
passthrough; the shim doesn't track in-progress upload state.
- `PUT  /:bucket/*key?partNumber=N&uploadId=…` — UploadPart. Body
buffered up to 256 MiB per part (above this → 501 NotImplemented
citing SHELF-21c streaming-parts). PartNumber bound-checked
against `[1, 10_000]` (S3's own limit). ETag returned to caller
verbatim from origin so a subsequent CompleteMultipartUpload's
composite hash matches.
- `POST /:bucket/*key?uploadId=…` — CompleteMultipartUpload. XML
body parsed by the hand-rolled `s3_shim::xml` codec; rejects
empty `<Part>` lists and `<PartNumber>` outside `[1, 10_000]`.
Caller order is preserved (S3 itself rejects out-of-order parts —
silent re-sorting would mask client bugs). On success runs the
same HEAD-LRU invalidation as a single-shot PUT.
- `DELETE /:bucket/*key?uploadId=…` — AbortMultipartUpload. Origin
404 NoSuchUpload → 204, so cleanup loops are idempotent.
- `GET  /:bucket?list-type=2&…` — ListObjectsV2. Forwards `prefix`,
`delimiter`, `continuation-token`, `start-after`, `max-keys`
verbatim. Pagination is pass-through (the SDK's
`next_continuation_token` is opaque; we don't stitch multiple
upstream pages — Iceberg's directory walk hands the token back
on the next request, so this is correct *and* avoids unbounded
buffering). v1 ListObjects (no `list-type` param) returns 501.
- `POST /:bucket?delete` — bulk DeleteObjects. Fan-out to N parallel
single-key deletes (32-way bounded), capped at 1000 keys per
request. `<Quiet>true</Quiet>` mode hides successful rows but
keeps `<Error>` rows. Per-key HEAD-LRU `record_missing` runs only
for outcomes with `error=None` — partial failures don't lie about
cache state.

Metrics: every new path label folds into the existing
`shelf_request_seconds{path,outcome}` histogram (new paths:
`/s3/create_multipart_upload`, `/s3/upload_part`,
`/s3/complete_multipart_upload`, `/s3/abort_multipart_upload`,
`/s3/list_objects_v2`, `/s3/delete_objects`). Outcome cardinality is
deliberately small: `ok | client_error | error`. The
`shelf_s3_shim_response_bytes_total` counter gains the matching `op`
labels with `outcome ∈ {ok, partial, error}`.

Tests:

- `shelfd/src/s3_shim/xml.rs` — 14 unit tests covering parser
rejection paths and renderer output shape.
- `shelfd/tests/it_shim_write_v2.rs` — 9 integration tests against
MinIO (3-part round-trip, abort + idempotent re-abort, list
ordering + delimiter, 3-page paginated list, bulk delete verbose
  - Quiet, malformed-body 400s, partNumber bound checks).

What is **not** in preview-6 (still tracked):

- Streaming UploadPart bodies (no per-part 256 MiB buffer) — SHELF-21c.
- Native AWS `DeleteObjects` SDK call instead of the per-key fan-out
— cosmetic for ≤ 1000-key requests; trade-off noted in the runbook.
- v1 `ListObjects` (`list-type` ≠ 2) — kept off on purpose; Trino +
Iceberg only call v2 and silently shipping a v1 envelope to a v1
caller would mask the protocol mismatch.
- Origin-failure replay / 5xx-tolerant invalidation — same as v1; the
shim trusts origin's response. A 5xx from CompleteMultipartUpload
*does not* invalidate the HEAD-LRU, so a subsequent retry from
the client doesn't observe a phantom 404.

## SHELF-21c — preview-7

Closes the two memory + round-trip cliffs preview-6 left in place.
Builds on the v2 surface; no new HTTP verbs, no chart shape changes.
Rollout runbook: `shelfd/docs/runbooks/2026-04-preview-7-rollout.md`.

### What changed (delta vs preview-6)

- **Streaming `UploadPart`.** `Origin::upload_part` now takes
`aws_sdk_s3::primitives::ByteStream` + an explicit
`content_length: u64` instead of a buffered `Bytes`.
`s3_shim::handle_upload_part` wraps the inbound `axum::body::Body`
in a `SyncBody` adapter (via `sync_wrapper::SyncWrapper`) and pipes
it straight through `ByteStream::from_body_1_x`. The 256 MiB
per-part buffer is gone; the shim now passes parts up to S3's hard
5 GiB ceiling without ever materialising the part in memory.
  - **Why a `SyncBody` adapter?** `axum::body::Body` is `Send + !Sync`; `ByteStream::from_body_1_x` requires `Send + Sync + 'static` because the SDK's HTTP plumbing can move the body
  between executor threads. `SyncWrapper` provides the missing
  `Sync` by allowing access only via `&mut self` — which `Body`
  polling already requires — so we get the bound for free without
  touching axum's body type.
  - `**Content-Length` is now mandatory.** Missing → 411
  `MissingContentLength`; malformed → 400 `InvalidArgument`. SigV4
    - HTTP-1.1 both require a known body length; chunked-encoding
    fallback would mask client bugs at indeterminate payload
    boundaries. Trino's S3 client always sets it.
  - **Per-part cap = 5 GiB** (`SHIM_MAX_PART_BYTES`). Anything
  larger → 501 `EntityTooLarge` before any byte hits the SDK.
  Mirrors AWS's own per-part ceiling.
- **Native bulk `DeleteObjects`.** `S3Origin::delete_objects_bulk`
drops the 32-way single-key fan-out and issues one
`delete_objects()` SDK call per chunk of 1000 keys (S3's hard
cap; chunking is transparent — callers can pass any vector size).
  - **Idempotency preserved.** S3 returns `NoSuchKey` per-row in the
  response `Errors` envelope when a key is already gone; we coerce
  that single code to "deleted" in `BulkDeleteOutcome` so
  Iceberg's `RemoveOrphanFiles` retries stay safe. Other error
  codes (`AccessDenied`, etc.) bubble up per-row in the verbose
  response and as the `outcome="partial"` series in metrics.
  - **Wire trade-off vs preview-6.** For 1000 keys, preview-6 sent
  ~32 SDK round-trips (32-way fan-out, 1000/32 ≈ 31 batches);
  preview-7 sends 1. Tail latency on bulk-delete-heavy operations
  (`DROP TABLE`, `EXPIRE_SNAPSHOTS`) drops from ~connection-pool-
  recycle-bound to one S3 round-trip.

### Cache-invalidation contract (preview-7)

Unchanged from preview-6. Streaming `UploadPart` body bytes never
hit the shim's caches (parts are intermediate state — only the
completed object has a cacheable identity), and bulk
`DeleteObjects` invalidations still run per-key on the
`error=None` rows after the SDK call returns. `record_missing` is
not run for `outcome="partial"` rows so we don't lie about cache
state on per-key AccessDenied.

### Metrics

`shelf_origin_request_seconds{op="delete_objects",outcome=…}` is the
new observability surface — outcome is `ok | partial | error`. The
upload-part span gains a `streaming = true` attribute so a Tempo
trace makes the streaming path visually distinct from a buffered
preview-6 trace if a side-by-side replay ever happens.

### Tests

- `shelfd/tests/it_shim_write_v2.rs` — 4 new cases (13 total in
the suite):
  - `upload_part_streams_large_body` — 32 MiB single-part round-trip
    - `head_object` size assertion. Proves the streaming path
    survives bodies that span many TCP reads.
  - `upload_part_rejects_oversized_content_length_header` — raw TCP
  request claiming 6 GiB CL → 501 `EntityTooLarge` before any
  body bytes flow.
  - `bulk_delete_handles_many_keys` — 50-key bulk delete via the
  new native SDK path; every key reflected as `<Deleted>`, no
  `<Error>`, all gone upstream.
  - `bulk_delete_is_idempotent_on_missing_keys` — bulk delete of
  never-existed keys returns 200 with no `<Error>` rows.

What is **not** in preview-7 (deliberately deferred):

- v1 `ListObjects` (`list-type` ≠ 2) — same rationale as preview-6.
- Bulk-delete chunking past 1000 keys is transparent (the impl
loops chunks), but the integration tests don't push past 1000;
the chunk-loop arithmetic + the `Errors` mapping are covered by
the 50-key happy path + the idempotent-on-missing test.
- Inbound SigV4 — same posture as v1/v2; the shim continues to
trust the in-cluster network.

## SHELF-21e-v2 — preview-8

Closes the Foyer LODC overflow gap that the SHELF-21e config-only
roll (`helm rev 16`, 2026-04-28 04:31 UTC — see
`shelfd/docs/runbooks/2026-04-shelf-1-oom.md`) did not actually
close. No new HTTP verbs, no chart shape changes outside a single
optional field, no memory-layout changes.
Rollout runbook: `docs/rollout-v1/shelfd-runbooks/2026-04-preview-8-rollout.md`.

### Why rev 16 was not enough

rev 16 set the three LargeEngineOptions knobs the preview-7 binary
already plumbed through:

```
flushers = 4                      # was 1
buffer_pool_size_bytes = 256 MiB  # was 16 MiB
submit_queue_size_threshold_bytes = 1 GiB
```

That bounds the **size** of the LODC pipeline. It does not bound
the **rate** at which shelfd tries to push admissions into it.
Under sustained ingress (Iceberg compaction + a Metabase-heavy
Tuesday), the read-side produces rowgroups faster than the
4-flusher × EBS gp3 drain pipeline can absorb. The submit queue
still fills to the 1 GiB threshold, Foyer emits

```
[lodc] submit queue overflow, new entry ignored
```

on every dropped entry, and DRAM eviction counts diverge from
disk write-back counts (the gap is exactly the dropped
admissions). RSS stays within budget this time — so the pod is
not OOM-killed — but cache hit-ratio on NVMe plateaus at a lower
value than the sizing math predicts.

### What changed (delta vs preview-7)

- **Foyer admission rate limiter** (`with_admission_picker`). Foyer
0.12.2 ships a built-in `RateLimitPicker<K>` (see
`~/.cargo/registry/.../foyer-storage-0.12.2/src/picker/utils.rs`)
implementing the `foyer::AdmissionPicker` trait via an internal
`RatedTicket` token bucket sized in bytes/sec. shelfd wires it
into `HybridCacheBuilder::with_admission_picker` in
`src/store.rs::FoyerStore::build_rowgroup_pool` when
`cache.pools.rowgroup.diskCache.admissionBytesPerSec` is set,
leaves Foyer's default `AdmitAllPicker` otherwise. DRAM behaviour
is unchanged — the picker gates only the disk admission seam, so
hot keys still live in DRAM even when the bucket is exhausted.
  - **Why built-in, not hand-rolled.** The initial design called
  for a hand-rolled token-bucket `AdmissionPicker` in a new
  `src/foyer_admission.rs` module. That was made obsolete by
  `foyer-storage-0.12.2`'s `RateLimitPicker`, which is exactly
  the token-bucket shape we wanted and integrates with Foyer's
  own `Statistics` (`cache_write_bytes()`) instead of tracking
  admission bytes in a shelfd-owned metric. Using the built-in
  keeps the shelfd crate graph one dep smaller and means this
  module gets free improvements when we bump Foyer.
- **Config plumbing.** `RowGroupDiskCacheConfig.admission_bytes_per_sec:
Option<u64>` in `src/config.rs`; chart value
`cache.pools.rowgroup.diskCache.admissionBytesPerSec` in
`charts/shelf/values.yaml`; rendered as `admission_bytes_per_sec:`
under the existing `disk_cache:` block by
`charts/shelf/templates/configmap-shelfd.yaml` using the same
`{{- with ... }}` pattern as the sister knobs.

### Cache-invalidation contract (preview-8)

Unchanged from preview-7. The picker only decides whether an
admitted entry gets written to NVMe; it has no interaction with
the HEAD-LRU or per-pool `FoyerStore::invalidate` paths used by
the shim's write-passthrough contract.

### Recommended rate + why

Production recommendation: **`209_715_200` B/s (~200 MiB/s)**.

EBS gp3 volumes on the alluxio NodePool (`*.4xlarge`) provision
at the stock **250 MiB/s** baseline throughput. At 200 MiB/s the
admission picker leaves ~50 MiB/s of headroom for:

1. Foyer's own region-reclaim reads (the LODC compacts regions
back into the device; those reads contend with our admission
writes on the same gp3 pipeline).
2. Occasional burst bias when `alluxio-sa` IRSA-authenticated S3
multi-part uploads from the shim fan out writes to the same
EBS volume on the way to origin (rare in practice — the shim
writes live on the same pod but the data is in a separate
path).

If `shelf_disk_bytes_used` ramps *slower* than the DRAM pool's
capacity-eviction rate after the roll, tune the limiter *up*
(250 MiB/s or more — but only by observing for a full 24 h soak
at each step). If `[lodc] submit queue overflow` lines return,
tune *down*.

### Tests

- `shelfd/src/config.rs` gains
`rowgroup_disk_cache_admission_defaults_to_none` and
`rowgroup_disk_cache_admission_accepts_set_value` — unit tests
that an unset chart value parses (backward-compat with previous
values.yaml) and a set value round-trips into the struct that
`build_rowgroup_pool` hands to Foyer.
- We deliberately do not add an integration test that exercises
the Foyer pipeline under a real load. Foyer's own test suite
covers `RateLimitPicker` behaviour; our plumbing test is just
"does the option reach the builder". The next 24 h soak on the
alluxio cluster is the real integration test.

What is **not** in preview-8 (deliberately deferred):

- **Per-pool override for the metadata pool.** Metadata pool is
DRAM-only (no LODC), so the knob is meaningless there and we
kept the field on `RowGroupDiskCacheConfig` only.
- **Dynamic re-tuning.** The limiter is only read at pool build
time; changing `admissionBytesPerSec` requires a rolling restart.
The HybridCache admission picker cannot be swapped at runtime
without tearing down and rebuilding the cache, which also
means losing the NVMe working set — not acceptable online.
- **Exporting the picker's decision as a metric.** Foyer does
not expose a hook for this today. Tracked as a follow-up upstream;
short-term proxy is `shelf_disk_bytes_used` slope vs
`shelf_memory_evictions_total` slope.

## Original spec (kept for reference; multipart parts apply to SHELF-21b)

## Problem

The shim today (`shelfd/src/s3_shim.rs:46`) registers exactly two
verbs:

```rust
.route("/:bucket/*key", get(handle_get_object).head(handle_head_object))
```

Anything else returns axum's default 405. That assumption was sound
when the shim only had to serve Metabase admin reads on rep-2, but
it falls down the moment a write-capable Trino replica points its
cdp catalog at the shim:

- dbt iceberg-maintain INSERTs.
- CTAS / INSERT / UPDATE / DELETE / MERGE.
- Iceberg metadata commits (every write produces new manifest +
snapshot files).
- `ALTER TABLE … EXECUTE optimize / expire_snapshots / remove_orphan_files`.

Trino's S3 filesystem applies `s3.endpoint` to every verb — there is
no per-verb endpoint split — so the only ways out are (a) keep the
catalog endpoint pointing at real S3 (loses the read cache), or (b)
make the shim transparently proxy non-cacheable verbs to the real
origin. (b) is the long-term answer.

## Design

### Surface to add


| Verb                                        | Trino S3 path                            | What the shim does                                                                                                                                                      |
| ------------------------------------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PUT /:bucket/*key`                         | `S3OutputStream.putObject` (single-shot) | Stream body → `S3Client::put_object` against the configured origin. On 200, evict the key from Foyer (both pools) and HEAD-LRU. Return upstream's status + ETag header. |
| `POST /:bucket/*key?uploads`                | multipart init                           | Forward to `create_multipart_upload`. Return upstream's `<UploadId>` XML.                                                                                               |
| `PUT /:bucket/*key?partNumber=N&uploadId=…` | upload part                              | Forward to `upload_part`. Return upstream's ETag.                                                                                                                       |
| `POST /:bucket/*key?uploadId=…`             | multipart complete                       | Forward to `complete_multipart_upload`. On 200, evict key.                                                                                                              |
| `DELETE /:bucket/*key?uploadId=…`           | multipart abort                          | Forward to `abort_multipart_upload`.                                                                                                                                    |
| `DELETE /:bucket/*key`                      | `DeleteObject`                           | Forward to `delete_object`. On 204, evict key.                                                                                                                          |
| `POST /:bucket?delete`                      | `DeleteObjects` (bulk)                   | Parse the XML list, fan out individual deletes (or forward as-is and parse the response), evict each key on success.                                                    |


Out of scope for v1 (raise as follow-ups if a workload needs them):

- `ListObjectsV2` (`GET /:bucket?list-type=2`) — Trino's S3 filesystem
uses HEAD + ListObjectsV2 for directory walks. dbt does not need
it for the failing INSERT path, but Iceberg's `RemoveOrphanFiles`
does. Tracked separately as SHELF-21a.
- Bucket-level operations (`GET /:bucket?location`, ACLs, etc.).
- SigV4 authentication on the inbound shim — current shim is
unauthenticated and trusts the in-cluster network. Continue that
posture; this is not a security regression.

### Cache-invalidation contract

A successful write **must** invalidate caches for the affected key
before returning 2xx to the client, otherwise a subsequent GET
through the shim could serve stale data:

```rust
async fn invalidate(state: &ServerState, bucket: &str, key: &str) {
    let storage_key = key_from_tuple(bucket, key);
    state.head_lru.invalidate(&storage_key);
    state.store.invalidate(&storage_key, Pool::RowGroup).await;
    state.store.invalidate(&storage_key, Pool::Metadata).await;
}
```

Notes:

1. `FoyerStore::invalidate` exists today via the underlying
  `HybridCache::remove`. If it doesn't, add the thinnest possible
   wrapper.
2. Invalidate **before** returning success so a same-client
  read-after-write sees the new bytes. The cost is one extra
   Foyer `remove` per write; writes are rare relative to reads.
3. For `DeleteObjects` (bulk), invalidate each key in the response's
  `<Deleted>` list — not the request list — so partial successes are
   handled correctly.

### Body streaming

Iceberg parquet files can be hundreds of MiB. Buffering in memory
would be a regression vs. real S3.

- Use `axum::body::Body` → `bytes::Bytes` stream → AWS SDK's
`ByteStream::from_body_1_x` so the upload streams without
buffering.
- The existing `origin.max_inflight` semaphore (already used by
reads) does not need to extend to writes — write rates are bounded
by Trino's own write parallelism, which is < 64 concurrent writes
per coordinator. But add a separate `origin.max_inflight_writes`
knob (`MembershipConfig`-style serde default) to be explicit.

### Metrics

Mirror the read-path metrics (`HITS_BY_TABLE_TOTAL`, etc.):

- `shelf_shim_writes_total{verb,result}` — counter, labels:
`verb={put,post,delete,multipart_complete,multipart_abort}`,
`result={ok,err}`.
- `shelf_shim_write_duration_seconds{verb}` — histogram.
- `shelf_shim_invalidations_total{pool}` — counter,
`pool={metadata,rowgroup,head_lru}`. Confirms the eviction is
actually firing.

### Failure modes & their mappings


| Origin response | Shim returns                                                                            |
| --------------- | --------------------------------------------------------------------------------------- |
| 2xx             | Forward status + headers (esp. `ETag`, `x-amz-version-id`). Body: forward XML if any.   |
| 3xx redirect    | Forward status + `Location:`. (Trino S3 client follows.)                                |
| 4xx             | Forward status + body. **Do not invalidate** the cache (write didn't land).             |
| 5xx             | Forward status + body. **Do not invalidate**.                                           |
| Network error   | Return 502 with S3 XML envelope mimicking AWS's `InternalError`. **Do not invalidate**. |


Critical: never evict the cache on a write that returned non-2xx.
Eviction on failure would degrade the cache for no reason and could
trigger thundering re-fetches.

### Security posture

The shim continues to trust the in-cluster network. The Trino S3
client provides AWS credentials via IRSA on the pod side; the shim
pod re-uses its own IRSA role (`alluxio-sa` in
`charts/shelf/values-prod.yaml`) to authenticate to S3. The
shim does **not** validate or forward incoming `Authorization`
headers — same as today. Write-passthrough does not change this.

## Implementation plan

1. **Origin trait extension** (`origin.rs`):
  - Add `put_object(bucket, key, body, content_length, content_type)  -> Result<PutObjectResponse>` with `ETag`.
  - Add `delete_object(bucket, key) -> Result<()>`.
  - Add multipart trio: `create_multipart_upload`,
  `upload_part`, `complete_multipart_upload`, `abort_multipart_upload`.
  - Mirror the existing `get_range`/`head` request-ID logging +
  timeout pattern. Reuse the existing `S3Client`.
2. **Shim handlers** (`s3_shim.rs`):
  - Extend the router:
  - Each handler invalidates after a successful origin call.
3. `**FoyerStore::invalidate`** (`store.rs`):
  - Confirm or add. Wrap `HybridCache::remove` for both pools.
4. **Tests** (`shelfd/tests/it_shim_write.rs` — new):
  - `put_object_round_trips_through_shim`: PUT to shim → GET to
   shim returns the same bytes (proves invalidation worked).
  - `put_object_5xx_does_not_invalidate`: mock origin returning 503
  → shim returns 503, the previously-cached value still serves a
  subsequent GET.
  - `delete_object_evicts_cache`: GET, then DELETE, then GET — the
  second GET hits origin and returns 404.
  - `multipart_complete_evicts_cache`: same pattern with the
  multipart trio.
  - `bulk_delete_partial_failure`: 3 keys requested, 2 succeed —
  only the 2 successful keys are evicted.
5. **Observability**: register the new metrics in `metrics.rs`,
  wire them through.
6. **Image**: `shelfd:0.1.0-preview-5` (multi-arch, push to GitLab
  registry). Bump `charts/shelf/values-prod.yaml`.
7. **Pre-rollout validation**:
  - Roll preview-5 to **rep-2 only** (the read-only path) and watch
   for 24 h. If GET hit ratio holds and no spurious invalidations
   fire, it's safe.
  - Then roll to rep-1 and rep-0.
8. **Re-cutover**: re-apply the equivalent of !17873 — point rep-1's
  `cdp.properties.s3.endpoint` at `shelf-1:9092`. dbt iceberg-maintain
   replays cleanly.

## Risk

- **Stale-read race**: client A writes, shim invalidates, client B
reads through the shim before the upstream PUT is fully visible
(S3 read-after-write consistency is strong as of 2020, but cross-
region replication can lag). Mitigation: invalidate **after** the
origin returns 200, not before; that way cache-miss reads go
through to S3 which gives strong-read-after-write semantics
itself.
- **Concurrent writes to the same key**: if two writers PUT the
same key simultaneously, the shim invalidates twice, which is
idempotent. Whichever PUT lands last wins on the origin — same as
direct-to-S3 today. No new failure mode.
- **Memory pressure from large multipart uploads**: addressed by
streaming (no buffering). Verify with a 5 GiB synthetic put in
the integration test.

## Out-of-scope rabbit holes (don't bikeshed)

- Caching writes on the way down (write-back). Adds complexity for
marginal benefit; Iceberg writes are rare relative to reads.
- ListObjectsV2 — separate ticket SHELF-21a.
- Per-bucket policy gates (`only forward writes for bucket X`).
Not needed; Trino's catalog config already restricts which buckets
reach the shim.
- Re-implementing SigV4 inbound. The shim is a trusted in-cluster
proxy.

## Estimate

- Origin extension: ~30 min (mostly boilerplate).
- Shim handlers + invalidation: ~90 min.
- Tests: ~60 min.
- Image build + push + chart bump: ~20 min.
- Validation on rep-2: 24 h soak before cutting over rep-1.

Total dev time: ~3.5 h. Wall-clock to re-cutover rep-1: ~24 h after
preview-5 lands on rep-2.