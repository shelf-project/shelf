# shelfd 0.1.0-preview-8 — rollout packet (SHELF-21e-v2)

**Status:** image built + pushed, **NOT** deployed.
**Predecessor:** preview-7 (SHELF-21c) shipped streaming `UploadPart`
and native bulk `DeleteObjects`; the `[lodc] submit queue overflow`
tunables (`flushers`, `buffer_pool_size_bytes`,
`submit_queue_size_threshold_bytes`) were already **wired in the
binary** but left unset in live values until SHELF-21e helm rev 16
(2026-04-28 04:31 UTC — see `2026-04-shelf-1-oom.md`). rev 16
stopped the OOM-kill but did **not** stop the overflow warnings:
sustained admission rate still exceeds the EBS gp3 drain ceiling
and Foyer keeps dropping admissions. preview-8 fixes that by
bounding the *rate* at the admission seam, not just the *size* of
the pipeline.

## What's in preview-8 (delta vs preview-7 + rev 16)

| Area | Change |
| --- | --- |
| `shelfd/src/store.rs::FoyerStore::build_rowgroup_pool` | New call: `.with_admission_picker(Arc::new(RateLimitPicker::<Key>::new(bps as usize)))` on the `HybridCacheBuilder`, wired only when `cache.pools.rowgroup.diskCache.admissionBytesPerSec` is set. Foyer 0.12.2's built-in `RateLimitPicker` (bytes/sec token bucket over the `AdmissionPicker` trait, implemented at `foyer-storage-0.12.2/src/picker/utils.rs`). When unset the builder keeps Foyer's default `AdmitAllPicker` — pre-preview-8 behaviour unchanged. |
| `shelfd/src/config.rs` | New optional field `RowGroupDiskCacheConfig::admission_bytes_per_sec: Option<u64>`. `#[serde(default)]` so values.yaml files without the field keep parsing. Two new unit tests: unset default is `None`, set value round-trips to `Some(209_715_200)`. |
| `charts/shelf/values.yaml` | New commented-out sample `cache.pools.rowgroup.diskCache.admissionBytesPerSec` with prose explaining the Foyer overflow relationship + production recommendation. |
| `charts/shelf/templates/configmap-shelfd.yaml` | New `admission_bytes_per_sec:` line under the existing `{{- with ... }}` disk_cache block, same wrap pattern as `flushers` / `bufferPoolSizeBytes` / `submitQueueSizeThresholdBytes`. |
| `shelfd/Cargo.toml` | Version override `0.1.0-preview-8`. Workspace stays at `0.1.0`; shelfctl unchanged. |
| `charts/shelf/Chart.yaml` | `appVersion: "0.1.0-preview-8"`. |
| `shelfd/docs/design-notes/SHELF-21-shim-write-passthrough.md` | New `## SHELF-21e-v2 — preview-8` section. |
| `shelfd/docs/runbooks/2026-04-shelf-1-oom.md` | Appended note: rev 16 was insufficient, preview-8 is the real fix. |

Foyer 0.12 exposes the admission picker on `HybridCacheBuilder` via
`with_admission_picker(Arc<dyn AdmissionPicker<Key = K>>)` (see
`foyer-0.12.2/src/hybrid/builder.rs:251`). The built-in
`RateLimitPicker::new(rate: usize)` at
`foyer-storage-0.12.2/src/picker/utils.rs:165` takes bytes/sec and
uses `foyer_common::rated_ticket::RatedTicket` internally — exact
shape SHELF-21e-v2 needed, so no hand-rolled picker in shelfd.

### Cache-invalidation contract (preview-8)

Unchanged from preview-7. The admission picker only decides whether
an admitted entry gets written to NVMe — DRAM and the HEAD-LRU and
the shim's per-pool `FoyerStore::invalidate` path are all
untouched.

## Pre-flight checks before rolling

1. **Verify the manifest list** (both amd64 + arm64 present):

   ```bash
   docker buildx imagetools inspect \
     registry.gitlab.com/penpencil-services/data/data-engineering/ranger/shelfd:0.1.0-preview-8
   ```

   Expect two platform digests (`linux/amd64`, `linux/arm64`) plus
   two `unknown/unknown` attestation manifests from buildkit.

2. **Confirm the chart renders** the new knob:

   ```bash
   cd /Users/aamir/trino/shelf
   helm template charts/shelf \
     --kube-version 1.30.0 \
     -s templates/configmap-shelfd.yaml \
     --set cache.pools.rowgroup.diskCache.admissionBytesPerSec=209715200 \
     | grep -A1 disk_cache
   ```

   Expect `admission_bytes_per_sec: 209715200` immediately under
   the existing `submit_queue_size_threshold_bytes:` line.

3. **Confirm the existing rev 16 settings are still in live values**
   (we do **not** un-do them; preview-8 adds on top):

   ```bash
   helm -n alluxio get values shelf | grep -A3 diskCache
   ```

   Expect `flushers: 4`, `bufferPoolSizeBytes: 268435456`,
   `submitQueueSizeThresholdBytes: 1073741824`.

## Helm upgrade

`--reuse-values` so rev 16's knobs stay in place; `--set` only the
new admission rate + image tag.

```bash
helm -n alluxio upgrade shelf /Users/aamir/trino/shelf/charts/shelf \
  --reuse-values \
  --set image.tag=0.1.0-preview-8 \
  --set cache.pools.rowgroup.diskCache.admissionBytesPerSec=209715200 \
  --description "SHELF-21e-v2 / preview-8 — Foyer admission rate limiter at 200 MiB/s"
```

Expect a new helm revision (likely rev 17). The configmap
should diff **exactly**:

- `image: …/shelfd:0.1.0-preview-7` → `…/shelfd:0.1.0-preview-8`
- new line `admission_bytes_per_sec: 209715200` under the
  existing `disk_cache:` block
- `app.kubernetes.io/version: "0.1.0-preview-8"` on every managed
  resource

No other diffs. If any other field moved, stop and investigate.

## Why 200 MiB/s

EBS gp3 on the alluxio NodePool (`*.4xlarge`) provisions at the
stock **250 MiB/s** baseline. 200 MiB/s leaves ~50 MiB/s for:

1. Foyer's region-reclaim reads (LODC compaction).
2. Occasional shim write-passthrough bursts that hit the same
   EBS volume on egress (the data path is S3, but buffer spill
   can touch local disk).

Tune up (250 MiB/s) after a 24 h soak at 200 MiB/s if
`shelf_disk_bytes_used` is growing slower than expected. Tune down
if `[lodc] submit queue overflow` lines come back.

## Rolling restart order

Same principle as the rev 16 roll (2026-04-28 04:31 UTC):
`OrderedReady + RollingUpdate` restarts highest-ordinal-first,
which historically left the hottest pod on old config the longest.
Override manually:

```bash
# pod that was hottest on the last Grafana "Pod RSS" panel check
# (usually whichever pod currently holds the largest DRAM working
# set — read it off `shelf_memory_bytes_used` before starting).
kubectl -n alluxio delete pod shelf-<hot>
kubectl -n alluxio rollout status pod shelf-<hot> --timeout=5m

# then the next-hottest
kubectl -n alluxio delete pod shelf-<next-hot>
kubectl -n alluxio rollout status pod shelf-<next-hot> --timeout=5m

# finally the coldest
kubectl -n alluxio delete pod shelf-<cold>
```

Watch `shelf-<hot>` for 10 min before restarting the next pod —
the post-restart cold-cache window is where this limiter has the
most obvious effect.

## Verification (first 30 minutes post-roll)

- **LODC overflow rate** — must drop to **zero**:

  ```bash
  kubectl -n alluxio logs shelf-0 --tail=-1 | grep -c 'submit queue overflow'
  kubectl -n alluxio logs shelf-1 --tail=-1 | grep -c 'submit queue overflow'
  kubectl -n alluxio logs shelf-2 --tail=-1 | grep -c 'submit queue overflow'
  ```

  Pre-fix baseline on `shelf-0` was thousands/sec during peak; the
  expected value after 5 min of steady load is `0`. A non-zero
  count in the first 2–3 minutes is OK (that's warm-up admissions
  that bypass the rate limiter because the bucket starts full); a
  non-zero count sustained past 5 min means the limiter is set too
  high — tune `admissionBytesPerSec` down.

- **Pod RSS** — steady-state ≤ 22 GiB, burst peaks ≤ 25 GiB on the
  Grafana **Shelf — Cache, Disk and Pods** dashboard. Identical
  budget to rev 16.

- **Disk bytes used** — `shelf_disk_bytes_used` should climb
  smoothly (no sawtooth from repeated submit-queue flushes/drops).

- **DRAM hit ratio** — `shelf_memory_hits_total` / (hits + misses)
  must stay within ±1 pp of the pre-roll value. DRAM behaviour is
  unchanged by design; a meaningful dip would indicate a
  regression, not a tuning issue.

## Rollback

`helm -n alluxio rollback shelf <previous-rev>` — the admission
picker only takes effect at pool-build time, so a rollback cleanly
tears down the Foyer HybridCache and rebuilds it with the old
`AdmitAllPicker`. Expect a ~5–10 min cold-cache window per pod on
the rollback restart; identical to any other shelfd restart.

If rollback is needed because of a crash-loop on preview-8, pin
the tag back to preview-7 explicitly to also drop the image bump:

```bash
helm -n alluxio upgrade shelf /Users/aamir/trino/shelf/charts/shelf \
  --reuse-values \
  --set image.tag=0.1.0-preview-7 \
  --set cache.pools.rowgroup.diskCache.admissionBytesPerSec=null \
  --description "SHELF-21e-v2 rollback — revert to preview-7 + AdmitAllPicker"
```

## Open questions for the user (before rolling)

1. Do we want to ship preview-8 to `values-prod.yaml` `image.tag`
   via the deployments-repo MR in one shot, or stage through a
   `--set` helm upgrade first and only flip the MR once the 24 h
   soak is clean? (The OOM runbook did the MR-last path — I'd
   do the same here unless you want it faster.)
2. Is the `workload=alluxio` NodePool still `*.4xlarge`? If the
   pool has been re-shaped to `8xlarge` since 2026-04-27, the gp3
   baseline is still 250 MiB/s per volume (volume size, not node
   size, drives the gp3 baseline) so the 200 MiB/s recommendation
   holds. But worth confirming before setting the knob.
