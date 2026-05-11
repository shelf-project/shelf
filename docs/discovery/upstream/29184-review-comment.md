# trinodb/trino #29184 — review comment from the Shelf project

**Status:** DRAFT — paste-ready content for the Shelf project's next response on `trinodb/trino#29184` (the blob-cache SPI authored by `@wendigo`). Created by T4 of the rc.9 plan after the May 4 v1.0.0 GA squash scrubbed the prior `docs/discovery/upstream/` tree (the v0.x copy of this doc is preserved at `agents/out/...` if a maintainer needs the diff history).

## Summary

We've been running a content-addressed, ETag-versioned blob cache (Shelf, Apache-2.0) in front of Trino 480 on Iceberg in production for ~30 days, serving 4 replicas of a Trino-on-EKS deployment behind the existing `s3.endpoint=` shim hook. The user-visible wins on the heaviest replicas are 50–62% read-latency reductions on Power-BI workloads and a step-function drop in `CLUSTER_OUT_OF_MEMORY` errors (off-loading scan buffers from worker JVMs to the cache pods). The shim is the one knob Trino exposes today; it's a workable production posture but introduces an extra HTTP hop that becomes the dominant overhead on synthetic same-region SF1 workloads.

We'd like to land Shelf as a `BlobCacheManagerFactory` provider against this SPI and would like to surface five design questions where the current draft is ambiguous from a content-addressed cache's perspective. We have no opposition to the existing `Plugin.getBlobCacheManagerFactories()` shape — these are scoping questions, not redesign requests.

## 5 design questions from a content-addressed cache implementation

### 1. `CacheKey` opacity for content-addressed caches

Shelf cache keys are `sha256(etag || u64_le(offset) || u64_le(length) || u32_le(rg_ordinal))` (ADR-0011 in our repo). We never look at the inner structure — the SHA-256 is opaque to the cache layer; Trino-side code that builds the key cannot peek inside.

Could the SPI document `CacheKey` as **structurally opaque** (only `equals` / `hashCode` semantically meaningful), or do downstream consumers (the bloom-cache analytics path in #22827, for example) need to reach in? If the latter, content-addressed caches need an escape hatch — perhaps `CacheKey.contentAddressedDigest()` returning `Optional<byte[]>`.

### 2. `CacheTier` breadth — DRAM, NVMe, RDMA-attached, ESS-style?

The draft groups everything under `CacheTier.MEMORY` and `CacheTier.DISK`. Shelf today has DRAM (Foyer) + NVMe (Foyer hybrid). Future: a third tier across pods via RDMA / cluster-shared NVMe is plausible (we ship cluster-wide peer-fetch via SHELF-23 today; an RDMA tier is a natural extension).

Suggest adding `CacheTier.REMOTE` with a documented latency expectation (μs vs the local-DRAM ns / local-NVMe μs / remote-S3 ms band). Otherwise a future Shelf RDMA tier has to declare itself as `DISK`, which loses the latency-class signal at the planner layer (cost-based optimizer might want to route reads differently for an μs vs ms remote tier).

### 3. `invalidate(CacheKey)` semantics on a content-addressed cache

For Shelf, `invalidate(key)` is structurally a no-op: a new ETag produces a new key, and the old key just becomes an unreachable orphan that Foyer evicts on capacity. This is the entire safety story for Iceberg snapshot transitions (commit → new metadata.json → new ETag → new key, no manual invalidation needed).

The draft's `BlobCacheManager.invalidate(CacheKey)` returning `void` implies invalidation is mandatory and observable. Could it be `Optional<Boolean>` (or a documented "best-effort, safe to no-op for content-addressed caches")? Otherwise content-addressed caches must implement a no-op that surfaces no error and silently succeeds — which is fine, but documenting the contract avoids a surprise for the next implementer.

### 4. `length(CacheKey)` metadata-only path

Trino-side code commonly wants to know the cached blob's length without fetching bytes (footer-discovery flow on Parquet, page-index sizing on bloom filters). If the SPI doesn't expose `length(key)` separate from `get(key)`, every length-only check pulls the full blob into Trino's heap.

Suggest `CompletableFuture<OptionalLong> length(CacheKey)` alongside `get(CacheKey)`. Shelf can serve this from its `head_lru_entries` LRU without touching the data plane.

### 5. Peer-fetch awareness — is the cache layer expected to know about cluster topology?

Shelf serves cluster-shared NVMe via peer-fetch (HRW-distributed; any pod can serve any key by racing peers against origin per SHELF-23). The draft `BlobCacheManagerFactory` doesn't expose a cluster-membership notion — caches are framed as worker-local.

If the SPI is going to stay worker-local, that's a fine design choice — Shelf would just present each pod as a separate `BlobCacheManager` instance and the cluster-fetch logic stays on our side, invisible to Trino. But if cluster-shared caches are a first-class concern (Alluxio, Starburst Warp Speed), it's worth surfacing `BlobCacheManager.knownPeers() : Set<URI>` so the planner can prefer affinity-routing splits to peer-locality (the `IcebergSplit.getAffinityKey()` path that landed in #29182). Otherwise we end up with two parallel locality stories.

## Reference impl: DataFusion 50.0.0 `FileMetadataCache`

A useful concrete prior-art is DataFusion 50.0.0's `FileMetadataCache` trait under `datafusion-execution`. We bring it up not as a "do it like DataFusion" suggestion but because it has working answers to the five questions above and would save a design round-trip:

| Question | DataFusion 50 `FileMetadataCache` answer |
|---|---|
| Q1 — key opacity | Key is `(object_path, last_modified, size)` — structurally transparent; consumers can read the path. Content-addressed caches in DataFusion adapt by using `last_modified` as the version axis (equivalent to Shelf's `etag`). Works. |
| Q2 — tier breadth | DataFusion ships an in-memory default impl; tier classification is left to the implementor. No `CacheTier` enum at the SPI layer — the SPI is single-tier and the implementor composes layers internally. **Simpler shape; might be the right call for #29184 v1.** |
| Q3 — invalidate semantics | `FileMetadataCache::remove(key) -> bool` returning whether the key was present. No mandatory observability — implementors can return `false` always (no-op safe). **This is a clean answer that #29184 could adopt.** |
| Q4 — length-only path | DataFusion stores per-file metadata blocks (Parquet `ParquetMetaData`, bloom filters, page indexes) as separate cache entries with their own keys, so length-of-data-block flows through the same `get()` path. No separate `length()`. **Less clean than what we'd want for Shelf; argues for the `length()` addition above.** |
| Q5 — peer-fetch awareness | None — DataFusion `FileMetadataCache` is process-local. Cluster-fetch is not modelled at the SPI. **Same answer the current #29184 draft has; both can defer this to a follow-on SPI.** |

The DataFusion shape is small enough to read end-to-end in one sitting and is a published, stable trait surface — useful as a reference even if `BlobCacheManagerFactory`'s ergonomics need to differ.

## What we're committing to

If the SPI lands roughly as drafted plus the Q3 / Q4 clarifications, Shelf will:
1. Ship a `ShelfBlobCacheManagerFactory` plugin against the first stable SPI tag.
2. Drop the `s3.endpoint=` shim posture from production (keep it shipped for users on older Trino versions).
3. Co-author the migration doc with `@wendigo` covering the Alluxio / Starburst Warp Speed / Shelf SPI provider matrix.

Shelf ships under Apache-2.0 with no commercial gating; we have no incentive to shape the SPI around any vendor's product.

## Anchors to the existing PR conversation

- Wendigo's BlobCacheManagerFactory draft commit (Apr 27–28 2026 force-push window in `trinodb/trino#29184`).
- `IcebergSplit.getAffinityKey()` from `trinodb/trino#29182` (merged in 481) — relevant to Q5 above. Today the binding is `NoopSplitAffinityProvider` unless `fs.cache.enabled=true` (verified in `FileSystemModule.java:124–135`); Shelf's path doesn't get affinity routing without a follow-on PR or a `fs.cache.enabled=true` workaround that double-caches.
- Workspace governance hazard log (#22827, #24737) — Shelf is happy to absorb review-cycles from those threads if it speeds the SPI to merge.

—
Draft owner: Shelf project. Will paste once one of us has a moment to finalize after the next #29184 force-push window.
