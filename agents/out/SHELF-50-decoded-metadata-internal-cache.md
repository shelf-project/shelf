# SHELF-50: Decoded metadata as in-process `Arc<ManifestFile>` cache

**Status:** Draft
**Tier:** B
**Estimated effort:** M
**Depends on:** none
**Blocks:** none

## Problem (OSS-cited)

The original idea was three new network pools (`pool.footer` / `pool.manifest` / `pool.puffin`) serving pre-decoded blobs. Verified against Trino 480: **no SPI lets a plugin hand a pre-parsed `ParquetMetaData` to the Iceberg reader**. `ParquetPageSourceFactory` always calls `MetadataReader.readFooter(TrinoInputFile)`. Same SPI gap as ADR-0012. Internal acceleration is still on the table — Shelf's own prefetch worker re-parses the same manifests / footers many times per second when scanning Phase 2b sort-order awareness, and an in-process `Arc<ManifestFile>` cache eliminates that CPU.

## Goal

A pure-internal `dashmap<key, Arc<ManifestFile>>` and `dashmap<key, Arc<ParquetMetaData>>` cache lives inside `shelfd` and is consumed exclusively by Shelf's prefetch worker (and SHELF-46 / SHELF-47 / SHELF-52 advisor recommenders). No external SPI surface; no new network pool.

## Approach

New module `shelfd/src/decoded_meta.rs`. Two type-parameterised caches:

```rust
pub struct DecodedMetaCache {
    pub manifests: foyer::Cache<KeyBytes, Arc<ManifestFile>>,        // iceberg-rust
    pub parquet_footers: foyer::Cache<KeyBytes, Arc<ParquetMetaData>>, // arrow-rs / parquet crate
}
```

DRAM-only; Foyer SIEVE policy; capped at `decoded_meta.max_bytes` (default 256 MiB total, split 50/50). Eviction by entry size weighter (use `mem::size_of_val` plus the deserialised payload's `heap_size_of_children` if available; conservative bytes-overhead estimate otherwise).

Population flow:
- On a successful `parquet_meta::parse_footer(bytes)` call, populate the entry under the same content-key the bytes are stored under.
- On `manifest_file::parse(bytes)` similarly.

Consumption flow:
- `shelfd::peer::prefetch_worker` looks up the decoded metadata before re-parsing.
- `mv_registry`, `compaction_watcher` (SHELF-45), and the SHELF-52 advisor query this cache when they need to inspect manifests or footers.

Crucially: this cache is *internal-only*. The HTTP / gRPC / S3-shim API surface is unchanged. The cache is invalidated by content-key naturally (same SHELF-04 keys; new ETag = new key).

Metrics:
- `shelf_decoded_meta_hits_total{kind=manifest|parquet_footer}`
- `shelf_decoded_meta_misses_total{kind}`
- `shelf_decoded_meta_evictions_total{kind}`
- `shelf_decoded_meta_bytes{kind}` gauge

Configuration in `shelfd/src/config.rs`:
- `decoded_meta.enabled` (default true)
- `decoded_meta.max_bytes` (default 256 MiB)
- `decoded_meta.split_manifest_fraction` (default 0.5)

## Acceptance criteria

- [ ] On a synthetic prefetch-heavy workload (100 manifests parsed 10× each), CPU time spent in `manifest_file::parse` drops by ≥ 80 % vs disabled mode.
- [ ] Memory footprint stays under `decoded_meta.max_bytes` (verified by gauge + a runtime allocator check).
- [ ] Disabled mode (`decoded_meta.enabled=false`) is a no-op and the prefetch worker re-parses on every hit (regression test against current behaviour).
- [ ] Quantitative gate: warm `Arc<ManifestFile>` lookup p99 ≤ 10 µs (in-process DRAM); warm `Arc<ParquetMetaData>` p99 ≤ 20 µs.
- [ ] Cache-key drift test: changing the underlying ETag invalidates the decoded entry (verified by feeding two ETags, asserting two cache slots).
- [ ] Unit tests ≥ 10 cases (population, eviction by size, disabled-mode no-op, key-drift, concurrent population race).

## Out of scope

- External / network SPI exposing decoded metadata to clients (verified infeasible against Trino 480 SPI).
- Puffin-payload decoded cache (revisit when [apache/iceberg #15311](https://github.com/apache/iceberg/pull/15311) lands).
- Persistence to NVMe (DRAM-only by design — re-parse on restart is fine).
- Cross-pod sharing (in-process only; cross-pod sharing is a re-parse).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Memory-unbounded growth from large footers | Per-entry size cap (default 16 MiB per `ParquetMetaData`); larger entries are not cached. |
| `iceberg-rust` / `arrow-rs` minor-version churn changes the decoded type | Pin both crate versions exact; quarterly bump cadence with regression tests. |
| Concurrent populate-then-read race | Foyer's built-in single-flight covers this for the bytes; for the decoded entry, use `Cache::insert_with_async` (or a `DashMap` + `tokio::sync::OnceCell` for the heavy parse). |
| `Arc` clone cost on hot path | Arc is fine; profile if needed; alternative is `tokio::sync::Arc<RwLock<T>>` only if mutation is desired (not in v1). |

## Test plan

- Unit tests: populate / look up / evict by size, disabled-mode no-op, key-drift, concurrent populate race, per-entry size cap.
- Integration tests: `shelfd/tests/it_decoded_meta.rs` boots the prefetch worker against a fixture S3, asserts CPU time drops on a re-parse-heavy workload.
- Bench: `cargo bench` micro-benchmark for `Arc<ParquetMetaData>` lookup vs re-parse on a 32-row-group footer.
- (If applicable) docker compose smoke: SHELF-12 + this cache on; assert `shelf_decoded_meta_hits_total > 0` after warm queries.

## Open questions

- Should the decoded entry be stored alongside the byte entry in the same Foyer cache (single key, two payloads) or in a parallel cache? Recommend parallel — simpler eviction policies.
- Default split `manifest=128 MiB / parquet_footer=128 MiB` — too generous on `parquet_footer`? Default; SHELF-38 `tune` can re-recommend.
- Should we also cache decoded `MetadataFile` (Iceberg `metadata.json`)? Recommend yes if measurement justifies; v1 covers manifests + parquet footers.
