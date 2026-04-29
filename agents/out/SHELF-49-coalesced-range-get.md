# SHELF-49: Coalesced range-GET in `s3_shim`

**Status:** Draft
**Tier:** B
**Estimated effort:** S
**Depends on:** none
**Blocks:** none

> **Sequencing note: do not start before SHELF-23 lands.** This ticket modifies `shelfd/src/s3_shim.rs` (and the sibling submodule under `shelfd/src/s3_shim/`), which is currently in-flight on `shelf-23-peer-fetch`. Resume after SHELF-23 merges.

## Problem (OSS-cited)

Trino's native S3 client (`io.trino.filesystem.s3.S3InputFile`) doesn't do vectored reads — there's a niche, but [Hadoop S3A vectored I/O (HADOOP-18103)](https://issues.apache.org/jira/browse/HADOOP-18103) and [AWS Analytics Accelerator Library (AAL)](https://github.com/awslabs/analytics-accelerator-s3) (Apache 2.0) already do this well; Iceberg PR [apache/iceberg #12299](https://github.com/apache/iceberg/pull/12299) integrates AAL. For Shelf's S3 shim, the niche is real on small-range-heavy Iceberg metadata reads where Trino issues many sub-128-KiB GETs back-to-back.

## Goal

When the S3 shim sees N intra-file range-GETs whose gaps are small (< 128 KiB), it coalesces them into a single origin GET, splits the response back to the callers, and saves N-1 round-trips.

## Approach

Internal optimisation in `shelfd/src/s3_shim.rs::handle_get_object`. Maintain a small per-`(bucket, key)` coalescing window (default 5 ms) keyed by the in-flight request's `(etag, range)`:

1. On incoming GET, compute the `(bucket, key, range)` and check the coalescing window for adjacent or near-adjacent in-flight GETs.
2. "Near-adjacent" = gap < `coalesce.max_gap_bytes` (default 128 KiB) AND the merged range size ≤ `coalesce.max_merged_bytes` (default 8 MiB) AND the fill ratio (`useful_bytes / merged_bytes`) ≥ `coalesce.min_fill_ratio` (default 0.25).
3. If criteria met, the second request *waits* on the merged inflight; on response, both callers receive their slice from the buffered bytes.
4. Bytes are inserted into the cache exactly once (under the merged key) per content-key rules; the per-caller slice is *also* inserted under its own key (so future independent reads still hit the cache).

Implementation uses a `DashMap<(Bucket, Key), CoalesceState>` where `CoalesceState` tracks `(merged_range, oneshot_subscribers)`. The window closes when (a) max-fill reached, (b) max-wait reached, or (c) merged size cap hit. After window close, exactly one origin GET fires.

Alternative considered: **vendor AAL** (Apache 2.0) instead of re-rolling. AAL is Java; Rust port would be substantial. Recommend hand-roll for v1, document the AAL alternative in `shelfd/docs/design-notes/SHELF-49-coalesced-rangeget.md`, revisit when [iceberg #12299](https://github.com/apache/iceberg/pull/12299) ships and the upstream behaviour is stable.

Metrics:
- `shelf_shim_coalesced_groups_total`
- `shelf_shim_coalesced_requests_saved_total`
- `shelf_shim_coalesce_filled_bytes_total`
- `shelf_shim_coalesce_wasted_bytes_total` (gap bytes inside the merged range that no caller consumed)

Configuration in `shelfd/src/config.rs`:
- `s3_shim.coalesce.enabled` (default true)
- `s3_shim.coalesce.max_gap_bytes` (default 128 KiB)
- `s3_shim.coalesce.max_merged_bytes` (default 8 MiB)
- `s3_shim.coalesce.min_fill_ratio` (default 0.25)
- `s3_shim.coalesce.max_wait_ms` (default 5 ms)

## Acceptance criteria

- [ ] Two adjacent range-GETs (gap < 128 KiB, fill ≥ 25 %) issued within 5 ms result in exactly 1 origin GET (verified via `shelfd/tests/it_shim_coalesce.rs`).
- [ ] Disabled mode (`s3_shim.coalesce.enabled=false`) is byte-identical to the current SHELF-22 behaviour (regression-tested against existing `it_s3_shim::*` tests).
- [ ] Two distant range-GETs (gap > 128 KiB) result in 2 origin GETs.
- [ ] Per-caller response bytes are byte-identical to the non-coalesced path (correctness invariant).
- [ ] Quantitative gate: on a synthetic Iceberg manifest-list read (12 small adjacent GETs), origin GET count drops from 12 to ≤ 3, and wall time drops by ≥ 40 %.
- [ ] Unit tests ≥ 12 cases (adjacent / distant / partial-overlap / mid-stream cancel / over-cap / under-fill).
- [ ] Metrics: each of the four counters increments correctly under fixture workloads.

## Out of scope

- Vectored reads at the read-from-cache path (this ticket is the shim path only).
- AAL vendoring (documented as an alternative; not built).
- Cross-file coalescing (file boundaries respected).
- Async pipelining of multiple merged windows (one in-flight merge per `(bucket, key)`).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Latency hit on isolated requests waiting 5 ms for a coalescing partner | `max_wait_ms` is small (5 ms default); the coalescing window only delays *if there is already an in-flight adjacent GET*; standalone requests fire immediately. |
| Memory blow-up on `DashMap` of in-flight merges | Bounded by `max_merged_bytes × max_concurrent_merges` (default 8 MiB × 256); cap enforced. |
| Wasted bytes if fill ratio drops between window-open and window-close | `min_fill_ratio` re-checked at window close; degenerate cases issue separate GETs. |
| Cache key drift: do we cache the merged blob or the per-caller slices? | Cache the per-caller slices (content-keys map to caller-visible ranges); merged blob is transient. |

## Test plan

- Unit tests: window opening / closing, gap detection, fill-ratio enforcement, max-wait timeout, disabled-mode pass-through.
- Integration tests: `shelfd/tests/it_shim_coalesce.rs` against MinIO with synthetic adjacent-range workloads; assert origin GET count + wall-time reduction.
- Correctness regression: re-run all existing `it_s3_shim::*` tests with `coalesce.enabled=true`; assert byte-identical responses.
- (If applicable) docker compose smoke: SHELF-12 with coalesce on; assert `shelf_shim_coalesced_groups_total > 0` after the 10-query smoke.

## Open questions

- Should coalescing apply to `HEAD` requests too? Recommend no; HEAD is already cheap and the head-LRU cache covers it.
- Per-tenant disable? Recommend follow-up if SHELF-48 Phase 2 demands it.
- Default `min_fill_ratio` 0.25 — empirical or tuneable? Default conservative; SHELF-38 `tune` can re-recommend per-cluster.
