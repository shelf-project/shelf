# ADR 0034: Trino native predicate pushdown plugin — feasibility assessment

*Status: Proposed (2026-05-01)*
*Deciders: shelf-maintainers, trino-plugin-eng-1*
*Supersedes: none*
*Superseded-by: none*
*Related: ADR-0012 (Trino read-path strategy — endpoint swap, blob-cache SPI), ADR-0014 (page-index sidecar), ADR-0021 (bloom-aware footer admission)*

## Context

Today's read path is straightforward and (deliberately) narrow: Trino's native S3 client points at `shelfd:9092`, the SHELF-22 shim serves byte-range GET / HEAD / PUT / DELETE requests. The shim is signature-agnostic; it does not know what the bytes mean. From Trino's perspective, Shelf is an S3 endpoint that happens to be very fast on warm data.

That's good for correctness (zero new failure modes vs direct S3) and good for portability (any S3 client works). It leaves performance on the table in two specific places.

**Place 1: cross-pod peer-fetch hops.** SHELF-23 redistributes hot keys across the shelf-pool via HRW; a Trino worker hits its local same-region pool, which then peer-fetches from the pod that actually owns the key. The peer-fetch hop costs ~5 – 10 ms RTT on warm hits (intra-AZ same-cluster, same-pod-network). On the 22.1 % of rep-1 queries that carry equality-pushdown predicates against the hot column set (workspace memory, May 1 audit), that hop sometimes runs per row group — measurable at p99.

**Place 2: misses that the cache could have anticipated.** Trino's optimiser already decides which row groups to scan (via Iceberg's column statistics + partition pruning). Shelf doesn't see that decision until the worker issues byte-range GETs in the order the optimiser produced them. A predicate-aware path could let Trino ask Shelf "do you have row groups matching `event_region=IN AND batch_id IN (...)`?" *before* the scan starts, and Shelf could answer from its own bloom-block index (ADR-0021) + page-index sidecar (ADR-0014) without serving any bytes — closer in shape to Starburst Warp Speed's index path, though not the same mechanism.

Both wins are marginal. The peer-fetch hop is 5 – 10 ms; the early-prune win on equality-pushdown queries is bounded by the existing scan-time savings the bloom blocks already produce. This ADR does **not** claim a step-change improvement; it claims a small, real, well-bounded latency win on a specific query shape, gated on weeks of upstream Trino work.

### What today's spike confirmed

The May 1 spike on Trino 481's `IcebergSplit.getAffinityKey()` SPI hook confirmed:

- `getAffinityKey()` exists on `IcebergSplit` and is called during split scheduling.
- The binding that wires `SplitAffinityProvider` into the optimiser's split scheduler is **gated on `fs.cache.enabled=true`** in `lib/trino-filesystem-manager/src/main/java/io/trino/filesystem/manager/FileSystemModule.java:124–135`. Native-S3 catalogs (Shelf's path) do not pick up the affinity provider out of the box.
- Decoupling the binding from `fs.cache.enabled` is a small upstream patch (~ a dozen lines in `FileSystemModule.java`) but lands on the upstream review timeline.

That confirms the lift exists but the path is gated on Trino-side work, not Shelf-side work.

## Decision

**Defer implementation to rc.8+.** This ADR captures the feasibility assessment that scopes the rc.8 ticket. Three paths are evaluated; the recommendation is **Path A with a parallel Path B prototype**.

### Path A — small upstream Trino PR, then a Shelf Java plugin

Submit a small PR to `trinodb/trino` decoupling `SplitAffinityProvider` binding from `fs.cache.enabled`. Once merged, ship a Shelf-side Java plugin that:

- Implements `SplitAffinityProvider` via the existing public SPI.
- Computes affinity keys aligned with shelfd's HRW key derivation: `affinityKey = HRW.preferredPodIndex(bucket, key)`.
- Registers via `getSplitAffinityProviders()` on `Plugin` (or whichever extension point the upstream PR lands at).

Trino's split scheduler then routes splits to the worker most likely to hit a local shelf pod first; the cross-pod peer-fetch hop fires only on KEDA-induced membership churn. Same key derivation as ADR-0011 means no consistency story to maintain.

| Dimension          | Estimate                                                                              |
| ------------------ | ------------------------------------------------------------------------------------- |
| Shelf-side effort  | ~ 1 engineer-week Java plugin + tests + Helm chart packaging                          |
| Trino-side effort  | ~ 0.5 engineer-week PR + review-cycle wait                                            |
| Upstream timeline  | 4 – 8 weeks for review + release (typical Trino cadence)                              |
| Maintenance burden | Low — public SPI, normal rev-by-rev compatibility                                     |
| Latency win        | 5 – 10 ms p99 on cross-pod hits — bounded                                             |

### Path B — shelfd-side admin HTTP hint, called by a Trino plugin before scan planning

A Trino plugin queries `shelfd /cache/granule-hints?table=foo&predicate=...` before issuing scan splits; shelfd's plugin returns a list of `(file_path, row_group_ordinal)` pairs that the bloom-block index + page-index sidecar predict are worth pruning. The plugin uses the hint to filter `IcebergSplit` candidates before splits leave the coordinator.

| Dimension          | Estimate                                                                              |
| ------------------ | ------------------------------------------------------------------------------------- |
| Shelf-side effort  | ~ 2 engineer-weeks new admin endpoint + predicate-encoding contract + tests           |
| Trino-side effort  | ~ 2 engineer-weeks plugin + smoke-test integration                                    |
| Upstream timeline  | None — the plugin is self-contained                                                   |
| Maintenance burden | Higher — the predicate-encoding contract is bespoke; Trino predicates evolve          |
| Latency win        | Variable — saves the shim hop entirely on filtered row groups, but only on the 22.1 % equality-pushdown cohort |

Path B has a higher Shelf-side surface and a bespoke contract; Path A leans on a public Trino SPI and Trino does the work it already does for filesystem-cache plugins. Path A is preferred for the production path; Path B is worth a small parallel prototype because it's the only path that doesn't depend on upstream review cadence.

### Path C — full Trino fork

**Explicit no.** Forking `trinodb/trino` to graft the affinity-provider binding directly into `IcebergModule` (or anywhere else) violates the same principle ADR-0012 rejected for the filesystem-factory case: connector-specific, obsoleted the day the upstream lands, maximum maintenance surface. We ship a plugin or we don't ship.

### Recommendation

Path A as the production path. Path B as a one-engineer-week prototype to de-risk the predicate-encoding contract and to give us a fallback if the upstream PR stalls. If the upstream PR merges in rev N and Path B's prototype shows an additional independent win on equality-pushdown queries (>= 10 ms p99 reduction beyond Path A), ship both.

### Cost-benefit vs the current shim approach

| Aspect                        | Today (SHELF-22 shim)                                | Path A (split affinity)                          | Path B (predicate hints)                              |
| ----------------------------- | ---------------------------------------------------- | ------------------------------------------------ | ----------------------------------------------------- |
| Cross-pod hop avoided         | No — peer-fetch fires on cross-pod hits              | Yes — local worker holds the key                 | Partially — only on filtered row groups               |
| Bytes moved on miss           | Full row group from origin via shim                  | Full row group from origin via shim              | Skip the row group entirely if pruned                 |
| Trino changes required        | None                                                 | Small upstream PR + plugin                       | Plugin only, no upstream                              |
| Workload coverage             | Universal                                            | Universal (every cache hit)                      | Equality-pushdown only (~22 %)                        |
| Latency target                | Baseline                                             | -5 to -10 ms p99 on cross-pod hits               | -5 to -50 ms p99 on filtered scans                    |
| Failure mode                  | None — direct S3 fall-through is the existing path   | Plugin disabled → default Trino scheduling       | Hint endpoint timeout → default scheduling             |

The current shim approach is **strictly correct**; the proposed paths are **strictly faster on a specific cohort**. None of these proposals replace the shim; they refine the path that runs on top of it.

## Alternatives considered

### Direct integration with Trino's split scheduling via reflection / classloader injection

Rejected. ADR-0012 already rejected this for the filesystem-factory case; the same principles apply (fragility, version-bump breakage, "one obvious way" violation).

### Wait for trinodb/trino#29184 (blob-cache SPI) to subsume this

Rejected as an alternative. #29184 lands the blob-cache SPI, not affinity-provider routing. Even when that PR merges (ADR-0012 Phase 2), the cross-pod peer-fetch hop still exists — Shelf is still a remote cache to Trino, and Trino still doesn't know which Shelf pod owns which key. Path A is orthogonal to #29184 and complementary.

### Skip this work entirely; declare the shim path "good enough"

Rejected as a no-investment baseline but kept as an honest fallback. The shim path is good enough for the v1.0.0 SLO. If rc.8+ bandwidth is consumed by Tier-A stability work and this ADR's recommendation slips to rc.9, that is fine — the shim path doesn't regress.

## Consequences

- **rc.7 deliverable is this ADR + a feasibility-tracking issue on the OSS repo.** No Shelf-side code in rc.7. No Trino-side PR in rc.7.
- **rc.8+ implementation lift.** ~ 3 – 4 engineer-weeks total if both Path A and Path B prototype run in parallel, gated on the upstream Trino PR landing within the rc.8 horizon. If the upstream PR stalls, Path B alone can ship for ~ 2 engineer-weeks Shelf-side + plugin.
- **Marginal win, well-bounded.** The latency improvement is 5 – 10 ms p99 on cross-pod hits, plus a per-prune saving on the equality-pushdown cohort. It is not a step change. The case for shipping it is operational efficiency on the warm-cache path, not a workload-class unlock.
- **Unblocks future Warp-Speed-style comparisons honestly.** Workspace memory's "realistic target: get within ~ 2 × of Warp Speed's selective-query p99" framing improves materially with predicate hints in place — without inventing a new index format Shelf does not maintain.

## Triggers for promotion to Accepted

This ADR stays at `Proposed` until **all three** hold:

1. The upstream `SplitAffinityProvider`-binding decoupling PR is merged to `trinodb/trino:master` and present in a tagged release.
2. rc.8 has Tier-A bandwidth to absorb 3 – 4 engineer-weeks of plugin work.
3. The SHELF-42 A/B tag rollup (D2 in the rc.7 roadmap) is shipping per-tag hit-ratio panels — without that, attributing the latency win to this work vs concurrent levers is impossible.

## Verification (rc.8+ scope)

- Path A unit tests: `affinityKey` derivation matches shelfd's HRW for a golden vector set spanning 100 random `(bucket, key)` tuples.
- Path A integration test: 4-pod shelf-pool + 4 Trino workers; assert each worker's split-scan-byte-from-peer counter drops > 80 % vs baseline on a synthetic warm workload.
- Path B unit tests: predicate-encoding round-trip (Trino `TupleDomain` → Shelf wire format → granule-hints response).
- Path B integration test: cold cache + bloom-block-populated + page-index-populated; assert `shelf_granule_hint_pruned_total` climbs and the corresponding splits never appear in `shelf_misses_total`.
- A/B soak: 24 h on a single canary replica, with `X-Shelf-Tag: rc8-pushdown` set on a fraction of queries via the catalog-side rule. Hit-ratio + p99 must improve vs the untagged control without regressing miss-cost.

## References

- [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184) — unified blob-cache SPI (orthogonal but cited in ADR-0012 Phase 2).
- [Trino `SplitAffinityProvider` SPI](https://github.com/trinodb/trino/blob/master/core/trino-spi/src/main/java/io/trino/spi/connector/SplitAffinityProvider.java) — the public extension point Path A uses.
- ADR-0012 — Trino read-path strategy (endpoint swap, then blob-cache SPI).
- ADR-0014 — page-index sidecar (the data Path B leans on).
- ADR-0021 — bloom-aware footer admission (the data Path B leans on).
- Workspace memory entry on the May 1 `getAffinityKey()` spike (Verdict B: gated on `fs.cache.enabled=true`).
- Workspace memory entry on rep-1 workload distribution (22.1 % equality-pushdown cohort).
