# Shelf upstream Trino integration strategy

How and when Shelf engages with the Trino OSS project to land native blob-cache integration upstream — instead of staying perpetually on the S3-endpoint-swap path.

> **Pivotal fact.** [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184) is a live DRAFT pull request — authored by `@wendigo`, force-pushed Apr 27–28 2026 — introducing the exact blob-cache SPI that Shelf needs. Shelf's strategy is not "wait years for an SPI"; it's "engage on this PR now while the API is still malleable, then ship `plugin/trino-blob-cache-shelf/` the day it merges."

## What Trino exposes today (verified against `main`)

This table reflects the live `core/trino-spi/src/main/java/io/trino/spi/Plugin.java` and surrounding files on the date this doc was last updated. Re-verify before any actual upstream commit; the SPI surface evolves quickly.

| Surface | State today | What it means for Shelf |
|---|---|---|
| `Plugin.getFileSystemFactories()` | Does not exist in public SPI | Cannot register a `TrinoFileSystemFactory` from a plugin JAR today |
| `Connector.getTrinoFileSystemFactory()` | Does not exist | Same |
| `Plugin.getBlobCacheManagerFactories()` | Proposed in #29184, not merged | The right entry point Shelf targets |
| `ConnectorContext.getCacheFactory()` | Proposed in #29184, not merged | Per-connector cache wiring |
| `EventListenerFactory` (`QueryCreatedEvent`, `QueryCompletedEvent`) | Exists; Shelf already uses it for prefetch | Sufficient for file-level prefetch |
| `SplitCompletedEvent` | Does not exist | Row-group prefetch via observation is structurally unavailable |
| Native Alluxio integration | `lib/trino-filesystem-alluxio/` (filesystem backend) + `lib/trino-filesystem-cache-alluxio/` (worker-local byte cache, gated on `fs.cache.enabled=true`) | Shelf would land alongside as `plugin/trino-blob-cache-shelf/` after #29184 |

## What #29184 actually adds

A new SPI package `core/trino-spi/src/main/java/io/trino/spi/cache/` introducing:

- `BlobCache` — interface with `get(CacheKey, BlobSource)`, `invalidate(CacheKey)`, `invalidate(Collection<CacheKey>)`
- `BlobCacheManager` — interface
- `BlobCacheManagerFactory` — interface with `name()` (or `getName()` — see naming-inconsistency CodeRabbit thread) and `cacheTier()`
- `BlobSource` — interface (lazy-vs-eager loading semantics still under review)
- `Blob` — interface
- `CacheKey` — type (shape under discussion; see Shelf's [SPI feedback](./upstream/29184-spi-feedback.md))
- `CacheTier` — enum `MEMORY | DISK`

Plugin entry point:

- `Plugin.getBlobCacheManagerFactories()` returns the factory set
- `ConnectorContext.getCacheFactory()` exposes per-connector cache wiring
- `cache-manager.config-files` property loads cache-manager configurations

Plugin module pattern:

- `plugin/trino-blob-cache-alluxio/` (relocated from `lib/trino-filesystem-cache-alluxio/`)
- `plugin/trino-blob-cache-memory/` (new in-memory reference implementation)

## How Alluxio actually integrates today

Worth dispelling a common misconception: Alluxio is **not** an S3-endpoint-swap upstream. Two real Trino modules live in `main`:

- `lib/trino-filesystem-alluxio/` — filesystem backend (alternative to S3 / GCS / Azure)
- `lib/trino-filesystem-cache-alluxio/` — worker-local byte cache, registered via internal Guice wiring under `fs.cache.enabled=true`

The S3-endpoint-swap that Shelf uses today is the **only** path available because the SPI for native cache plugins doesn't exist yet. After #29184 merges, the native path opens up and Shelf inherits the same shape Alluxio has.

## Right order vs wrong order

### Wrong order

1. Open a Trino PR introducing a Shelf-specific SPI right now → guaranteed rejection ("don't design SPIs for your own plugin")
2. Ship `plugin/trino-blob-cache-shelf/` against the current SPI → no SPI exists; the PR can't land

### Right order

| Step | Action | Effort | Dependency |
|---|---|---|---|
| 1 | Engage on #29184 — review the draft, give structured feedback on `BlobCache` / `CacheKey` / `CacheTier` / `invalidate` semantics ([feedback doc](./upstream/29184-spi-feedback.md)) | 1–2 days | None — do this now |
| 2 | Slack DM `@wendigo` introducing Shelf, offering to be the first OSS third-party plugin consumer ([draft](./upstream/wendigo-slack-dm.md)) | 30 min | None |
| 3 | Open small fix-up PRs against #29184 addressing CodeRabbit-flagged issues | ~50–200 LOC each | Maintainer goodwill from step 2 |
| 4 | Stub `ShelfBlobCacheManagerFactory` privately against the in-flight SPI ([sketch](../../clients/trino/docs/blob-cache-plugin-sketch.md)) | ~800–1200 LOC | None — can be done in parallel |
| 5 | Submit `plugin/trino-blob-cache-shelf/` within a week of #29184 merging | Stub already exists, just rebase | #29184 merged |
| Stretch | Standalone design issue for `SplitCompletedEvent` | 6–12 month champion-cycle | Out of scope for v1 |

## Governance hazards

Both surfaced during research; folding them into the strategy avoids known traps:

| Hazard | Source | How to avoid |
|---|---|---|
| Starburst-overlap concerns | [trinodb/trino#22827](https://github.com/trinodb/trino/issues/22827) | Position Shelf as Apache 2.0 OSS consumer; don't propose Warp-Speed-shaped APIs |
| External-cache PR going stale + closed | [trinodb/trino#24737](https://github.com/trinodb/trino/issues/24737) | Don't wait for a single maintainer to drive review; surface work in-band via Slack and PR comments |

## Cost-savings angle

Native plugin vs S3-endpoint-swap saves cost in four concrete ways:

| Cost lever | S3 swap (today) | Native plugin (post-#29184) |
|---|---|---|
| Extra in-pod HTTP hop on every read | Yes (Trino → shim → Foyer) | No (Trino's `TrinoInputStream` reads via the `BlobCache` SPI) |
| SigV4 signature CPU on Trino side | Wasted (Shelf ignores it) | Skipped — SPI bypasses SigV4 by construction |
| Worker-local affinity for cache hits | Lost (Trino treats Shelf as remote) | Restored via `SplitAffinityProvider` integration |
| Per-query metric attribution | Coarse (Shelf only sees S3 keys, not query IDs) | Fine — `BlobCache` API is called from a query-aware codepath |

Realistic ballpark: 5–15 % additional latency improvement on warm reads + correct per-query cost attribution for the SHELF-40 dollars-saved counter.

## What this strategy does NOT cover

- **GCS / Azure backends.** Same SPI, different origin client. Roadmap, not v1.0.
- **Trino blob-cache for non-Iceberg connectors.** SPI is connector-agnostic, but Shelf's prefetch listener is Iceberg-aware. Hive / Delta / Hudi support is a separate work item.
- **Spark / DuckDB / ClickHouse integration.** Out of scope — those engines have their own filesystem layers; Shelf's S3 shim already serves them.

## Tracking

Concrete progress against this strategy lives in the [tracking issue](https://github.com/shelf-project/shelf/issues) on `shelf-project/shelf` titled *track: native Trino integration via blob-cache SPI (trinodb/trino#29184)*. Update the checklist there as steps land; let this strategy doc drift as the ground truth changes.

## See also

- [docs/discovery/upstream/29184-spi-feedback.md](./upstream/29184-spi-feedback.md) — Shelf-specific design feedback on the proposed SPI shape
- [docs/discovery/upstream/29184-review-comment.md](./upstream/29184-review-comment.md) — paste-ready GitHub PR comment text
- [docs/discovery/upstream/wendigo-slack-dm.md](./upstream/wendigo-slack-dm.md) — paste-ready Slack DM draft
- [docs/discovery/upstream/contacts.md](./upstream/contacts.md) — quick reference for the key Trino maintainers and Slack channels
- [clients/trino/docs/blob-cache-plugin-sketch.md](../../clients/trino/docs/blob-cache-plugin-sketch.md) — Java sketch of `plugin/trino-blob-cache-shelf/` against #29184's API
