# Paste-ready review comment for trinodb/trino#29184

Copy the section between the `=== START ===` and `=== END ===` markers verbatim into a new review comment on https://github.com/trinodb/trino/pull/29184. ≤ 800 words. Tone: technical-peer, not vendor-pitch. The user must review and personally publish — this comment will appear under the user's GitHub identity on the upstream Trino project.

> **Pre-publish checklist.**
>
> 1. Verify the PR hasn't merged or been force-pushed in a way that invalidates the API names this comment cites — re-read the diff.
> 2. Confirm the user is OK with the Apache 2.0 / first-OSS-consumer framing.
> 3. Confirm the GitHub identity that will post this is the same one we want associated with Shelf's upstream presence (per `MAINTAINERS.md`, that's `@aamir306`).

=== START ===

Hi `@wendigo` and reviewers — thanks for taking on this SPI; the unified `BlobCacheManagerFactory` shape solves a class of integration problem that's currently blocking external cache projects from landing native Trino plugins.

I'm `@aamir306`, BDFL of [shelf-project/shelf](https://github.com/shelf-project/shelf) (Apache 2.0, v1.0 just released). Shelf is an Iceberg-native, row-group-granular read cache for Trino — it ships today as an S3-endpoint-shim because the SPI in this PR doesn't exist yet. We've measured 94 % → 5.7 % infra-failure-rate drop and p50 read wall time 5.74 s → 2.05 s on a four-replica production cluster after cutover. We'd very much like to be the first OSS third-party plugin consumer of the merged SPI, and we've kept a `ShelfBlobCacheManagerFactory` sketch ready to land as `plugin/trino-blob-cache-shelf/` once the API is locked.

Five concrete pieces of feedback from working through the proposed shape against Shelf's existing architecture. Detail and rationale in [this doc](https://github.com/shelf-project/shelf/blob/main/docs/discovery/upstream/29184-spi-feedback.md); the summary version below.

**1. `CacheKey` opacity.** Shelf's keys are content-addressed — `sha256(etag || u64_le(offset) || u64_le(length) || u32_le(rg_ordinal))` — which is load-bearing for Iceberg-snapshot safety (new ETag → new key, no invalidation queue, no stale-read class of bug). If `CacheKey` is constrained to a `(path, version)` shape, content-addressed plugins have to flatten their digest into a synthetic path and pay parsing cost on every lookup. Suggested: make `CacheKey` opaque (a `Slice` or `byte[]` plus pre-computed `hashCode`), or a record with both an `Optional<Location> path` and an `Slice opaqueBytes` field. Path-keyed plugins keep their shape; content-addressed plugins round-trip arbitrary digests.

**2. `CacheTier{MEMORY, DISK}` is narrower than common pool patterns.** Shelf has two pools — `metadata` (DRAM only, small entries, high request rate) and `rowgroup` (DRAM + NVMe spill, large entries, byte-volume) — and the split is access-pattern-shaped, not tier-shaped. Force-fitting metadata into `MEMORY` and rowgroup into `DISK` loses operator clarity (rowgroup has DRAM too). Suggested: allow a free-form name alongside the enum hint — `CacheTier.named("metadata", Hint.MEMORY)` — so operators see real pool names while the engine retains a tier hint for routing.

**3. `invalidate` semantics for content-addressed caches.** Content-addressed keys don't need invalidation — new content produces a new key, old entries become unreachable orphans collected by capacity-based eviction. Shelf's `invalidate` would be a no-op that bumps an observability counter. The current contract is implicit; suggested: explicit "MAY be a no-op when the cache is content-addressed and immutable" in the javadoc, with implementations expected to document this in their factory javadoc and bump a counter for operator visibility. Otherwise content-addressed plugins are technically non-compliant with their own correctness model.

**4. `length(CacheKey)` metadata-only path.** Mirroring the [CodeRabbit thread](https://github.com/trinodb/trino/pull/29184#discussion_r): the current pattern `cache.get(key, source()).length()` triggers a full-blob load on `InMemoryBlobCache`. For Shelf this is acutely wasteful — the metadata pool exists precisely to answer length / footer reads cheaply, and SPI-mediated calls would sidestep that pool entirely. Suggested: add `BlobCache.length(CacheKey, InputFile)` and `BlobCache.head(CacheKey, InputFile)` returning a small `BlobHead(length, optional etag, optional contentType)` descriptor. Path-keyed plugins implement these against their existing metadata index; content-addressed plugins serve them from their metadata pool. Eliminates the InMemoryBlobCache full-load gotcha by API design.

**5. Peer-fetch / clustered cache topology.** Out of scope for v1 of the SPI, but worth a one-line javadoc that `BlobCache` is a *logical* instance — clustered implementations may internally race peers, fan out, or do anything else they want, as long as the externally-observed contract holds. Shelf's SHELF-23 peer-fetch fits within this model; the future SPI v2 could expose `BlobCacheManager.members()` if multiple plugins end up wanting topology-aware affinity routing.

Happy to pair-review individual API shapes via comments here, draft small fix-up PRs against this branch addressing CodeRabbit's flagged issues, or work on the `plugin/trino-blob-cache-shelf/` sketch in parallel so it's ready to land as a PR within a week of this merging. Whichever you'd find most useful — let me know.

=== END ===

## Notes on tone

- Opens with technical context (who Shelf is, why it's relevant) before any opinion. Establishes Shelf as an actually-deployed system, not a hypothetical project.
- Citing measured production numbers (94 → 5.7 %, p50 5.74 → 2.05 s) — matches the README's discipline of showing real numbers, not projections.
- Each issue stated as observation + rationale + suggested fix, not as a demand. Maintainers respond well to "here's what I see and here's a constructive option" framing.
- Closes with a triage of what we'll do — review, fix-up PRs, parallel plugin work — leaving the maintainer to pick the lane.
- Avoids any version commitments ("we'll have it landed in N weeks") because that's the maintainer's call to schedule.

## What to do after posting

1. Watch the issue (top-right of the PR page) so notifications arrive
2. Set up email filter for `notifications@github.com` from `trinodb/trino` to a dedicated label so the thread isn't lost in noise
3. Track responses against the [tracking issue](https://github.com/shelf-project/shelf/issues) on `shelf-project/shelf`
4. If `@wendigo` or `@losipiuk` engages, follow up promptly (≤ 24 h ideally) — review-cycle inertia is real and an active responder gets reviewed faster

## What NOT to do

- Don't post the comment and immediately also Slack DM `@wendigo` — pick one channel and let the other follow naturally. The DM draft at [wendigo-slack-dm.md](./wendigo-slack-dm.md) explicitly notes "I posted a public review comment on #29184" so the two reach-outs are sequenced, not parallel.
- Don't bump the thread if there's no response within 48 h — give it a week. Trino reviews run on weekly cycles.
- Don't reply to the maintainer's eventual response with another wall of text — keep follow-ups tight and code-shaped (link to a PR, not another opinion).
