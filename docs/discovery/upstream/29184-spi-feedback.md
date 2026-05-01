# Shelf-perspective feedback on trinodb/trino#29184

Detailed design feedback on the blob-cache SPI proposed in [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184), grounded in Shelf's existing architecture (BLUEPRINT.md, ADR-0011, ADR-0012). Five concrete issues with suggested fixes; each issue is something an external cache plugin would otherwise have to work around at runtime.

This doc is the long-form reference. The compressed public-facing review comment lives at [29184-review-comment.md](./29184-review-comment.md).

> Verified against the live diff at https://github.com/trinodb/trino/pull/29184/files (last force-push 2026-04-28). API names may shift before merge — re-check before quoting.

## Issue 1 — `CacheKey` opacity

### What the PR has

`CacheKey` appears to be designed around a `(path, version)` shape consistent with Alluxio's path-keyed cache and the existing `CachingHostAddressProvider` work that landed in commit `82ca62d0`.

### Why it matters for Shelf

Shelf's cache key per [ADR-0011](../../../agents/out/adr/0011-content-addressed-cache-keys.md) is content-addressed:

```
key = sha256(etag || u64_le(offset) || u64_le(length) || u32_le(rg_ordinal))
```

This isn't a quirk — it's load-bearing for two correctness properties:

1. **Iceberg-snapshot safety by construction.** New snapshot → new ETag → different key. No invalidation queue, no TTL, no stale-read class of bug.
2. **Sub-file granularity without coordinator state.** Row groups of the same file produce different keys without needing the cache to know about Parquet structure.

If `CacheKey` is constrained to a path-shaped struct, Shelf has to either (a) flatten the digest into a synthetic path string and pay parsing cost on every lookup, or (b) ship a non-path implementation that drifts from the SPI contract.

### Suggested fix

Make `CacheKey` opaque from the SPI's perspective — either:

```java
// Option A — opaque byte array
public record CacheKey(Slice bytes, int hashCode) {}

// Option B — structurally-flexible record with optional path
public record CacheKey(
    Optional<Location> path,        // for path-keyed caches like Alluxio
    Slice opaqueBytes,              // for content-addressed caches like Shelf
    int hashCode
) {}
```

Either lets path-keyed plugins (Alluxio, the in-memory reference) keep their existing shape while content-addressed plugins (Shelf) round-trip arbitrary digests.

### Workaround if not adopted

Shelf can flatten `sha256(...)` into a synthetic `Location` like `shelf://content/<hex>` and parse it back on every `get` call. Costs ~50 ns per lookup; not fatal but ugly.

---

## Issue 2 — `CacheTier{MEMORY, DISK}` is too narrow

### What the PR has

`enum CacheTier { MEMORY, DISK }` and `BlobCacheManagerFactory.cacheTier()` returning one of those.

### Why it matters for Shelf

Shelf has two pools — **metadata** and **rowgroup** — and they differ on access pattern, not on tier:

| Pool | Lives on | Tuned for | Typical entry |
|---|---|---|---|
| metadata | DRAM only | High request rate, small entries | Iceberg manifest, Parquet footer |
| rowgroup | DRAM + NVMe spill | High byte volume, range reads | Parquet column chunk |

Force-fitting `metadata` into `MEMORY` and `rowgroup` into `DISK` loses operator clarity (rowgroup has DRAM too; the distinction isn't tier-shaped) and prevents the SPI from expressing other plausible splits (per-tenant, per-table-class, per-snapshot-cohort).

### Suggested fix

Allow a free-form discriminator alongside the enum hint:

```java
public final class CacheTier {
    public static final CacheTier MEMORY = new CacheTier("memory", Hint.MEMORY);
    public static final CacheTier DISK = new CacheTier("disk", Hint.DISK);
    public static CacheTier named(String name) { ... }
    public static CacheTier named(String name, Hint hint) { ... }

    public enum Hint { MEMORY, DISK, MIXED, UNKNOWN }
    public String name() { ... }
    public Hint hint() { ... }
}
```

Path-keyed plugins keep using the `MEMORY` / `DISK` constants exactly as today. Plugins with richer pool semantics (Shelf's metadata-vs-rowgroup, hypothetical per-tenant) declare their own tier names while still telling the engine which storage hint they fit.

### Workaround if not adopted

Shelf collapses both pools into `DISK` (since rowgroup spills to NVMe). Loses the "metadata pool stayed in DRAM" observability. Tolerable but a notable feature regression vs the standalone `shelfd` deployment.

---

## Issue 3 — `invalidate` semantics for content-addressed caches

### What the PR has

```java
public interface BlobCache {
    BlobSource get(CacheKey key, BlobSource source);
    void invalidate(CacheKey key);
    void invalidate(Collection<CacheKey> keys);
}
```

CodeRabbit threads on the diff have raised invalidation semantics questions (whether `invalidate` must cancel an in-flight populating `get`, atomicity guarantees across batched invalidations, etc.). The current contract is implicit.

### Why it matters for Shelf

Content-addressed keys don't need invalidation — a new write produces a new ETag, hence a new key, hence the old entry becomes an unreachable orphan that capacity-based eviction collects. Shelf's `invalidate` would be a no-op that bumps a metric counter for observability and otherwise does nothing.

But "no-op `invalidate`" is a load-bearing claim about the cache's freshness model. If the SPI contract requires `invalidate` to actually evict, Shelf is technically non-compliant.

### Suggested fix

Make the no-op behaviour an explicitly contract-allowed implementation:

```java
public interface BlobCache {
    /**
     * Invalidate the entry for {@code key}.
     *
     * <p>Implementations MAY treat this as a no-op when their cache key
     * scheme is content-addressed and immutable (i.e., new content
     * produces a new key, making invalidation structurally
     * unnecessary). Implementations that do treat it as a no-op SHOULD
     * document this in their factory javadoc and SHOULD bump an
     * observability counter for operator visibility.
     */
    void invalidate(CacheKey key);

    /**
     * Batch invalidation. Implementations MAY process keys atomically
     * or per-key best-effort; callers MUST NOT rely on either
     * behaviour. May be a no-op under the same conditions as the
     * single-key form.
     */
    void invalidate(Collection<CacheKey> keys);
}
```

### Workaround if not adopted

Shelf implements `invalidate` to forcibly evict from Foyer. Costs O(1) per call but adds an unnecessary write path on a system that doesn't need one. Functionally fine; conceptually wasteful.

---

## Issue 4 — `length(key)` metadata-only path

### What the PR has

CodeRabbit flagged the existing pattern:

```java
long length = cache.get(key, source()).length();
```

triggers a full blob load on `InMemoryBlobCache` (because `BlobSource.length()` calls `load(source)` to materialise the Blob). `AlluxioBlobCache` only queries metadata. The reviewer suggested adding a `BlobCache.length(CacheKey, InputFile)` metadata-only path.

### Why it matters for Shelf

Shelf's `metadata` pool exists to serve length / footer reads cheaply. The whole point of having a separate metadata pool is that footer reads don't touch the rowgroup pool's NVMe path. If the SPI forces full-blob load to answer length, Shelf's pool split is wasted work for SPI-mediated reads.

### Suggested fix

Mirror CodeRabbit's `length(CacheKey)` proposal — and go further, adding a `head(CacheKey)` returning a small descriptor:

```java
public interface BlobCache {
    /**
     * Look up the length of the entry for {@code key} without
     * materialising the blob body. Implementations MUST satisfy this
     * call from cache metadata if possible, falling back to
     * {@code delegate.length()} only on miss.
     */
    long length(CacheKey key, InputFile delegate) throws IOException;

    /**
     * Look up small metadata for the entry — length, optional ETag,
     * optional content-type — without materialising the blob body.
     * The returned descriptor MAY have null or absent fields when
     * the cache cannot satisfy them locally.
     */
    BlobHead head(CacheKey key, InputFile delegate) throws IOException;
}

public record BlobHead(
    long length,
    Optional<String> etag,
    Optional<String> contentType
) {}
```

Path-keyed plugins implement these against their existing metadata index. Shelf serves them from the metadata pool directly; full-blob load is never triggered for length / head queries. The InMemoryBlobCache full-load gotcha is fixed by API design.

### Workaround if not adopted

Shelf's metadata pool can still serve hot footer reads via the `get(...)` path, but every `length(...)` call materialises the row-group bytes too. Costs ~10× more memory bandwidth than necessary on metadata-heavy queries.

---

## Issue 5 — Peer-fetch / coherence

### What the PR has

`BlobCache` is a single-instance interface. There's no surface for clustered cache plugins where multiple `BlobCache` instances share a key space and can serve each other's hits.

### Why it matters for Shelf

Shelf's SHELF-23 peer-fetch lets any pod serve any key by racing peers against origin S3. The SPI's current shape doesn't expose this — Shelf would have to handle peer-fetch internally and present a single-instance facade to Trino.

That's actually the right answer for v1 of the SPI. Surfacing peer-fetch in the SPI would force Trino to know about cache topology, which is the right ignorance.

### Suggested fix

**No change to the SPI for v1.** Document explicitly that `BlobCache` is a logical instance — clustered implementations may internally race peers, fan out, or do anything else they want, as long as the externally-observed contract holds.

For v2 of the SPI (when there's evidence multiple plugins want it), `BlobSource` could grow an optional `peerHint` or `BlobCacheManager` could expose a `members()` query for the engine to do affinity routing.

### Workaround if not adopted

None needed — Shelf already handles peer-fetch internally. This issue is here only so the maintainers know it exists and the SPI can leave room for it later.

---

## What we're NOT raising

Several things we considered but deliberately left out, either because they're out of scope or because raising them now would slow #29184 down:

- **Async `get`** — `CompletableFuture<Blob>` instead of `BlobSource`. Genuinely useful for Shelf's HTTP-fetch path but a much larger SPI churn. Defer to a follow-on issue.
- **Range-typed `get`** — `get(CacheKey, long offset, long length, BlobSource)`. Would let Shelf serve sub-blob ranges directly. Probably belongs in v2; today's `BlobSource` already handles ranges via `InputStream` positioning.
- **Plugin-level cache statistics** — `BlobCacheManager.stats()` returning hit ratio / capacity / pin-list size for the engine to surface in `system.runtime.*`. Nice-to-have; not load-bearing for Shelf.
- **Metric labels** — `CacheKey` carrying a `tableName` / `tagId` for observability cardinality. Already proposed elsewhere; would conflict with the opaque-key proposal in Issue 1. Park.

## Summary table

| # | Issue | Severity | Workaround if rejected |
|---|---|---|---|
| 1 | `CacheKey` opacity | High — affects every `get` call | Synthetic path string, ~50 ns per lookup |
| 2 | `CacheTier` too narrow | Medium — operator clarity loss | Collapse both pools into `DISK` |
| 3 | `invalidate` no-op contract | Low — semantic clarification | Implement as forced eviction |
| 4 | `length(key)` metadata-only path | High — affects metadata pool ROI | Full-blob load on every length query |
| 5 | Peer-fetch / coherence | Informational — for SPI v2 awareness | None needed; handled internally |

## See also

- [docs/discovery/trino-upstream-strategy.md](../trino-upstream-strategy.md) — overall upstream strategy
- [docs/discovery/upstream/29184-review-comment.md](./29184-review-comment.md) — compressed public-facing version
- [agents/out/adr/0011-content-addressed-cache-keys.md](../../../agents/out/adr/0011-content-addressed-cache-keys.md) — Shelf's cache-key design
- [agents/out/adr/0012-trino-read-path-endpoint-swap-then-blob-cache-spi.md](../../../agents/out/adr/0012-trino-read-path-endpoint-swap-then-blob-cache-spi.md) — Shelf's two-stage Trino integration plan
