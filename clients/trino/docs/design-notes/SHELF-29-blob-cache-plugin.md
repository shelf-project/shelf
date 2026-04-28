# SHELF-29 — `trino-blob-cache-shelf` plugin (Phase 2 of ADR 0012)

**Status**: design complete; implementation **blocked on upstream merge** of
[trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184).
**Owner**: shelf-core + trino-plugin-eng-1
**Last SPI read**: branch `user/serafin/unified-caching-v2`, 2026-04-24.
**Estimated effort on merge**: ~1 sprint (400–600 LOC + tests + smoke
harness variant). Most of the code is glue; the primitives exist in
`io.shelf.client`.

## TL;DR

When #29184 lands, we ship a standalone plugin jar
(`trino-blob-cache-shelf`) that implements `BlobCacheManagerFactory` on
top of the existing `io.shelf.client` stack. Trino catalogs enable it
with `cache.manager=shelf`; the `s3.endpoint` override from Phase 1
disappears. The plugin talks to `shelfd` over the same HTTP/2 data
plane as today (`/cache/{pool}/{key}/{range}`); nothing in `shelfd`
changes.

## Why this note exists before the PR merges

Three reasons:

1. **ADR 0012 made the strategy decision but handwaved the shape.** That
   was honest when the SPI was 200 LOC old. It has since drifted — e.g.
   `CacheTier` was renamed to `CacheLatency`, `CacheKey` became a bare
   `record CacheKey(String)`. Re-reading the real signatures now stops
   us from re-deriving them the week we want to ship.
2. **The `shelfd` side is stable.** This plugin does not require any
   change to `shelfd`'s HTTP contract, Foyer config, or admission logic.
   Pinning that down now lets us reject "let's just adjust the server
   to make the client easier" drift later.
3. **`changelog.md` already forward-references this file.** Closing the
   dangling pointer is a small courtesy to whoever next reads the repo.

## SPI snapshot (verified 2026-04-24)

Read straight out of the draft-PR branch. Packages all under
`io.trino.spi.cache`. Javadoc elided.

```java
public interface Plugin {
    // ... existing methods ...
    default Iterable<BlobCacheManagerFactory> getBlobCacheManagerFactories() {
        return emptyList();
    }
}

public interface BlobCacheManagerFactory {
    String getName();                             // "shelf"
    CacheLatency latency();                       // DISK (see §"Tier choice")
    BlobCacheManager create(Map<String, String> config, CacheManagerContext context);
}

public interface BlobCacheManager {
    BlobCache createBlobCache(CatalogName catalog);
    void drop(CatalogName catalog);
    void shutdown();
}

public interface BlobCache {
    Blob get(CacheKey key, BlobSource source) throws IOException;
    void invalidate(CacheKey key);
    void invalidate(Collection<CacheKey> keys);
}

public interface Blob extends Closeable {
    long length() throws IOException;
    void readFully(long position, byte[] buffer, int offset, int length) throws IOException;
}

public interface BlobSource {
    long length() throws IOException;
    void readFully(long position, byte[] buffer, int offset, int length) throws IOException;
}

public record CacheKey(String key) { /* null-checked ctor */ }

public enum CacheLatency { MEMORY, DISK }

public interface CacheManagerContext {
    OpenTelemetry getOpenTelemetry();
    Tracer getTracer();
}
```

Reference implementation (`MemoryBlobCachePlugin`) uses Airlift
`Bootstrap` + Guice modules — standard Trino plugin idiom. We copy
that pattern.

## Tier choice: `DISK`

Shelf is structurally a remote HTTP cache from the plugin's
perspective (every call is a `/cache/...` GET over localhost TCP or
cluster-internal TCP). But the SPI does not model "remote" — only
`MEMORY` and `DISK`. The honest answer is `DISK`, because `shelfd`'s
canonical tier is NVMe; the HTTP transport to reach it is an
implementation detail the Trino planner does not need to know about,
and `MEMORY` would lie about the latency ceiling.

If the SPI ever gains a `REMOTE` tier we switch on sight; no migration
cost because `latency()` is declarative, not structural.

## Mapping `io.shelf.client` → the SPI

The plugin is ~90% adaptation. Nothing has to be rewritten.

| SPI concept                         | Shelf mapping                                                                 |
| ----------------------------------- | ----------------------------------------------------------------------------- |
| `BlobCacheManagerFactory.getName()` | returns `"shelf"`                                                             |
| `BlobCacheManagerFactory.latency()` | returns `CacheLatency.DISK` (see §"Tier choice")                              |
| `BlobCacheManagerFactory.create()`  | builds `ShelfBlobCacheManager` via Airlift Bootstrap, binds `ShelfConfig` + `ShelfHttpClient` + `MembershipResolver` (all already exist in `io.shelf.client`) |
| `BlobCacheManager.createBlobCache(catalog)` | returns a `ShelfBlobCache` scoped to the catalog's name, sharing the singleton HTTP client and ring |
| `BlobCacheManager.drop(catalog)`    | drops the `ShelfBlobCache` entry; does **not** wipe `shelfd` — pins outlive catalog drops by design |
| `BlobCacheManager.shutdown()`       | closes the `HttpClient`, cancels any in-flight CircuitBreaker half-open probes |
| `BlobCache.get(key, source)`        | returns a `ShelfBlob` that lazily hits `shelfd` on first `readFully`, falling through to `source` on any miss-or-failure (per BLUEPRINT §9.5 fail-open) |
| `BlobCache.invalidate(key/keys)`    | issues `DELETE /cache/{pool}/{contentKey}` against the owning pod per HashRing (ADR-0002); 404 is treated as success |
| `Blob.length()`                     | served from the HEAD response we cache on first `get()` — one round-trip, cached for the Blob's lifetime |
| `Blob.readFully(...)`               | `ShelfHttpClient.rangeGet(endpoint, pool, contentKey, position, length)`; on any `ShelfUnavailableException`, delegates to `source.readFully(...)` and increments the per-pod breaker |

### The CacheKey translation

Trino's `CacheKey` is an opaque string. Our HTTP contract is keyed on a
32-byte SHA-256 digest (SHELF-04). We cannot recover the SHELF-04
structure (etag, offset, length, ordinal) from Trino's opaque string,
and we do not need to — we only need a stable, collision-resistant
mapping from Trino's string namespace to ours:

```java
String contentKey = hex(sha256(utf8(cacheKey.key())));
```

Consequences:

- **The plugin's keyspace is disjoint from Phase 1's** (where the
  S3-endpoint shim keyed off `(bucket, object, etag, range)` via SHELF-04).
  This is fine: a cluster runs *either* Phase 1 *or* Phase 2 per
  catalog, not both. Coexistence within one shelfd is possible but
  pointless.
- **`shelfd` sees two distinct populations** if both paths are enabled
  pod-wide. The eviction policy (Foyer SIEVE) treats them
  independently, which is the behaviour we want.
- **We inherit Trino's key stability.** If Trino upstream changes how
  it constructs `CacheKey.key()`, all our entries effectively
  invalidate. That is their contract, not ours; our hash commutes.

### The pool split (metadata vs rowgroup)

Today the S3 shim (Phase 1) tags pool by inspecting the object path
(`*.metadata.json` / `manifest-list*.avro` → metadata, `.parquet`
footer range → also metadata, bulk parquet → rowgroup). In Phase 2 we
do not have the S3 path — only `cacheKey.key()`. Two options:

1. **Single-pool fallback.** Route everything to `Pool.ROWGROUP`.
   Simple, honest; loses the 10× size-asymmetry benefit that metadata
   pooling gives Foyer today.
2. **Hint from caller.** `source.length()` is ~free (already cached by
   Trino) and is a perfect classifier: `< 16 MiB` → metadata,
   `≥ 16 MiB` → rowgroup. Same heuristic the S3 shim uses today, just
   size-driven instead of path-driven.

**We pick option 2.** It costs us one probe into `source.length()`
before the first `readFully` — amortised to zero on a keepalive
connection, and we were going to call it for `Blob.length()` anyway.

The 16 MiB threshold matches
`benchmarks/smoke/config/trino/etc/catalog/iceberg.properties`
today; we promote it to a typed config key `shelf.pool-threshold-bytes`.

## Miss semantics: read-through, *not* put-through

The reference `MemoryBlobCache` reads from `BlobSource` on miss *and
populates* its own storage. We could mirror that — the Trino worker
reads from S3 via `source`, then `PUT /cache/...` into shelfd — but
we are not going to, at least not initially. Three reasons:

1. **Shelfd is the authority on its own disk.** The SHELF-18 NVMe
   hybrid pool assumes shelfd controls admission ordering. A Trino
   worker populating over HTTP PUT is a second writer, and
   coordinating pinning / SIEVE across two writers is exactly the
   complexity we avoided by building shelfd as a dedicated process.
2. **Population already happens.** Shelfd's prefetcher (SHELF-17 /
   SHELF-25) warms entries via its own S3 client on plan hints and
   scan predictions. Making Trino do it a second time is duplicative.
3. **It preserves fail-open.** A plugin that only *reads* from shelfd
   degrades to "shelfd adds no latency, adds no bytes served" on
   outage. A put-through plugin would have to decide what to do with
   a half-completed PUT.

Rephrased as a line of code, `ShelfBlob.readFully` is:

```java
@Override
public void readFully(long position, byte[] buf, int off, int len) throws IOException {
    try {
        byte[] bytes = shelfHttp.rangeGet(endpoint, pool, contentKey, position, len);
        System.arraycopy(bytes, 0, buf, off, len);
        metrics.hits.increment();
    } catch (ShelfUnavailableException miss) {
        source.readFully(position, buf, off, len);
        metrics.misses.increment();
        breaker.recordFailure();   // contributes to CLOSED→OPEN transitions
    }
}
```

The `Blob` returned from `get()` never blocks waiting for population;
it just falls through. A subsequent `get()` for the same key may well
hit, because shelfd's own warming loop has since pulled the bytes.

**Re-opening criterion for put-through.** If shadow-traffic
measurement (SHELF-13) shows Phase 2 hit ratio >5% below the Phase 1
baseline for the *same workload*, that is evidence shelfd's own
warming is not keeping up, and we re-evaluate. Until then, read-through
is the honest default.

## Configuration surface

Standard Trino plugin config map, typed via `@Config`:

```
cache.manager=shelf                               # selects this factory
cache.shelf.endpoint=http://shelfd:8080           # data-plane, NOT the :9092 shim
cache.shelf.endpoint-resolver=hashring            # hashring (default) | fixed
cache.shelf.pool-threshold-bytes=16777216         # 16 MiB; drives metadata/rowgroup split
cache.shelf.request-timeout=200ms                 # per-call deadline (BLUEPRINT §9.5)
cache.shelf.breaker-failure-threshold=5           # before CLOSED→OPEN
cache.shelf.breaker-open-duration=10s             # initial HALF_OPEN backoff
```

Three keys are deliberately **not** exposed: the SHELF-04 key function
(fixed), the HTTP/2 requirement (ADR-0004), and the fail-open policy
(non-negotiable). Making those configurable is a decision for an ADR,
not an ops dial.

## File layout

New module sibling to `clients/trino/`:

```
clients/trino-blob-cache-shelf/
  pom.xml                         # depends on io.shelf:trino-plugin for client code
  src/main/java/io/shelf/blobcache/
    ShelfBlobCachePlugin.java             # Plugin SPI override, ~15 LOC
    ShelfBlobCacheModule.java             # Guice wiring, ~40 LOC
    ShelfBlobCacheConfig.java             # @Config beans, ~80 LOC
    ShelfBlobCacheManagerFactory.java     # ~40 LOC
    ShelfBlobCacheManager.java            # per-plugin lifecycle, ~80 LOC
    ShelfBlobCache.java                   # per-catalog, ~70 LOC
    ShelfBlob.java                        # Blob impl, ~90 LOC
    ShelfBlobMetrics.java                 # Airlift metric beans, ~30 LOC
  src/main/resources/META-INF/services/
    io.trino.spi.Plugin                   # one line: io.shelf.blobcache.ShelfBlobCachePlugin
  src/test/java/io/shelf/blobcache/
    ShelfBlobCacheManagerTest.java
    ShelfBlobTest.java                    # mocks ShelfHttpClient + BlobSource
    ShelfBlobCacheSmokeTest.java          # live shelfd, Docker-compose harness
```

We deliberately **do not** merge this into the existing `clients/trino/`
module: keeping them separate means the read-path plugin
(`trino-blob-cache-shelf`) and the event-listener plugin (the current
`trino-plugin` — `ShelfPrefetchListener`) can ship on independent
Trino versions. If #29184 lands in Trino 485 and we are stuck on 480
for another quarter for other reasons, we can ship only the event
listener.

## Testing plan

1. **Unit**: mock `ShelfHttpClient` to emit {hit, miss, 5xx, timeout}
   per call; assert the metrics counters increment correctly and that
   miss-path `BlobSource` is invoked with the exact `(position, len)`.
2. **Contract**: one test per `BlobCache` method asserts the SPI
   invariant (e.g. `get` always returns a usable `Blob` even on a
   shelfd 100% failure rate — fail-open).
3. **Property**: generate `CacheKey` strings via quickcheck-style,
   check the SHA-256 mapping is a total function with no observable
   collisions at 10⁶ keys (sanity only; SHA-256 is not the failure
   mode we are worried about).
4. **Smoke**: copy `benchmarks/smoke/run-smoke.sh` into
   `benchmarks/smoke-phase2/`; the only iceberg.properties diff is:

   ```diff
   -s3.endpoint=http://shelfd:9092
   -iceberg.metadata-cache.enabled=false
   +cache.manager=shelf
   +cache.shelf.endpoint=http://shelfd:8080
   ```

   Gate: cold/warm byte-identical on the same 10 queries, and
   `shelf_hits_total` on the shelfd side rises by >90% of the request
   count on the warm run (same gate as Phase 1).

5. **A/B**: run the Phase 1 and Phase 2 smokes back-to-back on the
   same shelfd (different catalog names to keep keyspaces disjoint).
   Phase 2 warm-p50 should be ≤ Phase 1 warm-p50; if it is slower, we
   have a regression in the plugin glue and we do not ship.

## Pre-merge prep work we can do today

Two small items worth picking up before #29184 lands, both zero-cost
if the SPI drifts:

- **Plumb `OpenTelemetry` through `ShelfHttpClient`.** The SPI will
  give us a `Tracer` via `CacheManagerContext`; the current client
  does not accept one. One constructor arg + two span-wrap points.
  Ticket: SHELF-29a (tracked in changelog "Decided, not yet
  implemented" next revision).
- **Extract `ShelfHttpClient` fail-open semantics into a tested
  `FailOpenFetcher` abstraction.** The plugin needs exactly the
  current behaviour, minus the `TrinoFileSystem` coupling. Ticket:
  SHELF-29b.

Both are pure refactors with immediate test coverage. Neither blocks
anything, but doing them ahead of the merge cuts Phase 2 landing from
~1 sprint to ~3 days.

## Triggers to start the implementation

All three must be true (mirroring ADR 0012 §"Triggers for Phase 2"):

1. #29184 (or successor) is merged to `trinodb/trino` `master` and
   tagged in a release.
2. `BlobCacheManagerFactory` + `BlobCache` signatures are stable (no
   `@Experimental` / `@Deprecated` annotations, two consecutive
   patch releases without signature churn).
3. SHELF-13 (rep-2 shadow traffic) is green. We do not want to flip
   the read path and the rollout strategy in the same sprint.

## Re-open criteria for this note

Revisit the design if **any** of these happen:

- SPI drifts on any of the interfaces quoted in §"SPI snapshot"
  (re-verify against the branch at that point).
- `CacheLatency` gains a `REMOTE` tier (we switch on sight).
- Trino introduces a `CacheKey` *schema* (e.g. structured key parts)
  — the SHA-256 remapping becomes lossy-of-metadata and we would
  want to preserve the structure for debuggability.
- The `MemoryBlobCachePlugin` reference implementation changes the
  miss-side contract (e.g. mandates put-through).

## References

- `agents/out/adr/0012-trino-read-path-endpoint-swap-then-blob-cache-spi.md`
  (supersedes its `CacheTier` terminology; use `CacheLatency` here)
- `shelfd/docs/design-notes/SHELF-22a-unix-socket-mode.md`
  (why we aren't building the other Phase-1 optimisation)
- `clients/trino/src/main/java/io/shelf/client/ShelfHttpClient.java`
  (the primitive this plugin reuses)
- Upstream: [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184)
