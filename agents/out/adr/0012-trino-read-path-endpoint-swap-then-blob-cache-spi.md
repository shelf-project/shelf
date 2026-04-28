# ADR 0012: Trino read-path — S3-endpoint swap today, Trino blob-cache SPI tomorrow

*Status: Accepted (2026-04-23)*
*Deciders: eng-lead, trino-plugin-eng-1, rust-engineer-1*
*Supersedes: none*
*Superseded-by: none*
*Related: SHELF-10, SHELF-12, SHELF-22*

## Context

Shelf ships a Java side — `ShelfFileSystemFactory` + supporting
`io.shelf.client` machinery (~1 600 LOC, 116 green tests) — that was
originally designed to register with Trino as a first-class
`TrinoFileSystemFactory`. The plan was: Trino loads `ShelfPlugin`,
`ShelfPlugin.getFileSystemFactories()` exposes `ShelfFileSystemFactory`,
Iceberg's splits go through Shelf in-process.

During SHELF-22 we verified against the Trino 480 JARs (`javap io.trino.spi.Plugin`) and against `trinodb/trino@master` that
`**io.trino.spi.Plugin` exposes no `getFileSystemFactories()` method and
no `getFileSystemCache*()` method.** The interface surfaces connectors,
event listeners, types, functions, access control, resource groups,
exchange managers, spool managers — but filesystems are deliberately
*not* a plugin extension point today. Trino's own `TrinoFileSystemCache`
interface exists (and Alluxio binds to it via `FileSystemModule`'s Guice
`OptionalBinder`), but the Alluxio module is hard-linked into
`trino-filesystem-manager`. There is no third-party slot.

Two upstream escape hatches exist:

1. **Fork `lib/trino-filesystem-manager`** — single-file patch adding a
   `ShelfFileSystemCacheModule` next to `AlluxioFileSystemCacheModule`.
   Forces rebuilding the Trino image on every Trino release.
2. **Wait for trinodb/trino#29184** — "DRAFT: Implement unified blob
   cache plugin SPI", opened 2026-04-21 by @wendigo (Mateusz Gajewski,
   Trino committer), actively in development. Introduces
   `io.trino.spi.cache` with `BlobCache`, `BlobSource`, `Blob`,
   `BlobCacheManager`, `BlobCacheManagerFactory`, `CacheLatency`,
   `CacheManagerContext`, `CacheKey`, plus a new default method on
   `io.trino.spi.Plugin` (`getBlobCacheManagerFactories()`) and two
   reference plugins (`trino-blob-cache-alluxio`,
   `trino-blob-cache-memory`). This is the exact SPI hole Shelf was
   built for. The full verified SPI surface — read from branch
   `user/serafin/unified-caching-v2` on 2026-04-24 — is in
   `clients/trino/docs/design-notes/SHELF-29-blob-cache-plugin.md`
   §"SPI snapshot". That note is the single source of truth for the
   surface; this ADR captures only the strategy decision.

   One design friction worth calling out: **`CacheLatency` only has
   `MEMORY` and `DISK`**, no `REMOTE` value. (Note: in earlier drafts
   of #29184 this enum was called `CacheTier`; it was renamed to
   `CacheLatency` on or before 2026-04-24 — re-verify against the
   branch at implementation time.) Shelf is structurally a remote HTTP
   cache (calls go to `shelfd` over h2), but the shelfd pod itself is
   a DRAM+NVMe hybrid. Shelf would register as `CacheLatency.DISK` —
   honest, because the origin-of-truth for the cache is shelfd's
   NVMe — and the fact that the transport is HTTP is an implementation
   detail the SPI doesn't care about. If a `REMOTE` value is added
   later (plausible — Alluxio remote-cache work in PR #24737 points
   this direction), Shelf can migrate.

In the meantime, the read path has to work *today* for the v0.5 gate.

## Decision

Adopt a three-phase integration strategy:

- **Phase 1 — now, v0.5 → v1.0.** Route Trino's Iceberg catalog through
`shelfd`'s S3-compatible shim on `:9092` (SHELF-22). Iceberg config:
`s3.endpoint=http://shelfd:9092`, `s3.path-style-access=true`, pool
isolation already handled inside `shelfd`. No Trino fork. One extra
in-pod HTTP hop (~100–500 µs on cache hit) vs. tens of milliseconds on
a cross-AZ S3 miss — the latency math is dominated by the miss path,
so the extra hop is in the noise.
- **Phase 2 — when #29184 merges (target: Trino 485–490).** Ship
`clients/trino-blob-cache-shelf/` as a normal plugin jar implementing
`BlobCacheManagerFactory` on top of existing `io.shelf.client` code.
Drop into `$TRINO_HOME/plugin/`, set `cache.manager=shelf` in catalog
properties, delete the `s3.endpoint` override from
`iceberg.properties`. Estimated work: ~400–600 LOC of glue, because
`shelfd` already owns admission / pinning / hybrid-tier semantics.
The Phase-1 path stays available as a fallback.
- **Phase 3 — only if warranted.** Fork `FileSystemModule.java` to add
`ShelfFileSystemCacheModule` alongside Alluxio's. Do **not** take this
path unless a production flamegraph proves the Phase-1 HTTP hop is
measurable in the workload. The upstream SPI (Phase 2) is strictly
better if it exists.

## Alternatives considered

- **Fork `trino-iceberg` to inject `ShelfFileSystemFactory` directly.**
Rejected: connector-specific, obsoleted the day #29184 merges,
maximum maintenance surface.
- **Dynamic classloader / reflection injection.** Rejected: fragile,
breaks on every Trino version bump, violates the "one obvious way"
principle for ops.
- **Switch to Alluxio.** Rejected: Alluxio's Trino integration requires
a separately deployed Alluxio cluster next to Trino. Strictly heavier
ops footprint than `shelfd` and loses Shelf's working-set pinning and
SIEVE admission. The SHELF-27 dashboard's "beat Alluxio on rep-2"
gate (ADR 0010) explicitly names Alluxio as the baseline, not the
replacement.
- **Unix-socket mode on `shelfd:9092` for the Trino read path.**
**Rejected — structurally impossible.** Trino's native S3 client is
`software.amazon.awssdk.services.s3.S3Client` (AWS Java SDK v2), and
SDK v2's `endpointOverride(URI)` only accepts `http://` / `https://`
URIs; the Netty-based async HTTP client does not speak UDS. Shipping
a UDS listener on shelfd would help `shelfctl`, DuckDB (via
botocore's `unix://` socket shim), and Polars, but would not touch
the Trino read path at all. If in-pod latency on Phase 1 ever becomes
a real measured problem, the path forward is a Trino-upstream
contribution adding a UDS transport to the S3 client — significantly
heavier than the Phase 2 blob-cache plugin described above, so Phase
2 strictly dominates.

## Consequences

- **The Java `ShelfFileSystemFactory` is currently dormant but not
dead.** It is built, tested, and the `io.shelf.client` code under it
is actively used by `ShelfPrefetchListener` (which *does* wire through
the public `getEventListenerFactories()` SPI, running on every query
today). ~800 LOC of FS-interceptor code sits ready for Phase 2, which
reuses most of it unchanged.
- **The smoke harness has one non-production-grade knob.**
`iceberg.metadata-cache.enabled=false` in
`benchmarks/smoke/config/trino/etc/catalog/iceberg.properties` is
required only so warm runs re-hit the shim and `shelf_hits_total`
moves for the gate assertion. Production catalogs keep the default
(`true`): the JVM metadata cache sits on top of Shelf, saving the
footer re-parse cost on warm reads that Shelf wouldn't service
anyway.
- **We take a soft dependency on a draft PR.** If #29184 is rewritten
or abandoned, Phase 2 regresses to Phase 3 (the `FileSystemModule`
fork). Cost bound: ~20 lines of patched Trino Java + a periodic
rebase. Manageable.
- **Upstream contribution path opens up.** Once `BlobCacheManagerFactory`
is public SPI, a `trino-blob-cache-shelf` PR is a natural upstream
contribution — keeps us honest against API drift and gives Shelf
visibility in the Trino ecosystem.

## Triggers for Phase 2

Promote from Phase 1 to Phase 2 when **all three** hold:

1. trinodb/trino#29184 (or its successor) is merged to `master` and
  appears in a tagged release.
2. `BlobCacheManagerFactory` and `BlobCache` are in `io.trino.spi`
  with stable signatures (no `@Experimental` / no `@Deprecated`).
3. We have bandwidth for ~1 sprint of plugin work and the rep-2
  shadow-traffic rollout (SHELF-13) is green, so we're not changing
   two things at once.

## Test surface

- Phase 1 is gated by `benchmarks/smoke/run-smoke.sh` (Trino → shelfd
shim → MinIO, metadata and rowgroup pool counters must rise on warm
re-run).
- Phase 2 will be gated by a copy of the same smoke with `s3.endpoint`
removed and `cache.manager=shelf` added. Target: same hit-ratio
deltas, lower p50 read latency on cache hits.

## References

- `shelfd/docs/design-notes/SHELF-22-s3-compat-shim.md`
- `shelfd/docs/design-notes/SHELF-22a-unix-socket-mode.md` — measured-and-closed follow-up on the Phase-1 TCP hop.
- `clients/trino/docs/design-notes/SHELF-29-blob-cache-plugin.md` — concrete Phase-2 implementation plan against the SPI signatures in this ADR.
- `clients/trino/src/main/java/io/shelf/plugin/ShelfPlugin.java`
- `clients/trino/src/main/java/io/shelf/filesystem/ShelfFileSystemFactory.java`
- `benchmarks/smoke/config/trino/etc/catalog/iceberg.properties`
- `docs/cluster-handoff.md` §"S3-compat shim"
- Upstream: [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184) — unified blob cache plugin SPI (DRAFT, open)
- Upstream: [trinodb/trino#18719](https://github.com/trinodb/trino/pull/18719) — Alluxio cache (merged, precedent)
- `agents/out/adr/0010-v05-gate-beat-alluxio-on-rep2.md`

