# SHELF-22a — Unix-socket mode on `shelfd:9092`

**Status**: closed as **not-building (for now)**. Measured, not warranted. Re-open only if the numbers below change by >5×.
**Owner**: shelf-core
**Decided**: 2026-04-24

## TL;DR

We investigated adding a Unix-domain-socket (UDS) listener to `shelfd`'s
S3-compatible shim (`:9092`) in the hope of shaving TCP-localhost overhead
off the Phase-1 Trino read path. **Measurement shows the TCP hop costs
~325 µs per request on keep-alive, and Trino's native S3 client cannot
consume a UDS without a custom AWS-SDK-v2 HTTP client (weeks of work we
cannot justify for the expected ~0.5% end-to-end query savings).** We
are explicitly closing SHELF-22a as *measured, not needed*. The ticket
stays parked in the tracker as a pointer to this note.

## Why this was even a question

ADR 0012 (Trino read-path endpoint swap) parked UDS as a follow-up:

> **Unix-socket mode on `shelfd:9092`.** Not rejected, but parked as a
> SHELF-22a follow-up. Nets ~50 µs off the Phase-1 TCP localhost hop if
> measurement ever shows it matters; otherwise a YAGNI.

When the sidecar (Trino + `shelfd` in the same pod, sharing an
`emptyDir` volume) deployment pattern came up as a way to kill the
TCP-localhost hop, UDS became the obvious mechanism. So we measured.

## Measurement

Setup: `benchmarks/smoke` harness running on a MacBook Pro M3 (darwin
24.6.0). Docker Desktop with shelfd bound to `127.0.0.1:9092`. All
requests served from Foyer cache (warm pool). 2000 HEAD / 500 GET per
configuration.

Bench script: ad-hoc Python `http.client` (see
`benchmarks/smoke/bench-tcp-localhost.py` if committed, otherwise this
note is the record).

### HEAD (cache lookup + small response)


| mode                      | µs / req |
| ------------------------- | -------- |
| keep-alive (reused conn)  | **331**  |
| fresh connection per call | 985      |


→ TCP setup alone costs ~655 µs. **Trino reuses connections** via
the AWS-SDK-v2 Apache HTTP client (default `s3.max-connections=500`,
pool size 50 per endpoint), so in practice it sees the 331 µs
keep-alive figure, not 985 µs.

### GET (cache hit, varying body size)


| path                                | body  | µs / req |
| ----------------------------------- | ----- | -------- |
| metadata JSON                       | 1.2 K | 327      |
| whole parquet                       | 2.4 K | 319      |
| parquet footer range (`bytes=-512`) | 512 B | 324      |


For small payloads the per-request overhead dominates the wire time
(consistent with the ~325 µs HEAD number). For the sizes that matter
on a real Trino scan (≥1 MiB row-group prefetches), the bytes cost
completely swallows the TCP hop — the shim is no longer the bottleneck.

### UDS best case (literature)

UDS typically cuts the "socket send/recv" cost by ~~50–100 µs vs
TCP-localhost on Linux, because it skips the TCP/IP stack entirely.
Our most optimistic UDS number is therefore **~~150–200 µs / req** —
a savings of **~125–175 µs / req**.

### Query-level impact

The representative Iceberg query in our smoke harness issues 40–100
shim requests total (manifest-list read, manifest reads, parquet
footer reads, then the row-group prefetches).

- **Upper bound** of UDS savings on a 100-request metadata-heavy cold
query: 175 µs × 100 = **17.5 ms**.
- A cold metadata-heavy query on this harness takes ~~280 ms end-to-end.
17.5 ms is **~~6% of one cold query**, and essentially zero on a warm
query (where the JVM-level Iceberg cache absorbs most metadata ops
anyway — the reason we disable it in smoke is purely for
observability).
- On a production 30 GiB scan, the TCP hop is already <0.1% of the
query — UDS would be unmeasurable.

## Why we are not building it

Three independent reasons, any one of which is sufficient:

1. **Trino cannot consume a UDS endpoint.** AWS SDK v2's `S3AsyncClient`
  takes an `SdkHttpClient` / `SdkAsyncHttpClient`. Neither Apache
   (`ApacheHttpClient`) nor Netty (`NettyNioAsyncHttpClient`) ship UDS
   transports. Building one is a real project (custom connection
   factory, DNS resolver bypass, Netty `DomainSocketChannel` wiring,
   fork-join testing across Trino's tracer/metrics stack, Graal-native
   considerations). Call it 4–8 engineer-weeks including review. We
   cannot justify that spend for a 6%-of-one-cold-query win.
2. **Sidecar deployment is not our path.** The EKS/GKE deployment
  pattern for `shelfd` is a dedicated `StatefulSet` per AZ, not a
   sidecar per Trino worker. We chose this for cache-locality and
   eviction-coherence reasons (SHELF-18 NVMe hybrid pool assumes
   shelfd owns its disk, not a per-pod shard of it). In a non-sidecar
   deployment, worker and shelfd live in different pods → different
   network namespaces → **no shared filesystem for a UDS**. TCP is the
   only option, full stop.
3. **The measured overhead is already small enough to ignore.** 325 µs
  × 50 requests = 16 ms of total shim overhead per query. Foyer cache
   lookups themselves (memory-pool hit) take ~50 µs. We are within a
   small-constant factor of the best we could reasonably do, and the
   gap to "perfect" is smaller than the query planner jitter.

## What to do instead

The honest follow-ups, ranked:

1. **Keep-alive verification in the smoke harness.** Not a ticket —
  just confirm via `/proc/net/tcp` that Trino's S3 client reuses
   connections, and fail smoke if the per-query new-connection count
   exceeds a threshold (say, 5). This catches config regressions that
   would push us from 331 µs → 985 µs overnight. **Do this as part of
   SHELF-27 observability, not a new ticket.**
2. **Phase 2 blob-cache plugin (SHELF-29).** When
  `[trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184)`
   merges, the in-process `BlobCache` bypasses the shim entirely for
   Trino — zero TCP hops, zero localhost, zero serialization. This is
   the real fix. See `clients/trino/docs/design-notes/SHELF-29-blob-cache-plugin.md`.
3. **Range-coalescing / prefetch batching.** Reduce request count,
  which beats reducing per-request cost for both TCP and UDS. Already
   tracked under SHELF-17 (manifest pool) and SHELF-25 (prefetch
   hints). Much better ROI than transport-level work.

## Re-opening criteria

Revisit UDS only if **all** of these hold:

- A new benchmark shows shim overhead >2 ms / req (6× today's number).
- Trino ships a first-class UDS-capable HTTP client, or we commit to
shipping one upstream. (`trinodb/trino#29184` landing solves the
problem a different, better way — that also closes this ticket.)
- A sidecar deployment pattern becomes the default, meaning Trino
worker and `shelfd` actually share a filesystem namespace.

Until then: **TCP-localhost is fine.**