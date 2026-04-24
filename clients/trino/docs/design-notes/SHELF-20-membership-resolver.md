# SHELF-20 — Java membership resolver

## Purpose

Keep `io.shelf.client.HashRing` fresh by resolving the Shelf headless
service DNS every ~5 s, polling each pod's `/stats` endpoint for
capacity-weighted HRW membership, and publishing an atomic snapshot that
pairs each pod with its per-pod `CircuitBreaker` and an
`http://<ip>:<port>` endpoint URI.

## Public classes

- `io.shelf.client.MembershipResolver` (new): long-lived component,
  1 per plugin instance, owns a daemon `ScheduledExecutorService`.
- `MembershipResolver.Snapshot` (nested record): immutable triple of
  `(HashRing, Map<podId, URI>, Map<podId, CircuitBreaker>)`.
- `MembershipResolver.Target` (nested record): `(podId, URI endpoint,
  CircuitBreaker breaker)` returned by `ownerFor(byte[])`.
- `MembershipResolver.EndpointSource` (nested functional interface):
  test seam — returns the current list of pod endpoint URIs.
  Production is `InetAddress::getAllByName` over the headless DNS.

## SPI surfaces touched

None. `MembershipResolver` is a plain `io.shelf.client.*` utility.
It is wired into `ShelfFileSystemFactory` (replacing the single
`(endpoint, CircuitBreaker)` pair) and read from `ShelfInputFile` once
per `newStream()` call to select the target pod.

## Config keys (owned by `shelf.*` namespace; see BLUEPRINT §6.2)

- `shelf.membership.refresh-interval-ms` — default `5000`,
  `> 0`, `<= 300000`.
- `shelf.membership.stats-timeout-ms` — default `2000`,
  `> 0`, `<= 60000`. Independent of the 200 ms hot-path deadline
  (`shelf.rpc.timeout-ms`) because `/stats` runs on a background
  scheduler.

The JDK DNS cache must be trimmed for the refresh cadence to matter.
This is a JVM-wide setting (`networkaddress.cache.ttl`) and is therefore
not set by the resolver — documented as a caller responsibility, picked
up by the Helm chart (SHELF-21) which sets `-Dsun.net.inetaddr.ttl=0`
on the coordinator / worker JVMs.

## Thread-safety story

- `snapshot` is an `AtomicReference<Snapshot>`. Readers (`ownerFor`,
  `snapshot()`) are wait-free; refresh swaps once per tick.
- Breakers are kept in a `ConcurrentHashMap<String, CircuitBreaker>`
  keyed by `podId` so the same physical breaker persists across
  refreshes — only the membership view on top changes.
- Refresh runs on a single daemon thread. `close()` shuts the executor
  down and awaits termination.

## Failure matrix (fail-open invariant)

| Event                              | Resolver behaviour                                                                      |
| ---------------------------------- | --------------------------------------------------------------------------------------- |
| `UnknownHostException` on DNS      | Keep last good snapshot. Warn-log once per consecutive failure. Never throw.            |
| `/stats` connection refused        | Pod dropped from the next snapshot. Breaker retained in case it re-appears later.       |
| `/stats` HTTP != 2xx               | Same as connection refused.                                                             |
| `/stats` body JSON parse failure   | Same as connection refused. Schema is deliberately narrow; unknown fields are ignored.  |
| `/stats` returns `used > capacity` | Weight clamped to `0`, pod remains in the ring; HRW naturally avoids it.                |
| Ring becomes empty                 | `ownerFor` returns `Optional.empty()`. `ShelfInputFile` then returns the delegate stream directly — Trino reads S3. |

## Target selection in `ShelfFileSystem`

Phase-1 choice (b): resolve once per `TrinoInputStream`, not per
`read()`. `ShelfInputFile.newStream()` derives the content key, calls
`resolver.ownerFor(keyBytes)`, and either (a) wraps the delegate stream
in a `ShelfInputStream` with that target's endpoint + breaker, or
(b) returns the raw delegate stream when the ring is empty.

If membership flips mid-stream, the stream keeps talking to the stale
owner until the first Shelf failure, at which point the existing
sticky-delegate logic already fails open. The next `newStream()`
observes the fresh membership.

Key-granularity routing (one owner per read, with ring re-read on
retry per §9.5) is tracked separately for a follow-up ticket.

## JSON parser

Hand-rolled, zero dependencies. Only three top-level fields matter:
`pod_id` (string), `capacity_bytes` (integer), `used_bytes` (integer).
A 60-line depth-aware scanner extracts them and ignores everything
else (pool-level sub-objects, counters, future additions). We
explicitly do not pull in Jackson/Gson: Trino's plugin classloader
does not expose a stable Jackson version and shading it would inflate
the jar.

## Test strategy

Unit (JDK only, no testcontainers):

- `MembershipResolverTest`
  - 3 pods reachable → ring has 3 members with correct derived
    weights.
  - 1 pod unreachable → ring has 2 members, unreachable pod absent.
  - 1 pod returns malformed JSON → same graceful degradation.
  - Empty endpoint list → empty ring, `ownerFor` returns empty,
    no throw.
  - `used > capacity` is clamped to weight `0`.
  - Breakers are retained across refreshes (same `CircuitBreaker`
    instance observed on re-resolve).
- `ShelfFileSystemFactoryTest` — proves the factory uses the resolver
  snapshot (single-pod fixed resolver → target captured in stream).
- `ShelfConfigTest` — new key parse + validation.
- `ShelfFileSystemTest` — refactored to use
  `MembershipResolver.fixed(...)` helper for a deterministic single-pod
  resolver in unit tests.

Integration (deferred to SHELF-21/22 bring-up) and chaos conformance
(owned by this agent per `agents/5-plugin-builder.md`) re-use the same
public surface.
