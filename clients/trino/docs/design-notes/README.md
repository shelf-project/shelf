# shelf-trino-plugin — design notes

This folder holds one design-note markdown per ticket, named
`SHELF-NN-<slug>.md`, per agent-5 Pass 1. The design notes accumulate the
"why" alongside the code; the CHANGELOG captures the "what".

## Skeleton pass (this commit)

Scaffolding-only. No ticket has landed yet; this folder is seeded with a
summary of what the skeleton contains and how it conforms to the applied
ADRs.

### Public surface introduced

- `io.shelf.filesystem.ShelfFileSystem` — implements
  `io.trino.filesystem.TrinoFileSystem`, all methods throw
  `UnsupportedOperationException` with a ticket reference.
- `io.shelf.filesystem.ShelfFileSystemFactory` — `TrinoFileSystemFactory`
  skeleton.
- `io.shelf.filesystem.ShelfInputFile` / `ShelfInputStream` — `TrinoInputFile`
  and `TrinoInputStream` skeletons. `ShelfInputStream` carries the
  fall-through-to-S3 invariant as a class-level contract.
- `io.shelf.client.ShelfHttpClient` — HTTP/2 client skeleton built on
  `java.net.http.HttpClient`, 200 ms default deadline.
- `io.shelf.client.CircuitBreaker` — per-pod fail-open state machine.
  `CLOSED → OPEN` after 5 consecutive failures; 10 s open window;
  `HALF_OPEN` single-probe; exponentially-doubled timer on probe failure.
  Exactly as BLUEPRINT §9.5.
- `io.shelf.client.HashRing` — HRW/rendezvous hashing, skeleton per
  ADR-0002. Owner-of-key lookup.
- `io.shelf.eventlistener.ShelfPrefetchListener` — implements
  `io.trino.spi.eventlistener.EventListener`. `queryCreated` stubs predicate
  extraction from `QueryMetadata`; `queryCompleted` stubs
  `operatorSummaries` ingestion.
- `io.shelf.eventlistener.PrefetchClient` — Phase-2 gRPC client stub.
- `io.shelf.config.ShelfConfig` — reads the 6 BLUEPRINT §6.2 keys with
  defaults; parsing + validation lands in SHELF-10.
- `io.shelf.plugin.ShelfPlugin` — `io.trino.spi.Plugin` implementation;
  registered via `META-INF/services/io.trino.spi.Plugin`.

### ADR compliance

- **ADR-0004 (HTTP/2-only in v1).** `ShelfHttpClient` is the sole
  data-plane client; no Arrow Flight types appear anywhere in the
  classpath. The 1 MB-crossover logic from BLUEPRINT §8.1 is explicitly
  absent.
- **ADR-0005 (drop `SplitCompletedEvent`).** `ShelfPrefetchListener`
  implements only `queryCreated` + `queryCompleted`. There is no
  `splitCompleted` override; the plan-aware prefetch design relies
  exclusively on plugin-side footer observation (Phase 2b-signal-1) and
  post-hoc `operatorSummaries` learning.

### Why this layout

- Packages mirror the runtime responsibilities: `filesystem` is per-worker,
  `eventlistener` is per-coordinator, `client` is transport, `config` is
  glue. The split keeps the per-coordinator vs per-worker concurrency
  stories readable (agent-5 §3 Pass 1 thread-safety story).
- Every stub references the ticket that will replace it. This keeps the
  skeleton compiling while the per-ticket PRs flow in.

### Known deviations from the task spec

- **`io.trino.spi.filesystem.TrinoFileSystem` does not exist.** The task
  spec references that package, but `trino-spi:480` does not contain a
  `filesystem` subpackage — `TrinoFileSystem`, `TrinoFileSystemFactory`,
  `TrinoInputFile`, `TrinoInputStream`, and `Location` all live in
  `io.trino.filesystem` inside the separate `trino-filesystem` artifact
  (library module, not the SPI module). We take a compile-time dependency
  on `io.trino:trino-filesystem:480` at `provided` scope. Trino's plugin
  classloader exposes it at runtime; the plugin still takes zero
  dependencies on implementation-detail packages.
- **JDK 25, not JDK 21.** The task spec asked for Java 21. `trino-spi:480`
  is compiled for JDK 25 (class-file major 69). JDK 21 cannot load these
  classes even during compilation. We set `maven.compiler.release=25` and
  require a JDK 25 toolchain. This is purely a function of Trino's
  upstream bump; sticking to JDK 21 would require pinning to an older
  Trino SPI (≤ ~453).
