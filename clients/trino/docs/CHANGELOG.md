# Changelog

All notable changes to `shelf-trino-plugin` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **SHELF-15**: Parquet footer prefetch. `ShelfFileSystem.newInputFile(.parquet)`
  now best-effort range-GETs the last `shelf.footer.prefetch.kib` KiB
  (default 64, max 256) into the metadata pool via the new
  `io.shelf.client.FooterPrefetcher`. Fail-open: every failure is
  silently counted on the new `io.shelf.client.PrefetchMetrics` and
  never surfaces to Trino. Triggers only when `shelf.enabled` and
  `shelf.prefetch.enabled` are both true. Design note in
  `docs/design-notes/SHELF-15-footer-prefetch.md`.
- **Public API:** `FooterPrefetcher`, `PrefetchMetrics`,
  `ShelfFileSystem.prefetchMetrics()`, `ShelfFileSystem.poolForFooter()`,
  new config key `shelf.footer.prefetch.kib`. `ShelfFileSystemFactory`
  now implements `AutoCloseable` to drain the prefetch executor on
  plugin shutdown.

## [0.0.1] — 2026-04-23

### Added

- Maven project skeleton targeting Trino 480 SPI, Java 21, with
  `trino-spi` + `trino-filesystem` at `provided` scope.
- `io.shelf.filesystem` package: `ShelfFileSystem`, `ShelfFileSystemFactory`,
  `ShelfInputFile`, `ShelfInputStream` — all methods throw
  `UnsupportedOperationException` tagged with the landing ticket. Class-level
  javadoc documents the fail-open invariant from BLUEPRINT §9.5.
- `io.shelf.client` package: `ShelfHttpClient` (HTTP/2, 200 ms default
  timeout, per ADR-0004), `CircuitBreaker` (BLUEPRINT §9.5 state machine
  with stubbed transitions), `HashRing` (HRW skeleton per ADR-0002).
- `io.shelf.eventlistener` package: `ShelfPrefetchListener` (ADR-0005 —
  no `splitCompleted` dependency; uses `QueryMetadata` + `operatorSummaries`)
  and `PrefetchClient` (phase-2 gRPC stub).
- `io.shelf.config.ShelfConfig` — the six BLUEPRINT §6.2 keys with documented
  defaults.
- `io.shelf.plugin.ShelfPlugin` — `io.trino.spi.Plugin` entry point,
  registered via `META-INF/services/io.trino.spi.Plugin`.
- Mirrored JUnit 5 test skeletons — one `@Disabled` test per package tagged
  with its landing ticket.
- Shaded-JAR build via `maven-shade-plugin`; Apache 2.0 license headers
  enforced on every source file via `license-maven-plugin`.
- `docs/config.md`, `docs/design-notes/README.md`, `docs/PR/README.md`,
  top-level `README.md`.

### Explicit non-features

- **Arrow Flight.** Deferred per ADR-0004. `ShelfReadRequest` protobuf
  reserved in `contracts/protobuf/` but not pulled into the plugin.
- **`SplitCompletedEvent` path.** Dropped per ADR-0005. The listener
  implements only `queryCreated` and `queryCompleted`.
- **Wire behaviour.** Every stub body throws `UnsupportedOperationException`.
  Bodies land ticket-by-ticket starting with SHELF-10.
