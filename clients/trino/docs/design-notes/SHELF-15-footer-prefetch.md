# SHELF-15 — Parquet footer prefetch (plugin side)

> **Status:** Shipped behind `shelf.prefetch.enabled=true`.
> **Pool target:** `metadata`.
> **Default window:** 64 KiB. **Max:** 256 KiB. **Min:** 1 KiB.
> **Blueprint refs:** §6.1 (pool taxonomy), §7.3 (metadata prefetch
> budgets), §9.5 (fail-open invariant).

## 1. Problem

Every Iceberg-on-Parquet reader on the Trino worker does the same
opening dance for a new file:

1. Seek to last 8 bytes → read footer length + PAR1 magic.
2. Seek back by `footer_length` → read the thrift footer.
3. Read dictionary / bloom / page-index headers.
4. Finally start reading row-group body.

Steps 1–3 are metadata; they sit in the last few dozen KiB of the
file. On a cold coordinator they are three sequential S3 GETs and
dominate the first-query latency on small tables (Q1/Q2 of our
TPC-DS workload). If Shelf is present but a file is seen for the
first time, those bytes are still cold and cost exactly as much as
raw S3.

The cheapest win Shelf can buy on the plugin side is to _prime_ the
metadata pool with the last N KiB of every Parquet file we're
about to open, and let the foreground read find a cache hit when it
eventually asks for the same bytes.

## 2. Trigger

The trigger sits in `ShelfFileSystem.newInputFile(Location)` — the
single-argument variant that Trino's Iceberg connector always
calls before opening a parquet file. All of the following must be
true, in order:

1. `ShelfConfig.isEnabled()` — plugin globally active.
2. `ShelfConfig.isPrefetchEnabled()` — prefetch subsystem on.
3. The `ShelfFileSystemFactory` built a `FooterPrefetcher` — tied
   to `(isEnabled() && isPrefetchEnabled())` at factory
   construction time, so this flips on the same bit.
4. The path ends with `.parquet` (case-insensitive; we have seen
   `.PARQUET` and `.Parquet` in the wild).
5. `resolver.ownerFor(key)` returns a `Target` — empty ring means
   no pod to route to, and the foreground read path will fail open
   to S3 anyway.

If all five hold, we issue a best-effort async `rangeGet` and return.
If any fail, we return silently. Nothing in this path ever surfaces
an exception back to Trino.

## 3. Pool selection

`ShelfFileSystem.poolFor(Location)` routes `.parquet` to
`Pool.ROWGROUP` because _the body_ of a Parquet file is row-group
data. But the _footer_ is metadata per BLUEPRINT §6.1, and its
residency expectations (long-lived, never evicted for row-group
pressure) match the metadata pool.

The prefetch therefore calls `Pool.METADATA` directly, via the
explicit helper `ShelfFileSystem.poolForFooter()`. We do not change
the existing `poolFor` dispatch — streaming body reads stay on
rowgroup. A single file therefore has bytes in both pools, which
is the intended design. The hit-rate numerator is the metadata
pool; the denominator is `(metadata-footer-hits + metadata-footer-misses)`
exported by `shelfd`.

## 4. Content key

The prefetch must land bytes under the _exact_ key the subsequent
read will query. `ShelfInputFile.deriveContentKey(length, lastModified)`
is the single source of truth; `ShelfFileSystem.maybePrefetchFooter`
calls it. Phase-1 keys are built from `(lastModified, length)`
(SHELF-04 compromise — Trino SPI does not expose S3 ETag). SHELF-07
swaps in the real ETag; no wire-format changes.

## 5. Executor sizing & backpressure

- 2 fixed worker threads.
- 64-slot bounded `LinkedBlockingQueue`.
- `ThreadPoolExecutor.CallerRunsPolicy` on saturation.

Why these numbers:

- **2 threads** because prefetch is I/O-bound on HTTP/2 with full
  multiplexing. A bigger pool would burn heap for task queues that
  the transport can't drain any faster.
- **Bounded queue** because we prefetch on every `newInputFile`,
  and an Iceberg planning burst can produce hundreds of files per
  second on a hot coordinator. An unbounded queue = eventual OOM.
- **CallerRunsPolicy** because we want backpressure to land on the
  _submitter_ (the Trino thread calling `newInputFile`) if Shelf
  is pathologically slow. This costs a Trino thread at most one
  queue-full `rangeGet` (200 ms deadline at worst) before it gives
  up and returns. It never causes a query to fail — the caller
  thread just ran one rangeGet synchronously, same as if prefetch
  weren't wired.

## 6. Failure matrix

Every failure mode maps to "prefetch silently dropped, foreground
read unaffected":

| Failure                                         | Where caught                             | Counter                   |
|-------------------------------------------------|------------------------------------------|---------------------------|
| `ShelfUnavailableException` (5xx, timeout, …)   | `FooterPrefetcher.doPrefetch` catch      | `footerPrefetchFailed`    |
| `IOException` from fetcher                      | `FooterPrefetcher.doPrefetch` catch      | `footerPrefetchFailed`    |
| `RuntimeException`/`Error` from fetcher or JDK  | `FooterPrefetcher.doPrefetch` catch (Throwable) | `footerPrefetchFailed`    |
| `RejectedExecutionException` (post-shutdown)    | `FooterPrefetcher.prefetch` outer catch  | `footerPrefetchFailed`    |
| `IOException` from delegate `length()` / `lastModified()` | `ShelfFileSystem.maybePrefetchFooter` Throwable catch | none (never scheduled) |
| Empty ring (resolver snapshot empty)            | `ShelfFileSystem.maybePrefetchFooter` short-circuit | none (never scheduled) |
| `prefetchBytes <= 0` or `fileLength <= 0`       | `FooterPrefetcher.prefetch` guard        | none (never scheduled) |

The catch-all `Throwable` inside `doPrefetch` is the single
documented exception to the codebase's ban on `catch (Throwable)`;
see BLUEPRINT §9.5. A prefetch-induced `OutOfMemoryError` must be
silently degraded rather than killing a Trino worker.

## 7. Metrics surface

`PrefetchMetrics` exposes three monotonic `long` counters via public
getters:

- `footerPrefetchScheduled` — tasks successfully submitted.
- `footerPrefetchCompleted` — tasks that saw a 2xx `rangeGet`.
- `footerPrefetchFailed` — tasks that terminated exceptionally.

`ShelfFileSystem.prefetchMetrics()` returns the live sink (or an
empty sentinel when no prefetcher is wired). There is **no**
Micrometer, Dropwizard, or OTel dependency — the operator layer
(agent 8) scrapes these via JMX / admin RPC in SHELF-18. Grafana's
`shelf_footer_hits_ratio` panel derives from daemon-side counters
in the metadata pool and crosschecks this counter for sanity.

## 8. Out of scope

- **Page-index prefetch** — lands with SHELF-17 once we can
  confirm we pay for footer prefetch in query-plan time. Keeping
  this ticket narrow makes the benchmarker's A/B unambiguous.
- **Row-group prefetch based on `SplitCompletedEvent`** — Phase 2b
  of BLUEPRINT §7.2; different code path, lives in
  `ShelfPrefetchListener`.
- **Trino-native metrics integration** — SHELF-18.
- **Telemetry for prefetch hit / miss correlation** — the
  numerator / denominator sit on the daemon; the plugin only needs
  to know "did I submit the prefetch" so CI has an observable
  counter.

## 9. Acceptance check

- ✅ `FooterPrefetcherTest` — 5 cases covering happy path,
  `ShelfUnavailableException`, small-file clamp, zero-window
  no-op, and `Throwable` catch-all.
- ✅ `ShelfFileSystemTest` — 5 new cases covering parquet trigger,
  ORC non-trigger, empty ring, delegate pass-through, and the
  disabled-config path.
- ✅ `ShelfConfigTest` — 6 new cases covering default, parse,
  lower/upper bound, and rejection of non-numeric / out-of-range
  values.
- ✅ `mvn test` — 94 tests pass (78 baseline + 16 new, 2 skipped
  JUnit placeholders untouched).
- 🟡 Grafana `shelf_footer_hits_ratio > 90%` — verified on the
  smoke cluster by the benchmarker (agent 7) during
  SHELF-15-smoke. Not a plugin-side unit test.
