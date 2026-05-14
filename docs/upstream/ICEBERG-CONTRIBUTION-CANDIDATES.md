# Apache Iceberg contribution candidates for the Shelf maintainer

> Research-grounded shortlist of upstream Apache Iceberg PRs the Shelf BDFL
> could author that simultaneously (a) unblock or accelerate Shelf's read-cache
> path on Trino-on-Iceberg, (b) are a clean win for **every** Iceberg user and
> every engine (Trino, Spark, Flink, Dremio, StarRocks, PyIceberg, iceberg-rust
> consumers), and (c) are small enough to actually land as first-time-Iceberg
> contributor PRs.
>
> Research date: 2026-05-14. Verified live against the `apache/iceberg` (Java
> reference impl), `apache/iceberg-rust` (Rust SDK), and `apache/iceberg-python`
> (PyIceberg) repositories on github.com on that date. Every PR number, issue
> number, file path, and maintainer handle in this document was checked against
> the upstream page during this research run; see the **Verification log**
> appendix at the end for the exact URLs hit.
>
> Sibling document: `docs/upstream/TRINO-CONTRIBUTION-CANDIDATES.md` covers the
> Trino-side PRs. Several Iceberg PRs in this document **pair** with a Trino-side
> PR — the cross-reference matrix in §5 maps which composes with which.

---

## §1 — TL;DR

| Rank | Proposal | Repo | Scope | Ecosystem-side win | Shelf-side win | When |
|---|---|---|---|---|---|---|
| 1 | **Make `ASYNC_PLANNING_POOL` in the REST reference scan-planning impl bounded + configurable** (replace `Executors.newSingleThreadExecutor()` in `core/src/main/java/org/apache/iceberg/rest/CatalogHandlers.java:125` with a sized pool, configurable via system property or builder) | `apache/iceberg` (Java) | ~80–150 LOC prod + ~150 LOC tests | Every engine (Trino, Spark, Flink, Dremio, StarRocks) that talks to the reference REST impl benefits — today the server-side planner is a single-thread bottleneck per `chenwyi2`'s Dec-2025 review comment on PR #14480 | Shelfd's planned plan-endpoint server has a precedent for parallel manifest evaluation; Trino REST scan-planning client (sibling P2 in `TRINO-CONTRIBUTION-CANDIDATES.md`) gets a non-blocking server to talk to | Phase 1 (smallest, < 200 LOC, mechanical, lowest reviewer-controversy) |
| 2 | **Fix `ParallelIterable` O(N²) `ConcurrentLinkedQueue.size()` performance bug** (re-open the closed PR #11895 thread per maintainer guidance — needs a thread-safe size tracker, not the original counter that `@RussellSpitzer` flagged) | `apache/iceberg` (Java) | ~80–120 LOC prod + ~100 LOC tests + JMH benchmark | Trino, Spark, Flink all consume `ParallelIterable` for `planFiles` — issue #14790 (open, `agoncharuk`) reports seconds of `planFiles` time on 192-CPU machines on Trino | Trino-side `planFiles` becomes faster even before REST scan planning lands — keeps shelfd's metadata cache warm with fewer redundant manifest reads in the worst case | Phase 1 OR Phase 2 (depending on whether the contributor wants to engage with `RussellSpitzer`'s concurrency objection in PR review) |
| 3 | **Implement IRC Events Endpoint Request/Response Objects** (`apache/iceberg#13580`, marked `good first issue`, opened by `@c-thiel`, last updated 2026-05-12) | `apache/iceberg` (Java + spec) | ~150–250 LOC prod + ~100 LOC tests + OpenAPI spec update | Every REST-catalog operator gets a standardised event SPI for snapshot creation / table-update notifications — replaces today's per-engine polling | Direct enabler for shelfd's `compaction_rewarm.rs` / `rewarm_poller.rs`: replaces the metadata.json polling worker with a push-based snapshot-event subscription, eliminating the polling lag (~30s today) for snapshot-delta cache invalidation per ADR-0011 | Phase 2 (enables the Shelf §8.1 snapshot-delta invalidation roadmap) |

> **Phase 1 single recommendation:** open the **`ASYNC_PLANNING_POOL` bounded
> + configurable executor** PR. It is < 200 LOC, mechanical, addresses a
> reviewer-acknowledged bottleneck (`chenwyi2`'s comment on the merged #14480
> referenced the single-thread executor as a follow-up), there is no open PR
> against it as of 2026-05-14, and it composes directly with the sibling Trino
> P2 (REST scan planning client adoption) — both wins compound.

---

## §2 — Methodology

### How candidates were sourced

1. The user-supplied candidate list (12 items) was treated as input; every item was independently re-verified upstream on 2026-05-14.
2. The `apache/iceberg` repo was swept for: open issues with labels `good first issue`, `performance`, `rest-catalog`, sorted by `updated-desc`, last 90 days; open PRs touching `core/src/main/java/org/apache/iceberg/rest/`; recent merged PRs in the planning subsystem.
3. The `apache/iceberg-rust` repo was swept for the same labels plus the `[Discussion]` umbrella issue #1797 ("Reduce the need for iceberg-rust forks") which catalogues the contributor pain points.
4. The `apache/iceberg-python` repo was swept for incremental-scan API gaps (issue #2634).
5. The reference REST scan-planning implementation in `core/src/main/java/org/apache/iceberg/rest/CatalogHandlers.java` was fetched directly from `raw.githubusercontent.com` and read line-by-line for the executor configuration (line 125) and the `planTableScan`/`asyncPlanFiles` call sites (line 1055).
6. For each candidate the **state**, **last activity date**, **author**, **labels**, **reviewers**, and **maintainer responses** were re-read from the live page — not from in-repo memory.

### Verification commands

The full verification log lives at the end of this document (the **Appendix**). The short version: each upstream PR/issue page was fetched live with `WebFetch`, the relevant section quoted with author + date, and the date stamp recorded. File contents were fetched from `raw.githubusercontent.com/apache/iceberg/main/...`.

### What is *not* verified and needs re-check before opening any PR

| Gap | Why not verified | How to close it |
|---|---|---|
| Whether `apache/iceberg-rust` has an in-flight PR adding a sized executor analogous to candidate P1 | Time-bounded on this run; only Java repo was swept exhaustively for `ASYNC_PLANNING_POOL` | Run `git log -- crates/iceberg/src/catalog/rest/` on the Rust SDK before opening P1 |
| Whether the IRC Events Endpoint has had concrete spec text drafted in `open-api/rest-catalog-open-api.yaml` since the issue was opened | The OpenAPI spec file is large (~5k lines) and the most recent commit touching it is the `RemoveKey` cleanup, not events | `git blame open-api/rest-catalog-open-api.yaml` for any `events` / `notifications` keys |
| Exact `ParallelIterable` consumer call sites in Spark and Flink (the cross-engine impact framing) | Iceberg core is the source-of-truth and Spark/Flink consume it transitively; the issue author cites Trino specifically | Read `org.apache.iceberg.util.ParallelIterable` `usages()` in IntelliJ before drafting the PR body |

---

## §3 — Ranked proposals

### P1 — Make `ASYNC_PLANNING_POOL` bounded + configurable in the REST reference impl

| Field | Value |
|---|---|
| One-line | Replace `private static final ExecutorService ASYNC_PLANNING_POOL = Executors.newSingleThreadExecutor()` in `core/src/main/java/org/apache/iceberg/rest/CatalogHandlers.java:125` with a sized pool (`Executors.newFixedThreadPool(N, ThreadPools.newDaemonThreadFactory("iceberg-rest-async-plan"))`), wired through a system property `iceberg.rest.async-planning-threads` defaulting to `Runtime.getRuntime().availableProcessors()`. |
| Status today | The reference impl ships a server-side scan-planning handler in `CatalogHandlers.planTableScan()` (line 1055) and `asyncPlanFiles()` (line ~1090) that submits tasks to a singleton executor `ASYNC_PLANNING_POOL`. PR `apache/iceberg#14480` (merged 2025-11, `singhpk234`) added the handler. PR `apache/iceberg#15863` (merged 2026-04, `singhpk234`) made the poll timeout configurable. PR `apache/iceberg#15572` (merged 2026-03-17, `singhpk234`, milestone 1.11.0) added a table-level override. **The single-thread executor was never made configurable.** No open PR addresses it (verified by searching `pulls?q=is:pr+is:open+ASYNC_PLANNING_POOL` on 2026-05-14: zero matches; same query for `CatalogHandlers` returns 3 unrelated open PRs). |
| Proposed change | (a) Replace the hard-coded `newSingleThreadExecutor()` with a sized pool whose size is read from a system property (default = `availableProcessors()`); (b) keep the singleton field but lazy-init via a `@VisibleForTesting`-guarded method so unit tests can shut it down between cases; (c) add `@SuppressWarnings("PMD.AvoidUsingHardCodedIP")`-style javadoc explaining the choice of system property over Iceberg's table properties (because the executor is process-wide, not per-table). |
| Win for ecosystem | Every catalog implementation that subclasses or wraps `RESTCatalogAdapter` (the in-tree reference impl, plus Polaris, Lakekeeper, Tabular's downstream forks) inherits the fix. Per `@chenwyi2`'s December-2025 review comment on PR #14480: *"the executor is single-threaded, which seems like it could be a bottleneck for production deployments"*. Today's behaviour means a 192-CPU REST-catalog server processes scan-plan requests **serially across all clients** — verifiably wrong for any production deployment with concurrent Trino / Spark / Flink workloads. |
| Win for Shelf | When shelfd implements its own scan-planning endpoint server (per the rewritten `TODO-fix-shelf-performance.md` §6), the reference impl serves as the design reference. A bounded-pool reference reduces the chance shelfd's first-cut implementation copies the single-thread anti-pattern. Indirectly: the sibling Trino P2 (REST scan-planning client adoption) becomes more attractive once the reference impl is no longer a serial bottleneck — engines have less reason to keep planning client-side. |
| Estimated scope | Production: ~80–120 LOC (the executor change + system-property plumbing + lazy-init). Tests: ~100–150 LOC (extend `TestRESTCatalog` style tests to cover concurrent `planTableScan` calls; verify the property is honoured). No spec changes (this is reference-impl only, not OpenAPI surface). |
| Reviewers / maintainers | `@singhpk234` (authored #14480 + #15863 + #15572 — owner of the REST scan-planning subsystem); `@nastra` (active 2026 reviewer/merger of REST work); `@RussellSpitzer` (Apache Iceberg PMC member, frequent merger of perf / API PRs — see PR #15064 as exemplar). |
| Upstream-acceptance risk | **Low.** The single-thread executor was a pragmatic choice for the initial reference impl; making it configurable is the natural evolution. Risk vector: bikeshedding on the property name (system property vs catalog property vs HTTP-server property) — pre-flight in the dev mailing list to settle this before the PR. Cite `@chenwyi2`'s prior comment in the PR body to anchor the conversation. |
| First-PR friendliness | **Excellent.** Single file, single class, mechanical change, well-defined bug. Mirrors the shape of `joyhaldar`'s PR #15064 (155 LOC, 2 files, 4 commits, merged in 6 days) — a textbook first Iceberg PR. |

---

### P2 — Fix `ParallelIterable` O(N²) `ConcurrentLinkedQueue.size()` perf bug

| Field | Value |
|---|---|
| One-line | Re-open the closed PR `apache/iceberg#11895` line of work per `@RussellSpitzer`'s Dec-2025 guidance on issue #14790 — replace the per-`hasNext` `ConcurrentLinkedQueue.size()` (which is itself O(N)) with a `LongAdder`-backed size tracker that addresses Russell's concurrency-window concern. |
| Status today | Issue `apache/iceberg#14790` (OPEN, opened 2025-12-07 by `@agoncharuk`, last updated 2025-12-19) re-raises the perf issue: *"unreasonably long planning times (seconds) when using Trino on large tables on machines with 192 CPUs. Profiling shows that the time is consumed by `ConcurrentLinkedQueue.size()` which is called on each `hasNext()`, and internally runs `queue.size()` multiple times (2 * Number of processors times), which essentially makes the iteration O(N²)."*. Original PR `apache/iceberg#11895` was closed without merge — `@RussellSpitzer` raised a concurrency-correctness concern: the new `queueSizeTracker` introduces a window where multiple threads can each see `tracker < limit`, all add to the queue, and overshoot. The path forward is **not** to copy #11895 verbatim but to address Russell's concern in the PR design. |
| Proposed change | Two designs to consider in the dev-list discussion: (a) `AtomicInteger` size tracker with `get-and-conditionally-add` CAS loop for the bounded-add critical section (preserves overshoot bound at exactly N + numThreads); (b) accept Russell's larger window but document it explicitly + raise the queue-full threshold to compensate. Open as a draft PR with both designs in the description; let the dev list and Russell pick. JMH benchmark in the PR demonstrating the O(N²) → O(N) win on `ParallelIterableBenchmark` (does not exist today; new benchmark is part of the PR). |
| Win for ecosystem | `org.apache.iceberg.util.ParallelIterable` is the workhorse for any consumer that paginates manifest evaluation: Trino's `IcebergSplitSource`, Spark's `SparkInputPartitions`, Flink's `IcebergFilesCommitter`, Dremio's reflection refresh. Issue author's profiling data shows seconds of planning time on 192-CPU machines — every engine on every workload above a few thousand manifests benefits proportionally to CPU count. |
| Win for Shelf | Reduces Trino's coordinator-side planning time even on the **direct-S3** (non-cached) path, lowering the floor for what shelfd's metadata pool needs to beat. Indirectly stabilises shelfd's cold-cache regime where heavy planning correlates with metadata-pool warm-up bursts. |
| Estimated scope | Production: ~80–120 LOC (replacing `queue.size()` with a tracker + the bounded-add CAS path + a couple of `@VisibleForTesting` hooks). Tests: ~100 LOC (concurrent-add stress tests using `Awaitility` + a deliberate-overshoot assertion). JMH benchmark: ~80 LOC of new JMH file at `core/src/jmh/java/org/apache/iceberg/util/ParallelIterableBenchmark.java`. |
| Reviewers / maintainers | `@RussellSpitzer` (author of the concurrency-window concern — must approve), `@agoncharuk` (issue author, motivated reviewer), `@nastra`, `@manuzhang`. |
| Upstream-acceptance risk | **Medium.** The bug is real and uncontested. The risk is concurrency design — Russell's objection on #11895 was substantive, not stylistic. Mitigation: open as a *draft* PR with the JMH numbers up front and Russell's concern addressed in the design. If Russell signs off on the design before code review, the PR sails through. If not, a third design (e.g. lockless `LongAdder` with epoch-based reconciliation) may be needed. |
| First-PR friendliness | **Medium.** Smaller LOC than P1 but the concurrency design conversation is non-trivial — not a drive-by PR. Recommended for someone who wants to engage substantively with a senior PMC member. Realistic time-to-merge: 6–10 weeks including the design discussion. |

---

### P3 — IRC Events Endpoint: implement Request & Response Objects

| Field | Value |
|---|---|
| One-line | Implement the request/response object schema and Java POJO classes for the IRC (Iceberg REST Catalog) Events Endpoint per open issue `apache/iceberg#13580` — the first of three `good first issue`-tagged tickets (#13580, #13581, #13582) authored by `@c-thiel` for a push-based snapshot-event SPI. |
| Status today | Issue `apache/iceberg#13580` (OPEN, opened 2025-07-17 by `@c-thiel`, last updated 2026-05-12, labelled `good first issue`) tracks the request/response schema work. Sister issues #13581 (server-side catalog handlers) and #13582 (test harness) are also open and tagged `good first issue`. The umbrella tracking issue #13707 lists this as one of the missing reference-impl features. As of 2026-05-14 there is **no open PR** linked to #13580 (verified by searching `is:pr` for `#13580` references — zero matches). The 10-month idle window since opening is a strong signal that the maintainer (`c-thiel`) wants someone to pick it up. |
| Proposed change | (a) Add `org.apache.iceberg.rest.requests.{TableEvent,SnapshotEvent,...}` POJO classes with Jackson annotations matching the existing IRC patterns (see `LoadTableResponse` for the convention); (b) extend `open-api/rest-catalog-open-api.yaml` with the `events` endpoint definition (~80 LOC of YAML); (c) add JSON serialization round-trip tests in the style of `TestLoadTableResponseParser`. Defer the server-side handlers (#13581) and test harness (#13582) to follow-up PRs — explicitly cite this in the PR body. |
| Win for ecosystem | Replaces today's per-engine snapshot-detection polling with a standardised push interface. Concretely: Trino's `iceberg.metadata-cache` invalidation, Spark's `RefreshTableExtension`, and Dremio's metadata reflection refresh all currently poll `metadata.json` on a TTL — wasteful on tables with few writes, lagging on tables with many. A standard event endpoint lets every engine subscribe once and react in milliseconds rather than seconds. |
| Win for Shelf | **Direct enabler** for the rewritten `TODO-fix-shelf-performance.md` §8.1 snapshot-delta cache invalidation: shelfd's `rewarm_poller.rs` polls `metadata.json` every 30s today (the same wasteful pattern). With the events endpoint live, shelfd subscribes once per table, receives `SnapshotEvent` push notifications, and triggers `compaction_rewarm.rs` against the new snapshot's added files within seconds of the commit. The ADR-0011 content-addressed cache key auto-invalidates stale entries; the events endpoint just removes the polling lag from the warm-up side. |
| Estimated scope | Production: ~150–200 LOC of POJOs + ~50–80 LOC of OpenAPI spec YAML. Tests: ~100 LOC of round-trip serialization tests. **Spec change in `open-api/` triggers Apache governance**: per the contribution guide, `open-api/rest-catalog*` changes require a separate dev-list vote (ASF code modification model, no lazy consensus). Frame the PR description accordingly: scope = "request/response schema only, server-side handlers deferred to #13581" lowers the spec-change surface. |
| Reviewers / maintainers | `@c-thiel` (issue author, will be primary reviewer), `@nastra` (active REST-catalog reviewer), `@danielcweeks` (REST-catalog spec maintainer); for the OpenAPI spec changes, expect input from `@RussellSpitzer` and `@danielcweeks`. |
| Upstream-acceptance risk | **Medium.** The `good first issue` label is a strong invitation. Risk vector: the spec-change governance (dev-list vote) lengthens the calendar time even when the technical change is small. Mitigation: keep the OpenAPI delta minimal, offer to pull the YAML changes into a separate PR if reviewers prefer to vote on them in isolation. |
| First-PR friendliness | **Excellent for the POJO-only subset; medium for the full PR including spec.** If the contributor wants a fast first merge, split into two PRs: (1) Java POJOs only (no spec change, no vote needed, < 150 LOC) and (2) follow-up OpenAPI spec change. PR (1) is a textbook first PR. |

---

### P4 — Iceberg-rust: complete `ManifestEvaluator` expression coverage for transforms + decimals + timestamptz

| Field | Value |
|---|---|
| One-line | Audit `apache/iceberg-rust/crates/iceberg/src/expr/visitors/manifest_evaluator.rs` against the Java `ManifestEvaluator` for missing predicate handling — specifically `bucket(N)` / `truncate(W)` partition transforms, decimal/timestamptz literal coercion, and nested-struct field access — and ship the gap-fillers. |
| Status today | iceberg-rust shipped its first `ManifestEvaluator` in 0.3.0 (issues #348 and #350 closed by `@viirya`, `@marvinlanhenke`, 2024). The base `Eq`, `In`, `IsNull`, `NotIn`, `NotEq` paths exist. The newer `apache/iceberg-rust#1797` "Reduce the need for forks" discussion (OPEN, opened 2025-10-28 by `@alamb`, 17 👍 / 28 ❤️ reactions, last updated 2026-05-07) catalogues exactly this pain — `@Sl1mb0`: *"there are a lot of legacy constructs in the iceberg-rust crate that are (in my view) java-flavored"*; `@mbutrovich`: *"My Comet PR just today passed all Iceberg Java tests via iceberg-rust"* but only after months of backfilling. The `ArrowReader` parity work is also active per issue #1845 (open). Coverage gaps are concrete enough that any contributor can pick a single transform/type and ship the fix as one PR. |
| Proposed change | Pick **one** narrow gap to start (recommendation: `bucket(N)` partition transform predicate evaluation, since the Java side has it via `BucketTransform.canTransform` and Rust has the `Transform::Bucket` enum but not the predicate-evaluator side). Implement the visitor extension + tests + cross-reference the Java `BucketUtil` constants. Subsequent PRs pick off the next gap. |
| Win for ecosystem | Every Rust consumer of iceberg-rust — Comet (Spark on Rust), DataFusion-iceberg, Polars Cloud's iceberg backend, Cube.dev, vector-iceberg — gets the missing predicate support. Today's incomplete coverage is the #1 reason `@alamb` cites in #1797 for projects forking the crate. |
| Win for Shelf | Shelfd's `filter_service.rs` (G4 predicate-pushdown probe) needs a Rust-side manifest evaluator to push predicates down before fetching footers. Today shelfd cannot natively prune manifests; it relies on the Iceberg engine (Trino) doing the pruning before the read reaches the shim. With iceberg-rust's evaluator complete, shelfd can in-process prune manifests on `decoded_meta.rs` reads, dropping unnecessary footer fetches before they hit the cache. |
| Estimated scope | Per-transform PR: ~80–150 LOC + ~100 LOC of unit tests + a parametric integration test against `tests/parquet/`. Multiple PRs over 2–3 months; each is independent. |
| Reviewers / maintainers | `@liurenjie1024` (does the bulk of reviews per `@Xuanwo`'s comment in #1797), `@Xuanwo` (PMC member, also reviews actively), `@marvinlanhenke` (authored prior `ManifestEvaluator` work), `@mbutrovich` (Comet maintainer, motivated by parity). |
| Upstream-acceptance risk | **Low** for any single transform/type gap — the work is mechanical, the patterns are well-established, and the maintainers are explicitly soliciting contributions per #1797. Risk vector: review queue is the binding constraint per `@Xuanwo`'s comment *"The most limited resource for us is review"* — calendar time may be 4–8 weeks per PR even when the change is small. |
| First-PR friendliness | **Excellent if scoped tight.** Pick one transform, write the PR, ship. The `ASF ICLA` (separate from Trino's CLA — see §7) covers all three Apache repos in one signature. |

---

### P5 — `IncrementalChangelogScan`: add delete-file support + partition-spec evolution coverage

| Field | Value |
|---|---|
| One-line | Implement delete-file (positional + equality) support in `apache/iceberg`'s `IncrementalChangelogScan` per open issue `apache/iceberg#14264` so changelog scans correctly classify rows as `{added, deleted, updated}` even when the table uses MOR (merge-on-read) deletes. |
| Status today | Issue `apache/iceberg#14264` (OPEN, opened 2025-09 by `@hsiang-c`, active discussion through Q1 2026) tracks the gap. The current `IncrementalChangelogScan` only handles append-only changelog entries; tables with `format-version=2` MOR deletes get incomplete changelogs. Active maintainer guidance from `@RussellSpitzer` and `@flyrain` in the thread suggests a phased approach: (a) positional deletes first, (b) equality deletes second, (c) partition-spec evolution in a third PR. The PyIceberg counterpart (`apache/iceberg-python#2634`) is also open and waiting on the Java semantics to land first. |
| Proposed change | Phase (a) only as the first PR: extend `BaseIncrementalChangelogScan.planFiles()` to consume `DeleteFile` entries from each snapshot's manifest delta, attach them to the corresponding data file scan tasks via `FileScanTask.deletes()`, and assert the row-classification semantics against the existing `TestIncrementalChangelogScan` suite. Defer equality deletes and partition-spec evolution to follow-up issues — call this out in the PR body. |
| Win for ecosystem | CDC consumers (Flink CDC, Debezium-Iceberg, dbt-iceberg snapshot mode, Estuary Flow) all rely on `IncrementalChangelogScan` to surface row-level changes. Today's gap means MOR-mode tables produce incorrect changelogs, blocking CDC adoption on the ~50% of Iceberg tables that use format-v2 deletes. |
| Win for Shelf | Direct enabler for the rewritten `TODO-fix-shelf-performance.md` §8.1 snapshot-delta cache invalidation. With correct `{added, rewritten, deleted}` classification, shelfd's `compaction_rewarm.rs` knows precisely which content-addressed keys to evict (deleted), which to pre-warm (added), and which to leave alone (rewritten). Today shelfd over-evicts on snapshot transitions because it cannot distinguish rewritten from deleted. |
| Estimated scope | Production: ~200–300 LOC for phase (a). Tests: ~200 LOC of `TestIncrementalChangelogScan` extensions. Possibly a doc PR in `docs/spark-changelog-procedure.md`. |
| Reviewers / maintainers | `@flyrain` (active changelog-scan reviewer), `@RussellSpitzer` (approves planning PRs), `@hsiang-c` (issue author). |
| Upstream-acceptance risk | **Medium.** The semantics for changelog-with-deletes are non-trivial — what does a MOR-deleted-then-re-inserted row look like in the changelog? Mitigation: agree the semantic with `flyrain` in the issue thread before writing code; cite the Spark `Change Data Capture` literature in the PR body. |
| First-PR friendliness | **Not as a first PR.** Larger LOC, semantic discussion required, multi-phase work. Save for after one merged Iceberg PR establishes trust (e.g. P1 first). |

---

### P6 — Iceberg-rust: implement Incremental Append Scan (collaborate on open PR #2337)

| Field | Value |
|---|---|
| One-line | Help land `apache/iceberg-rust#2337` (OPEN, "feat: Incremental append scan", actively under review as of 2026-05-14) which implements the Rust-side `IncrementalAppendScan` matching the Java `BaseIncrementalAppendScan` behaviour — incorporates feedback from the closed prior attempt #2153. |
| Status today | Tracking issue `apache/iceberg-rust#2152` (OPEN), umbrella epic for incremental reads. Closed-stale prior PR `apache/iceberg-rust#2153` carried detailed maintainer feedback. Active PR `apache/iceberg-rust#2337` (OPEN, "feat: Incremental append scan") incorporates the prior feedback and is under active review with detailed technical commentary. The work is already ~80% there; what's needed is help landing it. |
| Proposed change | (a) Review #2337 with a focus on Java-parity correctness; (b) offer fix-up commits on the PR author's branch addressing reviewer comments; (c) once merged, follow up with the `IncrementalChangelogScan` Rust port (parallels the Java work in P5). The first contribution is collaborative review + fix-up commits, not a new PR. |
| Win for ecosystem | Rust consumers (Comet, DataFusion-iceberg, etc.) get incremental-scan support, parity with Java. The PyIceberg counterpart #2634 may follow with a similar shape, benefiting Python consumers too. |
| Win for Shelf | Shelfd's `compaction_rewarm.rs` and `mv_registry.rs` are Rust modules. With iceberg-rust's `IncrementalAppendScan` complete, shelfd can natively enumerate added files between snapshots — directly powering the snapshot-delta invalidation flow without going through the Trino engine. |
| Estimated scope | Review + fix-up commits on someone else's PR: ~50–100 LOC of fix-ups across 2–3 review rounds. The follow-up `IncrementalChangelogScan` Rust port: ~300 LOC + tests. |
| Reviewers / maintainers | `@liurenjie1024`, `@Xuanwo`, `@marvinlanhenke`. |
| Upstream-acceptance risk | **Very low** for fix-ups on someone else's open PR. **Medium** for the follow-up changelog scan port. |
| First-PR friendliness | **Excellent** as collaboration on #2337 — the PR is open and welcoming review. Becomes a Shelf-author Iceberg contribution as soon as a fix-up commit is merged into the PR branch. |

---

### P7 — Add `MetricsConfig` field declaring per-column bloom-filter presence

| Field | Value |
|---|---|
| One-line | Extend `org.apache.iceberg.MetricsConfig` with a per-column `hasBloomFilter` field populated from the Parquet writer's `WriteContext`, surfaced through `Schema.findField(...).bloomFilterEnabled()` so engines and caches can pre-warm bloom filters without parsing every footer. |
| Status today | `MetricsConfig` (live at `core/src/main/java/org/apache/iceberg/MetricsConfig.java`, verified 2026-05-14) currently exposes per-column metrics modes (`none`, `counts`, `truncate(N)`, `full`) but not bloom-filter presence. The Parquet write path in `org.apache.iceberg.parquet.Parquet.WriteBuilder` configures bloom filters via `parquet.bloom.filter.enabled.column.<name>` properties at write time — this information is not surfaced anywhere in Iceberg metadata. Open issue `apache/iceberg#16218` (OPEN, "Implement metrics evaluators that work directly with `ContentStats`") is the closest parallel discussion of `MetricsConfig` extension. |
| Proposed change | (a) Add `bloomFilterEnabledColumns()` method to `MetricsConfig`; (b) populate it from Parquet's `WriteContext` when writing data files; (c) plumb through `DataFile.bloomFilterColumnIds()` analogous to `nullValueCounts()` etc. so it survives manifest serialisation. **This is a spec-adjacent change** — if the field lands in `DataFile`, the format spec needs an update. Discuss on the dev list first. |
| Win for ecosystem | Trino's bloom-filter pushdown (`parquet.use-bloom-filter`, default true in Trino 480) currently inspects every Parquet footer to discover bloom-filter columns. With this metadata at the `DataFile` level, query planners can skip footer reads for files with no bloom filter on the relevant column. Spark's vectorised reader benefits identically. |
| Win for Shelf | Shelfd's `parquet_admit.rs` (bloom-aware footer admission) currently relies on a heuristic to decide which footers to admit to the metadata pool. With explicit bloom-presence metadata at the manifest level, admission can be exact: prefer footers from files with bloom filters on hot-predicate columns. This is exactly the §6 framing in the rewritten `TODO-fix-shelf-performance.md` for cache-friendly metadata enrichment. |
| Estimated scope | Production: ~150–200 LOC across `MetricsConfig`, `DataFile`, `Parquet.WriteBuilder`, and the manifest serialisation path. Tests: ~150 LOC. **Format spec PR** in `format/spec.md` if the field lands in `DataFile` — separate dev-list vote required per the Iceberg contribution guide's "Behavioral and functional changes to a specification" rule. |
| Reviewers / maintainers | `@RussellSpitzer`, `@nastra`, `@huaxingao` (active 2026 reviewer of API + metrics work, see PR #15064 review). |
| Upstream-acceptance risk | **Medium-high.** Spec-adjacent work has the dev-list vote overhead. The bloom-filter discovery cost is real but maintainers may prefer an alternative design (e.g. expose at the `Snapshot` summary level rather than `DataFile`). Mitigation: discuss the design on `dev@iceberg.apache.org` *before* writing code. |
| First-PR friendliness | **Not as a first PR.** Spec-level scope, multi-file change, governance overhead. Phase 3 candidate. |

---

## §4 — Recommended trajectory

### Phase 1 (weeks 1–4): one merged PR for trust

**Strongly recommended single pick: P1 (`ASYNC_PLANNING_POOL` bounded + configurable)**.

Why this one:

1. **No open competitor PR** as of 2026-05-14 (verified by exhaustive search of `apache/iceberg/pulls` for `ASYNC_PLANNING_POOL` and `CatalogHandlers` queries — three open PRs unrelated to this change).
2. **Single file, single class, mechanical** — modelled on `joyhaldar`'s PR #15064 which merged in 6 days at 155 LOC.
3. **Cited maintainer pain** — `@chenwyi2`'s December 2025 review comment on PR #14480 explicitly flagged the single-thread executor as a follow-up; PR author has the cover.
4. **Composes with sibling Trino P2** — once Trino-side REST scan-planning client adoption lands (sibling file, Phase 2), this Iceberg-side fix is what makes it usable in production.
5. **Dev-list pre-flight is a single email** — "I'd like to make `ASYNC_PLANNING_POOL` in `CatalogHandlers` configurable, defaulting to `availableProcessors()`, controlled by `iceberg.rest.async-planning-threads` system property. Any objections to the property name or default?"

**Estimated time-to-merged-PR: 3–5 weeks** from ICLA signature to merge, assuming a clean single-review cycle and one round of bikeshedding on the property name.

**Fallbacks if P1 hits an unexpected snag:**

1. **P3** (IRC Events Endpoint POJO subset) — splits into a no-spec-change first PR that ships fast.
2. **P6** (collaborate on iceberg-rust #2337) — fix-up commits on someone else's open PR are the absolute-fastest route to a Shelf-attributable Iceberg merge.
3. **P4** (iceberg-rust single-transform predicate gap) — pick `bucket(N)` and ship one ~150 LOC PR.

### Phase 2 (months 2–4): the Shelf-aligned high-leverage PR

**P3 (IRC Events Endpoint Request/Response Objects).**

This is the single biggest unblock for the rewritten `TODO-fix-shelf-performance.md` §8.1 snapshot-delta cache invalidation. Once shelfd can subscribe to push-based snapshot events, the entire `rewarm_poller.rs` polling pattern (~30s lag today) collapses to a millisecond-latency push subscription. The win compounds across every other engine that today polls `metadata.json`.

By the time this PR opens you should have:

- One merged Iceberg PR (Phase 1) so `@c-thiel` and `@nastra` recognise the contributor name.
- A short discussion in `#iceberg-dev` Slack and on `dev@iceberg.apache.org` confirming the spec scope.
- A draft of the OpenAPI spec change in a separate PR-prep branch so the spec discussion can move in parallel with the POJO code.

**Estimated time-to-merged-PR: 6–10 weeks** for the POJO-only first PR; 12–16 weeks for the full spec + POJO PR if not split.

### Phase 3 (months 4–9): the ambitious wins

After **two or three merged Iceberg PRs**, the credibility budget exists to attempt:

- **P5 (`IncrementalChangelogScan` delete-file support)** — large, semantically tricky, requires alignment with `@flyrain` and `@RussellSpitzer` on the row-classification semantics. Massive payoff for Shelf §8.1 because it gives shelfd accurate `{added, rewritten, deleted}` classification without engine round-trips.
- **P7 (`MetricsConfig` bloom-filter presence)** — spec-level change, dev-list vote required. The right design conversation to lead after building review trust.
- **P2 (`ParallelIterable` perf fix)** — if no one else has tackled it by then; the concurrency design conversation with `@RussellSpitzer` is high-quality but front-loaded.
- **P4 (iceberg-rust `ManifestEvaluator` completeness sweep)** — open multiple small Rust PRs in parallel, building a track record on the Rust side.

**Estimated time horizon: 6–9 months** from today for the full Phase 3 sweep.

---

## §5 — Cross-references

### Composition with the rewritten `TODO-fix-shelf-performance.md`

> A sibling worker rewrote `TODO-fix-shelf-performance.md` in parallel with this
> document. The cross-references below assume the §6 (REST scan-planning) and
> §8.1 (snapshot-delta invalidation) section structure described in the
> user-supplied context.

| Iceberg PR | Accelerates / unblocks |
|---|---|
| **P1** `ASYNC_PLANNING_POOL` configurable | Direct enabler for §6 — when shelfd implements its own scan-planning endpoint, the reference impl is no longer the upper-bound on what production catalogs can do, and shelfd can ship without competing-against-broken-baseline framing. |
| **P2** `ParallelIterable` perf fix | Indirect — reduces Trino-side `planFiles` time on the direct-S3 fallback path, lowering the bar Shelf must beat on cold-cache reads. |
| **P3** IRC Events Endpoint | **Direct primary enabler** for §8.1 — replaces shelfd's `rewarm_poller.rs` 30s polling with millisecond-latency push events for snapshot-delta invalidation. The single highest-leverage upstream PR for the §8.1 roadmap. |
| **P4** iceberg-rust `ManifestEvaluator` gaps | Direct enabler for shelfd's `filter_service.rs` (G4 predicate-pushdown probe) — lets shelfd prune manifests in-Rust before fetching footers, instead of relying on the engine to push predicates pre-shim. |
| **P5** `IncrementalChangelogScan` delete-file support | Direct enabler for §8.1 — gives shelfd's `compaction_rewarm.rs` accurate `{added, rewritten, deleted}` classification, eliminating the over-eviction problem on snapshot transitions. |
| **P6** iceberg-rust Incremental Append Scan (#2337) | Direct enabler for §8.1 Rust-side flow — lets shelfd enumerate added files between snapshots without Trino-engine round-trips. |
| **P7** `MetricsConfig` bloom-filter presence | Direct enabler for §6's cache-friendly metadata enrichment — shelfd's `parquet_admit.rs` becomes admission-exact rather than admission-heuristic. |

### Composition with the sibling Trino contribution candidates

| Iceberg PR | Trino-side counterpart in `TRINO-CONTRIBUTION-CANDIDATES.md` | How they pair |
|---|---|---|
| **P1** Iceberg `ASYNC_PLANNING_POOL` configurable | **Trino P2** REST scan-planning client adoption | Trino-side adoption is the demand; Iceberg-side fix is the supply. Shipping both makes REST scan-planning production-grade end-to-end. |
| **P2** Iceberg `ParallelIterable` perf fix | (Indirect) Trino's `IcebergSplitSource` consumes `ParallelIterable` directly | The Trino side gets the win automatically once Iceberg merges; no separate Trino PR needed. |
| **P3** IRC Events Endpoint | (Future) Trino-side event subscriber would be a follow-on Trino PR after Iceberg P3 lands | Pairs cleanly: once the spec exists, Trino's `iceberg.metadata-cache` invalidation can subscribe instead of polling. Sibling-file P5 (metadata-cache deferral) becomes more attractive once the events SPI exists. |
| **P5** `IncrementalChangelogScan` delete support | (Indirect) Trino's CDC support via `system.iceberg_changes_for(...)` table function | Currently incomplete on MOR tables; the Iceberg fix transparently fixes Trino's table function too. |
| **P6** iceberg-rust Incremental Append Scan | No Trino counterpart (Trino is JVM-only) | Pure Shelf-side enabler; doesn't compose with Trino work. |

### Composition with the existing Shelf workspace memory

| Iceberg PR | Notes from existing workspace memory |
|---|---|
| **P1** | Aligns with the rewritten TODO §6 framing. No conflict with shelfd's existing `decoded_meta.rs` ETag-LRU design. |
| **P3** | Complements shelfd's `compaction_rewarm.rs` snapshot-event-driven re-warm pattern — both push-based, both keyed off snapshot transitions. |
| **P4 / P6** | Strengthens iceberg-rust's role as the impl-of-record for shelfd's plan endpoint — the workspace already calls out iceberg-rust as the natural backing library for `decoded_meta.rs` and `parquet_meta.rs`. |
| **P5** | Replaces the shelfd workaround of treating any snapshot transition as a "burn the world, re-warm" event with a precise file-level classification. |

---

## §6 — What NOT to propose

Candidates investigated and rejected, with reasons. Recorded so the next contributor doesn't repeat the analysis.

| Rejected candidate | Why |
|---|---|
| **Reference REST catalog server-side scan-planning impl** (the user's literal candidate #1 framing — "the reference impl currently does not implement the plan endpoint server-side") | **Already shipped.** PR `apache/iceberg#14480` (merged 2025-11 by `@singhpk234`) added the server-side handler in `RESTCatalogAdapter` + `CatalogHandlers.planTableScan()`. The user's framing is stale. The **active** gap is the executor configuration (P1) — that's the open follow-up. |
| **`planFiles` parallelism via `ManifestGroup` re-architecture** (the user's candidate #3 framing) | The Trino-cited #11708 was closed completed in Jan 2025; the open follow-up issue #14790 traces the modern bottleneck to `ParallelIterable` (P2 above), not `ManifestGroup`. Re-architecting `ManifestGroup` would be a multi-month effort with high reviewer churn and unclear win over the targeted `ParallelIterable` fix. |
| **Server-side scan-task batching / paging knob** (the user's candidate #5 framing) | The current spec already supports `POST .../tasks` paging per #13400 / #15572. The actual reference-impl backpressure question is **executor capacity**, not response paging — that's covered by P1. No separate paging PR needed. |
| **`loadTable` snapshot-id pin / vended-credentials hardening** (the user's candidate #6 framing) | Active in-flight upstream work: PR `apache/iceberg#14781` (merged) added `AccessDelegation` header to `planAPI` calls; PR `apache/iceberg#15280` (in development) adds spec support for credential refresh on staged tables. Active upstream contributors `@yadavay-amzn` and others are driving this. **A Shelf-author PR here would step into someone else's lane.** Skip. |
| **`write.cache-hint` table property** (the user's candidate #7 framing) | Spec-level change requiring a dev-list vote, single-purpose for caches like Shelf, low ecosystem-wide leverage. Maintainers will reasonably ask "why not let the cache observe row-group rewrite frequency instead of declaring it?". The corresponding observability already exists via `IncrementalChangelogScan` (P5). Skip in favour of P5. |
| **REST first-class Tag concept** (issue `apache/iceberg#16165`) | Open discussion, last activity 2026-04. Genuinely interesting but governance-heavy: Tags are a long-running spec discussion (since 2023) and any PR will land in the middle of an active design conversation. Not a first-time-contributor surface. |
| **`ContentStats`-based metrics evaluators** (issue `apache/iceberg#16218`) | Open discussion, no maintainer consensus on the design yet. Wait for the issue to converge before spending effort here. |
| **Snapshot log / snapshot-summary introspection endpoint** (the user's candidate #11 framing) | Iceberg's `Snapshot.summary()` and `Table.history()` already expose this information — there is no missing API. The "no event stream" gap is exactly P3 (IRC Events Endpoint), so the right framing is **P3**, not a new endpoint. |
| **`ManifestFile.PartitionFieldSummary` completeness sweep** (the user's candidate #9 framing) | Spot-checked: the summary fields (`lower_bound`, `upper_bound`, `contains_null`, `contains_nan`) are populated for all partition columns in `ManifestWriter`. The actual gap (`NOT IN` / `!=` pruning when `lower == upper`) was already fixed in PR `apache/iceberg#15064` (`@joyhaldar`, merged 2026-01-22) — the textbook Iceberg first PR (155 LOC, 4 commits, 6 days from open to merge). No remaining gap to chase. |
| **Anything that adds a Shelf-specific spec field** | Apache Iceberg's spec-change governance (dev-list vote, ASF code-modification model) makes single-consumer spec extensions costly. Frame every spec change as a generic capability with multi-engine demand or skip. |
| **Multi-thousand-LOC SPI redesign as a first PR** | Apache reviewers will not engage. The IRC reference impl is ~10k LOC; touching > 200 LOC of it in a first PR is a credibility-loss event. P1 is the < 200 LOC anchor. |

---

## §7 — Engagement playbook

Apache Iceberg is an Apache Software Foundation project. The contributor model is **distinct from Trino's** — different CLA, different governance, different cadence. Apache governance details below verified against `iceberg.apache.org/contribute/?h=proposal` on 2026-05-14.

### Pre-flight (one-time setup)

| Step | Action | Reference |
|---|---|---|
| 1 | Sign the **Apache ICLA** (Individual Contributor License Agreement) — this is a one-time signature covering **all** Apache projects, not just Iceberg. If you've previously contributed to any ASF project (Apache Spark, Apache Airflow, Apache Kafka, Apache Hadoop, etc.) you've already signed. Distinct from Trino's CLA. | `apache.org/licenses/contributor-agreements.html` |
| 2 | Join the Apache Iceberg Slack: `apache-iceberg.slack.com` (invite link from `iceberg.apache.org/community/`). Subscribe to `#dev`, `#iceberg-rust`, `#general`. | `iceberg.apache.org/community/` |
| 3 | Subscribe to the dev mailing list: send `subscribe` to `dev-subscribe@iceberg.apache.org`. **The dev list is the source of truth for design decisions.** Spec votes (per the contribution guide's "Behavioral and functional changes to a specification" rule) happen here. | `iceberg.apache.org/community/` |
| 4 | Watch `apache/iceberg`, `apache/iceberg-rust`, `apache/iceberg-python` on GitHub. The three repos have **separate maintainers, separate review queues, and separate release cadences**. | — |
| 5 | Familiarise with the monthly community sync calls — schedule and notes are linked from `iceberg.apache.org/community/`. The June 2026 sync would be the natural venue to surface a Phase 2 PR like P3 if review stalls. | — |
| 6 | DCO (`Signed-off-by:`) is **NOT required** for Apache Iceberg — the ICLA is the only legal sign-off. Distinct from Trino (CLA, no DCO) and distinct from many other OSS projects (DCO-only, no CLA). | Confirmed by reading the contribution guide and checking recent merged PRs (no `Signed-off-by:` lines). |

### For each PR, in order

1. **Discuss first** for anything > 50 LOC or anything touching a public API. Channels in order of preference: GitHub issue (preferred for code-only changes) → `dev@iceberg.apache.org` mailing list (required for spec changes per the contribution guide) → Slack `#dev` (for quick design questions). Cite the relevant prior PRs / issues — this is a strong signal that the contributor has done their homework.
2. **Fork + branch.** Branch off `main`, name the branch using a topic-area prefix matching the PR title (e.g. `core/configurable-async-planning-pool` to mirror PR title prefix `Core:`).
3. **Use the topic-area prefix in the PR title.** Per the contribution guide: `Build:`, `Docs:`, `Spark:`, `Flink:`, `Core:`, `API:`, `REST:`, `Hive:`. PRs without prefixes get auto-relabelled by the GitHub-actions bot but human reviewers still notice.
4. **One topic per PR.** Iceberg's review culture rejects omnibus PRs. Even the multi-phase P5 work should ship as a stack of 2–3 PRs.
5. **Tests + JMH benchmarks (if perf-related).** Iceberg reviewers expect every behavioural change to come with tests. Perf claims must come with a JMH benchmark — see `core/src/jmh/java/org/apache/iceberg/...` for the existing benchmark suite.
6. **AssertJ + JUnit 5.** The contribution guide explicitly mandates AssertJ-style assertions and JUnit 5 (`org.junit.jupiter.api`) for all new tests. PRs using `Assert.assertEquals` will be asked to convert.
7. **Run `./gradlew spotlessApply` before pushing.** Iceberg uses Google Java Format + Scalafmt; non-conforming PRs fail CI on the first commit.
8. **CI green before requesting review.** PRs with red CI typically sit untouched. Re-push fixups, then `@<reviewer>` only after CI is green.
9. **Conventional review cycle is fast on small PRs, slow on spec PRs.** Joy Haldar's PR #15064 merged 6 days from open to merge at 155 LOC; the IRC Events Endpoint #13580 has been open 10 months because no one has picked it up. Calibrate expectations based on PR size.
10. **For spec changes (`format/`, `open-api/rest-catalog*`):** prepare for a dev-list vote (3 +1 PMC votes, ASF code modification model, no lazy consensus). Add 2–4 weeks to the calendar for the vote on top of the code review.

### Distinguishing the three Iceberg repos

| Repo | Language | Primary maintainers (active 2026) | Review queue | Sub-rules |
|---|---|---|---|---|
| `apache/iceberg` | Java + spec | `@RussellSpitzer`, `@nastra`, `@danielcweeks`, `@manuzhang`, `@huaxingao`, `@flyrain`, `@singhpk234`, `@amogh-jahagirdar` (planning) | Active; small PRs merge in days, large PRs in weeks | Spec changes need dev-list vote |
| `apache/iceberg-rust` | Rust | `@liurenjie1024` (does the bulk of reviews), `@Xuanwo`, `@marvinlanhenke`, `@viirya`, `@mbutrovich` | **Review is the binding constraint** per `@Xuanwo`'s comment in #1797 | Same ICLA, separate Slack channel `#iceberg-rust` |
| `apache/iceberg-python` | Python | `@Fokko`, `@kevinjqliu`, `@HonahX`, `@syun64` | Active; aligned with Java spec releases | Same ICLA |

### Specific suggested first reach-outs

| For | Channel | Message shape |
|---|---|---|
| **P1** (ASYNC_PLANNING_POOL configurable) | (a) Comment on the closed PR `#14480` thread tagging `@chenwyi2` and `@singhpk234`; (b) brief email to `dev@iceberg.apache.org` proposing the property name | Cite `chenwyi2`'s December comment, propose `iceberg.rest.async-planning-threads` system property, default `availableProcessors()`, ask for objections within a week |
| **P3** (IRC Events Endpoint POJOs) | (a) Comment on issue `#13580` tagging `@c-thiel` to claim the work; (b) Slack `#dev` to gauge interest in a POJO-only first PR | Cite the 10-month idle window, propose splitting the work into a no-spec-change first PR |
| **P4** (iceberg-rust ManifestEvaluator gaps) | Comment on `@alamb`'s discussion issue `#1797` indicating which specific gap you'll pick (e.g. `bucket(N)` predicate) | Frame as direct response to `@Xuanwo`'s "review capacity is the constraint" comment |
| **P6** (collaborate on iceberg-rust #2337) | Direct review comments on the open PR | Offer fix-up commits proactively; iceberg-rust maintainers welcome this per #1797 culture |

### What NOT to do (anti-patterns from past stalled external PRs)

| Anti-pattern | Source | What to do instead |
|---|---|---|
| Open a 1000-LOC PR with no prior dev-list discussion | Multiple stale PRs in the iceberg-rust queue per #1797 thread | Discuss first, code second. < 200 LOC for first PR. |
| Propose a spec change without a dev-list email first | Standard contribution guide rule | Email the dev list with `[DISCUSS]` prefix before any spec PR |
| Bump the PR thread daily | — | Weekly review cycles are normal on small PRs; bi-weekly on spec PRs. Wait two weeks, then a single polite ping with `@<reviewer>`. |
| Use JUnit 4 / `Assert.assertX` in new tests | Caught on most external PRs | Use JUnit 5 + AssertJ (mandated by the contribution guide) |
| Skip `./gradlew spotlessApply` | Caught by CI | Wire it as a pre-commit hook |
| Cite a benchmark without JMH source in the PR | — | Add the JMH file to `core/src/jmh/java/...` in the same PR |
| Frame the PR as solving "Shelf needs this" | Will read as single-consumer special-casing | Frame every PR as a generic capability with multi-engine demand. The Shelf use-case can appear as an "additional context" item, not the headline. |

---

## Appendix — Verification log

Every URL hit during research on 2026-05-14, with what was confirmed. A reviewer auditing this document can re-run each URL.

| URL | Date confirmed | What was extracted |
|---|---|---|
| `https://github.com/apache/iceberg/pull/14480` | 2026-05-14 | MERGED Nov 2025 by `@nastra`. Author `@singhpk234`. Adds reference impl for REST scan planning in `CatalogHandlers`. Review thread contains `@chenwyi2`'s December 2025 comment flagging `Executors.newSingleThreadExecutor()` as a potential bottleneck. |
| `https://github.com/apache/iceberg/pull/13400` | 2026-05-14 | MERGED 2025-12-10 by `@nastra`. Author `@singhpk234`. Adds client-side `RestTable` / `RestTableScan` / streaming iterator. |
| `https://github.com/apache/iceberg/pull/15572` | 2026-05-14 | MERGED 2026-03-17 by `@nastra` (milestone 1.11.0). Author `@singhpk234`. Adds table-level override for scan planning. |
| `https://github.com/apache/iceberg/pull/15863` | 2026-05-14 | MERGED 2026-04 by `@nastra`. Author `@singhpk234`. Makes REST scan planning poll timeout configurable. |
| `https://github.com/apache/iceberg/issues/13706` | 2026-05-14 | CLOSED, addressed by #14480. Tracking issue for the reference scan-planning impl. |
| `https://github.com/apache/iceberg/issues/13707` | 2026-05-14 | OPEN. Epic tracking missing IRC reference-impl features. |
| `https://github.com/apache/iceberg/issues/13580` | 2026-05-14 | OPEN, opened 2025-07-17 by `@c-thiel`, last updated 2026-05-12, label `good first issue`. IRC Events Endpoint Request/Response Objects. |
| `https://github.com/apache/iceberg/issues/13581` | 2026-05-14 | OPEN, sister to #13580. Server-side catalog handlers for events. |
| `https://github.com/apache/iceberg/issues/13582` | 2026-05-14 | OPEN, sister to #13580. Test harness for events. |
| `https://github.com/apache/iceberg/issues/14790` | 2026-05-14 | OPEN, opened 2025-12-07 by `@agoncharuk`, last updated 2025-12-19 with `@RussellSpitzer`'s concurrency-window concern. `ParallelIterable` O(N²) `ConcurrentLinkedQueue.size()` perf bug. |
| `https://github.com/apache/iceberg/pull/11895` | 2026-05-14 | CLOSED without merge — the original `ParallelIterable` fix attempt. Russell's concurrency-window concern (multiple threads can each see `tracker < limit`, all add, overshoot) is the substantive blocker. |
| `https://github.com/apache/iceberg/issues/14264` | 2026-05-14 | OPEN, opened 2025-09 by `@hsiang-c`. `IncrementalChangelogScan` delete-file support gap. |
| `https://github.com/apache/iceberg/issues/15063` | 2026-05-14 | CLOSED by PR #15064 (Joy Haldar). Used as exemplar of a successful first-time Iceberg contributor PR. |
| `https://github.com/apache/iceberg/pull/15064` | 2026-05-14 | MERGED 2026-01-22 by `@RussellSpitzer`. Author `@joyhaldar`. 155 LOC across 2 files, 4 commits, 6 days open-to-merge. Reviewers: `@huaxingao`, `@manuzhang`, `@nandorKollar`. **Reference template for first-time Iceberg PR shape.** |
| `https://github.com/apache/iceberg/issues/15109` | 2026-05-14 | OPEN. Freshness-aware loading follow-up. PR #16319 recently opened by `@yadavay-amzn` to address — out of Shelf's lane. |
| `https://github.com/apache/iceberg/pull/12194` | 2026-05-14 | MERGED. Extended header support for `RESTClient`, prerequisite for freshness-aware loading. |
| `https://github.com/apache/iceberg/issues/16165` | 2026-05-14 | OPEN. REST first-class Tag concept discussion. Rejected as too governance-heavy for first PR. |
| `https://github.com/apache/iceberg/issues/16218` | 2026-05-14 | OPEN. `ContentStats`-based metrics evaluators. Rejected — no maintainer consensus on design yet. |
| `https://github.com/apache/iceberg/issues/11118` | 2026-05-14 | CLOSED. Standardise vended credentials in `LoadTable`/`LoadView` responses. |
| `https://github.com/apache/iceberg/pull/14781` | 2026-05-14 | MERGED. Adds `AccessDelegation` header to `planAPI` calls for vended credentials. |
| `https://github.com/apache/iceberg/pull/15280` | 2026-05-14 | OPEN. Spec support for credential refresh on staged tables. Active upstream lane. |
| `https://github.com/apache/iceberg/pulls?q=is:pr+is:open+ASYNC_PLANNING_POOL` | 2026-05-14 | **Zero matches.** Confirms no open PR addresses the executor configuration gap (i.e. P1 is uncontested). |
| `https://github.com/apache/iceberg/pulls?q=is:pr+is:open+CatalogHandlers+sort:updated-desc` | 2026-05-14 | 3 open PRs (#15831 relation load endpoints, #9830 unknown, #14997 view-version concurrency). None overlap with P1. |
| `https://github.com/apache/iceberg/issues?q=is:open+is:issue+label:%22good+first+issue%22+sort:updated-desc` | 2026-05-14 | 11 open `good first issue` items. Top-of-list: #13580 (events endpoint), #12937 (Flink JUnit4 removal), #15924 (DV Puffin streaming), #14227 (REST fixture log noise), #15916 (Spark branch docs), #15852 (ADLSFileIO scheduled refresh), #15347 (statistics column disable), #15556 (benchmarks doc update), #13581/#13582 (events endpoint sisters), #12516 (Kafka Connect docs). |
| `https://github.com/apache/iceberg-rust/issues/1797` | 2026-05-14 | OPEN, opened 2025-10-28 by `@alamb`, 17 👍 / 28 ❤️ reactions, last updated 2026-05-07. "Reduce the need for iceberg-rust forks" discussion. Catalogues contributor pain points; `@Xuanwo` cites review capacity as the binding constraint. |
| `https://github.com/apache/iceberg-rust/issues?q=is:open+is:issue+label:%22good+first+issue%22+sort:updated-desc` | 2026-05-14 | 5 open `good first issue` items. Top-of-list: #1818 (community growth tracking), #1382 (write-support epic), #2028 (`TableProperties` HashMap replacement), #1780 (datafusion sqllogictest), the Discussion #1797. |
| `https://github.com/apache/iceberg-rust/issues/289` | 2026-05-14 | CLOSED 2024-04-15 (transforms projection — completed by sister PRs). |
| `https://github.com/apache/iceberg-rust/issues/153` | 2026-05-14 | CLOSED. Original ManifestEvaluator tracking issue. |
| `https://github.com/apache/iceberg-rust/issues/348` `/issues/350` | 2026-05-14 | CLOSED. ManifestEvaluator initial implementation TODOs. |
| `https://github.com/apache/iceberg-rust/issues/2152` | 2026-05-14 | OPEN. Umbrella issue for incremental reads in Rust. |
| `https://github.com/apache/iceberg-rust/pull/2337` | 2026-05-14 | OPEN. Active "feat: Incremental append scan" PR addressing prior #2153 feedback. P6 collaboration target. |
| `https://github.com/apache/iceberg-rust/pull/2153` | 2026-05-14 | CLOSED stale. Prior incremental-scan attempt; review feedback now in #2337. |
| `https://github.com/apache/iceberg-rust/issues/1636` | 2026-05-14 | OPEN. `IncrementalChangelogScan` for CDC use-cases. Rust counterpart to Java P5. |
| `https://github.com/apache/iceberg-rust/issues/1818` | 2026-05-14 | OPEN. Tracking issues for how to grow iceberg-rust. |
| `https://github.com/apache/iceberg-rust/issues/1845` | 2026-05-14 | OPEN. ArrowReader name mapping schema visitor. Mid-priority Rust SDK work. |
| `https://github.com/apache/iceberg-python/issues/2634` | 2026-05-14 | OPEN. PyIceberg `Incremental Append Scan` support. Python counterpart blocked on Java spec landing. |
| `https://raw.githubusercontent.com/apache/iceberg/main/core/src/main/java/org/apache/iceberg/rest/CatalogHandlers.java` | 2026-05-14 | Live on main. Confirmed line 125: `private static final ExecutorService ASYNC_PLANNING_POOL = Executors.newSingleThreadExecutor();`. Confirmed line ~1055: `planTableScan` and `asyncPlanFiles` submit to this executor. |
| `https://iceberg.apache.org/contribute/?h=proposal` | 2026-05-14 | Apache Iceberg contribution guide. Confirmed: ICLA required (no DCO); JUnit 5 + AssertJ mandated; spec changes require dev-list vote (no lazy consensus); PR title topic-area prefixes (`Core:`, `API:`, etc.); `./gradlew spotlessApply` for code style. |
| `https://iceberg.apache.org/community/` | 2026-05-14 | Community page. Slack invite link, dev mailing list address (`dev@iceberg.apache.org`), monthly sync calendar. |

### Items that need re-verification before any concrete PR opens

1. **Whether iceberg-rust has a parallel-executor analogue to P1 in flight.** Sweep `apache/iceberg-rust/pulls` for any PR touching `crates/iceberg/src/catalog/rest/` server-side handler before opening.
2. **Whether the IRC Events Endpoint OpenAPI schema has been drafted in any branch since #13580 was opened.** `git log --all -- open-api/rest-catalog-open-api.yaml` — if any active PR already drafts the events schema, P3 should be fix-ups against that PR rather than a new PR.
3. **Whether `@RussellSpitzer`'s December 2025 concurrency-window concern on PR #11895 has any newer follow-up discussion.** The thread on #14790 is the canonical reference, but Slack `#dev` may have moved further. Check before drafting P2.
4. **Whether iceberg-rust PR #2337 is still open and welcoming reviewers.** PR status changes daily; re-check before P6 work.

---

## See also

- `docs/upstream/TRINO-CONTRIBUTION-CANDIDATES.md` — sibling document for Trino-side PRs. P2-Trino (REST scan-planning client adoption) pairs with P1-Iceberg (this document).
- `TODO-fix-shelf-performance.md` — the rewritten internal TODO. §6 (REST scan-planning) and §8.1 (snapshot-delta invalidation) are the two sections this document's Phase 2/3 work most directly enables.
- `docs/discovery/upstream/contacts.md` — quick reference for both Trino and Iceberg maintainers, mailing lists, and Slack channels.
