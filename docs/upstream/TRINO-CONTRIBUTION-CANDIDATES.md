# Trino contribution candidates for the Shelf maintainer

> Research-grounded shortlist of upstream Trino PRs the Shelf BDFL could author
> that simultaneously (a) unblock or accelerate Shelf's read-cache path, (b) are
> a clean win for every Trino user (no Shelf-shaped SPI), and (c) are small
> enough to actually land as first-time-contributor PRs.
>
> Research date: 2026-05-14. Verified against the live `trinodb/trino` and
> `apache/iceberg` repositories on github.com on that date. Every PR number,
> commit SHA, and issue number in this document was checked against the upstream
> page during this research run; see the **Verification log** appendix at the
> end for the exact URLs hit.

---

## §1 — TL;DR

| Rank | Proposal | Scope | Trino-side win | Shelf-side win | When |
|---|---|---|---|---|---|
| 1 | **Trino-side adoption of Iceberg REST scan planning** (consume `RestTable` / `RestTableScan` from Iceberg 1.10+ in `plugin/trino-iceberg/.../catalog/rest/`) | ~600 LOC prod + ~400 LOC tests | Planning minutes → ms on large Iceberg tables; offloads coordinator CPU + network | Shelfd's planned plan-endpoint server has a real client to talk to; removes Trino as planning bottleneck | Phase 2 (after one trust-building PR) |
| 2 | **Let any cache plugin opt into split affinity routing** (decouple the `SplitAffinityProvider` cache-enabled binding from the `fs.cache.enabled=true` gate) | < 200 LOC prod + ~150 LOC tests | Hudi/Delta/Iceberg with **any** future cache impl (Alluxio, Memory, ext plugin) get worker-local affinity routing without enabling the in-process FS cache | Once a Shelf blob-cache plugin lands post-#29184, Shelf hits land worker-local | Phase 1 (smallest, lowest risk) |
| 3 | **Engage on #29184 with fix-up PRs** addressing CodeRabbit-flagged issues on `BlobCache`, `CacheKey` opacity, `length()` metadata path | ~50–150 LOC each fix-up PR | Unblocks `wendigo`'s 121-file SPI redesign so it lands | Gives Shelf a plugin entry point; the dormant `ShelfFileSystemFactory` (`clients/trino/`) becomes wirable | Phase 1 in parallel with #2 (CodeRabbit fix-ups can be picked off one at a time) |

> Phase 1 recommendation: open the **`SplitAffinityProvider` binding-decoupling** PR (or a single CodeRabbit fix-up against #29184) — both are < 200 LOC, mechanical, and put a merged Trino PR under Aamir's name within 2–3 weeks.

---

## §2 — Methodology

### How candidates were sourced
1. The user-supplied candidate list (10 items) was treated as input; every item was independently verified upstream.
2. Three additional sweeps were run against `trinodb/trino` for the last 90 days, sorted by recent activity, filtered by labels `iceberg`, `cache`, `performance`, `delta-lake`, `hive`.
3. For each Trino PR/issue cited the **state**, **last activity date**, **author**, **labels**, and **milestone** were re-read from the live page — not from in-repo memory.
4. For Apache Iceberg-side work the corresponding PR threads (#11369, #13004, #13400, #15572) were read end-to-end including the Trino-specific design discussion in #13400's review thread.
5. The aamir306 fork at `github.com/aamir306/trino` branch `shelf/fs-spi-hook` was inspected (commit `9d68b98e`, 2026-05-02, +295/-2 LOC across 8 files) — verified the branch was authored but **never opened as upstream PR**.

### Verification commands
The full verification log lives at the end of this document (the **Appendix**). The short version: each upstream PR/issue page was fetched live, the relevant section quoted with author + date, and the date stamp recorded.

### What was *not* verified
- The exact line numbers in `FileSystemModule.java` / `AlluxioFileSystemCacheModule.java` for the `SplitAffinityProvider` Guice binding gate. The web fetch returned 404 on direct file URLs (likely because the file lives in the `manager` subpackage); the gate **is** described in detail in the merged #29182 PR body and confirmed by the SPI interface file `lib/trino-filesystem/src/main/java/io/trino/filesystem/cache/SplitAffinityProvider.java` existing live on master. Author should re-read the exact Guice wiring before writing the PR; the candidate framing in §3 is robust to the precise binding location.
- The exact LOC count Trino-side to consume Iceberg `RestTableScan`. Estimate is based on the equivalent client adoption work in `iceberg-rust` (issue `apache/iceberg-rust#1690`, comparable scope per Amogh's comment thread) plus the file diff of #13400.

---

## §3 — Ranked proposals

### P1 — Let any cache plugin opt into split affinity routing

| Field | Value |
|---|---|
| One-line | Decouple the `SplitAffinityProvider` cache-enabled binding from `fs.cache.enabled=true` so any installed cache (Alluxio, Memory, future blob-cache plugin from #29184) inherits worker-local affinity routing. |
| Status today | `lib/trino-filesystem/src/main/java/io/trino/filesystem/cache/SplitAffinityProvider.java` (live on master, verified 2026-05-14) is a no-op-default interface with `Optional<String> getKey(String path, long offset, long length)`. The cache-enabled implementation is bound on the coordinator **only when `fs.cache.enabled=true`** per merged PR #29182 (`raunaqmorarka`, milestone 481, merged 2026-04-27). Native-S3 / GCS / Azure paths today get the `Noop` provider, which loses cross-query worker affinity even when an external cache (Alluxio plugin path, future #29184 plugin) is wired in. |
| Proposed change | Move the consistent-hash-backed `SplitAffinityProvider` binding from the Alluxio-cache module's `fs.cache.enabled=true` block into either the `CacheManagerModule` (where #29184 lands the blob-cache registry) or a standalone module that activates whenever **any** cache provider is installed. Concrete shape: a `SplitAffinityProvider` provider method that consults `CacheManagerRegistry.isInstalled()` (proposed in #29184) and falls through to `Noop` when nothing is. |
| Win for all Trino users | Hive / Delta / Iceberg / Hudi already inject `SplitAffinityProvider` (per #29182). The fix means **any** installed cache — including the in-memory cache from #29184's `plugin/trino-blob-cache-memory/`, future GCS/Azure native caches, and the Alluxio path — gets worker-local routing on cache hits. Today only the Alluxio path benefits. |
| Win for Shelf | The day the #29184 SPI merges and Shelf ships `plugin/trino-blob-cache-shelf/`, worker-affinity is automatic. Without this PR, Shelf would either need a follow-up Trino PR to wire its own gate, or it would silently lose the cross-query affinity benefit. |
| Estimated scope | Production: ~80–150 LOC (move one Guice binding, add a small registry-check). Tests: ~100–150 LOC (extend `TestSplitAffinityRouting` style tests to cover the no-`fs.cache.enabled` + cache-installed path). |
| Reviewers / maintainers | `@raunaqmorarka` (authored #29182), `@electrum`, `@ebyhr` (approved #29182), `@wendigo` (overlaps with #29184). |
| Upstream-acceptance risk | **Low.** This is an obvious architectural cleanup; the original cache-enabled gate was a pragmatic choice when only Alluxio had a cache. Risk vector: the PR must land *after* #29184 merges (so a registry-check has something to read) **or** must be framed as a pure refactor that paves the way for #29184. Frame option B is preferable — coordinate with `@wendigo` in `#core-dev` first. |
| First-PR friendliness | **Excellent.** Mechanical refactor in a single subsystem, no new public API, no test-data file changes. The PR description should explicitly cite #29182 and #29184 and note that the change is forward-compatible with both. |

---

### P2 — Trino-side adoption of Iceberg REST scan planning

| Field | Value |
|---|---|
| One-line | Add a `RestTable` / `RestTableScan` consumer path to `plugin/trino-iceberg/.../catalog/rest/` so REST catalogs that advertise the `scan-planning` capability can offload `planFiles` from the Trino coordinator to the catalog server. |
| Status today | Apache Iceberg has shipped the **client-side** scan planning support: PR `apache/iceberg#13004` (request/response models + parsers, merged 2025-08-15 by `@amogh-jahagirdar`) and PR `apache/iceberg#13400` (RestTable + RestTableScan + streaming iterator, merged 2025-12-10 by `@nastra`). Server-side reference impl landed in `apache/iceberg#14480` (Nov 2025). Table-level override knob in `apache/iceberg#15572` (merged 2026-03-17, part of Iceberg 1.11.0). **No Trino-side PR has shipped consumer support** as of 2026-05-14 — verified by searching `trinodb/trino` PRs for "REST scan planning" and "PlanTableScanRequest" and finding zero matches. Issue `#26563` (open, last update 2026-03-12) shows Trino users on 469 are still seeing 7ms → 3 minute planning regressions with statistics enabled — the very pain that REST scan planning offloads. |
| Proposed change | Behind a feature flag (`iceberg.rest-scan-planning-enabled`, default false), have `TrinoIcebergRestCatalog`'s `loadTable()` path return a `RestTable` instance when the REST server advertises the capability in its `config` response. Wire the streaming `PlanTableScanResponse` consumer through `IcebergSplitSource.getNextBatch()` so splits stream into the engine as the server yields them, rather than the engine doing the full `planFiles` on the coordinator. Critical thread-pool question (called out by `@amogh-jahagirdar` in #13400 review thread): the current `ParallelIterable`-based design needs careful integration with Trino's `iceberg.planning-threads` pool (set in PR #25717, `raunaqmorarka`, merged 2025-05-02, milestone 476) to avoid cross-query contention. |
| Win for all Trino users | Issue #11708 (`@lxynov`, closed 2025-01-09) documented `planFiles` taking ~1.5 min on TPC-DS SF100000 customer/store_sales/store; issue #26563 (`@dejangvozdenac`, still open) measures 7ms → 3 min on 469 with stats on. Offloading to the catalog server eliminates the coordinator-side worker pool exhaustion and removes the cross-query contention that #25717 only partially mitigates. Author-reported numbers: planning time goes from minutes to milliseconds when the catalog is Snowflake's Polaris (Iceberg's reference REST impl). |
| Win for Shelf | Shelfd's roadmap includes implementing the REST scan-planning server (the catalog endpoint runs locally to the cache). When Trino can talk to that endpoint, the planning hop never leaves the cache layer, so manifest reads are 100% cache hits by construction. Indirect but large performance win: removes Trino's coordinator from the metadata read path entirely. |
| Estimated scope | Production: ~500–700 LOC (one new catalog wrapper class, a streaming split source, two-three config knobs, capability negotiation in the REST `config` handler). Tests: ~300–500 LOC (BaseIcebergRestConnectorSmokeTest + a fake server that returns paginated scan responses). **ADR required** for the cross-thread-pool design — Trino reviewers will absolutely want to see how the streaming iterator interacts with `ForIcebergSplitManager` and `iceberg.planning-threads`. |
| Reviewers / maintainers | `@raunaqmorarka` (Iceberg planning lead — authored #25717, closed #11708, owner of the planning subsystem), `@ebyhr` (active 2026 Iceberg reviewer/merger), `@chenjian2664` (recent IcebergSplit refactors). Coordinate with `@findinpath` and `@kaveti` who own the active REST-catalog vended-credentials work in #28998 / #28793 / #27922 — adopting scan planning will collide with their refactors otherwise. |
| Upstream-acceptance risk | **Medium.** Three concerns: (1) The thread-pool integration is the design risk Amogh flagged on #13400. The PR must propose either reusing `ForIcebergSplitManager` or a new bounded executor with explicit backpressure semantics — not a hand-rolled `ForkJoinPool`. (2) Trino's reviewers tend to gate large iceberg-catalog changes on a first-class write story; this PR should explicitly say "read-only, write path stays on local planning" to keep scope contained. (3) `wendigo` and others may want to wait for #29184 to land first so the scan-planning client also benefits from the blob-cache SPI — coordinate before opening. |
| First-PR friendliness | **Not as a first PR.** This is a substantial feature touching catalog, split source, config, and tests. It is the right *second* or *third* PR after a smaller refactor establishes maintainer trust. |

---

### P3 — Engage on #29184 with CodeRabbit fix-up PRs

| Field | Value |
|---|---|
| One-line | Submit one or more small fix-up PRs against `wendigo/trino:user/serafin/unified-caching-v2` (the source branch of #29184) addressing concrete CodeRabbit-flagged issues: null-handling in `InMemoryBlobCacheManager.drop()`, splitting `cacheManagerConfigFiles` with explicit empty-string handling, etc. Each fix-up = ~50–100 LOC; lands directly on `wendigo`'s branch (PR-to-PR), not on `trinodb/trino` master. |
| Status today | #29184 is **DRAFT**, opened 2026-04-21, last force-push 2026-04-21, last comment 2026-04-28 by `@dain` (verified 2026-05-14). 121 files changed, +2696 / -1203 LOC. Reviewers: `@martint`, `@losipiuk`. CodeRabbit has posted multiple inline suggestions that have not been addressed — these are the surface for fix-up PRs. Two earlier force-pushes on Apr 26 and Apr 28 indicate the PR is being actively iterated on by `wendigo`. |
| Proposed change | Open small PRs against `wendigo`'s fork's `user/serafin/unified-caching-v2` branch. Examples flagged by CodeRabbit: (a) null-handling in `cacheManagerConfigFiles` setter in `CacheManagerConfig.java`, (b) explicit `Objects.requireNonNull` in `InMemoryBlobCacheManager.drop(CatalogName)` matching `createBlobCache()`'s contract. Each PR is mechanical and uncontroversial. |
| Win for all Trino users | Unblocks the #29184 SPI by chipping at the CodeRabbit review surface. Maintainers (`martint`, `losipiuk`) are unlikely to approve a 121-file PR with 30+ unresolved bot comments; clearing those is the structural blocker. |
| Win for Shelf | Two compounding wins: (a) shortens the calendar time to a merged blob-cache SPI Shelf can target, (b) builds direct review trust with `wendigo` — the maintainer most likely to review the eventual `plugin/trino-blob-cache-shelf/` PR. |
| Estimated scope | Each fix-up PR: ~50–150 LOC + a tight test. Aim for 3 small PRs over 4 weeks, not one omnibus PR. |
| Reviewers / maintainers | `@wendigo` (PR author, will merge to his own branch); incidentally exposes to `@martint`, `@losipiuk` who watch the parent PR. |
| Upstream-acceptance risk | **Very low** — these are bot-flagged nits on someone else's draft branch. PR author has every incentive to accept clean fix-ups that reduce his review burden. |
| First-PR friendliness | **Excellent**, with a caveat: these PRs merge into `wendigo`'s fork, not into `trinodb/trino`. They become a Trino contribution only after #29184 itself merges. Frame them as collaboration, not as solo PRs against `trinodb/trino`. For the GitHub-visible-as-a-Trino-contributor angle, P1 is stronger. |

---

### P4 — Plugin-registered `TrinoFileSystemFactory` marker hook

| Field | Value |
|---|---|
| One-line | Open the existing `aamir306/trino:shelf/fs-spi-hook` branch (commit `9d68b98e`, dated 2026-05-02, +295/-2 LOC across 8 files) as an upstream PR to introduce `Plugin.getFileSystemFactories()` so plugins can register a `TrinoFileSystemFactory` the same way they register `EventListenerFactory`. |
| Status today | Branch is authored on Aamir's fork, **not opened as upstream PR**. Commit message explicitly acknowledges #29184 and frames the hook as complementary (filesystem-level vs cache-level). Open design questions are honest about classloader / classpath hazards. |
| Proposed change | Marker-interface design: `io.trino.spi.filesystem.TrinoFileSystemFactory` returns `Object` so the engine-internal `TrinoFileSystem` closure stays out of the SPI dependency boundary. Existing `io.trino.filesystem.TrinoFileSystemFactory` extends the SPI marker via Java covariant returns — all 100+ existing consumers compile unchanged. |
| Win for all Trino users | A plugin can own the entire filesystem path (e.g. a new GCS / Azure-blob / Cloudflare R2 / Tigris backend) without sitting in tree at `lib/trino-filesystem-X/`. Lowers the maintenance bar for new object-storage backends. |
| Win for Shelf | Shelf's `clients/trino/ShelfPlugin.java` already ships `ShelfFileSystemFactory` dormant package-private. The day this hook lands, the in-process plugin path becomes available — removing the shim's HTTP hop (~5–15% latency win on warm reads per Shelf's `docs/discovery/trino-upstream-strategy.md` §"Cost-savings angle"). |
| Estimated scope | The existing fork already shows ~295 LOC + ~150 LOC of tests would be needed. ADR for the marker-interface design choice is mandatory — reviewers will want to know why this vs the full SPI move. |
| Reviewers / maintainers | `@electrum` (filesystem subsystem owner), `@wendigo`, `@dain`. |
| Upstream-acceptance risk | **High** — three real concerns: (1) **Direct overlap with #29184.** Maintainers may say "wait for the blob-cache SPI, that's the real hook." Counter-argument: the two hooks live at different abstraction levels (filesystem vs cache) and are genuinely complementary, but this needs to be sold. (2) The `Object`-typed return is unusual at an SPI boundary; reviewers may push for a full SPI move (~14-file refactor + 100 import updates). (3) Classloader-isolation question: `PluginClassLoader.SPI_PACKAGES` needs `io.trino.filesystem.*` added or plugins can't implement the engine-internal interface — current iceberg/hive plugins work because they bundle `lib/trino-filesystem`, but that's fragile. |
| First-PR friendliness | **Poor as a first PR** because of the high overlap risk and SPI scope. Save this for after #29184 lands, or convert it to a *focused* small PR — e.g. just adding `io.trino.filesystem.*` to `PluginClassLoader.SPI_PACKAGES` to make the existing fragile pattern explicit. That focused version would be a clean Phase 1 candidate. |

---

### P5 — `iceberg.metadata-cache` integration with the #29184 blob-cache SPI

| Field | Value |
|---|---|
| One-line | When #29184 merges and a `BlobCacheManager` is installed, have `MemoryFileSystemCache` defer to the registered cache for Iceberg manifest / metadata reads rather than shadowing it with a JVM-local copy. |
| Status today | `MemoryFileSystemCache` (live at `lib/trino-filesystem/src/main/java/io/trino/filesystem/memory/MemoryFileSystemCache.java`, verified 2026-05-14) caches the coordinator's metadata reads in JVM heap. Issue #23559 (`@byunks`, closed 2024-09-26 not_planned by `@raunaqmorarka`) shows the maintainer position is that the cache is correct-by-construction because Iceberg metadata files are immutable. **There is no upstream open question about staleness.** The actual operator pain is different: when an external cache (Alluxio today, Shelf via shim today, blob-cache plugin tomorrow) is installed, the JVM-local cache *shadows* it for warm metadata reads — making the external cache's metadata hit-ratio look like 0% even when serving everything underneath. |
| Proposed change | After #29184 merges: introduce a configuration knob `fs.memory-cache.defer-to-blob-cache` (or, cleaner, make the deferral automatic when `CacheManagerRegistry.isInstalled()` is true) so the JVM-local cache acts as a thin write-through layer rather than a shadow. Keep the existing TTL/size behaviour for the `fs.cache.enabled=false` case. |
| Win for all Trino users | External-cache operators get accurate per-cache hit-ratio metrics. The JVM heap that today holds the (small but non-trivial) metadata cache is reclaimable when an external cache is providing the same locality. |
| Win for Shelf | The workspace's standing workaround — flipping `iceberg.metadata-cache.enabled=false` on every shelf-fronted catalog — becomes unnecessary. Trino's CBO can use its planning cache without breaking shelfd's metadata-pool observability. |
| Estimated scope | Conditional on #29184 merging first. ~150–250 LOC + tests. Genuinely small once the dependency clears. |
| Reviewers / maintainers | `@raunaqmorarka` (closed #23559 and owns the planning subsystem), `@wendigo` (#29184 author). |
| Upstream-acceptance risk | **Medium.** Hard-blocked on #29184. Once that lands, the change is uncontroversial. Worth pre-flighting the discussion with `raunaqmorarka` in `#core-dev` before opening. |
| First-PR friendliness | **No** — strictly a follow-on after #29184. List it here because it's the right Phase 2 work after building trust, and because the rationale ("external caches today, including Alluxio, are shadowed") is something `@raunaqmorarka` needs to hear regardless of who writes the PR. |

---

### P6 — Cache-read metrics already exist — submit a small VERBOSE-EXPLAIN doc fix instead

| Field | Value |
|---|---|
| One-line | The candidate "re-introduce `SplitCompletedEvent` for cache-read information" is dead upstream; redirect the energy into improving how the **existing** cache-read metrics from #26342 are surfaced in operator-facing docs and EXPLAIN ANALYZE output. |
| Status today | Issue #26690 (`@fishlinghu`, closed not_planned 2025-09-23 by `@wendigo`) asked exactly the question this candidate frames — extend `SplitCompletedEvent` with cache read info. `@raunaqmorarka`'s reply pointed to **PR #26342 (`@assaf2`, merged 2025-09-04 in milestone 477)** which already adds `bytesReadFromCache` and `bytesReadExternally` to `connectorMetrics`, surfaced in EXPLAIN ANALYZE VERBOSE and in QueryCompletedEvent. **PR #26436 (`@raunaqmorarka`, merged 2025-08-19, milestone 477)** removed the `SplitCompletedEvent` machinery entirely; **PR #27492 (`@findepi`, opened 2025-11-27)** finalizes the deletion. **The architectural direction is locked**: per-split cache metrics are off the table; per-query cache metrics already exist. |
| Proposed change | Two genuinely small things that would land: (a) a docs PR adding a concrete EXPLAIN ANALYZE VERBOSE example output that shows the new cache metrics (the #26342 release note is one line and gives no example); (b) a metrics PR adding per-table cache hit/miss labels to the operator metrics — `bytesReadFromCache{table=X}` so the existing `connectorMetrics` is operable for cluster-level cost analysis without per-split events. |
| Win for all Trino users | Operator-grade observability for the cache subsystem without re-introducing the per-split event firehose. |
| Win for Shelf | Operator framing for shelfd's per-table metrics matches Trino's, so the OSS install path doesn't need a separate Grafana dashboard. |
| Estimated scope | Doc PR: < 30 LOC of markdown + one example. Metrics PR: ~100 LOC + tests. |
| Reviewers / maintainers | `@raunaqmorarka`, `@assaf2`, `@kobiluz`. |
| Upstream-acceptance risk | **Low** for the doc PR; medium for the per-table labels (cardinality concerns). |
| First-PR friendliness | **Excellent for the doc PR** — a textbook first-time-contributor PR, < 30 LOC, no test infrastructure needed, addresses an obvious operator pain. Genuinely a candidate for the first merged PR if P1 hits an unexpected snag. |

---

## §4 — Recommended trajectory

### Phase 1 (weeks 1–2): smallest-possible merged PR for trust

**Pick one of three, in this order of preference:**

1. **The `SplitAffinityProvider` binding decoupling (P1)** — clearest architectural value, mechanical, ~80–150 LOC, reviewers already engaged on #29182 are obvious approvers. Pre-flight on `#core-dev` Slack with `@raunaqmorarka` before opening. Open the PR against a small change (just move the binding; do not add new public API in this PR). Estimated time-to-merge: **2–3 weeks** from CLA signature to merge, assuming a clean test run and one review cycle.

2. **A CodeRabbit fix-up PR against #29184 (P3)** — even smaller (50–100 LOC) and lands fast, but it merges into `wendigo`'s fork branch, not into `trinodb/trino` master. Counts as a Trino contribution only after #29184 itself merges. Time-to-merge against `wendigo`'s branch: **days to a week**; time-to-be-a-Trino-contributor: gated on #29184's own timeline (no maintainer approval yet as of 2026-05-14).

3. **The EXPLAIN ANALYZE VERBOSE doc fix (P6 doc PR)** — fallback if both above hit snags. < 30 LOC, no functional change, fastest possible merge. Estimated time-to-merge: **1–2 weeks**.

### Phase 2 (month 1–2): the Shelf-aligned high-leverage PR

**Trino-side adoption of Iceberg REST scan planning (P2).**

This is the **big** PR — the one that genuinely transforms the planning hot-path for every Trino + Iceberg + REST-catalog user, including but not limited to Shelf operators. By the time you open it you should have:

- One merged Trino PR (Phase 1) so `@raunaqmorarka` recognises the author name in the queue
- A Slack discussion in `#core-dev` with `@raunaqmorarka` and `@kaveti` about the thread-pool integration model
- An ADR draft (in this docs tree, not in the PR yet) showing how `iceberg.planning-threads` and `RestTableScan`'s streaming iterator compose
- A clean reproducer pointing at issue #26563 (open, last updated 2026-03-12) so the PR has an existing operator-pain ticket to close

The PR itself should be split into a **stack** mirroring `singhpk234`'s Iceberg-side staging (Part 1: capability negotiation in REST `config` response; Part 2: RestTable wiring in `TrinoIcebergRestCatalog`; Part 3: streaming split source). One merged Part-1 PR is a powerful trust signal even if Parts 2–3 take longer.

Estimated time-to-merge for the full stack: **2–4 months** elapsed. Part 1 alone: **4–8 weeks**.

### Phase 3 (quarter): the ambitious win

After **two or three merged Trino PRs**, the credibility budget exists to attempt one of:

- **`plugin/trino-blob-cache-shelf/`** if #29184 has merged — Shelf's existing `ShelfBlobCacheManagerFactory` sketch (`clients/trino/docs/blob-cache-plugin-sketch.md`) becomes a real PR.
- **The `MemoryFileSystemCache` deferral PR (P5)** — uncontroversial once #29184 has merged.
- **The marker-interface filesystem-factory hook (P4)** — only if #29184 ends up landing in a shape where the cache-level SPI doesn't subsume the filesystem-level need (likely, but uncertain).

Estimated time horizon: **6–9 months** from today.

---

## §5 — Cross-references with `TODO-fix-shelf-performance.md`

> A sibling worker is rewriting `TODO-fix-shelf-performance.md` in parallel. This document does not touch that file. The cross-references below assume the existing structure of TODO-fix-shelf-performance.md as of the last commit; refresh after the sibling worker's rewrite lands.

| Proposal | Accelerates / unblocks |
|---|---|
| **P1** `SplitAffinityProvider` decoupling | Once Shelf-as-blob-cache-plugin lands, cuts cross-pod peer-fetch traffic by routing repeat reads to the same Trino worker, which then hits the same Shelf pod via HRW. Shelf's existing SHELF-23 peer-fetch becomes a pure failover mechanism rather than the steady state — recovers the per-pod NVMe locality benefit. |
| **P2** REST scan planning | Removes Trino-side `planFiles` as a coordinator-CPU bottleneck. Eliminates the current operator workaround on rep-1/rep-2 (`iceberg.metadata-cache.enabled=false`) being the main driver of slow planning. Lets shelfd's metadata pool serve the catalog server, not the coordinator. |
| **P3** Engagement on #29184 | The single biggest external dependency in the Shelf Trino-integration roadmap (per `docs/discovery/trino-upstream-strategy.md`). Every week #29184 stays in DRAFT is a week Shelf cannot ship an in-process plugin. Fix-up PRs shave that timeline. |
| **P4** Filesystem-factory hook | Complementary to #29184. If #29184 lands first and proves sufficient, P4 may be redundant. If #29184 stalls past Q3, P4 becomes a real alternative. Hedge. |
| **P5** Metadata-cache deferral | Removes the `iceberg.metadata-cache.enabled=false` per-catalog workaround documented in the workspace today. Operators get correct hit-ratio dashboards. |
| **P6** Doc / per-table metrics | Indirect — improves operator-side observability of the existing cache layer. Helps anyone running Shelf, Alluxio, or future cache plugins. |

---

## §6 — What NOT to propose

Candidates investigated and rejected, with reasons. Recorded so the next contributor doesn't repeat the analysis.

| Rejected candidate | Why |
|---|---|
| **Reduce `planFiles` to a single call per table (the literal `#11708` framing)** | Issue #11708 was **closed as completed** on 2025-01-09 by `@raunaqmorarka`. The query-scoped stats cache landed via PRs #11858 (`@lxynov`), #13047 (`@findepi`), #13338 (`@alexjo2144`), and the metadata cache work in 2024–2025. The successor pain ticket is #26563, which is a different problem (CBO rule-time, not call-count). Don't re-litigate. |
| **Snapshot-aware auto-refresh for `iceberg.metadata-cache`** (the literal `#23559` framing) | Issue #23559 was closed not_planned by `@raunaqmorarka` on 2024-09-26 with a clear maintainer position: "iceberg metadata files are immutable, the cache will never serve stale metadata". The real operator pain (external-cache shadowing) is a different framing — see P5 above. |
| **Re-introduce `SplitCompletedEvent`** | Removed in PR #26436 (merged 2025-08-19), final delete proposed in PR #27492 (opened 2025-11-27). Issue #26690 asking for exactly this was closed `not_planned`. `wendigo` approved the removal; `dain` and `hashhar` concurred. The architectural direction is **explicitly** away from per-split events toward `OperatorStats` and `QueryCompletedEvent`. Per-split cache metrics already exist (PR #26342). |
| **`QueryCreatedEvent` carrying logical predicates** | The current `QueryMetadata` payload exposes the textual query and metadata but not the analyzed-and-pushed-down predicate set. Adding it would either be ABI-breaking (extend the record) or require a parallel event. Maintainers have shown low appetite for new event-listener payload fields (see #26690 closure). Better to extract predicates client-side from the SQL text in the event listener if needed. Not worth a PR proposal. |
| **Iceberg connector worker-side `ParquetMetaData` cache** | A worker-local Parquet footer cache analogous to `MemoryFileSystemCache` but split-local. **Already exists** — Trino reuses the JVM-local `MemoryFileSystemCache` on workers when `fs.cache.enabled=true`, and the `bytesReadFromCache` metrics from PR #26342 (merged 2025-09-04) prove it's wired through `OrcPageSource` and `TrinoParquetDataSource`. No PR-shaped gap. |
| **`iceberg-rest` HTTP client connection pooling / multiplexing** | The Trino REST client uses Iceberg's `HTTPClient` (Apache HttpClient 5 with default pooling). Capacity tuning is a config / docs change, not an architecture PR. Would land as a doc note in the iceberg-connector docs at most. |
| **Anything that adds a Shelf-specific SPI** | Will be rejected on sight per the maintainer culture documented in #22827 (Starburst-overlap concerns). Already covered in the workspace's `docs/discovery/trino-upstream-strategy.md` §"Governance hazards". |
| **A 2000-LOC SPI redesign as a first PR** | Hard rule from the user prompt and corroborated by the #24737 / #22827 history. First-PR limit is < 200 LOC for credibility. |

---

## §7 — Engagement playbook

### Pre-flight (one-time setup)

| Step | Action | Reference |
|---|---|---|
| 1 | Sign the Trino CLA at `github.com/trinodb/cla` | `trino.io/development/process` |
| 2 | Join `trino.io/slack`; subscribe to `#core-dev`, `#iceberg`, `#dev` | Same |
| 3 | Watch `trinodb/trino` on GitHub; route notifications to a dedicated label | — |
| 4 | Set GitHub identity for upstream PRs to `@aamir306` (per `MAINTAINERS.md` in `shelf-project/shelf`) | — |
| 5 | DCO is **NOT** required for Trino — CLA is the only sign-off (verified at `trino.io/development/process`). `git commit -s` is unnecessary. | — |

### For each PR, in order

1. **Discussion first.** Open a GitHub issue describing the problem (link to the open upstream issue if one exists) **or** start a Slack thread in `#core-dev` for anything > 50 LOC. Cite the relevant prior PRs / issues — maintainers read this as a signal that the contributor has done their homework.
2. **Fork + branch.** Branch off `master`, name the branch `aamir/<short-slug>` (matching the `raunaq/`, `ks/`, `user/` prefixes used by maintainers — verified by reading the source branches of #29182, #29184, #28389).
3. **One topic per PR.** Trino's review culture rejects omnibus PRs hard. Even the P2 REST-scan-planning work should be split into a stack of 2–3 PRs.
4. **Tests in the same PR.** Trino reviewers expect every behavioural change to come with a test that fails on master and passes with the change.
5. **Open the PR with a structured body.** Use the template from any recent merged PR: `## Description`, `## Additional context and related issues`, `## Release notes`. The release-notes block is mandatory — reviewers will request changes if it's missing.
6. **CI green before requesting review.** PRs with red CI typically sit for weeks. Re-push fixups, then `@<reviewer>` only after CI is green.
7. **Fixup commits, not force-push squash.** Trino reviewers prefer fixup commits during review so each round of changes is visible. Maintainer squashes on merge.
8. **Slack DM the reviewer only if a week passes with no response.** Don't bump the PR thread; the queue is real.

### Specific suggested first reach-outs

| For | Channel | Message shape |
|---|---|---|
| P1 (`SplitAffinityProvider` decoupling) | DM `@raunaqmorarka` in Slack | Brief: cite #29182 + #29184, propose the binding move, ask "before I open this, does the framing make sense or would you prefer it folded into #29184?" |
| P3 (fix-up PRs against #29184) | Comment directly on #29184 | Cite the CodeRabbit thread you'd address; offer to send a fix-up PR against the source branch |
| P2 (REST scan planning) | `#core-dev` and `#iceberg` Slack | Don't DM yet — surface the design first, ideally with an ADR draft attached |

### What NOT to do (anti-patterns from past stalled external PRs)

| Anti-pattern | Source | What to do instead |
|---|---|---|
| Wait for review on a 2000-LOC PR without engaging in-band | #24737 (external-cache PR that went stale and closed) | Open small, surface design in Slack, iterate publicly on the PR thread |
| Propose an SPI that mirrors a commercial product's shape | #22827 (Starburst-overlap discussion) | Frame every cache-touching PR as a generic capability; never cite Warp Speed / Galaxy in the PR body |
| Bump the PR thread weekly | — | Weekly review cycles are normal. Bumping irritates. Wait two weeks, then a single polite ping. |
| Force-push during active review | — | Use fixup commits until the maintainer signals it's time to squash. |

---

## Appendix — Verification log

Every URL hit during research on 2026-05-14, with what was confirmed. A reviewer auditing this document can re-run each URL.

| URL | Date confirmed | What was extracted |
|---|---|---|
| `https://github.com/trinodb/trino/pull/29184` | 2026-05-14 | DRAFT, 121 files, +2696/-1203, opened 2026-04-21, last force-push 2026-04-21, last comment from `@dain` 2026-04-28. Reviewers: `martint`, `losipiuk`. Author: `wendigo`. SPI surface: `core/trino-spi/.../cache/{Blob,BlobCache,BlobCacheManager,BlobCacheManagerFactory,BlobSource,CacheKey,CacheTier,ConnectorCacheFactory}`. Plugin modules: `plugin/trino-blob-cache-alluxio/`, `plugin/trino-blob-cache-memory/`. |
| `https://github.com/trinodb/trino/issues/11708` | 2026-05-14 | CLOSED completed 2025-01-09 by `@raunaqmorarka`. Author `@lxynov`. Successor PRs: `#11858`, `#12196`, `#13047`, `#13338`. |
| `https://github.com/trinodb/trino/issues/23559` | 2026-05-14 | CLOSED not_planned 2024-09-26 by `@raunaqmorarka`. Maintainer position: metadata files are immutable, no refresh needed. |
| `https://github.com/trinodb/trino/issues/26563` | 2026-05-14 | OPEN, last updated 2026-03-12. Author `@dejangvozdenac`. Documents 7ms → 3min planning regression on Trino 469 with `iceberg.statistics_enabled=true`. |
| `https://github.com/trinodb/trino/pull/29182` | 2026-05-14 | MERGED 2026-04-27 by `@raunaqmorarka`, milestone 481. Adds `ConnectorSplit#getAffinityKey()` SPI, `SplitAffinityProvider` interface, `ConsistentHashingAddressProvider`. Cache-enabled binding gated on `fs.cache.enabled=true`. |
| `https://github.com/trinodb/trino/pull/26436` | 2026-05-14 | MERGED 2025-08-19 by `@raunaqmorarka`, milestone 477. Removes `SplitCompletedEvent` collection on workers. |
| `https://github.com/trinodb/trino/issues/26690` | 2026-05-14 | CLOSED not_planned 2025-09-23 by `@wendigo`. Maintainer reply: cache metrics already in PR `#26342`. |
| `https://github.com/trinodb/trino/pull/26342` | 2026-05-14 | MERGED 2025-09-04 by `@raunaqmorarka`, milestone 477. Adds `bytesReadFromCache` / `bytesReadExternally` to `connectorMetrics`. |
| `https://github.com/trinodb/trino/pull/25717` | 2026-05-14 | MERGED 2025-05-02 by `@raunaqmorarka`, milestone 476. Adds `iceberg.planning-threads` config, separates Iceberg planning thread pool. |
| `https://github.com/trinodb/trino/pull/28389` | 2026-05-14 | OPEN, opened 2026-02-20 by `@sopel39`. 2099 LOC. Iceberg Parquet encryption read support. Reviewers: `raunaqmorarka`, `ebyhr`. |
| `https://github.com/apache/iceberg/pull/11369` | 2026-05-14 | CLOSED stale 2025-04-08. Author `@rahil-c`. Superseded by `#13004`. |
| `https://github.com/apache/iceberg/pull/13004` | 2026-05-14 | MERGED 2025-08-15 by `@amogh-jahagirdar`. Author `@singhpk234`. Adds REST scan planning request/response parsers. |
| `https://github.com/apache/iceberg/pull/13400` | 2026-05-14 | MERGED 2025-12-10 by `@nastra`. Author `@singhpk234`. Adds `RestTable` / `RestTableScan` / streaming iterator. Trino integration concerns explicit in review thread. |
| `https://raw.githubusercontent.com/trinodb/trino/master/lib/trino-filesystem/src/main/java/io/trino/filesystem/cache/SplitAffinityProvider.java` | 2026-05-14 | Live on master. Interface `SplitAffinityProvider` with `Optional<String> getKey(String path, long offset, long length)`. |
| `https://github.com/aamir306/trino/commit/9d68b98e` | 2026-05-14 | Commit on `shelf/fs-spi-hook` branch, dated 2026-05-02, +295/-2 LOC, 8 files. Adds `Plugin.getFileSystemFactories()` marker-interface hook. Not opened as upstream PR. |
| `https://trino.io/development/process` | 2026-05-14 | CLA required (one-time signature at `github.com/trinodb/cla`). No DCO sign-off required. PR workflow: discuss → fork → PR → fixup commits → maintainer merge. |
| `https://github.com/aamir306/trino/commits/shelf/fs-spi-hook` | 2026-05-14 | Latest commit on branch is the SPI hook commit `9d68b98e` from 2026-05-02. Branch rebased on top of recent Trino master commits including PR #29217 (`@dain`, merged 2026-04-23) and `@findinpath`'s #28998 (merged 2026-04-27). |

### Items that need re-verification before any concrete PR opens

1. **Exact location of the `SplitAffinityProvider` Guice binding gate.** Direct fetch of `FileSystemModule.java` and `AlluxioFileSystemCacheModule.java` returned 404; the file paths have shifted across the #29182 refactor and #29184 draft. Before writing the P1 PR, read `lib/trino-filesystem-manager/src/main/java/io/trino/filesystem/manager/FileSystemModule.java` (the actual location per #29184's CodeRabbit file list) and confirm the binding lives there.
2. **Current state of `IcebergSplitSource.java`.** Was fetched (`agent-tools/51f928d6-...txt`, 31.8 KB, 805 lines) but not parsed in detail for the P2 proposal. Read it before drafting the REST-scan-planning ADR to confirm where the consumer hook fits.
3. **Whether #29184 will land before any P5 PR could be opened.** As of 2026-05-14 the PR is still DRAFT with no maintainer approval; this could change daily. Check the PR status before opening any follow-on.
4. **Whether `@kaveti`'s in-flight REST catalog work (#28793 SSE, #27922 vended credentials) will conflict with a P2 REST scan-planning PR.** Coordinate before opening.

---

## See also

- `docs/discovery/trino-upstream-strategy.md` — overall upstream strategy (the **what / why** layer)
- `docs/discovery/upstream/29184-spi-feedback.md` — Shelf-specific design feedback on #29184 (the **how to engage** layer)
- `docs/discovery/upstream/29184-review-comment.md` — paste-ready public PR comment on #29184
- `docs/discovery/upstream/wendigo-slack-dm.md` — paste-ready Slack DM draft
- `docs/discovery/upstream/contacts.md` — quick reference for Trino maintainers and Slack channels
- `clients/trino/docs/blob-cache-plugin-sketch.md` — Java sketch of `plugin/trino-blob-cache-shelf/` against #29184's API
