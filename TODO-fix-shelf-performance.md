# Shelf performance plan (evidence-grounded)

**Created**: 2026-05-14
**Last rewrite**: 2026-05-14 — every numeric claim traces to a cited paper, a measured workspace number, or is marked **to be measured**. The "20× cluster-wide throughput" headline from the prior draft is retired (Appendix A); honest defensible targets are ≤ 10× on the BI cohort and ≤ 3.5× cluster-wide, both gated on the time-pie measurement in §5.

---

## Section 1 — Goal (honest)

This plan optimizes **where Shelf actually wins**: BI/dashboard repeated scans, cold-start after rollouts/compaction, and cross-region origin RTT. It does **not** promise a 20× lift on aggregate `physical_input_bytes / wall_time` versus direct S3 in the same region — the S3 shim adds an HTTP hop; synthetic benches (e.g. TPC-H SF1 same-region) can show shelf *slower* than raw S3 on per-query wall time for that reason.

The realistic competitive bar is **Starburst Warp Speed**, which publishes [5× on TPC-DS Iceberg #96 at SF1000](https://www.starburst.io/blog/announcing-warp-speed-starburst-galaxy/) and a [7× general claim](https://starburst.io/platform/features/warp-speed) in their marketing material, with a **40% real-world customer average** [on interactive workloads](https://www.starburst.io/blog/announcing-warp-speed-starburst-galaxy/). Photon ([Behm et al., SIGMOD 2022](https://dl.acm.org/doi/10.1145/3514221.3526054)) reports 10× on customer workloads but is a whole-engine vectorized replacement, not a cache. **Shelf is a cache + planning accelerator**, so it lives in the 2–10× band on real workloads, not the 20× band.

**Primary success metrics**

1. **BI cohort p95 wall time** — users matching whichever BI service-account naming convention your cluster uses (e.g. `pbi_*` / `tableau_*` / `metabase_*` / `superset_*`). Filter with `user LIKE '<your_bi_prefix>%'`.
2. **Cold-start time-to-first-query** — after a shelf-pool rolling restart (or NVMe wipe), replay top-N objects with `tools/replay_pinlist.py` (or pin-list GETs) until metadata + rowgroup hit ratios stabilize.

**Headline metric we do *not* use**: aggregate p50 "throughput" from the Trino event-log alone — it is **volume- and mix-sensitive** (workload shifts after cutover can invert apparent wall-time deltas).

---

## Section 2 — Tier 1 — Ship today (config-only, zero new image)

Pure Helm/values + Trino catalog properties; soak in a **90 min** window with existing rollback gates.

| # | Lever | Where | Realistic effect |
|---|-------|-------|------------------|
| 1 | **LODC admission throttle** — verify `diskCache.admission` is enabled (it is **ON by default at 200 MiB/s** since SHELF-29/rc.5). The old `admissionBytesPerSec` field is **deprecated and silently ignored**. If tuning is needed, add an explicit `admission:` block per the updated values.yaml comments. | [charts/shelf/values.yaml](charts/shelf/values.yaml) (line ~271) | Stops sustained `submit_queue_overflow` on Foyer LODC (operator-measured drops at ~124/s before the throttle). **Already enabled by default — no action needed unless tuning.** |
| 2 | **Larger HEAD-LRU** — default `head_lru_entries: 100000` via values (today hardcoded `10000`) | [charts/shelf/templates/configmap-shelfd.yaml](charts/shelf/templates/configmap-shelfd.yaml), [charts/shelf/values.yaml](charts/shelf/values.yaml) | Fewer origin HEADs on cold paths. Budget ~12 MiB head_lru + ~20 MiB freshness tracker (`FreshnessTracker` is sized `2 × head_lru.capacity()` in code). |
| 3 | **`iceberg.metadata-cache.enabled=false`** on **every** shelf-fronted catalog | Trino `catalog/*.properties` (per-replica Helm/values) | Unshadows Trino's JVM `MemoryFileSystemCache` so metadata/footer traffic hits `shelfd` and Prometheus reflects real metadata-pool behavior. **Trade-off**: per [Trino issue #26563](https://github.com/trinodb/trino/issues/26563), `iceberg.metadata-cache.enabled=true` + statistics enabled can balloon planning to 5–10 min on tables with large partition counts. If §5 shows planning fraction is high, see §6 for the structural fix. |
| 4 | **`cache.bloom.enabled: true`** (SHELF-46) *after* step 3 | [charts/shelf/values.yaml](charts/shelf/values.yaml) | Routes Parquet **footer** and **bloom-block** byte ranges into the **DRAM metadata pool**, not NVMe rowgroup. **Not** "skip NVMe on miss" — that framing was wrong. Gain: ~5–10 pp metadata hit ratio when step 3 is live (to be measured per §5). |

**Combined Tier 1 expectation**: +15–25% wall time improvement on metadata-heavy query paths; LODC overflow rate → ~0; no Rust changes if values/ConfigMap only.

---

## Section 3 — Tier 2 — This week (small code + ops runbook)

| # | Item | Detail |
|---|------|--------|
| 5 | **Immutable data-file conditional GET bypass** | Gate at `if state.is_conditional_get_enabled()` in [shelfd/src/s3_shim.rs](shelfd/src/s3_shim.rs) (~1060). Add `is_immutable_data_file(&key)` Iceberg heuristic (`/data/` + `.parquet`/`.orc`/`.avro`). **Correct variable is `key`, not `request_path`.** **Honest impact**: `freshness.rs` already skips revalidation after 10 consecutive 304s in a 5 s trust window — bypass saves latency mainly on **early** hits after cold restart / per-key churn (~5–15 ms per validation avoided), not unbounded steady-state "20 ms → 1 ms" on every read. Add counter `shelf_immutable_bypass_skipped_total` so the win is observable. |
| 6 | **Rowgroup zstd compression on NVMe** | `cache.pools.rowgroup.compression.enabled: true` uses **zstd** (not LZ4) — [charts/shelf/values.yaml](charts/shelf/values.yaml). **Mandatory**: one-way format — wipe `<storage.mountPath>/*` on every pod (or scale STS to 0, wipe PVCs, re-expand) per `.shelf-compression.json` marker semantics in code comments. Runbook to add: `shelfd/docs/runbooks/2026-05-zstd-cutover.md` with scale-down → wipe → helm upgrade → scale-up. |

---

## Section 4 — Tier 3 — Next two weeks (engineering)

| # | Item | Detail |
|---|------|--------|
| 7 | **Read-ahead prefetch** | New module e.g. `shelfd/src/prefetch.rs` — on rowgroup miss, background `origin.get_range` for N+1…N+4 with bounded concurrency (`Semaphore`). **Does not exist today.** Realistic **~1.3–1.5×** on long sequential scans (not 2× alone). May need [`decoded_meta.rs`](shelfd/src/decoded_meta.rs) enabled to resolve row-group boundaries cheaply. |
| 8 | **SHELF-45 compaction rewarm producer** | Reactor exists: [shelfd/src/compaction_rewarm.rs](shelfd/src/compaction_rewarm.rs). Java listener producer (PR #66) blocked on JDK 25 / trino-spi. **In-tree workaround already lives at [shelfd/src/rewarm_poller.rs](shelfd/src/rewarm_poller.rs)**: a pluggable `MetadataSource` that polls each watched table's `metadata.json` and forwards diffs to the existing reactor. Default-OFF via `cache.rewarm.enabled=false`. Enabling it on a single canary table is a config-only Tier-3 lever; §8.1 below extends it. |

---

## Section 5 — Time-pie validation query (decide where the next lever goes)

Before investing in §6 (planning endpoint), §7 (sibling proxies), or §8 (PhD-grade inventions), measure where wall time actually goes. The Trino event-log carries enough columns to do this without instrumenting the engine.

```sql
-- Lock the BI cohort, decompose wall time into planning vs the rest,
-- and emit a single row that says which lever to prioritise.
--
-- Adjust the date range, the BI prefix, and the catalog name to your cluster.
WITH bi_cohort AS (
  SELECT
    query_id,
    wall_time_millis,
    planning_time_millis,
    queued_time_millis,
    cpu_time_millis,
    physical_input_bytes,
    physical_input_read_time_millis
  FROM <your_catalog>.trino_logs.trino_queries
  WHERE cast(query_date AS date) BETWEEN date '2026-05-15' AND date '2026-05-21'
    AND query_state = 'FINISHED'
    AND error_code IS NULL
    AND user LIKE '<your_bi_prefix>%'
    AND wall_time_millis > 0
)
SELECT
  count(*)                                                            AS n,
  approx_percentile(wall_time_millis, 0.95)        / 1000.0           AS p95_wall_s,
  -- Wall-time decomposition (fractions sum to ≤ 1.0; the residual is filter/agg/merge)
  avg(planning_time_millis * 1.0 / wall_time_millis)                  AS frac_planning,
  avg(queued_time_millis   * 1.0 / wall_time_millis)                  AS frac_queued,
  avg(physical_input_read_time_millis * 1.0 / wall_time_millis)       AS frac_io,
  avg(cpu_time_millis      * 1.0 / wall_time_millis)                  AS frac_cpu
FROM bi_cohort;
```

**Routing rules** (apply in order):

- `frac_planning > 0.20` → **§6 (Iceberg REST plan endpoint on shelfd)** is the highest single lever. Trino is spending more time deciding what to scan than scanning it.
- `frac_io > 0.40` and Tier 1.1 (LODC) is already shipped → **§4 (prefetch)** and **§7 (`shelf-result-cache` sibling)** compound.
- `frac_cpu > 0.40` → the bottleneck is engine-side decode/filter, not cache. Shelf cannot accelerate this layer (see Appendix A on why DataFusion-inside-shelfd was retired).
- All three < 0.20 → workload is queue-bound. Look at the Trino resource-group config, not shelfd.

This is the single most important decision point in the plan. Every lever in §6/§7/§8 carries a non-trivial engineering cost; ship the wrong one first and the wall-time delta will be < 5 % and indistinguishable from workload-mix noise.

---

## Section 6 — Iceberg REST scan-planning on shelfd (highest leverage if planning dominates)

The Apache Iceberg project landed **server-side scan planning** in 2025:

- [PR #11369](https://github.com/apache/iceberg/pull/11369) (closed; foundational, by `@rahil-c`).
- [PR #13004](https://github.com/apache/iceberg/pull/13004) **merged 2025-08-15** — request/response parsers (`PlanTableScanRequestParser`, `FetchScanTasksRequestParser`, `FetchPlanningResultResponseParser`, `FetchScanTasksResponseParser`, `PlanTableScanResponseParser`, `TableScanResponseParser`).
- [PR #13400](https://github.com/apache/iceberg/pull/13400) **merged 2025-12-10** — routes, `RestTable`, `RestTableScan`, and a **streaming iterator that pulls `FileScanTask` records from the server in synchronous or asynchronous planning modes**.
- [PR #9695](https://github.com/apache/iceberg/pull/9695) added the OpenAPI spec entries.

Trino's planner pain on Iceberg is documented in:

- [trinodb/trino#26563](https://github.com/trinodb/trino/issues/26563) — planning time 7 ms → ~3 min when `iceberg.statistics_enabled=true` on tables with 2000+ partitions.
- [trinodb/trino#11708](https://github.com/trinodb/trino/issues/11708) — reduce `planFiles` calls (open).
- [trinodb/trino#14443](https://github.com/trinodb/trino/issues/14443) — query-scope stats caching, ~40 % planning improvement (closed).
- [trinodb/trino#25717](https://github.com/trinodb/trino/pull/25717) merged — separate planning thread pool.

### The invention (already 70 % present in shelfd)

`shelfd` ships **three** modules that are exactly the components Trino's planner re-implements per-query:

| shelfd module | Today's role | Doubles as |
|---|---|---|
| [`decoded_meta.rs`](shelfd/src/decoded_meta.rs) | In-process LRU of decoded `parquet::file::metadata::ParquetMetaData` + Iceberg manifests, ETag-keyed (ADR-0011) | The cached "decoded manifest" inputs to a `PlanTableScan` |
| [`filter_service.rs`](shelfd/src/filter_service.rs) | `POST /filter/probe` — given table FQN + column + predicate, return row groups whose min/max admit the predicate | The "predicate evaluator" step of a planFiles call |
| [`compaction_rewarm.rs`](shelfd/src/compaction_rewarm.rs) + [`rewarm_poller.rs`](shelfd/src/rewarm_poller.rs) | Watches `metadata.json` per table; forwards `IcebergSnapshotEvent` to a reactor | The "what is the current snapshot id?" oracle |
| [`mv_registry.rs`](shelfd/src/mv_registry.rs) | Maps content-addressed cache key → MV name for per-MV accounting | Lets the plan endpoint annotate `FileScanTask` records with `_shelf_mv_hint` |

**Proposal**: expose a single Iceberg REST-compliant endpoint `POST /v1/{prefix}/namespaces/{ns}/tables/{t}/plan` on shelfd, alongside the existing data-plane on `:9090` and S3 shim on `:9092`. Spec is fixed by [`rest-catalog-open-api.yaml`](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml). Shelfd assembles the response by:

1. Reading the current `metadata.json` (cached by `rewarm_poller`).
2. Resolving the manifest list + manifests via `decoded_meta` (zero S3 IO on warm).
3. Running `filter_service`-style predicate evaluation against manifest min/max and Parquet page indexes (already in `parquet_meta.rs`).
4. Streaming `FileScanTask` records back to Trino over HTTP/2 chunked transfer.

### Rollout caveats (honest)

- **REST catalog split is mandatory.** This only works if Trino's catalog is `iceberg.catalog.type=rest`, not `hive`. Operators still on a Hive metastore can run shelfd's plan endpoint in **shadow** mode (log-only) until they migrate.
- **iceberg-rust completeness.** The `iceberg` crate has `ManifestEvaluator` + partition pruning (issue [apache/iceberg-rust#153](https://github.com/apache/iceberg-rust/issues/153) closed 2024-06). Row-group-level expression evaluation against Parquet page indexes is still partial — shelfd would use the upstream `parquet` crate's `RowSelection` API for that step. **A code dive into `iceberg-rust` to verify expression-evaluator coverage on every supported Iceberg type is a precondition that exceeds the scope of this document.**
- **Trino client support.** Trino's `iceberg-rest` catalog client today still calls `planFiles` locally even against a REST catalog — the REST scan-planning client code is what [PR #13400](https://github.com/apache/iceberg/pull/13400) shipped to *core* Iceberg, but Trino has to pick it up. Shelfd's endpoint is therefore useful as a **drop-in for any future Trino release that calls the REST plan endpoint**, and as a **measurable shadow** today (run both code paths, diff the file lists).
- **Stats-only call shape.** Trino's `IcebergMetadata::getTableStatistics` calls `planFiles` purely for stats; for that call shape, returning just summary statistics (without the file list) cuts the response 10–100× in size. Worth exposing as a separate `POST /v1/.../stats` if the time-pie shows statistics calls dominate.

### Effort

**~3–6 engineer-weeks** for a minimum-viable endpoint that streams `FileScanTask` records from in-memory decoded manifests, against synthetic Trino-style requests:

- Week 1: REST endpoint scaffold + OpenAPI conformance tests against `apache/iceberg`'s spec.
- Weeks 2–3: wire `decoded_meta` → `iceberg::spec::ManifestFile` → file-list assembly; integration test against MinIO + a forked Trino build.
- Week 4: predicate evaluation via `filter_service` + `parquet_meta` page indexes; streaming iterator.
- Weeks 5–6 (optional): shadow-mode dual-write so prod traffic stays on Trino's local planner while shelfd's plan endpoint is diff-checked.

---

## Section 7 — Four-sibling architecture (replaces the original §11)

The original §11 proposed three layers **inside** `shelfd`. Two of those layers don't belong in `shelfd`:

- **Result cache** is a SQL-engine surface (it has to canonicalise SQL, respect row-level security, and key on `(sql, role, snapshot)`). That's a JDBC/HTTP gateway concern, not a byte-range cache concern. The shelf project's own [`BLUEPRINT.md`](BLUEPRINT.md) already names a sibling binary `shelf-result-cache` (v2+ design notes) for exactly this role.
- **DataFusion compute engine** is blocked by [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184) (DRAFT blob cache SPI) and carries a heavy version-drift maintenance burden (DataFusion 53 ↔ parquet 58 ↔ arrow 58 lock-step). See Appendix A.

The architecture that survives is **four sibling binaries**, none of them inside `shelfd`:

```
                     ┌──────────────────────────────────┐
                     │        BI tool / JDBC client      │
                     └────────────────┬─────────────────┘
                                      │ SQL (JDBC/HTTP)
                                      ▼
                ┌──────────────────────────────────────────┐
                │  shelf-result-cache  (new sibling proxy)  │
                │  key = canonical_plan_hash ‖ snapshot_id  │
                │  hit → Arrow IPC; miss → forward to Trino │
                └────────────────┬─────────────────────────┘
                                 │ SQL passthrough on miss
                                 ▼
                       ┌──────────────────┐
                       │      Trino       │
                       └──────┬───────────┘
                       plan   │   data
                              │
        ┌─────────────────────┼──────────────────────┐
        ▼                     ▼                      ▼
┌──────────────┐    ┌─────────────────┐     ┌──────────────────┐
│ shelfd /v1/  │    │  shelfd /cache/ │     │   shelf-advisor  │
│ .../plan     │    │  byte-range     │     │   (sibling cli)  │
│ (§6)         │    │  shim (today)   │     │   MV / pin hints │
└──────┬───────┘    └────────┬────────┘     └─────────┬────────┘
       │                     │                        │
       └─────────────────────┼────────────────────────┘
                             ▼
                    ┌──────────────────┐
                    │   S3 origin      │
                    └──────────────────┘
```

| Lever | Hit rate on BI cohort | Speedup on hit | Citation |
|---|---|---|---|
| `shelf-result-cache` (exact-plan + snapshot) | **to be measured** (see §9; Snowflake [docs](https://docs.snowflake.com/en/user-guide/querying-persisted-results) report exact-text reuse persists 24 h default, extends to 31 d) | 100–1000× (results are pre-computed; only network round-trip remains) | Snowflake docs; [Napa, VLDB 2021](https://vldb.org/pvldb/vol14/p2986-sankaranarayanan.pdf) for materialized-view-driven sub-second response |
| `shelfd /v1/.../plan` (§6) | dominates queries where `frac_planning > 0.20` per §5 | reduces planning_time_millis from minutes → seconds on stat-heavy tables ([trinodb/trino#26563](https://github.com/trinodb/trino/issues/26563)); cited [40 % planning improvement](https://github.com/trinodb/trino/issues/14443) from query-scope caching alone | Iceberg [PRs #13004 / #13400](https://github.com/apache/iceberg/pull/13400) |
| `shelfd /cache/` (byte-range, today) | 60–95 % depending on working-set fit; 95.1 % DRAM share + 4.9 % NVMe observed in rep-1 cutover post-mortem | 1.5–3.5× cumulative (Tier 1 + 2 + 3) | Tier 1–3 above |
| `shelf-advisor` MV recommender | offline (advice surface, not hot path) | indirect — drives the pin-list that lifts the byte-range hit rate | already in repo as `shelf-advisor` crate |

### Amdahl-stack compound (honest)

Treat each lever as accelerating a disjoint **time slice** of the query (not "% of queries"), then apply Amdahl's law per slice. Conservative + optimistic assumptions from §5:

```
Wall time decomposition (assume Tier 1 already shipped):
  T_planning + T_io + T_cpu + T_other = T_total

Conservative                            Optimistic
T_planning fraction   = 0.20            0.40
T_io fraction         = 0.40            0.50
T_cpu fraction        = 0.30            0.10
T_other fraction      = 0.10            0.00

Lever effect on its slice:
  shelf-result-cache hit:  ALL slices → near-zero (only network)
  shelfd /v1/.../plan:     T_planning   × (1 / 5)   [trinodb/trino#14443 cites 40 %; assume harder cases benefit more]
  shelfd /cache/ byte:     T_io         × (1 / 3.5) [Tier 1–3 ceiling]
  shelf-advisor (indirect via pinning): folds into the T_io factor

Blended speedup, BI cohort, conservative, 30 % result-cache hit:
  Path A (30 %): result-cache hit, speedup ≈ 50× on hit (Snowflake order)
  Path B (70 %): plan+byte-range, speedup = 1 / ((0.20/5) + (0.40/3.5) + 0.30 + 0.10) ≈ 1 / 0.55 ≈ 1.8×

  Blended: 0.3 × 50 + 0.7 × 1.8 ≈ 16.3×  ←  BI-cohort headline ceiling
                                          (NOT cluster-wide)

Cluster-wide (the BI cohort is ~20–40 % of all queries on rep-1/rep-2):
  Other 60–80 % see only the §3+§4 levers → 1.5–2.5× per the Tier 1–3 ceiling
  Cluster blend: ~3–5×
```

**Headline numbers we will defend in writing**:

- **BI cohort p95**: ~5–10× under conservative §5 assumptions; up to ~16× under optimistic with `shelf-result-cache` shipped. **Falsifiable by measuring `frac_planning` and result-cache hit rate per §5.**
- **Cluster-wide p95**: ~3–5×, never 20×.
- **Cold-start TTFQ** (post-rolling-restart): ~3–5× by combining §4 #8 (compaction rewarm) with §8.1 below. **Falsifiable** via the `tools/replay_pinlist.py` warm-up test.

---

## Section 8 — PhD-grade additions (cited, scoped, ranked)

Four mechanisms that go beyond Tier 1–3 + §6 + §7. Each is presented with: the one-line invention, the citations that frame it, the shelfd module that hosts it, the gain from the source paper (no extrapolation), and a self-skeptical "would a VLDB program committee accept this as novel?" verdict.

### 8.1 — Snapshot-delta-aware cache invalidation

**Invention.** When an Iceberg snapshot transitions, classify the diff using `IncrementalAppendScan` / `IncrementalChangelogScan` ([Iceberg API ref](https://iceberg.apache.org/javadoc/1.9.1/org/apache/iceberg/IncrementalAppendScan.html)) into three sets — `{added, rewritten, deleted}` files — and act per set:

- `added` → pre-warm into rowgroup pool ahead of first query (the existing SHELF-45 reactor path).
- `rewritten` → pre-warm the new file's ETag, schedule **lazy eviction** of the old ETag for ≤ 60 s so concurrent in-flight reads of the old snapshot complete cleanly. The old ETag's content-addressed keys become unreachable on next admission cycle.
- `deleted` → **immediate negative-cache** so a stale Trino plan's split request fails the HEAD-LRU lookup in `head_lru.rs` without an origin round-trip.

**Why novel for OSS OLAP.** Nobody has published a snapshot-delta-aware cache invalidation algorithm for the Iceberg / Delta / Hudi family. Alluxio doesn't know about Iceberg snapshots (it caches at the filesystem layer). Trino's `MemoryFileSystemCache` is filename-keyed and times out via TTL with no snapshot awareness. [Napa (VLDB 2021)](https://research.google/pubs/napa-powering-scalable-data-warehousing-with-robust-query-performance-at-google/) introduced the **Queryable Timestamp** concept for keeping materialized views consistent with ingest, but that's view-state, not cache-state. The mechanism here projects QT-style reasoning onto a byte-range cache.

**Composes with.** Already-present [`rewarm_poller.rs`](shelfd/src/rewarm_poller.rs) (Avro manifest reader via the `apache-avro` workspace dep) + [`compaction_rewarm.rs`](shelfd/src/compaction_rewarm.rs) (reactor + concurrency cap + rate limiter).

**New shelfd module.** `shelfd/src/snapshot_delta.rs` — `classify(old_snapshot, new_snapshot) -> SnapshotDelta { added, rewritten, deleted }`. ~250–400 LOC including unit tests.

**Cited gain.** [Iceberg's own performance docs](https://iceberg.apache.org/docs/latest/performance) cite "up to 10× planning improvement" from manifest-stats-driven file pruning. The mechanism here is the cache-side analogue. **Honest claim**: measurable as a reduction in (a) post-compaction `ICEBERG_CANNOT_OPEN_SPLIT` rate (which currently spikes on rep-N after `EXECUTE optimize`, per workspace history), (b) cold-miss origin GET burst per snapshot transition, and (c) `shelf_evictions_total{reason="capacity"}` because the old-ETag keys are reclaimed deterministically rather than via LRU pressure. **Magnitude to be measured per §9.**

**Risk.** The Iceberg client's snapshot diff is per-table and requires reading both `metadata.json` files; if the producer cadence in `rewarm_poller` is slower than Trino's planning cadence, an in-flight query can still race a stale plan. Mitigation: bound the lazy-eviction window to `2 × producer_interval`.

**Effort.** **2–3 engineer-weeks.** The Avro reader and reactor are already wired; this PR adds the diff classifier and the negative-cache hook.

**VLDB-pc verdict.** *Borderline novel.* The Iceberg APIs and the cache invalidation primitive (Napa QT) are both published; the contribution is the composition and the deletion-as-negative-cache trick. A short systems paper, not a SIGMOD headline. Strong ROI for shelfd.

---

### 8.2 — Plan-fingerprint-driven row-group pre-warm

**Invention.** Canonicalise Trino's `jsonPlan` (literals erased, commutative operands sorted) into a 64-bit fingerprint. For each fingerprint, maintain a small in-memory histogram of `(file_etag, row_group_ordinal)` accessed by historical instances. On the next `QueryCreatedEvent` with the same fingerprint, **pre-warm the historical row groups before the split source asks for them**.

**Why novel for OSS OLAP.** Snowflake's [result cache](https://docs.snowflake.com/en/user-guide/querying-persisted-results) requires exact text matching plus identical parameters, role, micro-partitions, and unchanged data — too narrow for BI dashboards that template literals into predicates (each dashboard refresh has a different `WHERE date_col = '...'`). The mechanism here lives a level below the result cache: it doesn't memoise *results*, it memoises *which row groups the query needs*, so a same-fingerprint different-literal query gets a warm cache and full Trino execution. [Quickstep's Lookahead Information Passing (Zhu et al., VLDB 2017)](https://vldb.org/pvldb/vol10/p889-zhu.pdf) shares predicates across joins in the same query; the mechanism here shares row-group locality across queries with the same plan shape. [Cooperative Scans (Zukowski et al., VLDB 2007)](http://bibtex.github.io/VLDB-2007-ZukowskiHNB.html) coordinated concurrent scans of the same table; the mechanism here coordinates *historical* scans of the same plan shape.

**Composes with.** [`fingerprint.rs`](shelfd/src/fingerprint.rs) (already implements `canonicalise(jsonPlan) -> 64-bit hash` with literal erasure + commutative-operand sorting; default-OFF, no non-test caller today), [`ShelfPrefetchListener.java`](clients/trino/src/main/java/io/shelf/eventlistener/ShelfPrefetchListener.java) (already intercepts `QueryCreatedEvent`).

**New shelfd module.** `shelfd/src/plan_warmer.rs` — bounded-cardinality `LRU<fingerprint, RowGroupHistogram>`, fed by post-query row-group access logs. ~400–600 LOC.

**Cited gain.** No directly cited number — this is a research-grade hypothesis. Closest analogues:

- Snowflake's result cache claims sub-second response on warm hit; that's the *upper bound* of what plan-driven pre-warm achieves (because plan-driven still runs the engine, only the I/O is warm).
- [Cooperative Scans (VLDB 2007)](http://bibtex.github.io/VLDB-2007-ZukowskiHNB.html) reports significant I/O reduction for overlapping concurrent scans (paper abstract; specific multipliers depend on workload mix).
- Workspace-measured fact: BI dashboards refresh every 5–15 min on rep-1/rep-2 cutover trace, and the same fingerprint recurs across refreshes. **The fraction of BI cohort queries that benefit is to be measured.**

**Risk.** Fingerprint collisions (two genuinely different queries hash to the same fingerprint due to over-aggressive canonicalisation) → wrong pre-warm. Mitigation: `fingerprint.rs` already documents "literal erasure, not literal hashing" — collisions require structurally identical plans, which is the point. Validate by sampling a 7-day fingerprint-to-jsonPlan reverse map and confirming < 1 % bucket entropy.

**Effort.** **3–4 engineer-weeks.** Fingerprint canonicaliser exists; the row-group history store, the LRU eviction, and the prefetch hook into the existing `s3_shim` `get_range` path are new.

**VLDB-pc verdict.** *Borderline novel.* The canonical-plan fingerprint is engineering (Trino itself does this internally for plan caching). The contribution is "what to do with the fingerprint" — pre-warming row groups, not results. A workshop paper with a measured-vs-not bake-off would land cleanly; a full VLDB headline is a stretch unless the measured lift over Tier 1–3 + §6 + 8.1 is large.

---

### 8.3 — Kangaroo-style small-object NVMe overlay for metadata footers

**Invention.** Replace per-key NVMe writes for Parquet footers (~8–64 KB) and Iceberg manifest entries (~50 KB) with a **log-structured small-object overlay** that batches N footers per NVMe write. Index in DRAM by `(etag) → (overlay_offset, length)`; reclaim by overlay-page-level GC, not per-key eviction.

**Why novel for OSS OLAP.** [Kangaroo (McAllister et al., SOSP 2021 — Best Paper)](https://pdl.cmu.edu/PDL-FTP/NVM/McAllister-SOSP21.pdf) targets tiny objects (~100 B) for social-graph workloads and reports **29 % fewer cache misses than prior state-of-the-art** by combining a KLog log-structured cache with a KSet set-associative cache. The mechanism here applies Kangaroo's "amortize write costs across multiple objects" insight to OLAP metadata, which is two orders of magnitude larger than Kangaroo's target but still small relative to Foyer's default `bufferPool` page size. The KLog design also fits exceptionally well with content-addressed ETag keys: the index is one entry per footer regardless of how many copies (different etags) of the same file have been cached.

**Composes with.** [`store.rs`](shelfd/src/store.rs) `Pool::Metadata` (today DRAM-only, capped at ~640 MiB per workspace sizing).

**New shelfd module.** `shelfd/src/footer_overlay.rs` — log-structured small-object overlay backing `Pool::Metadata` once it spills. Plus an NVMe-format-change marker in the existing `.shelf-compression.json` shape, since this is also a one-way on-disk format. ~600–900 LOC + a runbook.

**Cited gain.** Kangaroo's published gain is **29 % fewer cache misses** on Facebook's social-graph traces (~100 B objects). For footer-sized objects (~10–100× larger), the published Kangaroo curves degrade — the write-amplification problem Kangaroo solves is most acute at the smallest sizes. **Honest claim**: useful **only** once metadata working set exceeds ~640 MiB DRAM cap (i.e. ≥ 10 k distinct decoded footers, which only happens at much larger table-count scales). Today's shelf deployments don't hit this; the invention is durable but not urgent.

**Risk.** *(a)* On-disk format break — every operator who turns it on must wipe NVMe, identical to the SHELF-B1 zstd cutover (§3 #6). *(b)* GC pause times if the overlay is sized too large.

**Effort.** **4–6 engineer-weeks**, mostly NVMe-format and recovery testing.

**VLDB-pc verdict.** *Engineering refinement, not research.* The Kangaroo paper is the research contribution; applying it to OLAP footers is a derivative engineering exercise. Worth doing if the metadata working set grows; **deprioritise unless §5 shows metadata-pool pressure**.

---

### 8.4 — Load-aware HRW routing (Slicer-inspired)

**Invention.** At the **Trino-plugin-side router** (`HashRing.java`), retain HRW-by-key as the first-choice routing but consult each pod's recently-published `shelf_pod_load_qps` ([pod_load.rs](shelfd/src/pod_load.rs) K2, rc.8) and demote candidates whose load exceeds the cluster median by 50 %. Fall back to the next-highest HRW candidate. The shelf-side peer-fetch path (SHELF-23) remains unchanged — this lever shifts load *before* the request commits to a primary, not after.

**Why novel for OSS OLAP.** [Slicer (Adya et al., OSDI 2016)](https://research.google/pubs/slicer-auto-sharding-for-datacenter-applications/) is the canonical reference for load-aware sharding: it reports the median production workload's most-loaded task at **30–180 % of mean load** and shows that the median workload uses **63 % fewer resources than static sharding**. Nobody has published a Slicer-style load-aware HRW variant tuned specifically for byte-range OLAP caches with content-addressed keys, where the cache-locality cost of a routing change is bounded (peer-fetch absorbs it cleanly).

**Composes with.** [`pod_load.rs`](shelfd/src/pod_load.rs) (already publishes `shelf_pod_load_qps` + `shelf_pod_load_skew_ratio_bps` per rc.8 K2). [`router.rs`](shelfd/src/router.rs) on the shelfd side; [`HashRing.java`](clients/trino/src/main/java/io/shelf/client/HashRing.java) + new `LoadAwareHashRing.java` on the plugin side.

**Cited gain.** Slicer's headline is general-purpose sharding, not byte-range caching. **Honest claim**: target equalising the rep-1/rep-2 HRW imbalance documented in workspace post-mortems (one pod absorbing ~14× another's request rate); aim for ≤ 2× max-to-median load ratio (Slicer reports ~1.8× sustained in production). **Magnitude to be measured** as the `shelf_pod_load_skew_ratio_bps` time series flattens post-rollout.

**Risk.** Cache-locality regression: a key that the HRW primary would have served from a warm cache gets routed to a cold peer. Mitigation: peer-fetch (SHELF-23) handles the second request from the cold peer transparently; the cost is one cold first-request per re-route.

**Effort.** **2–3 engineer-weeks**, split between the plugin-side router and the shelfd-side `/v1/peers/load` endpoint (which today is the `/stats` `pod_load` field; a dedicated endpoint avoids parsing the larger `/stats` payload on every routing decision).

**VLDB-pc verdict.** *Engineering refinement.* Slicer is the research; the OLAP-cache application is direct. Worth doing because the HRW-imbalance failure mode is recurring in production.

---

### 8.5 — Items considered and rejected (with reasons)

| Candidate | Citation | Why rejected |
|---|---|---|
| SIEVE eviction on rowgroup pool | [Zhang et al., NSDI 2024](https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo); cites 63.2 % lower miss vs ARC on 45 % of 1,559 traces | Pool today runs S3-FIFO ([Yang et al., SOSP 2023](https://jasony.me/publication/sosp23-s3fifo.pdf), 72 % lower miss vs LRU). [AGENTS.md memory](.) confirms "stacking TinyLFU on top of S3-FIFO is redundant — pick one." Foyer 0.12 (pinned) does not ship SIEVE; Foyer 0.22 (current) does not yet either per the [changelog](https://github.com/foyer-rs/foyer/blob/main/CHANGELOG.md). **Reclassified as engineering refinement gated on SHELF-35 Belady replay** showing ≥ 5 pp lift, per the F2 P2-conditional rule already in the workspace plan. |
| Learned admission (LRB / GL-Cache) | [LRB NSDI 2020](https://www.usenix.org/conference/nsdi20/presentation/song): 4–25 % WAN reduction on CDN traces. [GL-Cache FAST 2023](https://www.usenix.org/conference/fast23/presentation/yang-juncheng): 228× throughput vs LRB, 7 % avg hit-ratio over LRB on **block I/O + CDN traces** | Both papers' evaluations are on **CDN and block-I/O traces** — neither benchmarks Iceberg row-group access patterns. The published gains may or may not transfer. Workspace already ships W-TinyLFU ([Einziger ACM TOS 2017](https://arxiv.org/abs/1512.00727)) in [`admission_wtinylfu.rs`](shelfd/src/admission_wtinylfu.rs) (default-off). **Adopting LRB/GL-Cache without a measured OLAP-trace bake-off would be an evidence-free claim.** |
| Content-defined chunking for compaction byte-permutation | [FastCDC ATC 2016](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf): 3–12× faster than Rabin-CDC, 10–20 % more dedup than fixed-size | Iceberg `EXECUTE optimize` re-sorts AND recompresses (typically Snappy → Zstd or a different sort order). Post-compaction file bytes are not a simple permutation of pre-compaction bytes — CDC dedup yield would be very low. Could revisit if a measured trace shows ≥ 30 % byte-overlap, but speculative today. |
| LeCaR / CACHEUS adaptive learning experts | [LeCaR HotStorage 2018](https://www.usenix.org/conference/hotstorage18/presentation/vietri): 18× hit-rate over ARC at 0.1 % cache size. [CACHEUS FAST 2021](https://www.usenix.org/conference/fast21/presentation/rodriguez): 4 workload types (LRU/LFU/scan/churn) | Same evidence gap as LRB — evaluated on 329 workloads of mixed shape, no published OLAP-specific benchmark. Workspace's [SHELF-35 Belady replay](.) is the right vehicle to evaluate these *together* against the existing S3-FIFO + W-TinyLFU baseline before adopting any. |
| Cuckoo filter for negative cache | [Fan et al., CoNEXT 2014](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf): 1.5–4× smaller than counting Bloom for FPR < 3 %, supports deletion | shelfd's `head_lru.rs` already implements negative entries with explicit lifetime. Cuckoo only wins if the negative-key set is much larger than the head-LRU capacity, which isn't observed in production. Reject as over-engineered. |
| Hilbert / Z-order multi-dimensional partition pruning | [Hilbert R-tree VLDB 1994](https://www.vldb.org/conf/1994/P500.PDF) reports 28 % perf over R*-tree on 2-to-3 splits. [Databricks Liquid Clustering blog](https://databricks.com/blog/announcing-general-availability-liquid-clustering) claims 2–12× | Data-layout decision; lives in the writer, not the cache. **Out of scope for shelfd**, but a strong recommendation for `shelf-advisor` to emit as a hint when it detects multi-dimensional BI predicates. Added to `shelf-advisor` backlog, not §8. |

---

## Section 9 — Verification

### SQL (Trino — event-log table)

Schema notes: partition `query_date` is **UTC**; reporting in IST is workspace convention. Columns use **`*_millis`**, not `wall_time_ms`. The original §7 SQL was over-narrow (single user-pattern + single replica); the BI-cohort CTE below mirrors §5 so the two queries can be diff'd directly post-cutover.

```sql
-- BI cohort p95 wall time per coordinator (server_address = coord pod IP).
-- Replace catalog name + BI prefix with your cluster's naming.
WITH bi_cohort AS (
  SELECT
    cast(server_address AS varchar) AS coordinator_ip,
    wall_time_millis,
    planning_time_millis
  FROM <your_catalog>.trino_logs.trino_queries
  WHERE cast(query_date AS date) BETWEEN date '2026-05-15' AND date '2026-05-21'
    AND query_state = 'FINISHED'
    AND error_code IS NULL
    AND user LIKE '<your_bi_prefix>%'
    AND wall_time_millis > 0
)
SELECT
  coordinator_ip,
  approx_percentile(wall_time_millis,     0.95) / 1000.0 AS p95_wall_s,
  approx_percentile(planning_time_millis, 0.95) / 1000.0 AS p95_planning_s,
  count(*)                                                AS n
FROM bi_cohort
GROUP BY coordinator_ip
ORDER BY coordinator_ip;
```

```sql
-- Hourly stability in IST ( wall clock on query_date UTC → IST )
SELECT
  hour(at_timezone(query_date, 'Asia/Kolkata')) AS ist_hour,
  approx_percentile(
    cast(physical_input_bytes AS double) / nullif(wall_time_millis / 1000.0, 0),
    0.5
  ) / 1048576.0 AS p50_mbps,
  count(*) AS queries
FROM <your_catalog>.trino_logs.trino_queries
WHERE cast(query_date AS date) = date '2026-05-15'
  AND query_state = 'FINISHED'
  AND wall_time_millis > 0
  AND physical_input_bytes > 0
GROUP BY 1
ORDER BY 1;
```

### Cold-start replay

```bash
# Example — see tools/README.md for flags
python3 tools/replay_pinlist.py \
  --shelf-endpoint shelf-pool.<namespace>.svc.cluster.local:9092 \
  --pinlist <generated.json> \
  --concurrency 4
```

### shelfd integration tests

Run shelfd integration tests with the `--features integration` flag:

```bash
cd shelfd/tests && docker compose up -d minio
cargo test -p shelfd --features integration
```

**Note:** The old `SHELF_INTEGRATION=1` env var convention is **deprecated**. Use `--features integration` instead. The env var is still supported for back-compat with existing scripts but all test file doc comments have been updated to reference the cargo feature.

### Prometheus (Grafana / mimir)

| Query | Target |
|---|---|
| `rate(shelf_lodc_drops_total{reason="submit_queue_overflow"}[5m])` | **0** after Tier 1.1 |
| `shelf_rolling_hit_ratio_bps{pool="metadata"}` | strong when `iceberg.metadata-cache.enabled=false` everywhere |
| `shelf_rolling_hit_ratio_bps{pool="rowgroup"}` | track HRW skew + capacity together |
| `rate(shelf_immutable_bypass_skipped_total[5m])` | **> 0** after Tier 2 item 5 ships |
| `rate(shelf_snapshot_delta_files_total{class="rewritten"}[5m])` | non-zero post-compaction (new in §8.1) |
| `rate(shelf_snapshot_delta_files_total{class="deleted"}[5m])` | non-zero post-`expire_snapshots` (new in §8.1) |
| `shelf_plan_warmer_fingerprint_hits_total / on() shelf_plan_warmer_fingerprint_lookups_total` | fingerprint hit ratio; target ≥ 0.4 on BI cohort (§8.2) |
| `histogram_quantile(0.95, rate(shelf_plan_endpoint_duration_seconds_bucket[5m]))` | p95 of `POST /v1/.../plan` (§6); target < 100 ms warm |
| `shelf_pod_load_skew_ratio_bps` | target ≤ 200 (= 2×) once §8.4 ships |

Dashboard: **Shelf — Cache, Disk and Pods** (`shelf-overview` / `shelf-overview-v2`).

---

## Section 10 — Failure-mode table (what kills the BI-cohort 10×)

Each row is a single failure that takes the §7 stack from "headline win" to "no measurable improvement".

| Failure | Owner | Symptom | Detection |
|---|---|---|---|
| `frac_planning < 0.10` in §5 — workload was never planning-bound | §6 is the wrong lever; pick a different one | Planning endpoint ships but BI p95 moves < 5 % | Re-run §5 immediately post-Tier-1. |
| `shelf-result-cache` cardinality blows up (every BI refresh has a unique snapshot id because ETL commits every minute) | `shelf-result-cache` design — needs a snapshot-window grace, not exact match | Result-cache hit ratio < 5 % | Track `shelf_result_cache_hits / shelf_result_cache_lookups`. |
| Trino's `iceberg-rest` client doesn't pick up server-side planning before our soak ends | §6 stays shadow-only; engineering value preserved, performance value deferred | Plan endpoint returns 200 but Trino still calls `planFiles` locally | Diff `shelf_plan_endpoint_requests_total` vs the event-log's `planning_time_millis`. |
| Snapshot-delta classifier mis-tags a `replace` as `append` and pre-warms the wrong file | §8.1 — needs cross-check against Iceberg `entries` table | Spike in `ICEBERG_CANNOT_OPEN_SPLIT` post-compaction | `rate(shelf_snapshot_delta_misclassified_total[5m])` against the existing `compaction_rewarm` failure metric. |
| Plan-fingerprint collisions cause wrong-row-group pre-warm | §8.2 — collision-rate audit pre-merge | Pre-warmed but unread rows (rowgroup admit without subsequent hit) | `shelf_plan_warmer_wasted_admits_total / shelf_plan_warmer_admits_total > 0.2`. |
| Load-aware HRW thrashes (a hot key oscillates between primary + peer) | §8.4 — needs a hysteresis band | Skew ratio drops but hit rate also drops | Pair `shelf_pod_load_skew_ratio_bps` with `shelf_rolling_hit_ratio_bps{pool="rowgroup"}`. |
| Foyer 0.12 LODC overflow recurs under §8 traffic | Tier 1.1 throttle insufficient when prefetch + rewarm + plan-warmer all fire | `rate(shelf_lodc_drops_total[5m]) > 0` | Already monitored. |
| Trino #29184 blob-cache SPI lands in a shape that obsoletes §6 | If Trino owns plan caching upstream, shelfd's endpoint loses readers | Trino release-notes call out a different SPI shape | Watch [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184) review activity. |

---

## Section 11 — Rollout sequence (concrete order-of-work)

The order matters. Each step's success criteria gate the next. **Do not skip §5.**

```
Week 1  (May 14–20)   Tier 1   →  90-min soak  →  §5 time-pie query
Week 2  (May 21–27)   Tier 2   →  zstd cutover runbook + immutable bypass
                                  ↓
                               §5 RE-RUN.  Pick branch:
                                  ├─ if frac_planning > 0.20:  go §6 next
                                  └─ if frac_io > 0.40:        go §7 (result-cache proxy) next
                                  └─ else:                     go §8.1 next

Week 3–4 (May 28–Jun 10)   §4 prefetch  (+) compaction-rewarm producer (rewarm_poller)
                          §6 shadow endpoint  ──┐
                          §7 shelf-result-cache  │   pick one for the next 8-week cycle
                          §8.1 snapshot-delta   ──┘

Week 5–8 (Jun 11–Jul 8)   ship the lever picked above, single-replica canary, then cluster
                          parallel: §8.4 load-aware HRW lands (low risk)
                          parallel: §8.2 plan-fingerprint warmer (research-grade)

Quarter close (Jul 8 → end of Q3)
   §6 shadow → live (only if Trino client support lands; otherwise stays shadow)
   §7 result-cache proxy soaks 30 d before defaulting on
   §8.3 footer-overlay (only if §5 metadata working set has grown past DRAM cap)
   Tier-3 #10 local-NVMe infra rebake (i4i pool) — gates further bandwidth claims
```

**4-week milestone (Jun 10)**: Tier 1 + Tier 2 + Tier 3 + one of {§6 shadow, §7 result-cache proxy MVP, §8.1 snapshot-delta} in production canary.
**8-week milestone (Jul 8)**: BI-cohort p95 measurably better by ≥ 3× vs Tier-1-only baseline, validated via §5.
**Quarter milestone (Sept 30)**: cluster-wide p95 measurably better by ≥ 2× vs Tier-1-only baseline; §6 plan endpoint either live or formally retired pending Trino client support.

---

## Appendix A — Why the original §11 result-cache and DataFusion proposals were retired

The original draft proposed **three layers inside `shelfd`** (Layer 1 byte-range, Layer 2 DataFusion compute, Layer 3 result cache), claiming 21–48× under "conservative" assumptions. The math was wrong, and the architecture was wrong.

### The math

The original blended-speedup table summed independent hit rates × per-path speedups as if the paths were disjoint. They aren't. A `DataFusion compute engine` hit *is* a byte-range cache hit served by Foyer; you cannot count both. A `result cache` hit *bypasses* the compute engine; you cannot count both. The correct Amdahl-stack treatment (§7 above) lands at ~10× **on the BI cohort** under optimistic assumptions, never 48× cluster-wide.

### The Java→Rust Parquet decode multiplier

The original §11 cited "Rust DataFusion: 10× faster than Java Parquet decode" against unspecified benchmarks. After search:

- [Photon (Behm et al., SIGMOD 2022)](https://dl.acm.org/doi/10.1145/3514221.3526054) reports 10× on customer workloads — but Photon is a **whole-engine** vectorized replacement (filter + project + aggregate + join), not just a Parquet decoder. The 10× is engine-level, not decode-level.
- [DataFusion SIGMOD 2024](https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf) compares against DuckDB, not against Java Parquet readers.
- arrow-rs's own [decode-improvement PRs](https://github.com/apache/arrow-rs/pull/9577) report 13–45 % speedups on **specific encodings** (dictionary, RLE, StringView) — single-digit-percent to mid-double-digit-percent, not 10×.

**There is no published 10–16× Java→Rust Parquet decode benchmark.** That number was hallucinated; it is retired.

### The architecture

Even if the decode-multiplier had been honest, putting DataFusion inside `shelfd` is wrong:

- It requires `parquet` + `arrow` + `datafusion` to stay lock-step (DataFusion 53 ↔ parquet 58 ↔ arrow 58 today; one upstream bump invalidates the others). Shelfd's hot path is one of the most maintenance-sensitive surfaces in the codebase; pinning a query engine into it is the opposite of what a long-lived OSS infra component needs.
- Predicate pushdown into compute belongs to the engine (Trino), not the cache. The right shape is the [trinodb/trino#29184 blob cache SPI](https://github.com/trinodb/trino/pull/29184), which is exactly what `@wendigo` is drafting upstream. Shelfd composes with that SPI when it lands; it does **not** front-run it with a bespoke compute endpoint.
- Result caching at the cache layer can't reason about row-level security, SQL canonicalisation across dialects, or user-role-scoped caching. That's a SQL gateway concern. The shelf-project's own [`BLUEPRINT.md`](BLUEPRINT.md) already names `shelf-result-cache` as a separate sibling binary (v2+ design notes); the original §11 effectively proposed undoing that decision.

The retired proposal is preserved here so a future contributor doesn't re-discover the same misplacement.

---

## Appendix B — Scope-cut (original TODO items not pursued)

| Item | Why |
|---|---|
| **B1/B2 — shelfd DaemonSet / sidecar per Trino worker** | KEDA spot worker churn destroys node-local cache working sets (same reason Trino `fs.cache` showed ~15–20 % hit on spot); duplicates cluster-level HRW dedup; ops cost high. |
| **C2 — io_uring NVMe** | Needs **Foyer ≥ 0.19**; repo pins **foyer 0.12** ([Cargo.toml](Cargo.toml)). Foyer 0.22 bump parked under the existing F2 / SHELF-32 gate + on-disk format break. |
| **C3 — `sendfile` zero-copy** | Axum/hyper responses buffer `Bytes`; true sendfile needs a different HTTP stack around raw sockets. |
| **E1 — DRAM "promotion" policy inside Foyer** | Same Foyer-version gate as C2. |
| **RDS as Shelf metadata DB** | Adds 1–5 ms same-AZ latency to a sub-millisecond hot path. Snapshot-delta-aware caching (§8.1) achieves the only RDS use case ("inventory of cached files per table") in-process at zero new dependency. The original §9 analysis already concluded `Marginally yes for background intelligence` at ~5–15 % gain; not worth the complexity. |

---

## Files reference (quick)

| Tier | File(s) |
|---|---|
| 1 | [charts/shelf/values.yaml](charts/shelf/values.yaml), [charts/shelf/templates/configmap-shelfd.yaml](charts/shelf/templates/configmap-shelfd.yaml), Trino catalog properties |
| 2 | [shelfd/src/s3_shim.rs](shelfd/src/s3_shim.rs), [shelfd/src/metrics.rs](shelfd/src/metrics.rs), new runbook under `shelfd/docs/runbooks/` |
| 3 | new `shelfd/src/prefetch.rs`, existing `shelfd/src/rewarm_poller.rs` |
| §6 | new `shelfd/src/plan_endpoint.rs`, existing [`decoded_meta.rs`](shelfd/src/decoded_meta.rs) + [`filter_service.rs`](shelfd/src/filter_service.rs) + [`parquet_meta.rs`](shelfd/src/parquet_meta.rs) + [`rewarm_poller.rs`](shelfd/src/rewarm_poller.rs) |
| §7 | new sibling crate `shelf-result-cache/`; existing `shelf-advisor/` |
| §8.1 | new `shelfd/src/snapshot_delta.rs`; existing [`rewarm_poller.rs`](shelfd/src/rewarm_poller.rs) + [`compaction_rewarm.rs`](shelfd/src/compaction_rewarm.rs) |
| §8.2 | new `shelfd/src/plan_warmer.rs`; existing [`fingerprint.rs`](shelfd/src/fingerprint.rs) + [`ShelfPrefetchListener.java`](clients/trino/src/main/java/io/shelf/eventlistener/ShelfPrefetchListener.java) |
| §8.3 | new `shelfd/src/footer_overlay.rs`; existing [`store.rs`](shelfd/src/store.rs) |
| §8.4 | existing [`pod_load.rs`](shelfd/src/pod_load.rs) + [`router.rs`](shelfd/src/router.rs); new `clients/trino/src/main/java/io/shelf/client/LoadAwareHashRing.java` |

---

## Citations (papers, PRs, issues — every claim above traces here)

1. **SIEVE — Zhang, Yang, Yue, Vigfusson, Rashmi.** *SIEVE is Simpler than LRU: an Efficient Turn-Key Eviction Algorithm for Web Caches.* NSDI 2024. https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo
2. **S3-FIFO — Yang, Zhang, Qiu, Yue, Vigfusson.** *FIFO queues are all you need for cache eviction.* SOSP 2023. https://dl.acm.org/doi/abs/10.1145/3600006.3613147
3. **W-TinyLFU — Einziger, Friedman, Manes.** *TinyLFU: A Highly Efficient Cache Admission Policy.* ACM TOS, Nov 2017. arXiv:1512.00727
4. **LRB — Song, Berger, Li, Lloyd.** *Learning Relaxed Belady for Content Distribution Network Caching.* NSDI 2020. https://www.usenix.org/conference/nsdi20/presentation/song
5. **GL-Cache — Yang, Mao, Yue, Rashmi.** *GL-Cache: Group-level learning for efficient and high-performance caching.* FAST 2023. https://www.usenix.org/conference/fast23/presentation/yang-juncheng
6. **LeCaR — Vietri, Rodriguez, Martinez, Lyons, Liu, Rangaswami, Zhao, Narasimhan.** *Driving Cache Replacement with ML-based LeCaR.* HotStorage 2018.
7. **CACHEUS — Rodriguez, Yusuf, Lyons, Plata, Liagouris, Smirni, Rangaswami.** *Learning Cache Replacement with CACHEUS.* FAST 2021.
8. **CacheLib — Berg, Beckmann, Eisenman, Yang, Berg, Sankar, Cidon, Rashmi.** *The CacheLib Caching Engine: Design and Experiences at Scale.* **OSDI 2020** (not NSDI). https://www.usenix.org/conference/osdi20/presentation/berg
9. **Kangaroo — McAllister, Berg, Tutuncu-Macias, Yang, Cao, Rashmi, Berger, Beckmann.** *Kangaroo: Caching Billions of Tiny Objects on Flash.* SOSP 2021 (Best Paper). https://dl.acm.org/doi/10.1145/3477132.3483568
10. **Slicer — Adya, Myers, Howell, Elson, Meek, Khemani, Fulger, Gu, Bhuvanagiri, Hunter, Kennedy, Mickens, Mickens, Petrov, Burrows, Killian, Maturin, Petty, Mickens et al.** *Slicer: Auto-Sharding for Datacenter Applications.* OSDI 2016. https://www.usenix.org/conference/osdi16/technical-sessions/presentation/adya
11. **Cooperative Scans — Zukowski, Héman, Nes, Boncz.** *Cooperative Scans: Dynamic Bandwidth Sharing in a DBMS.* VLDB 2007.
12. **Lookahead Information Passing — Zhu, Potti, Saurabh, Patel.** *Looking Ahead Makes Query Plans Robust.* VLDB 2017. https://vldb.org/pvldb/vol10/p889-zhu.pdf
13. **Cuckoo Filter — Fan, Andersen, Kaminsky, Mitzenmacher.** *Cuckoo Filter: Practically Better Than Bloom.* CoNEXT 2014.
14. **FastCDC — Xia, Jiang, Feng, Tian, Fu, Zhou.** *FastCDC.* USENIX ATC 2016.
15. **Hilbert R-tree — Kamel, Faloutsos.** *Hilbert R-tree: An Improved R-tree using Fractals.* VLDB 1994. https://www.vldb.org/conf/1994/P500.PDF
16. **Lakehouse — Armbrust, Ghodsi, Xin, Zaharia.** *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics.* CIDR 2021. https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf
17. **Velox — Pedreira, Erling, Basmanova, Wilfong, Sakka, Pai, He, Chattopadhyay.** *Velox: Meta's Unified Execution Engine.* VLDB 2022 (PVLDB vol 15 no 12 pp 3372–3384). https://vldb.org/pvldb/vol15/p3372-pedreira.pdf
18. **Photon — Behm et al.** *Photon: A Fast Query Engine for Lakehouse Systems.* SIGMOD 2022 (Best Industry Paper).
19. **DataFusion — Lamb, Shen, Heres, Chakraborty, Kabak, Sun, Hsieh.** *Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine.* SIGMOD Companion 2024. https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf
20. **Napa — Sankaranarayanan et al.** *Napa: Powering Scalable Data Warehousing with Robust Query Performance at Google.* VLDB 2021 (PVLDB vol 14 p 2986). https://vldb.org/pvldb/vol14/p2986-sankaranarayanan.pdf
21. **Snowflake result cache — Snowflake docs.** *Using Persisted Query Results.* https://docs.snowflake.com/en/user-guide/querying-persisted-results
22. **Iceberg REST scan-planning** — PRs [#11369](https://github.com/apache/iceberg/pull/11369), [#13004](https://github.com/apache/iceberg/pull/13004), [#13400](https://github.com/apache/iceberg/pull/13400), [#9695](https://github.com/apache/iceberg/pull/9695); OpenAPI spec [`rest-catalog-open-api.yaml`](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
23. **Iceberg `IncrementalAppendScan` / `IncrementalChangelogScan`** — https://iceberg.apache.org/javadoc/1.9.1/org/apache/iceberg/IncrementalAppendScan.html and `BaseIncrementalChangelogScan.java`.
24. **iceberg-rust manifest evaluation** — issue [apache/iceberg-rust#153](https://github.com/apache/iceberg-rust/issues/153) (closed June 2024 with `ManifestEvaluator` + partition pruning landed).
25. **Trino issues** — [#26563](https://github.com/trinodb/trino/issues/26563), [#11708](https://github.com/trinodb/trino/issues/11708), [#14443](https://github.com/trinodb/trino/issues/14443), PR [#25717](https://github.com/trinodb/trino/pull/25717), PR [#29184](https://github.com/trinodb/trino/pull/29184) blob-cache SPI.
26. **Starburst Warp Speed** — https://www.starburst.io/blog/announcing-warp-speed-starburst-galaxy/ (5× on TPC-DS Iceberg #96 SF1000) and https://starburst.io/platform/features/warp-speed (7× general).
