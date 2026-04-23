# Shelf — a smart, Iceberg-native cache for Trino

> Codename: **Shelf** (iceberg shelf + "on the shelf"). Happy to rename.
> Status: design blueprint v0.3. Meant to be iterated on with the team.
> License intent: **Apache 2.0**. Public from day one.
>
> **v0.3 changes** (on top of v0.2): added § 7.4 *Approximate in-cache
> indexes* (Parquet bloom recommender, side-built blooms in DRAM,
> z-order awareness) to close the selective-point-lookup gap vs
> Warp Speed, and § 7.5 *MV-aware caching* + Phase 10 *Incremental MV
> refresh on snapshot delta* to close the dashboard-aggregation gap
> vs Firebolt. Added Phases 8, 9, 10 to the roadmap accordingly;
> total timeline now ≈ 9-10 months to Phase 10 (phases 8-10 can run
> in parallel with the OSS launch).
>
> **v0.2 changes**: (1) plan-aware prefetch rescoped to be honest about
> what `QueryCreatedEvent` actually exposes — file-level at plan time,
> row-group-level via plugin observation + `SplitCompletedEvent`
> learning; (2) ONNX admission latency corrected to 10-50 µs (was
> 1 µs, which was 10-50× optimistic); (3) data plane split into HTTP
> (< 1 MB: manifests, footers) and Arrow Flight (≥ 1 MB: row groups)
> to avoid IPC framing overhead on small objects; (4) `shelf-result-cache`
> explicitly separated from `shelfd` as a companion binary;
> (5) explicit client-side retry + circuit-breaker state machine for
> the Trino plugin to handle spot churn gracefully.

---

## 1. TL;DR

Existing analytical caches (Trino `fs.cache`, Alluxio 2.x / 3.x DORA, Dremio C3,
Starburst Warp Speed) share three limitations that we hit in production every day:

1. **They're file-block caches.** They don't know what a Parquet row group, a
  Parquet footer, or an Iceberg manifest is. So they waste NVMe on bytes nobody
   queries, and they can't warm just the 8 KB footer (which is usually the
   difference between a 50 ms and a 2 s query).
2. **They're blind to the engine's plan.** The engine decides which splits and
  which predicates 5 ms before it reads them. Every existing cache is reactive,
   it only caches after the first read misses. We pay that cold-miss tax on
   every scale-up.
3. **They're tied to the engine or the vendor.** fs.cache = Trino-only, can't
  share across replicas. Alluxio = Java/JVM, operationally heavy, proxy is a
   bottleneck. Warp Speed / C3 = proprietary.

**Shelf** is a small, focused, open-source service that fixes all three.


| Design choice                                                                                                                             | Why                                                                                     |
| ----------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Columnar-range granularity (Iceberg manifest / Parquet footer / row group byte-range)                                                     | 10-100× better cache density than file-block caches                                     |
| Plan-aware **push prefetch** from Trino coordinator → cache (file-level at plan time, row-group-level via plugin observation — see § 7.2) | Eliminates cold-miss tax, unique to engines with a real planner                         |
| Content-addressed keys (hash of the S3 ETag + byte range), snapshot-tagged for Iceberg pointers                                           | Dedup across Trino replicas, natural invalidation on Iceberg commits                    |
| Learned admission (trained on `trino_logs`) + SIEVE eviction                                                                              | Best-in-class hit rate, O(1) hit-path cost                                              |
| Rust cache plane; **HTTP for < 1 MB, Arrow Flight for ≥ 1 MB** data plane; embedded Raft control                                          | No JVM GC, zero-copy on bulk, no IPC framing tax on small objects                       |
| Shared across all 4 Trino replicas                                                                                                        | One warm cache instead of four cold ones                                                |
| Decoupled from compute (separate StatefulSet with NVMe)                                                                                   | Survives KEDA spot churn, unlike fs.cache                                               |
| Plugin is fail-open: every Shelf error becomes a transparent fall-through to S3                                                           | Trino never sees a Shelf-specific error, even during spot churn                         |
| Approximate in-cache indexes: Parquet bloom-filter recommender, side-built blooms in DRAM, z-order / sort-order awareness (§ 7.4)         | Closes most of Warp Speed's selective-filter advantage without building an index engine |
| MV-aware caching + incremental MV refresh on Iceberg snapshot delta (§ 7.5, Phase 10)                                                     | Matches Firebolt's aggregating indexes for dashboard queries using OSS components only  |


Target: **p50 scan latency ≤ 1.2× direct S3 on miss, ≥ 20× direct S3 on hit**, at
70-85% hit rate on our workload, with one operator on call instead of a team.
On the query patterns where commercial caches traditionally win — selective
equality predicates (Warp Speed) and dashboard aggregations (Firebolt) — Shelf
closes the gap via § 7.4 and § 7.5 rather than feature-matching with new
index engines.

### Non-negotiable invariants

These hold on every build, every release, forever. They are not
tuneables. If any of these is broken, it is a release-blocker bug,
not a configuration problem.

1. **Fallback-to-S3 is unconditional.** If any Shelf component
   (`shelfd`, `shelf-result-cache`, `shelf-advisor`, snapshot-watcher,
   Raft quorum, Arrow Flight server, HTTP server) is down, slow,
   unreachable, mid-restart, draining, or returning any error, the
   Trino query **must still succeed** using the default S3 endpoint.
   The user never sees a Shelf error code; they may see higher
   latency. This is implemented by the circuit-breaker state machine
   in § 9.5 and enforced by a mandatory chaos conformance test owned
   by agent 5 (see § 9.5 last paragraph).
2. **Shelf never mutates user data.** It is a read-through cache.
   All writes go straight to S3 via the engine's normal write path.
   (`shelf-advisor` in `auto-materialize` mode creates MV *definitions*
   under a scoped role — see § 7.6 — and is still bound by this
   invariant: it never writes data pages.)
3. **Content-addressed keys + snapshot-ID tagging are the only
   invalidation mechanism.** No TTLs on data keys. Iceberg snapshot
   changes invalidate by key design, not by timer.
4. **No Shelf-specific error ever reaches the engine.** Every
   Shelf-layer failure converts to either a direct-S3 read (on the
   data path) or a silent fall-back to non-accelerated planning (on
   the prefetch path).

---

## 2. Problem statement — our workload

Context this design must serve (condensed from our own history):

- 4 independent Trino 480 clusters (`replica-0..3`) on EKS, Iceberg on S3,
KEDA autoscaling with spot workers.
- Workers are ephemeral. **Node-local caches (`fs.cache`) sit at 15-20 % hit
rate** — pods die before the cache warms.
- Alluxio OSS 2.9.5 is the current shared cache. Operationally painful:
  - Proxy SDK v1 pool saturation (we just fixed this via `UfsIOManager=256`).
  - File-block granularity; a 512 MB Parquet file caches all 512 MB even if
  we only read one row group.
  - Metadata sync failures on Iceberg subpaths, benign but noisy.
  - `TempBlockMeta not found` races in zero-copy gRPC reader.
  - Redirect mode is EE-only, so reads fan through the proxy.
- 3 catalogs share the same buckets (`cdp`, `bronze`, `cdp_curated` on
`pw-data-cdp-prod-gold-layer` and `pw-data-cdp-prod-silver-layer`).
- `cdp.trino_logs.trino_queries` is a goldmine: every query's SQL, plan,
scanned bytes, user, wall time, and partitions — historical workload data
that nobody else has and that a cache can learn from.
- Hot dashboards (Metabase / Starburst) are ~80 % of queries and hit a small
set of tables repeatedly. Ad-hoc queries (notebooks) are long-tail and not
worth caching aggressively.
- We have the whole Iceberg metadata tree (manifest list → manifests →
data files) visible at plan time.

**Therefore:** the ideal cache for us is one that (a) knows which row groups
matter before we read them, (b) dedupes across replicas, (c) survives spot
churn, and (d) learns from history.

---

## 3. Landscape — what's out there, and why none of it is enough


| System                                | Granularity                                 | Plan-aware             | Learned         | Open-source           | Runs across engines          | Verdict                                            |
| ------------------------------------- | ------------------------------------------- | ---------------------- | --------------- | --------------------- | ---------------------------- | -------------------------------------------------- |
| Trino `fs.cache` (Alluxio lib, local) | File block                                  | No                     | No              | Yes                   | No (Trino-only, per catalog) | Dies with spot workers                             |
| Alluxio OSS 2.x                       | File block                                  | No                     | No              | Yes (Apache 2.0)      | Yes                          | Heavy JVM, proxy bottleneck, no columnar awareness |
| Alluxio 3.x DORA                      | File block                                  | No                     | No              | Community ed. limited | Yes                          | Better topology, still file-block, still JVM       |
| Dremio C3                             | Block-level NVMe                            | Partial (Dremio plans) | No (heuristic)  | No                    | No                           | Dremio-only                                        |
| Starburst Warp Speed (Varada)         | Columnar block + bitmap / dict / tree index | Partial                | Heuristic       | No                    | No                           | Starburst-only, closed                             |
| Databricks Photon Disk Cache          | Parquet file + stats                        | No (auto)              | No (heuristic)  | No                    | No                           | Databricks-only                                    |
| Snowflake SSD cache                   | Opaque                                      | No                     | No              | No                    | No                           | Snowflake-only                                     |
| Apache CacheLib (Meta)                | Object                                      | N/A                    | No (FIFO / LRU) | Yes (Apache 2.0)      | N/A (library)                | Building block — not a system                      |
| Foyer (Rust port of CacheLib)         | Object                                      | N/A                    | No              | Yes (Apache 2.0)      | N/A (library)                | Building block — excellent for DRAM+NVMe           |
| JuiceFS                               | Chunked FS                                  | No                     | No              | Yes                   | Yes                          | Filesystem, not analytical cache                   |


Nobody combines: **columnar granularity × plan awareness × learned admission ×
open-source**. That's the gap Shelf fills.

---

## 4. Research foundation

Papers that directly inform the design. Cited with the exact contribution we use.

### 4.1 Eviction

- **SIEVE (NSDI '24, Zhang et al.)** — beats ARC by up to **63.2 %** lower
miss ratio; ≤ 20 LoC to implement; **O(1) lock-free hit path** using a
visited-bit + roving hand. Used as default eviction in Shelf's DRAM tier.
Already adopted in 5+ production libs.
- **GL-Cache (FAST '23, Yang et al.)** — group-level learned eviction,
**228× throughput vs LRB** and +7 % hit rate. Good for the NVMe tier where
per-object ML is too expensive; we cluster objects by (table, column,
partition_depth, size_bucket) and learn group weights from `trino_logs`.
- **FrozenHot (EuroSys '23, Qiu et al.)** — partition the cache into a
"frozen" hot set + dynamic area. Frozen keys skip the lock. **Up to 5.5×
throughput** on skewed workloads. We use this on DRAM for Iceberg
manifests and Parquet footers — they're hotter than anything else and
never change (immutable files).

### 4.2 Prefetch and admission

- **PACMan (NSDI '12, Ananthanarayanan et al.)** — coordinated cache
admission beats per-node greedy admission for parallel jobs. We apply the
coordinator-coordinated variant.
- **LRB — Learning Relaxed Belady (NSDI '20, Song et al.)** — learn the
next-access-time distribution and evict the item least likely to be used
within a time horizon. We use LRB-style features (frequency, recency,
size) at the admission step for large scans, not eviction (too expensive).

### 4.3 Storage engine

- **CacheLib (SOSP '20, Berg et al.)** — battle-tested DRAM+NVMe hybrid,
powers 70+ Meta services. Apache 2.0. We use the Rust port **Foyer**
(RisingWave Labs) for easier deployment and Rust ecosystem fit.

### 4.4 Distributed topology

- **Alluxio DORA (2023 blog + code)** — consistent-hash ring over workers,
clients talk directly to the owning worker in 1 hop. We borrow this idea
but ship it in Rust + Raft for the ring membership.
- **Ceph CRUSH** — deterministic placement under heterogeneous capacity.
Inspiration for our capacity-weighted ring.

### 4.5 Columnar-aware caching

- **Parquet Page Index (Iceberg PR #6935, #6967)** — read only needed pages
by inspecting column-index metadata. Shelf caches page-index blocks
specifically so every engine benefits from them instantly on subsequent
reads.

---

## 5. Non-negotiable design principles

1. **Caching must be decoupled from compute.** Spot/KEDA churn has proven that
  node-local caches don't work for us. Shelf runs as its own StatefulSet on
    on-demand nodes with NVMe.
2. **Granularity must match how Trino actually reads.** That means caching at
  four levels: Iceberg metadata JSON / manifest list / manifests / row-group
    byte ranges. Never cache a whole Parquet file just to read 2 row groups.
3. **The engine pushes intent; the cache acts on it.** The coordinator knows
  which files will be read before the workers do. That signal is free and
    nobody else uses it.
4. **Immutable by construction.** Iceberg data files never change; cache keys
  are content-addressed (`sha256(etag + byte_range)`). No invalidation
    required, ever. TTL only for garbage collection of deleted files.
5. **Open first, and genuinely multi-engine.** The wire protocol must be an
  open standard (Arrow Flight / S3 API emulation) so Spark, DuckDB, Ray, and
    Python can reuse the cache. No Trino-specific lock-in in the cache plane.
6. **Simpler to operate than what it replaces.** If Shelf takes more operator
  time than Alluxio 2.x, it's a failed project. One binary, one dashboard,
    one runbook.
7. **Degrade transparently.** Any cache miss, error, timeout, or partition
  MUST fall through to direct S3 without Trino noticing. Pool saturation
    must never block a query.

---

## 6. Architecture

```
      ┌───────────────────── Trino coordinator ─────────────────────┐
      │                                                             │
      │  Planner  ──────────────►  PrefetchHintListener ──gRPC──┐   │
      │  (existing)                (new plugin, event listener) │   │
      │                                                         │   │
      │  Workers  ──read──►  ShelfFileSystem ──Arrow Flight──┐  │   │
      │           (TrinoFileSystem SPI plugin)               │  │   │
      └──────────────────────────────────────────────────────┼──┼───┘
                                                             │  │
                                     ┌───────────────────────┘  │
                                     │                          │
                                     ▼                          ▼
                      ┌──── Shelf cache plane (StatefulSet) ────┐
                      │                                         │
                      │  ┌──── cache-node-1 ────┐               │
                      │  │  control (Raft)      │  \            │
                      │  │  router (hashring)   │   ) gossip    │
                      │  │  data (Foyer: DRAM+NVMe)             │
                      │  │  prefetch worker     │               │
                      │  │  stats exporter      │               │
                      │  └──────────────────────┘               │
                      │      × N nodes (replicated peers)       │
                      │                                         │
                      └─────────────┬───────────────────────────┘
                                    │
                                    ▼
                                  ┌─────┐
                                  │ S3  │  (origin of truth)
                                  └─────┘

      ┌─── Shelf control-plane sidecar (non-critical) ─────────────┐
      │  trainer: nightly Flink/Spark job that reads                │
      │           cdp.trino_logs.trino_queries,                     │
      │           emits admission_model.onnx + pin_list.json        │
      │                                                             │
      │  ui: web dashboard + Prometheus + OpenTelemetry             │
      └────────────────────────────────────────────────────────────┘
```

### 6.1 Cache node (`shelfd`) internals

Single Rust binary. Each node runs:

- **Router** — consistent hash ring over object-key-hash. 2 000 virtual
nodes per physical node, capacity-weighted (NVMe size). Ring membership
stored in Raft; any node can route.
- **Storage** — `[foyer](https://github.com/foyer-rs/foyer)` hybrid cache
with **per-pool byte quotas** (inspired by Firebolt's engine-cache pools):
  - `pool.metadata` — Iceberg `metadata.json`, manifest lists, manifests.
  Always DRAM. Quota: 5 % of DRAM. FrozenHot (immutable, never evicted
  until file deletion).
  - `pool.footer` — Parquet footer + page index. Always DRAM. Quota:
  10 % of DRAM. FrozenHot.
  - `pool.rowgroup_hot` — DRAM row groups for dashboard tables.
  Quota: balance of DRAM. SIEVE eviction.
  - `pool.rowgroup` — NVMe row groups. Bulk of storage. GL-Cache-style
  group-level eviction.
  **Why separate pools:** a 50 GB ad-hoc scan must not be able to evict
  the 500 MB of hot Iceberg manifests that every dashboard needs. Firebolt
  validated this separation; we copy it.
- **Origin client** — AWS SDK v2 S3 client with pooled connections (gets
us past the SDK v1 pain we had with Alluxio). One connection pool per
S3 prefix to isolate noisy neighbours.
- **Prefetch worker** — pulls hints from an in-memory queue, fans out S3
GETs with byte-range headers for exactly the row groups the planner
asked for.
- **Admission policy** — by default SIEVE. On a miss, if the object is
larger than `admission.size_threshold` (e.g. 8 MB), consult the learned
admission model (ONNX file shipped by the trainer). Model output is
P(next_access_within_1h); cache only if ≥ 0.3.
- **Metrics** — Prometheus endpoint at `:9090/metrics`; per-tenant,
per-table, per-granularity counters.
- **Control RPC** — gRPC: `Read`, `ReadBatch`, `Prefetch`, `Evict`, `Pin`,
`Unpin`, `Stats`.
- **Data RPC** — Arrow Flight; client `DoGet` the byte range, server
streams zero-copy from DRAM/NVMe.

### 6.2 Client plugin (`shelf-trino-plugin`)

Two artifacts, both JARs loaded into Trino:

1. `**ShelfFileSystem`** — implements Trino's `TrinoFileSystem` SPI.
  Intercepts reads for configured S3 prefixes. Translates them to
   `(object_key, byte_range)`, picks protocol by size (HTTP for < 1 MB,
   Arrow Flight for ≥ 1 MB), and calls the Shelf node that owns the
   key via consistent-hash ring. Fail-open: every Shelf error becomes
   a transparent fall-through to S3, mediated by a per-node circuit
   breaker (see § 9.5).
2. `**ShelfPrefetchListener`** — implements `EventListener`. On
  `QueryCreatedEvent` it extracts referenced Iceberg tables, their
   predicates, and the current snapshot IDs from `QueryMetadata`
   (`plan` / `jsonPlan` + `tables`). It then reads the current
   `metadata.json` + manifest list (only a few KB) from Shelf's own
   metadata tier and fires a `Prefetch` RPC for **files and footers**
   — not row groups (see § 7.2 for why plan-time prefetch cannot know
   row-group byte ranges without re-implementing `IcebergSplitSource`).
   Row-group prefetch is triggered later by plugin-side observation of
   footer range-GETs on workers.

Trino config (one catalog):

```properties
# iceberg.properties
connector.name=iceberg
hive.metastore.uri=thrift://trino-prod-metastore.penpencil.co:9083
iceberg.catalog.type=hive_metastore

# enable Shelf
fs.shelf.enabled=true
shelf.endpoint=shelf.shelf.svc.cluster.local:9090
shelf.tenant=replica-2
# shelf.fallback.on-error is hard-wired to direct-s3 and is NOT a tuneable.
# It is exposed as a property only for observability (log line on startup).
# Any attempt to set it to anything other than "direct-s3" fails plugin init
# with a fatal error. See § 1 "Non-negotiable invariants" and § 9.5.
shelf.fallback.on-error=direct-s3
shelf.prefetch.enabled=true
shelf.granularity=row-group,footer,manifest
```

### 6.3 Control plane

- **Raft** (`openraft` crate) inside the cache pods themselves. 3- or
5-node quorum; stores ring membership, pinned-table list, tenant
quotas. No etcd required. Still supports `kubectl exec` admin.
- **Tenants** = Trino resource groups. Each tenant has an NVMe quota
(default: equal share) and a priority (default: equal).
- **Pin list** = operator-supplied list of tables / partitions that
must never be evicted. Written as `pin_list.json`, hot-reloaded.
- **Trainer** — separate Flink/Spark/Airflow job, runs nightly, reads
`cdp.trino_logs.trino_queries`, builds:
  1. Per-table access frequency (TF) and distinct-user count (TU)
  2. Per-(table, partition) access frequency, last 7 / 30 / 90 days
  3. Query-plan-to-row-group mapping for dashboard queries
  4. Trains a 3-layer MLP (10 features → P(reaccess_within_1h)) and
    exports as ONNX
- All training artifacts pushed to an S3 config bucket; cache nodes
reload on `SIGHUP` or every 15 min.

---

## 7. The three killer features

### 7.1 Columnar-range granularity

We never cache a whole Parquet file. We cache, at most:


| Level                        | Size (typical) | Lives in         | TTL                 | Populated by    |
| ---------------------------- | -------------- | ---------------- | ------------------- | --------------- |
| Iceberg `metadata.json`      | 10-50 KB       | DRAM (FrozenHot) | until deleted       | Prefetch + miss |
| Iceberg manifest list        | 5-20 KB        | DRAM (FrozenHot) | until deleted       | Prefetch + miss |
| Iceberg manifest (avro)      | 50 KB - 5 MB   | DRAM             | until deleted       | Prefetch + miss |
| Parquet footer (last 64 KB)  | 8-64 KB        | DRAM             | until deleted       | Prefetch + miss |
| Parquet page index           | 4-32 KB        | DRAM             | until deleted       | Prefetch + miss |
| Parquet row-group byte-range | 1-128 MB       | NVMe             | 7-30 d by admission | Prefetch + miss |


Impact: a user who filters `WHERE event_region = 'MP+CG'` on a 5 GB Parquet
file reads **one row group = ~32 MB**, not 5 GB. Our NVMe holds
150-200× more working set than a file-block cache.

Implementation note: the cache key for a row group is
`sha256(etag || rg_ordinal || offset || length)`. Iceberg gives us the
`etag` in the manifest; we never have to `HEAD` S3.

### 7.2 Plan-aware push prefetch

**Honest scoping first — verified against Trino 480 SPI.** At
`QueryCreatedEvent` time, `QueryMetadata` exposes `plan` / `jsonPlan`
(optimized logical plan), `tables`, and `routines`. It does **not**
expose split-level info: file paths, row-group byte ranges, and
offset/length pairs come out of `IcebergSplitSource.Scan#planFiles()`,
which reads manifests lazily during execution — only made async by
trinodb/trino#17631 precisely because it's expensive.

So plan-time prefetch cannot directly say "file X, row groups 2-5".
We design two staged mechanisms:

#### Phase 2a — file-level + metadata prefetch (ships first)

```
t=0      user clicks a Metabase dashboard
t=0      Trino analyzes query (QueryCreatedEvent fires)
t=0.01s  ShelfPrefetchListener receives:
           tables      = [cdp.icesheet.silver_offline_event_data_2026]
           predicates  = [event_region = 'MP+CG']
           snapshot_id = (from SnapshotWatcher / control plane)
t=0.02s  Listener reads the current metadata.json + manifest list
         from Shelf's own (already-cached) metadata tier, ≤ 20 KB.
t=0.05s  Emits Prefetch([
           ("cdp/.../metadata/00317.metadata.json", FULL),
           ("cdp/.../metadata/snap-xxx.avro",       FULL),
           ("cdp/.../data/00017.parquet",  FOOTER+PAGE_INDEX),
           ("cdp/.../data/00018.parquet",  FOOTER+PAGE_INDEX),
           ...                                                  # only files in matching partitions
         ])
t=0.2s   Shelf fans out the range-GETs to S3 in parallel.
t=0.3s   Trino workers start executing splits; every footer read
         is now a DRAM hit.
```

Win in Phase 2a: the chatty manifest + footer round trips (often
20-50 % of cold-start latency) disappear. Row-group prefetch is *not*
attempted here — doing so would re-implement `IcebergSplitSource`.

#### Phase 2b — row-group prefetch via observation, not replication

Row-group byte ranges come from two signals, neither of which asks us
to replicate split generation:

1. **Plugin-side observation.** `ShelfFileSystem` sits in the worker
  read path. When a worker issues a Parquet footer range-GET for
    file X, Shelf has the footer bytes on the same node — it can parse
    the page index + row-group statistics *on the spot*, correlate
    them against the predicate we captured from `QueryCreatedEvent`,
    and prefetch the likely matching row groups before the worker's
    next range-GET arrives. No new Trino-side hook needed; the
    plugin already observes every read.
2. `**SplitCompletedEvent` learning.** Each completed split reports
  its (file, byte ranges) to the coordinator. The nightly trainer
    aggregates this into `(table, snapshot_hash, partition_key,   predicate_sketch) → hot_row_group_ids`. For any future query
    matching the same sketch, Phase 2a's prefetch promotes from
    "fetch all footers" to "fetch footers + the N row groups
    historically likely to match".

#### Validation we will run BEFORE building Phase 2

Minimum viable verification (≤ 30 min of work):

```sql
-- What does QueryCreated-time plan actually contain?
-- Run on rep-0 where we're the only workload:
EXPLAIN (FORMAT JSON)
SELECT * FROM cdp.icesheet.silver_offline_event_data_2026
WHERE event_region = 'MP+CG' LIMIT 10;
-- Expected: TableScanNode with table=..., predicate=..., snapshot_id=...
-- Not expected: file paths, split offsets, row-group ordinals.
```

Then install a throwaway event listener that logs
`SplitCompletedEvent.getStatistics().getSplitsSpec()` / split byte
ranges for one table for one day. If that payload is rich enough,
Phase 2b-signal-2 is viable. If not, fall back to plugin-observation
only (Phase 2b-signal-1).

Neither of these requires us to replicate Iceberg split generation.
If both fail, Phase 2 still ships Phase 2a (file + footer prefetch),
which is by itself a meaningful improvement.

#### Edge cases

- Prefetch RPC slow or fails: the worker just does read-through. No
correctness impact, only latency.
- `LIMIT N` queries: over-prefetching footers is cheap (KB-scale);
row-group prefetches issued by Phase 2b are cancelled when the
coordinator fires `QueryCompletedEvent`.
- Per-tenant prefetch queue, bounded depth; scans > 10 GB get
half-rate.

### 7.3 Learned admission on large scans

SIEVE is great on hit-path, but it admits blindly. For ad-hoc
multi-gigabyte scans we want to *not* pollute the NVMe with bytes we'll
only read once.

On every miss larger than `admission.size_threshold`:

1. Build feature vector: `[table_tf_7d, table_tu_7d, partition_depth,
  user_type (dashboard/adhoc), size_MB, hour_of_day, recency_days,
   query_cost_rank, file_is_recent, file_is_on_pin_list]`.
2. Score via cached ONNX model (3-layer MLP). Realistic single-inference
  latency on a modern CPU with ONNX Runtime: **10-50 µs**, dominated
   by graph dispatch and input binding rather than the actual matmuls.
   This is fine: admission is on the cold-miss path (we're already
   about to do an S3 GET that takes 20-100 ms), not the hit path.
   If we ever needed µs-scale, hand-rolling the MLP in SIMD would get
   us there — explicitly out of scope for v1.
3. Admit iff `P(reaccess < 1h) > 0.3`.

Model trained on `cdp.trino_logs.trino_queries`, refreshed nightly.
Target: eliminate 80 % of ad-hoc-scan bytes from NVMe without losing
dashboard hit rate.

Fallback if model unavailable: size-based admission (refuse objects ≥
1 GB unless on pin list).

### 7.4 Approximate in-cache indexes (closing the Warp Speed gap)

Warp Speed / Varada win on selective point-lookup queries
(`WHERE user_id = 12345`) because they maintain bitmap/hash indexes
per column on SSD. Shelf does not build inverted indexes — but it
doesn't have to. Three cheap mechanisms close ~80 % of the gap without
a new index subsystem.

#### 7.4.1 Bring-your-own: Parquet bloom filters at write time

Parquet has supported bloom filters in the footer since v2.9; Trino
(400+) reads them for predicate pushdown. Most production tables don't
have them only because nobody set the write property. Workflow:

1. The trainer analyses `cdp.trino_logs.trino_queries` for
  `WHERE col = literal` patterns, computes per-column "equality
   selectivity × frequency × wall-time" and recommends the top-N
   columns per table.
2. Ops patches the dbt / airflow writer configs:
  ```sql
   ALTER TABLE cdp.bronze.page_open SET TBLPROPERTIES (
     'write.parquet.bloom-filter-enabled.column.user_id'    = 'true',
     'write.parquet.bloom-filter-enabled.column.event_id'   = 'true',
     'write.parquet.bloom-filter-fpp.column.user_id'        = '0.01'
   );
  ```
3. New data files carry bloom filters in their footer. Shelf already
  caches footers in the DRAM FrozenHot pool — so every row-group-skip
   decision becomes a DRAM hit.

Zero Shelf code. Expected effect: 40-60 % reduction in scanned bytes
for equality predicates on high-cardinality columns, measured against
`trino_logs`.

#### 7.4.2 Side-built bloom filters in `shelfd` (for tables we can't re-write)

For legacy / external tables we cannot re-write, `shelfd` builds its
own bloom filters lazily from observed reads:

- On admission of row group R for column C, `shelfd` samples values
and builds a compact bloom (~1 MB per 10 M rows × indexed column,
FPP = 0.01).
- Stored in DRAM alongside the footer, keyed by
`(file_etag, row_group_ordinal, column_name)`.
- Exposed via a `ShelfFilterService` gRPC:
`probe(table, column, value) → {row_group_ids_that_might_match}`.
- `ShelfFileSystem` (Trino plugin) intercepts the reader's
row-group-selection step, asks Shelf first: *"for this predicate,
which row groups can I skip?"* If Shelf answers "none match", the
range-GET is never issued.

Properties:

- **Fail-open:** if Shelf has no bloom for this (file, column), the
reader falls back to reading everything (status quo). No
correctness risk.
- **Additive:** if the Parquet footer already contains a bloom (from
7.4.1), Shelf defers to it and skips side-building.
- **Index admission policy:** columns are added to the side-index set
based on query-log evidence only. Low-signal columns are never
indexed, so memory spend is bounded.

This is genuinely novel — no open-source analytical cache does this.
Target paper: *"Cache-resident approximate indexes for columnar
lakehouses"*.

#### 7.4.3 Z-order and sort-order awareness

If a table is Z-ordered or sorted on column C, plain Parquet min/max
on C gives Warp-Speed-grade selectivity for free — we just need to
know it. On first touch of a table Shelf reads its Iceberg properties
(`write.distribution-mode`, sort-order spec) and tags the table in the
control plane as *selectivity-friendly on column C*. The prefetch
listener then:

- For `WHERE C = X` queries, prefetches only the 1-2 files whose
min/max range covers X (read straight from the manifest).
- For non-sorted columns, falls back to Phase 2a's "fetch all
footers" behaviour.

Pure metadata — no new index structures.

#### 7.4.4 What we don't build in v1

- True persistent bitmap indexes (Varada-style).
- Inverted indexes for text search.
- Bring-your-own-index via hidden Iceberg tables.

All three are feasible but they graduate Shelf from *cache* to *index
engine*, which is a different product. Deferred to v2.

#### Honest residual

Multi-predicate bitmap `AND`/`OR` queries across 5+ low-selectivity
filters, on tables with no bloom filters and no sort order — Warp
Speed still wins here by 2-3×. In our workload this is <5 % of queries
based on `trino_logs` pattern analysis. Acceptable loss.

---

### 7.5 MV-aware caching (closing the Firebolt gap)

Firebolt wins on pre-known aggregations because its aggregating
indexes materialise `SUM / COUNT / GROUP BY` results ahead of time.
Shelf doesn't compute aggregates — but by co-designing with Trino's
existing materialised-view support, we match or beat Firebolt on the
common case using only OSS components.

#### 7.5.1 Tier 1 — `shelf-result-cache` (already in § 13.5)

Literal-repeat queries on the same Iceberg snapshot return from a
snapshot-keyed Redis / sled-backed result cache in <5 ms. Covers
~60-70 % of Metabase / PBI / Superset dashboard traffic (same query,
same snapshot). No Trino cooperation needed; this sits in front.

#### 7.5.2 Tier 2 — Iceberg materialised views, accelerated by Shelf

Trino 468+ supports Iceberg materialised views with automatic rewrite:
a query against the base table is transparently routed to the MV when
the optimiser sees an equivalent aggregate. Shelf's role:

1. The trainer identifies repeat aggregation patterns from
  `trino_logs` (top-N by `frequency × avg_wall_time × stability_of_predicates`).
2. Ops reviews the shortlist; approved MVs are created:
  ```sql
    CREATE MATERIALIZED VIEW cdp.mart.dashboard_region_rev_1d
    AS SELECT region, DATE(event_ts) AS d, SUM(revenue) AS rev
       FROM cdp.silver.sales GROUP BY region, DATE(event_ts);
  ```
3. Trino's optimiser rewrites matching queries to read from the MV.
4. Shelf pins the MV's data files in the DRAM hot pool (they are
  small; an aggregate is orders of magnitude smaller than its base
    table) — so the MV read itself is <5 ms on any worker, including
    newly-scaled-up spot workers.

**Combined effect.** The MV eliminates the computation (Firebolt's
trick); Shelf eliminates the I/O tax on the MV (our trick). On a
`SUM(revenue) GROUP BY region` query that takes Firebolt ~80 ms via
aggregating index, this combo delivers ~10-20 ms.

#### 7.5.3 Tier 3 — Shelf as MV catalogue

Shelf's control plane tracks:

- MV → base-table dependency graph.
- Per-MV last-refresh snapshot ID.
- Per-MV hit rate (how often the optimiser routes to it) and bytes
served.

Unused MVs are flagged; over-hot ones are candidates for pinning /
replication. This is the MV-maintenance layer Firebolt hides inside
the engine — we expose it as a managed service, reviewable by ops.

#### 7.5.4 What we don't build in v1

- Speculative cube materialisation (pre-computing aggregates nobody
asked for). That's Firebolt's revenue model, not ours, and it's
expensive to run.
- True query-plan subplan caching (reusing cached `GROUP BY region`
for a `GROUP BY region WHERE year=2026` via predicate pushdown
over the cached aggregate). Research-grade; deferred.

#### Honest residual

Novel ad-hoc aggregations on cold cache pay a full-scan cost on the
first run. Acceptable — these are `airflow_user` / `dbt_user` queries,
not latency-sensitive.

---

## 8. API

### 8.1 Data-plane — hybrid HTTP + Arrow Flight

Arrow Flight is excellent for MB-to-GB payloads (6 GB/s single-stream
throughput, zero-copy into Arrow buffers). It is **wrong** for KB-scale
payloads: IPC schema transmission, `FlightDescriptor` framing, and
`RecordBatch` metadata cost thousands of ns even for near-empty batches
([apache/arrow benchmarks](https://github.com/apache/arrow)). At 8 KB
Parquet footers and 10 KB Iceberg `metadata.json`, framing is a
meaningful fraction of the payload.

So Shelf's data plane is **two protocols on the same endpoint**, branched
by payload size:


| Payload size                                               | Protocol                                                                  | Reason                                                                        |
| ---------------------------------------------------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| < 1 MB (manifests, footers, page indexes, tiny row groups) | HTTP/2 GET with `Range:` header, `Content-Type: application/octet-stream` | Sub-ms latency; no IPC framing overhead; reuses the S3-compat shim from § 8.3 |
| ≥ 1 MB (bulk row groups)                                   | Arrow Flight `DoGet`                                                      | Zero-copy, columnar, 6 GB/s throughput                                        |


The Trino plugin looks at `PrefetchTarget.length` before picking a
protocol. Client reuses the same connection pool for both; h2
multiplexing keeps this cheap.

```protobuf
// FlightDescriptor.cmd = serialized ShelfReadRequest
message ShelfReadRequest {
  string object_key = 1;          // e.g. "s3://pw-data-cdp-prod-gold-layer/silver/..."
  uint64 offset     = 2;
  uint64 length     = 3;
  string tenant     = 4;
  string query_id   = 5;          // for tracing + per-query accounting
  string etag_hint  = 6;          // optional; binds to immutable version
}
```

Flight returns one or more `RecordBatch` or raw bytes depending on payload
(row group vs manifest). Zero-copy memory mapping when possible.

### 8.2 Control-plane — gRPC

```protobuf
service Shelf {
  rpc Prefetch(PrefetchRequest) returns (PrefetchResponse);  // fire-and-forget ok
  rpc Pin     (PinRequest)      returns (PinResponse);
  rpc Evict   (EvictRequest)    returns (EvictResponse);
  rpc Stats   (StatsRequest)    returns (StatsResponse);
}

message PrefetchRequest {
  string query_id = 1;
  string tenant   = 2;
  repeated PrefetchTarget targets = 3;
  uint32 priority = 4;  // 0 = dashboard, 10 = bulk
}
message PrefetchTarget {
  string object_key = 1;
  uint64 offset     = 2;  // 0 = whole object
  uint64 length     = 3;
  Granularity granularity = 4;  // FOOTER | PAGE_INDEX | ROW_GROUP | MANIFEST | FULL
}
```

### 8.3 S3-compatible shim (optional, for non-Flight engines)

Any engine that speaks S3 (Spark, DuckDB, Polars, boto3) can point
`endpoint_url` at Shelf and transparently get cache. Shelf implements a
minimal subset: `GetObject` with `Range` header, `HeadObject`. Gives us
a free migration path for Python notebooks and Spark.

---

## 9. Operational story

### 9.1 Deployment

```yaml
# one StatefulSet, e.g. 5 pods, on-demand NVMe-backed nodes
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: shelf, namespace: shelf}
spec:
  serviceName: shelf
  replicas: 5
  template:
    spec:
      nodeSelector: {workload: shelf}
      containers:
      - name: shelfd
        image: shelf:0.1
        resources: {requests: {cpu: 8, memory: 48Gi}}
        volumeMounts:
        - {name: nvme, mountPath: /var/lib/shelf}
        ports:
        - {name: flight,    containerPort: 8815}
        - {name: control,   containerPort: 9090}
        - {name: prom,      containerPort: 9091}
        - {name: raft,      containerPort: 7000}
        livenessProbe:  {httpGet: {path: /healthz, port: 9090}}
        readinessProbe: {httpGet: {path: /readyz,  port: 9090}}
  volumeClaimTemplates:
  - metadata: {name: nvme}
    spec:
      storageClassName: ebs-gp3-wffc   # or local-nvme provisioner
      resources: {requests: {storage: 500Gi}}
```

Scaling is plain K8s replica scaling. Ring rebalances in seconds; missing
keys refetched from S3 transparently.

### 9.2 Comparison with our current Alluxio footprint


| Dimension         | Alluxio OSS 2.9.5 (now)                                 | Shelf (target)                                         |
| ----------------- | ------------------------------------------------------- | ------------------------------------------------------ |
| Container images  | master, worker, job-master, job-worker, proxy (5 kinds) | 1 (shelfd)                                             |
| JVMs to tune      | 3 heaps × 4 roles                                       | 0                                                      |
| CM / config files | 4 configmaps                                            | 1                                                      |
| Lines of YAML     | ~510 in alluxio-values.yaml                             | ~150 target                                            |
| External deps     | ZK / embedded Raft + S3                                 | embedded Raft + S3                                     |
| Pool timeouts     | pre-fix: 3 900 / 10 min                                 | design target: 0 by construction (SDK v2 pool + async) |


### 9.3 Runbook surface

- Health: `kubectl -n shelf get sts shelf`, Grafana "Shelf overview"
dashboard (provided).
- Hot-reload: `kubectl -n shelf exec shelf-0 -- shelfctl reload` (pin
list / admission model).
- Manual pin: `shelfctl pin cdp.icesheet.silver_offline_event_data_2026`.
- Eviction storm: `shelfctl stats --granularity=row-group` + ring
rebalance metrics.
- Evict a poisoned key: `shelfctl evict <key>` (content-addressed, so
re-fetch is authoritative).

### 9.4 Failure modes and how each degrades


| Failure                  | Behaviour                                                                                                                                                        |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Entire Shelf DOWN        | Plugin falls through to direct S3 via circuit breaker (see § 9.5). Queries slow, not failing.                                                                    |
| One Shelf pod gone       | Consistent-hash ring re-elects; in-flight reads to that pod retry once, then fall through to S3; missing keys re-fetched from S3 on first miss to the new owner. |
| Raft quorum loss         | Cache still serves reads (ring is in-process); no new prefetch hints accepted.                                                                                   |
| NVMe full                | Admission refuses new inserts; existing cache continues to serve.                                                                                                |
| Corrupt object           | Content-addressed key mismatch detected on read; object evicted + refetched.                                                                                     |
| Trino plugin unavailable | Shelf still usable via S3-shim for other engines.                                                                                                                |


### 9.5 `ShelfFileSystem` client resilience state machine

Spot churn is the common case, not the exception: a Shelf pod dying
mid-query while a Trino worker is mid-range-GET *will* happen daily.
The plugin must never surface an error to Trino for this — it must
gracefully fall through to S3 and keep the query running. The exact
semantics:

```text
For each read(range) issued by Trino's Iceberg reader:
  target = hash_ring.owner_for(key)
  if circuit_breaker[target].is_open():
      return s3.get_range(key)                 # short-circuit, don't even try

  try:
      return shelf_client.get(target, key, range, timeout=200ms)
  except (ConnectionClosed, Timeout, UnavailableHttp503):
      circuit_breaker[target].record_failure()
      if attempt == 1:
          # Ring may have re-elected; retry once on new owner.
          new_target = hash_ring.owner_for(key)  // recomputes post-failure
          if new_target != target:
              try:
                  return shelf_client.get(new_target, key, range, timeout=200ms)
              except (ConnectionClosed, Timeout, UnavailableHttp503):
                  circuit_breaker[new_target].record_failure()
      return s3.get_range(key)                   # always falls back, never raises
```

Circuit breaker per Shelf pod:

- **Closed** (normal): up to 5 consecutive failures before opening.
- **Open**: for the next 10 s, bypass Shelf entirely, go direct to S3
for any key hashing to this pod. No retries — prevents
thundering-herd reconnects while a pod is restarting or a spot node
is draining.
- **Half-open**: after 10 s, the next request is allowed through as a
probe. Success → closed. Failure → back to open, double the timer.

We will publish this state machine as a reference Java implementation
alongside the plugin; every user of `ShelfFileSystem` gets it for free.

**Invariant the plugin guarantees to Trino:** `ShelfFileSystem`
*never* returns a `Shelf-specific` error. Every Shelf failure becomes a
transparent S3 read. The only errors Trino sees are real S3 errors
(AccessDenied, NoSuchKey, genuine network partition to S3) — which it
already handles. Restated from § 1 because it is a
release-blocker if violated.

**Mandatory chaos conformance test (owned by agent 5 — plugin-builder).**
CI gates every plugin release on a test matrix that, for each scenario,
runs a fixed small workload (TPC-DS Q1-Q5 on a 1 GB dataset) and asserts
all queries succeed with only `direct-s3` errors or `success` status —
never a `shelf-specific` error:

| Scenario | Expected outcome |
|---|---|
| All `shelfd` pods killed mid-query | All queries succeed via S3 fallback; ≤ 3× latency regression |
| Raft quorum lost mid-query | All queries succeed; prefetch disabled silently |
| `shelf-result-cache` returns 500 | Queries bypass and go to Trino normally |
| Snapshot-watcher down | Prefetch uses last-known-good snapshot map or falls back to S3-only; queries succeed |
| Network partition between Trino and all Shelf pods | Every read goes to S3; circuit breaker opens across all keys within 5 s |
| Half of Shelf pods in `Terminating` state (rolling restart) | Hit rate drops; no query fails |
| `shelf-advisor` JDBC credential revoked mid-run | Advisor logs error and exits; no impact on queries |

This test suite lives under `chaos/plugin-conformance/` in the repo
and runs on every plugin PR. A release blocks if any scenario
produces a `shelf-*` error surfaced to Trino.

---

## 10. Benchmarks we will publish

To be credible as OSS, we publish reproducible benchmarks.

1. **TPC-DS @ 1 TB** on Iceberg, 3 node Trino + 3 node Shelf, vs Trino +
  Alluxio 3 DORA vs Trino + fs.cache vs Trino + raw S3.
  - Metrics: p50, p95, p99 query latency, $/query, cache hit rate,
  warm-up time to 80 % hit rate.
2. **Cold-start benchmark**: run the same 20 dashboard queries after
  scaling Trino from 2 → 20 workers. Measure time-to-first-query.
    Expected: Shelf ≈ 1-2 s, fs.cache ≈ 15-40 s, raw S3 ≈ 8-15 s.
3. **Spot-churn benchmark**: kill 50 % of Trino workers every 5 min
  for 1 h, run steady dashboard load. Measure hit-rate degradation.
    Expected: Shelf hit rate stays ≥ 75 %, fs.cache drops to ~20 %.
4. **Real replay**: replay last 7 days of `cdp.trino_logs.trino_queries`
  for replica-2 against each system. This is the authoritative
    benchmark for our shop.

All results + traces published under `benchmarks/` in the repo.

---

## 11. Open-source strategy

1. **License**: Apache 2.0. CLA required for contributions (so we can
  donate to a foundation later).
2. **Repo layout** (monorepo):
  ```
    shelf/
      shelfd/                Rust cache-node binary
      shelfctl/              Rust CLI
      clients/trino/         Java plugin (FileSystem + EventListener)
      clients/spark/         Spark datasource shim
      clients/python/        PyArrow + boto3 wrapper
      protos/                Proto + Flight schemas
      charts/                Helm charts
      benchmarks/            Reproducible benchmark harness
      docs/                  MkDocs site
      .github/               CI, release, codeowners
  ```
3. **Launch plan**:
  - Phase 0: internal canary on replica-2 for 4-6 weeks.
  - Phase 1: public repo, blog post "Why we replaced Alluxio: a
  smart, open-source, Iceberg-native cache for Trino".
  - Phase 2: submit a TIP (Trino Improvement Proposal) to upstream
  the plugin artifact into `trino-fs-shelf/`.
  - Phase 3: propose as Apache Incubator project or donate to LF AI & Data.
4. **Governance**: BDFL for 12 months, then move to PMC model after
  10+ external contributors.
5. **Community onramps**: `good-first-issue` pool, weekly office
  hours, public Discord / Slack.
6. **Competitive honesty in docs**: we compare ourselves to Alluxio,
  C3, Warp Speed with numbers, not marketing. That earns trust.

---

## 12. Roadmap


| Phase                                                    | Window     | Scope                                                                                                                                                                                                                                   | Success gate                                                                                                                                                       |
| -------------------------------------------------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **−1 — Stabilise with existing tools** (no new services) | 1 week     | Replace `emptyDir` → `hostPath` for all Trino `fs.cache` volumes on rep-1/2/3. Ship `iceberg.metadata-cache.enabled=false` + `hive.metastore-cache-ttl=10m` audit across replicas. Already-running Alluxio stays.                       | Worker cache survives pod restart. `fs.cache` hit rate climbs from 15-20 % to ≥ 45 %.                                                                              |
| **0 — Proof of concept**                                 | 2 weeks    | Rust `shelfd` with DRAM-only Foyer cache, file-granularity, no plan-hint. Trino plugin that does S3 read-through via Shelf.                                                                                                             | A single query against `cdp.icesheet.silver_offline_event_data_2026` served from cache on rep-0.                                                                   |
| **1 — Columnar granularity**                             | 3 weeks    | Parquet footer + row-group ranges. Content-addressed keys. NVMe tier via Foyer hybrid.                                                                                                                                                  | TPC-DS Q1-Q5 beats Alluxio OSS 2.9 on p95.                                                                                                                         |
| **2 — Plan-aware prefetch**                              | 2 weeks    | `ShelfPrefetchListener` + prefetch queue + cancellation on query finish.                                                                                                                                                                | Cold-start benchmark: TTFQ ≤ 3 s after 10× scale-up.                                                                                                               |
| **3 — Consistent-hash ring + Raft**                      | 3 weeks    | Multi-node, ring rebalance, pod replace. S3-compat shim.                                                                                                                                                                                | Chaos test: kill 1 pod / 5 min, hit rate stable.                                                                                                                   |
| **4 — Learned admission**                                | 3 weeks    | Nightly trainer on `trino_logs`, ONNX inference in `shelfd`, admission decision on large scans.                                                                                                                                         | NVMe-byte admission rate cut by 60 % without losing dashboard hit rate.                                                                                            |
| **5 — Productionise rep-2**                              | 2 weeks    | Grafana dashboards, runbook, oncall handoff, HA config, capacity plan.                                                                                                                                                                  | 7 days zero-incident on rep-2; cut `alluxio-worker` headcount.                                                                                                     |
| **6 — Roll to rep-0/1/3**                                | 3 weeks    | Multi-tenant (one Shelf cluster shared across replicas).                                                                                                                                                                                | All 4 replicas on Shelf, Alluxio retired.                                                                                                                          |
| **7 — Open-source launch**                               | 2 weeks    | Public repo, docs, blog, benchmarks.                                                                                                                                                                                                    | HN front page / repo starred / first external PR merged.                                                                                                           |
| **8 — Approximate in-cache indexes** (§ 7.4)             | 4 weeks    | Parquet bloom-filter recommender (trainer side) + ops playbook for writer configs. Side-built bloom filters in `shelfd` with `ShelfFilterService` gRPC. Z-order / sort-order detection in the control plane.                            | Selective-equality benchmark: scanned-bytes cut ≥ 60 % on top 20 `WHERE col = literal` queries vs phase 7 baseline.                                                |
| **9 — MV-aware caching** (§ 7.5)                         | 3 weeks    | MV recommender in the trainer; Shelf pins MV files in the DRAM hot pool; control-plane tracks MV → base-table graph and per-MV hit rate.                                                                                                | Top 10 dashboard aggregations served from MV + Shelf in < 20 ms p95.                                                                                               |
| **10 — Incremental MV refresh on snapshot delta**        | 8-12 weeks | Snapshot-watcher emits `(table, snap_from → snap_to)` deltas; a new `shelf-mv-refresh` service reads only delta files, computes the incremental aggregate, and commits via Iceberg `MERGE`. Trino TIP drafted & upstreamed in parallel. | MV refresh wall-time drops from O(table) to O(delta): ≥ 20× speed-up on 1 TB fact-table MV with 100 MB daily delta. TIP accepted (or at least discussed) upstream. |


Total ≈ 37-41 weeks (≈ 9-10 months) to Phase 10. Production value
delivered from phase 2 onward; phases 8-10 are the "close the gap vs
Warp Speed / Firebolt" track and can run in parallel with phase 7 if
we staff them.

---

## 13. Risks & open questions


| Risk                                                                                                                   | Mitigation                                                                                                                                                                                                   |
| ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| "Yet another cache" fatigue in the community                                                                           | Lead with benchmarks and the plan-aware differentiator. Don't frame as Alluxio-replacement; frame as *analytic-engine-aware* cache.                                                                          |
| Trino `EventListener` currently runs only on coordinator; plan may not be rich enough to enumerate row groups up-front | Two-phase: phase 2a uses file-level prefetch from `QueryCreatedEvent`, phase 2b adds worker-side `SplitCompletedEvent` for row-group-level hints. Upstream PR #26425 already enables worker event listeners. |
| Learned model drift / staleness                                                                                        | Trainer reports feature-distribution drift; fall back to SIEVE-only below a confidence threshold. Nightly retrain + canary.                                                                                  |
| Ring rebalance thundering herd on S3                                                                                   | Rate-limit refetch per-prefix; exponential backoff; and the lost pod's hot keys are usually a minority of total traffic.                                                                                     |
| Iceberg schema evolution on cached data                                                                                | Content-addressed keys include ETag, so schema-evolution rewrites create new keys. Old keys TTL out naturally.                                                                                               |
| Operational maturity vs battle-tested Alluxio 3                                                                        | Phase 0-2 stay shadowed alongside Alluxio. Only cut Alluxio at phase 5.                                                                                                                                      |
| How do we handle credential-protected S3 (IAM roles, STS)?                                                             | Shelf uses IRSA or per-tenant role assumption; Trino pushes tenant identity, Shelf fetches with the tenant's role.                                                                                           |
| Cross-engine consistency (Spark writes while Trino reads)                                                              | Irrelevant for immutable Iceberg files; Iceberg commit path flips `metadata.json` atomically and new `metadata.json` is a new cache key.                                                                     |
| Do we want to cache result sets (query → result)?                                                                      | **No**, explicitly out of scope. Result caching belongs in the engine (Trino already has some); Shelf is a data-cache. Stay focused.                                                                         |


Open questions for the team:

1. Name: keep **Shelf**? Alternatives: Tundra, Reef, Ledge, Gale.
2. Home: standalone repo `github.com/penpencil-services/shelf`, or under
  `github.com/penpencil-oss/shelf`?
3. Launch post coauthors: who (data-platform + infra + eng-leadership)?
4. Do we want to donate to Apache from day 1, or self-govern for a year?
5. Do we aim for a Spark plugin in v1 or keep Trino-only for focus?

---

## 13.5 Snapshot-aware keys (stolen from the TrinoCache blueprint)

A peer blueprint (`TRINOCACHE_BLUEPRINT.md`) correctly identified that
**Iceberg snapshot IDs are the natural cache invalidation key**, not TTLs.
We adopt this in two places:

1. **Metadata keys.** Shelf already uses content-addressed keys
  (`sha256(etag + range)`). For Iceberg pointer files (`metadata.json`,
    manifest list) we additionally tag the key with the snapshot ID. When
    a table gets a new snapshot, the old key is dead by construction —
    no invalidation storm, no TTL staleness. Keys naturally evict via
    cold-LRU on the old snapshot ID.
2. **Result cache (separate companion binary — not part of `shelfd`).**
  Shelf ships an optional `shelf-result-cache` binary — deployed as
    either a Trino Gateway plugin or a thin HTTP proxy — that caches
    whole query results keyed by
    `sha256(normalized_sql + map_of_referenced_tables_to_snapshot_ids)`.
    It is explicitly **not bundled into `shelfd`** (see § 14): `shelfd`
    stays a pure byte-range data cache, `shelf-result-cache` is
    independently deployable and optional. They share the control
    plane (snapshot map, tenant config) but no data-path dependency.
  - Dashboard queries (pbi_online, mbuser, Metabase) return in
  < 5 ms without touching Trino at all.
  - When any referenced Iceberg table gets a new snapshot, the key
  changes and the result evicts itself.
  - Users like `airflow_user`, `dbt_user` are skipped (they write,
  and their queries are not repeated).
  - Results stored as Arrow IPC (zero-copy into Python clients).
  The result cache is **strictly complementary** to the data cache:
  data cache speeds up misses, result cache eliminates repeated queries.
  Together they compound.

A small companion service `snapshot-watcher` polls the Hive metastore
every 30 s, maintains a `(table → current snapshot_id)` map in Shelf's
control plane, and serves it to both the metadata cache and the result
cache. On a snapshot change, Shelf publishes an event internally so any
plan-hint prefetches referencing the old snapshot are cancelled.

This tier was NOT in Shelf v0.1. Adding it as a first-class component,
targeted for Phase 1.5 (between phase 1 and phase 2).

---

## 14. What Shelf is NOT

Listed because every ambitious OSS project dies from scope creep.

- `**shelfd` (the data plane) is not a result cache.** `shelfd` caches
file-system bytes — Iceberg manifests, Parquet footers, row-group
ranges. It never sees row-set result frames. `shelf-result-cache` is
a **separate companion binary** in the same repo that caches query
results keyed on snapshot IDs (§ 13.5); it *shares* Shelf's control
plane (snapshot map, tenant config) but is independently deployable
and optional. If you run `shelfd` alone, you get a data cache. If
you run only `shelf-result-cache`, you get a Redis-backed Trino
Gateway result cache. They compose but do not depend on each other.
- Shelf is not a filesystem. No `ls`, no `mv`, no POSIX.
- Shelf is not a metastore. HMS / Glue / Nessie still own that.
- Shelf is not an index. Warp-Speed-style columnar indexes are a
phase 8+ experiment, not v1.
- Shelf is not write-through. It is read-through only. All writes go
straight to S3, Iceberg commit semantics unchanged.
- Shelf is not a compute engine. No pushdown of filters inside Shelf
in v1. (Potential v2 if we ever want `EXPLAIN ANALYZE`-like
advantages.)

---

## 15. Why this actually works for us, specifically

We are not designing in a vacuum. Every design choice maps to a
measured pain we lived through in the last 90 days:


| Our pain                                                                   | Shelf feature that addresses it                                                                 |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| KEDA workers cold-start with empty `fs.cache` at 15-20 % hit rate          | Cache is decoupled from Trino workers                                                           |
| Alluxio proxy SDK v1 connection-pool saturation (~3 900 timeouts / 10 min) | AWS SDK v2 + pool-per-prefix + async + fallback to S3                                           |
| Alluxio `UfsIOManager` default 36 threads bottlenecks throughput           | Rust async runtime, no fixed IO pool; concurrency = node CPU × N                                |
| Alluxio caches whole 512 MB files to serve 1 row group                     | Row-group-level caching                                                                         |
| `TempBlockMeta not found` races in zero-copy gRPC reader                   | Content-addressed keys + single-writer-per-key + Arrow Flight zero-copy                         |
| Metadata sync WARNs on Iceberg `_.metadata.json` subpaths                  | Iceberg-native cache: we cache manifests as known entities, not as a filesystem path to sync    |
| `hive.metastore-cache-ttl=0s` on 3 catalogs causing excessive HMS load     | Shelf caches the metastore table snapshot too (Iceberg `metadata.json` is the snapshot pointer) |
| 4 Trino replicas each warming the same files                               | Shared Shelf cache across replicas = 4× effective cache size                                    |
| Operator-heavy: 5 container types, 3 JVMs per worker to tune               | One Rust binary, one heap                                                                       |
| Alluxio pre-warm broke when live traffic ran simultaneously                | Plan-aware prefetch is first-class; admission knows about query priority                        |


---

## 15.5 What we borrow from Firebolt, Databricks, and others

Even though these are proprietary, their design choices inform Shelf:


| System                        | Design choice                                                            | Shelf's analogue                                                                                           |
| ----------------------------- | ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| Firebolt                      | Tablet storage on SSD with modified LRU persisting until engine shutdown | Foyer NVMe tier with SIEVE+GL-Cache eviction                                                               |
| Firebolt                      | RAM + SSD cache pools, separate eviction per pool                        | Per-pool byte quotas (§ 6.1)                                                                               |
| Firebolt                      | Sparse primary index per data block                                      | Cache Parquet page index + row-group stats aggressively; they ARE our sparse index                         |
| Firebolt                      | Aggregating indexes (pre-computed GROUP BYs, auto-synced)                | **Out of scope for v1.** Potential phase 8+ experiment: Iceberg materialized views + Shelf pre-compute.    |
| Firebolt                      | Warmup-engines API                                                       | Validates our plan-aware prefetch; directly inspired the `Pin`/`Prefetch` gRPC methods                     |
| Databricks Photon             | Auto-managed local NVMe, Parquet + stats cached                          | Similar to our NVMe tier but coupled to their runtime; we stay engine-agnostic                             |
| Snowflake                     | Result cache at virtual warehouse level                                  | We do this in `shelf-result-cache`, keyed on snapshot ID (correct invalidation, which Snowflake also does) |
| Starburst Warp Speed (Varada) | Block-level bitmap / dictionary / tree indexes on NVMe                   | **Phase 8+ possibility**: per-column indexes for predicate pre-evaluation. v1 sticks to byte-ranges.       |
| Dremio C3                     | NVMe local cache + Arrow-based zero copy                                 | Same tech choices (Foyer ≈ CacheLib, Arrow Flight)                                                         |


**The three things all of these systems independently converged on** and
that we therefore bake in as non-negotiable:

1. Hybrid DRAM+SSD cache with distinct eviction policies per tier.
2. Columnar/range granularity (not file-level).
3. A warm-up mechanism driven by either history or the planner.

---

## 16. Next step for the team

Minimum viable next step (≤ 1 day of work) to decide whether to proceed:

1. Spike: write the **ShelfFileSystem** Java class (~300 LOC) as a pure
  pass-through to S3. Measure overhead. If ≤ 5 % vs direct S3, we're
    good.
2. Spike: write the `**shelfd` skeleton** in Rust (`foyer` + Axum +
  Tonic), DRAM-only, single-node. Measure p99 read latency on a 1 MB
    object. Target ≤ 1 ms from DRAM.
3. Pick a name.
4. Open a private repo, import this blueprint as `docs/BLUEPRINT.md`,
  start tracking decisions in `docs/adr/`.

If the spike numbers hold, we move to phase 0.

---

*Last edited: 2026-04-23. Owner: @aamir. Status: draft, seeking review.*