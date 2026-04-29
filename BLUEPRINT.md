# Shelf — a smart, Iceberg-native cache for Trino

> Codename: **Shelf** (iceberg shelf + "on the shelf"). Happy to rename.
> Status: design blueprint v0.4 — last reviewed for 1.0.0-rc.4 (Apr 30, 2026); ACTIVE through v1.0.0. Meant to be iterated on with the team.
> License intent: **Apache 2.0**. Public from day one.
>
> **v0.4 changes** (on top of v0.3):
> - SplitCompletedEvent path removed — Trino PR #26436 (merged
>   2025-08-19) deleted `EventListener#splitCompleted`. Phase 2b now
>   relies on plugin-side observation plus `QueryCompletedEvent`
>   operator summaries.
> - `openraft` dropped; cluster membership comes from the K8s
>   headless service, and pin list + tenant quotas live in a
>   versioned S3-backed ConfigMap.
> - ONNX MLP admission dropped for v1; v1 ships size-threshold
>   admission + a pin list derived from `trino_logs`. LightGBM (not
>   ONNX) is an optional v1.x upgrade gated on a measured hit-rate gap.
> - Arrow Flight deferred to v1.x; v1 uses HTTP/2 range-GET for all
>   payload sizes.
> - `shelf-result-cache` dropped from v1 — the Redis + Trino-Gateway
>   result cache in `COMPARISON.md` Phase 0 owns result caching.
> - Phase 10 (incremental MV refresh) dropped entirely; it is a
>   compute service, not a cache (see ADR-0007).
> - Timeline re-estimated to ≈ 36-44 calendar weeks for a 3-person
>   team.
> - v0.5 gate added: Shelf must beat the stabilised Alluxio 2.9.5
>   baseline on rep-2 for 7 consecutive days before rollout widens.

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
| Size-threshold admission + pin list from `trino_logs`; SIEVE (DRAM) + S3-FIFO (NVMe) via Foyer built-ins                                  | Best-in-class hit rate, O(1) hit-path cost                                              |
| Rust cache plane; HTTP/2 range-GET for all payload sizes in v1; no embedded consensus                                                     | No JVM GC, simple operator surface, no IPC framing tax on small objects                 |
| Rendezvous (HRW) hashing over K8s headless service membership; pin list + tenant quotas in S3-backed ConfigMap                            | No ring data structure, no consensus, membership tracked by K8s itself                  |
| Shared across all 4 Trino replicas                                                                                                        | One warm cache instead of four cold ones                                                |
| Decoupled from compute (separate StatefulSet with NVMe)                                                                                   | Survives KEDA spot churn, unlike fs.cache                                               |
| Plugin is fail-open: every Shelf error becomes a transparent fall-through to S3                                                           | Trino never sees a Shelf-specific error, even during spot churn                         |
| MV-aware caching + incremental MV refresh on Iceberg snapshot delta (§ 7.5, Phase 10)                                                     | Matches Firebolt's aggregating indexes for dashboard queries using OSS components only  |


Target: **hit rate comparable to the stabilised Alluxio 2.9.5 baseline
(currently 71% on rep-2) at substantially lower operator surface; p50
scan latency within 20% of Alluxio on hit; fail-open to direct S3 on
miss or Shelf fault**. On the query patterns where commercial caches
traditionally win — selective equality predicates (Warp Speed) and
dashboard aggregations (Firebolt) — Shelf closes the gap via § 7.4
and § 7.5 rather than feature-matching with new index engines.

### Non-negotiable invariants

These hold on every build, every release, forever. They are not
tuneables. If any of these is broken, it is a release-blocker bug,
not a configuration problem.

1. **Fallback-to-S3 is unconditional.** If any Shelf v1 component
   (`shelfd`, `shelf-advisor`, `snapshot-watcher`, the HTTP/2 data
   plane, the K8s-headless-service membership path, or the S3-backed
   ConfigMap pin list) is down, slow, unreachable, mid-restart,
   draining, or returning any error, the Trino query **must still
   succeed** using the default S3 endpoint.
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
- Alluxio on rep-2 is now stable (2026-04-23, post `UfsIOManager=256`
  + 3-master HA migration) at ≈ 71% hit rate. Shelf's v0.5 must beat
  this baseline; see §12.
- 3 catalogs share the same buckets (`cdp`, `bronze`, `cdp_curated` on
`example-prod-gold-layer` and `example-prod-silver-layer`).
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

- **PACMan (NSDI '12, Ananthanarayanan et al.)** — coordinated
eviction and placement under an *all-or-nothing* parallel-job
objective (a parallel job is only as fast as its slowest task, so
evicting one input of a hot job is disproportionately costly). We
borrow this framing for cross-pod eviction coordination, not for
admission.
- **Pythia (EDBT '25)** — plan-aware prefetching for OLAP workloads,
validating that query plans are a strong signal for what to warm.
- **GrASP (preprint, 2025)** — graph-based access-sketch prefetch
for analytical caches; a modern counterpart to PACMan-style
coordination on lakehouse workloads.
- **LRB — Learning Relaxed Belady (NSDI '20, Song et al.)** — learn the
next-access-time distribution and evict the item least likely to be used
within a time horizon. We use LRB-style features (frequency, recency,
size) at the admission step for large scans, not eviction (too expensive).

### 4.3 Storage engine

- **CacheLib (OSDI '20, Berg et al.)** — battle-tested DRAM+NVMe hybrid,
powers 70+ Meta services. Apache 2.0. We use the Rust port **Foyer**
(RisingWave Labs) for easier deployment and Rust ecosystem fit. Foyer
ships S3-FIFO (SOSP '23) and SIEVE (NSDI '24) as pluggable policies;
we use them as-is rather than implementing custom policies.

### 4.4 Distributed topology

- **Alluxio DORA (2023 blog + code)** — consistent-hash ring over workers,
clients talk directly to the owning worker in 1 hop. We borrow the
one-hop idea but replace the ring + consensus stack with HRW (Rendezvous)
hashing over K8s headless-service membership: no ring data structure, no
embedded consensus, no Raft (see ADR-0001 / ADR-0002).
- **Ceph CRUSH** — deterministic placement under heterogeneous capacity.
Inspiration for our capacity-weighted ring.

DORA is not peer-reviewed; the underlying primitive is the well-worn
industrial pattern of consistent-hash / rendezvous (HRW) hashing +
capacity weighting (Dynamo, Cassandra, Riak, Alluxio DORA). We cite
it as convention, not as a novel contribution.

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
3. **The cache exploits whatever plan and observation signal the
  engine exposes, never blocks the engine waiting for any signal.**
    The coordinator knows which files will be read before the workers
    do; the plugin also observes every range-GET. Both signals are
    free; neither is on the query's critical path.
4. **Immutable by construction.** Iceberg data files never change; cache keys
  are content-addressed (`sha256(etag + byte_range)`). No invalidation
    required, ever. TTL only for garbage collection of deleted files.
5. **Wire protocol must be open enough that a non-Trino engine can
  adopt Shelf without Trino cooperation.** Shipping non-Trino clients
    in v1 is explicitly out of scope.
6. **Simpler to operate than what it replaces.** If Shelf takes more operator
  time than Alluxio 2.x, it's a failed project. One binary, one dashboard,
    one runbook.
7. **Degrade transparently.** Any cache miss, error, timeout, or partition
  MUST fall through to direct S3 without Trino noticing. Pool saturation
    must never block a query.
8. **Every RPC has a budget.** Every Shelf client call carries a hard
  timeout and falls open on expiry.
9. **No unbounded queue.** Prefetch queue and training batch queue
  both have explicit upper bounds and documented overflow behaviour.
10. **Every published metric has an SLO.**
11. **Every config key is reloadable at runtime OR documented as
    restart-required.**
12. **No new consensus systems without a failure case that demands
    them.**

---

## 6. Architecture

```
      ┌───────────────────── Trino coordinator ─────────────────────┐
      │                                                             │
      │  Planner  ──────────────►  PrefetchHintListener ──gRPC──┐   │
      │  (existing)                (new plugin, event listener) │   │
      │                                                         │   │
      │  Workers  ──read──►  ShelfFileSystem ──HTTP/2──────┐   │   │
      │           (TrinoFileSystem SPI plugin)             │   │   │
      └────────────────────────────────────────────────────┼───┼───┘
                                                           │   │
                                     ┌─────────────────────┘   │
                                     │                         │
                                     ▼                         ▼
                      ┌──── Shelf cache plane (StatefulSet) ───┐
                      │                                        │
                      │  ┌──── cache-node-1 ────┐              │
                      │  │  HTTP/2 data plane   │              │
                      │  │  router (HRW, DNS)   │              │
                      │  │  data (Foyer: DRAM+NVMe S3-FIFO)    │
                      │  │  prefetch worker     │              │
                      │  │  stats exporter      │              │
                      │  └──────────────────────┘              │
                      │   × N pods (stateless peers, K8s       │
                      │     headless-service membership)       │
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

- **Router** — Rendezvous (HRW) hashing over K8s headless-service
membership; capacity weights read from each pod's `/stats` endpoint;
no ring data structure, no vnode count.
- **Storage** — `[foyer](https://github.com/foyer-rs/foyer)` hybrid cache
with **per-pool byte quotas**. **Two pools for v1:**
  - `pool.metadata` — manifests + footers + page indexes, DRAM only,
    FrozenHot, 5 % of DRAM quota.
  - `pool.rowgroup` — DRAM+NVMe hybrid via Foyer, S3-FIFO (Foyer
    built-in). Note: separating `rowgroup_hot` is deferred to v1.1
    pending measurement.

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
larger than `admission.size_threshold` (default 1 GiB), refuse
admission unless the key matches a pin-list entry. A learned model
(LightGBM, not ONNX) is a v1.x upgrade path conditional on a
measured ≥ 5 pp hit-rate gap vs size-threshold alone; see §7.3.
- **Metrics** — Prometheus endpoint at `:9090/metrics`; per-tenant,
per-table, per-granularity counters.
- **Control RPC** — gRPC: `Read`, `ReadBatch`, `Prefetch`, `Evict`, `Pin`,
`Unpin`, `Stats`. `Evict` and `Pin` carry a per-tenant deadline and
bounded queue depth.
- **Data RPC** — HTTP/2 range-GET; client issues a ranged GET over a
pooled connection, server streams bytes from DRAM/NVMe. Arrow Flight
moved to v1.x.

### 6.2 Client plugin (`shelf-trino-plugin`)

Two artifacts, both JARs loaded into Trino:

1. `**ShelfFileSystem`** — implements Trino's `TrinoFileSystem` SPI.
  Intercepts reads for configured S3 prefixes. Translates them to
   `(object_key, byte_range)` and issues HTTP/2 range-GET over a
   pooled connection to the HRW-elected Shelf pod. Fail-open: every
   Shelf error becomes a transparent fall-through to S3, mediated by
   a per-node circuit breaker (see § 9.5).
2. `**ShelfPrefetchListener`** — implements `EventListener`. On
  `QueryCreatedEvent` it extracts referenced Iceberg tables, their
   predicates, and the current snapshot IDs from `QueryMetadata`
   (`plan` / `jsonPlan` + `tables`). It then reads the current
   `metadata.json` + manifest list (only a few KB) from Shelf's own
   metadata tier and fires a `Prefetch` RPC for **files and footers**
   — not row groups (see § 7.2 for why plan-time prefetch cannot know
   row-group byte ranges without re-implementing `IcebergSplitSource`).
   Row-group prefetch is triggered later by plugin-side observation of
   footer range-GETs on workers. The listener enforces a hard 10 ms
   coordinator-side deadline; it is fire-and-forget with a bounded
   queue — overflow drops the oldest pending hint rather than
   blocking the coordinator.

Trino config (one catalog):

```properties
# iceberg.properties
connector.name=iceberg
hive.metastore.uri=thrift://hms.example.internal:9083
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

- **No embedded consensus.** K8s headless-service provides membership.
Pin list + tenant quotas live in a versioned S3-backed ConfigMap,
pulled every 15 min and on SIGHUP. Trainer job writes the next
version; ops reviews diffs via PR before publication.
- **Tenants** = Trino resource groups. Each tenant has an NVMe quota
(default: equal share) and a priority (default: equal).
- **Pin list** = operator-supplied list of tables / partitions that
must never be evicted. Written as `pin_list.json`, hot-reloaded.
- **Trainer** — separate Flink/Spark/Airflow job, runs nightly, reads
`cdp.trino_logs.trino_queries`, builds:
  1. Per-table access frequency (TF) and distinct-user count (TU)
  2. Per-(table, partition) access frequency, last 7 / 30 / 90 days
  3. Query-plan-to-row-group mapping for dashboard queries
  4. Builds a pin list sorted by
    `scanned_bytes × wall_time × frequency`, top-N per tenant;
    merged via ops-reviewed PR. LightGBM model is a v1.x optional
    component, not shipped in v1.
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
file reads **one row group = ~32 MB**, not 5 GB. Cache density is
typically 5-20×, up to 100× on narrow predicates over wide tables;
measurement to be published from the 7-day `trino_logs` replay
(SHELF-26).

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

> **Note — Trino PR #26436 (merged 2025-08-19) removed
> `EventListener#splitCompleted`.** Operators are directed to
> `QueryStatistics#getOperatorSummaries` at `QueryCompletedEvent`
> time instead. Any earlier draft of this blueprint that cited PR
> #26425 (worker-side `SplitCompletedEvent`) is stale.

1. **Plugin-side observation (primary).** `ShelfFileSystem` sits in
  the worker read path. When a worker issues a Parquet footer
    range-GET for file X, Shelf has the footer bytes on the same
    node — it can parse the page index + row-group statistics *on
    the spot*, correlate them against the predicate we captured from
    `QueryCreatedEvent`, and prefetch the likely matching row groups
    before the worker's next range-GET arrives. No new Trino-side
    hook needed; the plugin already observes every read. **This is
    the row-group-level prefetch mechanism for v1.**
2. **Post-hoc learning via
  `QueryCompletedEvent.getStatistics().getOperatorSummaries()`** —
    coarser than split-level, but live on all shipped Trino. Trainer
    aggregates `(query_sketch → operator_summaries)` nightly and
    feeds the pin list / future admission model.

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
`QueryCompletedEvent.getStatistics().getOperatorSummaries()` for one
table for one day. This is the richest post-hoc signal available
after PR #26436 removed `splitCompleted`. If the operator-summary
payload is rich enough to identify hot files per
`(table, predicate_sketch)`, the Phase 2b post-hoc learner is viable.
If not, Phase 2 ships with plugin-side observation only.

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

**v1 — size-threshold admission + pin list.** Refuse admission for
any object larger than `admission.size_threshold` (default 1 GiB)
unless the key matches a pin-list entry. The pin list is built
nightly by the trainer from `cdp.trino_logs.trino_queries` — top-N
objects per tenant sorted by
`scanned_bytes × wall_time × frequency` — and published as a
versioned S3 ConfigMap, merged via ops-reviewed PR. No model
inference on the admission path in v1.

Target: eliminate the bulk of ad-hoc-scan bytes from NVMe without
losing dashboard hit rate, measured against the `trino_logs`
replay.

#### v1.x possible upgrade — LightGBM model

If after Phase 1 we measure a ≥ 5 pp hit-rate gap between
size-threshold admission and the pin-list augmented variant on
replay, we add a LightGBM model (tiny C runtime, no ORT dependency)
as an optional admission input:

1. Build feature vector: `[table_tf_7d, table_tu_7d, partition_depth,
  user_type (dashboard/adhoc), size_MB, hour_of_day, recency_days,
   query_cost_rank, file_is_recent, file_is_on_pin_list]`.
2. Score via LightGBM (tree model, ≤ 5 µs inference on a modern
  CPU). Admission is on the cold-miss path (we're already about
   to do an S3 GET that takes 20-100 ms), not the hit path.
3. Admit iff `P(reaccess < 1h) > 0.3`.

LightGBM is chosen over an ONNX-packed MLP to avoid the ORT
dependency and to stay in tree-model territory, which is a better
fit for the small tabular features we have.

### 7.4 Approximate in-cache indexes (closing the Warp Speed gap)

Warp Speed / Varada win on selective point-lookup queries
(`WHERE user_id = 12345`) because they maintain bitmap/hash indexes
per column on SSD. Shelf does not build inverted indexes — but it
doesn't have to. Three cheap mechanisms close ~80 % of the gap without
a new index subsystem.

#### 7.4.1 Bring-your-own: Parquet bloom filters at write time (ops playbook, no Shelf code in v1)

Parquet has supported bloom filters in the footer since v2.9; Trino
(400+) reads them for predicate pushdown. Most production tables don't
have them only because nobody set the write property. **This is an
ops-playbook item: no Shelf code ships for it in v1.** Workflow:

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

#### 7.4.2 Side-built bloom filters in `shelfd` — **Phase 8 only, not v1**

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

#### 7.4.3 Z-order and sort-order awareness — **Phase 8 only, not v1**

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

#### 7.5.1 Tier 1 — result cache (owned by COMPARISON.md Phase 0, not `shelfd`)

Literal-repeat queries on the same Iceberg snapshot return from a
snapshot-keyed Redis result cache sitting behind the Trino Gateway.
Covers ~60-70 % of Metabase / PBI / Superset dashboard traffic (same
query, same snapshot). **The v1 result cache is the Redis +
Trino-Gateway plugin documented in `COMPARISON.md` Phase 0 — it is
*not* built inside `shelfd`, and `shelf-result-cache` is not a v1
artefact.**

#### 7.5.2 Tier 2 — Iceberg materialised views, accelerated by Shelf

Shelf caches MV files like any other Iceberg file. Explicit MV
pinning in the DRAM hot pool is a **Phase 9** item, not core v1
behaviour — the description below is the Phase 9 target state.

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

#### 7.5.3 Tier 3 — Shelf as MV catalogue — **Phase 9, not v1**

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
first run. Acceptable — these are ETL writer queries,
not latency-sensitive.

---

## 8. API

### 8.1 Data-plane — HTTP/2 range-GET (v1)

**v1 uses HTTP/2 range-GET for all payload sizes.** The original
DaMoN '22 Arrow Flight throughput number (6 GB/s single-stream) was
measured on Mellanox InfiniBand; commodity EKS ENIs cap at
10-25 Gbps per node with per-stream throughput of 1-3 GB/s, so the
Flight advantage is much smaller than the headline suggests. Given
that, v1 runs a single protocol — HTTP/2 with `Range:` header — for
manifests, footers, page indexes, and row groups alike. One
connection pool, h2 multiplexing, no IPC framing to decide on per
payload.

**Arrow Flight is a v1.x upgrade** contingent on a measured ≥ 20 %
throughput gain over HTTP/2 at our per-stream realistic bandwidth.

```protobuf
// Reserved for v1.x Flight use. Not wired up in v1.
// FlightDescriptor.cmd = serialized ShelfReadRequest
message ShelfReadRequest {
  string object_key = 1;          // e.g. "s3://example-prod-gold-layer/silver/..."
  uint64 offset     = 2;
  uint64 length     = 3;
  string tenant     = 4;
  string query_id   = 5;          // for tracing + per-query accounting
  string etag_hint  = 6;          // optional; binds to immutable version
}
```

The proto definition is kept in the repo but marked "reserved for
v1.x"; no Flight server is shipped in v1.

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
`endpoint_url` at Shelf and transparently get cache. Shelf implements
**only `GetObject` with `Range` header and `HeadObject`** — no
`PutObject`, no `ListObjects`, no bucket management. Read-only,
scope deliberately minimal. Gives us a free migration path for
Python notebooks and Spark.

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
| External deps     | ZK / embedded Raft + S3                                 | K8s headless service + S3 (no embedded consensus)      |
| Pool timeouts     | pre-fix: 3 900 / 10 min                                 | explicit failure modes documented: Tokio task starvation under NVMe write pressure; Foyer write back-pressure; per-prefix origin-pool saturation. Mitigations itemised in §9.4. |


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
| One Shelf pod gone       | HRW re-election off the next DNS refresh; in-flight reads to that pod retry once, then fall through to S3; missing keys re-fetched from S3 on first miss to the new owner. |
| K8s headless-service DNS stale | Plugin sees the stale set for up to `shelf.membership.dns_ttl` (default 15 s); mis-routed requests are absorbed by the receiving pod with an HRW re-hash + one hop, or fall through to S3 on breaker open. |
| NVMe full                | Admission refuses new inserts; existing cache continues to serve.                                                                                                |
| Corrupt object           | Content-addressed key mismatch detected on read; object evicted + refetched.                                                                                     |
| Trino plugin unavailable | Shelf still usable via S3-shim for other engines.                                                                                                                |
| All Shelf pods down simultaneously (cluster network partition or KEDA mass-rotation) | Plugin falls through to direct S3; per-prefix rate limiter on the fallback path caps S3 GET rate at 5 000 / s / prefix to avoid `SlowDown` responses. |

> **Per-prefix rate limiting on the fallback path is mandatory, not
> optional.** A mass fall-through from every Trino worker to S3 with
> no throttle is the fastest path to provoking `SlowDown` 503s and
> amplifying the incident.


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

This state machine ships as a **committed Java reference
implementation + unit tests in v0.1 (SHELF-11)**, not as
blueprint-only pseudocode. Every user of `ShelfFileSystem` gets it
for free.

**Membership is re-read on retry.** Inside the retry path,
`hash_ring.owner_for(key)` must recompute against the *current*
DNS-refreshed K8s headless-service membership — never the snapshot
cached at the start of the request. If a pod died between the
initial call and the retry, the new owner comes from the live DNS
answer, not stale state in the client.

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
| K8s headless-service DNS record empty (zero Shelf pods) | Plugin circuit-breaker opens across every pod id within 5 s; all reads flow to S3 |
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


| Phase | Window | Scope | Success gate |
|-------|--------|-------|--------------|
| −1 | 1 w | Stabilise existing stack | fs.cache ≥ 45% for 5 days |
| 0 | 2-3 w | v0.1 single-pod DRAM PoC; shadow traffic | Plugin overhead ≤ 5% on rep-2 canary |
| 0R | 2-3 w (parallel) | Redis + Gateway result cache | ≥ 60% BI hit rate for 5 days |
| 1 | 6-8 w | v0.5 row-group + NVMe + HRW + 3-pod StatefulSet | Beat Alluxio on rep-2 for 7 consecutive days |
| 2 | 4-5 w | Plan-aware prefetch (2a + 2b-signal-1) | TTFQ ≤ 3 s p95 after 10× scale-up |
| 3 | 3-4 w | Scale to 5-7 pods; per-prefix S3 limiter; tokio hardening | Chaos drills pass; no per-prefix throttles in cold-replay |
| 4 | 2-6 w | Pin list live; LightGBM evaluated (only shipped if ≥ 5 pp) | NVMe write bytes cut ≥ 40% |
| 5 | 3-4 w | Productionise rep-2; retire rep-2 Alluxio | 7 days zero pages on rep-2; Alluxio pods = 0 |
| 6 | 4-6 w | Roll to rep-0/1/3 | Alluxio retired across all 4 replicas |
| 7 | 3-4 w | OSS launch | Public repo + blog + benchmark + first external PR response in 48 h |
| 8 | 4-6 w (parallel w/ 7 if team ≥ 4) | Approximate in-cache indexes | Selective-equality scanned bytes cut ≥ 60% |
| 9 | 3 w (parallel w/ 7 if team ≥ 4) | MV-aware pinning | Top 10 dashboard aggregates < 20 ms p95 |
| **10** | **—** | **REMOVED** — incremental MV refresh is a compute service, not a cache (see ADR-0007). | — |


Total ≈ 36-44 calendar weeks to OSS launch for a 3-person team.
Phases 8 and 9 parallelise with Phase 7 only if team expands to 4+.

---

## 13. Risks & open questions


| Risk                                                                                                                   | Mitigation                                                                                                                                                                                                   |
| ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| "Yet another cache" fatigue in the community                                                                           | Lead with numerically-measured comparison against the stabilised Alluxio 2.9.5 baseline on our real workload. Do not frame Shelf as an Alluxio-replacement in marketing; frame as an analytic-engine-aware cache with row-group granularity. |
| Trino `EventListener` currently runs only on coordinator; plan may not be rich enough to enumerate row groups up-front | Trino PR #26436 (merged 2025-08-19) **removed** `EventListener#splitCompleted` entirely. Phase 2b redesigned to plugin-observation + `QueryCompletedEvent` operator summaries. Do **not** cite PR #26425 as if its direction prevailed. |
| Learned model drift / staleness                                                                                        | Trainer reports feature-distribution drift; fall back to SIEVE-only below a confidence threshold. Nightly retrain + canary.                                                                                  |
| Ring rebalance thundering herd on S3                                                                                   | Rate-limit refetch per-prefix; exponential backoff; and the lost pod's hot keys are usually a minority of total traffic.                                                                                     |
| Iceberg schema evolution on cached data                                                                                | Content-addressed keys include ETag, so schema-evolution rewrites create new keys. Old keys TTL out naturally.                                                                                               |
| Operational maturity vs battle-tested Alluxio 3                                                                        | Phase 0-2 stay shadowed alongside Alluxio. Only cut Alluxio at phase 5.                                                                                                                                      |
| How do we handle credential-protected S3 (IAM roles, STS)?                                                             | Shelf uses IRSA or per-tenant role assumption; Trino pushes tenant identity, Shelf fetches with the tenant's role.                                                                                           |
| Cross-engine consistency (Spark writes while Trino reads)                                                              | Irrelevant for immutable Iceberg files; Iceberg commit path flips `metadata.json` atomically and new `metadata.json` is a new cache key.                                                                     |
| Do we want to cache result sets (query → result)?                                                                      | **No**, explicitly out of scope. Result caching belongs in the engine (Trino already has some); Shelf is a data-cache. Stay focused.                                                                         |


Open questions — resolved for v0.4 (per plan §8):

1. **Name.** TBD; "Shelf" stays as working codename until we ship
  v0.1. Alternatives (Tundra, Reef, Ledge, Gale) remain open.
2. **Repo home.** TBD — to be decided before Phase 7 (OSS launch).
3. **Launch-post co-authors.** TBD — lined up closer to Phase 7.
4. **Apache donation.** **Deferred 12 months.** Self-govern until
  we have ≥ 10 external contributors and a track record.
5. **Spark client.** **Not in v1.** Trino-only for focus; the S3
  shim gives Spark / DuckDB / boto3 a read-path today without a
  dedicated client.

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
2. **Result cache (v2+ — not a v1 artefact).** `shelf-result-cache`
  is **out of scope for v1**. The v1 result-cache shipping vehicle
    is the Redis + Trino-Gateway plugin documented in
    `COMPARISON.md` Phase 0, which keys whole query results by
    `sha256(normalized_sql + referenced_tables → snapshot_ids)` and
    sits in front of Trino. The discussion below of an independent
    `shelf-result-cache` binary is retained as **v2+ design notes**
    only; no such binary ships with v1.
  - Dashboard queries (pbi_online, mbuser, Metabase) return in
  < 5 ms without touching Trino at all.
  - When any referenced Iceberg table gets a new snapshot, the key
  changes and the result evicts itself.
  - ETL writer users (e.g. `airflow_user`, `dbt_user`) are skipped (they write,
  and their queries are not repeated).
  - Results stored as Arrow IPC (zero-copy into Python clients).
  The result cache is **strictly complementary** to the data cache:
  data cache speeds up misses, result cache eliminates repeated queries.
  Together they compound.

`snapshot-watcher` is a **COMPARISON.md Phase 0 deliverable** (not
net-new from Shelf) that polls the Hive metastore every 30 s and
maintains a `(table → current snapshot_id)` map. In v1 it is
consumed by the Redis-Gateway result cache; Shelf re-uses the same
signal for its metadata-tier keys. On a snapshot change, the Redis
cache invalidates naturally and Shelf cancels any plan-hint
prefetches referencing the old snapshot.

---

## 14. What Shelf is NOT

Listed because every ambitious OSS project dies from scope creep.

- **`shelfd` caches file-system bytes only.** Result caching in v1
is handled by the separate Redis + Trino-Gateway plugin documented
in `COMPARISON.md` Phase 0 — that is the v1 result-cache shipping
vehicle, and it is **not** in this repo. A future
`shelf-result-cache` binary is speculative, not a v1 artefact (see
§ 13.5, tagged v2+).
- Shelf is not a filesystem. No `ls`, no `mv`, no POSIX.
- Shelf is not a metastore. HMS / Glue / Nessie still own that.
- Shelf is not an index. Warp-Speed-style columnar indexes are a
**Phase 8** experiment, not v1. In-cache side-built blooms (§ 7.4.2)
are Phase 8+, not v1.
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
| `TempBlockMeta not found` races in zero-copy gRPC reader                   | Content-addressed keys + single-writer-per-key; reads served over HTTP/2 range-GET (zero-copy Arrow Flight is a v1.x option) |
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
| Firebolt                      | Tablet storage on SSD with modified LRU persisting until engine shutdown | Foyer NVMe tier with S3-FIFO eviction (SOSP '23)                                                           |
| Firebolt                      | RAM + SSD cache pools, separate eviction per pool                        | Per-pool byte quotas (§ 6.1)                                                                               |
| Firebolt                      | Sparse primary index per data block                                      | Cache Parquet page index + row-group stats aggressively; they ARE our sparse index                         |
| Firebolt                      | Warmup-engines API                                                       | Validates our plan-aware prefetch; directly inspired the `Pin`/`Prefetch` gRPC methods                     |
| Databricks Photon             | Auto-managed local NVMe, Parquet + stats cached                          | Similar to our NVMe tier but coupled to their runtime; we stay engine-agnostic                             |
| Snowflake                     | Result cache at virtual warehouse level                                  | Out of scope for v1; result caching lives in the Redis + Trino-Gateway plugin from COMPARISON.md Phase 0   |
| Dremio C3                     | NVMe local cache + Arrow-based zero copy                                 | Shared NVMe tier (Foyer ≈ CacheLib); data plane is HTTP/2 range-GET in v1, Arrow Flight reserved for v1.x  |


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
    object. Target: **p99 DRAM read 1-3 ms, p99.9 10-50 ms under
    NVMe pressure** (honest tail-latency expectation — p99.9 gets
    worse as the NVMe tier fills).
3. Pick a name.
4. Open a private repo, import this blueprint as `docs/BLUEPRINT.md`,
  start tracking decisions in `docs/adr/`.

If the spike numbers hold, we move to phase 0.

---

*Last edited: 2026-04-23. Version: v0.4. Owner: @aamir. Status: draft, seeking review.*