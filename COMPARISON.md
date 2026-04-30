# Shelf vs TrinoCache Stack — side-by-side

Two blueprints were drafted independently:

- `shelf/BLUEPRINT.md` — **Shelf**: a new open-source, Iceberg-native,
plan-aware, columnar-range cache. Larger scope, bigger bet.
- `~/Desktop/trino/TRINOCACHE_BLUEPRINT.md` — **TrinoCache Stack**: a
pragmatic 4-tier stack composing existing OSS components (Trino
Gateway + Redis + `fs.cache` + a custom Go S3 proxy).

They are **complementary, not competing**. Shelf is the long-term
platform; TrinoCache Stack is what ships in the next 2-3 weeks. This doc
reconciles them.

> **v0.2 note:** after review, the Shelf blueprint was tightened on five
> points: (1) plan-time prefetch is now correctly scoped to file+footer
> level (the `QueryCreatedEvent` does not expose row-group byte ranges;
> `IcebergSplitSource` generates those lazily during execution); (2)
> ONNX admission latency reset to a realistic 10-50 µs; (3) the data
> plane now branches HTTP vs Arrow Flight by payload size to avoid IPC
> framing overhead on small objects; (4) `shelf-result-cache` is now an
> explicit companion binary, not part of `shelfd`; (5) the Trino plugin
> has an explicit circuit-breaker + retry state machine for spot churn.
> None of these change the Shelf↔TrinoCache Stack reconciliation below.

---

## 1. Side-by-side


| Dimension                  | TrinoCache Stack (pragmatic)                                | Shelf (platform)                                                      |
| -------------------------- | ----------------------------------------------------------- | --------------------------------------------------------------------- |
| Time to first value        | 1 week (Phase 0)                                            | 5-6 weeks (Phase 2 — plan-aware prefetch)                             |
| Net-new services           | Redis, Go proxy, 3 Python sidecars                          | `shelfd` (Rust) + trainer + result-cache sidecar                      |
| Granularity                | File / range-GET                                            | Row group + footer + manifest + page index                            |
| Cache-miss behaviour       | Non-blocking (serve S3, warm async)                         | Non-blocking + coordinator pre-fetches before the miss happens        |
| Invalidation               | Iceberg snapshot ID in result-cache key                     | Content hash + snapshot-ID on metadata pointers; data files immutable |
| Admission policy           | All reads admitted; ETL users skipped at result-cache layer | Learned admission (ONNX) trained on `trino_logs`                      |
| Eviction                   | LRU                                                         | SIEVE (DRAM) + GL-Cache (NVMe) + FrozenHot (immutable metadata)       |
| Cross-replica sharing      | Result cache shared; block cache per-worker                 | Every tier shared across all 4 replicas                               |
| Language stack             | Python + Go + YAML glue                                     | Rust (`shelfd`) + Java plugin + Python trainer                        |
| Open-source posture        | Internal tools, not designed to be published                | Apache 2.0, public repo, TIP track into Trino                         |
| Competitive differentiator | None — same as Dune / stock Trino advice                    | Plan-aware prefetch + row-group granularity                           |
| Ops burden                 | Moderate (5 new deployables)                                | Moderate (1 binary + sidecar) once mature                             |
| Risk                       | Low — mostly config + small services                        | Medium — new Rust service, new protocol                               |


---

## 2. Merged roadmap

These phases replace both individual roadmaps. Every phase delivers
standalone value.

### Phase −1 — Stabilisation (Week 1, **zero new services**)

Lifted straight from the TrinoCache blueprint, because it's right.

1. `emptyDir` → `hostPath` for every Trino `fs.cache` volume on
  rep-1/2/3.
2. Audit `hive.metastore-cache-ttl=0s` catalogs → set to `10m`.
3. Verify `iceberg.metadata-cache.enabled=false` wherever
  `fs.cache.enabled=true` (they conflict).
4. Commit the already-landed Alluxio `UfsIOManager=256` patch to git
  (right now it exists only as a CM patch).
5. Move rep-2 KEDA cooldown MR to merge.

Expected: `fs.cache` hit rate 15-20 % → 45-55 %. No query regressions.

### Phase 0 — Quick-win result cache (Weeks 2-3)

Lifted from TrinoCache Tier 0.

1. Deploy Redis 7 cluster (`cache` ns, 3 primaries × 32 GB).
2. Write `SnapshotWatcher` (Python, 200 LoC) — polls Iceberg
  `metadata.json` via Trino system tables every 30 s, writes
    `snapshot_ids` hash to Redis.
3. Write `trino-gateway-result-cache` plugin — intercepts queries from
  `pbi_`*, `mbuser`, `commonuser`, builds snapshot-aware key, caches
    Arrow IPC-serialised results.
4. Enable for BI users only.

Expected: dashboard queries ≤ 5 ms on cache hit, 60-80 % hit rate on BI
traffic, instant removal from Trino CPU usage charts.

### Phase 1 — Shelf PoC (Weeks 4-6)

From Shelf Phase 0.

1. Rust `shelfd` skeleton, DRAM Foyer, file-granularity read-through.
2. Java `ShelfFileSystem` as a pass-through wrapper.
3. Canary on rep-0 for non-critical queries.

Expected: functional parity with Alluxio 2.9, measurable on cold-start
benchmark.

### Phase 2 — Columnar granularity + plan-aware prefetch (Weeks 7-10)

From Shelf Phase 1 + 2.

1. Row-group + footer + manifest granularity.
2. `ShelfPrefetchListener` on coordinator, feeding plan-time hints.
3. Per-pool quotas (manifest / footer / rowgroup_hot / rowgroup).
4. Content-addressed keys including snapshot ID.

Expected: TTFQ (time-to-first-query) after scale-up ≤ 3 s on rep-2.
Cold-start tax eliminated.

### Phase 3 — Multi-node Shelf + S3-shim (Weeks 11-13)

From Shelf Phase 3.

1. Consistent-hash ring, `openraft` membership.
2. NVMe tier via Foyer hybrid.
3. S3-compatible HTTP shim (so Spark/DuckDB/Python notebooks benefit).
4. Migrate rep-2 traffic from Alluxio → Shelf.

Expected: Alluxio retired from rep-2 path. 4-5× effective cache size vs
node-local `fs.cache` thanks to sharing.

### Phase 4 — Learned admission (Weeks 14-16)

From Shelf Phase 4.

1. Nightly trainer on `your_query_log_table`.
2. ONNX model, per-tenant features.
3. Admission gate on > 8 MB misses.

Expected: NVMe admission bytes cut 60 %; hot dashboards unaffected.

### Phase 5 — Merge tiers + migrate everyone (Weeks 17-19)

1. Result cache moves from Redis → Shelf's DRAM tier (so one storage
  layer, not two).
2. Retire Alluxio from all 4 replicas.
3. Retire Redis from cache critical path (keep for `SnapshotWatcher`
  if still needed).

### Phase 6 — Open-source launch (Weeks 20-22)

From Shelf Phase 7.

1. Public repo, docs, benchmarks, blog post.
2. Trino Improvement Proposal (TIP) for plugin upstream.

---

## 3. What got dropped

From TrinoCache blueprint:

- **PW-CacheProxy in Go** — dropped. Its purpose (non-blocking S3 with
NVMe cache) is exactly what `shelfd` does in Rust with Arrow Flight
zero-copy. Shipping both would create two implementations of the same
idea.
- **CacheAffinity Router in Gateway** — dropped. Trino's
`fs.cache.preferred-hosts-count=N` already gives consistent-hash
affinity at the worker level. Gateway-level routing adds complexity
without clear win once Shelf's shared cache is live.
- **Per-replica `fs.cache` as a permanent tier** — kept through Phase 3
only. Once Shelf is on all replicas with shared NVMe, node-local
`fs.cache` becomes redundant and can be turned off.

From Shelf blueprint v0.1:

- **Separate Redis for result cache** — eventually folded into Shelf's
DRAM tier (Phase 5). Redis stays as the quick-win Phase 0 shipping
vehicle.
- **Assumption that result cache was out of scope** — reversed. It's a
first-class component now, just lives inside Shelf.

---

## 4. Decision log


| Decision                                                   | Outcome                                                                                        |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Keep two blueprints or merge?                              | Merge into BLUEPRINT.md with Phase −1 and result-cache adopted; this doc keeps the comparison. |
| Ship quick-win result cache now (Redis) or wait for Shelf? | Ship now (Phase 0). Too much value to delay 4 months.                                          |
| Build PW-CacheProxy in Go as an interim?                   | No. Keep Alluxio on the `UfsIOManager=256` fix; go straight to Shelf from there.               |
| Include Iceberg snapshot IDs in cache keys?                | Yes, across metadata + result cache tiers. Acknowledged as adopted from TrinoCache blueprint.  |
| Per-pool byte quotas (Firebolt-style)?                     | Yes, required to prevent ad-hoc scans from evicting hot metadata.                              |


---

*Last updated: 2026-04-23. Read alongside `BLUEPRINT.md`.*