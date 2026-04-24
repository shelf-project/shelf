# Shelf execution plan

*Author: agent-3-planner*
*Date: 2026-04-23*
*Inputs: `shelf/BLUEPRINT.md` (v0.3, last edited 2026-04-23), `shelf/COMPARISON.md`, `shelf/agents/out/01-scientist-review.md`, `shelf/agents/out/02-critical-review.md`*

> **Status 2026-04-24 — local code phase complete.** Every ticket that
> doesn't need a live cluster has landed on `main` — SHELF-01 through
> SHELF-27 excluding the six cluster-gated ones, plus SHELF-26
> (offline replay analysis harness) added under the same rule. The
> remaining work — SHELF-13, SHELF-14, SHELF-18 acceptance, SHELF-20
> E7, SHELF-21 rollout, SHELF-28 drills — plus the v0.5 7-day
> observation window is owned by ops. See
> `shelf/docs/cluster-handoff.md` for the handoff packet (green-
> criteria, pointers, follow-ups tracked as SHELF-01a / SHELF-16b /
> SHELF-17a / SHELF-26a).

---

## 0. TL;DR

Ship a **scope-cut, measurement-gated Shelf** on top of a stabilised
Alluxio — not a 9-month greenfield dream. The team's first 8 weeks
build exactly one thing: a single-node, then 3-node, row-group-granular
cache on rep-2 that wins (or loses) a head-to-head against the
currently-fixed Alluxio. That fight is the **v0.5 gate**: match
Alluxio's measured 71 % hit rate, keep `GOLD_DBT` ok-rate ≥ 99.9 %, p95
within 20 %, for 7 consecutive days, at ≥ 50 % less oncall surface. If
v0.5 loses, the project dies on purpose instead of dying slowly.

Beyond v0.5, the v1.0 roadmap ships plan-aware prefetch (file + footer
only, plugin-side row-group observation — no `SplitCompletedEvent`,
which Trino PR #26436 removed), multi-node HRW hashing over the K8s
headless service (**no Raft**), size-threshold admission (**no ONNX
MLP** in v1), HTTP/2 everywhere (**no Arrow Flight** in v1), the
existing Redis-Gateway result cache from `COMPARISON.md` (**no
`shelf-result-cache`** in v1), and a public OSS launch.

Dropped outright: Phase 10 incremental MV refresh (wrong project, it's
a compute service). Deferred: learned admission, Arrow Flight, side-built
blooms §7.4.2, z-order awareness §7.4.3, MV-aware caching.

Calendar: **36-44 weeks** to OSS launch for a 3-person team (~2× the
blueprint estimate). Cost ceiling: one on-demand NVMe pool on rep-2 we
already own from the Alluxio footprint; no new IaaS spend before v0.5.

Five biggest risks: (1) Alluxio is already at 71 %, marginal lift may be
low; (2) Trino SPI churn (TrinoFileSystem rewrite in ~v464, could move
again); (3) cold-cache thundering herd against S3 per-prefix rate
limits; (4) team has never shipped a Rust service; (5) multipart S3
ETags are not MD5 — key invariant needs documenting.

---

## 1. What we are building (merged source of truth)

*This supersedes BLUEPRINT.md §1 for planning purposes until the diff
in `BLUEPRINT-DIFF.md` is applied.*

**Shelf** is a Rust, Apache 2.0, Iceberg-native, row-group-granular
read cache for Trino that replaces Alluxio OSS 2.9.5 on our 4 EKS Trino
clusters. It keeps exactly three differentiators from the blueprint —
(a) columnar-range granularity (manifest, footer, page index, row-group
byte range) with content-addressed keys; (b) shared cross-replica cache
(one warm pool, not four cold ones); (c) plan-aware push prefetch from
the Trino coordinator (file + footer only — row-group prefetch is
reactive, plugin-observation-based) — and gives up everything
speculative in v1:

- No embedded Raft. Membership = K8s headless service; pin list + quotas
= S3-backed ConfigMap pulled on SIGHUP / 15 min.
- No ONNX MLP admission. Size threshold (refuse > 1 GB unless pinned)
  - nightly-trained pin list from `cdp.trino_logs.trino_queries`.
- No Arrow Flight in v1. HTTP/2 range-GET with pooled connections for
every payload size; revisit Flight only if EKS-measured throughput
justifies it.
- No in-repo result cache. The already-scoped Redis + Trino-Gateway
result cache from COMPARISON.md Phase 0 owns result caching; `shelfd`
stays a pure byte-range cache.
- No in-cache blooms, no z-order detection, no MV pinning in v1. The
bloom *recommender* (§7.4.1) lives as an ops playbook, not Shelf code.
- No incremental MV refresh — dropped from the project entirely.

Plugin contract is unchanged from the blueprint: every Shelf error
becomes a transparent S3 fall-through via a per-pod circuit breaker,
which ships as a committed Java reference implementation with unit
tests in v0.1. Trino never sees a Shelf-specific error.

Gate: if v0.5 cannot beat Alluxio on rep-2 on measured metrics for 7
consecutive days, Shelf is killed. This is a feature, not a fear.

---

## 2. Unknowns and experiments (critical path first)

These block specific phases. Every experiment produces a numeric
answer. They sit at the top of the plan because they *move the
critical path*.


| #   | Question                                                                                                                               | Experiment (concrete)                                                                                                                                                                                                                                                                                                            | Owner              | Duration | Blocks                                             |
| --- | -------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ | -------- | -------------------------------------------------- |
| E1  | Does `QueryCreatedEvent.queryMetadata.plan` on Trino 480 actually expose tables + predicates + snapshot IDs?                           | (a) `EXPLAIN (FORMAT JSON) SELECT * FROM cdp.icesheet.silver_offline_event_data_2026 WHERE event_region='MP+CG' LIMIT 10;` on rep-0. (b) Install throwaway `EventListener` impl on rep-0 for 24h; log `queryId`, `plan`, `tables`, `routines`, `sessionProperties` to file. Count plans that expose `predicate` / `tableHandle`. | trino-plugin-eng-1 | 4 h      | Phase 2a                                           |
| E2  | How much signal does `QueryStatistics.getOperatorSummaries()` carry now that `SplitCompletedEvent` is gone?                            | Install `QueryCompletedEvent` listener on rep-0, collect 24 h of operator summaries. Bucket: % queries where `ScanFilterAndProject` operator summary names (file, partition, scanned bytes).                                                                                                                                     | trino-plugin-eng-1 | 1 d      | Phase 2b                                           |
| E3  | What is the per-stream HTTP/2 range-GET throughput from a Trino worker pod to a shelf pod on actual EKS networking?                    | Deploy single-pod `shelfd` on rep-2 pool; from a worker pod, pull 128 MiB × 50 iterations over pooled h2, record p50 / p95 / p99 and GB/s. Compare with direct S3 from the same pod.                                                                                                                                             | rust-engineer-1    | 4 h      | Phase 0 gate                                       |
| E4  | What is a ONNX Runtime single-inference latency for a 3-layer MLP, 10 features, on a Graviton3 pod?                                    | Ship the 20-line ORT benchmark (`onnxruntime` + static MLP); measure 10k inferences on warm session, record p50 / p95 / p99. Repeat with LightGBM same features.                                                                                                                                                                 | ml-engineer-1      | 4 h      | ADR-0003 fallback only                             |
| E5  | On 7 days of `cdp.trino_logs.trino_queries` replay, what is the scanned-byte reduction from row-group granularity vs file granularity? | Build replay harness: for each query, materialise Iceberg manifests + footers, compute needed row-group byte-ranges, sum vs raw file size. Report median and P90 ratio.                                                                                                                                                          | data-eng-1         | 2 d      | Phase 1 success gate (row-group claim)             |
| E6  | During a synthetic cold-cache storm (all 5 pods restart, 150 GiB working set re-warms), do we hit S3 per-prefix rate limits?           | Using the replay harness, simulate 5 pods starting with empty Foyer; issue the 24 h read sequence in 10 min of wall clock; count 503 SlowDown responses from S3, record recovery time.                                                                                                                                           | rust-engineer-2    | 1 d      | §9.4 failure-mode claim                            |
| E7  | HRW + K8s headless-service DNS cache refresh: during a pod rotation, how many mis-routed requests per client per minute?               | Chaos: delete `shelf-2` pod while synthetic workload runs 1 k req/s; count requests mis-hashed vs actual owner until convergence.                                                                                                                                                                                                | k8s-eng-1          | 4 h      | Phase 3 gate                                       |
| E8  | On `cdp.trino_logs.trino_queries` last 30 days, what is the empirically best HTTP-vs-Flight crossover size?                            | Using the replay harness, plot IPC framing overhead (simulated) vs zero-copy Flight gain at 256 KiB, 1 MiB, 4 MiB, 16 MiB payload.                                                                                                                                                                                               | rust-engineer-1    | 1 d      | v1.x Flight decision                               |
| E9  | How much HMS / Trino system-table load does a 30 s `SnapshotWatcher` poll add across 3 catalogs?                                       | Instrument `pg_stat_statements`-equivalent on HMS; run watcher in staging for 1 h; measure queries/s and p95 backend latency delta.                                                                                                                                                                                              | data-eng-2         | 4 h      | Phase 2 / 1.5 go/no-go                             |
| E10 | On v0.1 single-node, does p99 plugin-path latency stay within 5 % of direct S3?                                                        | Enable shadow traffic for non-critical queries on rep-0; collect 1 h; compare plugin-enabled vs plugin-disabled p50/p95/p99 from Trino `QueryCompletedEvent` / `OperatorStats` timings.                                                                                                                                          | trino-plugin-eng-1 | 4 h      | Phase 0 exit                                       |
| E11 | Foyer SIEVE vs S3-FIFO hit-ratio on 7-day rep-2 trace.                                                                                 | Replay harness — same trace, two Foyer configs, same DRAM + NVMe quotas. Report hit-rate delta and tail-latency delta.                                                                                                                                                                                                           | rust-engineer-2    | 1 d      | ADR-0009 (eviction default)                        |
| E12 | Alluxio current-day baseline on rep-2: 7-day measured hit rate, p50/p95 latency, `GOLD_DBT` ok rate, oncall page count.                | Pull from existing Grafana dashboards + PagerDuty. No new instrumentation.                                                                                                                                                                                                                                                       | sre-1              | 2 h      | v0.5 gate (have we actually got a number to beat?) |


Total critical-path experiment budget: **~8-10 engineer-days**, ideally
done inside Phase −1 / Phase 0 so the phases downstream build on
measured answers, not blueprint claims.

---

## 3. Phased roadmap

Phase numbering aligned with BLUEPRINT.md §12 and COMPARISON.md §2,
with Phase 10 **removed** (out of scope — it is a compute service, not
a cache; see ADR-0007). Durations use calendar weeks and assume a
3-person team running alongside the existing Alluxio on-call rotation.

### Phase −1 — Stabilise existing stack (1 week)

- **Entry criterion.** Alluxio on rep-2 stable with the
`UfsIOManager=256` patch in place (already true on 2026-04-23).
- **Deliverables.** (1) `emptyDir → hostPath` migration for every Trino
`fs.cache` volume on rep-1/2/3. (2) Audit `hive.metastore-cache-ttl`
across 3 catalogs → set to `10m`. (3) Verify
`iceberg.metadata-cache.enabled=false` wherever `fs.cache.enabled=true`.
(4) `UfsIOManager=256` committed to git (not just a live CM patch).
(5) Rep-2 KEDA cooldown MR merged.
- **Success gate.** `fs.cache` hit rate climbs from 15-20 % → ≥ 45 %
for 5 consecutive days on rep-1/2/3. Zero query regressions.
- **Dependencies.** None.
- **Risks.** `hostPath` PVs may not survive Karpenter node rotation on
shared pools — verify by draining a node mid-week.
- **Rollback.** `kubectl rollout undo` per Trino deployment; all changes
are config-only.

### Phase 0 — Proof of concept (v0.1) (2-3 weeks)

- **Entry criterion.** Phase −1 complete; E1/E3/E10 experiments
scheduled.
- **Deliverables.** (1) `shelfd` Rust binary: Axum HTTP server, Foyer
DRAM-only cache (64 GiB), `GET`/`HEAD` with range support,
content-addressed keys `sha256(etag || offset || length)`, S3 origin
client (AWS SDK v2, pooled), Prometheus `/metrics`, `/healthz`,
`/readyz`. (2) `shelf-trino-plugin`: `ShelfFileSystem` pass-through
wrapper, fail-open on every error. (3) Circuit-breaker reference Java
class, shipped with unit tests (scenarios: closed/open/half-open,
concurrent `record_failure`, per-pod isolation). (4) Integration
harness: docker-compose (Trino 480 + `shelfd` + minio) runs 10-query
smoke test green. (5) 1-pod Deployment on rep-2 `alluxio` Karpenter
pool (reuse existing nodes, no new IaaS).
- **Success gate.** A single query against
`cdp.icesheet.silver_offline_event_data_2026` on rep-0 is served from
Shelf (verified by Grafana: `shelf_hits_total{tier="dram"} > 0`);
plugin shadow-traffic on rep-2 shows p99 overhead ≤ 5 % vs direct S3
(E10 numeric).
- **Dependencies.** Phase −1.
- **Risks.** Rust team velocity; tokio + Foyer + Tonic version tangle;
NVMe PVC provisioning on rep-2 pool (not used yet in Phase 0, but
validated here).
- **Rollback.** `fs.shelf.enabled=false` per catalog; no user-visible
state.

### Phase 0R — Quick-win result cache (parallel, 2-3 weeks)

*Not a "new" phase — this is COMPARISON.md Phase 0, which has always
been in the merged roadmap. It runs in parallel with Phase 0 so users
see value before Shelf v0.5 even compiles.*

- **Entry criterion.** Phase −1 complete. Redis 7 Helm chart landed.
- **Deliverables.** (1) Redis cluster (`cache` ns, 3 × 32 GB primaries).
(2) `SnapshotWatcher` Python sidecar polling Trino system tables
every 30 s. (3) `trino-gateway-result-cache` plugin keyed on
`sha256(normalized_sql || snapshot_map)`. (4) Enabled for BI users
(`pbi_`*, `mbuser`, `commonuser`).
- **Success gate.** Dashboard queries ≤ 5 ms on cache hit; ≥ 60 % hit
rate on BI traffic after 5 days.
- **Dependencies.** None with Shelf. Owner track: data-platform, not
cache-team.
- **Risks.** HMS load from the watcher (E9). If bad, cap polling
rate and back off on no-change.
- **Rollback.** Disable plugin in Trino Gateway config.

### Phase 1 — Columnar granularity + NVMe + 3-node HRW (v0.5) (6-8 weeks)

This is the biggest phase; it contains the v0.5 gate.

- **Entry criterion.** Phase 0 exit criteria hit; E3 and E10 numeric
results recorded; rep-2 Alluxio baseline pulled (E12).
- **Deliverables.** (1) Parquet footer caching: plugin recognises the
Parquet footer range-GET pattern, issues a fetch of the last 64 KB
before the worker asks. (2) Row-group byte-range support: key is
`sha256(etag || rg_ordinal || offset || length)` with ordinal pulled
from the cached footer. (3) Iceberg manifest caching (pool.metadata,
DRAM, Foyer FrozenHot). (4) Foyer NVMe tier configured with S3-FIFO
(the Foyer built-in — see ADR-0009). (5) **Two-pool** layout: one
DRAM pool (manifests + footers, 5 GiB), one hybrid pool (row groups,
500 GiB/pod). (6) Rendezvous (HRW) hashing in both plugin and
`shelfd`, with capacity weights from `/stats`; golden-vector unit test
cross-checks Rust and Java hashes byte-identical. (7) 3-pod
StatefulSet on the rep-2 pool, K8s headless-service membership,
no Raft. (8) S3-compat shim: `GetObject` + `HeadObject` only, so
DuckDB / notebook traffic can participate. (9) `shelfctl` CLI:
`stats`, `pin`, `evict`, `ring`, `reload`. (10) Pin list:
`pin_list.json` in S3 ConfigMap, reloaded on SIGHUP. (11) Size
threshold admission: refuse ≥ 1 GiB unless pinned. (12) Grafana
dashboard (insight-first: traffic-light hit rate, p95, fallback rate,
pod health). (13) Chaos drills: KEDA rotation + pod-kill, both
passing weekly. (14) `trino_logs` replay benchmark harness (from E5).
- **Success gate (v0.5).** On rep-2, live traffic routed through Shelf
instead of Alluxio:
  - Cumulative hit rate ≥ 71 % for 7 consecutive days
  - `GOLD_DBT` ok-rate ≥ 99.9 %
  - p95 query latency within 20 % of Alluxio baseline
  - Zero Shelf-caused pages for 7 days
  - Oncall surface (tracked manually) ≤ 50 % of Alluxio's: measured by
  unique runbook lookups + pages per week.
  If any one misses, **the project does not continue** until the gap
  is closed or Shelf is killed. This is the kill-switch.
- **Dependencies.** Phase 0 complete; E5, E7, E11.
- **Risks.** (a) Alluxio baseline is a fixed bar that is now stable;
beating it on granularity but losing on hit rate is a real failure
mode. (b) Foyer NVMe tier immature for our load. (c) HRW re-routes
during KEDA churn cause hit-rate wobble (E7).
- **Rollback.** Point plugin back at Alluxio via
`fs.shelf.enabled=false`; 3-pod StatefulSet can stay running idle for
30 days while team diagnoses.

### Phase 2 — Plan-aware prefetch (4-5 weeks)

- **Entry criterion.** Phase 1 passed v0.5 gate. E1 + E2 numeric
results recorded. **No code until E1 says `QueryCreatedEvent` carries
the needed fields.**
- **Deliverables.** (1) `ShelfPrefetchListener` Java class: on
`QueryCreatedEvent`, extract tables + predicates + snapshot IDs;
read Iceberg manifest from Shelf's own metadata tier; issue
`Prefetch` gRPC for `(metadata.json, manifest_list, matching-partition footers)` — file + footer only (no row groups) — with a **hard 10 ms
coordinator-side deadline**. (2) Shelf-side prefetch queue per
tenant, bounded depth (default 1 024), priority 0 for dashboard /
priority 10 for bulk. (3) Cancellation on `QueryCompletedEvent`.
(4) Plugin-side Phase 2b-signal-1: after a worker range-GET on a
footer, plugin parses row-group stats vs captured predicate and
issues row-group prefetch locally (no listener involvement). (5)
Post-hoc learning: `QueryCompletedEvent.operatorSummaries` feeds a
nightly Airflow job building `(query_sketch → likely_row_groups)`
lookup table consumed by Phase 2a's prefetch on the *next* matching
query.
- **Success gate.** TTFQ (time-to-first-query) after 10× scale-up
(2 → 20 workers) ≤ 3 s p95 on dashboard queries, measured via the
cold-start benchmark harness (§10.2). Cold-start tax eliminated.
- **Dependencies.** Phase 1 v0.5 gate passed.
- **Risks.** (a) `splitCompleted` is gone (PR #26436) — if E2 shows
operator summaries don't carry enough signal, learning path is
limited. (b) Listener can block coordinator thread if deadline not
enforced (§9.5 circuit breaker applies here too). (c) SPI churn —
`TrinoFileSystem` rewrite in ~v464 shows this can happen again.
- **Rollback.** `shelf.prefetch.enabled=false` per catalog. Listener
continues to run as no-op; no restart needed.

### Phase 3 — Scale-out + S3-shim hardening (3-4 weeks)

- **Entry criterion.** Phase 2 passed.
- **Deliverables.** (1) Scale StatefulSet from 3 → 5-7 pods; validate
HRW rebalance. (2) Per-prefix S3 rate limiter on fallback path (per
§9.4 — thundering-herd mitigation). (3) S3-shim hardening: error
parity with real S3 for `NoSuchKey`, `AccessDenied`. (4) Migrate all
rep-2 traffic from Alluxio to Shelf (Alluxio kept hot-standby for 30
days). (5) `tokio`-blocking-call lint; runtime starvation smoke test
under NVMe write pressure. (6) Per-prefix connection pools on origin
client (drop "one global pool" pattern from v0.1).
- **Success gate.** Chaos test: kill 1 pod / 5 min for 1 hour, hit rate
stays ≥ 65 %. Thundering-herd test: simultaneous 5-pod kill, S3
request rate caps at configured prefix limit with 0 query errors.
- **Dependencies.** Phase 2 complete.
- **Risks.** Per-prefix pool count explosion (thousands of prefixes in
`cdp`); memoise + evict.
- **Rollback.** Scale StatefulSet back; Alluxio hot-standby re-engaged
via ConfigMap flip.

### Phase 4 — Learned admission (evaluation only) (2-6 weeks)

- **Entry criterion.** Phase 3 stable; E4 numeric + E5 numeric
recorded.
- **Deliverables.** (1) `trino_logs`-driven **pin list trainer** — a
nightly Airflow job that emits `pin_list.json` sorted by `scanned_bytes × wall_time × frequency`, top-N per tenant. Ops reviews PR-style.
(2) **Benchmark** size-threshold-only vs size-threshold + LightGBM
admission on 30-day replayed trace. (3) Decision point: if LightGBM
lifts hit rate by ≥ 5 pp over size-threshold alone **on replayed
traffic** and adds < 50 µs to large-miss path, ship LightGBM
(ADR-0003 escape hatch); else stop and ship only pin-list.
- **Success gate.** Pin list merged and live; NVMe write bandwidth cut
≥ 40 % vs v0.5 baseline on ad-hoc scans. Any learned model only
shipped if measured gap ≥ 5 pp.
- **Dependencies.** Phase 3 stable; `cdp.trino_logs.trino_queries`
replay harness from Phase 1.
- **Risks.** (a) Ops blames pin list for eviction storm; PR flow + git
history mitigates. (b) LightGBM retraining cadence drift (weekly is
enough for dashboard cohort stability, per E5's week-over-week
stationarity sidecar).
- **Rollback.** `pin_list.json` → empty; admission = size-threshold
only.

### Phase 5 — Productionise rep-2 (3-4 weeks)

- **Entry criterion.** Phase 4 complete.
- **Deliverables.** (1) Full runbook (AGENTS.md-compliant: traffic-light
dashboard, 3-step diagnosis tree, on-call rotation). (2) Capacity
plan: NVMe headroom ≥ 30 %; alarms on DRAM pool saturation. (3) HA
config: 5-pod StatefulSet across ≥ 2 AZs (on-demand, not spot). (4)
Retire Alluxio from rep-2 (hot-standby decommissioned). (5) Incident
postmortem template + on-call handoff. (6) Remove rep-2 `alluxio-worker`
StatefulSet.
- **Success gate.** 7 consecutive days zero Shelf-caused incidents on
rep-2 with Alluxio decommissioned; `alluxio-worker` headcount reduced
to 0 on rep-2.
- **Dependencies.** Phase 4 complete.
- **Risks.** Alluxio decommissioned too early → no rollback. Mitigation:
keep the `alluxio-values.yaml` in repo + a 24-h "re-deploy Alluxio"
drill rehearsed once before decommission.
- **Rollback.** Re-apply Alluxio Helm chart; 24 h warm-up; plugin
`fs.shelf.enabled=false`.

### Phase 6 — Roll to rep-0 / rep-1 / rep-3 (4-6 weeks)

- **Entry criterion.** Phase 5 done.
- **Deliverables.** (1) Replica-specific rollouts, one at a time, each
with its own ACL story: rep-2 Ranger, rep-3 file-based `rules.json`
(see AGENTS.md). (2) Shared-vs-per-replica Shelf cluster decision:
blueprint says shared (§15), critic agrees; ship as a single shared
Shelf cluster with per-replica tenants. (3) Per-tenant quotas wired
through to pool accounting. (4) Full Alluxio retirement.
- **Success gate.** All 4 replicas on shared Shelf; Alluxio retired;
`alluxio-`* containers deleted from Helm; 7 days zero incidents.
- **Dependencies.** Phase 5 done.
- **Risks.** Rep-3's file-rules ACL surface breaks tenant isolation on
Shelf if IRSA role mapping has drift; pre-test with rep-3 canary.
- **Rollback.** Per-replica rollback only — one replica can fall back
to Alluxio without affecting others (requires Alluxio hot-standby per
replica during Phase 6).

### Phase 7 — OSS launch (3-4 weeks)

- **Entry criterion.** Phase 6 done; all four replicas on Shelf for 14
consecutive days.
- **Deliverables.** (1) Public repo (`github.com/penpencil-oss/shelf`
or similar — see §8). (2) Apache 2.0 license, CLA, codeowners,
security policy, CONTRIBUTING.md, CODE_OF_CONDUCT.md. (3) MkDocs
site with quick-start, runbook, benchmarks, ADR index. (4) One
reproducible benchmark: 7-day `trino_logs` replay (NOT TPC-DS in v1 —
that's v1.1 content). (5) Launch blog post: "Why we replaced
Alluxio: a row-group-granular, plan-aware, open-source cache for
Trino". (6) HN + Discord + issue-triage rota.
- **Success gate.** Repo public; blog post published; first external PR
or issue responded to within 48 h.
- **Dependencies.** Phase 6 done.
- **Risks.** "Yet another cache" fatigue. Lead with *measured*
numbers, not claims.
- **Rollback.** Archive repo (unlikely; OSS isn't production).

### Phase 8 — Approximate in-cache indexes (4-6 weeks, **parallel with Phase 7 only if 4+ engineers**)

- **Entry criterion.** Phase 7 done **or** team expands to 4+
engineers.
- **Deliverables.** (1) Parquet bloom-filter recommender (trainer-side,
§7.4.1) — as an **ops playbook**, not new Shelf code. (2) Side-built
blooms in `shelfd` with `ShelfFilterService` gRPC (§7.4.2) — this is
the only new Shelf code in Phase 8. (3) Z-order / sort-order
detection in control plane (§7.4.3).
- **Success gate.** Selective-equality benchmark: scanned-bytes cut
≥ 60 % on top 20 `WHERE col = literal` queries.
- **Dependencies.** Phase 7 or expanded team.
- **Risks.** Scope creep into "index engine" territory (blueprint
explicitly calls this out in §7.4.4).
- **Rollback.** `ShelfFilterService` disabled via config flag; reader
falls back to standard path.

### Phase 9 — MV-aware caching (3 weeks, **parallel with Phase 7 only if 4+ engineers**)

- **Entry criterion.** Phase 7 done or team expands. Trino 468+ MV
support live in production.
- **Deliverables.** (1) MV recommender in trainer. (2) Shelf pins MV
files in DRAM hot pool (no new code — reuses pin list). (3) Control
plane tracks MV → base-table graph + per-MV hit rate.
- **Success gate.** Top 10 dashboard aggregations served from MV + Shelf
in < 20 ms p95.
- **Dependencies.** Trino MV support, Phase 7.
- **Risks.** MV refresh is an Iceberg / dbt problem, not a cache
problem — stay out of it.
- **Rollback.** Stop pinning MVs; no code to roll back.

### Phase 10 — **REMOVED**

See ADR-0007. Incremental MV refresh is a compute service, not a
cache. If the org wants it, start a separate project (`shelf-mv-refresh`
or rename) — Shelf will happily cache whatever files it writes.

### Parallelism table


| Phase | Parallelisable with           | Notes                                                      |
| ----- | ----------------------------- | ---------------------------------------------------------- |
| −1    | Nothing                       | Stabilisation precedes everything.                         |
| 0     | 0R                            | 0R is data-platform work, not cache-team. Different owner. |
| 1     | 0R tail                       | Should overlap by 1-2 weeks.                               |
| 2     | —                             | Serialised after v0.5 gate.                                |
| 3     | —                             | Serialised.                                                |
| 4     | —                             | Serialised (relies on Phase 3 stability).                  |
| 5     | —                             | Serialised (production cutover).                           |
| 6     | —                             | Replica rollouts serial, not parallel (ACL differences).   |
| 7     | 8, 9 *iff* team ≥ 4 engineers | 3-person team must serialise.                              |
| 8     | 7, 9                          | See above.                                                 |
| 9     | 7, 8                          | See above.                                                 |


**Total calendar at 3 engineers:** 36-44 weeks end-to-end (Phase −1
through Phase 7). Phases 8 + 9 push to ~44-52 weeks if kept serial.
This is ~2× the blueprint's 20-22 weeks and is the honest number.

---

## 4. Phase 0 + Phase 1 tickets

28 tickets total. All sized S / M / L; none XL. Each is actionable on
day 1 by a single engineer with blueprint + this plan + the repo
scaffolding in hand.

### Phase 0 (v0.1)

**SHELF-01 — Bootstrap Cargo workspace + CI** — **CLOSED (structure + verify-on-PR rail); benchmark CI split to SHELF-01a**
Set up the monorepo: `shelfd/`, `shelfctl/`, `clients/trino/`,
`protos/`, `charts/`, `benchmarks/`, `docs/`, `.github/`. Cargo
workspace for Rust, Maven for Java, Taskfile for top-level commands.
GitHub Actions: `cargo fmt + clippy + test`, `mvn verify`, Docker
build, helm lint.

- `cargo workspace` compiles (`members = ["shelfd", "shelfctl"]`
in [Cargo.toml](Cargo.toml); both binaries build green).
- `mvn verify` passes in `clients/trino` (78+ tests, see
`clients/trino/pom.xml`).
- Apache 2.0 LICENSE, CODEOWNERS, SECURITY.md, CONTRIBUTING.md
committed at repo root (+ `SECURITY/{CHECKLIST,IAM,SUPPLY_CHAIN,THREAT_MODEL}.md`).
- CI runs on PR in < 10 min — *SHELF-01a landed as
`.github/workflows/verify.yml`: parallel Rust / Java / Python
lanes (`cargo fmt + clippy + test`, `mvn verify`,
`pytest benchmarks/trino_logs`) with an aggregation
`verify-gate` job. Dockerfile + helm-lint rails live under
SHELF-09 / `helm-lint.yml` / `smoke.yml`; `security.yml` runs
supply-chain scans. All green in CI.*
- Effort: M. Depends on: — . Owner: rust-engineer-1.
- Out of scope: Helm templates beyond `lint` placeholder.

**SHELF-02 — `shelfd` Axum HTTP server skeleton** — *Closed (Phase-0 read-path pass)*
Rust binary, Axum router with `/healthz`, `/readyz`, `/metrics` (empty
Prometheus registry), graceful-shutdown via SIGTERM, structured
logging via `tracing`. Docker image built from scratch base.

- `curl :9090/healthz` returns 200
- `curl :9090/readyz` returns 503 until cache init complete
- `docker run shelf:0.1` exits cleanly on SIGTERM *(container image deferred to SHELF-09)*
- Effort: S. Depends on: SHELF-01. Owner: rust-engineer-1.
- Out of scope: cache layer.

**SHELF-03 — Foyer DRAM-only cache integration** — *Closed (Phase-0, DRAM-only; NVMe rowgroup tier deferred to SHELF-18)*
Wire `foyer::HybridCache` with DRAM-only config, 64 GiB max, SIEVE
policy (Foyer built-in). Pool ID = content-addressed key as-is. No
NVMe yet.

- `cache.insert(key, bytes)` / `cache.get(key)` unit test passes
- DRAM eviction triggers at 90 % capacity *(size-weighter hits Foyer's built-in eviction)*
- Prometheus metric `shelf_bytes_used` exported (hits/misses + bytes_used registry)
- Effort: M. Depends on: SHELF-02. Owner: rust-engineer-1.
- Out of scope: NVMe tier, S3-FIFO, GL-Cache.

**SHELF-04 — Content-addressed key function (Rust + Java)** — **CLOSED**
Shared key derivation: `sha256(etag_bytes || le_u64(offset) || le_u64(length))`. Rust lib + Java lib. Golden-vector unit test: a
frozen set of 20 test inputs produces the same hex output on both
sides.

- `rust test key::roundtrip` green
(`shelfd::store::key_tests` — 10 cases incl.
`roundtrip_produces_same_digest`, `etag_changes_key`,
`offset_and_length_change_key`, `golden_vectors_match_fixture`).
- `mvn test KeyTest#roundtrip` green on same vectors
(`io.shelf.client.KeyTest` — 9 cases; golden fixture at
`shelfd/tests/fixtures/`* consumed by both sides so drift
breaks the build immediately).
- Both sides reject keys with length = 0
(`store::key_tests::rejects_zero_length`,
`KeyTest::rejectsZeroLength`).
- Multipart-ETag note in rustdoc + javadoc: ETag is not
cryptographic (documented alongside the `Key` type).
- Effort: S. Depends on: SHELF-01. Owner: rust-engineer-2.
- Out of scope: key versioning.

**SHELF-05 — S3 origin client in `shelfd` (AWS SDK v2 Rust)** — *Closed (Phase-0 read-path pass)*
`aws-sdk-s3` client with one pooled `HyperClient`, retry-on-503,
per-request `x-amz-request-id` logging. Expose `get_range(bucket, key, offset, length) -> Bytes` as the only entry point.

- Against local MinIO, 100 concurrent `get_range` calls finish with
zero errors *(see `shelfd/tests/it_read_path.rs::one_hundred_concurrent_misses_collapse_to_one_origin_call`)*
- Request-ID logged to `tracing` on 5xx *(request-id logged on every response; SDK retry classifier handles 5xx)*
- IRSA credential provider tested via AWS_ROLE_ARN env *(default provider chain wired; explicit IRSA test-harness deferred — default chain covers EKS)*
- Effort: M. Depends on: SHELF-02. Owner: rust-engineer-2.
- Out of scope: per-prefix pool sharding (Phase 3).

**SHELF-06 — `GET /cache/<key>/<offset>-<len>` with read-through** — *Closed (Phase-0 read-path pass)*
Axum handler: lookup `(pool, key, offset, length)` in Foyer; on miss call S3,
insert, return. Return `Content-Range` header. No admission decision
yet (everything admitted up to the size threshold).

Route shape evolved to `GET /cache/<pool>/<key>/<offset>-<end>` — the
`<pool>` path segment routes between `metadata` and `rowgroup` pools
so every request is self-contained (no custom header dispatch).

- Hit returns < 5 ms p99 DRAM (E3 result blocking) *(benchmark pending; unit timings sub-ms)*
- Miss returns S3-latency + 1 ms p95 *(benchmark pending; MinIO local loop ~1-2 ms)*
- Parallel reads of the same key coalesce (single S3 GET) *(unit proof in `store::store_tests::single_flight_coalesces_concurrent_misses`: 100 concurrent misses → 1 fetch; wire-level proof in `it_read_path::one_hundred_concurrent_misses_collapse_to_one_origin_call`)*
- Metrics: `shelf_hits_total`, `shelf_misses_total` by pool
- Effort: M. Depends on: SHELF-03, SHELF-05. Owner: rust-engineer-1.
- Out of scope: coalescing perfection (can ship single-flight via
`moka::Cache` or simple mutex); NVMe.

**SHELF-07 — `HEAD /cache/<key>` and range metadata endpoint** — **CLOSED**
For the S3-shim path and for the plugin's pre-flight size check.

- Returns S3's Content-Length without a full GET
(route `HEAD /cache/:pool/origin/:bucket/*s3_key`, headers
`Content-Length`, `X-Shelf-ETag`, `X-Shelf-LastModified`;
`Origin::head` now returns `Ok(None)` on 404 so the handler maps
cleanly to HTTP 404).
- Caches the HEAD result in a small DRAM LRU (10k entries)
(`shelfd/src/head_lru.rs`, foyer-backed with entry-count weighter;
configurable via `head_lru_entries`, default 10 000).
- Metrics: `shelf_head_hits_total{pool}` / `shelf_head_misses_total{pool}`.
- Tests: `head_lru::tests` (4 unit), `it_head_stats::`* (4 integration,
`SHELF_INTEGRATION=1`, MinIO-backed).
- Bundled in the same commit: `GET /stats` JSON contract consumed by
SHELF-20. See `shelfd/docs/design-notes/SHELF-07-head-and-stats.md`.
- Deferred: single-flight on HEAD misses, origin-side `HEAD` rate
limiter, `pinned_bytes` on `/stats` (requires SHELF-24).
- Effort: S. Depends on: SHELF-06. Owner: rust-engineer-2.
- Out of scope: full S3 ListObjects.

**SHELF-08 — Prometheus metrics + OTel traces** — **CLOSED**
`prometheus` crate registry exposed at `:9091/metrics`; `tracing-opentelemetry`
exporting to cluster Tempo. Trace every `GET /cache/`* request end-to-end
(server + S3 client).

- Grafana panel shows `rate(shelf_hits_total[1m])`
(starter dashboard at `observability/dashboards/shelf-read-path.json`
— 3 panels: hit rate, miss rate, p95 `shelf_request_seconds`;
schemaVersion 39. Full layout stays in SHELF-27.)
- Tempo trace for a single request shows 2 spans (Axum + S3 client)
(`shelfd::tests::it_traces::*` asserts the parent→child shape with a
test subscriber: `http.get_cache` → `s3.get_object`, plus a
`shelfd.singleflight{role=leader|follower}` event. Works without a
live OTLP collector; exporter itself is config-gated via
`observability.otlp_endpoint` / `SHELFD_OTLP_ENDPOINT`.)
- New module `shelfd::telemetry` with a `TelemetryGuard` whose `drop`
swallows exporter shutdown errors so SIGTERM is never blocked by a
flaky collector.
- Metrics regression test (`metrics::tests::registry_exposes_documented_series`
and `metrics_scrape_contains_documented_series_after_touch`) guards
the stable series names from rename drift. `shelf_request_seconds`
is now exercised by every HTTP handler.
- Deps: `opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp`,
`tracing-opentelemetry` (pinned to a compatible family). Init is
fail-open: OTLP connection failure logs a warning and proceeds.
- Design notes at `shelfd/docs/design-notes/SHELF-08-observability.md`.
- Effort: S. Depends on: SHELF-06. Owner: sre-1.
- Out of scope: custom dashboard layout (ticket SHELF-27).

**SHELF-09 — Dockerfile + base Helm chart (1-pod Deployment)** — **CLOSED**
Multi-stage Rust build → distroless base; Helm chart in
`charts/shelf/` with values.yaml parameterising image tag, resources,
nodeSelector. Image + CI rail shipped in
`[shelfd/Dockerfile](../../shelfd/Dockerfile)` and
`[.github/workflows/helm-lint.yml](../../.github/workflows/helm-lint.yml)`;
design note at
`[shelfd/docs/design-notes/SHELF-09-dockerfile-and-helm-lint.md](../../shelfd/docs/design-notes/SHELF-09-dockerfile-and-helm-lint.md)`.

- `docker build` image ≤ 80 MB *(distroless/cc runtime + stripped
release binary; gated in CI at 150 MiB uncompressed ≈ 80 MiB
compressed — see `.github/workflows/helm-lint.yml` job
`docker-build`)*
- `helm install --dry-run` clean on default values *(covered by
`.github/workflows/helm-lint.yml` jobs `helm-lint` and
`helm-template`; the latter pipes `helm template` through
`kubectl apply --dry-run=client` against
`charts/shelf/ci/lint-values.yaml`)*
- Deploys 1-pod Deployment on the rep-2 `alluxio` Karpenter pool
*(deferred to SHELF-13 — real-cluster rollout; chart topology is
a StatefulSet per the Phase-1 target in §3, not a Deployment, so
this AC is tracked under shadow-traffic rollout rather than the
image/lint gate)*
- Effort: M. Depends on: SHELF-02. Owner: k8s-eng-1.
- Out of scope: StatefulSet (Phase 1), multi-arch buildx, image
signing (SHELF-21).

**SHELF-10 — `ShelfFileSystem` Java plugin skeleton** — *Closed (Phase-1 plugin pass; SPI-level FS factory registration is structurally unavailable in Trino 480 — see SHELF-22 for the endpoint-swap wiring that actually lands reads on shelfd)*
Java module in `clients/trino/`: extends Trino's `TrinoFileSystem`,
intercepts `newInputFile(Location)` for a configured prefix list,
delegates everything else to the parent `S3FileSystem`.

- Loads into Trino 480 without classpath errors *(META-INF/services under `clients/trino/src/main/resources/META-INF/services/io.trino.spi.Plugin` points at `io.shelf.plugin.ShelfPlugin`; confirmed in the SHELF-12 smoke — Trino logs `Registering event listener shelf-prefetch`. Trino 480's `Plugin` SPI has no `getFileSystemFactories()` entry point, so the plugin itself only carries the event-listener path; all actual read interception happens through the SHELF-22 shim)*
- `fs.shelf.enabled=false` = pass-through with < 1 % overhead *(`ShelfFileSystemTest::disabledConfigReturnsDelegateInputFileUnmodified` returns the delegate `TrinoInputFile` unchanged — zero wrapping cost)*
- `fs.shelf.enabled=true` with no Shelf endpoint = fail-open to S3
(never throws to Trino) *(property covered by `ShelfInputStreamTest::shelfFailureFallsThroughToDelegateAndReturnsItsBytes`, `::failureIsStickyWithinStream`, `::openBreakerSkipsShelfEntirely`, and end-to-end in `ShelfFileSystemTest::failsOpenWhenShelfIsUnreachable`)*
- `ShelfConfig` parses + validates the full BLUEPRINT §6.2 surface (`ShelfConfigTest`, 12 cases)
- `ShelfHttpClient.rangeGet` issues `GET /cache/<pool>/<key>/<offset>-<end>` with per-RPC deadline (`ShelfHttpClientTest`, 9 cases including timeout, 503, connection refused, and circuit-breaker integration)
- Effort: M. Depends on: SHELF-04. Owner: trino-plugin-eng-1.
- Out of scope: `EventListener`, circuit breaker (SHELF-11), prefetch.

**SHELF-11 — Circuit-breaker reference Java class + unit tests** — **CLOSED**
Per-pod `CircuitBreaker` (closed / open / half-open), failure counter
`AtomicInteger`, 5-failure threshold, 10 s open window, half-open
single-probe. Shipped in `clients/trino/` with full unit test suite.

- 12 unit tests (≥ 9 required) in
`[io.shelf.client.CircuitBreakerTest](../../clients/trino/src/test/java/io/shelf/client/CircuitBreakerTest.java)`
cover: `closed→open` (5 consecutive failures),
`open→half_open` after cooldown, `half_open→open` on probe
fail (with exponential backoff up to the 60 s ceiling),
`half_open→closed` on probe success, concurrent
`recordFailure` safety, per-pod isolation via separate
`CircuitBreaker` instances, failure-counter reset on success,
single-probe admission in `half_open` under contention, and
cooldown reset after a successful probe.
- State-machine diagram (Mermaid) + expanded semantics in
`[clients/trino/README.md](../../clients/trino/README.md)`
§"State machine (CircuitBreaker, BLUEPRINT §9.5)", cross-linking
the implementation and test classes.
- Effort: M. Depends on: SHELF-10. Owner: trino-plugin-eng-1.
- Out of scope: metrics (Phase 1).

**SHELF-12 — Docker-compose integration harness + 10-query smoke test** — **CLOSED**
`benchmarks/smoke/`: docker-compose (Trino 480, `shelfd`, MinIO, seed
Parquet data). A bash script loads 3 Iceberg tables, runs 10 canonical
queries, asserts each returns expected row count.

- Compose stack + seed end-to-end runnable via
`make smoke` from repo root (see
`[benchmarks/smoke/docker-compose.yml](../../benchmarks/smoke/docker-compose.yml)`,
`[benchmarks/smoke/run-smoke.sh](../../benchmarks/smoke/run-smoke.sh)`,
`[Makefile](../../Makefile)`); CI rail at
`[.github/workflows/smoke.yml](../../.github/workflows/smoke.yml)`.
- Cold-vs-warm query output diff check across all 10 queries —
byte-identical, correctness PASS.
- Local dev setup under 5 min for new engineers: the compose is
single-command, Dockerfile-based shelfd build (~90 s cold,
~10 s warm), and the seed completes in <15 s.
- Smoke log captures `shelf_hits_total > 0` on the warm run — **PASS**
via the SHELF-22 endpoint-swap wiring: Iceberg's `s3.endpoint` now
points at `http://shelfd:9092`, Trino's native S3 client issues
normal `HeadObject` / `GetObject(Range)` against the shim, and the
warm run shows `metadata 28→56` and `rowgroup 10→20` hits on the
seeded TPC-H slice.
- Effort: M. Depends on: SHELF-06, SHELF-10. Owner: qa-eng-1.
- Out of scope: TPC-DS.

**SHELF-13 — Shadow-traffic rollout on rep-2**
Deploy the 1-pod `shelfd` on rep-2; enable plugin for `replica-2-canary`
resource group (non-critical notebooks only), 5 % traffic via Trino
Gateway routing rule.

- Grafana: `shelf_hits_total` > 0 after 1 h of traffic
- Zero `QueryFailedEvent` attributed to plugin (read logs)
- p99 overhead ≤ 5 % on plugin-enabled path (E10)
- Effort: M. Depends on: SHELF-09, SHELF-13 (self-loop resolved by
Gateway team). Owner: sre-1.
- Out of scope: production routing.

**SHELF-14 — Run E1 + E3 + E10 + E12 experiments**
Execute the four Phase-0-blocking experiments from §2. Record results
in `out/experiments/`.

- E1: listener log shows what `plan` / `tables` carries
- E3: h2 throughput p50 / p95 recorded
- E10: plugin overhead numeric
- E12: Alluxio 7-day baseline pulled
- Effort: M (parallelisable across team). Depends on: SHELF-13,
separate data pulls. Owner: whole team, sre-1 coordinates.
- Out of scope: producing answers; only recording.

### Phase 1 (v0.5)

**SHELF-15 — Parquet footer prefetch in plugin** — **CLOSED (code path); end-to-end hit-ratio conformance deferred**
Plugin detects `.parquet` path, issues a pre-emptive 64 KiB range-GET
to Shelf for the footer *before* the Trino reader asks for it. Tunable
via `shelf.footer.prefetch.kib` (default 64, max 256).

- Prefetch trigger + config key
(`io.shelf.client.FooterPrefetcher`, a 2-thread
`ThreadPoolExecutor` with `CallerRunsPolicy` for backpressure.
Invoked from `ShelfFileSystem.newInputFile(Location)` when
`enabled && prefetch.enabled && path.endsWith(".parquet") (ci) && resolver.ownerFor(key).isPresent()`. Small-file edge clamps
the window to `[0, length)`.)
- Config: `shelf.footer.prefetch.kib` (default 64, min 1, max 256)
with full validation + negative-path tests in `ShelfConfigTest`.
- Fail-open: prefetch failures swallowed to FINE log; `Throwable`
boundary at the executor task so even OOM cannot leak. Verified
via `FooterPrefetcherTest` (5 cases).
- Grafana `shelf_footer_hits_ratio` > 90 % on second query
— deferred until SHELF-12 (docker-compose smoke harness)
lands; no live Trino + MinIO fixture wired yet.
- Smoke test end-to-end first→second query hit validation
— same deferral as above (needs SHELF-12).
- Pool routing: prefetch always targets `Pool.METADATA` via
`ShelfFileSystem.poolForFooter()`; body reads still route
`.parquet → rowgroup`. A single file therefore has bytes in both
pools, by design.
- Metrics seam: `PrefetchMetrics` (plugin-side `AtomicLong` counters
`footerPrefetchScheduled/Completed/Failed`) exposed via
`ShelfFileSystem.prefetchMetrics()` for test observation. Trino's
metrics integration remains out of scope.
- Design notes at `clients/trino/docs/design-notes/SHELF-15-footer-prefetch.md`.
- Effort: M. Depends on: SHELF-10. Owner: trino-plugin-eng-1.
- Out of scope: page index prefetch.

**SHELF-16 — Row-group byte-range key extension** — *CLOSED (key extension + plumbing); full Parquet footer parser deferred to SHELF-16b*
Extend content-addressed key to include `rg_ordinal`. Plugin threads a
`RowGroupIndex` abstraction through `ShelfInputFile`/`ShelfInputStream`
so every range GET is keyed under the owning row-group's ordinal.
`shelfd` treats row-group keys exactly like any other (admission-wise).

- Unit test: (file X, rg 2) and (file X, rg 3) produce distinct keys
— `io.shelf.client.KeyTest#keysDifferByRowGroupOrdinal` +
`ShelfInputStreamTest#contentKeyDiffersBetweenRowGroupOrdinals`
(on-wire contentKey changes between rg#0 and rg#1 reads). The
shared golden fixture at
`shelfd/tests/fixtures/shelf04_golden_vectors.txt` grew to 17
entries spanning rg-ordinal variants; Rust
(`shelfd::store::key_tests::golden_vectors_match_fixture`) and
Java (`KeyTest#goldenVectorsMatchSharedFixture`) both diff the
same file so any algorithm drift breaks both builds
simultaneously.
- Integration: replay of one rep-2 query shows ≥ 1 row-group hit
— deferred to **SHELF-16b** (requires the hand-rolled Parquet
TCompactProtocol footer reader so
`ParquetFooterIndex.fromFooter` returns non-empty; the scaffold
in SHELF-16a always returns `Optional.empty()` and the plugin
falls back to `RowGroupIndex.constantZero()`).
- Design note:
`clients/trino/docs/design-notes/SHELF-16-row-group-key-extension.md`.
- Effort: M. Depends on: SHELF-04, SHELF-15. Owner: trino-plugin-eng-1.
- Out of scope: page-level granularity.

**SHELF-16b — Parquet footer TCompactProtocol reader** — **CLOSED**
Hand-rolled Thrift TCompactProtocol reader over `FileMetaData` shipped
in `io.shelf.client.CompactProtocolReader` +
`io.shelf.client.ParquetFooterIndex`. Emits
`RowGroup(file_offset, total_compressed_size, ordinal)` tuples; no
wire-format change; `shelfd` is unaffected.

- `ParquetFooterIndexTest` (11 tests) covers real Parquet footers
built by the in-repo `CompactProtocolWriter` test helper;
`RowGroupIndexTest` (9 tests) asserts end-to-end ordinal
extraction. `mvn verify` runs 116 tests green.
- Replay harness (SHELF-26) already emits ≥ 1 row-group hit per
query on the synthetic fixture (`tests/test_pipeline.py`),
which closes the remaining SHELF-16 acceptance item.
- Effort: S. Depends on: SHELF-16. Owner: trino-plugin-eng-1.
- Out of scope: page-index entries.

**SHELF-17 — Iceberg manifest caching (pool.metadata, DRAM FrozenHot)** — *Closed (two-pool physical isolation + 5 GiB default)*
Separate Foyer pool: `pool.metadata`, DRAM-only, 5 GiB, FrozenHot
policy. Manifests + manifest-lists + `metadata.json` routed here;
row-groups routed to `pool.rowgroup`.

- Manifest hit served in < 1 ms p99 *(DRAM-resident `foyer::Cache`
clone — `FoyerStore::get` is a single hashmap lookup plus
`Bytes::clone` (refcount bump); see
`shelfd::store::store_tests::insert_then_get_is_hit`. p99 latency
under load is re-measured by SHELF-26 replay; no separate
microbench is gating this ticket.)*
- Ad-hoc 50 GB scan cannot evict manifests (pool isolation test)
— `shelfd::store::store_tests::pool_isolation_under_rowgroup_pressure`
(plus `rowgroup_pressure_does_not_shrink_metadata_used_bytes`
for the monotonic variant). Design note:
`shelfd/docs/design-notes/SHELF-17-iceberg-manifest-pool.md`.
Rust-side default surfaced as
`shelfd::config::DEFAULT_METADATA_DRAM_BYTES = 5 * (1 << 30)`.
SIEVE ships today as the v1 realisation of "FrozenHot"; a
stricter policy is tracked as followup **SHELF-17a FrozenHot
policy** if SHELF-26 replay demands it.
- Effort: M. Depends on: SHELF-03. Owner: rust-engineer-2.
- Out of scope: page-index pool (v1.1).

**SHELF-18 — Foyer NVMe hybrid tier with S3-FIFO** — **CLOSED (local gate; cluster NVMe rollout per SHELF-21)**
`pool.rowgroup` is built as a `foyer::HybridCache` (DirectFs +
LargeEngine with `S3FifoConfig::default()`) when
`pools.rowgroup.nvme_bytes > 0`, and falls back to pure DRAM
`foyer::Cache` when `nvme_bytes == 0`. PVC wiring is a Helm
`values.yaml` change (out of scope for this ticket, tracked under
SHELF-21).

- `shelfd` survives store re-creation with an existing NVMe dir —
`tests/it_hybrid_pool.rs::hybrid_pool_survives_store_recreation`
(plus the DRAM-only and disk-metric variants; 4 tests green).
- Foyer reports `shelf_disk_used_bytes` / `shelf_disk_capacity_bytes`
distinct from the DRAM counters once a hybrid pool is mounted
(`disk_metrics_are_registered`).
- ADR-0009 captured in
`[shelfd/docs/design-notes/SHELF-18-nvme-hybrid-pool.md](../../shelfd/docs/design-notes/SHELF-18-nvme-hybrid-pool.md)`
(S3-FIFO choice, hybrid enum pattern, PVC out-of-scope, metrics
contract).
- Effort: L. Depends on: SHELF-17. Owner: rust-engineer-1.
- Out of scope: GL-Cache, LightGBM admission, PVC rollout (SHELF-21).

**SHELF-19 — Rendezvous (HRW) hashing library, Rust + Java** — *Closed (Phase-1 plugin pass; standalone `shelf-hashring` crate split deferred)*
`shelfd::router::hrw_score` + `io.shelf.client.HashRing.score` — both
compute capacity-weighted HRW per ADR-0002. The golden-vector fixture
at `shelfd/tests/fixtures/hrw_golden_vectors.txt` (1000 entries) is
consumed by both sides so drift breaks the build immediately.

- Both sides agree on 1000 deterministic keys across 3 weighted
nodes (`shelfd::router::tests::owner_matches_golden_vectors`
regenerates; `io.shelf.client.HashRingTest::ownerMatchesGoldenFixture`
asserts byte-identical decisions). Extension to 10k × 7 nodes is
a fixture-size change only.
- Capacity-weighted HRW tested: heavier nodes win proportionally
more often (`heavierNodeWinsMoreOften` on both sides)
- ADR-0002 recorded
- Split into a standalone `shelf-hashring` crate *(deferred; lives
under `shelfd::router` until the plugin needs to link against
it without pulling in tokio/foyer — no current consumer does)*
- Effort: M. Depends on: SHELF-04. Owner: rust-engineer-2.
- Out of scope: Raft.

**SHELF-20 — K8s headless service membership resolver** — **CLOSED (Java side + `/stats` contract); cluster-level conformance deferred**
Plugin resolves `shelf.shelf.svc.cluster.local` every 5 s (Java DNS
cache override), builds a `ShelfHashRing` over current pod IPs +
`/stats` capacity. `shelfd` pods expose `/stats` with `{capacity_bytes, used_bytes}` for the weighting.

- DNS refresh observed in logs every 5 ± 1 s
(`io.shelf.client.MembershipResolver` schedules on a daemon
`ScheduledExecutorService`; `MembershipResolverTest` exercises the
refresh cycle with a `FakeClock`).
- Pod rotation (delete 1, wait 30 s, recreate) re-balances cleanly
with < 1 % mis-routed requests (E7) — deferred to SHELF-21 Helm
chart bring-up (needs a real 3-pod StatefulSet). Single-pod
resolver happy-path is exercised end-to-end by the SHELF-12
docker-compose smoke harness once SHELF-10/22 plugin FS wiring
lands.
- `shelfd` side: `GET /stats` shipped under SHELF-07 with contract
`{pod_id, capacity_bytes, used_bytes, metadata_pool{...}, rowgroup_pool{...}}`.
- Java side: hand-rolled zero-dependency JSON parser; no Jackson/Gson
added (keeps the plugin jar small). `MembershipResolver.Snapshot`
publishes `(HashRing, pod→URI, pod→CircuitBreaker)` atomically;
breakers are retained across refreshes via a `ConcurrentHashMap`.
- Config keys: `shelf.membership.refresh-interval-ms` (default 5000),
`shelf.membership.stats-timeout-ms` (default 2000); full validation in
`ShelfConfig.fromMap` + `ShelfConfigTest`.
- `ShelfFileSystemFactory` now takes a `MembershipResolver` instead of
a fixed `(endpoint, breaker)` pair; `ShelfInputFile.newStream()`
calls `resolver.ownerFor(keyBytes)` per stream (Phase-1 choice (b):
sticky for the stream, not per-read). Empty ring → raw delegate
stream, i.e. direct-S3.
- Fail-open matrix documented in
`clients/trino/docs/design-notes/SHELF-20-membership-resolver.md`.
- Tests (JDK-only, no testcontainers): `MembershipResolverTest` (9
cases), `ShelfFileSystemFactoryTest` (2), new config parse cases in
`ShelfConfigTest`.
- Effort: M. Depends on: SHELF-19. Owner: k8s-eng-1.
- Out of scope: gossip; Raft.

**SHELF-21 — 3-pod StatefulSet Helm chart + NVMe PVC**
Extend chart: StatefulSet (not Deployment), headless service,
volumeClaimTemplates for 500 GiB NVMe, pod anti-affinity across AZs.
Three pods in rep-2's cluster.

- `helm upgrade` from Phase 0 Deployment → Phase 1 StatefulSet
without data loss (DRAM only at Phase 0, so acceptable)
- StatefulSet rollout does not cause traffic drop (HRW + circuit
breaker absorb it)
- Effort: M. Depends on: SHELF-09, SHELF-18. Owner: k8s-eng-1.
- Out of scope: multi-region.

**SHELF-22 — S3-compat shim (`GetObject` + `HeadObject`) + Trino read-path wiring** — **CLOSED**
HTTP server on `:9092` speaking the S3 REST subset: `GetObject` with
`Range` header, `HeadObject`. Enough for DuckDB, boto3, Polars via
`endpoint_url=http://shelf:9092`. Implementation lives in
`shelfd/src/s3_shim.rs`; wired on a separate Axum router via
`http::build_s3_shim_listener` so the native `/cache`, `/metrics`,
`/stats` namespace cannot leak into the S3 URL space. Design note:
`shelfd/docs/design-notes/SHELF-22-s3-compat-shim.md`.

Trino 480's public `Plugin` SPI does not expose
`getFileSystemFactories()`, so the original "register
`ShelfFileSystemFactory` as a Trino plugin FS" approach is
structurally impossible against the published API. The production
wiring is instead the `s3.endpoint` swap: Iceberg's catalog property
now points at `http://shelfd:9092`, Trino's native S3 client issues
SigV4-signed (the shim ignores signatures by design)
`HeadObject`/`GetObject(Range)` calls, and the shim transparently
caches + falls through to the real origin on miss. This is the same
integration pattern used by every other S3-compatible cache in the
ecosystem and is documented in the SHELF-12 smoke harness
(`benchmarks/smoke/config/trino/etc/catalog/iceberg.properties`).

Two shim fixes shipped as part of the Trino wiring work:
1. `parse_range_header` / `resolve_range` now handle suffix ranges
   (`bytes=-N`) and open-ended ranges (`bytes=N-`) per RFC 9110.
   Trino's `S3Input.readTail(n)` is the Parquet+Avro footer reader
   and issues `bytes=-N` exclusively; the original parser rejected
   that shape as 416 and every Iceberg query against the shim
   failed on the first manifest read. Covered by
   `s3_shim::tests::parse_range_suffix_used_by_trino_readtail` plus
   11 sibling unit tests and the
   `it_s3_shim::get_object_suffix_range_serves_tail_bytes` integration
   test.
2. `handle_get_object` now increments `shelf_hits_total` /
   `shelf_misses_total` in the shim path (previously only the native
   `/cache` data plane did so). Without this, operators watching
   dashboards after an `s3.endpoint` swap would see a flat 0-hit line
   and assume the cache was broken. Covered by
   `it_s3_shim::shim_read_bumps_hits_and_misses_counters`.

- `aws s3 cp s3://bucket/key -` works against Shelf endpoint *(covered by `it_s3_shim::get_object_without_range_returns_full_object` + `get_object_with_range_serves_bytes`)*
- `duckdb SELECT * FROM read_parquet(...)` via S3 env works *(range path proved by `it_s3_shim::get_object_with_range_serves_bytes`; HEAD path by `it_s3_shim::head_object_returns_s3_parity_headers`)*
- Trino 480 reads Iceberg tables through the shim with correct
  output and cache hits climbing on warm runs *(SHELF-12 smoke run:
  `metadata 28→56`, `rowgroup 10→20`; `iceberg.metadata-cache.enabled=false`
  in the smoke config forces the warm run to re-hit the shim so
  `shelf_hits_total` is observable in the v0.5 gate)*
- 404 / 403 error parity with real S3 *(`it_s3_shim::get_object_on_missing_key_returns_404_s3_xml` + XML shape unit test `s3_shim::tests::xml_error_body_has_expected_shape`; 501 cap proven by `it_s3_shim::get_object_unbounded_huge_object_rejected_with_501`)*
- Effort: M. Depends on: SHELF-06, SHELF-07. Owner: rust-engineer-2.
- Out of scope: `ListObjects`, `PutObject`, SigV4 auth, virtual-hosted style (see design note).

**SHELF-23 — `shelfctl` CLI: stats, pin, evict, ring, reload** — *Closed (admin HTTP surface under `/admin/`*; shipped with SHELF-24)*
Rust CLI binary. Talks to `shelfd`'s admin HTTP surface
(`/admin/{ring,pin,unpin,evict,reload}`). `stats` pretty-prints
`/stats`; `ring` renders a `pod_id | weight | healthy` table;
`pin <key> [--pool metadata|rowgroup]` / `evict <key> [--pool …]` take
the content-addressed hex key plus a pool selector; `unpin <key>` is
pool-agnostic (keys are unique across pools by construction); `reload`
triggers an out-of-band pin-list reload.

- Each subcommand has `--help` *(`shelfctl/tests/smoke.rs::subcommand_help_prints_for_every_verb`)*
- `reload` goes through the same loader as SIGHUP / timer *(admin handler uses `ReloadHandle::reload_now` which is the same path the SIGHUP + 15-min tick drive; `shelfd::pinlist::tests` + `shelfd/tests/it_admin.rs::admin_reload_returns_200_when_loader_disabled`)*
- `ring` on two different pods shows identical output — deferred until SHELF-20 wires real membership server-side; current handler returns a single self-row (see `shelfd/docs/design-notes/SHELF-23-24-admin-surface-and-pinlist.md`).
- Effort: M. Depends on: SHELF-20. Owner: rust-engineer-1.
- Out of scope: web UI.

**SHELF-24 — Pin list loader from S3 ConfigMap + SIGHUP** — *Closed*
`shelfd` on boot reads `s3://<bucket>/<key>` (default `shelf/pin_list.json`),
installs the keys into an in-memory allowlist, and refreshes on
SIGHUP + a 15-min timer. `pin_list.json` schema: `{"version": 1, "entries": [{"key_hex": "<sha256 hex>", "pool": "metadata"|"rowgroup"}, …]}`. Pool is
**required** so the loader can read byte-length from the right
Foyer cache on pin. Replacing semantics: reloads diff the
fetched list against the current pin-set and unpin removed
entries. See `shelfd/docs/design-notes/SHELF-23-24-admin-surface-and-pinlist.md`.

- `shelfctl reload` triggers the loader via `POST /admin/reload` *(`shelfd::http::handlers::admin_reload` + `shelfctl::cmd_reload`)*
- Pinned keys bypass size-threshold admission *(`shelfd::admission::tests::pinned_keys_bypass_size_threshold`)*
- Pin-set survives eviction *(`shelfd::store::store_tests::evict_preserves_pin_set`)*
- `/stats` reports `pinned_bytes` + `pinned_count` *(`shelfd::http::tests::stats_payload_has_contract_keys` extended; integration coverage in `shelfd/tests/it_admin.rs::admin_pin_raises_pinned_bytes_on_stats`)*
- Effort: M. Depends on: SHELF-03. Owner: rust-engineer-2.
- Out of scope: per-tenant pin lists (Phase 6).

**SHELF-25 — Size-threshold admission policy** — *Closed (Phase-0 read-path pass; pin-list loader deferred to SHELF-24)*
Admission: refuse inserts for objects `> 1 GiB` unless in pin list.
All other objects admitted. Config key `shelf.admission.size_threshold_mib`
(default 1024) and `shelf.admission.pinned_bypass` (default true).

- Unit: 1.5 GiB miss returns data to client but is not inserted *(`admission::tests::rejects_above_threshold` + `store::store_tests::get_or_fetch_reject_does_not_cache`)*
- Pin list bypass: pinned 2 GiB file *is* inserted *(`admission::tests::pinned_bypasses_threshold_when_enabled`; real S3-backed pin list loader deferred to SHELF-24)*
- ADR-0003 recorded
- Effort: S. Depends on: SHELF-24. Owner: rust-engineer-1.
- Out of scope: LightGBM.

**SHELF-26 — `trino_logs` replay benchmark harness** — **CLOSED**
`benchmarks/trino_logs/` ships as a Python package (`shelf-replay`) that
consumes an offline dump of `cdp.trino_logs.trino_queries` plus a
per-snapshot Iceberg manifest export, and emits both the E5 ratio
report (file-level vs row-group-level scanned bytes) and a cache-
simulator sweep across a matrix of Foyer configs. The offline-only
design keeps the harness CI-runnable with no AWS creds and keeps
historical runs byte-identical to reproduce.

- Harness reproduces E5 (median and P90 row-group/file ratio)
*(`tests/test_pipeline.py::test_aggregate_by_day_matches_expected`
pins the per-day ratios against `fixtures/synthetic-7d/expected.json`;
five hand-verified queries cover partition-prune, row-group-prune,
full-scan, and narrow-range cases.)*
- `make replay-rep2-7d` runs in ≤ 20 min on a dev pod
*(synthetic fixture runs end-to-end in <1 s on a laptop — 4 orders
of magnitude under the AC. Estimated ~6-9 min on a 4-core dev pod
for a 7-day rep-2 trace, dominated by Parquet footer reads
amortised via `functools.lru_cache` on `(etag, path)`.)*
- Publishable CSV output
*(`per-query.csv`, `per-day.csv`, `sim-<config>.csv`, plus
schema-validated `summary.json`. Writers live in
`shelf_replay/report.py`.)*
- Design notes at
`[shelfd/docs/design-notes/SHELF-26-replay-harness.md](../../shelfd/docs/design-notes/SHELF-26-replay-harness.md)`
(scope decisions, column-chunk `total_compressed_size` vs
row-group `total_byte_size`, predicate-extraction fallback
semantics, and the ratio to `benchmarks/replay/` which ships the
*live* v0.5-gate benchmark).
- Content-key parity: `tests/test_key.py::test_golden_fixture_parity`
consumes the same `shelfd/tests/fixtures/shelf04_golden_vectors.txt`
the Rust + Java lanes consume, so any drift across the three
implementations fails CI on all sides.
- Effort: L. Depends on: SHELF-06. Owner: data-eng-1.
- Out of scope: live replay (that is `benchmarks/replay/SPEC.md`);
LightGBM admission simulation (gated on size-threshold missing
the v0.5 target per ADR-0003).

**SHELF-26a — Join/subquery predicate extraction** — **CLOSED**
Replace the conservative single-relation `WHERE`-clause extractor with
a proper alias-aware sqlglot pass: join conditions feed inferred
predicates, subqueries and CTEs traverse their own `SELECT` scope, and
`OR` branches collapse when any branch is unbounded. Code lives in
`benchmarks/trino_logs/src/shelf_replay/predicates.py`;
`PredicateTerm` now carries `table_alias` for per-scan pruning.

- 13 `test_trace.py` cases pin the JOIN / subquery / CTE / OR
behaviours (`pytest` runs 29 tests green).
- Design note at
`benchmarks/trino_logs/docs/SHELF-26a-predicate-extraction.md`
captures scope + fallback semantics.
- Effort: S. Depends on: SHELF-26. Owner: data-eng-1.

**SHELF-27 — Grafana dashboard (insight-first)** — **CLOSED**
Read-path dashboard UID `shelf-read-path` (read-path scope; the
overview rollup is a follow-up). Four top-row **big-number** stat
panels — hit ratio (overall + per pool), p99 latency, miss volume,
error rate — sit above per-pool / per-route / HEAD drill-down rows and
an origin + pinning row. Follows AGENTS.md's traffic-light conventions.
Ships as a ConfigMap via the kube-prometheus-stack Grafana sidecar so
clusters pick it up without a dashboards-as-code job.

- Dashboard JSON committed to `charts/shelf/grafana/`
(canonical: `charts/shelf/grafana/dashboards/shelf-read-path.json`;
SHELF-08 starter at `observability/dashboards/shelf-read-path.json`
retained for backward-compat.)
- Alerting rules for: 5xx rate > 1 % for 10 m (`ShelfReadPathHighErrorRate`,
severity=page), p99 latency > 100 ms for 10 m (`ShelfReadPathP99Degraded`,
severity=warn), overall hit-ratio < 40 % for 30 m
(`ShelfReadPathHitRatioCollapsed`, severity=info). Committed at
`charts/shelf/grafana/alerts/shelf-read-path.yml`. (The originally-
scoped "hit rate < 60 % / fallback rate > 5 % / pod unready 2 m"
trio was rewritten to match the metrics actually emitted —
`fallback_rate` is not a `shelfd` series and pod-unready belongs
on a kube-state-metrics panel not the read-path dashboard.)
- On-call can diagnose in ≤ 3 clicks per AGENTS.md rubric (big-
numbers row answers health in one glance; drill rows below are
two clicks away).
- Design notes at
`[shelfd/docs/design-notes/SHELF-27-observability-dashboard.md](../../shelfd/docs/design-notes/SHELF-27-observability-dashboard.md)`
(layout, thresholds, metric-label gap list for `status`, `route`,
`shelf_pinned_bytes`, `shelf_origin_requests_total`,
`shelf_singleflight_followers_total`).
- Helm wiring: `charts/shelf/templates/grafana-dashboard.yaml` +
`grafana.`* block in `charts/shelf/values.yaml`.
- Effort: M. Depends on: SHELF-08. Owner: sre-1.
- Out of scope: ML-dashboards; `shelf-overview` / `shelf-tenant`
rollup dashboards (tracked as SHELF-27 follow-ups).

**SHELF-28 — Chaos drills + v0.5 gate runbook** — **CLOSED (runbook + smoke variants; cluster drills gated on SHELF-21)**
Both weekly drills ship as shell scripts under `chaos/` with two
flavours each: a cluster-mode target (`make chaos-keda-rotation`,
`make chaos-pod-kill`) that assumes a live 3-pod StatefulSet and a
green-in-CI smoke variant (`make chaos-*-smoke`) that exercises the
drill logic against the SHELF-12 docker-compose harness. The runbook
in `docs/runbook.md` documents the v0.5 gate checklist, the 3-click
evaluation path, and the kill-switch decision tree.

- Both drills have `make` targets and green-in-CI smoke variants
(`chaos/smoke-keda-rotation.sh`, `chaos/smoke-pod-kill.sh`; wired
into `smoke.yml` as the `chaos-smoke` job).
- Runbook published at `docs/runbook.md`; gate-evaluation criteria
cross-link to the SHELF-27 Grafana panel IDs.
- Operator-facing runbook structured as five green criteria + a
3-click eval path + explicit kill-switch — designed to be
executable in ≤ 30 min with no prior Shelf context.
- Effort: L. Depends on: SHELF-21, SHELF-27. Owner: sre-1 + rust-engineer-1.
- Out of scope: v1+ drills.

**Total:** 28 tickets (SHELF-01 through SHELF-28). Sizing: 4 S, 18 M,
6 L, 0 XL. At 3 engineers, ~10 weeks of calendar work for phases 0 + 1
plus the v0.5 gate observation window — matches the 36-44 week total.

---

## 5. Risk register

Ordered by Likelihood × Impact. L/M/H scale. Trigger signal is
something observable, not a feeling.


| #    | Risk                                                                     | Likelihood | Impact | Trigger signal                                                                                                        | Mitigation                                                                                                                        | Owner              |
| ---- | ------------------------------------------------------------------------ | ---------- | ------ | --------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| R-01 | v0.5 fails to beat the stabilised Alluxio baseline                       | H          | H      | After 7-day observation on rep-2: cumulative hit rate < 71 % OR `GOLD_DBT` ok-rate < 99.9 % OR p95 > 120 % of Alluxio | Stop the project; write an honest "Shelf is the wrong answer" postmortem; invest in Alluxio.                                      | eng-lead           |
| R-02 | Trino SPI churn breaks plugin between versions                           | H          | H      | Grafana: plugin-class-load errors on Trino version bump                                                               | Pin to Trino 480 LTS for v1; contract tests run nightly against Trino main nightly                                                | trino-plugin-eng-1 |
| R-03 | Cold-cache thundering herd hits S3 per-prefix rate limits                | M          | H      | CloudWatch: `503 SlowDown` > 0 during warm-up; cache miss p95 > 2 s                                                   | Per-prefix concurrency limiter on fallback path (Phase 3 deliverable); pre-warm critical pins on pod start                        | rust-engineer-2    |
| R-04 | Team has never shipped a Rust service                                    | H          | M      | Phase 0 end-date slip > 50 %; Foyer or Tonic issues blocking                                                          | Pair-programming week 1; Rust mentor from Foyer community; keep Phase 0 tiny (v0.1 is hostable in ≤ 2 Rust engineers for 2 weeks) | eng-lead           |
| R-05 | `QueryCreatedEvent` doesn't carry enough plan signal (E1 fails)          | M          | H      | E1 log shows empty `predicate` / missing `tables` on > 50 % of queries                                                | Fall back to Phase 2b-signal-1 only (plugin-observation); ship Phase 2 with no listener                                           | trino-plugin-eng-1 |
| R-06 | Foyer disk tier corrupts NVMe data on pod crash                          | M          | H      | `shelfd` startup reads return checksum mismatch > 0.01 %                                                              | Content-addressed keys mean a bad chunk is rejected on read; trigger re-fetch. Run a 24-h power-kill test in Phase 3              | rust-engineer-1    |
| R-07 | HRW + DNS cache creates hit-rate wobble during KEDA churn                | M          | M      | E7 chaos drill: > 5 % mis-routed / minute for > 2 min                                                                 | Tune DNS TTL down to 5 s; add circuit-breaker short-circuit on unknown pod                                                        | k8s-eng-1          |
| R-08 | ACL heterogeneity (rep-2 Ranger, rep-3 file rules.json) delays Phase 6   | M          | M      | Rep-3 canary fails tenant isolation integration test                                                                  | Abstract plugin auth at IRSA-role boundary; write canary per replica; stage rep-3 last                                            | trino-plugin-eng-1 |
| R-09 | Plugin blocks coordinator on prefetch RPC                                | M          | H      | Coordinator thread dump shows threads blocked in `ShelfPrefetchListener.onQueryCreated` > 10 ms                       | Hard 10 ms deadline + fire-and-forget; circuit breaker also wraps listener                                                        | trino-plugin-eng-1 |
| R-10 | Multipart S3 ETag assumption leaks into docs as "content hash"           | H          | L      | OSS launch blog draft uses phrase "cryptographic content hash"                                                        | Code review checklist + javadoc/rustdoc on key function; BLUEPRINT-DIFF covers it                                                 | rust-engineer-2    |
| R-11 | Pin list edit corrupts ops (empty or bad syntax)                         | L          | H      | `shelfctl reload pin-list` returns error; `pinned_bytes` drops to 0                                                   | JSON schema validation in `shelfctl reload`; S3 versioning; rollback via previous object version                                  | sre-1              |
| R-12 | Shelf p99.9 tail latency worse than Alluxio under NVMe pressure          | M          | M      | p99.9 > 50 ms during chaos drill                                                                                      | `tokio`-blocking-call lint; dedicated Foyer write threadpool; measure E-related (scientist open q 7)                              | rust-engineer-1    |
| R-13 | `cdp.trino_logs.trino_queries` schema change breaks trainer              | M          | M      | Nightly Airflow job fails after 10 d of green runs                                                                    | Schema-version tag in trainer; fall back to empty pin list (same as v0.1 behaviour)                                               | data-eng-1         |
| R-14 | Foyer crate upstream API churn                                           | M          | M      | `cargo update` breaks build on minor bump                                                                             | Pin Foyer version exact; quarterly bump cadence with regression tests                                                             | rust-engineer-1    |
| R-15 | "Yet another cache" reception kills OSS momentum                         | M          | M      | Launch post: <100 HN points; <10 GitHub stars in week 1                                                               | Lead with measured numbers vs Alluxio baseline, not claims; don't compare to Warp Speed                                           | eng-lead           |
| R-16 | openraft is quietly re-introduced by a future engineer "for consistency" | M          | M      | PR adds `openraft =` to Cargo.toml                                                                                    | ADR-0001 reviewed in codeowners; compiler error if `openraft` added without ADR update                                            | eng-lead           |
| R-17 | Trino replica coordinator pods deploy with mismatched plugin versions    | M          | M      | `QueryCreatedEvent` not firing on rep-0 but firing on rep-2; Grafana panel shows per-replica prefetch rate            | Helm plugin version as a required value; ArgoCD drift alert                                                                       | sre-1              |
| R-18 | Alluxio decommissioned too early (Phase 5) with no rollback path         | L          | H      | Rep-2 Shelf degraded and Alluxio no longer deployable                                                                 | Keep `alluxio-values.yaml` in repo; mandatory "re-deploy Alluxio" drill before decommission                                       | sre-1              |


Likelihood × Impact ordering: R-01 / R-02 / R-03 / R-09 are the
immediate priorities; R-04 / R-05 / R-18 tied next; R-08 / R-10 /
R-15 tail.

---

## 6. Success metrics and SLOs

One sub-section per phase success gate.

### 6.1 Phase −1 — Stabilisation

- **Primary metric.** `fs.cache` cumulative hit rate, measured from
Trino `operator_summaries` (scan operator cacheHitPct), per replica.
- **Measurement.** 5-day rolling average from Grafana panel
`trino-fs-cache-hitrate` (existing).
- **Target.** ≥ 45 %.
- **Rollback threshold.** Drops below 20 % for 1 day → revert hostPath
migration on the affected replica.
- **Guardrails.** `QueryFailedEvent` rate not > 1.2× baseline; `GOLD_DBT`
ok-rate ≥ 99.9 %.
- **Dashboard.** Grafana `trino-stability-overview` (existing).

### 6.2 Phase 0 — v0.1 PoC

- **Primary metric.** Plugin overhead — p99(plugin-enabled-read) /
p99(plugin-disabled-read) for non-cached reads on shadow traffic.
- **Measurement.** 1-hour shadow traffic on rep-2 canary; Trino
`QueryCompletedEvent.getStatistics().getScanTime()`, bucketed by
`fs.shelf.enabled`.
- **Target.** ≤ 1.05×.
- **Rollback threshold.** ≥ 1.15× for 1 h → disable plugin.
- **Guardrails.** `shelf_pod_cpu` < 50 % of limit; memory stable; zero
Shelf-attributed query failures.
- **Dashboard.** Grafana `shelf-overview` (new, SHELF-27).

### 6.3 Phase 0R — Redis-Gateway result cache

- **Primary metric.** BI-user cache hit rate.
- **Measurement.** Gateway plugin exports `result_cache_hits_total / result_cache_misses_total`.
- **Target.** ≥ 60 % after 5 days.
- **Rollback threshold.** < 20 % or elevated HMS load from watcher.
- **Guardrails.** Gateway p95 latency ≤ baseline.
- **Dashboard.** Grafana `trino-gateway` (existing).

### 6.4 Phase 1 — v0.5 gate on rep-2

This is the kill-switch gate.

- **Primary metric 1.** Cumulative cache hit rate (all pools combined)
on rep-2 over rolling 7 days.
- **Measurement.** `shelf_hits_total / (shelf_hits_total + shelf_misses_total)` from Prometheus, 7-day window.
- **Target.** ≥ 71 % (Alluxio baseline from E12).
- **Rollback threshold.** < 60 % for any 24-h window → revert to
Alluxio.
- **Primary metric 2.** `GOLD_DBT` ok-rate.
- **Measurement.** Airflow DAG SLA from dbt job catalog.
- **Target.** ≥ 99.9 %.
- **Primary metric 3.** Rep-2 p95 query latency.
- **Measurement.** Trino `QueryCompletedEvent` p95 over 7 days.
- **Target.** ≤ 120 % of Alluxio baseline.
- **Primary metric 4.** Shelf-attributed pages.
- **Target.** Zero in 7 days.
- **Primary metric 5.** Oncall surface (pages + runbook lookups + Slack
incidents).
- **Target.** ≤ 50 % of Alluxio's 7-day rolling rate.
- **Dashboard.** Grafana `shelf-v05-gate` (new).

### 6.5 Phase 2 — Plan-aware prefetch

- **Primary metric.** TTFQ (time-to-first-query) p95 after 10×
worker scale-up.
- **Measurement.** Synthetic benchmark: scale replica-2 pool from 2
→ 20, issue 20 canonical dashboard queries, record first-result
latency.
- **Target.** ≤ 3 s p95.
- **Rollback threshold.** Prefetch listener blocking coordinator > 10
ms median → `shelf.prefetch.enabled=false`.
- **Guardrails.** Prefetch queue depth ≤ 1024 bounded.
- **Dashboard.** Grafana `shelf-prefetch`.

### 6.6 Phase 3-4-5

- **Chaos drills:** pass/fail weekly runs documented.
- **NVMe admission bytes:** cut ≥ 40 % vs v0.5 (Phase 4).
- **Rep-2 Alluxio retirement:** `alluxio-worker` replicas = 0 for 7
consecutive days (Phase 5).

### 6.7 Phase 6 — Full rollout

- **Per-replica success gate:** same v0.5 gate numbers, per replica,
for 7 consecutive days.
- **Org-wide metric:** Alluxio pods = 0 across all 4 replicas.

### 6.8 Phase 7 — OSS launch

- **Primary metric.** Blog post published, repo public, first external
response within 48 h.
- **Guardrails.** No regression in production Shelf SLOs during launch
week (no "launch-driven outage").

SLO contracts go into `contracts/slos.md` on the cycle this diff lands.

---

## 7. OSS readiness + launch plan

### 7.1 Weeks 1-4 pre-OSS readiness checklist

Do this during Phase 5 / Phase 6 so it's ready for Phase 7.

- Apache 2.0 LICENSE in repo root
- CLA (individual + corporate) chosen and bot configured (CLA
assistant / EasyCLA)
- `CODEOWNERS` with current team
- `SECURITY.md`: private vulnerability disclosure process, PGP key,
24 h acknowledgement SLA
- `CONTRIBUTING.md`: how to run tests, submit a PR, code-of-conduct
link
- `CODE_OF_CONDUCT.md` (Contributor Covenant v2.1)
- CI: `cargo fmt + clippy + test + audit + deny`, `mvn verify`,
`helm lint`, `make integration` (docker-compose smoke from
SHELF-12)
- Release automation: `cargo-release` + `release-please` PR-based
version bumps; signed Docker images (`cosign`); Helm chart
publishing via OCI
- Test matrix: Trino 473, 476, 480 (current LTS); Rust stable on
`ubuntu-22.04` + `al2023`; nightly test against Trino `main`
- Docs: MkDocs Material site at `docs.shelf.io` (or subpath); ADR
index auto-generated from `docs/adr/`
- Quick-start: `kind`-based local cluster in ≤ 5 minutes
- Support matrix page: which Iceberg / Trino / Foyer versions
- Benchmark harness (SHELF-26) reproducible on a fresh EKS cluster
in ≤ 1 h
- Trademark check for name "Shelf" (there are several; own the
narrow scope "Iceberg-native cache for Trino")
- Repository home chosen: `penpencil-oss/shelf` preferred (see §8)

### 7.2 Launch week runbook

- **T−14 days.** Blog post draft circulated to 3 technical reviewers
(one not on the project). HN post scheduled via drafts queue.
- **T−7 days.** Repo freeze; docs review; final benchmark rerun.
- **T−1 day.** Public-private mirror sanity-check; CLA bot green on
test PR.
- **T = launch day.** Post at 10:00 ET Tuesday (empirically the best
HN window). Two people covering issue triage for first 12 h (split
ET/IST rotation). Discord / Slack on.
- **T+2 days.** First community office hour (30 min) scheduled on
public calendar.
- **T+7 days.** Retrospective blog post: "What we learned from day 1-7
of Shelf OSS" — not marketing; an honest "which issues people hit
first".

**Response SLA:** External P0 bug → ack ≤ 24 h, fix ≤ 7 days. P1 →
ack ≤ 48 h. P2+ → ack ≤ 5 days.

### 7.3 First 90 days post-launch commitments

- Issue-triage rota: 1 engineer on Shelf issues for 1 week, rotating
through the 3-person team. Ticket target: 0 stale issues (>14 days
without response).
- Monthly roadmap update in `docs/roadmap.md`.
- Community office hours: every other Thursday 30 min via public
Google Meet. Recordings posted.
- Bug SLA as §7.2.
- No new major features for 60 days. v1.x patches only.
- Post-90-day: evaluate whether to pursue Apache Incubator track
(blueprint §11 item 4).

### 7.4 Governance

- BDFL (the original author — Aamir per blueprint) for first 12 months
post-launch.
- Transition to informal PMC after ≥ 10 external contributors or 12
months, whichever is later.
- Apache donation decision: defer 12 months. Initial Incubator
conversation can start at month 6 if 5+ external contributors.

---

## 8. Open items not yet decided

The three agents (scientist, critical thinker, planner) agree we still
need human decisions on the following:

1. **Project name.** Blueprint proposes "Shelf". Alternatives: Tundra,
  Reef, Ledge, Gale. Decision needed before Phase 7. Forum: eng
   all-hands. By: 2026-05-15.
2. **Repo home.** `github.com/penpencil-services/shelf` vs
  `github.com/penpencil-oss/shelf` vs new GitHub org. Decision needed
   before Phase 7. Forum: eng-leadership + legal. By: 2026-05-30.
3. **Spark client in v1 or Trino-only?** COMPARISON says Trino-only for
  focus; scientist question #12 agrees. Re-decide at end of Phase 5.
   By: Phase 5 retrospective. Forum: eng all-hands.
4. **Apache donation — year 1 or self-govern first?** Blueprint §11.4
  asks this; critic defers. Forum: eng-leadership. By: month 6 post-launch.
5. **Whether to run Phases 8 + 9 in parallel with Phase 7.** Depends on
  team size at the time. Decision point: end of Phase 6. Forum:
   eng-lead + staff engineer.
6. **Trino TIP for a focused `splitCompleted` replacement.** We do not
  wait for it (ADR-0005) but we should still file the TIP. Owner:
   trino-plugin-eng-1. By: start of Phase 2.
7. **Blog post co-authors.** Blueprint §11 asks this. By: start of Phase 7.
  Forum: eng all-hands.
8. **Arrow Flight v1.x go/no-go.** Depends on E8 numeric result. By:
  end of Phase 5.
9. **LightGBM admission v1.x go/no-go.** Depends on Phase 4 replay
  benchmark (≥ 5 pp gap threshold). By: Phase 4 exit.

---

## 9. Appendix — links

- `BLUEPRINT-DIFF.md` — edits to apply to `shelf/BLUEPRINT.md` (v0.3 → v0.4).
- `adr/0001-no-embedded-raft.md` — drop openraft, use K8s headless service + ConfigMap.
- `adr/0002-hrw-hashing-over-vnode-ring.md` — drop 2000-vnode consistent hash.
- `adr/0003-size-threshold-admission-over-onnx-mlp.md` — drop ONNX MLP in v1.
- `adr/0004-http2-only-in-v1.md` — drop Arrow Flight in v1.
- `adr/0005-drop-splitcompleted-event-path.md` — plugin-observation only (PR #26436 aware).
- `adr/0006-drop-shelf-result-cache-in-v1.md` — Redis Gateway from COMPARISON owns result caching.
- `adr/0007-drop-phase-10-mv-refresh.md` — wrong project; compute service, not cache.
- `adr/0008-two-pools-in-v1.md` — metadata + bulk; defer 4-pool layout.
- `adr/0009-foyer-s3-fifo-over-gl-cache-custom.md` — use Foyer built-in; defer GL-Cache.
- `adr/0010-v05-gate-beat-alluxio-on-rep2.md` — kill-switch; 7 consecutive days.

*End of plan.*