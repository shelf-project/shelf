# Critical engineering review of shelf/BLUEPRINT.md

*Author: agent-2-critical-thinker*
*Date: 2026-04-23*
*Reviewed blueprint version: working copy, v0.3, last edited 2026-04-23*
*Reviewed scientist output: `shelf/agents/out/01-scientist-review.md`, 2026-04-23*

## TL;DR

The blueprint is not unreasonable, but as written it is a 9-10 month
greenfield bet that **rebuilds three things we already own or have just
fixed** (Alluxio's 6-node, 2.4 TiB HA cache; the `UfsIOManager=256`
saturation fix; the metadata-cache / HMS-TTL plumbing), in order to win
three things that are real (columnar granularity, shared cross-replica
cache, plan-aware prefetch) and three things that are speculative (learned
admission, approximate in-cache blooms, MV-aware caching + incremental
refresh).

What I would cut: **embedded Raft** (replace with K8s-native membership

- ConfigMap state), **ONNX learned admission** (replace with
size-threshold + pin-list; LightGBM later if measured gap), **Phase 10
incremental MV refresh** (not a cache — it's a compute service, move it
to a separate project), `**shelf-result-cache` in Phase 1**, **the 6 GB/s
Arrow Flight claim** (EKS will not deliver that).

What I would keep: columnar range granularity, content-addressed keys,
fail-open plugin with circuit breaker, FrozenHot on manifests + footers,
the hybrid HTTP/Flight data plane (but at a measured threshold).

What I would add: a **side-by-side trial against the currently-healthy
Alluxio deployment** on rep-2 as the v0.5 gate — not a TPC-DS flex. If
we cannot demonstrably match Alluxio's current 71% hit rate and its
post-fix stability budget in 2 months with v0.5, the project should be
killed, not iterated.

**Single biggest concern (§8):** the blueprint is being written at the
exact moment Alluxio on rep-2 started working. The team is primed to
over-value "never again an Alluxio incident" and under-value the
operational sunk-cost of a Rust service new to the organisation.

---

## 1. Attack surface

For each major design choice: failure modes, operator, blast radius,
simpler replacement, steady-state cost.

### 1.1 Rust `shelfd` with embedded openraft (§6.1, §6.3)

- **What can go wrong.** openraft is pre-1.0 (its own docs say so). We
have watched the Alluxio master quorum eat us alive once — `POST_MORTEM.md`
documents 50+ `ICEBERG_COMMIT_ERROR`s when one master restarted. An
embedded-Raft data-plane fails the same way: election storm during a
node rotation (KEDA + Karpenter recycle nodes multiple times a day on
this cluster), split-brain if gossip probes race a Raft reconfig,
snapshot-chunking bugs on membership change. Additional Rust-specific
ones: async runtime starvation under NVMe pressure, openraft storage
layer bugs (the crate recommends RocksDB in production, which adds
another dependency and another tuning surface), `tokio` version pinning
conflicts with Foyer / Tonic.
- **Who operates it.** Nobody on our team has shipped a Rust service
before. Alluxio's Raft quorum was visible in `alluxio fsadmin journal quorum info`; openraft's quorum is visible in a crate-specific debug
endpoint we will have to build, document, and train for.
- **Blast radius.** Control-plane Raft loss ≠ data unavailability (ring
keeps serving; blueprint is correct on this). **But:** if a Raft bug
writes a bad ring snapshot to all peers, we lose the mapping from keys
to nodes and the cache becomes unavailable until rollback. This is the
Alluxio "journal wipe reformats mounts" failure mode re-skinned.
- **Simpler replacement.** Ring membership is pulled from the K8s
StatefulSet headless-service endpoints (one DNS lookup every 5 s,
cached). Pin list + tenant quotas live in a versioned ConfigMap that
pods reload on `SIGHUP`. Admin writes go through `kubectl apply`. Zero
new consensus systems. The scientist independently proposed this
(§4.10); I second it.
- **Steady-state cost.** Raft keeps 5 RPC listeners per pod × 5 pods =
25 connections always open, plus periodic heartbeat churn. Non-trivial
but small. Bigger cost is cognitive — every future on-call has to
learn openraft's state transitions.

### 1.2 Consistent-hash ring with 2 000 vnodes (§6.1)

- **What can go wrong.** 2 000 vnodes × N physical nodes is a 10k-20k
entry map that mutates on membership change. A buggy serialiser or a
silent tie-break asymmetry between Rust's and Java's hash
implementations turns "route to owner" into "ask the wrong node",
which looks like a 100% miss rate for a subset of keys — nearly
impossible to diagnose from metrics because hit-rate just drops without
error. We have seen this shape before: Alluxio's client-side affinity
key had to be forced identical across client versions.
- **Who operates it.** Nobody, if it works. If it breaks, ops reads
`shelfctl ring` on two nodes and compares the map. That tool does not
exist yet.
- **Blast radius.** Degraded hit rate on a slice of keys; correctness
preserved (fail-open to S3).
- **Simpler replacement.** Rendezvous (HRW) hashing with capacity
weights. O(N) hash comparisons per lookup for N ≈ 10 nodes = 10 hashes
= sub-µs. No map to maintain, no vnode count to argue about, and the
hash-function contract is "SHA-256 of (key, node-id)" which is
trivially testable across Rust and Java with a golden-vector unit
test. Scientist also raised this (§4.5).
- **Steady-state cost.** Zero. Less memory. Less tuning.

### 1.3 Learned admission via nightly-trained ONNX MLP (§6.1, §7.3)

- **What can go wrong.** Feature drift (user mix changes; we are
onboarding replica-1 bronze-layer in the next month and the feature
distribution for that tenant is different). Model regression after a
`trino_logs` schema change. ONNX Runtime upgrade breaking binary
compat with our exported graph. Training job silently producing an
untrained model that admits nothing or everything. The blueprint says
"fallback to size-threshold if model unavailable" which covers the
file-missing case but not the "model loaded, giving garbage" case.
- **Who operates it.** The data platform team is already the
trainer-oncall for feature store + recommendation models. This is one
more graph to babysit with few tangible users downstream.
- **Blast radius.** Bad admission = worse hit rate. Correctness: zero
risk.
- **Simpler replacement.** Size-threshold + pin-list in v1. In v2, if we
measure an actual hit-rate gap vs ceiling, LightGBM with five features
and a C runtime. Zero ONNX dependency; zero Python at serve time.
Scientist agrees (§4.1, §3.3); I would be more aggressive — **do not
build any learned admission before Phase 5** and only if there is
measured evidence it matters.
- **Steady-state cost.** Training job: 1-2 CPU-hours/night + ongoing
pipeline maintenance. Inference overhead on large-object admission:
negligible. **Operator overhead: multiple hours/week during tuning,
dropping to "periodically fix" steady-state.**

### 1.4 Plan-aware prefetch via `EventListener` (§7.2)

- **What can go wrong.** As the scientist flagged, **Phase 2b-signal-2
is dead**: Trino PR #26436 (merged 2025-08-19) removed
`EventListener#splitCompleted` entirely. The blueprint's risk table
(§13) says "Upstream PR #26425 already enables worker event listeners"
— that is the opposite of what happened. We need to fix this **before
writing a listener**, not before launching.
- Second risk: `QueryCreatedEvent` fires from the coordinator. Our 4
replicas run independent coordinators. Shelf plugin has to be
deployed, configured, and version-matched across **four** coordinator
JVMs — the same operational surface that today means metadata-cache
TTL drifts between replicas.
- Third risk: Trino SPI surface for `TrinoFileSystem` was rewritten in
version ~464 (the Unified File System work). Our plugin is a
`TrinoFileSystem`; if Trino's SPI evolves again before v1 ships (we
are ~10 months out), we will be fighting a moving target. This is
distinct from, but the same shape as, the `SplitCompletedEvent`
problem.
- **Who operates it.** Oncall reads plugin logs (`/var/log/trino/server.log`).
If the plugin misbehaves we have to be able to toggle
`fs.shelf.enabled=false` catalog-side without a redeploy. The
blueprint doesn't explicitly commit to runtime-toggle support.
- **Blast radius.** Bad listener = queries go slower (missed prefetch)
or — worse — coordinator threads block on a hung `Prefetch` gRPC.
That is actually a correctness-adjacent bug because a slow coordinator
eats the whole replica. Fire-and-forget + hard timeout is
**mandatory**; the blueprint mentions it but does not bound it
(§8.2 says `fire-and-forget ok`; ship with hard 10 ms coordinator-side
deadline, not "okay").
- **Simpler replacement.** Ship only plugin-side observation (Phase
2b-signal-1) in v1. Defer anything listener-based to v2 once the
listener-API churn in upstream Trino has settled.
- **Steady-state cost.** One gRPC per query × ~200k queries/day × 4
replicas = ~800k RPC/day, bounded. Fine.

### 1.5 Hybrid HTTP (< 1 MB) + Arrow Flight (≥ 1 MB) data plane (§8.1)

- **What can go wrong.** Two transports = two failure modes = two sets
of pool tuning. Arrow Flight's gRPC layer is sensitive to gRPC version
(Arrow issue #35910 — 10-15% throughput loss on gRPC 1.46 upgrade);
our plugin will pin Java gRPC, our server will pin Rust Tonic, the two
can skew. Keep-alive misconfigurations blow up as cascade failures on
worker scale-up. The HTTP path is easy. The Flight path is not.
- **Blast radius.** Latency regression, not correctness.
- **Simpler replacement.** HTTP/2 range-GET for everything in v1; add
Arrow Flight for row groups only if we can **measure** the IPC framing
cost matters at our scale. On EKS `c6a.4xlarge` we will not saturate
a 10-25 Gbps ENI with one Flight stream, so the "6 GB/s" argument is
moot. The scientist agreed this number is from a Mellanox benchmark
(§1, §2.6, §4.6) — I would strike it from the blueprint entirely.
- **Steady-state cost.** Single-protocol is meaningfully cheaper to
operate and benchmark.

### 1.6 Content-addressed keys + snapshot-tagged metadata (§13.5, §4)

- **What can go wrong.** ETag semantics: S3 ETags are MD5 for single-part
uploads but are *not* MD5 for multi-part uploads — they are
`MD5(concat(part-MD5s))-N`. Our Iceberg writers use multipart for any
file > 100 MB, which is most silver/gold row-group-sized files. The
blueprint implies "hash of etag + range" works uniformly; it works, but
the property "ETag is a content hash" does not hold. That is fine for
caching (we use it as an opaque identifier), but do not claim in OSS
marketing that Shelf keys are cryptographic content hashes — they are
not.
- **Blast radius.** A bucket-level replication event that re-writes
objects with different ETags (intentional on DR replication) appears
as a full cache miss across the tenant. Recoverable.
- **Simpler replacement.** None — this design is correct.
- **Steady-state cost.** 32 bytes key + ~8 bytes metadata per object.
At 2.4 TiB / 64 MB row-groups = 37k keys + orders of magnitude more
footer/manifest keys = still trivially fits in DRAM. Fine.

### 1.7 Per-pool byte quotas (§6.1)

- **What can go wrong.** `pool.rowgroup_hot` steals from
`pool.rowgroup`; a quota miscalculation starves one tier permanently.
Alluxio's tiered-store quotas (we just tuned them) caused
`NodeHasDiskPressure` eviction storms on 2026-04-20 — same failure
class.
- **Blast radius.** Hit-rate tanks, not correctness.
- **Simpler replacement.** None — separating metadata from bulk is
genuinely right (Firebolt validated it, Alluxio validated it). **But:**
ship v1 with two pools (metadata-DRAM + bulk-NVMe), not four. Split
`rowgroup_hot` out only when we measure scan-based eviction hurting
dashboards.
- **Steady-state cost.** Bookkeeping. Manageable.

### 1.8 `shelf-result-cache` as a separate binary (§13.5, §14)

- **What can go wrong.** Two binaries → two deployments → two oncall
paths → two incident histories. It "shares the control plane" so in
practice it will share Raft too if we're not careful. The blueprint
says "independently deployable and optional" — good — but then the
v0 roadmap ships it in Phase 1.5 as a first-class component. Those
two statements contradict.
- **Blast radius.** Stale result served for < 30 s (snapshot-watcher
lag). Real consequence: a dashboard shows yesterday's number. This
is *exactly* what Snowflake's result cache does and what BI users
tolerate. Not a hair-on-fire bug.
- **Simpler replacement.** **Do not build in v1**. The TrinoCache
blueprint's Redis-backed Trino Gateway result cache (Phase 0 in
`COMPARISON.md`) is already shipping a result cache. Keep it. Do not
re-host it inside Shelf until the data-plane story is proven.
- **Steady-state cost.** +1 deployable, +1 dashboard, +1 runbook. Not
justified if Redis-path already covers the use case.

### 1.9 Client-side circuit-breaker state machine (§9.5)

- **What can go wrong.** The state machine is on the hot path; a bug
in it degrades Trino. Specifically:
  - Counter overflow / thread-unsafe increment on `record_failure` under
  load = false open-circuits.
  - `hash_ring.owner_for(key)` in the retry path reads a ring that may
  not yet reflect the pod death — the blueprint assumes "ring may
  have re-elected" but the lag is measured in seconds, not ms. Retry
  to same dead pod = another 200ms added to the query.
  - Half-open probe pattern: a single probe request carries per-key
  variance; we might probe a key that genuinely has an S3 problem and
  false-fail-again.
- **Blast radius.** Latency only. Never correctness — Trino falls
through to S3. This is the best-designed part of the blueprint.
- **Simpler replacement.** None needed. **But:** the blueprint's
state-machine spec is pseudocode in a Markdown file. It needs a real
reference implementation + unit tests shipped on day one; otherwise
every downstream plugin user re-implements it differently.
- **Steady-state cost.** Negligible.

---

## 2. Honesty audit

### 2.1 "p50 scan latency ≤ 1.2× direct S3 on miss, ≥ 20× on hit, at 70-85 % hit rate"

Under what assumptions? Row-group-granular misses that are never larger
than a single range-GET (so the "1.2× miss" budget isn't blown by an
extra RTT to Shelf). Hit rate is measured on rep-2 workload, which —
per production data, `RUNBOOK.md` Phase 10 — Alluxio already delivers
71% cumulative, 76% instantaneous. So the **marginal hit rate lift of
Shelf over the currently-fixed Alluxio is potentially zero to low
double-digits**, not "vs raw S3". Reframe the claim: "hit rate
comparable to Alluxio at substantially lower operational surface and
with row-group granularity that is not possible in Alluxio OSS 2.9.5."
That is honest. The 20× on hit number is defensible (DRAM-cached
footer vs S3 GET) and the 1.2× on miss is defensible if Shelf is in
the same AZ and the plugin path is genuinely fail-open.

If any one breaks: if Shelf misses a lot (say, first day after a KEDA
scale-in rotates every worker), the miss path is now Shelf-probe-fail
→ S3-read, which is > 1.2× because of the probe time. The circuit
breaker mitigates this, but `rep-3` re-cutover of 2026-04-22 showed
exactly this pattern kill write-path with Alluxio; Shelf needs to
*measure* the miss overhead under KEDA-churn load, not assume it.

### 2.2 "One operator on call instead of a team"

For v5, realistic. For v1, not remotely. A new Rust service, a new
trainer, a new Trino plugin, a new S3-compat shim, a new result-cache
sidecar, a new Helm chart, new Grafana dashboards, a new admission
model, a new openraft — each one of those is a page at 3 a.m. the
first time it misbehaves, and the blueprint ships all of them before
Phase 7. **Realistic oncall headcount from Phase 2 → Phase 6: two
engineers shadowing, plus one SRE with paging on Shelf itself.** The
"one operator" claim is the mature state (Phase 8+).

### 2.3 "Rust cache plane; no JVM GC"

The implication is that Rust eliminates pause-class tail latency. It
reduces one class of pause and introduces others:

- Allocator pauses: `jemalloc` / `mimalloc` default to eager-returns
that can stall a core for hundreds of µs on a large free; we saw
this in every Rust service I've shipped in prior roles.
- `tokio` executor starvation: one synchronous-blocking call inside an
async task blocks the whole worker thread; under NVMe I/O pressure
this is not rare, and Foyer's disk-write path is a plausible
offender.
- Page-cache pressure: NVMe mmap + Linux page-cache reclaim can pause
any reader for milliseconds. This has nothing to do with GC.
- Kernel scheduler: EKS CFS scheduling under co-located DaemonSets
(monitoring, log-shipper) adds real p99.9 jitter.

Realistic p99 on a warm Rust hot-path: 1-3 ms. p99.9: 10-50 ms. p99.99:
100-500 ms. That is *better* than JVM equivalents, but **it is not zero**.
The blueprint should remove any language that implies GC elimination
also eliminates pause-class tail latency — it does not.

### 2.4 "Fail-open: every Shelf error becomes a transparent fall-through to S3"

Model the thundering herd: Shelf has 5 pods × 16 GB DRAM + 500 GB NVMe
= ~2.5 TB total cache (same order as Alluxio today). If all 5 pods die
simultaneously (KEDA node rotation + a cluster-wide network hiccup —
we had one of these on 2026-04-20), rep-2's working set of 150 GiB
needs to be re-fetched from S3. At 100 Gb/s cluster egress that is 12
seconds; at the actual 10-25 Gbps ENI per node × 4 replicas × 32
workers each = ~3 Tb/s *theoretical* cluster capacity. S3 can absorb
this with ease. **But:** S3 has per-prefix rate limits (5500 GET/s per
prefix, historically). A cold cache replay hitting the same
`/cdp/icesheet/silver_offline_event_data_2026/` prefix at 10k
concurrent GETs per second will throttle. Alluxio's worker-level
retry-with-backoff absorbs this; Shelf's fallback path has to, too.
The blueprint does not specify per-prefix rate limiting on the
fallback path. **Add it.**

### 2.5 "20 weeks to public launch" (actually Phase 7 = 22 weeks per blueprint §12 arithmetic)

Counting: Phase 0 (2w) + Phase 1 (3w) + Phase 2 (2w) + Phase 3 (3w) +
Phase 4 (3w) + Phase 5 (2w) + Phase 6 (3w) + Phase 7 (2w) = 20 weeks
engineering work, not counting Phase -1 (1w) which is unrelated. But
that's **end-to-end wall-clock**, not calendar — and assumes one
engineer can fully context-switch at phase boundaries.

Realistic calendar with a 3-person team (which is what the "what would
you actually build on Monday" exercise assumes), in a shop that is also
running the Alluxio stack in parallel, is training, and has on-call:

- Phase 0-1 (PoC + row-group): 6 weeks realistic (vs 5 planned)
- Phase 2 (prefetch, and re-design around the removed splitCompleted
event): 4 weeks realistic (vs 2 planned)
- Phase 3 (ring + NVMe + shim): 6 weeks realistic (vs 3 planned)
- Phase 4 (learned admission, or its replacement): 2 weeks if we pick
size-threshold; 6 weeks if we insist on ONNX
- Phase 5 (productionise rep-2): 4 weeks realistic (vs 2); co-exist
with Alluxio requires shadow-traffic validation
- Phase 6 (roll to rep-0/1/3): 4-6 weeks; each replica is a separate
rollout with its own ACL story (replica-2 is Ranger, replica-3 is
file rules.json; see AGENTS.md — these are not trivially homogeneous)
- Phase 7 (OSS launch): 4 weeks (vs 2)

Realistic end-to-end to "Alluxio retired from all 4 replicas":
**32-38 calendar weeks**, ~8 months. OSS launch: **36-44 weeks**, ~9-10
months. That is roughly 2× the planned timeline.

**Phases 8-10 (§7.4, §7.5) do not run in parallel with phase 7** in a
3-person team — they compete for the same engineers. The "run in
parallel" line in §12 is wishful.

---

## 3. Trade-off rewrites

### Embedded Raft vs a 3-node etcd vs a single-leader coordinator pod

**Blueprint approach.** `openraft` crate inside each `shelfd` pod;
3- or 5-node quorum; stores ring membership, pinned-table list, tenant
quotas.

**Simpler alternative.** Ring membership is pulled from the StatefulSet
headless service (K8s is already a consensus system for us); pin list

- quotas live in an S3-backed ConfigMap reloaded on `SIGHUP` or 15 min
polled; admin writes go through `kubectl apply`. One node is elected
"coordinator" via K8s lease lock for the 30-second training-ingest job
that reloads the admission model; everything else is consensus-free.

**Trade.** We lose: atomic, strongly-consistent multi-key updates
across the cluster (e.g. "pin these 7 tables in a single transaction").
We gain: no pre-1.0 Raft crate in the build, no quorum outages during
node rotation, no new storage layer, no new RPC listeners. For a cache
that is fail-open by construction we do not need consensus.

**Recommendation.** Ship v1 consensus-free. Keep Raft on the roadmap if
and only if we discover a real multi-key consistency requirement in
Phase 5+. Scientist's §4.10 reaches the same conclusion.

---

### Learned ONNX admission vs size-threshold + pin-list

**Blueprint approach.** A nightly-trained 3-layer MLP on 10 features,
exported to ONNX, invoked on every cold miss > 8 MB (§7.3).

**Simpler alternative.** Size-threshold: refuse admission to any object

> 1 GB unless on the operator-supplied pin list, whose entries come
> from querying `cdp.trino_logs.trino_queries` for top-N tables by
> `scanned_bytes × wall_time × frequency`. Pin list is a JSON file
> committed to git.

**Trade.** We lose: marginal NVMe write bandwidth improvement on the
10-15% of queries that are very-large ad-hoc scans by users who then
immediately issue a *different* very-large ad-hoc scan — i.e. the
case where the MLP correctly predicts "no re-access". We gain: a
model-free, Python-free, ONNX-Runtime-free data plane; an admission
decision ops can read and reason about without a Jupyter notebook.

**Recommendation.** v1 ships size-threshold. v2 swaps in LightGBM if
we measure a ≥ 5 pp hit-rate gap vs size-threshold on replayed
`trino_logs`. Never ship ONNX for this use case; the C++ binary cost is
not justified by a 10-feature MLP that could be hand-coded in 20 lines
of Rust.

---

### Consistent-hash ring with Raft-stored membership vs K8s-headless-service + client-side hashing

**Blueprint approach.** 2000 vnodes per physical node, capacity-weighted,
Raft-stored ring.

**Simpler alternative.** Plugin resolves `shelf.shelf.svc.cluster.local`
(headless service) every 5 s, gets pod IPs, runs Rendezvous (HRW) hash
with capacity weights pulled from each pod's `/stats` endpoint. No ring,
no vnodes, no Raft.

**Trade.** We lose: ≤ 1/N extra mis-routes during the 5 s DNS-cache
window after a pod rotation (because one client may resolve the new
membership before another). This is trivially absorbed by the
fail-open circuit breaker. We gain: operationally identical model to
every other StatefulSet we run; zero new concepts for ops.

**Recommendation.** Ship HRW + DNS membership in v1. Scientist §4.5
agrees.

---

### Hybrid HTTP/Arrow Flight protocol split vs HTTP-only for v1

**Blueprint approach.** HTTP/2 for < 1 MB, Arrow Flight for ≥ 1 MB, on
the same connection pool with h2 multiplexing.

**Simpler alternative.** HTTP/2 range-GET for everything in v1. Arrow
Flight added in v2 only if benchmark shows > 20 % throughput gain at
our realistic per-stream bandwidth (1-3 GB/s on EKS, not 6 GB/s on
InfiniBand).

**Trade.** We lose: some theoretical throughput on bulk row-group
reads. We gain: one protocol to tune, one connection pool to size, one
benchmark to publish, one less place for gRPC-version regression to
bite us.

**Recommendation.** v1 HTTP-only. v2 Flight for bulk. The blueprint's
"6 GB/s" number has to go either way (scientist §4.6).

---

### `shelf-result-cache` as a separate binary vs deferring result caching entirely

**Blueprint approach.** `shelf-result-cache` is a separate optional
companion binary shipped in Phase 1.5, shares control plane with
`shelfd`.

**Simpler alternative.** Ship the Phase 0 Redis-backed Trino Gateway
result cache from `COMPARISON.md` — it is already in the merged roadmap
and solves 60-70 % of dashboard traffic in week 2. Defer
`shelf-result-cache` to v2; evaluate whether it justifies folding into
`shelfd`'s DRAM tier at that point.

**Trade.** We lose: having one binary that does both. We gain: not
building a second binary during the period when we are struggling to
ship the first one; not giving ops two cache layers to reason about.

**Recommendation.** Drop `shelf-result-cache` from the v1 roadmap. The
Phase 0 Redis-path is the result cache.

---

### (Extra) Phase 10 Incremental MV Refresh vs drop entirely

**Blueprint approach.** §7.5 + Phase 10 (8-12 weeks) builds
`shelf-mv-refresh` — a new service that watches Iceberg snapshots,
reads delta files, computes incremental aggregates, and commits via
Iceberg `MERGE`. Positioned as the Firebolt-gap closer.

**Simpler alternative.** Drop from the cache project. Propose it as a
separate dbt / Airflow-native project, or as a Trino TIP for native
incremental MV refresh. It is a **compute service**, not a cache.

**Trade.** We lose: the marketing story that Shelf closes the Firebolt
gap end-to-end. We gain: Shelf stays a cache, which is already two
ambitious subsystems (data cache, result cache); not three.

**Recommendation.** Cut Phase 10 from the Shelf roadmap. If we want
this capability, start it as a separate project and let Shelf *consume*
its MV files (which Shelf would do automatically anyway — they are just
more Iceberg tables).

---

## 4. What I'd build on Monday

3-person team, hands me the repo, target the `replica-2` workload where
we already have an Alluxio baseline of 71% hit rate to beat on
something other than hit rate (granularity + operational simplicity).

### 4.1 v0.1 (2 weeks)

**Scope:** smallest demonstrable win on one replica. Single-node
Shelf. No ring, no Raft, no trainer, no prefetch listener, no result
cache, no ONNX.

**In:**

- `shelfd` skeleton in Rust: Axum HTTP server, Foyer DRAM-only cache
(64 GiB max), `GET /cache/<sha256-key>` returns the range or 404.
- Content-addressed keys: `sha256(etag + offset + length)`.
- S3 origin client (AWS SDK v2, one connection pool).
- `ShelfFileSystem` Java plugin (~400 LOC) that wraps Trino's
`S3FileSystem` and intercepts reads for configured prefix list. On
miss: pass-through to S3 and populate Shelf async (fire-and-forget).
- Circuit-breaker reference implementation (§9.5) as a standalone
Java class, unit-tested.
- Deployed as a 1-pod Deployment (not StatefulSet) on rep-2 with
NodeSelector on the existing `alluxio` Karpenter pool (we already
own 6 nodes there — reuse them).
- Hooked up as a secondary path in parallel with Alluxio via a
shadow-traffic toggle; no production queries routed yet.

**Out:** everything else in the blueprint.

**Retires risk:** Shelf binary can start; plugin is fail-open; S3
fallback works; p99 overhead ≤ 5 % on the read path.

### 4.2 v0.5 (2 months)

**Scope:** replace Alluxio on one replica (rep-2) for the `cdp`
catalog's gold/silver read path. **Explicit gate:** Shelf must match or
beat Alluxio's current measured metrics on rep-2 (71% hit rate,
`GOLD_DBT` ok rate ≥ 99.9 %, p95 latency within 20 %) for 7 consecutive
days, with 50 % less oncall surface, or the project does not proceed.

**In:**

- Add row-group granularity + Parquet footer caching (§7.1).
- Two pools: metadata-DRAM (FrozenHot, 5 GiB) + rowgroup-NVMe (Foyer
hybrid, 500 GiB/node).
- NVMe tier, Foyer's built-in SIEVE for DRAM and S3-FIFO for NVMe —
both already in Foyer, no custom eviction code (scientist §4.2).
- 3-node Shelf StatefulSet with Rendezvous hashing + K8s headless
service membership (no Raft).
- Size-threshold admission + manually-curated pin list in
`pin_list.json` (not an MLP).
- S3-compat shim on `GET` + `HEAD` only (§8.3); enables DuckDB /
notebook usage without Trino.
- Full `shelfctl` CLI with `stats`, `pin`, `evict`, `ring` commands.
- Grafana dashboard (insight-first per AGENTS.md — traffic-light hit
rate, p95, fallback rate, pod health).
- Chaos: KEDA rotation drill + pod-kill drill, both passing weekly.

**Out:** plan-aware prefetch (Phase 2 of blueprint), learned admission,
Arrow Flight, result cache, blooms/MV awareness, Raft.

**Retires risk:** Shelf can actually replace Alluxio as a read cache
on one real workload. If it can't, nothing else matters.

### 4.3 v1.0 (5 months)

**Scope:** what the blueprint claims, minus the cuts above.

**In:**

- v0.5 rolled to all 4 replicas. Replica-specific ACL handled (rep-2
Ranger, rep-3 file rules.json) by keeping Shelf's only auth
surface its tenant-id → IRSA role map.
- Plan-aware prefetch via `QueryCreatedEvent` → file + footer only
(Phase 2a from §7.2). Plugin-side observation for row-group
prefetch (Phase 2b-signal-1). **No `SplitCompletedEvent` mechanism
anywhere** — that path is dead.
- HTTP/2 for all payload sizes. Arrow Flight deferred until v1.1 and
only if benchmark motivates it.
- Nightly trainer emits a pin list + per-table frequency stats. No
ONNX model.
- OSS launch scoped down: repo, docs, blog, one reproducible
benchmark (replay of 7 days of `trino_logs`). No TPC-DS flex — that
is Phase 2 content.
- Trino TIP filed to re-introduce a scoped split-level cache-interest
event (because we will want it, even if we can ship without it).

**Out of v1.0 (into v1.x):**

- Learned admission (LightGBM if ever; evaluate in v1.1).
- Arrow Flight bulk path (measure first, add if justified).
- Side-built bloom filters §7.4.2 (keep §7.4.1 bring-your-own blooms
as an ops playbook item, no Shelf code).
- Sort-order / z-order awareness §7.4.3 (simple to add but not a v1
differentiator).
- MV-aware caching + Phase 10 incremental MV refresh: MV caching is
a thin wrapper that costs nothing if we have Iceberg MVs; refresh
service is a separate project.

---

## 5. Design-principle review

§5 of the blueprint lists 7 principles. Verdict per principle:

1. **"Caching must be decoupled from compute."** Non-negotiable.
  Honoured throughout. **Keep.**
2. **"Granularity must match how Trino actually reads."** Non-negotiable.
  Honoured in §7.1. **Keep.**
3. **"The engine pushes intent; the cache acts on it."** Negotiable and
  probably too strong. With `splitCompleted` gone, the best we can do
   at plan time is "here are the files/footers". Plugin-side
   observation is *reactive*, not push. Reword to: **"The cache
   exploits whatever plan and observation signal the engine exposes,
   never blocks the engine waiting for any signal."**
4. **"Immutable by construction."** Non-negotiable. Honoured. **Keep.**
5. **"Open first, and genuinely multi-engine."** Aspirational, not
  non-negotiable for v1. Keep the intent; recognise that shipping
   Spark + Python in v1 is out of scope per §4.3 above. Reword as:
   **"Wire protocol must be open enough that a non-Trino engine can
   adopt Shelf without Trino cooperation."**
6. **"Simpler to operate than what it replaces."** Non-negotiable.
  Honoured *as a principle*; not honoured *by v1 scope* (four pools,
   ONNX, Raft, two protocols, two binaries). My scope cuts in §4 are
   what makes this principle actually true. **Keep, enforce via the
   v0.5 gate.**
7. **"Degrade transparently."** Non-negotiable. Honoured via §9.5
  circuit breaker. **Keep.**

Principles missing:

- **"Every RPC must have a budget."** Every Shelf client call has a
hard timeout and a fail-open path. This saves us from the Alluxio
"master stuck, Trino S3 client drops after 15 s with no diagnostic"
class of bug. Make it explicit.
- **"No unbounded queue."** The prefetch queue and the training batch
queue both need explicit upper bounds with overflow behaviour.
- **"Every published metric must have an SLO."** Grafana has room for
hundreds of charts; we only care about the ones that alert. Per
AGENTS.md this is already the house style.
- **"Every config key must be reloadable at runtime OR documented as
restart-required."** Alluxio burned us on `tieredstore.*.quota`
requiring full pod restart.
- **"No new consensus systems without a failure case that demands
them."** The anti-Raft principle, encoded.

Revised 12-point list = original 7 (with rewords) + 5 new. Defer
authoring in BLUEPRINT.md to the planner.

---

## 6. Responses to the scientist's open questions

1. **Raft or no Raft.** No Raft. K8s headless service + ConfigMap-
  backed pin list, one-pod lease for training-job ingest. Delete the
   openraft dependency. (§3 above, matches scientist §4.10.)
2. **LightGBM vs MLP vs size-threshold.** Size-threshold + manually
  curated pin list in v1. LightGBM in v1.x if measured hit-rate gap.
   Never ONNX MLP — the dependency is not justified by the problem.
3. **GL-Cache vs S3-FIFO.** S3-FIFO (already in Foyer). Re-evaluate
  only if a 30-day trace replay shows > 3 pp gap vs the GL-Cache
   paper's numbers. We are not going to bet 6 weeks of engineering on
   a 3-pp hit-rate ceiling.
4. **Replacing Phase 2b-signal-2.** Ship plugin-side observation only
  (signal-1). Use `QueryCompletedEvent.operatorSummaries` to build
   post-hoc `(query fingerprint → accessed row-group set)` maps for
   warmup on the next occurrence — that is the *learning* part of
   Phase 2b without depending on the deleted SPI. File a Trino TIP for
   a focused cache-interest split event as a v2 thing; do not wait
   for it.
5. **1 MB threshold for Flight.** Do not ship Flight in v1. In v1.x,
  benchmark with thresholds at 256 KiB, 1 MiB, 4 MiB, 16 MiB on actual
   EKS networking, pick empirically.
6. **ONNX Runtime vs Rust MLP vs LightGBM.** Do not bring ONNX into
  `shelfd`. If we ever ship a learned model, LightGBM via the Rust
   binding. The scientist and I agree.
7. **SIEVE lock-freedom on our workload.** Irrelevant — Foyer ships
  SIEVE and S3-FIFO both; we benchmark on traces and pick. Do not
   market SIEVE as the lead feature; our workload is low-QPS per-key,
   not 1M-QPS KV.
8. **Drop plan-aware prefetch for v1 entirely?** My v0.5 gate says yes
  — we prove the cache first, add prefetch later. The blueprint's
   Phase 2 becomes a v1.0 feature, not a v0.5 one. This aligns with
   the blueprint's own §16 "minimum viable next step" language.
9. **Bloom recommender §7.4.1 vs §7.4.2.** Ship only §7.4.1 (bring-your-own
  Parquet blooms) as an *ops playbook*, not as Shelf code. §7.4.2
   (side-built blooms in `shelfd`) is a different product; defer to
   v2. Scientist's §2 / §4 essentially agrees.
10. `**shelf-result-cache` belongs in Shelf repo?** No. The Phase 0
  Redis-backed Trino Gateway result cache (from COMPARISON.md) is
    the v1 result cache. Revisit v2+ whether to fold its successor
    into Shelf.
11. **Smallest viable v1.** My §4.2 "v0.5" is my answer: row-group
  granularity + content-addressed keys + fail-open plugin + Foyer
    hybrid + HTTP range-GET + Rendezvous hashing + size-threshold
    admission. No prefetch, no Raft, no ONNX, no Flight, no result
    cache.
12. **Shared across all 4 replicas or per-replica cluster?** Shared
  across all 4. Per-replica removes the single biggest
    non-hit-rate win (4× effective cache size; AGENTS.md confirms
    cross-replica duplication as a live pain). Per-tenant quotas are
    fine — tenant ≠ replica; tenant = Trino resource group. Make the
    distinction explicit in the blueprint; it conflates them today.

---

## 7. Recommended blueprint edits

For the planner's diff intent. File reference is `shelf/BLUEPRINT.md`.

- **§1 TL;DR table, last row ("Approximate in-cache indexes")** — drop
from TL;DR table. Move to roadmap phase 8+ only.
- **§1 TL;DR**, remove "≥ 20× direct S3 on hit, at 70-85 % hit rate"
framing. Replace with "hit rate comparable to the fixed Alluxio 2.9.5
baseline at substantially lower ops surface".
- **§4.3** — fix "CacheLib (SOSP '20)" → "CacheLib (OSDI '20)". Add
explicit note that Foyer ships S3-FIFO + SIEVE as pluggable policies.
- **§4.4** — soften DORA citation; note DORA is not peer-reviewed. Add
Rendezvous hashing as the actual primitive, per scientist §2.4.
- **§5 principle 3** — reword per §5 above.
- **§5** — add principles 8-12 per §5 above.
- **§6.1 `pool.rowgroup*`* — replace "GL-Cache-style group-level
eviction" with "S3-FIFO (Foyer built-in)". GL-Cache becomes a v1.1
upgrade path.
- **§6.1 admission policy** — replace "consult the learned admission
model (ONNX file shipped by the trainer)" with "size-threshold
(refuse > 1 GB unless on pin list). Learned model is a v1.1 upgrade
path; see §7.3 revised."
- **§6.1 router** — replace "2000 vnodes per physical node,
capacity-weighted. Ring membership stored in Raft" with "Rendezvous
(HRW) hashing over pod DNS entries; capacity weights from pod
`/stats`."
- **§6.3** — delete the openraft dependency. Pin list + tenant quotas
live in a versioned S3 ConfigMap pulled on `SIGHUP` / 15 min.
- **§7.2 Phase 2b-signal-2** — rewrite: delete the `SplitCompletedEvent`
mechanism. Replace with `QueryCompletedEvent.operatorSummaries`-based
post-hoc learning. Explicitly note Trino PR #26436 (merged 2025-08-19)
removed `splitCompleted`. Update §13 risks row accordingly — the
current claim is inverted (scientist §1).
- **§7.3** — rewrite: lead with size-threshold admission. Demote ONNX
MLP to "possible v2 upgrade; never required". Drop the "10-50 µs ORT
inference" claim without a measurement (scientist §1, §2.7).
- **§7.4.1 & §7.4.3** — keep as ops playbook items, not Shelf code.
- **§7.4.2** — move to v2 roadmap (phase 8+).
- **§7.5 & §10 (Phase 10)** — remove MV-aware caching from the v1/v2
core; reframe MV awareness as "Shelf caches MV files like any
Iceberg file, no special code". Incremental MV refresh (Phase 10) is
out-of-scope — it is a compute service, not a cache.
- **§8.1** — remove the "6 GB/s" Arrow Flight number. Note the
Mellanox context per scientist §4.6. For v1, HTTP/2 only.
- **§8.3** — shrink scope: `GetObject` + `HeadObject` only, no writes.
- **§9.2** — replace "design target: 0 by construction (SDK v2 pool +
async)" with an explicit statement of the equivalent failure mode in
Shelf (Tokio task starvation on NVMe I/O; Foyer write pressure), and
what we do about it (rate-limit, per-prefix pools, circuit breaker).
The magic-thinking version of this line is what got us the misdiagnosed
"SDK maxConnections=50 is hardcoded" in 2026-04-21; do not repeat it.
- **§9.4** — add thundering-herd row: if all Shelf pods die
simultaneously, fallback per-prefix rate limit required.
- **§9.5** — promote from pseudocode to committed reference Java
implementation shipped in v0.1; add unit-test list.
- **§12 roadmap** — retime per §2.5 above (~2× blueprint estimate). Move
Phase 10 out of roadmap entirely.
- **§13 risks table** — invert the "PR #26425 already enables worker
event listeners" row.
- **§13 open questions** — answer or defer each per §6 above.
- **§15.5** — drop the "Aggregating indexes" row pointing at Phase 8+
MV experiment; we cut that.
- **§16 next step** — already sensible; align target p99 DRAM read from
"≤ 1 ms" to a ranged "1-3 ms" per honest tail-latency discussion
(§2.3 above).

---

## 8. My single biggest concern

**Shelf is being designed at exactly the moment Alluxio started
working.** The post-mortem for the 2026-04-22 replica-3 cutover is 24
hours old relative to this blueprint. The 3-master HA migration
completed on 2026-04-23 — today — and we now have 6 Alluxio workers
with 2.4 TiB of cache delivering 71% hit rate on the replica-2
workload, with a known, documented concurrency-cap (Phase 16) that
survived a 30-min write-heavy canary with zero pool timeouts.

In that emotional state, it is extremely easy to design a 9-10 month
greenfield Rust project to "never again deal with Alluxio" while
mentally discounting three uncomfortable facts: (1) *our* Alluxio
problems were two specific OSS bugs (UfsIOManager default of 36 threads
and a DNS-TTL race on master restart, both now fixed), not fundamental
architectural flaws; (2) a new Rust service with openraft, ONNX,
Arrow Flight, row-group parsing, plan-aware prefetch, and a companion
result-cache binary has a *much* bigger surface for 3 a.m. pages than
a fixed Alluxio; (3) the genuinely differentiated wins — columnar
granularity and cross-replica sharing — are the two smallest chunks of
the blueprint and could be prototyped in 2 months, not 10.

**If the team ignores everything else in this review, they must not
ignore this: gate the entire Shelf project on the v0.5 milestone
beating Alluxio on replica-2 on measured metrics, for 7 consecutive
days, with the scope cuts in §4.2. If v0.5 cannot do that, Shelf is
the wrong answer and we should instead invest in the Alluxio path we
already understand.** That is the honest framing, and it is the one
the engineer on call in 6 months will thank us for.