# Scientist review of shelf/BLUEPRINT.md

_Author: agent-1-scientist_
_Date: 2026-04-24_
_Reviewed blueprint version: working copy, v0.3, last edited 2026-04-23_
_Peer review of: `shelf/agents/out/02-critical-review.md` (engineering critic)_

## TL;DR

The blueprint's research scaffolding is 70% defensible. Core primitives
(SIEVE, CacheLib/Foyer, FrozenHot, PACMan-style coordinated caching,
content-addressed keys, immutable-file reasoning) are cited against the
right papers with approximately the right numbers. But five specific
citations are wrong or decayed, three killer-feature claims (§7.2
plan-aware prefetch, §7.3 learned admission, §7.4.2 side-built blooms)
rest on research whose workload assumptions do not match ours, and one
entire phase (Phase 10 incremental MV refresh) duplicates work that is
already merged into Trino itself. Of 17 cited claims and upstream
references I examined, I mark **9 ✅ accurate, 5 ⚠ misread/outdated,
3 ❌ unsupported**.

Most important findings:

1. **Trino PR #26425 is CLOSED, not merged** — the blueprint §13 risks
   table has the claim *inverted*, and PR #26436 (merged 2025-08-19,
   Trino 477) went on to REMOVE `splitCompleted` entirely. §7.2
   Phase 2b-signal-2 is dead on arrival.
2. **Trino already ships incremental MV refresh** — PR #20959 (merged
   2024-06-20, milestone 451) implements it for single-table,
   predicate-only MVs. Blueprint's Phase 10 is duplicative for the
   simplest (and most common) case and needs to be repositioned as
   "extend Trino's existing incremental refresh to multi-table /
   aggregating MVs".
3. **Iceberg PR #6935 is CLOSED without merge; PR #6967 is also
   closed.** Working Parquet page-index skipping in Iceberg landed via
   PR #10399 (non-vectorized) and PR #15211 (vectorized). The §4.5
   citation is out of date.

Overall verdict: **the science supports shipping a columnar-granular,
plan-aware data cache**, but the blueprint overclaims on row-group-level
prefetch, learned admission, and the MV-refresh angle. The critic's
scope cuts (no Raft, no ONNX, no Flight in v1, drop Phase 10) align
with what the literature actually justifies.

---

## 1. Verification of cited claims (§4)

| # | Paper / ref | Blueprint claim (§) | Actual claim (source) | Verdict | Note |
|---|---|---|---|---|---|
| 1 | SIEVE (NSDI '24, Zhang et al.) | "beats ARC by up to 63.2 % lower miss ratio … ≤ 20 LoC … O(1) lock-free hit path … 5+ production libs" (§4.1) | Paper: up to 63.2% lower miss ratio vs ARC on 1559 traces; **wins vs 9 SOTA algorithms on only 45% of traces** (not always); implemented in 5 prod cache libs (C++, Go, JS, Python, Rust); "prototype implementation achieved twice the throughput of an optimized 16-thread LRU" [1] | ✅ accurate | Omitted caveat: SIEVE is not dominant on *all* traces. Our workload is analytical (low-QPS per key, very skewed), which is the setting SIEVE targets best. |
| 2 | GL-Cache (FAST '23, Yang et al.) | "228× throughput vs LRB and +7 % hit rate" (§4.1) | Paper: 228× throughput vs LRB on average, +7% hit ratio on average, +25% at P90. **Evaluated on 118 block-I/O and CDN traces**; uses gradient-boosted trees and insertion-time grouping. [2] | ⚠ misread | Numbers correct; *workload assumption is not*. No published evaluation of GL-Cache on analytical / Parquet-row-group workloads. Blueprint should acknowledge this. S3-FIFO [6] is a more defensible default for the NVMe tier and is already in Foyer. |
| 3 | FrozenHot (EuroSys '23, Qiu et al.) | "Up to 5.5× throughput on skewed workloads" (§4.1) | Paper: up to **551% (i.e. 5.51×) improvement** on baseline cache algorithms; up to 90% for RocksDB on YCSB; requires < 100 LoC to integrate. [3] | ✅ accurate | Matches. FrozenHot's assumption (hot-set stable over short windows) is a good fit for Iceberg metadata and Parquet footers, which are immutable for the life of a snapshot. |
| 4 | CacheLib (Berg et al.) | "SOSP '20" (§4.3) | Actual venue: **OSDI '20**, Nov 4–6 2020 [4] | ❌ unsupported | Wrong conference. Critic also flagged. Trivial fix. |
| 5 | PACMan (NSDI '12, Ananthanarayanan et al.) | "coordinated cache admission beats per-node greedy admission for parallel jobs" (§4.2) | Paper: "all-or-nothing" caching property for parallel jobs; 53% (Facebook) / 51% (Bing) job-completion improvement via LIFE/LFU-F; evaluated on MapReduce-style batch. [5] | ⚠ misread | The *coordination* insight generalises; the *all-or-nothing* result does **not** generalise to modern Trino, where pipeline execution starts reading splits long before all splits are enumerated. Blueprint implies more transfer than the paper supports. |
| 6 | LRB (NSDI '20, Song et al.) | "learn the next-access-time distribution … LRB-style features at the admission step" (§4.2) | Paper: LRB approximates Belady MIN using a learned P(reaccess within horizon); reduces CDN WAN traffic by 5–24% vs production CDN designs; 6 CDN traces. [7] | ⚠ misread | LRB's evaluation is on **CDN traces** (Wikipedia-scale request traffic), not analytical scans. The feature *vocabulary* transfers; the published *win* does not. See §3.3 below. Critic §1.3 agrees. |
| 7 | Alluxio DORA | "consistent-hash ring over workers, clients talk directly to the owning worker in 1 hop" (§4.4) | Alluxio docs confirm: consistent hashing over file paths, direct client-worker communication, **membership held in an external KV store (e.g. etcd)**. [8] | ⚠ misread (partial) | Blueprint says we "borrow this idea but ship it in Rust + Raft". DORA's membership is **not** in Raft — it is in an external KV. So Shelf's Raft choice is an architectural *divergence* from DORA, not an inheritance. Worth spelling out. |
| 8 | Ceph CRUSH | "deterministic placement under heterogeneous capacity" (§4.4) | CRUSH (Weil et al., SC '06) does exactly this. [9] Canonical reference. | ✅ accurate | Correct, but for our scale (5-10 nodes) CRUSH is over-engineered. Rendezvous hashing [20] is the simplest primitive that delivers capacity-weighted deterministic placement without maintaining a ring. Critic §3 agrees. |
| 9 | Parquet Page Index (Iceberg PR #6935, #6967) | "read only needed pages by inspecting column-index metadata" (§4.5) | PR #6935 by @rdblue: **CLOSED, never merged** (opened Feb 2023, closed Sept 2024). PR #6967 by @zhongyujiang: **CLOSED** (21 commits, closed Sept 2024). Actual merged page-skipping in Iceberg landed via PR #10399 (non-vectorized) and PR #15211 (vectorized). [10][11][12][13] | ❌ unsupported | Misleading citation. The *feature* exists in Iceberg today — just not at the PRs cited. Fix: cite #10399 + #15211, and on the Trino side cite PR #9955 (Trino's own page-skipping for Iceberg Parquet). [14] |
| 10 | "Upstream PR #26425 already enables worker event listeners" (§13 risks row) | Blueprint: PR #26425 is merged and enables worker listeners, making `SplitCompletedEvent` a viable prefetch source (§7.2 Phase 2b-signal-2) | Actual: PR #26425 is **CLOSED, superseded by PR #26436**, which **removed `splitCompleted` support entirely**. Merged 2025-08-19 by @raunaqmorarka, Trino milestone 477. Replacement guidance: use `QueryCompletedEvent` / `QueryStatistics#getOperatorSummaries`. [15][16] | ❌ unsupported | **The blueprint's claim is the exact opposite of reality.** Critic §1.4 caught this. This is the single most important correction to make in the blueprint. Phase 2b-signal-2 must be rewritten to use `QueryCompletedEvent.operatorSummaries` for post-hoc learning, or dropped. |
| 11 | "Trino 468+ supports Iceberg materialized views with automatic rewrite" (§7.5.2) | Blueprint: MV + auto-rewrite is a Trino 468+ feature | Actual: MV support with auto-rewrite has existed since well before 468 (feature since Trino ~380s); **incremental refresh for single-table predicate-only MVs** landed via **PR #20959** (merged 2024-06-20, milestone 451). Further improvements in 479. [17][18] | ⚠ outdated | Version 468 is not the cutoff; the feature is older. Incremental refresh is *already* partly built — blueprint's Phase 10 needs to be scoped against Trino PR #20959, not proposed as a greenfield service. Critic §3 "Phase 10 drop" is too aggressive; the science says "scope Phase 10 to multi-table / aggregating MVs where Trino does *not* yet support incremental refresh". |
| 12 | Parquet bloom filter write via Iceberg TBLPROPERTIES (§7.4.1) | Blueprint: set `write.parquet.bloom-filter-enabled.column.*` and writers produce blooms in footers | Actual in Trino: PR #21602 "Add Parquet bloom filter write support to Iceberg connector" **merged 2024-06-24** by @jkylling. Reader side landed via PR #14428 (Hive, Jan 2023) and #17192 (Iceberg). [19][28][29] | ✅ accurate | Claim works end-to-end in Trino ≥ 451 for Iceberg writers. Critic implied this doesn't work; it does — but only via Trino-issued writes. Spark-side bloom write also supported. |
| 13 | ONNX Runtime inference 10-50 µs for a 3-layer MLP on CPU (§7.3) | Blueprint (v0.2-corrected): "10-50 µs, dominated by graph dispatch and input binding" | No public ORT benchmark pins a 3-layer MLP with 10 inputs at exactly 10-50 µs. Published ORT benches report ms-scale for ResNet-class models; microsecond-scale for tiny MLPs is plausible but would require our own benchmark. [30] | ⚠ outdated | Number is *plausible* but **not evidence-based**. Fix: strike the specific range, replace with "expected sub-ms on modern CPU, to be benchmarked as an acceptance criterion". Critic §2.7 agrees. |
| 14 | Arrow Flight "6 GB/s single-stream throughput, zero-copy" (§8.1) | Blueprint: Flight delivers 6 GB/s single stream, justifying protocol split above 1 MB | Actual published single-stream: **2.77 GB/s** on 100 Gbit Mellanox ConnectX-5 with gRPC transport; 3.87 GB/s with UCX/RDMA (unstable at time of test). [21][22] | ❌ unsupported | The 6 GB/s number appears to conflate aggregate (multi-stream) throughput with single-stream. On EKS `c6a.4xlarge` with 10-25 Gbps ENIs we cannot reach 6 GB/s on *any* number of streams. Fix: strike the number, replace with "2-3 GB/s single-stream on 100 Gbit fabrics per published benchmarks". Critic §1.5 / §2.3 agrees and goes further to recommend HTTP-only v1; I concur. |
| 15 | `QueryCreatedEvent.QueryMetadata` does not expose split-level info (§7.2) | Blueprint: `plan`/`jsonPlan`/`tables`/`routines` only; split info comes later via `IcebergSplitSource` | Confirmed against Trino 480 SPI javadoc: `QueryMetadata` fields = `queryId, transactionId, encoding, query, preparedQuery, queryState, plan, jsonPlan, tables, routines, uri`, no split info. [23] | ✅ accurate | Blueprint's honest scoping of plan-time prefetch (file/footer only) is correct. |
| 16 | `openraft` suitable for embedded control plane (§6.3) | Blueprint: embedded Raft in `shelfd`, no etcd | Actual: `openraft` is **pre-1.0** (v0.9.20, June 2025); project explicitly says "not yet production-ready"; chaos testing not completed; ~48k writes/s single-writer; API incompatible changes allowed until 1.0. [24] | ⚠ outdated | Technically usable, but maturity mismatch with blueprint's "one binary to rule them all" simplicity promise. Critic §1.1 / §3 recommend dropping Raft entirely in v1; I strongly agree. See §4 below. |
| 17 | Foyer hybrid cache ships SIEVE + S3-FIFO (by implication, §4.3 / §6.1) | Blueprint names Foyer as the storage engine | Foyer docs confirm: pluggable memory-cache policies FIFO / LRU / LFU (w-TinyLFU) / **S3-FIFO** / **SIEVE**. [25] | ✅ accurate | This means Shelf does not need to implement SIEVE or S3-FIFO itself — they are library features. Both GL-Cache and FrozenHot, by contrast, would require implementation effort. This substantially weakens the "we need GL-Cache" and "we need FrozenHot" arguments for v1 (see §3). |

Summary: **9 accurate, 5 misread/outdated, 3 unsupported.** Three
corrections materially affect the v1 plan:

- Fix §13 risks row about PR #26425 (inverted today).
- Redirect Phase 10 to complement Trino PR #20959 rather than
  duplicate it.
- Strike the 6 GB/s Arrow Flight number and the specific 10-50 µs ORT
  latency.

---

## 2. Missing research

### 2.1 Plan-aware / planner-driven caching beyond PACMan

The blueprint cites PACMan [5] as the plan-aware-caching foundation,
then designs plan-aware prefetch as if PACMan covers the modern case.
It does not; PACMan's "all-or-nothing" result is a property of
MapReduce-era batch (every task must start before the first task
finishes to get the win). Trino pipelines data differently —
row-groups stream to workers as they are planned. The blueprint needs
citations closer to the target engine:

- **Dremio Columnar Cloud Cache (C3)** — NVMe-backed, per-node, cached
  Arrow-format blocks; published only as a vendor blog. [26] No
  academic paper. Noteworthy: Dremio C3 is *not* plan-aware at cache
  admission time — it caches on read and evicts LRU. This means
  Shelf's plan-aware-push idea genuinely is differentiated from C3
  (critic §2.1 notes the hit-rate comparison with Alluxio but not this
  point).
- **Databricks Photon disk cache** — auto-managed local NVMe, Parquet
  + stats cached; "transparent to Spark's logical plan", LRU. [27]
- **Snowflake result cache + virtual-warehouse cache** — three-tier
  (result, virtual-warehouse local SSD, metadata); the result cache
  and the data cache are decoupled, and the result cache is global
  across warehouses. [31] This is the pattern the blueprint splits
  into `shelfd` + `shelf-result-cache`; Snowflake validates the split.
- **Firebolt warmup-engines API** — used as inspiration in §15.5 but
  not cited in §4. No academic paper; vendor docs only.

*Net finding:* No peer-reviewed successor to PACMan addresses the
modern analytical engine case. The design space is vendor-blog-only.
Shelf's plan-aware-push idea is therefore **publishable as a first
open-source design of planner-driven prefetch against a modern
pipelined engine**, provided the implementation actually delivers a
measured win (critic §8 is right to gate this on v0.5 against Alluxio).

### 2.2 Columnar-range admission and row-group scoring

Active 2023-2026 work on Parquet page-index predicate pushdown and
predictive prefetch is concentrated in Trino, Iceberg, and vendor
engineering, not in academic venues:

- Iceberg PR #10399 / #15211 — merged page skipping via column
  indexes [12][13].
- Trino PR #9955 — skip reading Parquet pages using column indexes
  for Iceberg [14]. Merged.
- Trino PR #21602 — Parquet bloom filter write support in Iceberg
  connector [19]. Merged 2024-06-24.

There is no peer-reviewed paper validating the specific "10-100×
cache-density" claim in §7.1. The claim is plausible by back-of-envelope
— a 5 GB Parquet file with one 32 MB matching row group is a 156×
density ratio for the hit — but the *aggregate* cache-density win
depends on how tight per-query predicates are, and that distribution
has not been published for Trino-on-Iceberg at our scale.
**Recommend:** the blueprint's v0.5 benchmark should measure cache
density directly (bytes cached / bytes queried) and report as the
primary headline number, because it is *our* novel data point.

### 2.3 Learned cache / learned index work post-LRB

Published successors and practical reports:

- **Mockingjay** (ISCA '22, Shah et al.) — micro-architecture cache
  replacement using multi-class prediction; 15.2% avg improvement over
  LRU. [32] Not applicable to our tier (bytes-to-MB objects, not
  64-byte cache lines) but demonstrates the learned-replacement idea
  is mature at the hardware tier.
- **Stormbird** (IEEE Access '25) — RL-based hardware cache
  replacement with bypass; 10.5 KB HW overhead. [32] Same caveat as
  above.
- **GL-Cache's own ablation** [2] — the key finding the blueprint
  should have cited: *object-level* learning (LRB) is 228× slower than
  *group-level* learning, at similar hit rate. The research case for
  "learn per object" is weaker than the blueprint implies.

*Net finding:* No peer-reviewed published evidence that a 3-layer MLP
with 10 features meaningfully beats a size threshold on analytical
scan workloads. LRB's CDN result does not transfer (see §3.3).

### 2.4 Distributed consistent-hash caching beyond DORA and CRUSH

- **Anna (ICDE '18, VLDB '19)** [33][34] — wait-free shared-nothing
  KV store, lattice-based coordination-free consistency, vertical
  tiering. Relevant because Anna explicitly *avoids* consensus for the
  data plane; uses external coordination only for auto-scaling
  policy. Supports the critic's (and my) recommendation to drop Raft
  from `shelfd` for v1.
- **FaRM** (OSDI '14, NSDI '15) — RDMA-based distributed transactions.
  Out of scope (we don't have RDMA on EKS).
- **Rendezvous hashing (Thaler & Ravishankar, 1996)** [20] — *simpler*
  than consistent hashing, handles capacity weighting via weighted
  HRW (IETF draft). Used by GitHub LB, Apache Ignite, Kafka,
  Twitter EventBus. The blueprint's "2000 vnodes per pod, stored in
  Raft" is strictly more complex than HRW for N ≤ 10 nodes. Critic §3
  also reaches this conclusion.

### 2.5 Embedded Raft in data-plane services — is `openraft` right?

- `openraft` (databendlabs) is pre-1.0, v0.9.20 (June 2025), **chaos
  testing not complete**, 48k writes/s single writer. [24]
- Alternative: `raft-rs` (TiKV) — used at scale in TiKV but depends on
  a specific `tokio` + `prost` stack.
- Alternative that avoids Raft: Anna-style policy-only coordination;
  K8s lease locks; ConfigMap + `SIGHUP`.

No peer-reviewed published work validates any Rust Raft crate at our
scale. **Recommend:** drop Raft from v1.

### 2.6 Arrow Flight vs gRPC vs HTTP/2 for analytical data planes

Benchmarks with real numbers:

- Apache Arrow issue #30728: **gRPC single-stream 2.77 GB/s** on 100
  Gbit; UCX/RDMA 3.87 GB/s but unstable. [21]
- gist (Abadi et al., Arrow serialization bench): multi-stream 4
  streams 2.4–2.5 GB/s on localhost. [22]
- No published benchmark on EKS. Published benchmarks are on
  100 Gbit RDMA fabrics, not 10-25 Gbps cloud ENIs.

*Net finding:* The "6 GB/s" number is not reproducible on a realistic
cloud fabric. On EKS, a single HTTP/2 range-GET at ~1-3 GB/s is the
realistic ceiling for both protocols, and the IPC framing overhead of
Flight matters only below ~100 KB [critic §2.3 implicitly assumed].
*The 1 MB payload cutoff in §8.1 is plausible but unsupported by our
own benchmarks.* Recommend: benchmark first, then decide; Critic's
"HTTP-only v1" is the safe default.

### 2.7 ONNX Runtime inference latency for tiny MLPs

No published ORT benchmark directly pins a 3-layer 10-feature MLP at
10-50 µs. [30] Public benches are for ResNet-class models in the
millisecond range. Plausible but *unverified*. Fix per §1.

### 2.8 MV research (for §7.5 + Phase 10)

- **DBSP** (VLDB '23, Budiu et al.) — differential-dataflow-based IVM
  for arbitrary SQL. [35][36] Powers Feldera / Materialize-adjacent
  work. Relevant if Shelf wants to support non-append-only refresh.
- **Materialize** — uses McSherry's differential dataflow (CIDR '13)
  [37]; operates on CDC streams, not Iceberg snapshots directly.
- **BigSubs** (VLDB '18, Jindal et al.) [38] — task-level
  subexpression reuse at Microsoft SCOPE scale, up to 40%
  machine-hour savings. **Prompt said SIGMOD '19 — actually VLDB '18.**
- **MISO** (SIGMOD '14, LeFevre et al.) — multistore physical
  placement; **not** MV advisor. **Prompt said SIGMOD '22 — actually
  SIGMOD '14** and unrelated to MV selection. The closest actual
  SIGMOD '22 MV-advisor work is **QO-Advisor** (steered query
  optimization), not in the same space.
- **Redshift AutoMV** — vendor feature, not a VLDB '23 paper.
  Incremental refresh supported for SELECT / FROM / INNER JOIN / WHERE
  / GROUP BY / HAVING / SUM/MIN/MAX/AVG/COUNT. Automatic MV creation
  and auto-rewrite via ML-driven workload monitoring. [39]
- **Trino's own incremental MV refresh** (PR #20959, merged
  2024-06-20, milestone 451) [17] — single-table, predicate-only,
  append-only. *This is the baseline Phase 10 must clear*.
- **Chimera** — I could not find a canonical peer-reviewed Chimera MV
  paper matching the prompt's description. The prompt may be referring
  to an internal/vendor system. Flagging as unverifiable.

*Net finding:* **Phase 10 is not greenfield**. Trino already supports
incremental refresh for the simplest MV case (single-table
predicate-only). The publishable contribution Shelf can make is to
extend this to the **aggregating, multi-table case using
Iceberg-snapshot-delta + DBSP-style incremental aggregates**. Framed
that way, it aligns with DBSP / Materialize's research frontier. As a
*cache project*, Phase 10 is out of scope — critic §3 is right. As a
follow-on open-source project it has real research merit.

---

## 3. Killer-feature reassessment

### 3.1 Columnar-range granularity (§7.1)

**State of the art.** File-block caching is the norm (Alluxio, fs.cache,
Dremio C3 block-level, Databricks Delta cache). Columnar-range
granularity at the cache layer is *not* published in peer-reviewed
form as of 2026-04-24. The closest analog is Starburst Warp Speed /
Varada — proprietary, columnar-block + bitmap/dict/tree index on SSD;
Starburst claims up to 7× perf and 40% cost cuts. [40] No peer-review.

**Blueprint's position.** Cache at 4 levels: metadata.json / manifest
list / manifest / footer / page-index / row-group byte range. DRAM
for the first five, NVMe for row-groups. 10-100× density claim.

**Gap.** The 10-100× density claim is not supported by *any*
peer-reviewed paper; it is back-of-envelope and depends entirely on
predicate selectivity in real workloads. Known downsides *not*
addressed in §7.1:

- **Key-cardinality explosion.** A 512 MB Parquet file with 16 row
  groups becomes 16 cache keys + 1 footer key + metadata — 18 keys
  vs 1 for file-block. At our scale (millions of data files) this
  adds 10-50M in-memory key entries. DRAM cost: manageable (critic
  §1.6 notes 32 B key + 8 B metadata ≈ 50 MB per 1M keys).
- **Fragmentation on NVMe.** Row-group writes to NVMe are typically
  1-128 MB; Foyer handles this with segment-based writes. No
  published concern at this size.
- **Partial-row-group misses.** If Trino reads columns 1-3 of row
  group 7 but caches row-group 7 wholesale, we waste bytes on columns
  4-N. True *column*-level caching is a larger research step; v1 does
  not attempt it.

**Recommendation.** Keep the feature as-is; re-phrase the 10-100× claim
to *"density improvement measured at X% on replay of our 7-day
workload"* after v0.5, not a promise up front. This is the single most
differentiated and publishable piece of the blueprint.

### 3.2 Plan-aware push prefetch (§7.2)

**State of the art.** No peer-reviewed paper describes pushing
plan-extracted byte ranges from a coordinator to a shared data cache.
Firebolt's "warmup-engines" is vendor-only; Snowflake does not publish
its cache interaction; PACMan is the closest academic antecedent and
targets MapReduce all-or-nothing.

**Blueprint's position (§7.2, honest-scoped to Trino 480 SPI).**
`QueryCreatedEvent` exposes `plan`/`jsonPlan`/`tables`/`routines`
only. Confirmed against Trino SPI javadoc [23] — the blueprint is
**right** that split info is not available at plan time. Phase 2a
(file + footer prefetch) ships first; Phase 2b targets row-group
prefetch via two signals.

**Gap — signal 2 is dead.** PR #26425 (the one the blueprint's §13
claim cites) is **closed**; its successor PR #26436 **removed**
`splitCompleted` entirely (Trino 477, merged 2025-08-19). [15][16]
Phase 2b-signal-2 is dead on arrival. The replacement path, per the
Trino PR guidance, is `QueryCompletedEvent.operatorSummaries` — but
that's *post-hoc* (arrives after the query finishes), so it cannot
drive in-query prefetch; it can only feed the nightly trainer for
*next-time* warmup. That is strictly weaker than the blueprint claims.

**Gap — signal 1 is healthy but reactive.** Plugin-side observation
(intercept footer range-GET → parse page index → prefetch likely
row-groups) is implementable and genuinely novel in open source. It is
*reactive*, not *push*, because the trigger is the worker's own
footer read. Critic §5 is right to propose rewording principle 3
("The engine pushes intent") to "The cache exploits whatever plan and
observation signal the engine exposes".

**Recommendation.**

1. **Rewrite §13 risks row** (PR #26425 claim is inverted).
2. **Rewrite §7.2 Phase 2b-signal-2** to use
   `QueryCompletedEvent.operatorSummaries` for post-hoc learning
   feeding the nightly trainer. File a Trino TIP for a *focused*
   cache-interest split event if we decide we want in-query row-group
   push; do not block v1 on it. Critic §6.4 concurs.
3. **Set a hard coordinator-side deadline** on the prefetch gRPC.
   Critic §1.4 is right that "fire-and-forget ok" in §8.2 is not a
   bound; hard-cap at 10 ms.

### 3.3 Learned admission (§7.3)

**State of the art.** LRB (NSDI '20) [7] on CDN traces delivers
5-24% WAN-traffic reduction. GL-Cache (FAST '23) [2] on block-I/O
and CDN traces delivers +7% hit ratio over LRB on average with 228×
throughput. Neither is evaluated on analytical scan workloads.

**Blueprint's position.** 3-layer MLP, 10 features, ONNX Runtime,
10-50 µs per inference, invoked on misses > 8 MB.

**Gap — workload mismatch.** CDN traces are
millions-of-requests-per-second of small objects with Zipfian access.
Our workload is ~200k queries/day across 4 replicas — roughly 9 QPS
global, with per-key revisit time measured in minutes to days. A
3-layer MLP trained on those volumes may not accumulate enough signal
to beat a simple size-threshold heuristic.

**Gap — ONNX dependency cost.** Adding ONNX Runtime to `shelfd`
introduces (1) a C++ binary dependency, (2) model-format lock-in, (3)
an ORT version axis. For a 3-layer 10-feature MLP the actual
inference can be done by hand in ~30 lines of Rust with no dependency.
ONNX makes sense when the model evolves to something non-trivial
(e.g. GBT, deep model); for a fixed small MLP it is overkill.
Critic §1.3 / §3 agrees.

**Gap — no published evidence.** I could not find any published
evaluation of a small MLP admission controller beating size-threshold
on columnar scan workloads. The closest evidence is LRB's 5-24% CDN
win, which does not transfer.

**Recommendation.**

1. Ship v1 with **size-threshold + pin-list** (hand-curated from
   `cdp.trino_logs.trino_queries` top-N). This matches the critic's
   recommendation (§3) and has research support via FrozenHot's
   "keep the stable hot set" intuition.
2. If in v1.x a measured gap emerges, **LightGBM with 5 features** is
   the simplest-defensible upgrade, with a native Rust binding; it
   matches GL-Cache's published model class (GBT). Do not use an MLP
   unless we see non-linearity in the feature-to-outcome relationship,
   which is unlikely for this problem.
3. Do not ship ONNX for this use case. Re-evaluate only if we decide
   to ship a deep model for anomaly detection or workload forecasting
   in v2+.

### 3.4 Approximate in-cache indexes (§7.4)

**State of the art.**

- **Parquet bloom filter reader support in Trino**: yes, via
  `parquet.use-bloom-filter=true` (default on) [41].
- **Parquet bloom filter *writer* support in Iceberg**: yes, via PR
  #21602 merged 2024-06-24 [19]. Configured via
  `write.parquet.bloom-filter-enabled.column.<name>` Iceberg
  TBLPROPERTIES [29]. **So §7.4.1's "bring-your-own" path is
  operational end-to-end in Trino today.**
- **Side-built bloom filters in a cache layer**: no peer-reviewed
  paper I can find that builds blooms lazily from observed column
  reads. The closest precedent is Varada/Warp Speed, which builds
  full per-block indexes (bitmap/dict/tree) at admission time, not
  blooms [40].

**Blueprint's position.**
- §7.4.1 Parquet bloom recommender: bring-your-own + ops playbook.
- §7.4.2 Side-built blooms in `shelfd` + `ShelfFilterService` gRPC.
- §7.4.3 Z-order / sort-order awareness via Iceberg table properties.
- §7.4.4 "We don't build" for Varada-style indexes.

**Gap.**

- §7.4.1 is sound and operational.
- §7.4.2 is genuinely novel — *no open-source cache layer builds
  side-indexes from observed reads*. But: no published evaluation of
  false-positive behaviour on our workload; no published memory cost
  per column at realistic scale; no published operational
  experience. The blueprint even calls this out as a candidate paper
  ("Cache-resident approximate indexes for columnar lakehouses").
  That self-awareness is correct, but the risk of building a novel
  mechanism *in v1* is high.
- §7.4.3 (sort-order awareness) is pure metadata — cheap to build,
  directly uses Iceberg properties. Low risk.

**Recommendation.**

1. §7.4.1: **Keep.** Ship as an ops playbook driven by the trainer.
   Zero `shelfd` code.
2. §7.4.2: **Defer to v2**. The mechanism is novel; the research
   value of publishing it is high; but it does not belong in v1 where
   we are still proving the cache itself works. Critic §3 agrees.
3. §7.4.3: **Keep.** Minimal code; large payoff for already-sorted
   tables.

### 3.5 MV-aware caching + incremental MV refresh (§7.5, Phase 10)

**State of the art.**
- **Trino Iceberg MV auto-rewrite**: available pre-468 [18];
  freshness checks optimised more recently [42].
- **Trino incremental refresh**: PR #20959 merged 2024-06-20,
  milestone 451 — single-table, predicate-only, append-only [17].
- **DBSP** (VLDB '23 / VLDB Journal '25) [35][36] — general IVM for
  SQL including joins, aggregation, recursion.
- **Materialize** — differential dataflow on CDC streams (not Iceberg
  snapshots directly); McSherry et al. CIDR '13 foundations [37].
- **ClickHouse incremental MV** and **PostgreSQL pg_ivm** — operational
  but restricted subsets [43].
- **Iceberg `IncrementalAppendScan`** — API supports reading only
  files added between two snapshots [44].
- **Redshift AutoMV** — ML-driven MV creation + auto-refresh + auto-
  rewrite; vendor feature, not a paper [39].

**Blueprint's position (Phase 10, 8-12 weeks).** Build
`shelf-mv-refresh`: snapshot-watcher emits (table, snap_from →
snap_to) deltas; service reads delta files, computes incremental
aggregate, commits via Iceberg `MERGE`. Target: ≥ 20× speedup on 1 TB
fact-table MV.

**Gap.**
1. **Partial duplication of Trino PR #20959** for the simplest
   (predicate-only) case. Cannot be the publishable headline.
2. **The aggregating case (SUM/COUNT/GROUP BY) is the genuine gap.**
   This is what DBSP addresses. Shelf could contribute an
   Iceberg-snapshot-aware DBSP-style refresh, which would be
   publishable.
3. **This is a compute service, not a cache.** Critic §3 is right on
   this point; it belongs in a sibling project or upstreamed to Trino.

**Recommendation.**

1. **Remove Phase 10 from the cache roadmap.** Reframe MV-aware
   caching (§7.5.2) as "Shelf pins the data files of MVs just like
   any Iceberg file" — ~0 code.
2. **Keep the MV catalogue control-plane idea (§7.5.3)** as a thin
   addition. It is operationally useful (MV hit-rate visibility) and
   costs little.
3. **File a separate repo / project proposal** for
   `shelf-mv-refresh` if the team wants to pursue incremental
   aggregating MV refresh. Frame it against DBSP and Trino PR #20959.
   This is a real publishable contribution, but it is not a cache.

---

## 4. Proposed enhancements

Each enhancement below has at least one peer-reviewed paper or credible
industrial source backing it. Proposals are ranked by expected
impact-per-complexity, highest first.

### E1. Replace Raft with K8s-native membership + Rendezvous hashing

**Motivation.** `openraft` is pre-1.0 with chaos testing incomplete
[24]. Anna [33][34] demonstrated that data-plane KV stores can be
operated without consensus by moving coordination to a policy engine.
Rendezvous hashing [20] handles capacity-weighted placement without
a ring.

**Replaces.** `openraft` embedded in `shelfd`, 2000-vnode consistent
hash ring.

**Expected impact.** Eliminates election storms and snapshot-chunking
bugs during pod rotation; removes an entire failure class we have
already been bitten by in the Alluxio master quorum.

- p50/p95 latency: neutral.
- Hit rate: −0 to −1 pp during rotations (5 s DNS cache window
  adds transient mis-routes).
- Operator cost: meaningfully lower (no new consensus state
  transitions to learn).

**Risk.** Loses atomic multi-key updates (e.g. "pin 7 tables in one
transaction"). For a fail-open cache, we do not need this.

### E2. Default NVMe policy: S3-FIFO, not GL-Cache

**Motivation.** S3-FIFO (SOSP '23, Yang et al.) [6] achieves the
**lowest miss ratio on 10 of 14 datasets** among published policies,
with 6× LRU throughput, using 2 bits per object. It is **already in
Foyer** [25]. GL-Cache requires GBT training and per-group feature
collection; it is not in Foyer; no peer-reviewed evaluation on
analytical workloads exists.

**Replaces.** `pool.rowgroup` GL-Cache in §6.1 with Foyer's built-in
S3-FIFO.

**Expected impact.**
- Hit rate: within 3 pp of GL-Cache per S3-FIFO paper.
- Throughput: equivalent or faster vs GL-Cache.
- Engineering: saves 4-6 weeks of custom policy code.

**Risk.** If S3-FIFO turns out to be a poor match (unlikely based on
the trace evaluation), Foyer's policy is pluggable, so GL-Cache remains
a v1.1 upgrade path.

### E3. Size-threshold + pin-list admission for v1; LightGBM later if measured

**Motivation.** No peer-reviewed paper validates a 3-layer MLP over
size-threshold on analytical scans. LRB's CDN result [7] does not
transfer. GL-Cache's own ablation [2] shows GBT > MLP at the scale we
could train.

**Replaces.** §7.3 ONNX MLP admission.

**Expected impact.**
- Admission-bytes reduction: within 10 pp of the MLP target (80% per
  blueprint) on high-signal workloads; possibly more for
  low-cardinality access patterns where size is a strong proxy.
- Operator cost: far lower — the pin list is a JSON file in git.
- Dependency graph: no ONNX, no Python at serve time.

**Risk.** If the pin list gets stale it under-caches some hot tables.
Mitigation: trainer auto-generates top-N pin candidates; ops reviews
weekly.

### E4. Strike the 6 GB/s Arrow Flight number; benchmark HTTP/2 vs Flight on EKS

**Motivation.** The 6 GB/s number is not reproducible on cloud ENIs
[21][22]. Critic §2.3 is right to call this out.

**Replaces.** §8.1 claim; v1 protocol split.

**Expected impact.** Honest numbers in the launch blog. v1 ships
HTTP-only; measured thresholds determine when / whether to add Flight.

**Risk.** None — this is a documentation / scoping fix.

### E5. Rewrite §13 risks row about PR #26425; rewrite §7.2 Phase 2b-signal-2 around `QueryCompletedEvent.operatorSummaries`

**Motivation.** PR #26425 is closed; PR #26436 removed `splitCompleted`
entirely [15][16]. The blueprint's claim is the opposite of reality.
The replacement surface Trino itself recommends is
`QueryCompletedEvent`.

**Replaces.** §7.2 Phase 2b-signal-2; §13 risks row.

**Expected impact.** Converts a dead-on-arrival feature into a viable
one (post-hoc learning feeds the nightly trainer; Phase 2a's
file+footer prefetch on the *next* matching query becomes row-group
targeted). This is strictly weaker than in-query row-group push but
still delivers.

**Risk.** Phase 2b becomes *next-query-warmup*, not *this-query-push*.
We should not claim same-query row-group prefetch in v1.

### E6. Rescope Phase 10: build against Trino PR #20959 (single-table, predicate-only incremental refresh) — or drop

**Motivation.** Trino 451 already ships incremental refresh for the
simplest case [17]. The aggregating/multi-table case is the research
frontier, best framed against DBSP [35]. That is a compute project,
not a cache.

**Replaces.** §7.5 / Phase 10 as currently scoped.

**Expected impact.** Removes ~10 weeks of work from the cache
critical path. Optionally spawn `shelf-mv-refresh` as a sibling
repo/project; cite DBSP as the theoretical basis.

**Risk.** Loses the "close the Firebolt gap end-to-end" marketing
story — but the existing MV + Shelf-pins-MV-files story still closes
80% of it via OSS components (critic §3 agrees).

### E7. Add per-prefix rate limiting on the S3-fallback path

**Motivation.** S3 enforces per-prefix GET rate limits (historically
5,500 req/s per prefix). A thundering-herd fallback after all Shelf
pods die is plausible (critic §2.4). Alluxio's worker-level
retry-with-backoff absorbed this; Shelf does not specify equivalent
behaviour today.

**Replaces.** §9.4 failure-mode row (add thundering herd).

**Expected impact.** Prevents cache-wipe cascade from triggering S3
503s.

**Risk.** Minor — adds a per-prefix semaphore in the client path.

### E8. Add FrozenHot to the metadata tier — but only because it's easy, not because GL-Cache/SIEVE aren't enough

**Motivation.** FrozenHot [3] validates that for workloads with stable
hot sets (exactly Iceberg `metadata.json` + manifest list + Parquet
footer — they never change for a snapshot's lifetime), partitioning
into frozen + dynamic yields up to 5.51× throughput. The metadata
tier in Shelf is an ideal fit.

**Replaces.** Nothing; augments §6.1 `pool.metadata` / `pool.footer`
pools.

**Expected impact.** DRAM throughput on metadata hits increases; hot
path becomes lock-free for the frozen portion.

**Risk.** FrozenHot is library code (< 100 LoC integration per paper)
[3]; if Foyer does not ship a FrozenHot primitive, we'd need to port
it. Check Foyer first; if not present, this is a ~1-week spike.

### E9. Publish cache-density as a primary benchmark metric

**Motivation.** The 10-100× density claim in §7.1 is our novel data
point, and no peer-reviewed paper has reported it. Publishing it is
the OSS-launch headline.

**Replaces.** §10 benchmark set — add a primary "bytes cached / bytes
queried per query" metric.

**Expected impact.** Credible blog post; defensible OSS positioning
against Alluxio and C3.

**Risk.** The number might be less than 10× on our real workload; that
is *also* publishable and honest.

### E10. Add `fs.shelf.enabled` runtime toggle and a reference unit-tested Java circuit-breaker implementation

**Motivation.** Critic §1.4 / §1.9: the state machine is pseudocode.
Every downstream plugin user will re-implement it. Runtime-togglable
behaviour is what got Alluxio out of the `UfsIOManager` hole (we did
not need to redeploy pods).

**Replaces.** §9.5 pseudocode.

**Expected impact.** Operator confidence; external users can trust the
plugin without reading its source.

**Risk.** None beyond the engineering work.

---

## 5. Open questions for the critical thinker

These are the decisions science alone cannot resolve — they turn on
operational judgement, team capacity, and product priorities. Each is
actionable.

1. **Hit-rate gate vs feature gate for v0.5.** Critic proposes "match
   Alluxio's 71% hit rate or kill the project". Science agrees hit-rate
   parity is measurable, but hit-rate is not the only axis — granularity
   (bytes-cached/bytes-queried) and plan-aware TTFQ after scale-up are
   arguably more differentiated. Which gate does the team prefer: pure
   hit-rate parity, or a bundle (hit-rate + cache density + TTFQ)?

2. **Rendezvous HRW vs weighted consistent hash with no Raft.** Both
   eliminate `openraft`. HRW is simpler; weighted consistent hash has
   better minimum-disruption property on capacity-heterogeneous clusters.
   At our 5-10 node scale, HRW is indistinguishable. Confirm HRW as the
   v1 choice?

3. **Eviction policy default: S3-FIFO vs SIEVE.** Both are in Foyer.
   SIEVE wins on 45% of NSDI-'24 traces; S3-FIFO wins on 10 of 14
   SOSP-'23 dataset families. The decision is benchmark-first. Who
   owns replaying 30 days of `trino_logs` through each policy before
   v0.5 locks the default?

4. **Phase 2b-signal-2 replacement: `QueryCompletedEvent` for post-hoc
   learning only, or also file a Trino TIP?** The TIP is cheap
   (scientist can draft); the question is whether to *depend* on it
   for v1.0 or not. My recommendation: file the TIP, ship v1.0
   without depending on it.

5. **MV-refresh: drop, spin out, or defer?** Critic says drop. Science
   says the aggregating case is publishable (DBSP-adjacent) but does
   not belong in a cache. Recommend: spin out as a sibling project
   (separate repo, same license), keep §7.5.1/§7.5.2 as thin pinning
   logic in Shelf.

6. **Side-built blooms (§7.4.2): defer to v2, or drop entirely?** I
   call this publishable-if-built. Critic says defer. If deferred,
   does the team commit to revisiting in a year, or is this effectively
   abandoned?

7. **Result cache: Redis gateway (Phase 0 per COMPARISON.md) only, or
   build `shelf-result-cache` eventually?** The split between
   data-plane cache and result cache is validated by Snowflake [31].
   But shipping two binaries in the Shelf repo in parallel doubles the
   ops surface. Confirm Redis-only for v1?

8. **ONNX vs LightGBM vs hand-rolled MLP vs pure heuristic.** My
   recommendation and critic's match: heuristic now, LightGBM if
   measured gap in v1.x, never ONNX. Confirm?

9. **Target Trino version.** Blueprint says 480. Incremental MV
   refresh is in 451+. `splitCompleted` was removed in 477. Our
   clusters run 480 today. Any clusters still on older versions that
   the plugin must support? If yes, the SPI surface widens.

10. **Benchmark against Alluxio 2.9.5 (our current) or Alluxio 3.x
   DORA (the upgrade path we did not take)?** Critic §8 implicitly
   benchmarks against 2.9.5 (our real baseline). Some OSS readers
   will ask "but why not just use DORA?" The blueprint does not yet
   answer this in measurement form. Do we benchmark all three?

11. **Per-tenant quotas: by Trino resource group, by K8s namespace,
   or by catalog?** Blueprint §6.3 says resource group. §6.2's
   `shelf.tenant=replica-2` says replica. Pick one and use it
   consistently.

12. **Open-source license and governance.** Blueprint §11 says Apache
   2.0, CLA, BDFL 12 months → PMC. Critic does not weigh in.
   Confirm the CLA stance (some contributors object to CLAs; DCO is
   an alternative).

---

## 6. Bibliography

[1] Zhang, Yang, Yue, Vigfusson, Rashmi. *SIEVE is Simpler than LRU:
An Efficient Turn-Key Eviction Algorithm for Web Caches.* NSDI '24.
<https://www.usenix.org/system/files/nsdi24-zhang-yazhuo.pdf>

[2] Yang, Mao, Yue, Rashmi. *GL-Cache: Group-level learning for
efficient and high-performance caching.* FAST '23.
<https://www.usenix.org/conference/fast23/presentation/yang-juncheng>
and <https://www.pdl.cmu.edu/PDL-FTP/BigLearning/2023_FAST_GL_Cache.pdf>

[3] Qiu, Yang, Yue, Rashmi, et al. *FrozenHot Cache: Rethinking Cache
Management for Modern Hardware.* EuroSys '23.
<https://dl.acm.org/doi/10.1145/3552326.3587446> and
<https://www.pdl.cmu.edu/ftp/Storage/FrozenHot-Eurosys23.pdf>

[4] Berg, Berger, et al. *The CacheLib Caching Engine: Design and
Experiences at Scale.* **OSDI '20** (not SOSP).
<https://www.usenix.org/conference/osdi20/presentation/berg>
<https://www.usenix.org/system/files/osdi20-berg.pdf>

[5] Ananthanarayanan, Ghodsi, Wang, et al. *PACMan: Coordinated Memory
Caching for Parallel Jobs.* NSDI '12.
<https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/ananthanarayanan>

[6] Yang, Qiu, Zhang, et al. *FIFO queues are all you need for cache
eviction (S3-FIFO).* SOSP '23.
<https://dl.acm.org/doi/abs/10.1145/3600006.3613147>
<https://jasony.me/publication/sosp23-s3fifo.pdf>

[7] Song, Berger, Li, Lloyd. *Learning Relaxed Belady for Content
Distribution Network Caching.* NSDI '20.
<https://www.usenix.org/conference/nsdi20/presentation/song>

[8] Alluxio. *Original Hash Algorithms (DORA).* Vendor docs.
<https://www.alluxio.io/blog/consistent-hashing-in-alluxio-dora>
and *Core Concepts (DORA).*
<https://documentation.alluxio.io/ee-da-en/core-concepts>

[9] Weil, Brandt, Miller, Maltzahn. *CRUSH: Controlled, Scalable,
Decentralized Placement of Replicated Data.* SC '06.
<https://ceph.io/assets/pdfs/weil-crush-sc06.pdf>

[10] Apache Iceberg PR #6935 (closed, not merged).
<https://github.com/apache/iceberg/pull/6935>

[11] Apache Iceberg PR #6967 (closed, not merged).
<https://github.com/apache/iceberg/pull/6967>

[12] Apache Iceberg PR #10399 (merged; non-vectorized page skipping).
<https://github.com/apache/iceberg/pull/10399>

[13] Apache Iceberg PR #15211 (merged; vectorized page skipping).
<https://github.com/apache/iceberg/pull/15211>

[14] Trino PR #9955 — *Skip reading Parquet pages using Column Indexes
feature of Parquet for Iceberg.*
<https://github.com/trinodb/trino/pull/9955>

[15] Trino PR #26425 — *Add ability to load event listeners on
workers.* **Closed**, superseded by #26436.
<https://github.com/trinodb/trino/pull/26425>

[16] Trino PR #26436 — *Remove code for collecting
SplitCompletedEvent on workers.* **Merged 2025-08-19**, milestone 477.
<https://github.com/trinodb/trino/pull/26436>

[17] Trino PR #20959 — *Implement incremental refresh for single-table,
predicate-only MVs.* Merged 2024-06-20, milestone 451.
<https://github.com/trinodb/trino/pull/20959>

[18] Trino release 468 notes (17 Dec 2024).
<https://trino.io/docs/current/release/release-468>

[19] Trino PR #21602 — *Add Parquet bloom filter write support to
Iceberg connector.* Merged 2024-06-24.
<https://github.com/trinodb/trino/pull/21602>

[20] Thaler, Ravishankar. *A Name-Based Mapping Scheme for Rendezvous.*
UMich tech report 1996; modern summary:
<https://en.wikipedia.org/wiki/Rendezvous_hashing> and weighted HRW:
<https://www.ietf.org/archive/id/draft-ietf-bess-weighted-hrw-01.html>

[21] Apache Arrow issue #30728 — *Evaluate UCX/RDMA transport
performance.*
<https://github.com/apache/arrow/issues/30728>

[22] Arrow Flight serialization benchmark (raulcd gist).
<https://gist.github.com/raulcd/f139ccaaeb700a3ffe23a0c914e48c7c>

[23] Trino SPI `QueryMetadata` source, Trino 480.
<https://github.com/trinodb/trino/blob/89dc9346/core/trino-spi/src/main/java/io/trino/spi/eventlistener/QueryMetadata.java>
Javadoc for the `EventListener` interface at
<https://javadoc.io/static/io.trino/trino-spi/456/io/trino/spi/eventlistener/EventListener.html>

[24] openraft project status.
<https://github.com/databendlabs/openraft> and
<https://docs.rs/openraft/latest/openraft/>

[25] Foyer documentation — policies and architecture.
<https://foyer-rs.github.io/foyer/docs/design/architecture>
and <https://foyer-rs.github.io/foyer/docs/case-study/risingwave>

[26] Dremio. *How Dremio delivers fast queries on object storage
(C3).* Vendor blog.
<https://www.dremio.com/blog/how-dremio-delivers-fast-queries-on-object-storage-apache-arrow-reflections-and-the-columnar-cloud-cache/>

[27] Databricks. *Optimize performance with caching on Databricks
(Delta / disk cache).*
<https://docs.databricks.com/spark/latest/spark-sql/dbio-commit.html>

[28] Trino PR #14428 — *Enable reading Parquet bloomfilter statistics
for hive connector.* Merged Jan 2023.
<https://github.com/trinodb/trino/pull/14428>

[29] Apache Iceberg configuration — bloom filter write properties.
<https://iceberg.apache.org/docs/latest/configuration/>

[30] Microsoft ONNX Runtime benchmarking documentation.
<https://mintlify.com/microsoft/onnxruntime/performance/benchmarking>
and issue #15630 for CPU inference numbers:
<https://github.com/microsoft/onnxruntime/issues/15630>

[31] Snowflake documentation. *Using Persisted Query Results* and
*Virtual Warehouses.*
<https://docs.snowflake.com/en/user-guide/querying-persisted-results.html>
<https://docs.snowflake.com/en/user-guide/warehouses.html>

[32] *Effective Mimicry of Belady's MIN Policy* (Mockingjay).
<https://par.nsf.gov/biblio/10334308-effective-mimicry-beladys-min-policy>
And *Stormbird* (IEEE Access '25).
<https://ieeeaccess.ieee.org/featured-articles/cachereplacement_modern_bypass/>

[33] Wu, Faleiro, Lin, Hellerstein. *Anna: A KVS For Any Scale.*
ICDE '18. <https://dsf.berkeley.edu/jmh/papers/anna_ieee18.pdf>

[34] Wu et al. *Autoscaling Tiered Cloud Storage in Anna.* VLDB '19
extended journal version:
<https://link.springer.com/article/10.1007/s00778-020-00632-7>
PDF: <https://dsf.berkeley.edu/jmh/papers/anna_vldb_19.pdf>

[35] Budiu et al. *DBSP: Automatic Incremental View Maintenance for
Rich Query Languages.* VLDB '23.
<https://vldb.org/pvldb/vol16/p1601-budiu.pdf>

[36] Budiu et al. *DBSP: automatic incremental view maintenance for
rich query languages.* VLDB Journal (2025).
<https://link.springer.com/article/10.1007/s00778-025-00922-y>

[37] McSherry, Murray, Isaacs, Isard. *Differential Dataflow.* CIDR
'13. <https://www.cidrdb.org/cidr2013/Papers/CIDR13_Paper111.pdf>

[38] Jindal, Qiao, Patel, Yin, et al. *Computation Reuse in Analytics
Job Service at Microsoft (BigSubs).* **VLDB '18** (not SIGMOD '19).
<https://www.microsoft.com/en-us/research/wp-content/uploads/2018/03/bigsubs-vldb2018.pdf>

[39] AWS. *Automated materialized views — Amazon Redshift.*
<https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-auto-mv.html>
and *Refreshing a materialized view.*
<https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-refresh.html>

[40] Starburst. *Warp Speed / Smart Indexing.*
<https://www.starburst.io/platform/features/smart-indexing/>
and <https://www.starburst.io/press-releases/starburst-acquires-varada-to-deliver-the-new-standard-of-data-lake-analytics/>

[41] Trino 480 docs. *Object storage file formats — Parquet bloom
filter use.*
<https://trino.io/docs/current/object-storage/file-formats.html>

[42] Trino release 479 notes (14 Dec 2025).
<https://trino.io/docs/current/release/release-479.html>

[43] ClickHouse docs. *Incremental materialized view.*
<https://clickhouse.com/docs/materialized-view/incremental-materialized-view>
and PostgreSQL pg_ivm. <https://wiki.postgresql.org/wiki/Incremental_View_Maintenance>

[44] Apache Iceberg. *`IncrementalScan` / `IncrementalAppendScan`
API.* <https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/IncrementalScan.html>

---

*End of scientist review. Handoff to agents 2 (critical thinker —
already landed) and 3 (planner).*
