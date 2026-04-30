# BLUEPRINT-DIFF.md

Edits to apply to `shelf/BLUEPRINT.md` (currently v0.3, last edited
2026-04-23). Listed in **file order** so a single reviewer can apply
them in one pass. Sources: critical thinker §7, scientist corrections
(§1 venue + content errors, §2.6 Arrow Flight caveat, §4.10 consensus,
§4.2/4.5 eviction + hashing, §3.2 SplitCompletedEvent dead, §4.1
LightGBM over MLP).

**Version after applying:** v0.4 (minor bump — scope cut + algorithm
swap + phase restructure are all major amendments per the README's
"amendment flow").

---

## Header block (top of file)

- Update the `Status` line: `design blueprint v0.3` → `design blueprint v0.4`.
- Replace the `**v0.3 changes** ...` callout with a new `**v0.4
  changes**` paragraph summarising:
  - SplitCompletedEvent path removed (Trino PR #26436 merged 2025-08-19)
  - openraft dropped; K8s headless service + S3 ConfigMap pin list
  - ONNX MLP admission dropped in v1; size-threshold + pin list
  - Arrow Flight deferred to v1.x; HTTP/2 range-GET only in v1
  - `shelf-result-cache` dropped from v1 (COMPARISON Phase 0 Redis Gateway owns it)
  - Phase 10 incremental MV refresh dropped entirely (wrong project)
  - Timeline re-estimated to ~36-44 weeks for 3-person team
  - v0.5 gate added: must beat Alluxio on rep-2 for 7 consecutive days

## §1 TL;DR

- In the "Design choice" table, **drop the last row** ("Approximate
  in-cache indexes… Parquet bloom-filter recommender, side-built
  blooms, z-order awareness"). Move to Phase 8+ roadmap only.
- In the "Design choice" table, replace the `embedded Raft` row with
  "Rendezvous (HRW) hashing over K8s headless service membership; pin
  list + tenant quotas in S3-backed ConfigMap".
- In the "Design choice" table, replace "Learned admission (trained on
  `trino_logs`) + SIEVE eviction" with "Size-threshold admission + pin
  list from `trino_logs`; SIEVE (DRAM) + S3-FIFO (NVMe) via Foyer
  built-ins".
- In the "Design choice" table, replace "Rust cache plane; HTTP for <
  1 MB, Arrow Flight for ≥ 1 MB data plane; embedded Raft control"
  with "Rust cache plane; HTTP/2 range-GET for all payload sizes in
  v1; no embedded consensus".
- Target-metric paragraph: drop `≥ 20× direct S3 on hit, at 70-85% hit
  rate on our workload`. Replace with "hit rate comparable to the
  stabilised Alluxio 2.9.5 baseline (currently 71% on rep-2) at
  substantially lower operator surface; p50 scan latency within 20% of
  Alluxio on hit; fail-open to direct S3 on miss or Shelf fault".

## §2 Problem statement

- Add a bullet under the Alluxio pain list: "Alluxio on rep-2 is now
  stable (2026-04-23, post `UfsIOManager=256` + 3-master HA migration)
  at ≈ 71% hit rate. Shelf's v0.5 must beat this baseline; see §12."

## §4.3 Storage engine

- Fix the citation: `CacheLib (SOSP '20, Berg et al.)` → `CacheLib
  (OSDI '20, Berg et al.)`.
- Add a trailing sentence: "Foyer ships S3-FIFO (SOSP '23) and SIEVE
  (NSDI '24) as pluggable policies; we use them as-is rather than
  implementing custom policies."

## §4.2 Prefetch and admission

- Rewrite the PACMan bullet to emphasise "coordinated eviction/placement
  under an *all-or-nothing* parallel-job objective" rather than
  "coordinated admission" (per scientist §1).
- Add bullets citing Pythia (EDBT '25) and GrASP (preprint, 2025) as
  the modern counterparts validating plan-aware prefetch on OLAP
  workloads.

## §4.4 Distributed topology

- Append: "DORA is not peer-reviewed; the underlying primitive is the
  well-worn industrial pattern of consistent-hash / rendezvous (HRW)
  hashing + capacity weighting (Dynamo, Cassandra, Riak, Alluxio
  DORA). We cite it as convention, not as a novel contribution."

## §5 Non-negotiable design principles

- Reword principle 3 from "The engine pushes intent; the cache acts on
  it" to: "The cache exploits whatever plan and observation signal
  the engine exposes, never blocks the engine waiting for any signal."
- Reword principle 5 from "Open first, and genuinely multi-engine" to:
  "Wire protocol must be open enough that a non-Trino engine can
  adopt Shelf without Trino cooperation. Shipping non-Trino clients in
  v1 is explicitly out of scope."
- Add principle 8: **"Every RPC has a budget."** Every Shelf client
  call carries a hard timeout and falls open on expiry.
- Add principle 9: **"No unbounded queue."** Prefetch queue and
  training batch queue both have explicit upper bounds and documented
  overflow behaviour.
- Add principle 10: **"Every published metric has an SLO."**
- Add principle 11: **"Every config key is reloadable at runtime OR
  documented as restart-required."**
- Add principle 12: **"No new consensus systems without a failure case
  that demands them."**

## §6.1 Cache node (`shelfd`) internals

- **Router.** Replace "consistent hash ring over object-key-hash. 2000
  virtual nodes per physical node, capacity-weighted (NVMe size). Ring
  membership stored in Raft; any node can route." with "Rendezvous
  (HRW) hashing over K8s headless-service membership; capacity weights
  read from each pod's `/stats` endpoint; no ring data structure, no
  vnode count."
- **Storage — pool layout.** Simplify to **two pools for v1**:
  - `pool.metadata` — manifests + footers + page indexes, DRAM only,
    FrozenHot, 5% of DRAM quota.
  - `pool.rowgroup` — DRAM+NVMe hybrid via Foyer, S3-FIFO (Foyer
    built-in). Note: separating `rowgroup_hot` is deferred to v1.1
    pending measurement.
- **Admission policy.** Replace "On a miss, if the object is larger
  than `admission.size_threshold` (e.g. 8 MB), consult the learned
  admission model (ONNX file shipped by the trainer)" with: "On a
  miss, if the object is larger than `admission.size_threshold`
  (default 1 GiB), refuse admission unless the key matches a
  pin-list entry. A learned model (LightGBM, not ONNX) is a v1.x
  upgrade path conditional on a measured ≥ 5 pp hit-rate gap vs
  size-threshold alone; see §7.3."
- **Control RPC.** Note that `Evict` and `Pin` carry a per-tenant
  deadline and bounded queue depth.
- **Data RPC.** Replace "Arrow Flight" with "HTTP/2 range-GET". Arrow
  Flight moved to v1.x.

## §6.2 Client plugin

- Second-paragraph bullet 1 `ShelfFileSystem`: replace "picks protocol
  by size (HTTP for < 1 MB, Arrow Flight for ≥ 1 MB)" with "issues
  HTTP/2 range-GET over a pooled connection to the HRW-elected Shelf
  pod".
- Second-paragraph bullet 2 `ShelfPrefetchListener`: add explicit hard
  10 ms coordinator-side deadline; explicit "fire-and-forget, bounded
  queue" guarantee.

## §6.3 Control plane

- **Delete** the Raft sub-bullet ("Raft (`openraft` crate) inside the
  cache pods themselves. 3- or 5-node quorum; stores ring membership,
  pinned-table list, tenant quotas. No etcd required.")
- Replace with: "No embedded consensus. K8s headless-service provides
  membership. Pin list + tenant quotas live in a versioned S3-backed
  ConfigMap, pulled every 15 min and on SIGHUP. Trainer job writes
  the next version; ops reviews diffs via PR before publication."
- Trainer bullet 4: replace "Trains a 3-layer MLP (10 features →
  P(reaccess_within_1h)) and exports as ONNX" with "Builds a pin
  list sorted by `scanned_bytes × wall_time × frequency`, top-N per
  tenant; merged via ops-reviewed PR. LightGBM model is a v1.x
  optional component, not shipped in v1."

## §7.1 Columnar-range granularity

- Soften the "10-100× better cache density" claim to "typically 5-20×,
  up to 100× on narrow predicates over wide tables; measurement to be
  published from the 7-day `trino_logs` replay (SHELF-26)".

## §7.2 Plan-aware push prefetch

- **Major rewrite** of the Phase 2b section:
  - Delete the `SplitCompletedEvent learning` sub-bullet entirely.
  - Add a note that Trino PR #26436 (merged 2025-08-19) **removed
    `EventListener#splitCompleted`**; operators are directed to
    `QueryStatistics#getOperatorSummaries` at `QueryCompletedEvent` time.
  - Replace the SplitCompletedEvent bullet with: "Post-hoc learning
    via `QueryCompletedEvent.getStatistics().getOperatorSummaries()` —
    coarser than split-level, but live on all shipped Trino. Trainer
    aggregates `(query_sketch → operator_summaries)` nightly."
  - Keep plugin-side observation (signal-1) as the primary
    row-group-level prefetch mechanism.

## §7.3 Learned admission on large scans

- Rewrite to lead with **size-threshold admission** as the v1 default
  (refuse > 1 GiB unless pinned). Move the MLP text to a "v1.x
  possible upgrade" subsection. Change any reference to "ONNX" to
  "LightGBM" — tiny C runtime, ≤ 5 µs inference, no ORT dependency.
  Drop the unsourced "10-50 µs" latency claim.

## §7.4 Approximate in-cache indexes

- §7.4.1 (bring-your-own Parquet blooms): keep, but reframe as an
  "ops playbook" item with **no Shelf code** in v1.
- §7.4.2 (side-built blooms in `shelfd`): move to Phase 8 only.
  Remove "This is genuinely novel — no open-source analytical cache
  does this. Target paper: ..." — this is aspirational marketing,
  not a v1 commitment.
- §7.4.3 (z-order / sort-order awareness): move to Phase 8.

## §7.5 MV-aware caching

- Keep §7.5.1 note that the Redis + Trino-Gateway result cache in
  COMPARISON.md Phase 0 is **the** result cache for v1; `shelf-result-cache`
  is **not** built in v1.
- §7.5.2 (MV files pinned in hot pool): reframe as "Shelf caches MV
  files like any other Iceberg file; explicit MV pinning is a Phase 9
  item, not core v1 behaviour".
- §7.5.3 (Shelf as MV catalogue): move to Phase 9.

## §8.1 Data-plane

- Remove the "6 GB/s" Arrow Flight throughput claim. Replace with a
  sentence noting that the original DaMoN '22 number was measured on
  Mellanox InfiniBand; commodity EKS ENIs cap at 10-25 Gbps per node
  with per-stream throughput of 1-3 GB/s.
- Restructure to: **v1 HTTP/2 only** for all payload sizes. Arrow
  Flight is a v1.x upgrade contingent on a measured ≥ 20% throughput
  gain at our per-stream realistic bandwidth.
- Keep the `ShelfReadRequest` proto definition for eventual Flight use
  but note it is "reserved for v1.x".

## §8.3 S3-compatible shim

- Shrink scope explicitly: `GetObject` with `Range` header +
  `HeadObject` only. No `PutObject`, no `ListObjects`.

## §9.2 Comparison with Alluxio table

- Change the last row "Pool timeouts" — replace "design target: 0 by
  construction (SDK v2 pool + async)" with "explicit failure modes
  documented: Tokio task starvation under NVMe write pressure; Foyer
  write back-pressure; per-prefix origin-pool saturation. Mitigations
  itemised in §9.4."

## §9.4 Failure modes and how each degrades

- Add a new row: "All Shelf pods down simultaneously (cluster network
  partition or KEDA mass-rotation) — plugin falls through to direct
  S3; per-prefix rate limiter on the fallback path caps S3 GET rate
  at 5 000 / s / prefix to avoid `SlowDown` responses."
- Add a note that **per-prefix rate limiting on the fallback path is
  mandatory** and not optional.

## §9.5 Client resilience state machine

- Promote from "pseudocode in blueprint" to "committed Java reference
  implementation + unit tests ship in v0.1 (SHELF-11)". Reference the
  ticket ID.
- Add a sub-section noting: `hash_ring.owner_for(key)` inside the
  retry path must recompute against the **current** DNS-refreshed
  membership, not the one cached at the start of the request.

## §12 Roadmap

**Whole table rewrite.** Replace with the phase structure from the
plan (§3 of `03-plan.md`):

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
| **10** | **—** | **REMOVED** — incremental MV refresh is a compute service, not a cache (see ADR-0007). |

- Update the trailing paragraph: "Total ≈ 36-44 calendar weeks to OSS
  launch for a 3-person team. Phases 8 and 9 parallelise with Phase 7
  only if team expands to 4+."

## §13 Risks & open questions

- **Invert** the risk row that reads "Upstream PR #26425 already enables
  worker event listeners". Replace with: "Trino PR #26436 (merged
  2025-08-19) **removed** `EventListener#splitCompleted` entirely.
  Phase 2b redesigned to plugin-observation + `QueryCompletedEvent`
  operator summaries. Do **not** cite PR #26425 as if its direction
  prevailed."
- Replace the "Yet another cache" risk mitigation wording to:
  "Lead with numerically-measured comparison against the stabilised
  Alluxio 2.9.5 baseline on our real workload. Do not frame Shelf as
  an Alluxio-replacement in marketing; frame as an
  analytic-engine-aware cache with row-group granularity."
- Answer the 5 open questions at the end of §13 per the plan §8
  (project name TBD; repo home TBD; launch-post co-authors TBD; Apache
  donation deferred 12 months; Spark client not in v1).

## §13.5 Snapshot-aware keys

- Clarify that `shelf-result-cache` is **out of scope for v1**.
  Reference COMPARISON.md's Phase 0 Redis + Trino-Gateway result cache
  as the v1 result-cache shipping vehicle. Add "v2+" tag on the
  blueprint's own `shelf-result-cache` discussion.
- Note that `snapshot-watcher` is a COMPARISON Phase 0 deliverable (not
  net-new from Shelf) and is consumed by the Redis-Gateway cache in v1.

## §14 What Shelf is NOT

- Update the first bullet: `shelf-result-cache` is now **not in the
  repo for v1**. Rephrase to: "`shelfd` caches file-system bytes only.
  Result caching in v1 is handled by the separate Redis + Trino-Gateway
  plugin documented in COMPARISON.md Phase 0. A future `shelf-result-cache`
  binary is speculative, not a v1 artefact."
- Update the "Shelf is not an index" bullet: "Warp-Speed-style columnar
  indexes are a **Phase 8** experiment, not v1. In-cache side-built
  blooms (§7.4.2) are Phase 8+, not v1."

## §15.5 What we borrow

- Drop the row "Firebolt — Aggregating indexes — Out of scope for v1.
  Potential phase 8+ experiment" — we cut that experiment.
- Drop the row "Starburst Warp Speed — Block-level bitmap... — Phase 8+
  possibility" — move fully to Phase 8 in §12 instead of hinting here.
- Keep the rest.

## §16 Next step

- Update target latency: "p99 DRAM read ≤ 1 ms" → "p99 DRAM read
  1-3 ms, p99.9 10-50 ms under NVMe pressure" (honest tail-latency
  discussion per critic §2.3).

## Trailing metadata

- Update `Last edited: 2026-04-23. Owner: @aamir. Status: draft,
  seeking review.` to `Last edited: <date of diff application>.
  Version: v0.4. Owner: @aamir. Status: draft, seeking review.`

---

**Apply order:** top of file down. Every edit above is scoped to its
named section so a single reviewer pass is sufficient. After applying,
move `out/01-scientist-review.md`, `out/02-critical-review.md`, and
`out/03-plan.md` to `out/archive/v0.3/` per the amendment flow in
`agents/README.md`.
