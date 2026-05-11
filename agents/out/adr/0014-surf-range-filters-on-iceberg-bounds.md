# ADR 0014: SuRF range filters on Iceberg `lower_bounds` / `upper_bounds` (research spike)

*Status: Proposed (2026-05-04)*
*Deciders: shelf BDFL + reviewers TBD*
*Supersedes: none*
*Superseded-by: none*
*Source: T5 of `analyst_report_validation_rc9_plan_de82494e.plan.md`*
*Related: ADR-0011 (cache-key spec — content-addressed by ETag), [PR #46 SHELF-46 bloom-aware admission](https://github.com/shelf-project/shelf/pull/46) (gates this work on the same Tier-1 measurement substrate)*

## Context

Today shelf accelerates the Trino + Iceberg + Parquet read path by caching byte ranges (rowgroup pool) and metadata blobs (metadata pool). The pruning decision — *"do I need to read this rowgroup at all?"* — happens **on the Trino side**, using:

1. **Iceberg manifest stats** (`lower_bounds` / `upper_bounds` per data-file per column, embedded in the manifest entries — Trino reads them out of the cached manifest file)
2. **Parquet column statistics** (per-rowgroup min/max in the Parquet footer — Trino reads them out of the cached footer)
3. **Parquet bloom filters** (per-rowgroup bloom bytes in the Parquet file body — Trino has to fetch the bloom bytes; SHELF-46 caches these so the second predicate-pushdown query against the same key set doesn't re-fetch)

What the pipeline *does not* do today is build a richer pruning data structure ON TOP of the Iceberg manifest stats. Iceberg's per-file `lower_bounds`/`upper_bounds` are scanned linearly per query — fine when there are 100 files, painful when there are 50 000 (rep-1's `cdp.icesheet.`* tables hit this regime). For range predicates (`WHERE col BETWEEN x AND y`), this is a real planner cost.

**SuRF** (Succinct Range Filter, SIGMOD 2018) is a trie-based probabilistic index that supports point lookups *and* range queries with a tunable false-positive rate. Built once per partition / table from the existing `lower_bounds` / `upper_bounds`, it gives Trino a fast pre-pruning filter: *"are there any rowgroups in [x, y]?"* in O(log range) time without scanning the manifest.

The analyst's rc.9 report flagged SuRF as the highest-leverage *new* idea (not currently in any shelf design doc, complementary to SHELF-46 bloom caching).

## Decision (proposed — pending Belady-equivalent evidence)

**Implement a SuRF-backed range filter as an opt-in advisor module in `shelf-advisor*`* (the existing companion crate, see `crates/shelf-advisor/`), exposed to Trino via a thin SPI on top of the SHELF-37 Iceberg event listener once that lands. This ADR scopes the design; implementation is gated on:

1. **Belady-replay evidence (Tier-2 gate)** that range predicates are a meaningful cost on the rep-1 trace — measured as % of total `physical_input_bytes` attributable to queries with `BETWEEN` / range filters in their plan. Threshold: ≥ 15 % of bytes.
2. **SHELF-37 Iceberg event listener jar** lands (currently parked on JDK 25 — workspace memory — making the listener-driven build trigger unavailable until then). A polling fallback is acceptable for v0 (read manifests from shelfd directly), with the listener-driven path being the v1 efficiency win.

Both gates align with the Tier-1 measurement-substrate ordering rule from `shelf-cost-reduction-research_*.plan.md` — no production default-flip until the measurement substrate (SHELF-37 + SHELF-40 + SHELF-42) is live.

## Open design questions answered

### Q1. Probe location — in shelfd, in the SHELF-37 listener, or in shelf-advisor?

**Answer: shelf-advisor.** Three reasons:

- shelfd's hot path is the data plane; adding a trie probe there would balloon p50 latency for every GET, even when the query has no range predicate.
- The SHELF-37 listener is JVM-side and runs in-process with Trino, so a SuRF probe there means shipping the trie into the JVM heap on every coordinator — bad fit because the trie is large (~1–10 MiB per partitioned table).
- shelf-advisor is already the home for "build-once, consult-many" structures (it's the F2 LightGBM admission home in the cost-reduction plan and a natural advisor companion for the Trino plugin). The Trino plugin makes a side-car HTTP call to shelf-advisor for "should I prune this rowgroup?" advice.

### Q2. Build trigger — Iceberg snapshot transition, lazy on first range query, or scheduled?

**Answer: snapshot-transition (preferred), with a lazy fallback.**

- **Preferred path (post-SHELF-37):** the listener fires on `OperationType.REPLACE` (compaction) or `OperationType.APPEND` (new data). shelf-advisor subscribes, reads `removed_data_files` + `added_data_files` from the snapshot JSON, incrementally rebuilds the trie for the affected partitions only.
- **Fallback (pre-SHELF-37 OR for tables not yet listener-wired):** lazy build on the first range-predicate query against the table. Cache the built trie in shelf-advisor's local DRAM with a TTL of one Iceberg snapshot (invalidate on snapshot-id change observed via periodic poll — every 60 s, identical to the SHELF-21 freshness loop).

### Q3. Fail-open semantics

**Strict fail-open.** SuRF is an *optimization*, not a *correctness* primitive — a bug in the trie code would silently drop rowgroups, returning incomplete results. The shelf-advisor `/range-prune?table=X&col=Y&lo=A&hi=B` endpoint returns:

```json
{ "decision": "prune" | "scan" | "unknown", "trie_age_seconds": N, "fpr_estimate": 0.001 }
```

The Trino plugin treats `unknown` and any HTTP error as `scan` (the safe choice — incurs the existing manifest scan; same cost as today). The plugin only acts on `decision: "prune"` when:

- `trie_age_seconds < 600` (10 min — guards against advisor lag during snapshot churn)
- `fpr_estimate < 0.01` (1 % — anything higher and the probability of a false-prune crossed with multi-column predicates becomes unsafe)

If either gate fails, treat as `unknown`. A correctness regression mode is structurally impossible here.

### Q4. Integration with existing `parquet_meta.rs` and `freshness.rs` modules

- `parquet_meta.rs` stays unchanged — it caches Parquet *column-level* stats from the file footer, which is per-rowgroup. SuRF is per-partition (one level above). They're complementary: SuRF prunes the *file* candidate set; `parquet_meta.rs` prunes the *rowgroup* candidate set within survivor files.
- `freshness.rs` provides the snapshot-id polling primitive that the lazy fallback re-uses (60 s loop already implemented). The snapshot-id change → rebuild trigger lives in shelf-advisor's listener path, not in shelfd's freshness module.

### Q5. Per-table memory cost — is SuRF small enough to keep all hot tables in DRAM?

**Estimate (back-of-envelope, needs measurement):** SuRF's published numbers from the SIGMOD 2018 paper are ~10 bits per key in the FST encoding for 64-bit keys with ~1 % FPR. For an Iceberg table with 50 000 data files × 4 bounds-tracked columns × 2 bounds (lo/hi) = 400 000 keys ⇒ ~500 KiB per table. Even 1 000 active tables ⇒ ~500 MiB total — fits in shelf-advisor DRAM with headroom.

For larger tables (10× the file count), zstd-3 compression on the trie storage drops it another ~2× — workspace SHELF-B1 protocol applies if we choose to spill to NVMe.

## Consequences

**Positive:**

- For tables hit by frequent range predicates (Iceberg time-partitioned BI workloads, the rep-1 PowerBI signature), expected planner-time savings 20–40 % on a clear-cut hit (range filter excludes ≥ 80 % of files).
- Composes with SHELF-46 bloom caching: SuRF prunes *files*, SHELF-46 prunes *rowgroups* within survivor files, no overlap.
- shelf-advisor stays a sidecar — zero impact on shelfd's data-plane code or hot path.

**Negative:**

- Trie build cost on snapshot transition is non-zero (~100 ms per partition for the file counts above). Mitigated by incremental build on `removed_data_files` / `added_data_files` diff, not full rebuild.
- Adds a new HTTP dependency from the Trino plugin (the sidecar `/range-prune` call). Fail-open semantics make this safe but surface a new latency budget — measure p99 of the sidecar call, target ≤ 5 ms.
- Trino plugin needs a new code path for the prune-advice consultation, which is a non-trivial Java change (~300 LOC + tests). Workspace memory: the plugin already calls into the FS factory; adding an advisor call is structurally similar.

**Neutral:**

- Defers the SHELF-37 listener dependency; v0 polling fallback is acceptable for the launch.

## Out of scope (in this ADR)

- The Trino-side Java plugin changes (separate ADR + PR once the shelf-advisor side is proven on a benchmark).
- Multi-column SuRF (cross-column range queries) — v0 builds one trie per column per partition; combining is at the Trino-plugin level.
- SuRF over non-numeric types (strings, timestamps) — covered by the surf-rs crate's lexicographic ordering, but specific Iceberg type-coverage tracking is left for the implementation ticket.

## Validation gate (before promoting v0 → v1)

1. **Belady-replay measurement** on rep-1 trace: % of `physical_input_bytes` attributable to range-predicate queries — must be ≥ 15 % to justify the implementation cost.
2. **Synthetic micro-bench** in shelf-advisor: build trie for a synthetic 50 000-file Iceberg table, measure (build-time per partition, memory size, query latency p99). Targets: ≤ 200 ms build, ≤ 1 MiB per table, ≤ 5 ms query p99.
3. **End-to-end shadow test** on the dev cluster: the Trino plugin makes the `/range-prune` call but treats every response as `scan` (ignore the answer). Measure: HTTP latency overhead p99, sidecar uptime under load. Target: < 5 ms p99 overhead, > 99.5 % uptime over a 24 h window.

Only after all three gates pass does the plugin start *acting* on `decision: "prune"`.

## Decision record

- **Author:** Shelf BDFL (T5 of analyst-report-validation rc9 plan)
- **Date:** 2026-05-04
- **Reviewers:** TBD — this ADR is a research spike; the implementation ticket (SHELF-NN, to be filed post-Belady-measurement) is where reviewers sign off on the plan-of-record.
- **References:**
  - SuRF paper: Zhang et al., "SuRF: Practical Range Query Filtering with Fast Succinct Tries" (SIGMOD 2018)
  - `surf-rs` crate (Rust impl): [https://crates.io/crates/surf](https://crates.io/crates/surf)
  - ADR-0011 (cache-key spec) — content-addressed keys mean SuRF doesn't break cache-correctness invariants
  - PR #46 (SHELF-46 bloom-aware admission) — sister technique gated on the same measurement substrate

