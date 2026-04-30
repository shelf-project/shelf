# SHELF Tier-4 — Page-Index Pool: Post-B1 Evaluation Memo

Post-B1 decision memo for the Tier-4 page-index pool. Plan §5 marks this
as *"defer; evaluate after compression lands"*. **B1 has now shipped**
(PR-merged on `main`, todo `tier1-compression` is `COMPLETED`), so the
gate is unblocked and this memo records the post-gate decision.

## TL;DR

**Recommendation: defer indefinitely.**

After B1 zstd compression on the rowgroup pool, page-index objects
compress as well or better than rowgroup data — page-index is
dictionary-encoded varint metadata (`OffsetIndex` / `ColumnIndex`
structures, mostly small integers and short repeating strings), giving
**~3–5× compression ratio** vs the **~1.4–2.5×** typical for rowgroup
data. Isolating page-index into a third Foyer pool gains marginal
eviction-control at the cost of:

- an additional cache-pool config surface (one more `pool.*` block in
  `shelfd.toml`),
- an additional `ConfigMap` knob in the Helm chart,
- another set of Foyer init / admission paths in `store.rs`,
- another set of `shelf_*{pool="page_index"}` metric labels and the
  dashboard panels that go with them.

Revisit only if a future SHELF-XX advisor identifies a specific table
family where page-index thrash dominates (>20 % of metadata-pool
evictions tagged `page_index`). Until then, page-index continues to
share `pool.rowgroup` and benefits from B1's compression directly.

## What "page-index" means in this context

Iceberg tables are stored as Parquet files; each Parquet file's footer
optionally carries a **page-index** sidecar — the `OffsetIndex` and
`ColumnIndex` structures defined in the Parquet format spec. These let
a reader skip Parquet **pages within a row group** (a strictly finer
granularity than the row-group-level skip you get from column
statistics in the file footer).

For Iceberg-on-Trino on this stack, the page-index is fetched as a
small byte-range read from the Parquet file (typically the tail of the
file, contiguous with the footer), keyed by ETag in the
content-addressed cache (ADR-0011). It is **not** a separate object on
S3 — it is bytes inside the Parquet file — but in the cache it is a
distinct hot range that sees a different access pattern from row-group
bodies.

## Current state

- Page-indexes share `pool.rowgroup` (per plan §5 Tier-5 #2).
- They are byte-range fetches over the same ETag-keyed,
  content-addressed cache (ADR-0011); same admission path, same
  eviction policy as everything else in `pool.rowgroup`.
- **With B1 now live**, NVMe effective capacity is **+60–150 % on
  rowgroup** (compression ratio range observed during B1 canary). The
  marginal eviction pressure that motivated isolating page-index into
  its own pool — namely "page-indexes are evicted because rowgroup
  bodies fill the pool" — no longer applies at constant pod count.
- Page-indexes themselves compress *better* than the bulk rowgroup data
  they describe, because they are dominated by varint-encoded offsets
  and short repeating column-name strings — exactly the inputs zstd's
  default dictionary handles best.

## Decision criteria for revisit

Three conditions must hold **simultaneously** to fund this work:

1. **Concentration.** SHELF-60 + SHELF-61 measurement shows page-index
   access patterns concentrate on a single table family — i.e.
   **>40 % of metadata-pool reads** are tagged on one schema. Without
   concentration, isolating into a dedicated pool just shifts global
   eviction noise from one pool label to another.
2. **Residual eviction pressure.**
   `shelf_evictions_total{pool="metadata", reason="capacity"}` rate
   after B1 still exceeds **1 k / min on at least 2 replicas** for a
   sustained ≥ 24 h. (Spiky eviction during a single dbt-run window
   does not count.)
3. **No cheaper writer-side fix.** SHELF-52 bloom-write advisor
   (PR #70) does **not** also cover the same table family — i.e. the
   win cannot be obtained more cheaply via "have the writer enable
   page-index / better stats / writer-side blooms" rather than
   "shelfd carries another pool".

If any one of the three fails, the work stays deferred.

## Effort if revisited

**M (~1.5 wk)** assuming the rest of the cache-pool plumbing is
untouched:

- New `pool.page_index` config block in `shelfd.toml` and the chart
  `values.yaml` (capacity, eviction policy, admission policy).
- New Foyer pool init in `store.rs` mirroring `pool.metadata` /
  `pool.rowgroup`.
- New admission path that decodes `OffsetIndex` to derive byte ranges
  routed into the new pool (the simple "always promote page-index
  reads" variant; an LRU-with-page-index-bias variant is a follow-up).
- New chart values + Helm template wiring.
- New `shelf_*{pool="page_index"}` metric labels everywhere (Prom
  recording rules, Grafana panels, dashboards in plan §10).
- ADR amendment (or new ADR) noting the third-pool decision.

The cost is mostly config-surface and dashboard-churn, not algorithmic.
That's why the bar to fund it is "concentration + residual pressure +
no cheaper alternative" rather than "page-index reads happen": the
work itself is mechanical, but the maintenance tax is permanent.

## Status

**Closed-deferred.** Re-open only when all three decision criteria
above are met simultaneously.

**Owner:** cost-plan orchestrator.
