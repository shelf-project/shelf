# SHELF-52 — bloom-write advisor (design note)

| Field | Value |
| --- | --- |
| Status | Implemented (this PR) |
| Crate | `shelf-advisor` (workspace member; not under `crates/`) |
| Tier (cost-reduction plan) | Tier-3 |
| Plan link | `/Users/aamir/.cursor/plans/shelf-cost-reduction-research_97107ffb.plan.md` § Tier-3 #9 |
| Related design note | `agents/out/SHELF-52-bloom-advisor.md` (original 2026-04-25 draft) |
| Depends on | SHELF-37 (event-listener jar, PR #66 — open), SHELF-46 (footer admission, PR #50 — open), SHELF-40 (`Cents` newtype, PR #68 — open) |
| Blocks | Tier-4 SHELF-G2 (`shelfd::side_bloom`) — wired only if SHELF-52 shows > 30 % of cost in no-writer-bloom tables |

## Why a separate recommender from SHELF-46

SHELF-46 (PR #50) is the **runtime** side of bloom filtering: when
shelfd's `parquet_meta::extract_footer_ranges` discovers a
`FooterRangeKind::BloomFilter` blob in a Parquet footer, it admits
the bloom blocks into `Pool::Metadata`, where Trino's predicate
pushdown reader subsequently finds them and skips matching row
groups. That win only fires for tables whose Parquet writers
**already** emitted bloom filters. Most analytics tables in
production were written without them — Trino didn't ship native
bloom-write until [trinodb/trino #20662](https://github.com/trinodb/trino/pull/20662)
(merged 2024-04-16, in 445; the originating cluster runs Trino 480
so it is available, just not retroactively applied).

SHELF-52 is the **upstream** lever: identify tables where rewriting
with bloom-filter columns would pay back the rewrite cost in a
defensible number of queries, and emit a JSON recommendation an
operator can apply. The two tickets close the loop:

- SHELF-46 caches blooms when present.
- SHELF-52 recommends *creating* them.

## Detection algorithm

```text
read_window(QueryRecord, lookback)
    │
    ▼
aggregate_by_table  (BTreeMap<table, TableStats>)
    │
    ▼
filter:  query_count >= min_query_count
         avg_input_bytes >= min_query_bytes
    │
    ▼
rank_columns  (regex over query_text → BTreeMap<col, freq>, top-N)
    │
    ▼
for each candidate:
    selectivity = ndv ? clamp(1/ndv) : default_selectivity (0.1)
    saving_per_query_bytes = avg_input_bytes * (1 - selectivity)
    rewrite_bytes = 2 * table_total_bytes
    payback_queries = rewrite_bytes / saving_per_query_bytes
    severity = bucket(payback_queries)
    confidence = severity.confidence()
```

### Why ≥ 50 queries × ≥ 1 GiB (default)

- `min_query_count = 50` filters out one-off ad-hoc scans. A 50-query
  threshold over a 7-day window is roughly "queried once per
  business-hour during one work-week" — enough for the recommendation
  to clear seasonality noise.
- `min_query_bytes = 1 GiB` is the per-query physical-input cutoff
  below which a bloom filter cannot meaningfully repay even a half-GiB
  rewrite. Iceberg writes typically sit at ~128 MiB per Parquet file,
  so 1 GiB ≈ 8 row-group fetches per query — coarse enough that the
  bloom-skip math is honest.

Both are tunable per `BloomWriteConfig` and via
`shelf-advisor/config.example.yaml`.

## Column ranking — regex caveat (honest limitations)

The recommender extracts column names from
`QueryRecord::query_text` via a single configurable regex (default in
`config::DEFAULT_PREDICATE_COLUMN_REGEX`):

```regex
(?i)\b(?:WHERE|AND)\s+(?:[a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s*=
```

This is a **deliberate heuristic, not a SQL parser.** Concretely, the
following are silently missed:

| Pattern | Why missed | Operator workaround |
| --- | --- | --- |
| Function-wrapped predicates (`WHERE lower(col) = 'x'`) | LHS is not a bare identifier | Tighten the regex per cluster, or move to `sqlparser-rs` post-v1 |
| CTE inlining (`WITH t AS … SELECT * FROM t WHERE …`) | The `WHERE` references a CTE projection, not the base table | Operator deduplicates the column list before applying |
| Subquery predicates (`WHERE x IN (SELECT …)`) | Not an equality of literal | Bloom filters help only on equality; this is correctly out-of-scope |
| `BETWEEN` / range predicates | Not equality | Out-of-scope for bloom |
| Comments (`-- WHERE col = 'x'`) | Regex matches inside comments | Strip comments upstream if seen in practice |

The recommendation rationale carries an explicit `regex_caveat`
string that surfaces the same warning to the operator-facing JSON.

## Cost-savings math

```
expected_bytes_saved_per_query = avg_input_bytes * (1 - selectivity)
selectivity = 1 / NDV  (clamped to [1e-6, 0.99])  if Iceberg manifest exposes NDV
            = BloomWriteConfig::default_selectivity (0.1)  otherwise
```

`selectivity ≈ 1 / NDV` is the standard equality-predicate fan-out
estimate. The clamp prevents `NDV = 1` (or 0) from collapsing the
projection to zero saving, and it prevents arithmetic from generating
a negative saving on a pathological NDV.

When NDV is unavailable — which is the case **today** because
`iceberg-rust` is not yet in the workspace dependency tree (verified
in `Cargo.toml` / `Cargo.lock` at branch creation) — the recommender
falls back to the `default_selectivity = 0.1` figure called out in
the original SHELF-52 draft. This is conservative: 0.1 selectivity
projects a 90 % bytes-skipped per query, which is on the high end
of what Trino actually achieves with writer-side blooms in
production. Once `IcebergManifestReader::ndv()` is wired
(post-SHELF-53), the same recommender uses the real number and the
recommendation evidence's `selectivity_estimate.comment` flips from
`"NDV unavailable — falling back to BloomWriteConfig::default_selectivity"`
to `"1 / NDV from Iceberg manifest stats"`.

### Trade-off if NDV is permanently unavailable

If `iceberg-rust` never lands and operators want a more honest
selectivity estimate, the SHELF-53 follow-up should add a
sample-driven path: pull a small sample of `column = literal`
selectivity from `cdp.trino_logs.trino_queries` operator summaries
post-SHELF-37 (`bytes_read_externally / total_input_bytes` per
query). That number is per-query, not per-(column, value), but it
is closer to ground truth than the 0.1 default.

## Rewrite cost / payback

```
rewrite_bytes = 2 * table_total_bytes  // read + write for OPTIMIZE
rewrite_cents = Cents::from_bytes_rewrite(rewrite_bytes,
                                          cost_cents_per_gib)
payback_queries = rewrite_bytes / saving_per_query_bytes
```

The payback formula intentionally divides **bytes by bytes** so it
stays defensible when no `Cents` tariff is wired. The `rewrite_cents`
figure is informational (it lands in evidence + `suggested_change`)
but is **not** used to decide severity.

`Cents` itself is a local stub in `shelf-advisor::cost`. The shape
matches SHELF-40 / PR #68's `crates/shelf-cost::Cents`; the swap
post-merge is `s/crate::cost::Cents/shelf_cost::Cents/` plus a
workspace-member dependency line. Until then, the
`cost_cents_per_gib = 4` default is a placeholder anchored on the
"PUT/GET request charges + a small compute pad" math in
`cost.rs`, **not** a measured benchmark — the recommendation
evidence labels the `rewrite_cost_cents` row with
`"placeholder tariff (see SHELF-52 design note); replaced by
shelf_cost::Cents once PR #68 lands"`.

## Severity ladder

| Bucket | `payback_queries` | Confidence | Operator action |
| --- | --- | --- | --- |
| `critical` | < 100 | 0.75 | Apply this sprint — pays back inside a single busy day on a hot table. |
| `warn` | 100 ≤ × < 1000 | 0.625 | Review next sprint — pays back inside a typical week. |
| `info` | ≥ 1000 | 0.5 | Long tail — keep the recommendation visible, no urgency. |

Confidence values are deliberate exact-binary-fraction f32s
(`0.75 = 3/4`, `0.625 = 5/8`, `0.5 = 1/2`). This is **not** cosmetic:
the `Recommendation::confidence` field is f32; an "ordinary"
confidence like `0.65` would extend losslessly to a different f64
than the `0.65` parsed from JSON, breaking the
`serde_json::Value`-based snapshot test. The exact-binary values
round-trip cleanly between f32 and f64.

## Recommendation output shape

```json
[
  {
    "recommendation_type": "bloom_write",
    "table": "cat.s.t",
    "confidence": 0.625,
    "rationale": {
      "id": "bloom_write_cat.s.t",
      "severity": "warn",
      "columns": [{"column": "user_id", "frequency": 60}],
      "evidence": [
        {"metric": "query_count", "value": 60, "threshold": "min_query_count", "comment": "…"},
        {"metric": "avg_input_bytes", "value": 2147483648, …},
        {"metric": "selectivity_estimate", "value": 0.1, …},
        {"metric": "saving_per_query_bytes", "value": 1932735283, …},
        {"metric": "rewrite_bytes", "value": 214748364800, …},
        {"metric": "rewrite_cost_cents", "value": 800, …},
        {"metric": "payback_queries", "value": 111, "threshold": "100/1000", "comment": "…"}
      ],
      "regex_caveat": "Predicate columns are extracted via a configurable regex over raw SQL text (BloomWriteConfig::predicate_column_regex). CTE inlining, function-wrapped predicates, and subqueries are silently missed; review the column list before applying.",
      "tier4_link": "If >30% of advisor cost concentrates in tables with no writer-side blooms, this recommender's output gates Tier-4 SHELF-G2 (`shelfd::side_bloom`) — see plan §Tier-4."
    },
    "suggested_change": {
      "action_yaml": "-- SHELF-52 bloom-write recommendation\nALTER TABLE cat.s.t SET PROPERTIES (\n  'write.parquet.bloom-filter-columns' = 'user_id'\n);\nALTER TABLE cat.s.t EXECUTE optimize;\n",
      "rewrite_bytes": 214748364800,
      "rewrite_cost_cents": 800,
      "rewrite_cost_dollars": "$8.00",
      "payback_queries": 111,
      "severity": "warn"
    }
  }
]
```

## Post-rewrite cold-miss interaction (ADR-0011)

A successful `ALTER TABLE … SET PROPERTIES` + `EXECUTE optimize`
sequence produces **new Parquet files** with **new ETags**. Per
ADR-0011 (`agents/out/adr/0011-…`) shelf cache keys are
`sha256(etag || offset || length || rg_ordinal)`, so the existing
NVMe + DRAM entries become **unreachable orphans** the moment the
rewrite commits — Foyer evicts them on capacity, but the working
set transiently doubles.

Concretely, the post-rewrite morning resembles the
"post-`EXECUTE optimize` 100 %-miss morning" SHELF-45 / SHELF-63
was built to fix. The bloom-write recommendation should therefore
**always** be paired with one of:

1. SHELF-45 / SHELF-63 compaction-aware re-warm reactor (preferred —
   the reactor pre-fetches the new files so the cold-miss morning
   never lands).
2. A manual pin-list pre-warm via `tools/gen_pin_list.py` + the
   shelfctl `prewarm` path before the operator schedules the
   `EXECUTE optimize`.

The recommendation's `tier4_link` evidence row is the
operator-facing reminder that bloom-write isolation isn't free.

## Tier-4 SHELF-G2 gate (`shelfd::side_bloom`)

Per the cost-reduction plan §Tier-4 #1 ("wire `side_bloom.rs` +
`filter_service.rs` only if SHELF-52 shows > 30 % of cost in
no-writer-bloom tables"), this advisor is the gating signal for the
side-built bloom escape hatch. The decision flow:

1. Run `shelf-advisor analyze --window 30d` post-SHELF-37 cutover.
2. Sum `rewrite_cost_cents` across all `bloom_write`
   recommendations whose tables also report
   `shelf_bloom_admit_total{kind="bloom_block"} ≈ 0` per
   shelfd `:9090/metrics` (the SHELF-46 absence signal).
3. If that sum > 30 % of the cluster's total
   `shelf_s3_dollars_saved_total` rate (post-SHELF-40), Tier-4
   SHELF-G2 is in scope. Otherwise it stays gated.

## Test plan

| Test | Location | Coverage |
| --- | --- | --- |
| Column ranking (multi-record, table-qualified, fallback to pre-extracted, top-N truncation, table filter, invalid regex) | `bloom_write::tests` | 7 cases |
| Cost-savings projection at selectivity 0.0 / 0.1 / 0.5 / 0.9 / 1.0 | `bloom_write::tests` | 5 cases |
| Severity threshold boundaries (0, 99, 100, 999, 1000, MAX) | `bloom_write::tests` | 4 cases |
| Selectivity from NDV (clamping, missing-NDV fallback, NDV=0) | `bloom_write::tests` | 4 cases |
| End-to-end candidate filter (skip-low-volume, qualifying table, NDV usage) | `bloom_write::tests` | 3 cases |
| Determinism (in-module) | `bloom_write::tests` | 1 case |
| Snapshot vs committed fixture | `tests/it_bloom_write.rs` | 1 case (Value-equality) |
| Determinism (integration) | `tests/it_bloom_write.rs` | 1 case (byte-equality) |
| Integration smoke (gated `SHELF_INTEGRATION=1`) | `tests/it_bloom_write.rs` | 1 case (noisy skip when unset) |

Total: **27 cases** (22 unit + 3 integration + 2 doc-only).

The integration test uses the same fixture as the snapshot until
SHELF-46 (PR #50) lands and a synthetic shelfd-with-footer-admission
docker-compose target is available. The honest framing of that
substitution is in the test's module doc.

## Out of scope (for follow-ups)

- Auto-removal recommendations for over-bloomed columns (`bloom_remove`).
- Trino-specific SQL parser (`sqlparser-rs`) replacing the regex.
- Iceberg sample-stats path when `iceberg-rust` is unavailable
  (mentioned above; tracked as SHELF-53 follow-up).
- Bytebase / dbt-cloud / GitHub Action MR opener — explicitly
  rejected per SHELF-53 ADR-0013 ("advisor JSON-only").
- ONNX / LightGBM model-driven scoring — explicitly rejected per
  ADR-0003 (size-threshold admission over ONNX MLP); the SHELF-52
  advisor stays heuristic.

## Plan checkbox

- [x] SHELF-52 bloom-write advisor — implemented in
  `shelf-advisor/src/recommenders/bloom_write.rs` with config,
  tests, and design note.
