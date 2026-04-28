# SHELF-52: Bloom-write advisor extension (`shelf-advisor` add-on)

**Status:** Draft
**Tier:** S
**Estimated effort:** M
**Depends on:** SHELF-37, SHELF-53
**Blocks:** none

## Problem (OSS-cited)

Trino landed bloom-write in PR [trinodb/trino #20662](https://github.com/trinodb/trino/pull/20662) (445, Apr 2024); it reads them on predicate pushdown when present. But no OSS tool today recommends *which columns to enable bloom on* per Iceberg table — operators guess, ship a writer-side change, and re-measure manually. The Apache Parquet bloom-filter spec ([docs](https://parquet.apache.org/docs/file-format/bloomfilter/)) and the [DuckDB Parquet bloom blog (Mar 2025)](https://duckdb.org/2025/03/07/parquet-bloom-filters-in-duckdb.html) document the cost-benefit space; nobody automates the recommendation.

## Goal

`shelf-advisor` emits a JSON list of recommended `write.parquet.bloom-filter-enabled.column.<col>=true` per (catalog, schema, table), driven by mining the SHELF-37 event-listener log for high-frequency selective-equality predicates.

## Approach

New recommender under `shelf-advisor/src/recommenders/bloom.rs`. Inputs:

1. SHELF-37 event-listener log table — pull last N days (default 30) of `WHERE col = literal` patterns from `query` + `plan` columns. Use sqlglot (Python) or a Rust SQL parser; ship a sqlglot-based pre-processor as a separate Python sidecar if Rust SQL parsing is too brittle.
2. Iceberg manifest stats (existing `shelf-advisor/src/input/iceberg_metadata.rs`) for column cardinality estimates and row counts per table.

Score per `(table, column)`:

```
score = equality_selectivity × frequency × wall_time_seconds × bytes_scanned
       / (column_cardinality_estimate * row_count_total)
```

`equality_selectivity` ≈ `1 / column_cardinality` (cheap upper bound from Iceberg `lower_bounds`/`upper_bounds`/null-fraction stats); `frequency` = number of distinct queries with `WHERE col = literal` against this table over the window; `wall_time_seconds` = sum of those queries' `wall_ms`; `bytes_scanned` = sum of `physical_input_bytes`.

Top-N per table (default N=3, cap N=10) with `score >= score_threshold` (default 100, dimensional). Recommendations are JSON, not SQL:

```json
{
  "kind": "bloom",
  "catalog": "cdp",
  "schema": "icesheet",
  "table": "silver_offline_event_data_2026",
  "columns": ["event_region", "user_id"],
  "confidence": 0.78,
  "evidence": {
    "queries_matched": 4321,
    "wall_time_seconds": 9432.1,
    "bytes_scanned": 8.2e12,
    "score": 432.1
  }
}
```

The advisor never opens an MR (per SHELF-53's design — JSON only). Operator merges into table properties via their writer-side workflow.

Module layout:
- `shelf-advisor/src/recommenders/bloom.rs` — the recommender.
- `shelf-advisor/src/input/predicate_extractor.rs` — sqlglot-backed predicate mining (or Rust SQL parser fallback).
- `shelf-advisor/src/output.rs` — JSON emitter (already exists).

Cross-references SHELF-46 (the *runtime* bloom admission); together they close the loop: SHELF-46 caches blooms when present, SHELF-52 recommends *creating* them when missing.

## Acceptance criteria

- [ ] On a seeded fixture with 3 columns of ground-truth-bloom-worthy patterns, advisor emits exactly those columns with `confidence >= 0.7` and they appear in the top-3 per table.
- [ ] On a clean fixture (no selective-equality patterns), advisor emits `[]` with no false positives.
- [ ] Score is deterministic: two runs over the same fixture produce byte-identical output.
- [ ] Top-N gate (default 3) is enforced; emitting > N requires explicit `--max-per-table` flag.
- [ ] JSON output validates against the committed `shelf-advisor/schema/bloom_recommendation.schema.json`.
- [ ] Quantitative gate: on a 30-day fixture from a representative Trino log, the advisor's top-N recommendations match a hand-curated reference set with ≥ 80 % precision and ≥ 70 % recall.
- [ ] Unit tests ≥ 12 cases (single-column, multi-column, low-cardinality dropped, OR predicate split, NOT-equality skipped, malformed SQL fallback).

## Out of scope

- Writer-side change application (operator merges via Bytebase / GitHub Action / dbt).
- Side-built blooms inside `shelfd` (BLUEPRINT §7.4.2 — separate ticket).
- Puffin-backed bloom (orthogonal upstream; [apache/iceberg #15311](https://github.com/apache/iceberg/pull/15311)).
- Z-order / sort-order recommendations (BLUEPRINT §7.4.3 — out of scope).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Cardinality estimation is wrong → recommends bloom on a high-cardinality column where bloom hurts | Cardinality floor: skip columns with `column_cardinality_estimate / row_count > 0.5`; document the heuristic. |
| sqlglot parsing fails on Trino-specific SQL | Fallback regex extractor for `<ident> = '<literal>'` patterns; tagged as `confidence: 0.5`. |
| Top-N gate causes important columns to be missed | `--max-per-table` operator override; the score column makes the long tail visible if requested. |
| False positives flood the operator | `score_threshold` default plus operator review (advisor is JSON-only, no MR open). |

## Test plan

- Unit tests: predicate extraction, score calculation, top-N gate, cardinality floor, sqlglot fallback.
- Integration tests: seeded SHELF-37 log fixture + Iceberg metadata fixture; assert byte-identical golden JSON output.
- Bench: 30-day 1 M-query fixture runs in ≤ 10 min on a 4-core dev pod.
- (If applicable) docker compose smoke: SHELF-12 + listener; run `shelf-advisor recommend bloom` and assert non-empty output for a fixture-bloom-worthy table.

## Open questions

- Should the advisor also recommend *removing* bloom from columns where it's enabled but the predicate frequency is now zero? Recommend yes, as a `bloom_remove` recommendation kind, post-v1.
- Should sqlglot run as a Python sidecar or be replaced by a Rust SQL parser (`sqlparser-rs`)? Recommend Python sidecar for v1; the SHELF-26 replay harness already runs Python.
- Confidence calibration: how do we choose 0.7 default? Recommend hand-tuned against the fixture; revisit with operator feedback.
