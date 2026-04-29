# SHELF-46: Bloom-aware footer admission

**Status:** Draft
**Tier:** S
**Estimated effort:** S
**Depends on:** none
**Blocks:** none

## Problem (OSS-cited)

Parquet bloom filters are **not** in the trailing-64-KiB footer; each lives at an arbitrary `bloom_filter_offset` in the file ([Apache Parquet bloom-filter spec](https://parquet.apache.org/docs/file-format/bloomfilter/), [DuckDB Parquet bloom blog Mar 2025](https://duckdb.org/2025/03/07/parquet-bloom-filters-in-duckdb.html)). A naive "cache the trailing footer" misses bloom payloads entirely. Trino landed bloom-write in PR [trinodb/trino #20662](https://github.com/trinodb/trino/pull/20662) (445, Apr 2024); it reads them on predicate pushdown. Iceberg PR [apache/iceberg #15311](https://github.com/apache/iceberg/pull/15311) explores Puffin-backed bloom, an orthogonal direction. **No OSS cache caches the bloom payload separately today.** That's a real hole in `pool.metadata` design.

## Goal

When `shelfd` admits a Parquet footer, it walks each column's `bloom_filter_offset` and admits the bloom blocks under the metadata pool with a distinct key suffix, so subsequent predicate-pushdown reads hit the cache instead of round-tripping to S3.

## Approach

Extends `shelfd/src/parquet_meta.rs`. After admitting a Parquet footer, parse `FileMetaData` (already done by SHELF-50's decoded-metadata cache, or by a fresh parse if SHELF-50 hasn't landed). For each `column_metadata` with non-null `bloom_filter_offset`:

1. Bound the bloom block size: read `bloom_filter_length` if present (Parquet spec ≥ 2.10) else default to a 256 KiB upper bound clamped to `[start_of_next_column, end_of_file)`.
2. Issue a follow-up range-GET to the origin (`shelfd/src/origin.rs::get_range`) for `(etag, offset, length)`.
3. Admit under `pool.metadata` (DRAM) with key `sha256(etag || le_u64(offset) || le_u64(length) || "bloom")` — the literal `"bloom"` suffix differentiates bloom from regular footer ranges. (This requires extending the SHELF-04 key function to optionally accept a 1-byte tag; tag=0 for "data", tag=1 for "bloom"; documented in the rustdoc + Java javadoc as a forward-compatible extension.)
4. Size-cap per column (default 1 MiB) and per-file (default 16 MiB); bloom admissions exceeding the cap are dropped + counted in `shelf_bloom_dropped_oversize_total{table}`.

Trino's existing bloom reader fetches the same byte range via the SHELF-22 S3 shim and gets a hit transparently. No plugin-side change required.

Metrics:
- `shelf_bloom_admitted_total{table}`
- `shelf_bloom_admitted_bytes{table}`
- `shelf_bloom_skipped_no_offset_total{table}` (file has no bloom)
- `shelf_bloom_dropped_oversize_total{table}`

Configuration in `shelfd/src/config.rs`:
- `bloom.admission.enabled` (default true)
- `bloom.admission.per_column_max_bytes` (default 1 MiB)
- `bloom.admission.per_file_max_bytes` (default 16 MiB)

## Acceptance criteria

- [ ] On a Parquet file with N columns where M have bloom filters, `shelfd` issues exactly M follow-up range-GETs (not more, not less) and admits M entries to `pool.metadata`.
- [ ] Subsequent reads of the bloom byte range via `/cache/metadata/...` (or via the S3 shim) hit the cache (`shelf_hits_total{pool="metadata"}` increments).
- [ ] Per-column and per-file caps fire correctly under a synthetic oversized-bloom test fixture.
- [ ] Bloom-admission disabled mode (`bloom.admission.enabled=false`) is a no-op; no follow-up GETs issued.
- [ ] Quantitative gate: across 100 bloom-tagged queries on a fixture table, the row-count returned is byte-identical to the no-cache baseline (correctness invariant; bloom is metadata, not data, so this is a sanity check that admission doesn't corrupt anything).
- [ ] Quantitative gate: warm bloom-block hit p99 ≤ 5 ms (DRAM pool).
- [ ] Unit tests ≥ 10 cases (no-offset column, single-column-with-bloom, multi-column-mixed, oversized-bloom, length-derivation when missing).

## Out of scope

- The bloom-write *advisor* extension that recommends `write.parquet.bloom-filter-enabled.column.<col>=true` — that's SHELF-52.
- Iceberg Puffin-backed bloom (orthogonal upstream effort, [apache/iceberg #15311](https://github.com/apache/iceberg/pull/15311)).
- Page-index admission — covered by Phase 2a of `agents/out/03-plan.md`.
- Side-built blooms (BLUEPRINT §7.4.2 — separate ticket).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Cardinality blow-up if every column gets bloom admission | Per-file cap (16 MiB default) + the SHELF-52 advisor's top-N recommendation gate. |
| Bloom block size drift between Parquet versions | Defensive clamp to `[offset, end_of_file)`; default 256 KiB upper bound when length is missing. |
| Key-function extension breaks SHELF-04 golden vectors | Tag byte defaults to `0`; existing 17 golden vectors regenerate with explicit tag=0 and continue to match Java + Python. |
| S3 cost increase from extra GETs | Counter exposed; SHELF-40 dollars-saved formula already nets the increase against hit savings. |

## Test plan

- Unit tests: footer parsing for bloom offsets, key-function extension parity (Rust ↔ Java ↔ Python golden vectors), per-column / per-file caps, disabled-mode no-op.
- Integration tests: `shelfd/tests/it_bloom_admission.rs` builds a fixture Parquet with bloom-enabled columns, asserts admission and warm-read paths.
- Correctness invariant: replay 100 bloom-pushdown queries against the fixture and assert byte-identical row counts vs the no-cache baseline.
- (If applicable) docker compose smoke: SHELF-12 + a bloom-enabled fixture; assert `shelf_bloom_admitted_total > 0`.

## Open questions

- Should bloom admission be triggered eagerly (on footer admit) or lazily (on first range-GET against the column)? Default eager; lazy is a follow-up if the eager fetch wastes bytes.
- Per-column cap default 1 MiB — too restrictive for high-cardinality string columns? Revisit after SHELF-52 advisor recommendations land.
- Key tag byte choice (`"bloom"`-string vs single-byte tag)? Recommend single-byte tag for fixed-width keys; spell-out string is documentation.
