# SHELF D3 — Pre-extract Parquet PageIndex + Bloom ranges into Pool::Metadata

Status: phase-1 API landed in `shelfd::parquet_meta`. Phase 2
(feature-gated `parquet_meta` integration with the upstream
`parquet` crate) is tracked separately; the no-op extractor ships as
the safe default for every existing build target.

## Why

Parquet 2.9+ writes `ColumnIndex` + `OffsetIndex` thrift regions
into the file. When Trino's Iceberg reader receives a predicate it
reads these (typically tens of KiB per row group × column) to decide
which pages to skip. Without them cached, every query pays a
separate origin GET per `(file, row_group, column)` just to fetch a
few hundred bytes — the round-trip cost dominates. Bloom filters
have the same profile: small, compulsory reads ahead of predicate
push-down.

Pre-extraction means: the first time we admit a footer, we also
enumerate every `(ColumnIndex | OffsetIndex | BloomFilter)` byte
range the footer points at, fetch those ranges from origin, and
admit the resulting bytes into `Pool::Metadata` under their
content-addressed keys. The next predicate-pushdown scan finds them
already in DRAM.

## Phase-1 shape (landed)

- `FooterRange` / `FooterRangeKind` / `Extracted` — the public data
  model. Callers (admission + prefetch) consume `Vec<FooterRange>`
  and do not depend on any Parquet crate directly.
- `FooterExtractor` trait — a single `extract(footer_bytes,
  object_size)` method; concrete implementations live in feature-
  gated modules.
- `NoopExtractor` — returns `Extracted::empty()`. Used in every
  build where the `parquet_meta` feature is off.
- `ExtractError` — three variants (`Malformed`, `Thrift`,
  `FeatureDisabled`) so the wiring layer can emit
  `shelf_footer_extract_failures_total{reason}` without surprises.
- 4 unit tests covering labels, sums, no-op behaviour, defaults.

## Phase-2 plan (feature `parquet_meta`, not yet wired)

Add to `shelfd/Cargo.toml`:

```toml
[features]
parquet_meta = ["dep:parquet"]

[dependencies]
parquet = { version = "54", default-features = false, features = [
  "thrift",
  "base64",
], optional = true }
```

New module `shelfd/src/parquet_meta_real.rs` (behind
`#[cfg(feature = "parquet_meta")]`):

```rust
use parquet::file::metadata::{ParquetMetaDataReader, PageIndexPolicy};

pub struct ParquetFooterExtractor;

impl FooterExtractor for ParquetFooterExtractor {
    fn extract(
        &self,
        footer_bytes: &[u8],
        object_size: u64,
    ) -> Result<Extracted, ExtractError> {
        let meta = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .parse_metadata(footer_bytes)
            .map_err(|e| ExtractError::Malformed(e.to_string()))?;

        let mut out = Extracted::empty();
        for (rg_idx, rg) in meta.row_groups().iter().enumerate() {
            for (col_idx, col) in rg.columns().iter().enumerate() {
                if let Some((off, len)) = col.column_index_offset()
                    .zip(col.column_index_length())
                {
                    out.ranges.push(FooterRange {
                        offset: off as u64,
                        length: len as u64,
                        kind: FooterRangeKind::ColumnIndex,
                        row_group: rg_idx as u32,
                        column: col_idx as u32,
                    });
                }
                if let Some((off, len)) = col.offset_index_offset()
                    .zip(col.offset_index_length())
                {
                    out.ranges.push(FooterRange {
                        offset: off as u64,
                        length: len as u64,
                        kind: FooterRangeKind::OffsetIndex,
                        row_group: rg_idx as u32,
                        column: col_idx as u32,
                    });
                }
                if let Some(bf_off) = col.bloom_filter_offset() {
                    // Bloom filter length is not always stored; if
                    // missing, budget up to 1 MiB and let the
                    // admission policy trim.
                    let bf_len = col.bloom_filter_length()
                        .unwrap_or(1 << 20) as u64;
                    out.ranges.push(FooterRange {
                        offset: bf_off as u64,
                        length: bf_len,
                        kind: FooterRangeKind::BloomFilter,
                        row_group: rg_idx as u32,
                        column: col_idx as u32,
                    });
                }
            }
        }
        out.footer = Some(FooterRange {
            offset: object_size.saturating_sub(footer_bytes.len() as u64),
            length: footer_bytes.len() as u64,
            kind: FooterRangeKind::OffsetIndex,
            row_group: 0,
            column: 0,
        });
        Ok(out)
    }
}
```

## Wiring (phase 3)

In `store::FoyerStore::admit_footer` (new helper):

1. After the footer lands in `Pool::Metadata`, call
   `FooterExtractor::extract`.
2. For every returned `FooterRange`, issue a per-range origin GET
   and admit the bytes into `Pool::Metadata` under their content-
   addressed keys.
3. Emit `shelf_footer_extract_ranges_total{kind}` for observability.

The admission policy's existing `size_threshold` still applies; the
point is not to admit every byte the file contains, just the few
tens of KiB per column that predicate push-down needs.

## Rollout gates

- Gate 1: `parquet_meta` feature on in dev build; enumerate ranges
  only, no prefetch yet. Measure
  `shelf_footer_extract_ranges_total` is non-zero and stable.
- Gate 2: Wire the prefetch. Gate on SHELF-26 replay showing
  ≥ 3× reduction in `shelf_origin_request_bytes_total{op="get_range"}`
  for Parquet metadata byte ranges (offset > `object_size - 1 MiB`).
- Gate 3: Promote feature to default-on in `v0.6`.

## Why not ship phase 2 in this session

Adding `parquet` pulls in `arrow-*` and ~60 s of CI wall-clock on
the default `shelfd` build — we already ship `shelfd` as a single
static binary, and any size regression needs its own perf note +
ADR. Phase 1 unblocks every caller without forcing that decision;
phase 2 is a tiny module sitting behind a feature flag, which can
land in the follow-up PR as a standalone review.
