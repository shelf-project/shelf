# ADR 0033: Iceberg v3 Puffin DV-aware admission (SHELF-46 v2)

*Status: Proposed (2026-05-01)*
*Deciders: shelf-maintainers, trino-plugin-eng-1*
*Supersedes: none*
*Superseded-by: none*
*Related: ADR-0021 (bloom-aware footer admission, SHELF-46 v1), ADR-0008 (two-pool architecture), ADR-0011 (ETag content-addressing), ADR-0014 (page-index sidecar)*

## Context

ADR-0021 wired bloom-aware footer admission for Iceberg v1/v2 tables: classify trailing reads + per-row-group bloom blocks at the admission seam, route them to the metadata pool (DRAM-only), avoid the size-threshold gate that would otherwise serve them once and evict. It works because Iceberg v1/v2 deletion files (position deletes, equality deletes) are small Parquet objects whose footer-and-bloom shape the same classifier already handles.

Iceberg v3 changed the deletion mechanism. The format spec ([Iceberg PR #11240](https://github.com/apache/iceberg/pull/11240), merged 2024-11) replaces position-delete files with **deletion vectors** stored in [Puffin](https://iceberg.apache.org/puffin-spec/) blobs — a binary container format with a different footer layout, different blob-addressing semantics, and per-blob compression. The Trino reader landed in [trinodb/trino#24882](https://github.com/trinodb/trino/pull/24882) (merged 2025-03) and is generally available in Trino 481+. Production deployments on Trino 480 do not yet exercise the Puffin path; production deployments on a future Trino 481+ rev will.

The admission classifier in `shelfd/src/parquet_admit.rs` does not recognise Puffin blobs today. Symptoms when v3 tables go live without this work:

1. **Deletion vector reads land in the rowgroup pool.** Puffin DVs look like ordinary byte ranges to a Parquet-only classifier; without Puffin awareness, every DV read goes through the size-threshold gate. DVs are small (typically a few KiB to a few MiB per file) and high-touch (read on every query that visits an updated row group); they should be in the metadata pool, not racing rowgroups for residency.
2. **Hit-ratio for v3 tables sits at the floor.** Without admission-side priority, DV residency is governed by S3-FIFO eviction churn. v2 bloom blocks dodged this with ADR-0021; v3 DVs don't.
3. **The shim path already works.** SHELF-22's S3-compat shim is signature-agnostic and serves Puffin GETs correctly today (bytes are bytes). The gap is purely in admission classification, not transport.

The existing footer-suffix heuristic in `parquet_admit` does still help: any Puffin blob's footer lands inside the trailing-bytes slot, so trailing reads of Puffin objects do reach the metadata pool already. What's missing is the *body* of the Puffin file — the DV blob itself, addressed by `(blob_offset, blob_length)` from the Puffin footer — landing in the metadata pool too.

## Decision

Extend the admission classifier to recognise Puffin v3 blobs and grant them the same admission-priority treatment as Parquet bloom blocks. Concretely:

1. **Add a Puffin footer parser** alongside the existing Parquet `FileMetaData` parser. Puffin's trailing magic is `PFA1` (4 bytes), and the footer carries `BlobMetadata { type, fields, snapshot-id, sequence-number, offset, length, properties }` for each blob. The parser is gated behind a new non-default `puffin_meta` cargo feature, mirroring the `parquet_meta` feature gate from ADR-0021 (keeps stock builds lean).
2. **Index DV blobs by ETag.** When the classifier admits a Puffin footer, it parses the `BlobMetadata` array and inserts every blob whose `type` is `"deletion-v1"` (the v3 DV type, per Iceberg spec) into the existing per-ETag bloom-block index. The index is reused — DV blobs and bloom blocks are both small high-touch metadata-pool candidates with the same `(offset, length)` shape, and a unified index keeps the LRU memory budget bounded.
3. **Treat DV reads as `FORCE_ADMIT` to the metadata pool.** Same admission code path as bloom blocks. No size-threshold gate.
4. **Surface counters.** Existing `shelf_bloom_admit_total{kind="bloom_block"}` gets a sibling label value `kind="dv_blob"`; `shelf_bloom_parse_errors_total{reason}` gets a `reason="puffin_decode"` value. Operators can read v3 admission rate cleanly without any new dashboard work.

The chart value lands behind a new flag, default-off in OSS:

```yaml
cache:
  bloom:
    enabled: false
    puffinDvEnabled: false   # new in rc.8+
    maxIndexEntries: 50000
    minFooterBytes: 8192
```

`puffinDvEnabled` is gated separately from `bloom.enabled` so an operator can turn on bloom-block classification without committing to the Puffin parser, and vice versa.

## Consequences

- **No correctness change.** Cache keys still derive from `sha256(etag || offset || length || rg_ordinal)` per ADR-0011. Puffin blobs use `rg_ordinal=0` (no row groups), same as Parquet footers and manifests today. A v3 DV serves byte-identical bytes to direct S3 by construction.
- **No state migration.** Existing caches that don't carry DV blobs (because the v3 reader isn't deployed yet) simply admit them normally once it is. No on-disk format change in shelfd, no Foyer wipe.
- **Test fixtures need v3 tables.** `shelfd/tests/fixtures/` currently has Parquet footer + bloom-block fixtures. rc.8+ test surface adds at least one Puffin v3 DV fixture (~8 KiB) covering a single DV blob, plus golden vectors for the parser's per-blob `(offset, length)` extraction.
- **Trino-version dependency.** This ADR is gated on Trino 481+ being in production. Deploying the Puffin classifier on a Trino 480 cluster is harmless (no v3 DVs in the wild) but useless; the soak gate is "Trino 481+ deployed on at least one canary replica AND at least one v3 table in active use".
- **Stacks with ADR-0014 (page-index sidecar).** The page-index sidecar reads Parquet column-chunk page indexes; Puffin DVs are orthogonal. No interaction risk.
- **Stacks with ADR-0021.** The bloom-block index is the same data structure. Memory budget for the LRU stays at the existing default (50 000 entries, ~4 MiB worst-case RSS); DV-heavy workloads might want `cache.bloom.maxIndexEntries=100000`, documented as an op-level knob.

## Rollback

Puffin admission is opt-in. To roll back:

- **Disable**: flip `cache.bloom.puffinDvEnabled=false`, ConfigMap reload, rolling restart of `sts/shelf`. DV blobs go back to the size-threshold gate; existing entries in the bloom index for DV blobs age out via LRU within ~1 day.
- **Hard revert**: revert the rc.8+ PR. The `puffin_meta` cargo feature is non-default; binaries built without it will simply skip DV classification.
- **State migration on disable**: none needed. Cache keys are unchanged; DV blobs cached during the enabled window remain valid and serve correctly even after disable (they just race rowgroup eviction).

## Triggers for promotion to Accepted

This ADR stays at `Proposed` until **all three** hold:

1. Trino 481+ is deployed on at least one canary replica.
2. At least one Iceberg v3 table is in active use on that replica.
3. SHELF-46 v1 (ADR-0021, bloom-aware admission) has soaked at least 24 h with `cache.bloom.enabled=true` on the canary, with hit-ratio impact attribution from the SHELF-42 A/B tag rollup.

If any of those is missing, this ADR ships as design-only; the rc.8 ticket that lights up `puffinDvEnabled` will reopen it and flip status to Accepted at that point.

## Verification (rc.8+ scope)

- Unit tests in `shelfd/src/parquet_admit.rs`:
  - `puffin_footer_magic_match` — golden Puffin footer fixture parses correctly.
  - `puffin_footer_magic_mismatch_increments_parse_errors` — corrupted fixture flips the parse-error counter.
  - `puffin_dv_blob_admitted_to_metadata_pool` — DV blob's `(offset, length)` matches what hit-path classification returns.
  - `puffin_disabled_falls_back_to_size_gate` — feature flag off → size-gate behaviour preserved.
- Integration test gated on `SHELF_INTEGRATION=1`: minio + shelfd + a v3 table fixture; assert `shelf_bloom_admit_total{kind="dv_blob"}` increments on a DV-touching query.
- Smoke against a Trino 481 dev pod: byte-identity on a 5-query v3 replay; no `ICEBERG_BAD_DATA` / `ICEBERG_INVALID_METADATA` errors after a SHELF-21-style write/read cycle through the shim.

## Threat model

- **Footer-parse DoS.** Reject any `BlobMetadata` array with > `max_blob_count = 4096` entries or any individual blob `length > 256 MiB`. A single malicious Puffin object cannot stall the classifier or blow the LRU.
- **PII leak.** DV bytes are bitmaps over row positions, not column values. They carry less PII surface than bloom blocks. The classifier still emits only `(offset, length)` byte-range tuples + blob types in metrics; never blob bodies.

## References

- [Iceberg PR #11240](https://github.com/apache/iceberg/pull/11240) — Iceberg v3 deletion vectors in Puffin format (merged 2024-11).
- [Trino PR #24882](https://github.com/trinodb/trino/pull/24882) — Trino reader for Puffin DV (merged 2025-03, available in Trino 481+).
- [Puffin spec](https://iceberg.apache.org/puffin-spec/) — Iceberg's binary container format used for DV and statistics blobs.
- ADR-0021 — bloom-aware footer admission (the v1 design this ADR extends).
- ADR-0008 — two-pool architecture (the metadata-pool routing target).
- ADR-0011 — ETag content-addressing (the cache-key substrate).
- ADR-0014 — page-index sidecar (orthogonal, no interaction).
- SHELF-46 PR (currently #50 in the repo) — the v1 implementation tracker.
