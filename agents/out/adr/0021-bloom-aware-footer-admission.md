# ADR 0021: Bloom-aware footer admission

_Status: Accepted (2026-04-30)_
_Deciders: shelfd-maintainers, trino-plugin-eng-1_
_Supersedes: none_
_Superseded-by: none_

## Context

Iceberg on S3 + Trino's predicate pushdown re-reads Parquet **footers** and
**bloom blocks** on almost every query, yet a naive size-threshold
admission gate (ADR-0003) treats those ranges like any other byte range:
if they land above `cache.admission.sizeThresholdMiB` they get served
once and evicted. Bloom blocks in particular are structurally small
(hundreds of bytes to a few MiB per row group) but are accessed across
many queries in the same workload shape, so their hit profile is much
closer to a metadata object than to a rowgroup payload. Leaving them
at the mercy of the size gate is ~40 % of the avoidable S3 GET cost we
observe on rep-2 on Iceberg-heavy workloads (Metabase, Power BI, dbt
incremental models) — which is the same neighbourhood F5's deep
research puts SHELF-46 at.

Trino's bloom-filter **writer** side landed upstream in PR #20662
(merged 2024-04-16, first available in Trino ≥ 445), so production
clusters on the 480-series stack already produce bloom blocks
consistently. The reader side in Trino/Iceberg already consults the
same footer slot structure we need to parse from the cache's side.

The signal we can reliably detect at byte level (before we hand bytes
back to Trino) is the **Parquet footer magic** at the trailing
`PAR1` marker plus the `FileMetaData` thrift envelope. Once we have
the envelope, the `column_chunks[*].bloom_filter_offset` /
`bloom_filter_length` slots give us the authoritative bloom ranges
without us having to sniff the stream ourselves.

## Decision

Gate **two** byte-range shapes through a dedicated admission policy
(`FORCE_ADMIT`), independent of the size threshold:

1. **Footer reads** (trailing `≥ min_footer_bytes`, default 8 KiB): any
   byte range whose `[end-length, end)` overlaps the final
   `min_footer_bytes` of the object.
2. **Bloom-block reads**: any byte range that exactly matches a
   `(offset, length)` entry in the per-ETag bloom-block index, built
   lazily from the footer's `FileMetaData` the first time we admit a
   footer for that `etag`.

The classifier lives in `shelfd::parquet_admit`, carries an LRU index
(default 50 000 entries, ~4 MiB worst-case RSS; configurable via
`cache.bloom.maxIndexEntries`), and runs inside the admission seam, so
no change is required in the read hot path. Classified reads are
routed to `Pool::Metadata` (DRAM-only, longer residency) regardless of
the file extension's default pool routing.

Feature defaults:

- `cache.bloom.enabled = false` on the OSS chart, **default off**.
- `cache.bloom.maxIndexEntries = 50000`.
- `cache.bloom.minFooterBytes = 8192`.

The footer parser is gated behind the non-default `parquet_meta` cargo
feature so stock builds stay lean (the `parquet` crate adds ~4 MB of
compile output and ~60 s of CI time). Without the feature the
footer-suffix heuristic still routes trailing reads to
`Pool::Metadata`; only the bloom-block index is empty.

## Consequences

- **One hot-pool boundary, two admission paths.** Size-threshold
  admission stays default for the rowgroup pool. Bloom-aware admission
  stacks on top only at the metadata-routing seam, so nothing in
  `s3_shim::handle_get_object` / `store::get` changes. Roll-back is
  `cache.bloom.enabled=false` in the overlay; no state migration, no
  cache wipe.

- **Observability is first-class.** Three new Prometheus series cover
  the three interesting counters:
  - `shelf_bloom_admit_total{kind="footer"|"bloom_block"|"not_applicable"}`
  - `shelf_bloom_index_entries` (gauge of the LRU size)
  - `shelf_bloom_parse_errors_total{reason}` (footer magic mismatch,
    short read, thrift decode error, per reason for triage)

  These let the operator tell the difference between "the gate is on
  but the classifier is never matching" (miss-category drift) vs "the
  index is full but the hit rate did not move" (Trino isn't actually
  re-reading bloom blocks on this workload).

- **Interaction with `iceberg.metadata-cache.enabled`.** Trino's
  JVM-local `MemoryFileSystemCache` caches manifest/metadata files
  and silently bypasses any external cache on warm reads. For
  bloom-admission to show up in `shelf_hits_total` counters during
  A/B, the catalog running on the shelf-fronted replica must set
  `iceberg.metadata-cache.enabled=false`. Leave it on everywhere
  else.

- **Rollout discipline.** This lands default-off. Canary order is one
  replica at a time, 24 h soak per replica, **after** SHELF-49 (range
  coalesce) and B1 (zstd metadata compression) have already soaked.
  The three levers stack multiplicatively and rolling them together
  makes attribution impossible.

## Rollback signals

Two production signals flip `cache.bloom.enabled` back to `false`. Both
are evaluated in the `shelf-overview` dashboard's SHELF-46 row:

| Trigger (PromQL) | Action |
|---|---|
| `sum(rate(shelf_bloom_admit_total{kind=~"footer\|bloom_block"}[1h])) / sum(rate(shelf_bloom_admit_total[1h])) < 0.30` sustained ≥ 12 h | Flip `cache.bloom.enabled=false`. Less than 30 % of classified reads are actually hot footer / bloom shapes; the index is pure overhead on this workload. |
| `histogram_quantile(0.99, sum by (le, verb, status) (rate(shelf_origin_request_seconds_bucket[5m])))` regresses > 50 % vs the pre-enable baseline for > 10 min | Flip `cache.bloom.enabled=false`. The admission seam is slowing origin calls faster than the DRAM hit-rate gain saves them. |

Baseline for the second trigger must be captured during the preceding
soak window (SHELF-49 + B1 green), not on a cold-cache pod, otherwise
the cold-start histogram biases the "regression" threshold.

## References

- **SHELF-46** (cost-reduction plan) — Bloom-aware footer admission,
  this ADR.
- **SHELF-46** (algorithmic roadmap) — Puffin v3 DV decomposition.
  Same ticket ID, different scope; see the note on PR #50 for the
  non-renumbering decision.
- Trino PR **#20662** — bloom-filter write path (merged 2024-04-16,
  Trino ≥ 445).
- Trino PR **#24882** — v3 DV reader (merged 2025-03, referenced by
  the algorithmic-roadmap SHELF-46).
- ADR-0003 — size-threshold admission.
- ADR-0008 — two-pool architecture.
- ADR-0011 — content-addressed keys by ETag.

## Threat model

- **Path-traversal containment.** The classifier never synthesises
  pathnames; it keys the LRU by the S3 `etag` bytes only. A
  crafted object name can't escape its S3 bucket namespace via the
  cache.
- **Footer-parse DoS.** Reject any `FileMetaData` envelope whose
  declared size is over `max_footer_bytes = 8 MiB` (8 × the 1 MiB
  soft ceiling Parquet tooling actually emits) or whose column-chunks
  declare more than `max_blob_count = 4096` columns. A single
  malicious object cannot stall the classifier or blow the LRU.
- **Negative-cache poisoning.** `shelf_bloom_parse_errors_total{reason}`
  is bumped and the ETag is negative-cached on parse failure, but the
  negative entry has a short TTL (reusing `head_lru_entries`'
  5 s `NEGATIVE_TTL_DEFAULT`) so an etag that starts returning valid
  footers after a rewrite is picked up on the next read.
- **PII leak.** Bloom-block bytes are a side-channel for column
  values when the Parquet writer uses low-cardinality dictionaries.
  The admission classifier never emits content into logs or metrics,
  only `(offset, length)` byte-range tuples + row-group ordinals. The
  same guard applies to Prometheus exemplars.
