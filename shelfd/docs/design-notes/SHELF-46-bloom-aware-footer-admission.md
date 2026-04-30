# SHELF-46 — Bloom-aware footer admission

**Status:** implemented, default OFF (`cache.bloom.enabled=false`).
**Owner module:** [`shelfd::parquet_admit`](../../src/parquet_admit.rs).
**Wired in:** [`shelfd::s3_shim::handle_get_object`](../../src/s3_shim.rs).

## TL;DR

Today shelfd admits **the entire Parquet footer slice** every time a
reader fetches it, and lands the bytes in whichever Foyer pool the
file extension picks (`.parquet` → rowgroup pool, NVMe-backed). Within
that footer, the `bloom_filter_offset` / `bloom_filter_length`
blocks attached to columns are the only bytes Trino's predicate
pushdown actually consumes for predicate skipping; column data and
dictionary pages are read separately.

SHELF-46 splits that traffic:

- **Footer reads** (last 64 KiB-ish of a `.parquet` object) →
  `Pool::Metadata` (DRAM-only, longer residency).
- **Bloom block reads** (bytes whose `(offset, length)` matches a
  `(bloom_filter_offset, bloom_filter_length)` pair previously parsed
  from this object's footer for the current ETag) → `Pool::Metadata`.
- **All other reads** → unchanged. Size-threshold admission per
  ADR-0003 still applies.

The classifier is **fail-open** — every parse error increments
`shelf_bloom_parse_errors_total{reason}` and the read falls back to
today's behaviour. Default-off, opt-in per replica via the chart.

## Problem

Two pain points compose:

1. **Metadata pool churn.** The metadata pool is sized for hot
   metadata residency (manifest lists, manifests, page indexes). When
   `.parquet` footer bytes land in the rowgroup pool instead, the
   metadata pool ends up with a smaller-than-intended working set and
   evicts hot manifests early, losing the residency benefit.
2. **Trino predicate skipping under-skips.** Trino landed Parquet
   bloom-filter writes in [trinodb/trino#20662][trino-pr-20662]
   (merged 2024-04-16, releases ≥ 445). Predicate-pushdown can skip
   row groups by reading bloom filter blocks — but only if those
   bytes are cheap enough to read repeatedly. Today they aren't:
   each bloom block lives in the body of the file at a footer-recorded
   offset, and the rowgroup pool's S3-FIFO/LRU evicts them under scan
   pressure long before the next predicate query lands.

Promoting footers and bloom blocks into `Pool::Metadata` fixes both:
manifest residency improves and bloom blocks stay DRAM-resident
across query gaps. Both effects lower S3 GET cost on the rep-2 / rep-1
`cdp.*` workload.

## Goal

Same byte-for-byte response. Different pool routing for two
narrowly-defined classes of reads:

| Read shape | Today | After SHELF-46 |
|---|---|---|
| `bytes=-N` suffix on a `.parquet`, N ≤ 64 KiB | rowgroup pool, size-threshold may reject | metadata pool, **always admit** |
| Mid-file read whose `(offset, length)` matches a known bloom block for the ETag | rowgroup pool, size-threshold may reject | metadata pool, **always admit** |
| Anything else | rowgroup pool (or metadata for `.json`/`.avro`/`.metadata.json`) | unchanged |

## Approach

### 1. Footer detection

A read is a *footer* read if `length <= cache.bloom.minFooterBytes`
(default 64 KiB) AND `offset + length == total_size`. The Parquet 1.x
spec puts the footer length + `PAR1` magic in the last 8 bytes of the
file; production Trino + Iceberg footers we have observed range from
~2 KiB to ~48 KiB. 64 KiB is a comfortable upper bound that still
lets the parser walk the trailing block.

The full footer parser is gated behind the `parquet_meta` cargo
feature so the default `shelfd` build does not pull the `parquet`
crate's transitive tree (~4 MB compile output, ~60 s of CI time).
Without the feature, the **footer-suffix heuristic still routes
trailing reads to `Pool::Metadata`**; only the bloom-block index is
empty. Operators flip on `parquet_meta` once the canary gate passes.

```
                          0                           total_size
                          ├─────────── object ────────────┤
                          │ data │ bloom │ data │ footer │
read offset=Q length=L    │ ← we classify (Q, L) → │
                                                  ↑
                                                  Q + L == total_size
                                                  AND L ≤ minFooterBytes
                                                  ⇒ BloomKind::Footer
```

### 2. Bloom block index

After a successful Footer-classified read, shelfd attempts to parse
the trailing bytes for `(bloom_filter_offset, bloom_filter_length)`
pairs across all row groups via `parquet::file::metadata::ParquetMetaDataReader`.
The pairs are stored in an in-process LRU map:

```
HashMap<etag: Vec<u8>, BloomIndexEntry { ranges: Vec<BloomBlockRange>, gen: u64 }>
```

with a generation counter for LRU eviction (no extra crate). Default
cap is 50 000 entries (~4 MiB worst-case at ~10 ranges/file).

```
   shim GET (etag=E1, offset=0, length=16, total=256K)
                  │
                  ▼
       BloomAdmission::classify
                  │
       ┌──────────┼─────────────┐
       │          │             │
       │   length ≤ 64 KiB      │
       │   AND offset+length    │   else: lookup
       │   == total_size?       │     index[E1]
       │          │             │       │
       │      YES │             │       │ matches one
       │          ▼             │       ▼ (offset,length)?
       │   BloomKind::Footer    │   BloomKind::BloomBlock
       │                        │
       └────────────┐           │
                    ▼           ▼
                NotApplicable  Pool::Metadata + FORCE_ADMIT
```

Index entries are dropped on **etag change** so ADR-0011's content-
addressed-key invariant (different etag ⇒ different cache key) keeps
holding; a stale entry can never produce a false `BloomBlock` hit
that lands in the wrong slot — the cache key disagrees and the read
goes through fetch + insert again.

### 3. Admission decision

`BloomKind::{Footer, BloomBlock}` causes the shim to:

1. Hard-wire `Pool::Metadata` regardless of the file extension's
   `pool_for(&key)` mapping.
2. Bypass the size-threshold policy via the static
   [`FORCE_ADMIT`](../../src/parquet_admit.rs) admission policy, so
   even a footer larger than `cache.admission.sizeThresholdMiB` lands
   in the cache. This matters because `FORCE_ADMIT` is `Admit` for
   any `(pool, size, hint)` triple, but the value is still bounded
   by the Foyer pool capacity — the metadata pool's 5 GiB DRAM cap
   still evicts under pressure.

`BloomKind::NotApplicable` falls through to the existing path: the
default `pool_for(&key)` routes by extension and the existing
`SizeThresholdPolicy` decides admission.

### 4. Failure modes

| Failure | Effect | Metric / Log |
|---|---|---|
| Footer parser disagrees with the file's actual `bloom_filter_offset` | Index may carry stale ranges; later `classify` returns `BloomBlock` for a non-bloom range; the read still succeeds, just lands in the wrong pool | `shelf_bloom_parse_errors_total{reason}` incremented when parse returns `Err`; never panics |
| Footer parsing not enabled (`parquet_meta` cargo feature off, default) | `parse_footer_blooms` returns `Err(ParseError::FeatureDisabled)`; the bloom-block lookup path is a no-op; the footer-suffix heuristic still routes trailing reads to `Pool::Metadata` | `shelf_bloom_parse_errors_total{reason="feature_disabled"}` |
| LRU eviction races a bloom-block lookup | `classify` returns `NotApplicable`; the read falls back to size-threshold admission (the existing default); correctness preserved | none |
| Etag changes between `insert` and a subsequent lookup | `insert` overwrites the prior entry under the same etag key; the new ranges win; the old ranges become unreachable | none (deterministic) |
| Footer read smaller than `minFooterBytes` (e.g. `bytes=-128`) | `classify` returns `Footer` because both clauses (length ≤ minFooterBytes AND tail-aligned) hold; routes to metadata pool; correctness preserved | `shelf_bloom_admit_total{kind="footer"}` |
| Object < `minFooterBytes` total size and a full read happens | `offset == 0 && length == total_size`, classifier returns `Footer` because `offset + length == total_size`. Acceptable: small Parquet files are dominated by footer/header bytes; landing them in metadata is a net win and they are tiny anyway | `shelf_bloom_admit_total{kind="footer"}` |

## Trino caveat — `iceberg.metadata-cache.enabled`

Trino + Iceberg has a JVM-local `MemoryFileSystemCache` that caches
manifest/metadata files and silently bypasses any external cache on
warm reads. **For SHELF-46 to be visible** — i.e. for
`shelf_hits_total{pool="metadata"}` to climb cold→warm on Parquet
footer reads — the Iceberg catalog properties must include
`iceberg.metadata-cache.enabled=false`. Without this, the JVM cache
absorbs every footer hit after the first and shelfd sees a flat-line
on the metadata pool even when this policy is routing things
correctly.

This is the same gotcha called out in `AGENTS.md` for the
SHELF-23 peer-fetch metric debugging.

## Interaction with B1 (zstd) and SHELF-49 (range coalesce)

- **B1 NVMe zstd compression** lives on `Pool::RowGroup` only.
  Footers and bloom blocks land in `Pool::Metadata` (DRAM-only),
  so SHELF-46 is orthogonal to B1; the two compose without conflict.
- **SHELF-49 range coalesce** quantises adjacent GETs into one wider
  GET. A coalesced fetch may straddle the footer suffix and a
  non-bloom column body. SHELF-46 classifies on the *pre-coalesce*
  per-read offsets observed by the shim, so an 8-call coalesced
  batch still bumps `shelf_bloom_admit_total{kind="footer"}` for the
  one trailing call and `kind="not_applicable"` for the others.
  Coalesce short-reads do NOT poison the footer suffix heuristic.

## Metrics

Three series, registered in [`shelfd::metrics`](../../src/metrics.rs)
and asserted in `EXPOSED_SERIES` regression tests:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `shelf_bloom_admit_total` | counter | `kind` ∈ `footer`, `bloom_block`, `not_applicable` | Per-read classifier outcomes |
| `shelf_bloom_index_entries` | gauge | none | Entries currently held in the etag → bloom-block-list LRU |
| `shelf_bloom_parse_errors_total` | counter | `reason` (`too_short`, `bad_magic`, `bad_length`, `feature_disabled`, `decode_*`) | Footer parse failures |

A simple Grafana row of `rate(shelf_bloom_admit_total[5m])` stacked
by `kind` lets operators eyeball the cutover the moment
`cache.bloom.enabled=true` lands in a pod.

## Config

```yaml
cache:
  bloom:
    enabled: false           # master switch, default off
    maxIndexEntries: 50000   # ~4 MiB worst-case RSS
    minFooterBytes: 65536    # 64 KiB suffix heuristic
```

The shelfd YAML mirror lives at `Config.bloom_admission` (snake_case
keys) — see `shelfd/src/config.rs`.

## Rollout plan

Default off. Per-replica 24 h canary in this order, gated on
`shelf_bloom_admit_total{kind="footer"}` rising and
`shelf_bloom_parse_errors_total` staying near zero:

1. rep-2 (lowest infra failure rate post-cutover).
2. Re-baseline `your_query_log_table` per-replica
   `physical_input_read_time_millis` and `ICEBERG_BAD_DATA` /
   `ICEBERG_INVALID_METADATA` counts against the previous 7-day
   window. Acceptable: directional improvement OR no regression on
   any of the three.
3. Extend to rep-1, then rep-0, then rep-3.

## Out of scope (intentionally)

- Bloom-filter *evaluation* on the shelfd side. We never read the
  bitset; we only route the bytes. Trino does the actual probing.
- Cross-pod coordination of the bloom index. The index is per-pod
  and SHELF-04's content-addressed keys are stable across pods, so
  a peer fetch (SHELF-23) lands in the right pool on the receiving
  pod once it sees its first footer for the ETag.
- Eviction-on-write. SHELF-46 keys are content-addressed by ETag —
  a write produces a new etag → new key, the old key becomes an
  unreachable orphan that Foyer evicts on capacity. No explicit
  invalidation is required.

[trino-pr-20662]: https://github.com/trinodb/trino/pull/20662
