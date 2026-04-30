# SHELF-50 — Decoded metadata in-process cache

> **Status**: Implemented in [`shelfd/src/decoded_meta.rs`].
> Off by default (`cache.decodedMeta.enabled: false`). Downstream
> tickets SHELF-46 / SHELF-37 / SHELF-47 flip it on.

## TL;DR

- New module `shelfd/src/decoded_meta.rs` with two parallel LRUs:
  - `ManifestCache` keyed by S3 ETag → `Arc<ManifestFile>`.
  - `ParquetFooterCache` keyed by S3 ETag → `Arc<ParquetMetaData>`
    (the upstream `parquet::file::metadata::ParquetMetaData`).
- Shape: `parking_lot::Mutex<lru::LruCache<EtagKey, Arc<…>>>`,
  configurable size caps (default 10 000 entries each).
- Producer: `decoded_meta::on_metadata_admit(etag, hint, bytes)`
  is called from the `Pool::Metadata` admission seam. The hot read
  path returns the raw bytes immediately; a fire-and-forget
  `tokio::task::spawn_blocking` decode populates the LRU.
- Consumer: typed accessors `decoded_meta::get_manifest(etag)` and
  `decoded_meta::get_parquet_footer(etag)`. **No existing call site
  reads from the LRU yet** — that wiring is explicit downstream
  scope (SHELF-46 / SHELF-37 / SHELF-47).
- ETag-keyed invalidation via `decoded_meta::invalidate(etag)` —
  the byte cache's eviction path (e.g. `s3_shim` conditional-GET
  on a 200/new-ETag response) calls it to keep the decoded LRU
  consistent with ADR-0011's content-addressed invariant.
- Metrics:
  `shelf_decoded_meta_hits_total{kind}`,
  `shelf_decoded_meta_misses_total{kind}`,
  `shelf_decoded_meta_decode_seconds{kind}` histogram,
  `shelf_decoded_meta_entries{kind}` gauge,
  `shelf_decoded_meta_decode_errors_total{kind, reason}`.
- Helm: `cache.decodedMeta.{enabled, maxManifestEntries, maxFooterEntries}`
  in `charts/shelf/values.yaml`. The downstream-cluster overlay
  (under `infra/`, stripped at release time per `release.yml`)
  ships commented-out, with the explicit "flip after SHELF-46 ships"
  hint.

## Why a separate LRU vs co-locating in `Pool::Metadata`

Foyer's `Pool::Metadata` is a generic **bytes** cache. The decoded
metadata cache is a **typed** structural cache. Mixing them creates
three concrete problems:

1. **Type erasure.** Foyer's `Cache<K, V>` is generic in `V`. To
   hold both `Bytes` and `Arc<ParquetMetaData>` in the same Foyer
   handle we'd need either a tagged enum (`enum BytesOrDecoded`,
   blowing up every byte read with a discriminant check) or
   `dyn Any`. Both add hot-path overhead for the byte-cache path,
   which is the cache's primary product.
2. **Eviction policy mismatch.** The metadata pool runs Foyer SIEVE
   to keep small high-frequency entries hot. The decoded cache
   wants the simplest possible policy because hits dominate misses
   by 2–3 orders of magnitude (every read of a manifest re-uses
   the same `ManifestFile` until the ETag changes). Plain LRU is
   cheap and predictable; SIEVE's frequency tracking is overhead
   we don't need.
3. **Invalidation surface.** ADR-0011 (content-addressed keys)
   says a new ETag = a new byte-cache key, so byte-cache eviction
   is implicit. The decoded cache needs an *explicit* `invalidate`
   call because the same ETag can refer to two distinct decoded
   payloads in pathological cases (a compaction that picks the
   same MD5 hash; vanishingly rare but the API has to handle it).
   Wiring an explicit `invalidate(etag)` into a Foyer-shared cache
   would require leaking the ETag into the Foyer key, undoing the
   SHELF-04 content-addressed key invariant.

A separate `parking_lot::Mutex<LruCache<EtagKey, Arc<T>>>` keeps
the byte cache untouched and the decoded cache narrow.

## ETag-invalidation safety story

The invariant is: at any moment, every entry in the decoded cache
has an exact byte-cache counterpart under the same ETag.

Producer side: `on_metadata_admit` is called from the
`Pool::Metadata` admission path **after** the byte cache accepted
the bytes. The byte-cache key is `sha256(etag || …)`; the decoded
key is `etag`. Both insertions reference the same ETag.

Eviction side: when the conditional-GET freshness loop in
`s3_shim.rs` observes a 200 OK with a new ETag, it derives the
new byte-cache key (already in the code at `s3_shim.rs:691-694`)
*and* calls `decoded_meta::invalidate(old_etag)`. The byte cache's
own internal eviction (capacity, manual `evict`) does NOT call
invalidate because the ETag itself didn't change — the decoded
entry stays valid against the same content.

This means the decoded cache can briefly hold an entry whose byte
counterpart was capacity-evicted from `Pool::Metadata`. That's a
*good* state: it means the next consumer asking via
`get_parquet_footer(etag)` skips a Foyer probe + Avro/Thrift parse
and goes straight to the typed structure. No correctness risk
because the underlying object hasn't changed (same ETag).

## Fire-and-forget decode threading model

`on_metadata_admit` runs on the caller's thread; it does:

1. Cheap `sniff_kind` (magic-byte check on the head + tail of the
   bytes; ~ns).
2. `Arc::from(etag)` — one small allocation.
3. `tokio::task::spawn_blocking` to ship the heavy parse off the
   tokio worker pool.
4. Returns immediately.

The hot read path observes ~10 ns of overhead in the disabled case
(one atomic load on the `enabled` bool) and ~hundreds of ns in the
enabled case (the sniff + spawn). The decode itself runs on a
blocking thread, so any 1-100 ms parse never extends a Trino
read.

`spawn_blocking` is correct here because Avro container walks and
Parquet `ParquetMetaDataReader::try_parse` are CPU-bound; running
them on a `tokio` worker would steal capacity from the data
plane. The blocking-thread pool defaults to 512 threads; a single
shelfd process never decodes more than ~10 K manifests/sec at
peak, so saturation is structurally impossible.

## Memory budgeting (entry count vs byte tracking)

v1 caps memory by **entry count** only:

```
maxManifestEntries × per_entry_avg_size  +  maxFooterEntries × per_entry_avg_size
```

At the default caps (10 000 each) and typical production sizes:

- Manifests: 32–256 KiB per entry → 320 MiB – 2.5 GiB worst case.
- Parquet footers: 4–32 KiB per entry → 40 MiB – 320 MiB worst case.

Combined worst case ≈ 2.8 GiB; combined typical case ≈ 600 MiB.
This fits inside the ~3 GiB of RSS headroom the SHELF-21f sizing
rule budgets on a 27.3 GiB-allocatable `m6a.4xlarge` node (see
`shelfd/docs/runbooks/2026-04-shelf-1-oom.md`).

**Why not byte-size weighting in v1?** Because the entry-count cap
is a *bound*, not a budget — and operators can already verify
the bound matches reality by graphing `shelf_decoded_meta_entries`
× a known per-entry size. Adding a real byte weighter requires
an `lru` extension (the upstream `lru` crate is fixed-arity, not
weighted), or a swap to Foyer's weighter — both deferred to
SHELF-50b once production data shows whether the entry-count cap
is the right axis.

## Trino `MemoryFileSystemCache` interaction

Trino's Iceberg connector ships a JVM-local
`MemoryFileSystemCache` (configured by
`iceberg.metadata-cache.enabled`). When that cache is on, Trino's
own coordinator silently bypasses shelfd's `s3.endpoint` for warm
metadata reads — the bytes are served from JVM heap.

SHELF-50 lives **shelf-side**. It runs whenever shelfd's
`Pool::Metadata` admission seam fires, which depends on Trino's
*first* read of an object (cold against MemoryFileSystemCache),
not on every read. So SHELF-50 captures every distinct (ETag,
metadata-object) pair Trino touches, regardless of whether
MemoryFileSystemCache subsequently shadows it.

For the SHELF-50 hit-ratio panel to actually climb on a Trino-
backed dev catalog, set `iceberg.metadata-cache.enabled=false` on
the catalog. This is the same gotcha called out in `AGENTS.md`
and is documented on the rep-2 / rep-1 cutover runbooks.

## Interaction with B1 (NVMe compression)

B1 (Foyer 0.12.2 zstd compression on the rowgroup pool) operates
on the **byte** representation of NVMe-resident entries — it
compresses on insert, decompresses on hit. SHELF-50 holds the
**decoded** representation in DRAM and is orthogonal: a B1-
compressed byte entry that lands in `Pool::Metadata` is
decompressed on read by Foyer, fed through the SHELF-50 producer
hook, and the *decoded* `ParquetMetaData` lands in the in-process
LRU. The two caches never overlap.

If both ship and both flip on, the per-warm-read cost ladder is:

| Layer | What happens | Wall-clock |
|---|---|---|
| `decoded_meta` hit | `Arc::clone(&Arc<ParquetMetaData>)` | ~50 ns |
| `decoded_meta` miss → `Pool::Metadata` hit | Foyer `get` + Avro/Thrift parse | ~50 µs |
| Both miss | Foyer `get` + B1 zstd decode + parse | ~200 µs |
| All miss | Origin GET + B1 admit + decode + parse + LRU populate | ~10–50 ms |

## Interaction with SHELF-46 (bloom-aware footer admission)

SHELF-46 reads `column_metadata.bloom_filter_offset` from a
parsed `ParquetMetaData` to decide whether to issue follow-up
range GETs for SBBF bytes. The natural call shape is:

```rust
let md = decoded_meta::get_parquet_footer(etag)
    .or_else(|| decode_now(...))?;
for col in md.row_group(0).columns() {
    if let Some(off) = col.bloom_filter_offset() {
        prefetch(off, col.bloom_filter_length()?);
    }
}
```

i.e. SHELF-46 is the canonical first consumer. Without SHELF-50,
SHELF-46 would re-parse the footer on every admission decision;
with it, the parse runs once per ETag.

## Acceptance criteria mapping (SHELF-50)

| # | Criterion | Where |
|---|---|---|
| 1 | New module with two LRUs | `shelfd/src/decoded_meta.rs` |
| 2 | Construction integration: producer hook on `Pool::Metadata` admit | `decoded_meta::on_metadata_admit` (call sites left to downstream tickets per the ticket scope) |
| 3 | Consumption hook: `get_manifest` / `get_parquet_footer` | `decoded_meta::get_manifest`, `decoded_meta::get_parquet_footer` |
| 4 | ETag-keyed eviction | `decoded_meta::invalidate(etag)` + `etag_change_yields_two_independent_slots` test |
| 5 | Memory budgeting | entry-count cap; this section |
| 6 | Metrics + EXPOSED_SERIES + regression tests | `shelfd/src/metrics.rs` |
| 7 | Config + helm + downstream-cluster overlay | `cache.decodedMeta.*` |
| 8 | Tests (LRU, round-trip manifest, round-trip parquet, malformed, integration) | `shelfd/src/decoded_meta.rs` + `shelfd/tests/it_decoded_meta.rs` |
| 9 | Design note | this file |

## Open follow-ups

- **SHELF-50b — replace `ManifestFile` shim with `iceberg::spec::ManifestFile`.**
  Requires an ADR for the iceberg-rust crate addition (heavy
  transitive). The wrapper struct in this module exposes a `raw`
  field of `Bytes` so the swap is diff-equivalent at the call
  sites — every reader either matches against future structural
  fields or falls through to the bytes.
- **SHELF-50c — byte-size weighter.** If the `shelf_decoded_meta_entries`
  gauge plateaus at the cap and pod RSS climbs visibly, swap the
  `lru::LruCache` for a weighted variant (or migrate to Foyer's
  in-memory cache with a custom weighter). Out of scope until
  measurement justifies it.
- **SHELF-50d — admin endpoint.** `POST /admin/decoded_meta/clear`
  for ops escape hatches. Trivial extension once `mv_registry`-
  style routing lands.
