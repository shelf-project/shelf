# SHELF-17 — Iceberg manifest pool (`pool.metadata`)

_Status: implemented (v1)._
_Scope: `shelfd::store::FoyerStore`, `shelfd::config::PoolsConfig`,
 client-side routing in `clients/trino/.../ShelfFileSystem.java`._

## Guarantee

1. **Two physical caches, not two logical partitions.**
   [`FoyerStore::open`](../../src/store.rs) constructs two
   independent `foyer::Cache<Key, Bytes>` instances — one for
   `Pool::Metadata`, one for `Pool::RowGroup`. Foyer's eviction
   machinery is per-cache, so inserting into `rowgroup` cannot see,
   weight, or evict any key in `metadata`. Pool isolation is a
   structural property, not a policy knob.
2. **5 GiB metadata budget.** Rust-side default constant
   `DEFAULT_METADATA_DRAM_BYTES = 5 * (1 << 30)` (ADR-0008 §Decision;
   mirrored in `charts/shelf/values.yaml` as
   `cache.pools.metadata.sizeBytes: 5368709120`).
3. **Manifest-shaped entries fit comfortably.** Iceberg manifests
   average ~1 MB, so `5 GiB / 1 MB ≈ 5000` manifests resident. A
   wide table (thousands of data files across dozens of snapshots)
   fits in a single pod's metadata pool with slack left for
   `metadata.json`, manifest-lists, Parquet footers, and page
   indexes.

## Routing happens on the client

`shelfd` trusts whatever pool the URL says. The decision lives in
`ShelfFileSystem.poolFor`:

```java
static Pool poolFor(Location location)
```

The body lowercases the path and routes by suffix: `.json`, `.avro`,
and the literal tail `metadata.json` go to `Pool.METADATA`; everything
else falls through to `Pool.ROWGROUP`. Iceberg's layout is covered:
`metadata.json` matches explicitly, manifest lists and manifests are
`.avro`, Parquet data files fall through to `ROWGROUP`, and the
Parquet footer slice of a data file is re-routed to `METADATA` via
the separate `poolForFooter()` helper. We did not modify routing as
part of SHELF-17; if a concrete gap shows up (e.g. Puffin stats
files) that becomes its own ticket.

## SIEVE vs FrozenHot

ADR-0008 and the BLUEPRINT refer to the eviction policy on
`pool.metadata` as **FrozenHot** — shelf-project jargon for "keep the
hot working set pinned, evict cold fringe only". Foyer ships
**SIEVE** (via `SieveBuilder`), which gives the same qualitative
property — SIEVE is a hot-retention policy that resists scan
evictions — but it is not a byte-for-byte implementation of a
"FrozenHot" primitive. Shipping SIEVE today is deliberate: Foyer
provides it built-in; ADR-0008's acceptance criteria are pool
isolation + 5 GiB budget, both orthogonal to the in-pool eviction
order; and the correctness test for this ticket
(`pool_isolation_under_rowgroup_pressure`) is policy-agnostic — it
asserts a structural invariant about two `foyer::Cache`s, not a
replacement ordering.

If empirical evidence later shows SIEVE admits scan evictions that a
strict FrozenHot would have resisted (e.g. SHELF-26 replay shows
manifest hit-ratio collapse under a dashboard-vs-ad-hoc mix), open
**SHELF-17a FrozenHot policy** to swap the backing policy. That is
additive — no contract change, no ADR revision.

## Tests encoding the invariant

`shelfd::store::store_tests::pool_isolation_under_rowgroup_pressure`
seeds 16 × 8 KiB entries into a 128 KiB metadata pool, then blasts
the 64 KiB rowgroup pool with 256 × 8 KiB entries (> 30× capacity),
and asserts every metadata entry is still retrievable byte-identical.
A companion test,
`rowgroup_pressure_does_not_shrink_metadata_used_bytes`, asserts the
monotonic companion property: metadata `used_bytes` cannot drop under
rowgroup pressure. Both scale the SHELF-17 guarantee from "50 GB
ad-hoc scan" down to a fast unit test — the physical-isolation
property they verify is scale-invariant.
