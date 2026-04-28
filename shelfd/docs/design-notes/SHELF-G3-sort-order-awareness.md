# SHELF-G3 — Iceberg sort/Z-order awareness

> Status: **scaffolded** (pure-metadata, no new indexes).
> Ships: `shelfd::table_props::TableTag` + control-plane cache.

## Why

Iceberg tables that are sorted (`default-sort-order-id`) or
Z-ordered (`write.metadata.z-order.columns`) can prune > 90 % of
files on a point/range predicate without any bloom filter. The
manifest min/max already encodes the answer; the engine just
needs to *know* the column is worth pruning on.

BLUEPRINT §7.4.3 calls for shelfd to tag tables on first touch so
the prefetch listener can short-circuit straight to "fetch the 1-2
files that bracket the predicate" instead of running G2 side
blooms over the whole table. G3 is the cheapest Track-G win by an
order of magnitude — it's pure metadata and it fires once per
table.

## Shape

`TableTag { clustered_columns, has_z_order, hash_distributed }`.

- `clustered_columns` is the union of:
  - `write.metadata.z-order.columns` (split by comma)
  - Field names in the active sort order (the one pointed to by
    `default-sort-order-id`). Fields that didn't carry an
    explicit `name` land as `#<source-id>` sentinels; the caller
    resolves the name from the schema it already has for the
    table.
- `has_z_order` and `hash_distributed` are independent booleans
  used for tie-breaking in the listener.

## Flow

```
ShelfFileSystem     shelfd
    │ writes metadata.json into Pool::Metadata (D1)
    │────────────────────────▶
    │                         │ on admission:
    │                         │   table_props::TableTag::from_metadata_json
    │                         │   → control plane cache
    │                         │
    │ prefetch listener asks  │
    │ "is (table, col) clustered?"
    │◀────────────────────────│
```

The control-plane cache is keyed by `table_uuid` so snapshot
commits (D2) transparently refresh the tag — the new metadata
JSON hashes to a new content-addressed key, `ShelfFileSystem`
re-admits it, and the tag recomputes on the next touch.

## What G3 does not do

- Does not walk manifests. The listener already has them; G3
  just tells it *which column's* min/max to look at.
- Does not schedule prefetches. The listener owns that.
- Does not read remote data. Callers pass bytes that are already
  in `Pool::Metadata`.

## Test plan

- `empty_metadata_is_unclustered` — no properties, no sort order.
- `z_order_columns_are_parsed` — the direct property path.
- `sort_order_names_come_through` — the active sort order wins
  and unnamed fields fall back to `#<source-id>`.
- `hash_distribution_sets_flag` — the tie-breaker bool works.

End-to-end validation lives in `benchmarks/tpcds/` under the G
track gate: TPC-DS queries 6, 13, 15, 28, 33, 34, 52, 59, 62, 71,
82, 95 must match Warp Speed within 10 % once G3 + G5 are wired.
