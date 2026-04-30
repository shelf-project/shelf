# SHELF-47: MV-aware pinning advisor

**Status:** Draft
**Tier:** S
**Estimated effort:** M
**Depends on:** SHELF-53
**Blocks:** none

## Problem (OSS-cited)

Iceberg materialized-view (MV) refresh is base-table-heavy and bounded by S3 GET throughput on cold cache. Trino issue [trinodb/trino #24734](https://github.com/trinodb/trino/issues/24734) and PR [trinodb/trino #26149](https://github.com/trinodb/trino/pull/26149) (backport into 474, Jan 2025) document MV freshness retrieval was serial across N base tables. Iceberg PR [apache/iceberg #14440](https://github.com/apache/iceberg/pull/14440) lands a dual `expire-after-access`/`expire-after-write` cache policy *because* MV / streaming kept hot tables forever. There is no OSS cache today that pins the right slice of base-table data so MV refreshes finish in human time.

## Goal

`shelf-advisor` (per SHELF-53) emits pin-list entries for MV base tables — scoped to the MV's predicate and TTL'd to the next refresh + 1 h — so MV refresh hits a warm cache and the operator never has to hand-pin.

## Approach

New recommender under `shelf-advisor/src/recommenders/mv_pinning.rs` (the advisor's recommender plugin shape is established by SHELF-53). Inputs:

1. Trino MV definitions stored as Iceberg metadata-table properties — keys `trino.materialized-view.storage-table`, `trino.materialized-view.fresh-snapshot-id`. Read via the iceberg-rust crate's `Table::properties()` (iceberg-rust does not yet parse MV definitions natively, so the advisor walks `metadata.json` properties directly).
2. The SHELF-37 log table for MV refresh frequency — queries whose `query` column matches `REFRESH MATERIALIZED VIEW <mv>`.
3. The MV's freshness via `ConnectorMetadata.getMaterializedViewFreshness()` (Trino 416+ public SPI) — surfaced through the iceberg snapshot pointer.

For each MV with refresh frequency ≥ N/day (default N=1), emit a pin-list entry:

```
{ table: <base_table>, partition_filter: <MV_predicate>, ttl: until_next_MV_refresh + 1h, pool: rowgroup }
```

Total bytes capped at `nvme_quota * pin_fraction` (default 0.3) — measured against the live `/stats` endpoint at advisor run time. Pin-set compiled into the standard `pin_list.json` format consumed by SHELF-24's loader; the advisor never directly writes to `shelfd`. Output is JSON to stdout / a file; the operator merges it into the pin-list ConfigMap via PR.

Module layout:
- `shelf-advisor/src/recommenders/mv_pinning.rs` — the recommender.
- `shelf-advisor/src/input/iceberg_metadata.rs` — metadata.json walker for MV properties.
- `shelf-advisor/src/input/refresh_history.rs` — SQL pull from SHELF-37 log table.
- `shelfd/src/mv_registry.rs` — already exists; advisor reads from it where the registry is populated.

Cross-reference `BLUEPRINT.md §7.5` (MV-aware caching) for the design rationale; this ticket's scope is *recommendation only*, not pinning.

## Acceptance criteria

- [ ] Advisor emits a pin-list JSON entry for each MV with refresh frequency ≥ 1/day in the seeded fixture.
- [ ] Each entry's `partition_filter` matches the MV's defining predicate exactly (parsed from the storage-table SQL).
- [ ] TTL is computed correctly: `next_refresh_estimated_at + 1h`.
- [ ] Total pinned bytes never exceed `nvme_quota * pin_fraction` (default 0.3); over-cap MVs are dropped with a counter `shelf_advisor_mv_dropped_overcap_total`.
- [ ] Output JSON validates against the SHELF-24 pin-list schema (`shelfd/docs/api/pin_list.schema.json`).
- [ ] Quantitative gate: in a synthetic 10-MV fixture, ≥ 80 % of next-day refresh-time S3 GET bytes are eliminated when the operator merges and applies the recommended pin list.
- [ ] Unit tests ≥ 12 cases (single MV, multi-MV, over-cap, malformed predicate fallback, no-MV no-op).

## Out of scope

- Auto-merging the pin list (operator merges via ConfigMap PR per BLUEPRINT §9.5 / ADR-0001).
- MV refresh execution itself (compute service, see ADR-0007 — Phase 10 is dropped).
- Cross-cluster MV scoping.
- Reading MV definitions from non-Iceberg catalogs.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Multi-TB base-partition blanket-pin | Hard cap on pin-set bytes (`nvme_quota * pin_fraction`, default 0.3); per-MV byte-cap as well. |
| MV predicate parsing fails on complex SQL | Conservative fallback: emit a partition-filter-less entry if predicate is unparseable, but require operator review (`confidence: 0.4` in JSON). |
| Pin-list churn from frequent MV redefinition | Diff against previous pin-list output; only emit a recommendation when the delta exceeds a threshold (default 5 % of bytes). |

## Test plan

- Unit tests: predicate parsing, TTL computation, over-cap drop, JSON schema validation.
- Integration tests: seeded `metadata.json` + SHELF-37 log fixture; assert byte-identical golden output.
- (If applicable) docker compose smoke: SHELF-12 + an MV refresh; assert advisor recommends the right base-table partition.

## Open questions

- Should the advisor also recommend `expire-after-access` cache TTLs at the Iceberg layer, mirroring [apache/iceberg #14440](https://github.com/apache/iceberg/pull/14440)? Recommend post-v1.
- Should `pin_fraction` default be lower than 0.3 to leave room for compaction re-warm (SHELF-45)? Recommend 0.25 default; SHELF-45's per-snapshot cap stays at 5 GiB.
- How should the advisor handle nested MVs (MV-on-MV)? Recommend depth-2 max in v1.
