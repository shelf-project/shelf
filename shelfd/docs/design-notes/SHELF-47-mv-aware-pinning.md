# SHELF-65 — MV-aware pinning recommender (design note)

**Ticket:** SHELF-65 (was SHELF-47 before the 2026-04-29 renumber
in the cost-reduction roadmap; the legacy design-note draft at
`agents/out/SHELF-47-mv-aware-pinning.md` keeps its filename per
the plan's convention).

**Status:** landed in the `shelf-advisor` crate on the
`feat/shelf-65-mv-aware-pinning` branch (Draft PR).

**Depends on (in flight):**
- `feat/shelf-37-iceberg-event-listener` (PR #66) — SHELF-60.
  Future cutover may collapse `IcebergRefreshLogReader` into a
  thin adapter over `IcebergEventLogReader` once `QueryRecord`
  carries `user`, `query_sql`, and `inputs_json`.
- `feat/shelf-40-dollars-saved-counter` (PR #68) — SHELF-61.
  Provides the `Cents` newtype this recommender imports under the
  `shelf_dollars_saved` cargo feature.
- SHELF-53 advisor crate scaffolding — already on `main` (the
  `Recommender` trait + the existing `MaterializedViewRecommender`
  stub were SHELF-53 fixtures; this PR replaces the stub with the
  real pinning impl).

## Problem

Materialized-view refreshes in production are base-table-heavy and
bounded by S3 GET throughput on cold cache. Without pinning, base
tables churn out of cache between refreshes; the refresh's hit
ratio collapses to ~60 %. The cost-reduction plan's framing
(§5 #8): every materialized view in production refreshes hourly,
the base tables behind each MV are read every refresh, and pinning
should keep them warm so the next refresh's hit ratio goes from
~60 % to >95 %.

The advisor doesn't pin anything itself — it has no write
credentials. It emits a JSON list of `mv_pinning` recommendations
the operator merges into the pin-list ConfigMap via PR. The
apply-side pinner is SHELF-24's loader.

## MV detection algorithm (3-way OR-union)

```
classify_mv(table) :=
       NameRegex      ∧ name_regex.is_match(leaf(table))
    ∨  TrinoProperty  ∧ properties(table).trino_storage_table.is_some()
                       ∨ properties(table).trino_fresh_snapshot_id.is_some()
    ∨  IcebergProperty ∧ properties(table).is_materialized_view = Some(true)
```

Each strategy is opt-in via `mv_pinning.detect_strategies`. A
property-based strategy enabled but with no plumbed
`IcebergTablePropertiesReader` degrades to a single per-run WARN
log — never a hard error. The advisor's binary today registers the
recommender via `default_recommenders()` *without* the property
reader (the full reader lands with SHELF-53); regex-only detection
is sufficient for naming-convention-driven MV catalogs (i.e. teams
that follow the `mv_*` / `materialized_*` table-name discipline).

The Trino keys (`trino.materialized-view.storage-table`,
`trino.materialized-view.fresh-snapshot-id`) are taken from
[trinodb/trino#26149](https://github.com/trinodb/trino/pull/26149).
The plain `is_materialized_view = true` flag is rare in
Trino-only stacks but supported because the user spec calls it
out and we don't want to silently drop non-Trino MV writers.

## Refresh detection (2-way OR-union) and refresh-window grouping

A `RefreshEvent` is treated as an MV refresh iff *either*

- `query_sql` matches `refresh_sql_pattern`
  (default `(?i)^\s*REFRESH\s+MATERIALIZED\s+VIEW\s+`), *or*
- `user` matches `refresh_user_pattern`
  (default `^airflow_`)

…AND `written_table` classifies as an MV.

Refreshes are bucketed into windows of size
`max(lookback_hours, 1) × 3600 s`, keyed by
`started_at_unix_seconds / bucket_seconds`. One recommendation per
`(base_table, window_id)`. This avoids one-recommendation-per-
refresh spam when an MV refreshes every 15 minutes.

`refresh_count_in_window` drives `expected_hit_ratio_lift`:

```
expected_hit_ratio_lift = 1 - 1 / refresh_count   if refresh_count > 1
                        = 0                       otherwise
```

The interpretation: the first refresh in the window pays the
cold-miss tax regardless; the remaining `refresh_count - 1`
refreshes hit the warmed cache. This is the conservative reading
of the user spec's "cold-miss rate on these specific files" with
the SHELF-37 schema as it exists *today* (no
`bytesReadFromCache` / `bytesReadExternally` split on
`QueryRecord`). Once SHELF-37 PR #66 lands and the split is
available, this approximation tightens — see "Follow-up cutovers"
below.

## Cap-protection story

Aggregate `pin_bytes_estimate` is summed across all
recommendations in a single run. If the sum exceeds
`nvme_capacity_bytes × max_pin_bytes_pct` (default 240 GiB × 0.5),
every recommendation in the run:

1. Has its `severity` downgraded one tier
   (`critical → warn`, `warn → info`, `info → info` — sticky at
   `info`).
2. Is tagged `pin_bytes_too_large: true` in `rationale`.
3. Has `confidence` reduced from 0.85 to 0.55, dropping it into the
   "needs ops eyeballs" band (per `Recommendation::confidence`
   contract).

The operator sees the warning and knows that blindly applying
every recommendation would fill the cache. They can either widen
the cap (adjust `max_pin_bytes_pct`) or pick a subset of
recommendations to apply. Cap protection is a *visibility*
mechanism, not a hard filter — we never silently drop a
recommendation because that hides the underlying capacity
mismatch.

## Pin-key derivation (ADR-0011 + v1 path-proxy ETag)

`pin_keys` are ADR-0011 SHA-256 hex digests over
`etag || offset_le_u64 || length_le_u64 || rg_ordinal_le_u32`.

The advisor does not see live S3 ETags (it runs offline against
the event log + manifests). The v1 proxy uses
`etag := DataFile::path.as_bytes()` — a deterministic, opaque
version token that satisfies the ADR's "not required to be a
cryptographic hash" clause. The trade-off is that an in-place
overwrite of the same S3 path produces an identical pin-key, which
the cache would treat as a hit even though the underlying object
churned.

Mitigation: every recommendation tags
`rationale.pin_key_derivation: "v1_path_proxy_etag"`. The
apply-side pinner (SHELF-24's loader) MUST detect the proxy tag
and re-derive keys against the live S3 ETag at apply time before
inserting them into the pin-set. This is the canonical pattern —
the advisor emits *advisory locators*, the loader produces *cache
lookup keys*.

## Interaction with SHELF-53

The `Recommender` trait + the `BloomFilterRecommender` /
`OptimizeRecommender` / `MaterializedViewRecommender` stubs landed
with SHELF-53's Phase-1 scaffold. This PR:

- Replaces the `MaterializedViewRecommender` stub at
  `recommenders/mv.rs` with `MaterializedViewPinningRecommender`
  (rename: kind goes from `mv_candidates` to `mv_pinning`; the
  original `mv_candidates` heuristic — repeated-aggregation
  detection — was a stub anyway and is captured as a follow-up
  TODO rather than carried as a dead second recommender).
- Adds two optional input traits at
  `input/mv_pinning.rs`:
  `IcebergTablePropertiesReader`, `IcebergRefreshLogReader`.
  Both are plumbed into the recommender via builder methods, kept
  off the `Recommender::analyze` signature so the SHELF-53 trait
  surface is unchanged.
- Extends `AdvisorConfig` with `mv_pinning: MvPinningConfig`. This
  is a new field on `AdvisorConfig`; the only existing caller
  (`main.rs`) is updated to populate it with `Default::default()`.
- Adds two cargo features to `shelf-advisor`:
  `shelf_dollars_saved` (off by default; cost-model dependency
  cutover for SHELF-61 / PR #68) and `integration` (off by
  default; gates the live-stack integration smoke).

No edits to `shelfd`'s hot path. No edits to `s3_shim.rs`,
`store.rs`, `freshness.rs`. No edits to `clients/trino`.

## Tests

| Suite | Gate | Scope |
| --- | --- | --- |
| `recommenders/mv.rs` `#[cfg(test)] mod tests` | none | Unit tests for detection (regex/property/both/neither), refresh-detection-by-user-pattern, cost calc against known savings, severity boundaries, cap-protection downgrade, pin-key determinism + sort + dedup, regex-error surfacing. |
| `tests/it_mv_pinning.rs` | none | Fixture-driven snapshot test (asserts every field except `pin_keys` against `expected_recommendations.json`), pin-key format test (64-char lowercase hex per ADR-0011), determinism test (two consecutive runs are byte-identical). |
| `tests/it_mv_pinning_live.rs` | `--features integration` AND `SHELF_INTEGRATION=1` | Strict-gated live smoke. Compiles only under the cargo feature; panics at runtime if `SHELF_INTEGRATION=1` is missing — never silent-passes. |

The fixture under `shelf-advisor/tests/fixtures/mv_pinning/`
exercises:
- Three MV detection paths (`mv_dau` via name regex + Trino
  property; `mv_orders` via name regex + Trino property;
  `materialized_revenue` via name regex only).
- Refresh detection by SQL pattern (events 1–3) and by
  user-pattern fallback (event 4: `INSERT INTO …` SQL that misses
  the SQL regex but `airflow_etl_revenue` user matches the user
  regex).
- Two distinct base tables (`cdp.bronze.events`, `cdp.bronze.users`)
  with different refresh counts (4 vs 2) to exercise the
  refresh-window grouping logic.
- Cap protection NOT triggered (the fixture's aggregate stays
  ≪ 0.5 × 240 GiB); the cap-trigger path is covered by the unit
  test `cap_protection_downgrades_and_flags`.

## Follow-up cutovers (one-line PRs after dependencies merge)

1. **SHELF-61 / PR #68 merges** → flip the `shelf_dollars_saved`
   feature on by default; import `Cents` from the new crate;
   delete `s3_get_cost_picodollars_per_byte` from
   `MvPinningConfig` (it becomes documentation-only).
2. **SHELF-37 / PR #66 merges** → if `QueryRecord` grows `user`,
   `query_sql`, `inputs_json` fields, replace the
   `IcebergRefreshLogReader` trait with a thin adapter over
   `IcebergEventLogReader` and remove the trait. The recommender's
   public API stays the same; only the wiring changes.
3. **Live stack** → `shelf-advisor/tests/docker-compose.yml`
   spinning up MinIO + a synthetic shelfd + a synthetic Trino,
   driven from `it_mv_pinning_live.rs`. One PR per stack
   component.

## Out of scope (carried over from the legacy design note)

- Auto-merging the pin list (operator merges via ConfigMap PR per
  BLUEPRINT §9.5 / ADR-0001).
- MV refresh execution itself (compute service, see ADR-0007 —
  Phase 10 dropped).
- Cross-cluster MV scoping.
- Reading MV definitions from non-Iceberg catalogs.
