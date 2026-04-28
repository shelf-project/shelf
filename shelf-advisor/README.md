# `shelf-advisor`

Standalone Rust binary that mines Trino event-listener data and Iceberg
manifests, then emits JSON recommendations for table-level layout and
index changes. The advisor is **read-only** and **does not open MRs** —
downstream CI/CD (GitHub Actions, GitLab CI, Bytebase, dbt-cloud, …)
consumes the JSON document and decides whether to apply the suggested
`ALTER TABLE` / `OPTIMIZE` / MV-create statements.

This split keeps the advisor's scope narrow (no IDP integration, no
write credentials, no review workflow) and matches the Tier-S #4 design
in `docs/launch/feature-ideas-ranked.md`.

> **Phase 1 scaffold.** This crate currently ships type definitions,
> traits, and a working CLI that emits an empty JSON array (`[]`).
> The actual mining logic lands under follow-up tickets — see
> [Roadmap](#roadmap) below.

## Usage

```bash
shelf-advisor analyze --window 7d --output /tmp/recs.json
shelf-advisor --version
```

The `analyze` subcommand takes a lookback window (humantime-style:
`1d`, `7d`, `24h`) and a destination path. It writes a JSON array of
`Recommendation` documents (one per recommendation), suitable for
piping into `jq`, GitHub Actions matrix jobs, or a Bytebase issue
opener.

### Output schema

```json
{
  "recommendation_type": "bloom_filter_columns",
  "table": "demo.events",
  "confidence": 0.87,
  "rationale": {
    "equality_selectivity": 0.91,
    "frequency": 12000,
    "wall_time_p50_ms": 8400
  },
  "suggested_change": {
    "alter_table": "ALTER TABLE demo.events SET TBLPROPERTIES ('write.parquet.bloom-filter-enabled.column.user_id' = 'true')"
  }
}
```

Top-level output is a JSON array (`[]` when there is nothing to
recommend, including the Phase-1 stub case).

## Architecture

The advisor is composed of three pluggable pieces, all defined as
traits so the real Iceberg / event-listener readers can be swapped in
behind feature flags or alternative backends without changing the
recommenders.

| Trait | Module | Responsibility |
| --- | --- | --- |
| `IcebergEventLogReader` | `input::event_listener` | Reads `QueryCompletedEvent` records (written by the Shelf-maintained Iceberg-sink event-listener jar) for a given lookback window. |
| `IcebergManifestReader` | `input::manifest` | Lists `DataFile` entries for a given table by walking Iceberg manifests. |
| `Recommender` | `recommenders` | Consumes the two readers above and produces zero or more `Recommendation`s. |

Three concrete recommenders ship as stubs and will be filled in by
their respective tickets:

- `BloomFilterRecommender` — `equality_selectivity × frequency ×
  wall_time` heuristic per `(table, column)` pair. (SHELF-46)
- `OptimizeRecommender` — small-file ratio + write-amplification
  estimate per table. (SHELF-53)
- `MaterializedViewRecommender` — repeated subqueries / hot
  aggregations that beat their base-scan cost. (SHELF-47)

Each stub currently returns `Ok(vec![])`. The trait + types are
production-shaped so the integration test (`tests/it_smoke.rs`)
exercises the real CLI surface end-to-end against in-process mocks.

## Roadmap

| Ticket | Scope |
| --- | --- |
| SHELF-46 | Real `BloomFilterRecommender` impl driven by `QueryStatistics.getOperatorSummaries()` from the Iceberg-sink event-log table. |
| SHELF-47 | `MaterializedViewRecommender` impl: parse Trino MV definitions from Iceberg metadata-table properties, score against base-scan cost. |
| SHELF-53 | Full advisor pipeline: real Iceberg manifest reader (`iceberg-rust`), real event-log reader, `OptimizeRecommender`, end-to-end JSON emission. |

> These ticket IDs are forward pointers — they will be filed when the
> Phase-1 scaffold lands. Treat any reference here as
> non-authoritative until the ticket exists.

## Non-goals (v1)

- **No MR opener.** No GitHub / GitLab / Bytebase / dbt-cloud client.
  JSON output only.
- **No write credentials.** The advisor never executes `ALTER TABLE`
  itself.
- **No engine plugin.** The advisor is a batch CLI; the Trino-side
  event-listener jar lives elsewhere.

See `BLUEPRINT.md` §7.4 and `docs/launch/feature-ideas-ranked.md`
Tier S #4 for the full design context.
