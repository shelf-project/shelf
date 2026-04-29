# SHELF-37: Iceberg-sink event-listener jar

**Status:** Draft
**Tier:** S
**Estimated effort:** M
**Depends on:** none
**Blocks:** SHELF-38, SHELF-39, SHELF-40, SHELF-42, SHELF-43, SHELF-52, SHELF-53

## Problem (OSS-cited)

Trino does not ship an Iceberg-sink for `EventListener` — only `mysql`, `kafka`, and `http`-style sinks live under `io.trino.plugin.eventlistener.*`. Every shop that wants per-query I/O accounting (cache-hit bytes, scanned bytes, wall time, operator summaries) ends up writing the same jar privately. The public SPI is stable since Trino 350: `io.trino.spi.eventlistener.QueryCompletedEvent` exposes `QueryStatistics.physicalInputBytes`, `wallTime`, `cpuTime`, and `getOperatorSummaries()` (per-operator I/O including `bytesReadFromCache` / `bytesReadExternally` from [trinodb/trino #26342](https://github.com/trinodb/trino/issues/26342)). Without this jar, Shelf cannot ship `tune`, `regret`, `dollars-saved`, A/B-tag analysis, the advisor, or `explain query` — every downstream Tier-S/Tier-B feature reads from this log table.

## Goal

A drop-in `event-listener-shelf-iceberg` jar lands in Trino's `plugin/` directory and writes every `QueryCompletedEvent` to a generic Iceberg table whose schema is committed in the Shelf repo, so any Shelf CLI / advisor / metric job can run a SQL `GROUP BY` over real query history.

## Approach

New Java module under `clients/trino/event-listener-iceberg/` (Maven sub-module of the existing `clients/trino` jar pom). Implement `io.trino.spi.eventlistener.EventListener` and `EventListenerFactory` (factory name `shelf-iceberg-sink`) and register via `META-INF/services/io.trino.spi.eventlistener.EventListenerFactory`. On `queryCompleted`, project `QueryCompletedEvent` to a row matching the schema:

```
query_id STRING, create_time TIMESTAMP, end_time TIMESTAMP, user STRING,
catalog STRING, schema STRING, query STRING, query_state STRING,
wall_ms BIGINT, cpu_ms BIGINT, physical_input_bytes BIGINT,
bytes_read_from_cache BIGINT, bytes_read_externally BIGINT,
operator_summaries ARRAY<STRUCT<operator STRING, input_bytes BIGINT, ...>>,
session_properties MAP<STRING,STRING>, tables ARRAY<STRING>,
shelf_arm STRING /* set by SHELF-42 */, plan STRING
```

Writes go through the iceberg-java SDK (`org.apache.iceberg.data.GenericRecord` + `AppendFiles`) directly to S3 via the catalog's existing IRSA credentials — no JDBC, no Hive metastore round-trip. Buffer rows in-memory with a 30 s / 10 MiB / 1000-row flush trigger; on flush failure log + drop (the listener is fail-open per BLUEPRINT §9.5 — no `QueryCompletedEvent` may block coordinator threads). Configuration in `etc/event-listener.properties` reads `iceberg.catalog`, `iceberg.target-table`, `iceberg.commit.batch-size`, `iceberg.commit.timeout-ms`. Schema-evolution: jar version embeds the column set, attempts schema-evolve via `Table.updateSchema().addColumn(...)` on first commit if missing; never drops columns. Helm values get a sub-block under `clients/trino/eventListener.iceberg.*`. Schema DDL committed at `clients/trino/event-listener-iceberg/schema.sql` so downstream consumers (`shelfctl tune`, `shelf-advisor`, `dollars-saved` exporter) can `CREATE TABLE IF NOT EXISTS` deterministically.

## Acceptance criteria

- [ ] Jar loads into Trino 480 with no classpath errors; `event-listener-shelf-iceberg` registered in `EventListenerFactory` SPI.
- [ ] On a fresh catalog, listener auto-creates the target Iceberg table from the committed schema; subsequent runs append.
- [ ] Buffer flush is bounded: ≤ 30 s lag from `queryCompleted` to a committed Iceberg snapshot at p95.
- [ ] Commit failure (network blip, HMS unreachable) does not raise to Trino: `QueryCompletedEvent` returns within 5 ms p99 even when the writer is offline (drop-on-flush behaviour).
- [ ] Schema evolution: jar v2 with one extra column upgrades the table on first commit without manual intervention.
- [ ] Unit tests cover projection of `QueryCompletedEvent` → Iceberg row (≥ 12 cases including null `plan`, missing `operatorSummaries`, multi-catalog query).
- [ ] Integration test under `benchmarks/smoke/` runs Trino + MinIO + the jar + 10 canonical queries and asserts the Iceberg table has 10 rows with non-null `physical_input_bytes`.

## Out of scope

- `shelfctl tune` / `regret` / `explain` CLI surfaces (SHELF-38, SHELF-39, SHELF-43).
- `shelf_arm` population (SHELF-42 owns the deterministic-hash assignment).
- Advisor consumption (SHELF-52, SHELF-53).
- A/B routing — this jar only records, never routes.
- Non-Iceberg sink targets (Postgres, Kafka). Out of scope by design.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Coordinator thread blocks on Iceberg commit | All commits run on a bounded `ExecutorService`; `queryCompleted` only enqueues; queue full → drop with warn-log. |
| Iceberg schema drift between jar versions | Schema is committed in repo; jar version embedded in `metadata.commit-id`; evolve-add-only policy. |
| PII in `query` column | Optional `redact-sql=true` config swaps the column for a SHA-256 of the normalised SQL. |
| HMS / Glue load from frequent small commits | Commit batch ≥ 1000 rows or 10 MiB minimum; exposed as a tunable for ops. |

## Test plan

- Unit tests: row projection, schema evolution, flush triggers, drop-on-flush, redaction toggle.
- Integration tests: in `benchmarks/smoke/` add a `make smoke-event-listener` target that brings up the docker-compose stack, runs 10 queries, queries the Iceberg table, and asserts row count + non-null cache-bytes columns.
- (If applicable) docker compose smoke: extends the existing SHELF-12 harness with the listener properties wired into Trino's `etc/event-listener.properties`.

## Open questions

- Should the listener also handle `QueryCreatedEvent` (for SHELF-42 `shelf_arm` tagging) or do we add a second listener? Recommend: same listener, two callbacks, single buffer.
- Single global Iceberg table vs per-catalog tables? Default global; per-catalog as a v1.x option.
- Does `iceberg-java` 1.4.x play well with Trino 480's shaded `iceberg-core`? Validate during Phase 0 of SHELF-37.
