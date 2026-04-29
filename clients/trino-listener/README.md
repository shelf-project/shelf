# shelf-trino-listener

**SHELF-37** — A Trino `EventListener` SPI plugin that captures every
`QueryCompletedEvent` and writes it into a configurable Iceberg table.
Cluster-agnostic, append-only, fail-open by construction.

> This module is the **measurement substrate** for the rest of Shelf's
> Tier-2 plan: SHELF-40 (`shelf_s3_dollars_saved_total`) and SHELF-42
> (A/B query tagging) both read from the table this listener fills. It
> is the OSS-clean replacement for the in-house MySQL-backed listener
> some shops have running today.

## Why an Iceberg-sink event listener

Trino ships a `mysql`-style event-listener and a generic `http` poster.
Neither survives at scale: the MySQL writer is the source of the
"writer freezes for 30 minutes" footgun (rows in `trino_queries` lag
real-time by hours, and the MySQL connection silently drops mid-batch
under load). The Iceberg sink is append-only, partition-pruned at read
time, and runs the same iceberg-core writer surface every other
batch-write workload uses — no JDBC driver in the hot path.

The OSS deliverable here is intentionally narrow: a writer. Reading is
"`SELECT * FROM <table>` against any Trino / Spark / Athena / DuckDB
engine that speaks Iceberg".

## Quick start

```bash
# Build (Temurin 25 required; Trino 480 SPI is class-file major 69).
mvn -B -f clients/trino-listener/pom.xml package

# Drop the shaded jar into Trino's plugin dir on every coordinator.
cp clients/trino-listener/target/shelf-trino-listener-*.jar \
   $TRINO_HOME/plugin/shelf-iceberg-listener/

# Wire the listener.
cat > $TRINO_HOME/etc/event-listener.properties <<'EOF'
event-listener.name=shelf-iceberg-listener
shelf.listener.iceberg.catalog=hive
shelf.listener.iceberg.catalog-impl=org.apache.iceberg.hive.HiveCatalog
shelf.listener.iceberg.uri=thrift://hms.example.local:9083
shelf.listener.iceberg.warehouse=s3a://my-warehouse-bucket/trino-logs/
shelf.listener.iceberg.table=trino_logs.queries
shelf.listener.fail-mode=drop
EOF
```

The table is auto-created on first commit. The schema is in
[Schema](#schema); to back-port to other engines run
`SHOW CREATE TABLE trino_logs.queries` after the first event lands.

## Configuration matrix

| key                                              | required | default                                          | meaning                                                                                                                  |
| ------------------------------------------------ | -------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| `shelf.listener.iceberg.catalog`                 | **yes**  | —                                                | Catalog name handed to `CatalogUtil.loadCatalog`. The listener instantiates its own catalog handle independent of any Trino connector. |
| `shelf.listener.iceberg.table`                   | **yes**  | —                                                | Fully-qualified `<schema>.<table>`. Auto-created on first commit.                                                       |
| `shelf.listener.iceberg.catalog-impl`            | no       | `org.apache.iceberg.hadoop.HadoopCatalog`        | Catalog impl class. Override with `HiveCatalog` / `RestCatalog` / `GlueCatalog` etc.                                   |
| `shelf.listener.iceberg.warehouse`               | no       | —                                                | Warehouse path. Required by `HadoopCatalog` and `HiveCatalog`.                                                          |
| `shelf.listener.iceberg.*`                       | —        | —                                                | Any other key under this prefix is forwarded verbatim (prefix stripped) into the catalog properties — `uri`, `s3.endpoint`, `s3.region`, `io-impl`, `client.region`, etc. |
| `shelf.listener.batch.max-rows`                  | no       | `1000`                                           | Flush trigger by row count.                                                                                              |
| `shelf.listener.batch.max-interval-secs`         | no       | `30`                                             | Flush trigger by wall time. The smaller of the two wins.                                                                |
| `shelf.listener.queue.capacity`                  | no       | `8192`                                           | Hard cap on in-memory event count.                                                                                       |
| `shelf.listener.queue.block-timeout-ms`          | no       | `50`                                             | Maximum block under `fail-mode=block`.                                                                                  |
| `shelf.listener.write.enabled`                   | no       | `true`                                           | Kill switch. When `false`, events are received but no Iceberg writes occur.                                              |
| `shelf.listener.fail-mode`                       | no       | `drop`                                           | One of `drop`, `block`, `log_only`. **Only `drop` is safe by default for production** (see Failure modes).              |
| `shelf.listener.query-text-max-bytes`            | no       | `65536`                                          | Hard cap on the `query_text` column.                                                                                    |
| `shelf.listener.metrics.prometheus.enabled`      | no       | `false`                                          | Bind a `GET /metrics` HTTP server. Off by default — most operators scrape the JMX MBean instead.                       |
| `shelf.listener.metrics.prometheus.port`         | no       | `9099`                                           | TCP port for the Prom HTTP exporter.                                                                                    |
| `shelf.listener.metrics.prometheus.bind`         | no       | `0.0.0.0`                                        | Bind address.                                                                                                            |

## Schema

```sql
CREATE TABLE trino_logs.queries (
    query_id                         STRING NOT NULL,
    query_state                      STRING NOT NULL,
    error_code                       STRING,
    error_type                       STRING,
    error_message                    STRING,

    principal                        STRING,
    "user"                           STRING NOT NULL,
    source                           STRING,
    catalog                          STRING,
    "schema"                         STRING,
    resource_group_id                STRING,

    query_text                       STRING NOT NULL,    -- truncated at shelf.listener.query-text-max-bytes
    query_hash                       STRING NOT NULL,    -- sha256 of the un-truncated query

    create_time                      TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time                         TIMESTAMP WITH TIME ZONE NOT NULL,
    execute_time_millis              BIGINT NOT NULL,
    queued_time_millis               BIGINT NOT NULL,
    planning_time_millis             BIGINT NOT NULL,
    wall_time_millis                 BIGINT NOT NULL,
    cpu_time_millis                  BIGINT NOT NULL,

    physical_input_bytes             BIGINT NOT NULL,
    physical_input_read_time_millis  BIGINT NOT NULL,
    physical_input_rows              BIGINT NOT NULL,

    processed_input_bytes            BIGINT NOT NULL,
    processed_input_rows             BIGINT NOT NULL,
    output_bytes                     BIGINT NOT NULL,
    output_rows                      BIGINT NOT NULL,
    peak_user_memory_bytes           BIGINT NOT NULL,
    peak_total_memory_bytes          BIGINT NOT NULL,

    server_address                   STRING,    -- coordinator pod IP, NOT a hostname
    inputs_json                      STRING NOT NULL,    -- [{"catalog":"...", "schema":"...", "table":"...", ...}]
    outputs_json                     STRING NOT NULL,
    tags_json                        STRING NOT NULL     -- {"<suffix>":"<value>"} for shelf.tag.* session props
)
PARTITIONED BY day(create_time);
```

> **Schema note for downstream tooling.** `server_address` is the
> coordinator's *pod IP*, not a hostname. This mirrors Trino's own
> `QueryContext.serverAddress` convention; SHELF-40 / SHELF-42 must not
> silently assume a DNS name.

### `tags_json` contract (SHELF-42 hand-off)

Any session property whose key starts with `shelf.tag.` lands in
`tags_json` as the bare suffix → value. A `SET SESSION shelf.tag.experiment='B'`
ends up in the row as `tags_json = {"experiment":"B"}`. SHELF-42 owns
the population side; this listener only owns the projection.

## Failure modes

| `fail-mode`  | Behaviour when queue is full                                                                | Coordinator latency |
| ------------ | ------------------------------------------------------------------------------------------- | ------------------- |
| `drop` **(default)** | Increment `shelf_listener_dropped_total{reason="queue_full"}`, return immediately.    | Bounded by `BlockingQueue.offer()` only. |
| `block`      | Block the SPI thread for up to `queue.block-timeout-ms`, then drop with the same counter.   | Up to `block-timeout-ms`. |
| `log_only`   | Short-circuit before the queue. Never write. Throttled WARN every 1024 events.              | None. |

The listener never propagates an exception out of `queryCompleted`. The
worst case it can do is be slow.

## Metrics

Two surfaces:

1. **JMX MBean** — `io.shelf.listener:type=Listener` (preferred — every
   Trino pod already runs `jmx_prometheus_javaagent`).
2. **Prometheus HTTP exporter** — `GET /metrics` on
   `${prometheus.bind}:${prometheus.port}` when
   `shelf.listener.metrics.prometheus.enabled=true`.

Both surfaces expose the same series:

| Series                                                | Kind      | Labels                                | Notes                                                |
| ----------------------------------------------------- | --------- | ------------------------------------- | ---------------------------------------------------- |
| `shelf_listener_events_total{outcome}`                | counter   | `outcome ∈ {received, written, dropped}` | Lifecycle counters; pre-populated to 0 so dashboards never see a missing series. |
| `shelf_listener_queue_depth`                          | gauge     | —                                     | Sampled per-event.                                  |
| `shelf_listener_queue_capacity`                       | gauge     | —                                     | Configured cap.                                     |
| `shelf_listener_write_seconds_*`                      | histogram | —                                     | 17-bucket exponential, 500 µs → ~33 s.              |
| `shelf_listener_write_errors_total{reason}`           | counter   | `reason ∈ {iceberg_commit, serialization, unknown}` | All counters pre-populated to 0.            |
| `shelf_listener_dropped_total{reason}`                | counter   | `reason ∈ {queue_full, log_only, shutdown}`         | Mirrors the SPI-side decision path.                 |

## Build

```bash
mvn -B -f clients/trino-listener/pom.xml verify
```

The build runs:

- license-header check (Apache 2.0 across `src/main`, `src/test`),
- compile (`<release>25</release>`),
- surefire (unit tests),
- failsafe (integration tests gated on `SHELF_INTEGRATION=1`),
- shade (relocates `com.fasterxml.jackson.*`, `com.google.common.*`,
  `org.apache.commons.*` so the listener jar never collides with the
  Trino server's classpath).

Slow integration tests:

```bash
SHELF_INTEGRATION=1 mvn -B -f clients/trino-listener/pom.xml verify
```

The integration suite boots a `HadoopCatalog` against a `@TempDir`
warehouse — no Hive metastore, no S3, no Docker required. ~5 s on a
warm laptop.

## Known deviations

- **Java 25, not Java 21.** Trino 480's `trino-spi` is class-file major
  69 (JDK 25); JDK 21 cannot load it even at compile time. The sibling
  `clients/trino` plugin made the same call. CI's `verify.yml`
  java-verify lane installs Temurin 25 explicitly. If your runner is
  pinned at JDK 17 / 21, you will need to add a Temurin-25 install step
  before `mvn verify`.
- **Hadoop 3.5.0, not 3.4.x.** Hadoop 3.4.x's `UserGroupInformation`
  calls `Subject.getSubject(AccessControlContext)` which JDK 23+ throws
  `UnsupportedOperationException` on; `HADOOP-19212` (released in 3.5.0)
  switches to `Subject.current()` and lets the listener boot under JDK
  25. We ship 3.5.0 in `compile` scope so the shaded jar is
  self-contained on JDK 25 hosts.
- **`maven-shade-plugin` 3.6.2.** Earlier shade versions bundle ASM 9.5
  and abort packaging with "Unsupported class file major version 69" on
  every JDK-25-compiled class. 3.6.2 ships ASM 9.8 which understands
  class-file 69; we also pin `org.ow2.asm:asm` at 9.8 in the plugin
  `<dependencies>` as a belt-and-suspenders.
- **`iceberg-data` ORC + Arrow excluded.** We only emit Parquet; the
  ORC and Arrow transitives are dead weight in the shaded jar.
- **No catalog driver bundled.** The listener calls
  `CatalogUtil.loadCatalog(catalog-impl, ...)` so users supply the
  driver jars on Trino's plugin classpath. `HadoopCatalog` ships with
  `iceberg-core` and works out of the box; HiveCatalog / RestCatalog /
  GlueCatalog need their respective extra jars dropped next to this one.
- **`hadoop-mapreduce-client-core` is `test`-scope only.** The Iceberg
  Parquet *reader* class-loads `org.apache.hadoop.mapreduce.lib.input.FileInputFormat`
  via `HadoopReadOptions`; the *writer* path the listener exercises in
  production does not. We pull mapreduce-client-core into test scope so
  the IT round-trip can read its own output back. Production deploys
  inherit it transitively from the Trino server's plugin classpath.

## Out of scope (today)

- `QueryCreatedEvent` capture — the SPI exposes it but the schema +
  contract live in SHELF-43 (which uses the same event type for
  prefetch).
- A/B routing — SHELF-42 owns the deterministic-hash assignment that
  populates `tags_json`. This jar only records.
- PII redaction beyond UTF-8-safe truncation. Operators who need to
  hash or strip the SQL text wrap a SQL-rewriter at the Trino
  resource-group / `query-rewriter` level.
- A non-Iceberg sink. By design.
