# `shelf-advisor`

Standalone Rust binary that mines Trino event-listener data,
Iceberg manifests, and shelfd `/stats` and emits JSON
recommendations for layout / pinning changes operators apply
through their own CI/CD. The advisor is **read-only** — it never
opens a PR, never runs `ALTER TABLE`, never mutates cluster state.
Downstream automation (GitHub Actions, GitLab CI, Bytebase,
dbt-cloud, …) consumes the JSON and decides whether to apply.

This split keeps the advisor's scope narrow (no IDP integration,
no write credentials, no review workflow) and matches the
canonical SHELF-53 design at
[`agents/out/SHELF-53-shelf-advisor-full-impl.md`](../agents/out/SHELF-53-shelf-advisor-full-impl.md).

## Status

SHELF-53 ships the framework + two real recommenders:

| Recommender                  | Owner ticket | Status   |
| ---------------------------- | ------------ | -------- |
| `OptimizeRecommender`        | SHELF-53     | **real** |
| `PinListRecommender`         | SHELF-53     | **real** |
| `MaterializedViewRecommender` | SHELF-65    | stub     |
| `BloomFilterRecommender`     | SHELF-52     | stub     |

Sibling SHELF-65 / SHELF-52 PRs land their real implementations
against the same `Recommender` trait + `AnalysisContext` exposed
by `lib.rs`.

## Install

The binary ships from the same release pipeline as `shelfd` /
`shelfctl`. Pre-built tarballs land on the GitHub release for
`linux/amd64` (musl, static) and `darwin/arm64` (Apple Silicon
native). Linux/arm64 is **deferred** — the QEMU + arm64 matrix
exceeds the 90-min runner cap (see release.yml comment).

```bash
# Linux x86_64
curl -L -o shelf-advisor.tar.gz \
  https://github.com/shelf-project/shelf/releases/download/v1.0.0-rc.X/shelf-advisor-v1.0.0-rc.X-linux-amd64.tar.gz
tar -xzf shelf-advisor.tar.gz
sudo install shelf-advisor-*/shelf-advisor /usr/local/bin/

# macOS Apple Silicon
brew install gnu-tar # if not already
curl -L -o shelf-advisor.tar.gz \
  https://github.com/shelf-project/shelf/releases/download/v1.0.0-rc.X/shelf-advisor-v1.0.0-rc.X-darwin-arm64.tar.gz
tar -xzf shelf-advisor.tar.gz
install shelf-advisor-*/shelf-advisor /usr/local/bin/
```

Build from source against the workspace:

```bash
cargo build --release -p shelf-advisor
ls target/release/shelf-advisor
```

## Configure

Drop a YAML config at `~/.shelf-advisor/config.yaml` (or pass
`--config <path>`). The OSS-clean default lives at
[`config.example.yaml`](./config.example.yaml). Every field has a
compiled default; the file is optional.

Site-specific overlays live in the operator's own
`infra/<cluster>/` directory (the release pipeline strips that
tree from the OSS publish surface, so deployment-specific
identifiers never leak). Compose the overlay with `--config`:

```bash
shelf-advisor --config /etc/shelf-advisor/cluster-overlay.yaml \
  recommend all --window 7d --output-dir ./reports/
```

Minimum useful config:

```yaml
event_log_table: example.events.query_log
window: 7d
shelfd_stats_urls:
  - http://shelf-0.shelf.svc.cluster.local:8080/stats
  - http://shelf-1.shelf.svc.cluster.local:8080/stats
```

## Run

The binary has four subcommands. They all run the same recommender
pipeline; only the I/O surface differs.

### `recommend [all | optimize | pin-list | bloom | mv]`

Run once and write a versioned envelope. Pass `all` to emit every
recommendation kind in one go, or one of the kind names to narrow.
Kind names are kebab-case at the CLI (clap convention); the
recommendation `recommendation_type` field in the JSON output keeps
the canonical snake_case form (`optimize_targets`,
`pin_list_candidates`, `bloom_filter_columns`, `mv_candidates`).

```bash
# single-file envelope
shelf-advisor recommend all \
  --window 7d \
  --output /tmp/recs.json

# per-kind directory layout (mirrors the SHELF-53 design note)
shelf-advisor recommend all \
  --window 7d \
  --output-dir ./recommendations/

# narrow to one kind
shelf-advisor recommend optimize \
  --window 30d \
  --output /tmp/optimize.json

# pin-list candidates only
shelf-advisor recommend pin-list \
  --window 24h \
  --output /tmp/pinlist.json

# replay a fixture (CI / local sanity check)
shelf-advisor recommend all \
  --fixture tests/fixtures/dry_run_input.json \
  --output /tmp/recs.json \
  --as-of 2026-04-30T00:00:00Z
```

`--output-dir` writes `<dir>/<YYYY-MM-DD>/<kind>.json` per the
canonical design note layout.

### `analyze` — backward-compat alias

Same pipeline as `recommend all` but writes a bare JSON array
(no envelope) to `--output`. Preserves the SHELF-34 phase-1
scaffold's CLI contract for any downstream pipeline that pinned
to that shape.

```bash
shelf-advisor analyze --window 7d --output /tmp/recs.json
```

### `watch` — periodic loop with Prometheus exposition

Re-runs the pipeline every `--interval`, writes the latest
envelope to `--output`, and exposes run / per-category counters at
`--prom-listen` (default `:9100`).

```bash
shelf-advisor watch \
  --interval 15m \
  --window 24h \
  --output /var/lib/shelf-advisor/report.json \
  --prom-listen 0.0.0.0:9100
```

The exposed metrics are:

```text
# HELP shelf_advisor_runs_total Total advisor runs (success).
shelf_advisor_runs_total <n>
# HELP shelf_advisor_runs_failed_total Advisor runs that failed mid-pipeline.
shelf_advisor_runs_failed_total <n>
# HELP shelf_advisor_recommendations_total Recommendations emitted by the most recent run.
shelf_advisor_recommendations_total{category="optimize_targets",severity="critical"} 3
shelf_advisor_recommendations_total{category="optimize_targets",severity="warn"}     5
shelf_advisor_recommendations_total{category="pin_list_candidates",severity="warn"}  2
```

Severity is derived from confidence (`>=0.8` → `critical`,
`>=0.6` → `warn`, else `info`).

### `dry-run` — replay a fixture

Used by CI tests. Replays a single JSON fixture (event log +
manifests + `/stats` rolled into one document) and writes the
resulting envelope. `--as-of` defaults to a frozen value so
snapshot tests are byte-stable.

```bash
shelf-advisor dry-run \
  --fixture tests/fixtures/dry_run_input.json \
  --output /tmp/dry-run.json
```

## Output schema

The envelope shape is versioned at
[`schema/envelope.schema.json`](./schema/envelope.schema.json).
Per-recommender narrowings live alongside it
([`schema/optimize_recommendation.schema.json`](./schema/optimize_recommendation.schema.json),
[`schema/pinlist_recommendation.schema.json`](./schema/pinlist_recommendation.schema.json)).

```json
{
  "generator": "shelf-advisor",
  "schema_version": "1.0.0",
  "as_of": "2026-04-30T03:14:15Z",
  "inputs": {
    "trino_query_count": 12453,
    "tables_scanned": 87,
    "shelfd_pods_scraped": 4,
    "window_secs": 604800,
    "event_log_table": "example.events.query_log"
  },
  "recommendations": [
    {
      "recommendation_type": "optimize_targets",
      "table": "example.events.purchases",
      "confidence": 0.83,
      "rationale": {
        "small_file_bytes_threshold": 33554432,
        "small_files": 142,
        "total_files": 167,
        "small_file_ratio": 0.85,
        "small_bytes": 873435136,
        "total_bytes": 9876543210,
        "avg_file_bytes": 59141647
      },
      "suggested_change": {
        "alter_table": "ALTER TABLE example.events.purchases EXECUTE optimize(file_size_threshold => '32MB')",
        "rewrite_data_files": true
      }
    }
  ]
}
```

The legacy `analyze` command emits a bare `[Recommendation, …]`
array to a single file (no envelope). Either shape sorts
`(recommendation_type, table, -confidence, stable-id)` for
byte-stable output across runs.

## Architecture

Three input adapters feed an `AnalysisContext` which the
`Recommender` trait consumes. The default recommender set ships
the four kinds above; `Recommender::analyze(&AnalysisContext)`
is the contract sibling tickets SHELF-65 / SHELF-52 import.

| Trait                    | Module                       | Responsibility                                      |
| ------------------------ | ---------------------------- | --------------------------------------------------- |
| `IcebergEventLogReader`  | `input::event_listener`      | `QueryRecord`s from the SHELF-60 listener log table |
| `IcebergManifestReader`  | `input::manifest`            | Current-snapshot data files for one Iceberg table   |
| `ShelfdStatsReader`      | `input::shelfd_stats`        | Per-pod `/stats` snapshots (capacity / used bytes)  |
| `Recommender`            | `recommenders::*`            | Consumes the readers; emits `Recommendation`s       |

Production readers (JDBC bridge for the event-log, `iceberg-rust`
for manifests) are deferred to a follow-up ticket. Today the
binary ships with:

- a fixture-backed `IcebergEventLogReader` / `IcebergManifestReader`
  used by `dry-run`, the integration tests, and any operator who
  wants to replay a captured workload locally,
- a real `HttpShelfdStatsReader` against shelfd `/stats`,
- a `LiveEventLogReader` / `LiveManifestReader` placeholder that
  returns `Ok(vec![])` — the trait is honoured, the readers
  ship empty until the production client lands.

This means `analyze` against a live cluster currently emits `[]`.
Operators run the advisor with `--fixture` (after capturing the
event log via Trino's CLI / dbt-cloud / their own pipeline) until
the production reader ticket lands.

## Recommenders

Per-recommender thresholds, math, and tuning notes:
[`docs/recommenders.md`](./docs/recommenders.md).

## Determinism

Every output flavour is byte-identical between runs given the
same inputs:

- recommendations are sorted by `(kind, table, -confidence,
  stable-id)` before emission,
- IDs are derived from `(kind, table, sorted-rationale-key)` —
  zero wall-clock noise,
- `as_of` is the only wall-clock field; `--as-of` overrides it
  for tests and snapshot pinning.

The integration test
`tests/it_recommend.rs::dry_run_byte_identical_between_runs`
asserts this property by running the binary twice on the same
fixture and `assert_eq!`-ing the bytes.

## Tests

```bash
# unit + integration (fixture-driven)
cargo test -p shelf-advisor

# with the SHELF-53 compose-stack integration
SHELF_INTEGRATION=1 cargo test -p shelf-advisor -- --test-threads=1
```

The `SHELF_INTEGRATION` gate follows the AGENTS.md "no silent
skip" rule — when the env var is unset, the gated test prints a
banner and exits cleanly without claiming false success.

## Non-goals (v1)

- **No PR opener.** No GitHub / GitLab / Bytebase / dbt-cloud
  client. JSON output only.
- **No write credentials.** The advisor never executes `ALTER
  TABLE` itself.
- **No engine plugin.** The advisor is a batch CLI; the Trino-side
  event-listener jar lives elsewhere (SHELF-60).
- **No new heavy deps in this ticket.** The Trino-Rust client is
  intentionally deferred per the SHELF-53 user override.
