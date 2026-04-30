# SHELF-08 — Prometheus metrics + OTel traces

Ticket scope:

- **Prometheus surface.** Keep the `/metrics` endpoint wired by the
  earlier pass; audit the emitted series against
  `shelfd/docs/metrics.md`, wire the `shelf_request_seconds` histogram
  into the HTTP hot path, and add a metrics-regression test that
  guards the stable names from renames.
- **OTel OTLP tracing exporter.** Optional, config-driven. When
  `observability.otlp_endpoint` (or `SHELFD_OTLP_ENDPOINT`) is set,
  `shelfd` layers a `tracing-opentelemetry` OTLP exporter onto the
  existing `tracing-subscriber` pipeline. When unset, `shelfd` runs
  exactly as before — no background exporter task, no panic if the
  collector is down.
- **Span hygiene.** Give the HTTP handlers and the S3 origin client
  named spans with low-cardinality fields so a single Tempo trace
  resolves `GET /cache/*` into `http.get_cache → s3.get_object`.
- **Starter dashboard.** Ship a three-panel Grafana dashboard JSON
  under `observability/dashboards/shelf-read-path.json` covering
  `rate(shelf_hits_total)`, `rate(shelf_misses_total)`, and the p95
  of `shelf_request_seconds`. The full layout is SHELF-27.

## Public types / touched surface

- New module `shelfd::telemetry` — one public `init(..)` that returns a
  `TelemetryGuard` (drops flush the OTLP exporter).
- New field `Config::observability: ObservabilityConfig` with
  `otlp_endpoint: Option<String>` (default `None`) and a
  `SHELFD_OTLP_ENDPOINT` env override. The block is `#[serde(default)]`
  so existing YAMLs load unchanged.
- `http.rs` — handlers gain manual `info_span!("http.get_cache", …)` /
  `http.head_cache` / `http.stats` wrappers that record `route`,
  `pool`, and `status` as fields. The HTTP handler also drives the
  `shelf_request_seconds{path,outcome}` histogram.
- `origin.rs` — `S3Origin::get_range` and `S3Origin::head` wrap their
  SDK futures in `s3.get_object` / `s3.head_object` spans. Fields:
  `bucket`, `key`, `range` (for GET), and `aws.request_id` recorded on
  completion.
- `store.rs` — `FoyerStore::get_or_fetch` emits a
  `shelfd.singleflight` event labeled `role = "leader" | "follower"`
  once per caller, so a trace shows fan-in ratio.

## Module layout

New:

- `shelfd/src/telemetry.rs`
- `shelfd/tests/it_traces.rs` (unit-style integration test — no
  external services, does not require `SHELF_INTEGRATION=1`)
- `observability/dashboards/shelf-read-path.json`

Touched:

- `shelfd/src/lib.rs` — expose `telemetry` module.
- `shelfd/src/config.rs` — `ObservabilityConfig` + env override.
- `shelfd/src/main.rs` — `telemetry::init` replaces `init_tracing`.
- `shelfd/src/http.rs` — per-handler span + request_seconds histogram.
- `shelfd/src/origin.rs` — `s3.{get,head}_object` spans.
- `shelfd/src/store.rs` — singleflight events.
- `shelfd/src/metrics.rs` — metrics regression test.
- `shelfd/docs/metrics.md` — align with emitted surface, separate
  implemented rows from planned rows.
- `Cargo.toml` (workspace) + `shelfd/Cargo.toml` — OTel deps.

## Invariants

- **Fail-open init.** If the OTLP exporter cannot be built, `shelfd`
  logs a `warn!` and continues without tracing export. Startup never
  panics on telemetry.
- **Never-panic drops.** `TelemetryGuard::drop` swallows shutdown
  errors — a failing collector must not take `shelfd` down on SIGTERM.
- **Low-cardinality labels.** Span fields use static names; dynamic
  keys (`bucket`, `key`) are bounded by the workload and not emitted
  as Prometheus labels.
- **No metric renames.** Existing series (`shelf_hits_total`,
  `shelf_misses_total`, `shelf_head_*_total`, `shelf_bytes_used`,
  `shelf_request_seconds`, `shelfd_error_total`) keep their names and
  labels. Additions are additive.
- **HTTP handler never blocks on the exporter.** The
  `tracing-opentelemetry` layer uses a batch span processor so the
  hot path only writes in-memory queues.

## New dependencies

Pinned as a compatible set (per crates.io meta as of 2026-04; the
`tracing-opentelemetry 0.28 ↔ opentelemetry 0.27` pairing is the
last widely-used stable version before the 0.29/0.30 API churn):

| Crate                    | Version | Features                    | Why                                                    |
|--------------------------|---------|-----------------------------|--------------------------------------------------------|
| `opentelemetry`          | `0.27`  | (default)                   | Core API, `Tracer`, `KeyValue`.                        |
| `opentelemetry_sdk`      | `0.27`  | `rt-tokio`                  | Batch span processor on the shared Tokio runtime.      |
| `opentelemetry-otlp`     | `0.27`  | `grpc-tonic`, `trace`       | gRPC OTLP exporter → cluster Tempo.                    |
| `tracing-opentelemetry`  | `0.28`  | (default)                   | `tracing` ↔ OpenTelemetry bridge (span fan-out).       |

Dev-only:

| Crate           | Version | Why                                                   |
|-----------------|---------|-------------------------------------------------------|
| (none)          |   —     | Span capture uses a hand-rolled `Layer` impl in-file. |

License signal: all four telemetry crates are Apache-2.0. Maintenance
signal: part of the CNCF OpenTelemetry Rust SIG cadence. Binary-size
impact on the release build is ~5 MB stripped (tonic already linked).

## Test plan

Unit (in `shelfd/src/metrics.rs::tests`):

- `registry_exposes_documented_series` — gathers from `REGISTRY` after
  `Registry::init()` and asserts every series name listed in
  `shelfd/docs/metrics.md` "Phase 0 (implemented)" is present.

Unit (in `shelfd/src/telemetry.rs::tests`):

- `init_without_otlp_is_ok` — disabled path returns a guard.
- `init_with_bad_endpoint_logs_and_succeeds` — fail-open behaviour.

Integration (new, `shelfd/tests/it_traces.rs`; NOT gated on
`SHELF_INTEGRATION`):

- `get_range_emits_s3_span_under_parent_handler_span` — installs a
  custom subscriber that captures span `(name, parent_id)`, wraps the
  failing `S3Origin::get_range` call inside an
  `info_span!("http.get_cache")`, and asserts both `http.get_cache`
  and `s3.get_object` are present with the former as parent.
- `singleflight_emits_leader_and_follower_events` — drives 2 concurrent
  `FoyerStore::get_or_fetch` calls against the same cold key and
  asserts we see one `leader` and at least one `follower` event.

The existing `it_read_path.rs` and `it_head_stats.rs` suites continue
to gate on `SHELF_INTEGRATION=1`; their behaviour must not change.

## Deferred follow-ups

- Full dashboard layout (SHELF-27).
- W3C tracecontext propagation inward from Trino's plugin — opens a
  cross-service trace once the plugin ships a `trace_id`. Requires
  plugin cooperation; tracked under SHELF-22.
- Per-pool `shelf_bytes_capacity` gauge + `shelf_origin_seconds`
  histogram. Promised in `docs/metrics.md` but owned by SHELF-05's
  origin-observability pass; left for that ticket to avoid polluting
  this one's surface.
- Metrics cardinality guardrail test (static assertion that no label
  set exceeds the `ADR-0010` budget). Lands with SHELF-26.
