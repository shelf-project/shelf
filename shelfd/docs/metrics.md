# shelfd metric dictionary

Source of truth for the `shelfd` Prometheus surface. The canonical
root-level `contracts/metrics.md` is not yet populated; until it
lands, this file is authoritative and must be updated in the same PR
as any new metric.

Low-cardinality rule: every label here has ≤ 10 values in production.
See `agents/4-shelfd-builder.md` Pass 4 ("every code path that can
fail emits a typed `shelfd_error_total` counter with a
low-cardinality label set").

## Phase-0 (implemented) — emitted by `/metrics` today

These series appear in `shelfd::metrics::EXPOSED_SERIES` and are
covered by the `registry_exposes_documented_series` regression test.

| Name                     | Kind      | Labels                 | Description                                                        | Ticket   |
|--------------------------|-----------|------------------------|--------------------------------------------------------------------|----------|
| `shelf_hits_total`       | counter   | `{pool}`               | Cache hits per Foyer pool (`metadata`, `rowgroup`).                | SHELF-06 |
| `shelf_misses_total`     | counter   | `{pool}`               | Cache misses that fell through to S3 origin.                       | SHELF-06 |
| `shelf_head_hits_total`  | counter   | `{pool}`               | `HEAD /cache/...` responses served from the HEAD-LRU.              | SHELF-07 |
| `shelf_head_misses_total`| counter   | `{pool}`               | `HEAD /cache/...` responses that required a live `HeadObject`.     | SHELF-07 |
| `shelfd_error_total`     | counter   | `{component,kind}`     | Typed errors; `component` = `error::Error::component()`.           | SHELF-08 |
| `shelf_bytes_used`       | gauge     | `{pool,tier}`          | Bytes held per `(pool, tier)` — tier ∈ `{dram, nvme}`.             | SHELF-08 |
| `shelf_request_seconds`  | histogram | `{path,outcome}`       | End-to-end HTTP request latency. `path` ∈ `/cache`, `/cache/head`, `/stats`; `outcome` ∈ `hit`, `miss`, `bad_request`, `not_found`, `error`, `ok`. | SHELF-08 |

## Planned (future tickets)

Rows listed here are promised by BLUEPRINT §8 but not yet emitted.
Adding one is a ticket-scoped change: the owning ticket must move the
row into the table above in the same PR that wires the emission.

| Name                          | Kind      | Labels                 | Description                                                    | Owning ticket |
|-------------------------------|-----------|------------------------|----------------------------------------------------------------|---------------|
| `shelf_admit_total`           | counter   | `{pool,decision}`      | Admission decisions (`admit`, `reject`).                       | SHELF-25      |
| `shelf_origin_requests_total` | counter   | `{verb,status}`        | Calls to S3 (`get`, `head` × `2xx/4xx/5xx`).                   | SHELF-05 (obs pass) |
| `shelf_origin_retries_total`  | counter   | `{verb,reason}`        | Retried origin requests (`slowdown`, `timeout`, `5xx`).        | SHELF-05 (obs pass) |
| `shelf_prefetch_enqueued_total` | counter | `{priority}`           | Prefetch gRPC requests accepted (Phase 2).                     | SHELF-2x      |
| `shelf_bytes_capacity`        | gauge     | `{pool,tier}`          | Configured capacity per `(pool, tier)`.                         | SHELF-18 / follow-up |
| `shelf_pinned_bytes`          | gauge     | –                      | Bytes pinned via the pin list.                                  | SHELF-24      |
| `shelf_ring_size`             | gauge     | –                      | Number of peer pods in the HRW view.                           | SHELF-20      |
| `shelf_ready`                 | gauge     | –                      | 1 when `/readyz` has returned 200 at least once; 0 otherwise.  | SHELF-02 (follow-up) |
| `shelf_origin_seconds`        | histogram | `{verb,outcome}`       | S3 origin request latency.                                     | SHELF-05 (obs pass) |
| `shelf_store_insert_seconds`  | histogram | `{pool}`               | Foyer insert latency (tail is the scan-eviction signal).       | SHELF-17 / SHELF-18 |

## Notes

- All `*_seconds` histograms use the exponential buckets
  `prometheus::exponential_buckets(0.0005, 2.0, 16)` — 500 µs → ~33 s.
- `pool` label values are `{metadata, rowgroup}` (ADR-0008).
- `tier` label values are `{dram, nvme}`.
- The `outcome` label on `shelf_request_seconds` follows the
  `{hit, miss, bad_request, not_found, error, ok}` set. `ok` is the
  catch-all for non-cache paths (e.g. `/stats`); cache paths must use
  `{hit, miss, error, bad_request}` to keep the Grafana panel queries
  monotonic.

## Traces (SHELF-08)

`shelfd` exports spans over OTLP/gRPC when
`observability.otlp_endpoint` (or `SHELFD_OTLP_ENDPOINT`) is set. The
exporter is fail-open: a missing/bad endpoint never crashes the
daemon.

Resource attributes: `service.name = "shelfd"`,
`service.version = <crate version>`, `pod.id = <node.id>`.

Span graph for `GET /cache/:pool/:key/:range`:

```
http.get_cache            (server)
  └── shelfd.singleflight (event; role = leader | follower)
  └── s3.get_object       (client)
        - bucket, key, range, aws.request_id
```

Span graph for `HEAD /cache/:pool/origin/:bucket/*s3_key`:

```
http.head_cache           (server)
  └── s3.head_object      (client, only on LRU miss)
        - bucket, key, aws.request_id
```

## Alerts (Grafana rules, SHELF-27)

These are the initial alerts the v0.5 gate depends on:

1. Cumulative hit rate < 60 % for 10 min → page on-call.
2. Fallback rate > 5 % of requests for 5 min → page on-call.
3. Any pod `shelf_ready == 0` for 2 min → page on-call (gated on the
   `shelf_ready` gauge landing; see "Planned" above).
4. `shelfd_error_total{component="origin"}` rate > 1/s for 5 min →
   warn (thundering-herd signal, R-03).

Dashboards:

- `observability/dashboards/shelf-read-path.json` — SHELF-08 starter:
  hits, misses, p95 latency.
- `observability/dashboards/shelf-overview.json` — broader Phase-0
  overview (pre-existing scaffold).
- Full production dashboard lands with SHELF-27.
