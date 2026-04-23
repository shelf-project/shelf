# shelfd metric dictionary

This is the **planned** metric surface for Phase 0 + Phase 1. The
authoritative source is `contracts/metrics.md` at the repo root; this
document mirrors the rows that `shelfd` owns. A metric must exist in
both files before it is emitted in production.

Low-cardinality rule: every label here has ≤ 10 values in production.
See `agents/4-shelfd-builder.md` Pass 4 ("every code path that can
fail emits a typed `shelfd_error_total` counter with a
low-cardinality label set").

## Counters

| Name                     | Labels                 | Description                                                        | Ticket   |
|--------------------------|------------------------|--------------------------------------------------------------------|----------|
| `shelf_hits_total`       | `{pool}`               | Cache hits per Foyer pool (`metadata`, `rowgroup`).                | SHELF-06 |
| `shelf_misses_total`     | `{pool}`               | Cache misses that fell through to S3 origin.                       | SHELF-06 |
| `shelf_admit_total`      | `{pool,decision}`      | Admission decisions (`admit`, `reject`).                           | SHELF-25 |
| `shelf_origin_requests_total` | `{verb,status}`   | Calls to S3 (`get`, `head` × `2xx/4xx/5xx`).                       | SHELF-05 |
| `shelf_origin_retries_total`  | `{verb,reason}`   | Retried origin requests (`slowdown`, `timeout`, `5xx`).            | SHELF-05 |
| `shelf_prefetch_enqueued_total` | `{priority}`    | Prefetch gRPC requests accepted (Phase 2).                         | SHELF-2x |
| `shelfd_error_total`     | `{component,kind}`     | Typed errors; `component` = `error::Error::component()`.           | SHELF-08 |

## Gauges

| Name                   | Labels        | Description                                                    | Ticket    |
|------------------------|---------------|----------------------------------------------------------------|-----------|
| `shelf_bytes_used`     | `{pool,tier}` | Bytes held per `(pool, tier)` — tier ∈ `{dram, nvme}`.         | SHELF-08  |
| `shelf_bytes_capacity` | `{pool,tier}` | Configured capacity per `(pool, tier)`.                         | SHELF-08  |
| `shelf_pinned_bytes`   | –             | Bytes pinned via the pin list.                                 | SHELF-24  |
| `shelf_ring_size`      | –             | Number of peer pods in the HRW view.                           | SHELF-20  |
| `shelf_ready`          | –             | 1 when `/readyz` has returned 200 at least once; 0 otherwise.  | SHELF-02  |

## Histograms

| Name                     | Labels             | Description                                                    | Ticket   |
|--------------------------|--------------------|----------------------------------------------------------------|----------|
| `shelf_request_seconds`  | `{path,outcome}`   | End-to-end HTTP request latency (server-side).                 | SHELF-08 |
| `shelf_origin_seconds`   | `{verb,outcome}`   | S3 origin request latency.                                     | SHELF-08 |
| `shelf_store_insert_seconds` | `{pool}`       | Foyer insert latency (tail is the scan-eviction signal).        | SHELF-08 |

## Notes

- All `*_seconds` histograms use the exponential buckets
  `prometheus::exponential_buckets(0.0005, 2.0, 16)` — 500 µs → ~33 s.
- `outcome` label values are `{hit, miss, fallback, error}`.
- `pool` label values are `{metadata, rowgroup}` (ADR-0008).
- `tier` label values are `{dram, nvme}`.

## Alerts (Grafana rules, SHELF-27)

These are the initial alerts the v0.5 gate depends on:

1. Cumulative hit rate < 60 % for 10 min → page on-call.
2. Fallback rate > 5 % of requests for 5 min → page on-call.
3. Any pod `shelf_ready == 0` for 2 min → page on-call.
4. `shelfd_error_total{component="origin"}` rate > 1/s for 5 min →
   warn (thundering-herd signal, R-03).

Dashboard JSON lives under `charts/shelf/grafana/shelf-overview.json`
after SHELF-27.
