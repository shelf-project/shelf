# B2 — Iceberg scan-path tuning

## Settings (landed in [`infra/trino/dev/cdp_shelf.properties`](../../infra/trino/dev/cdp_shelf.properties))

| Setting | Value | Rationale |
| --- | --- | --- |
| `iceberg.split-size` | `128MB` | Default `64MB` generates too many tiny shelf GETs at SF1000 scale; 128 MB doubles the useful h2-multiplex work per roundtrip without exceeding a single row-group in most CDP tables (typical row-group = 128 MB after Iceberg's default `write.target-file-size-bytes`). |
| `iceberg.max-initial-splits` | `200` | Coordinator builds the first 200 splits synchronously, then streams the rest. 200 × 128 MB = 25 GB saturates the network faster on large scans. Default (16) under-drives shelf's connection pool. |
| `iceberg.min-assigned-split-weight` | `0.05` | Balances split scheduling across workers; lower weight = more stealing, which helps when shelf makes some workers "cheap" (hit) and others "expensive" (miss). |
| `fs.native-s3.http2` | `true` | Trino client uses h2 multiplexing on the connection to shelfd (already h2-only per ADR-0004). Eliminates TCP handshake + slow-start on each range GET. |
| `s3.max-connections` | `512` | shelfd's HTTP/2 path can serve 200+ concurrent streams per connection. Raising the client pool prevents the connector from head-of-lining on its own side. |
| `s3.request-timeout` | `30s` | shelfd p99 on a miss-all-the-way-to-S3 is ~600 ms; 30 s is generous to absorb an S3 throttling event without tripping Trino's retry loop. |

## Measurement

Before/after comparison using the existing `/tmp/shelf_bench2.py` harness,
plus the shelf metrics:

- `rate(shelf_hits_total[5m])` / `rate(shelf_misses_total[5m])`
- `shelf_origin_request_bytes_total` (after B3 lands — Track B3)
- `trino_splits_in_flight` (Trino coord JMX)

Acceptance: same p50 or better, at equal or lower shelf miss rate. The
B2 knobs *alone* are unlikely to move p50 by > 10 %; the real win comes
from the combination with B3 coalescing + D3 page-index pre-extraction.

## What NOT to touch in B2

- `iceberg.max-splits-per-second` — leave at default. Throttling the
  coordinator slows everything down equally; doesn't improve cache
  effectiveness.
- `s3.streaming.part-size` — this is for writes, and dbt already tunes it
  per-table via `write.target-file-size-bytes`.
- `s3.sse.*` — encryption-at-rest is irrelevant to cache perf and flipping
  it would rewrite every file. Don't touch.
