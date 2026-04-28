# H5 — Materialized view telemetry

Closes the H-track loop: H1 recommends MVs, H2 opens the dbt PR,
H3 pins the resulting files, and H5 is how we *prove* the loop
actually saved Trino work. Without H5 the advisor's
recommendations are pure theory — the only way to know an MV
earns its keep is to watch the bytes it serves out of shelfd.

## What ships

| Artifact | Role |
|---|---|
| `shelf_mv_hits_total{mv_name}` | Counter bumped on every `/cache/:pool/:key` hit whose key maps to a pinned MV file |
| `shelf_mv_bytes_served_total{mv_name}` | Matching byte counter, charged with the same `slice_len` the client actually receives |
| `crate::mv_registry::MvRegistry` | O(1) key → mv_name map; written by `/admin/pin` (when `mv_name` is present) and read on every hit |
| `observability/dashboards/shelf-mv-acceleration.json` | Grafana dashboard: hits/sec, bytes/sec, MV share of shim response bytes, top-10 tables, scoreboard table |

## Why an MV registry instead of a dimension

Adding `mv_name` to `shelf_hits_total` would duplicate a series
whose cardinality is already capped at `pool × tier` — the MV
label can only make the series wider. The registry approach keeps
the shelf-wide counters lean while giving the H5 panel a
parallel, MV-only surface. The registry is the single writer of
the MV-scoped counters so the existing hit-accounting code path
stays one-liner-sized.

## Data flow

```
mv-pin-watcher (H3)
  └─▶ POST /admin/pin { key_hex, pool, mv_name: "schema.table" }
           └─▶ ServerState.mv_registry.pin(key_hex, mv_name)

GET /cache/:pool/:key/:range (hit)
  └─▶ ServerState.mv_registry.record_hit(key_hex, served_bytes)
           ├─▶ shelf_mv_hits_total{mv_name}.inc()
           └─▶ shelf_mv_bytes_served_total{mv_name}.inc_by(served_bytes)

Grafana dashboard (shelf-mv-acceleration)
  └─▶ topk / rate / share panels scraping the above
```

## Cardinality & memory

- `mv_name` is the fully-qualified table name (`schema.table`);
  production clusters run <500 MVs so 500 active series is the
  practical ceiling. Prometheus starts to complain around ~10k,
  which leaves 20× headroom.
- The registry map is `HashMap<String, String>` behind an
  `RwLock`; each entry is ~112 B. One million pinned MV files
  cost ~112 MB — comfortably under any shelfd budget, and the
  realistic upper bound is ~100k files.

## What it does not do

- **No tenant dimension yet.** `shelf_queries_served_total` (E7)
  already carries `tenant`, so a per-tenant MV view will compose
  the two queries rather than doubling the label set here.
- **No backfill of existing pinned files.** The registry is
  populated on `POST /admin/pin` with an `mv_name`; pins that
  happened before the watcher stamps the field show up in the
  general hit counters but not the MV dashboard. Restarting the
  watcher re-sends the same pin requests, which is idempotent.
- **No eviction-reason-aware decrement.** `unpin` drops the
  registration but Foyer-initiated evictions currently don't.
  This is safe (the MV counter is a monotonic counter; an
  evicted key simply stops bumping it) and avoids wiring a new
  eviction callback into `FoyerStore` before H5 needs it.

## Test plan

- Unit — `mv_registry::tests` covers pin / unpin / overwrite /
  no-op on unregistered keys, plus the record-hit counter bump.
- Unit — `metrics::tests::metrics_scrape_contains_documented_series_after_touch`
  asserts both new series appear on `/metrics` after a single
  `inc_by(0)`.
- Integration — the H3 `test_mv_pin_watcher.py` regression test
  asserts every `/admin/pin` body carries the expected `mv_name`,
  so a refactor that drops the field blows up in CI.
