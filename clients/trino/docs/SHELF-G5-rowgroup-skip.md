# SHELF-G5 — Trino split skipping via `ShelfFilterService`

> Status: **scaffolded** — client + batching logic landed under
> `io.shelf.eventlistener`. Actual split-source wrapping lands
> with SHELF-29 (upstream cache SPI).

## What ships today

- `ShelfFilterClient` — HTTP-only client for `POST /filter/probe`.
  Hand-rolled JSON to avoid a Jackson dependency on the
  coordinator hot path. Default timeout 5 ms; timeouts fail open.
- `SkippableSplitFilter` — groups splits by
  `(table, column, predicate)`, batches one probe per group,
  and drops splits whose `(file_etag, row_group_ordinal)` isn't
  in `maybe_match`. Failure modes (timeout, HTTP error, empty
  shelf signal) keep every split — the filter is strictly
  opportunistic.
- Unit tests for probe batching, fail-open preservation, the
  JSON wire shape, and the response parser.

## What's parked

- **Split-source integration.** Trino does not expose a hook
  for intercepting `IcebergSplitSource.splits()` from a plugin.
  The plan is to land this through SHELF-29 (upstream cache SPI)
  or, failing that, through a `ConnectorFactory` wrapper that
  intercepts `IcebergConnector#getSplitManager`. Both are
  deferred until the upstream PR merges or is closed.
- **Metrics.** `shelf_skipped_rowgroups_total{table, column}` is
  emitted by the Rust side today (Track B); the Java counterpart
  lands with the split-source wrapper since the drop decision is
  only meaningful once it's enforced.

## Wire shape

Request (JSON) — matches the `ProbeRequest` proto in
[`shelfd/proto/shelf_filter.proto`](../../../shelfd/proto/shelf_filter.proto):

```json
{
  "table_fqn": "iceberg.analytics.events",
  "column": "user_id",
  "predicate": {"kind": "equal", "value": [1,2,3]},
  "manifest_files": ["s3://bucket/…/m.avro"]
}
```

Response:

```json
{
  "maybe_match": [
    {"file_etag": "e1", "row_group_ordinal": 0},
    {"file_etag": "e1", "row_group_ordinal": 3}
  ],
  "fail_open": false
}
```

## Deadlines

- Per-probe 5 ms. Short enough that the coordinator thread never
  waits long; long enough to cover the p95 shelfd probe latency.
- `SkippableSplitFilter.apply` is O(splits) + O(probes);
  callers must invoke it after `IcebergSplitSource.splits()`
  already produced the list, not inline on the scheduling path.
