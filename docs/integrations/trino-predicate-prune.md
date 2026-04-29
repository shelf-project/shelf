# Trino integration: `/predicate-prune` sidecar

Status: design sketch (no code shipped from this repo).
Tracking ticket: [SHELF-34](../../agents/out/SHELF-34/).

## Why

Trino's Iceberg connector evaluates min/max page statistics by reading
the Parquet footer once per split. Where the cache fronting Trino has
already parsed the footer (shelfd does, on first GET, see
[ADR-0014](../../agents/out/adr/0014-page-index-predicate-prune-sidecar.md)),
asking the cache *which page byte ranges actually match this
predicate* lets the worker fetch the smaller subset directly and skip
the second footer read entirely.

Expected lift: 10–30% scan-byte reduction on selective queries (the
Power BI cohort dominates this band — see plan §SHELF-34).

Upstream context:

- [Iceberg PR #15211](https://github.com/apache/iceberg/pull/15211)
  adds vectorized-reader page skipping. `/predicate-prune` plays the
  same role at split-prepare time, before the reader runs.
- [Iceberg PR #10090](https://github.com/apache/iceberg/pull/10090)
  cooperates multi-predicate row-group filters; `/predicate-prune`
  consumes one column at a time today (one HTTP call per pushable
  predicate). A multi-predicate variant is a future ticket.
- [Trino #24007](https://github.com/trinodb/trino/issues/24007)
  proposed footer-reader scope reduction. **Closed, not merged** —
  SHELF-34 stands alone.

## Endpoint contract

```
GET /predicate-prune?
        path=s3a://<allowlisted-bucket>/<key>.parquet
        &col=<column_name>
        & (min=<value>&max=<value> | eq=<value>)
```

Response (200 OK, all numeric values are `u64`):

```json
{
  "path": "s3a://<bucket>/<key>.parquet",
  "column": "<col>",
  "total_pages": 123,
  "kept_pages": 17,
  "pages": [[<offset>, <length>], [<offset>, <length>], ...]
}
```

Errors: 400 on bucket-allowlist rejection, missing column, ambiguous
predicate shape, oversized footer, or any 4xx from the origin (negative
caching is not applied — see ADR-0014 §Negative-cache discipline).

## Trino-side wiring sketch

The integration belongs in `IcebergSplitSource` (or a thin wrapper
around `ParquetReaderProvider`) — the goal is to convert a domain-bound
`TupleDomain<IcebergColumnHandle>` into a list of byte ranges before
the connector hands the split to a worker.

```java
// Pseudocode — illustrative shape, not a drop-in patch.
final class ShelfPredicatePruneClient {
    private final HttpClient http;
    private final URI base;          // http://<shelfd-svc>:9090

    List<Range> pruneOrEmpty(String s3aPath,
                             String column,
                             Optional<Object> min,
                             Optional<Object> max,
                             Optional<Object> eq) {
        // io.trino.spi.connector.SourcePage docs: byte ranges returned
        // here become the candidate set for IcebergPageSource;
        // an empty list means "no rows match" and the split can be
        // skipped entirely.
        URI url = build(base, s3aPath, column, min, max, eq);
        try (var resp = http.execute(GET(url), JSON)) {
            if (resp.status() != 200) {
                return List.of();      // fail-open: fall back to full footer read
            }
            return resp.body().getArray("pages")
                .stream()
                .map(arr -> new Range(arr.getLong(0), arr.getLong(1)))
                .toList();
        }
    }
}
```

The split scheduler then either passes the kept-pages list down to the
worker (so the Parquet reader can `setSelectedPages(...)`) or, when the
list is empty, drops the split.

## Failure modes

- **shelfd 5xx or timeout**: fail open. The worker still reads the
  footer locally; this is just a missed optimization, never a
  correctness issue.
- **Bucket not on the allowlist**: shelfd returns 400. The client
  treats this as fail-open. Operators populate the allowlist via an
  out-of-tree overlay file; the OSS default is empty.
- **Page-index absent (e.g. very old Parquet writer)**: shelfd returns
  `kept_pages == total_pages` (every page kept). The worker reads the
  full set of row groups as it would today.

## Why not gRPC

The endpoint is consumed once per split and returns a small JSON
payload (≤ a few hundred byte-range tuples). HTTP keeps the contract
compatible with the existing shelfd data plane on port 9090 and matches
what the [Trino HTTP-event-listener][trino-spi] already does for
out-of-band coordinator integrations.

[trino-spi]: https://trino.io/docs/current/develop/event-listener.html

## See also

- [ADR-0014](../../agents/out/adr/0014-page-index-predicate-prune-sidecar.md) — design and security rationale.
- [THREAT_MODEL.md](../../agents/out/SHELF-34/THREAT_MODEL.md) — concrete code-line references for the five sidecar review items.
- [Apache Parquet page index spec](https://github.com/apache/parquet-format/blob/master/PageIndex.md).
