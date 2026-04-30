# SHELF-22 — S3-compatibility read shim

Status: closed (Phase-0). Owner: rust-engineer-2. Depends on SHELF-06
(read path), SHELF-07 (HEAD + HEAD-LRU). Related ADRs: ADR-0003
(size-threshold admission), ADR-0008 (two-pool layout).

## Motivation

Trino is the first-class client for Shelf, but a long tail of tools
(boto3, DuckDB, Polars, `aws s3 cp`, pandas, Ray) cannot load the
Java plugin. Asking every consumer to rebuild against our SPI is a
non-starter. The S3 REST protocol is the common denominator those
clients already speak; pointing `endpoint_url` at Shelf is a one-line
change on the client side.

The shim serves **just enough** of the protocol to unblock analytics
reads:

- `GET  /:bucket/*key` — `GetObject` with optional `Range:` header
- `HEAD /:bucket/*key` — `HeadObject`

Both verbs flow through `FoyerStore::get_or_fetch` so a shim read
warms the same pool as a native `/cache/...` read — the two surfaces
collide on the exact same content-addressed key, keyed on
`(etag, offset, length)` via `store::key_from_tuple`.

## Why a dedicated port

The shim listens on `0.0.0.0:9092` (default, overridable via
`s3_shim.bind_address`) rather than multiplexing onto the native
`:9090` data-plane port. Reasons:

1. **Namespace hygiene.** S3 clients will happily follow malformed
   bucket prefixes onto paths like `/metrics/...`. If we mounted the
   shim on the native router, a misconfigured `boto3` client would
   download Prometheus text instead of an XML `NoSuchBucket`. The
   dedicated router keeps the surfaces disjoint by construction.
2. **Independent firewalling.** Operators can expose `:9092` to tenant
   network segments while keeping `:9090` reachable only from the
   Trino cluster.
3. **Starvation isolation.** A hot boto3 loop hitting `:9092` cannot
   crowd Trino split I/O sharing the same Axum acceptor.
4. **Kill switch.** `s3_shim.enabled = false` flips the listener off
   without redeploying; the native data plane is unaffected.

## Out of scope

The shim is **read-only and unauthenticated**. Explicitly rejected:

| Surface                     | Rationale                                |
|-----------------------------|------------------------------------------|
| SigV4 authentication        | Shelf pods already trust their VPC       |
| Presigned URLs              | Signatures require auth to begin with    |
| `ListObjects(V2)`           | Pagination + cursor semantics non-trivial|
| `PutObject` / `DeleteObject`| Shelf is a cache, not an origin          |
| Multipart upload            | Writes are out of scope                  |
| Virtual-hosted-style URLs   | One URL shape is plenty for the shim     |
| S3 Select                   | Compute surface, not a cache concern     |
| Versioning                  | Cached objects are snapshot-by-ETag only |

## Error parity matrix

Clients (boto3 especially) branch on `Error.Code` in the XML
response, not on HTTP status. The shim emits the S3 envelope
(`<?xml...?><Error><Code>...</Code><Message>...</Message></Error>`)
with these codes:

| Shim situation              | HTTP | `<Code>`          | Extra headers                     |
|-----------------------------|-----:|-------------------|-----------------------------------|
| Object missing              |  404 | `NoSuchKey`       | `x-amz-request-id`                |
| Malformed / unsatisfiable   |  416 | `InvalidRange`    | `Content-Range: bytes */<size>`   |
| Full-object read too large  |  501 | `NotImplemented`  | `x-amz-request-id`                |
| Upstream SDK / socket error |  502 | `InternalError`   | `x-amz-request-id`                |

The 501 path exists because unbounded `GetObject` on, say, a 12 GiB
Parquet file would walk past the row-group pool's single-entry
ceiling (ADR-0003). Rather than thrash the cache, we return 501 and
hint the client to issue a ranged read; DuckDB and Polars already
issue range-based reads for Parquet footers and row groups, so this
failure mode is reachable only by misconfigured clients.

## Cache reuse

Pool routing mirrors the Java `ShelfFileSystem.poolFor`:

- `metadata.json`, `.json`, `.avro` → `Pool::Metadata` (DRAM-only)
- everything else (typically `.parquet`) → `Pool::RowGroup` (hybrid)

This is a suffix heuristic, deliberately kept in sync between the
shim (`s3_shim::pool_for`) and the plugin (`ShelfFileSystem.poolFor`)
so a shim read and a plugin read of the same object land in the same
pool and the same Foyer slot.

The HEAD-LRU (`HeadLru`, SHELF-07) is shared too: every shim
`HeadObject` hits or backfills the LRU keyed on `(bucket, s3_key)`,
and a subsequent native `HEAD /cache/:pool/origin/...` finds the
entry warm with zero extra S3 calls.

## Request IDs

Every shim response carries `x-amz-request-id: <16 hex chars>`
derived from `nanos(now) XOR std::process::id()`. It is not globally
unique — only grep-friendly within a pod — but that is enough to
correlate access-log entries with Sift / Grafana timelines. No new
dependency on `uuid` or `rand`.
