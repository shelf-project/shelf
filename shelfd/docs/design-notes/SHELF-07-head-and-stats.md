# SHELF-07 + `/stats` — HEAD endpoint, HEAD-LRU, and the stats contract

Ticket scope:

- **SHELF-07** — `HEAD /cache/:pool/origin/:bucket/*s3_key` backed by
  an in-memory LRU keyed on `(bucket, s3_key)` with a default capacity
  of 10 000 entries.
- **`GET /stats`** — the JSON contract Agent 5 (SHELF-20) consumes to
  weight the HRW ring.

## Public types

- `origin::ObjectHead { content_length, etag, last_modified }` — gains
  `last_modified: Option<String>` (RFC-3339). `Origin::head` now
  returns `crate::Result<Option<ObjectHead>>`; `None` signals
  `HeadObject → 404 NoSuchKey`.
- `head_lru::HeadMeta { content_length, etag, last_modified }` — the
  LRU value. Cloned by value (all fields are cheap).
- `head_lru::HeadLru` — `foyer::Cache<(String, String), Arc<HeadMeta>>`
  with an entry-count weighter (each entry has weight 1, so the Foyer
  capacity is the entry count).
- `http::ServerState` — new fields `head_lru: Arc<HeadLru>` and
  `pod_id: Arc<str>`. `ServerState::new` keeps its existing signature
  and builds defaults from env (pod_id = `SHELFD_POD_ID` → `HOSTNAME`
  → `"shelfd-unknown"`; head_lru = 10 000 entries). A new
  `ServerState::with_head_lru_and_pod_id(..)` builder threads through
  explicit values from `main`.
- `control::Stats` — reshaped to the contract Agent 5 consumes:

  ```json
  {
    "pod_id": "shelf-2",
    "capacity_bytes": 12884901888,
    "used_bytes":      3221225472,
    "metadata_pool": { "capacity_bytes": ..., "used_bytes": ... },
    "rowgroup_pool": { "capacity_bytes": ..., "used_bytes": ... }
  }
  ```

## Module layout

New file:

- `shelfd/src/head_lru.rs`

Touched files:

- `shelfd/src/origin.rs` — extend `ObjectHead`, map 404 → `Ok(None)`,
  surface `last_modified` as RFC-3339.
- `shelfd/src/http.rs` — wire HEAD + `/stats` routes, extend
  `ServerState`.
- `shelfd/src/control.rs` — reshape `Stats`.
- `shelfd/src/config.rs` — add `head_lru_entries` (default 10_000) and
  `SHELFD_POD_ID` env override.
- `shelfd/src/metrics.rs` — add `shelf_head_hits_total{pool}` and
  `shelf_head_misses_total{pool}`.
- `shelfd/src/main.rs` — build the HeadLru and thread `pod_id`
  through.
- `shelfd/src/lib.rs` — expose the new module.

## Invariants

- `Origin::head` **never** panics; 4xx returns a typed `Ok(None)` so
  the handler can emit 404 without string-matching error messages.
- HEAD handler **never** issues a GET — either the LRU answers or one
  `HeadObject` call answers.
- The LRU is capped: entries beyond `head_lru_entries` are evicted by
  foyer's built-in SIEVE.
- `/stats` is lock-free: `FoyerStore::{used_bytes, capacity_bytes}` are
  already atomic reads against Foyer state.
- Fail-open: 5xx on `HeadObject` yields `502` — the plugin's
  `ShelfFileSystem` then falls through to S3.

## Route shape

- `HEAD /cache/:pool/origin/:bucket/*s3_key` (new) — returns
  `Content-Length`, `X-Shelf-ETag`, `X-Shelf-LastModified`.
- `GET /stats` (new) — JSON, `Content-Type: application/json; charset=utf-8`.
- The older `HEAD /cache/:pool/:key` stub is retired; the client now
  passes the origin `(bucket, key)` directly because it does not own
  the content-addressed hash without first knowing the size.

## New dependencies

None. The LRU is built on top of the existing `foyer` dep.

## Test plan

Unit (in `shelfd/src/head_lru.rs`):

- `hit_returns_cached_meta`
- `miss_returns_none`
- `insert_then_get_round_trip`
- `capacity_enforced_lru_evicts_oldest`

Unit (in `shelfd/src/http.rs`):

- `stats_payload_serializes_with_contract_keys`

Integration (new, `shelfd/tests/it_head_stats.rs`, gated on
`SHELF_INTEGRATION=1`):

- `head_returns_content_length_matching_object_size`
- `second_head_hits_lru_after_origin_delete`
- `head_on_missing_object_returns_404`
- `stats_reflects_pool_usage_after_cache_populate`

## Deferred follow-ups

- Single-flight on HEAD misses (volume is ≪ GET; left for a later
  ticket if we ever see a thundering-herd HEAD workload).
- Origin-side per-bucket `HEAD` rate-limiter (SHELF-3x).
- `pinned_bytes` on `/stats` — requires SHELF-24 pin list; returns 0
  today.
