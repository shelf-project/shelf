# SHELF-44: `POST /v1/hint` engine-agnostic plan-hint REST surface

**Status:** Draft
**Tier:** S
**Estimated effort:** S
**Depends on:** none
**Blocks:** none

> **Sequencing note: do not start before SHELF-23 lands.** This ticket adds a new route to `shelfd/src/http.rs`, which SHELF-23 currently has in-flight on `shelf-23-peer-fetch`. Resume after SHELF-23 merges.

## Problem (OSS-cited)

Spark and Trino each build their own pre-fetch path; Shelf's existing `ShelfPrefetchListener` is Trino-only via the `EventListener` SPI. No upstream protocol exists for "engine-X tells the cache to prepare these manifests." Trino issue [trinodb/trino #29184](https://github.com/trinodb/trino/issues/29184) (blob-cache SPI) is the long-tail upstream answer; until it lands, a thin REST surface unblocks every other engine — Spark, DuckDB, Polars, Daft, ClickHouse, StarRocks, PyIceberg — that might want to push hints.

## Goal

A single REST endpoint `POST /v1/hint` accepts `{snapshot_id, manifest_paths[], data_file_paths[], priority}` and schedules prefetch via the existing `Prefetch` machinery; engines integrate with one HTTP call.

## Approach

New route in `shelfd/src/http.rs` under `/v1/hint`, wired into the existing axum router. Request body schema (committed at `protos/v1_hint.json` for cross-language consumption):

```json
{
  "snapshot_id": "1234567890",
  "table": "cdp.icesheet.silver_offline_event_data_2026",
  "manifest_paths": ["s3://.../manifest-list-...avro", "s3://.../manifest-...avro"],
  "data_file_paths": ["s3://.../data-...parquet"],
  "priority": 0,
  "deadline_ms": 50,
  "tenant": "bi"
}
```

Response: `200 OK` with `{accepted: N, queued: N, rejected: N, queue_depth: N}`; `429 Too Many Requests` if the per-tenant queue is full; `400 Bad Request` if any path fails the allowed-prefix validator. Hint is **fail-open by contract** — engines never wait on it; client retry is opt-in. Internal flow:

1. `http::handlers::v1_hint` validates the body (path prefix allowlist from `config.hint.allowed_prefixes`, max 1024 paths/request).
2. Submits `Prefetch` requests to the existing prefetch worker (`shelfd/src/peer.rs` / `shelfd/src/peer_fetch.rs` for cross-pod fan-out, `shelfd/src/origin.rs` for direct fetch).
3. Each request tags the `tenant` so SHELF-48 priority lanes can dequeue with weighting.
4. Per-tenant bounded queue (default 1024 entries); overflow → 429 + `shelf_hint_dropped_total{tenant}` Prom counter.

Auth: optional bearer token configured via `config.hint.auth.bearer_tokens`. Default off in dev / on in prod.

## Acceptance criteria

- [ ] `POST /v1/hint` returns within 10 ms p99 for a 100-path body on a warm process.
- [ ] At default queue depth 1024, a 2 K-path body returns 429 with `Retry-After`.
- [ ] Hints flow into the same prefetch worker that `ShelfPrefetchListener` already uses (single queue, single worker).
- [ ] Prom counters: `shelf_hint_received_total`, `shelf_hint_dropped_total`, `shelf_hint_accepted_total{priority}`.
- [ ] Auth: with bearer-token config set, a missing/wrong token returns 401; with config unset, no auth required.
- [ ] OpenAPI schema committed at `shelfd/docs/api/v1_hint.yaml`; CI runs `redocly lint` and rejects schema drift.
- [ ] Integration test issues a hint, waits for the prefetch worker to drain, then issues a `GET /cache/...` and asserts a hit (warm-from-hint path).
- [ ] Cross-language smoke: `examples/duckdb/hint_demo.py` posts a hint and reads back; runs in CI.

## Out of scope

- Engine-side adapters (Spark connector, DuckDB extension). Examples ship under `examples/` but in-tree adapters are post-v1.
- gRPC variant — REST only in v1.
- Authn beyond bearer token (mTLS, OIDC).
- Retries / acknowledgements — the protocol is fire-and-forget by design.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Abusive caller floods the queue | Per-tenant bounded queue; per-tenant rate limiter; queue-overflow → 429. |
| Path-allowlist bypass | Strict prefix match; URL canonicalisation before allowlist check; unit tests per CWE-22 fixture. |
| Hint payload references stale snapshot | Snapshot id passes through to prefetch worker as a tag only; cache invalidation still runs on content-key, so stale hints are harmless. |
| Bearer-token leakage in logs | Token redaction filter on `tracing` events. |

## Test plan

- Unit tests: body validation, path-allowlist, per-tenant queue overflow, auth path with/without token.
- Integration tests: `shelfd/tests/it_hint.rs` posts hints + asserts cache-warm follow-up.
- Cross-language smoke: example clients in `examples/duckdb` and `examples/polars` invoke `/v1/hint` over HTTP.
- (If applicable) docker compose smoke: SHELF-12 extension `make hint-smoke` asserts hint-then-read returns hit.

## Open questions

- Should `priority` be 0 (highest) – 10 (lowest) like the existing prefetch queue, or 1–5 like REST conventions? Recommend 0–10 to match internal queue for zero-translation.
- `deadline_ms` semantics: hard deadline drops the hint, soft deadline reorders the queue? Recommend hard, with a Prom counter for `expired` hints.
- Filed upstream TIP referencing #29184 (already item #2 in `agents/out/03-plan.md` §8) — link from this ticket's doc.
