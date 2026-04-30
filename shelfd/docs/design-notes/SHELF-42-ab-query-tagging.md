# SHELF-42 — A/B query tagging (design note)

**Status**: implemented in `0.1.x` — receive path default-off in OSS, opt-in via Helm.

**Owner**: shelf-core. Touches `clients/trino/` (Java) and `shelfd/` (Rust).

**Related**: [`docs/contracts/ab-tag.md`](../../../docs/contracts/ab-tag.md) (wire contract). Downstream consumers: SHELF-37 (event listener that writes `tags_json`) and SHELF-40 (`shelf_s3_dollars_saved_total{tag}`); both depend on this PR's wire format and are sibling tickets.

## TL;DR

Without an A/B tag, every cost / cache-effectiveness diff between two configurations (B1 compression on/off, SHELF-46 bloom on/off, SHELF-49 row-group pruning on/off, SHELF-50 metadata cache on/off) is contaminated by traffic shifts and naturally evolving query mix. SHELF-42 makes per-cohort attribution honest by piping a small, sorted, validated `{key:value}` map from a Trino session through the shelf data plane and onto the existing hit / miss / response-byte counters. The map is the only A/B knob: shelfd does not branch on its content. Tagging is observability, not control.

## Lifecycle

```
                          ┌─────────────────────────────────────────────┐
                          │   Trino session (analyst / operator)        │
                          │                                              │
                          │   SET SESSION shelf.tag.experiment =         │
                          │                'b1_compression_on';          │
                          └───────────────────────┬─────────────────────┘
                                                  │ session props /
                                                  │ clientTags
                                                  ▼
                          ┌─────────────────────────────────────────────┐
                          │  io.shelf.tag.SessionTagProvider             │
                          │  (per-thread; AutoCloseable lifetime)        │
                          │                                              │
                          │  fromSessionProperties(...) → TagSet         │
                          │     - filter `shelf.tag.*` prefix            │
                          │     - validate keys (regex), values (≤128B)  │
                          │     - sort keys lexicographically            │
                          │     - size cap (≤8 keys)                     │
                          └───────────────────────┬─────────────────────┘
                                                  │ TagSet#toWire()
                                                  │ → URL-encoded JSON
                                                  ▼
                          ┌─────────────────────────────────────────────┐
                          │  io.shelf.client.ShelfHttpClient             │
                          │   .withTagProvider(provider)                 │
                          │   .rangeGet(...)                             │
                          │     ⇒ stamps header `X-Shelf-Tag: <wire>`    │
                          └───────────────────────┬─────────────────────┘
                                                  │  HTTP/2 GET
                                                  ▼
                          ┌─────────────────────────────────────────────┐
                          │  shelfd s3_shim.rs / native router           │
                          │   crate::ab_tag::extract_from_headers()      │
                          │     - reject if disabled                     │
                          │     - reject duplicate / non-ASCII / bad JSON │
                          │     - cap: 16 distinct/pod/scrape window     │
                          │           else fold to label `tag="other"`   │
                          │     - bump shelf_ab_tag_cap_violations_total │
                          │       once per (window, offending wire)      │
                          │   ⇒ TaggedContext { tag, label }              │
                          └───────────────────────┬─────────────────────┘
                                                  │ per-request only
                                                  │ (no thread-locals,
                                                  │  no cache-key inputs)
                                                  ▼
                          ┌─────────────────────────────────────────────┐
                          │  metric increments — companion series:       │
                          │   shelf_hits_by_tag_total{pool, tag}         │
                          │   shelf_misses_by_tag_total{pool, tag}       │
                          │   shelf_s3_shim_response_bytes_by_tag_total{ │
                          │     verb, outcome, tag}                      │
                          │  Existing series (hits_total etc.) untouched.│
                          └─────────────────────────────────────────────┘
```

## Cardinality story

The single biggest risk of "free-form tagging" is Prometheus cardinality blow-up. SHELF-42 mirrors the `table_label` precedent shelfd already established (see `s3_shim::table_label`):

- Per-pod state in `AbTagState` tracks the set of admitted tag wire-forms inside the current scrape window.
- The 17th distinct tag in the window is mapped to the sentinel string `other`, so the per-tag series stays bounded at `(cap + 2) × pools × outcomes` even under hostile clients (`cap` admitted + `other` + `none` for the unset case).
- Each *new* offending tag bumps `shelf_ab_tag_cap_violations_total{reason="cardinality"}` exactly once per window and emits a one-shot `WARN`. Repeated requests with the same offender stay quiet, so a misconfigured pipeline does not flood logs.
- Window length defaults to 60 s (longer than Prometheus' 30 s scrape interval, short enough that a stale cohort eventually rolls out of the sentinel).
- Cap and window are configurable via `cache.abTag.maxDistinctTags` and `cache.abTag.scrapeWindow`.

The cap is enforced *only* on the receive side. Trino-side forwarding has no kill switch — it is pure metadata and would otherwise become an operational footgun ("did the tag get set or not?" debugging).

## Default-off in OSS, on in operator overlays

The Helm chart's default `cache.abTag.enabled: false` reflects the OSS-friendly stance: a fresh shelf deployment sees no cardinality bloom even if a misbehaving client starts spamming `X-Shelf-Tag`. The header is read, validated, ignored.

Operator overlays (e.g. a `<prod-overlay>/values-prod.yaml`) can flip the toggle to `true`. Typical operator rationale is concrete: the post-cutover analysis SQL on the query-log Iceberg table already relies on per-cohort labelling, Prometheus retention is sized for the per-tag series, and the SHELF-37 listener feeds the Iceberg event log with the same map.

The Trino plugin's session-property forwarding has no toggle — it is metadata, no perf cost, and we prefer "always on, ignored on the receive side" over "configure two things to make tagging work".

## Per-request lifetime

Tags belong to a single request. That is enforced four ways:

1. `TaggedContext` is built inside `s3_shim::handle_get_object` / `handle_head_object` and dropped when the response future completes.
2. `crate::ab_tag` exposes no global state; consumers either receive the context as a function argument (the metric helpers do this) or call `extract_from_headers` themselves.
3. `SessionTagProvider` (Java) uses a `ThreadLocal` only for the brief window the worker thread is processing one Trino split; the `AutoCloseable` returned by `install(...)` guarantees the slot is cleared even on exception.
4. Tags are **not** part of the cache key. SHELF cache keys are content-addressed by ETag (see [`docs/adr/0011-content-addressed-keys-by-etag.md`](../../../docs/adr/0011-content-addressed-keys-by-etag.md)). Adding a tag dimension to keys would break the per-snapshot semantics shelf relies on for Iceberg correctness.

## Interaction with SHELF-37 / SHELF-40

- **SHELF-37** (event listener, sibling PR): reads the same `shelf.tag.*` session properties off the Trino `QueryCreatedEvent` and writes the resulting JSON map into a `tags_json` column in the Iceberg trino-events table. This PR exposes the wire-level contract (`docs/contracts/ab-tag.md`) the listener follows; the actual listener code lives in SHELF-37.
- **SHELF-40** (cost counter, sibling PR): adds `shelf_s3_dollars_saved_total{tag}` to the `shelf_s3_dollars_saved_total` family. This PR's `Cargo.toml` introduces a `[features]` flag `ab_tag = []` so SHELF-40's optional wiring can be feature-gated against this PR landing first. While SHELF-40 is unmerged, the feature flag is a no-op; once SHELF-40 lands, it depends on the public `crate::ab_tag::TaggedContext::metric_label()` accessor exposed here.

## Tested invariants

- **Round-trip golden vectors** between Java and Rust live in `tests/fixtures/ab-tag-vectors.json`. Both sides parse the fixture and must agree on the wire form for each entry. This is the contract regression guard; do not edit one side without updating the other.
- **Receiver fail-open**: malformed / oversized / duplicate-instance `X-Shelf-Tag` headers behave identically to "header absent". The shim continues to serve the read; only the per-tag metric labelling is affected.
- **Cardinality cap fires without flooding**: a 32-distinct-tag synthetic load fires the cap-violation counter exactly once per offending tag per scrape window, and rolls over cleanly when the window resets (see `shelfd/src/ab_tag.rs::tests::cardinality_cap_*`).
- **Plugin fails open on provider crash**: a `TagProvider` implementation that throws does not break the read; the request goes out without `X-Shelf-Tag` (`ShelfHttpClientTagTest::misbehavingProviderFailsOpen`).

## Out of scope (intentionally)

- **Encrypting the tag.** Tags are an analyst convenience, not a security boundary. Operators who must hide cohort identity strip the header at the load balancer or rename keys before they reach shelfd.
- **System-property registration of `shelf.tag.*` in Trino.** Trino requires session-property names to be statically registered per-catalog. The pragmatic surface today is: the operator's coordinator-side glue (or the SHELF-37 listener) reads the tag from `clientTags` / catalog session-properties and calls `SessionTagProvider.install(...)` before the worker thread enters the Shelf data plane. Once trinodb/trino#29184 lands a stable file-system-factory hook, the plugin's own factory can read `ConnectorSession` directly and the install step becomes implicit.
- **Tag-aware cache admission / eviction.** Out of scope and would violate the per-request-lifetime invariant. The roadmap is "tag observes, never decides".
