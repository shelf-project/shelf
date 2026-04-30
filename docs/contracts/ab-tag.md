# A/B Tag Propagation Contract — `X-Shelf-Tag` v1

**Spec version:** 1
**Status:** Stable
**Owner:** SHELF-42
**Companion tickets:** SHELF-37 (event listener consumer), SHELF-40 (`shelf_s3_dollars_saved_total` consumer)

This contract defines how an A/B label flows from a Trino query session to
the shelfd cache plane and out to downstream observability surfaces. It is
the **only** sanctioned way for an analyst or operator to attach a cohort
label to a query without spinning up a parallel catalog.

The contract is deliberately narrow: it describes a single HTTP header,
its on-the-wire shape, the cardinality rules that protect Prometheus, and
the lifetime rules that protect the cache from cross-query bleed. It does
**not** describe runtime A/B routing — that requires upstream Trino SPI
work tracked in `trinodb/trino#29184` and is out of scope for v1.

## 1. Vocabulary

- **Tag**, **Tag set** — a small map of `{key: value}` string pairs
  attached to one query. `{"experiment": "b1_on", "cohort": "rep1"}` is a
  two-entry tag set.
- **Wire form** — the URL-encoded JSON form of a tag set carried on the
  `X-Shelf-Tag` HTTP header.
- **Cap** — the maximum number of distinct tag *values* a shelfd pod is
  willing to expose as a Prometheus label within a scrape window.
- **Sentinel `other`** — the label value used for any tag value above the
  cap, mirroring the `table_label` sentinel from `s3_shim.rs`.

## 2. Header

| Property      | Value                                                                           |
| ------------- | ------------------------------------------------------------------------------- |
| Header name   | `X-Shelf-Tag`                                                                   |
| Multiplicity  | At most **one** `X-Shelf-Tag` header per request. Multiple instances ⇒ reject.  |
| Body          | URL-encoded UTF-8 JSON object. Decoded body MUST be a JSON object.              |
| Size          | ≤ **4096 bytes** (after URL-decoding). Larger payloads ⇒ reject.                |
| Compatibility | Header absent ⇒ tag is empty. Empty tag is **never** an error.                  |

Decoded JSON shape:

```json
{
  "experiment": "b1_compression_on",
  "cohort": "prod_rep1",
  "epoch": 1714512345
}
```

Rules for the JSON object:

- Keys are non-empty ASCII identifiers matching `[A-Za-z_][A-Za-z0-9_]{0,63}`.
- Values are scalars only — JSON `string`, `number` (integer or finite
  float), or `boolean`. Nested objects, arrays, and `null` ⇒ reject.
- Numeric and boolean values are coerced to their canonical string form
  (`"42"`, `"true"`) at ingest time so the rest of the pipeline only
  handles `Map<String, String>`.
- At most **8 keys** per tag set. Above the limit ⇒ reject.
- Each value, after coercion, is at most **128 bytes** UTF-8. Above the
  limit ⇒ reject.

A request whose `X-Shelf-Tag` header fails any of these rules is treated
as if the header were absent. shelfd MUST NOT fail the underlying
GET/HEAD because of a malformed tag — the cache contract is "tags
propagate, but never block reads".

## 3. Reserved keys

These keys carry stable meaning across surfaces (listener, cost counter,
dashboards). Operators are free to add additional keys, but must not
collide with the reserved set.

| Key          | Type    | Meaning                                                                                       |
| ------------ | ------- | --------------------------------------------------------------------------------------------- |
| `experiment` | string  | Lever name under test (`b1_compression_on`, `shelf_46_bloom`, …). Free-form within `[a-z0-9_]+`. |
| `cohort`     | string  | Operator-defined cohort (`prod_rep1`, `bi_canary`, `dbt_baseline`, …).                        |
| `epoch`      | integer | Salt-rotation epoch. Used by analysis SQL to bucket runs across salt windows.                 |

Unknown keys are passed through unchanged but are subject to the same
length / cardinality rules as reserved ones.

## 4. Cardinality cap

A shelfd pod publishes per-tag metrics by attaching a single `tag` label
whose value is the URL-encoded JSON wire form, normalised by sorting keys
lexicographically before encoding. The number of *distinct* `tag` label
values exposed by a single pod within a scrape window is capped at
`cache.abTag.maxDistinctTags` (default **16**).

When the cap would be exceeded, the offending tag falls back to the
sentinel label value `other` and the counter
`shelf_ab_tag_cap_violations_total{reason="cardinality"}` is incremented
exactly once per scrape window. A one-time `WARN` log line records the
event with the offending tag's normalised wire form.

The cap is applied *per pod*, not cluster-wide, because each pod
publishes its own Prometheus series; this matches the existing precedent
set by `HITS_BY_TABLE_TOTAL` (see `shelfd/src/metrics.rs`).

## 5. Lifetime

A tag belongs to **a single HTTP request**. shelfd MUST NOT persist tag
values across requests, MUST NOT store them in cache keys, and MUST NOT
infer a tag from a previous request to the same key. Cache keys remain
content-addressed by ETag (ADR-0011) — tag propagation is purely a
*labelling* concern.

Two practical implications:

1. The same byte-range is served from the same Foyer slot regardless of
   which tag the request carried. Two requests with different tags share
   the same cache content.
2. After a request returns, the per-request tag context is dropped. The
   downstream listener path (SHELF-37) reads the tag from the *Trino
   session* directly, not from shelfd's request context — they
   independently materialise the same label from the same origin
   (session properties).

## 6. Trino session-property convention

Tags originate as Trino session properties under the `shelf.tag.*`
namespace. A Trino session sets:

```sql
SET SESSION shelf.tag.experiment = 'b1_compression_on';
SET SESSION shelf.tag.cohort = 'prod_rep1';
SET SESSION shelf.tag.epoch = '1714512345';
```

The Shelf Trino plugin's HTTP-client layer collects every session
property whose name starts with `shelf.tag.` (case-sensitive), strips the
prefix, builds a tag-set map, sorts keys lexicographically, JSON-encodes,
URL-encodes, and attaches the result on `X-Shelf-Tag`. The plugin's
session-property forwarding is **always on** — it is metadata only and
carries no measurable runtime cost.

The same `shelf.tag.*` session properties are visible to the SHELF-37
event listener via `QueryContext.getSessionProperties()`. The listener
materialises the same map and writes it to its `tags_json` column.
shelfd and the listener thus surface the *same* tag for the *same*
query without the listener depending on shelfd-emitted state.

## 7. Default-off propagation

shelfd's reception path (`s3_shim.rs` header extraction → `ab_tag` module
→ optional `tag` label on metrics) is gated behind
`cache.abTag.enabled`. The chart default is `false` so a freshly
deployed OSS cluster never exposes tag-cardinality surface area until an
operator explicitly opts in. Penpencil overlay (and any operator who has
sized their Prometheus retention for the cap) sets `enabled: true`.

The Trino plugin's *forwarding* side does not have an enabled toggle —
session properties are metadata, the cost of attaching one HTTP header
to a request that is already crossing a Service is negligible, and a
tag with no downstream consumer (because shelfd has `enabled: false`)
is silently ignored.

## 8. Versioning

This is the **v1** contract. Future minor revisions (`v1.x`) MUST be
backwards-compatible at the wire level: an old plugin and a new shelfd
or vice-versa must continue to parse each other's headers without error.
Breaking changes to the wire format require a `v2` spec, a new header
name (`X-Shelf-Tag-V2`), and a transition period during which both
headers are accepted.

## 9. Test vectors

Five canonical tag sets that both the Java and Rust parsers MUST accept
and serialise identically live at
`tests/fixtures/ab-tag-vectors.json`. Both implementations have unit
tests asserting parity. Adding a new vector to that file is the canonical
way to extend test coverage; do not duplicate fixtures across the two
languages.

## 10. Out of scope

- Runtime A/B routing (tag → "use shelfd" vs "use direct S3"). Requires
  upstream Trino SPI — see `trinodb/trino#29184`.
- Mutating the cache key or admission decision based on a tag. Tags are
  observability metadata only.
- Persistence of tags into shelfd's pin-list, MV registry, or any other
  per-key state.
- Cluster-wide cardinality enforcement. Each pod manages its own cap;
  Prometheus aggregates.
