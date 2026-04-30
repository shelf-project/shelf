# ADR 0026: `/admin/pin` accepts replay-list manifest schema (RC6 P1.3)

*Status: Accepted (2026-04-30)*
*Deciders: rust-engineer-1, ops-aamir*
*Supersedes: none*
*Superseded-by: none*
*Related: ADR-0011 (cache-key spec — `sha256(etag||offset||length||rg_ordinal)`),
SHELF-24 file-driven pin loader, `agents/out/03-plan.md` rc.6 P1.3*

## Context

`POST /admin/pin` ships in v1 with one schema:

```json
{ "key_hex": "<64-hex>", "pool": "metadata|rowgroup", "mv_name": "<opt>" }
```

This is fine for callers that already hold the pre-computed
SHELF-04 cache key:

- `shelfctl pin <key_hex>` (it just forwards the operator's
  hex string).
- The H3 mv-pin-watcher Python sidecar (it computes the key
  from manifest metadata before calling).
- The SHELF-24 file-driven loader (parses `pin_list.json` and
  calls the in-process pin path, bypassing HTTP entirely).

Pre-warm tooling sits in a different category. During the rep-0
cutover prep on Apr 30 the operator wanted to push a list of
manifest-entry tuples (one per Iceberg metadata file the cutover
window would re-read) into the pinned set so the pre-warm bytes
would be immune from eviction during the cold-cache thundering
herd. The natural shape for that tooling is a replay-list:

```json
[
  { "bucket": "...", "key": "...", "etag": "...", "size_bytes": ... },
  ...
]
```

That JSON does not match the strict schema, so `POST /admin/pin`
returned `400 invalid_request`. The fallback at the time was to
re-derive `key_hex = sha256(etag || u64_le(offset) || u64_le(length)
|| u32_le(rg_ordinal))` client-side and rewrap each entry into the
strict shape — which is reasonable but means every operator team
needs to ship a copy of the SHELF-04 derivation. Workspace memory
codified this gap on 2026-04-30 as a P1 follow-up:

> `tools/gen_pin_list.py` /admin/pin schema gap — its output is a
> "replay-list" schema (manifest entries with bucket/key/etag) that
> does NOT match the strict-pin schema shelfd's `/admin/pin` POST
> handler validates; pushing it via `/admin/pin` returns a
> schema-validation error. … If pinned protection becomes
> important, either adapt `gen_pin_list.py`'s output schema OR
> teach `/admin/pin` to accept the replay schema.

This ADR picks the second path.

## Decision

Widen `POST /admin/pin`'s deserializer via a `serde(untagged)` enum
that accepts three wire shapes:

| Shape | Trigger | Body |
|---|---|---|
| `Strict` (existing) | object with `key_hex` | `{key_hex, pool, [mv_name]}` |
| `ReplaySingle` (new) | object with `bucket+key+etag+size_bytes` | manifest-entry object |
| `ReplayBatch` (new) | top-level array | array of manifest-entry objects |

Variant order is **load-bearing**: `Strict` is declared first so
serde tries it first, which means pre-RC6 callers continue to bind
to the existing variant bit-for-bit. Replay shapes only match when
the strict required fields are absent.

Replay-shape entries are converted to strict-pin internally:

```rust
let etag = entry.etag.trim_matches('"');
let length = entry.length.unwrap_or(entry.size_bytes);
let key = key_from_tuple(etag.as_bytes(), entry.offset, length, entry.rg_ordinal)?;
let key_hex = key.to_hex();
```

This is exactly the algorithm `tools/gen_pin_list.py:_sha256_key`
already implements, and exactly the algorithm
`crate::store::key_from_tuple` already implements — so a key
computed from the same Iceberg manifest entry hits the same Foyer
slot regardless of which channel admitted it. A unit test in the
new module asserts the two derivations agree byte-for-byte for a
hand-crafted input.

### Replay-shape fields and defaults

```json
{
  "bucket":     "<s3-bucket>",        // required, audit only
  "key":        "<s3-object-key>",    // required, audit only
  "etag":       "<etag-as-string>",   // required (quotes stripped)
  "size_bytes": <u64>,                // required (and > 0)
  "pool":       "metadata|rowgroup",  // optional, default "metadata"
  "offset":     <u64>,                // optional, default 0
  "length":     <u64> | null,         // optional, default size_bytes
  "rg_ordinal": <u32>,                // optional, default 0
  "mv_name":    "<schema.table>"      // optional, Track H5
}
```

Defaults are chosen to match `gen_pin_list.py`'s "whole metadata
file, manifest pool" output, which is the realistic 95th-percentile
case for cutover-window pre-warm. A pre-warm tool that wants to
pin row-group-level ranges supplies non-zero `offset` / `length`
/ `rg_ordinal` explicitly.

### Response shapes

| Wire shape | Response shape |
|---|---|
| `Strict` (single) | unchanged: `{pinned, pool, pinned_bytes, pinned_count, mv_name}` (+ a new optional `audit` field) |
| `ReplaySingle` | same as strict — single entry |
| `ReplayBatch` | new: `{accepted, rejected, pinned_bytes, pinned_count, results: [{key_hex, pool, status, audit, mv_name}, ...]}` where `status ∈ {"pinned", "not_resident", "invalid_key"}` |

The strict-form response is **bit-compatible** with pre-RC6 — the
new optional `audit` field is empty string for strict callers and
clients that don't know about it ignore unknown JSON fields by
default. The batch form is a new contract that only callers
sending the array shape see; they self-select into it.

### Cardinality cap

Batch arrays are capped at `MAX_REPLAY_BATCH = 65 536` entries per
request, the same value `cache_contains` uses. This is the upper
bound of "one Iceberg snapshot's manifest file count" by a
comfortable factor and prevents a misbehaving pre-warm script from
scheduling unbounded blocking work in one POST. Larger pre-warm
sets must be chunked client-side.

### `?caller=` is **NOT** added

We considered adding an audit query parameter (mirroring P1.2's
`/admin/cap-ready`). Rejected because:

- The replay-list entries already carry per-entry audit metadata
  (`bucket`/`key`/`etag`); adding a request-level `caller` would
  duplicate without adding signal.
- The strict shape has no caller dimension to retrofit.

If a future need surfaces, a `caller` query parameter is wire-additive.

## Alternatives considered

### A. "Adapt `gen_pin_list.py` to emit strict shape"

Reject. Two reasons:

1. The strict shape requires `key_hex` — i.e. every pre-warm
   tool must reimplement the SHELF-04 sha256 derivation. The
   moment we have a second pre-warm tool (likely on the rep-0
   cutover the dbt MV pinner), we'd need a third copy.
2. `gen_pin_list.py`'s actual today-output is *already* the strict
   `{key_hex, pool}` shape (it computes the sha256 in Python).
   The "replay-list with bucket/key/etag" shape is a richer
   format the tool *could* emit if `/admin/pin` accepted it —
   and a richer format is what an operator scrubbing a pre-warm
   log actually wants (the `s3://bucket/key` line is far more
   greppable than a 64-char hex blob). Adapting the tool away
   from a richer format is moving in the wrong direction.

### B. Add a versioned wrapper (`{schema: "replay-v1", entries: [...]}`)

Reject for v1. The untagged-enum path is structurally simpler and
the wire shapes are unambiguous (object-with-`key_hex` vs object-
with-`bucket+key+etag` vs top-level array). If the schema
proliferates further (per-entry TTLs, priority hints), the wire
shape that adds those gets a discriminator and we revisit. ADR-0026
itself is the schema-version inventory marker for now.

### C. Two endpoints (`/admin/pin` strict, `/admin/replay-pin` replay)

Reject. The cap-ready playbook curls one URL; doubling the surface
area doubles the runbook complexity for the same operational
outcome. Pre-warm tools and `shelfctl` already convergently target
`/admin/pin`; adding a second route would force every operator to
remember which URL takes which shape.

### D. Future schema-versioning header

Park. If a v2 replay shape lands (e.g. with bloom-filter ranges,
TTLs, priority queues), the wire shape that adds those gets a
discriminator field. We can also adopt an `X-Shelf-Pin-Schema:
v2` header at that point. This ADR explicitly does **not** lock
us into a no-version future — the untagged enum can grow a
fourth variant without breaking either of the existing three.

## Rollback

The endpoint widening is wire-additive on the request side and
backward-compatible on the response side.

- **Disable**: `kubectl set image sts/shelf shelf=<previous-image>`.
  Pre-warm tooling that has switched to the replay shape will
  start receiving 400s again, but no data integrity is at risk
  because the cache is content-addressed (a key that was pinned
  via the replay path is still keyed by the same SHELF-04 hash
  it would have been keyed by via the strict path).
- **Hard revert**: revert this PR. The `audit` field on the
  strict response goes away; pre-RC6 strict clients ignore it
  anyway.

## Verification

- 11 unit tests in `shelfd/src/admin_pin_payload.rs`:
  strict shape deserializes / replay-single deserializes /
  replay-batch deserializes / replay-and-strict produce identical
  keys for same inputs / quoted vs unquoted etag yield same key /
  empty etag rejected / zero size rejected / unknown pool
  rejected / batch cap enforced / batch resolves in order /
  strict variant wins on overlap.
- 4 integration tests in `shelfd/tests/it_admin_pin_schema_flex.rs`:
  strict shape round-trips unchanged (backward-compat lock-in) /
  replay-single pins a resident key / replay-batch returns
  per-entry results with `pinned` and `not_resident` statuses /
  unknown pool returns 400.
- All 8 existing `it_admin.rs` integration tests still pass —
  the `admin_pin_rejects_unknown_key` and
  `admin_pin_raises_pinned_bytes_on_stats` tests both bind to
  the strict shape and continue to work bit-for-bit.
- All 371 existing shelfd lib tests still pass.
- `cargo clippy --all-targets -- -D warnings` clean.
- `cargo fmt --check` clean.
- `SHELF_INTEGRATION=1 cargo test -p shelfd --test
  it_admin_pin_schema_flex` passes in 0.21s wall-time (real,
  not silent-skip).

## References

- Workspace memory entry "`tools/gen_pin_list.py` /admin/pin
  schema gap" (codified Apr 30 during rep-0 cutover prep).
- ADR-0011 — cache-key derivation
  `sha256(etag || u64_le(offset) || u64_le(length) || u32_le(rg_ordinal))`.
- `tools/gen_pin_list.py:_sha256_key` — Python reference
  implementation of the same algorithm.
- `crate::store::key_from_tuple` — Rust reference implementation
  used by the file-driven SHELF-24 loader. The replay-list path
  reuses it verbatim so all three channels (file loader, strict
  POST, replay POST) compute identical keys for identical inputs.
