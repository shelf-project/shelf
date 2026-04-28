# SHELF-23 + SHELF-24 — admin surface + pin-list loader

This note explains the two pieces that ship together: the operator
HTTP surface (`/admin/*`) behind `shelfctl`, and the pin-list loader
that seeds `shelfd`'s in-memory allowlist from S3.

## Why a separate `/admin/*` URL prefix

`shelfd` already exposes three URL surfaces:

| Prefix         | Purpose                                         | Port (default) |
|----------------|-------------------------------------------------|----------------|
| `/cache/*`     | Native content-addressed read path (SHELF-06)    | 8080           |
| `/stats`, `/metrics`, `/healthz`, `/readyz` | Plugin + scrape contract | 8080           |
| `/:bucket/*key` | SHELF-22 S3-compat shim                         | 9092           |
| `/admin/*`     | SHELF-23 operator control plane                 | 8080           |

The admin surface sits on the same port as `/cache/*` because they
share an `axum::Router` and speak the same TLS stance. What we do NOT
want is for `/admin/*` to appear on the SHELF-22 shim port — generic
S3 clients should never be able to mutate server state. Keeping a
distinct URL prefix means a reverse-proxy rule (`location /admin/`)
can block the entire control plane in one line without enumerating
routes. It also lets us evolve the admin surface (HTTP/2 pushes, SSE
progress on long reloads, Bearer auth) without touching the shim or
the data-plane contract.

## Route shapes

All admin routes return JSON. Errors carry
`{"error": "<kind>", "detail": "<human-readable>"}`.

| Method | Path            | Body                                                  | Notes                                                        |
|--------|-----------------|-------------------------------------------------------|--------------------------------------------------------------|
| GET    | `/admin/ring`   | —                                                     | Returns `[{pod_id, weight, healthy}, …]`                     |
| POST   | `/admin/pin`    | `{"key_hex":"<64-hex>","pool":"metadata"\|"rowgroup"}` | `404` when key is not resident in the requested pool         |
| POST   | `/admin/unpin`  | `{"key_hex":"<64-hex>"}`                              | `404` when key was not pinned. No `pool` — unpin is pool-agnostic because content-addressed keys are unique across pools |
| POST   | `/admin/evict`  | `{"key_hex":"<64-hex>","pool":"metadata"\|"rowgroup"}` | `404` when key is not resident in that pool; pin-set preserved |
| POST   | `/admin/reload` | —                                                     | `200 {pinned_bytes, pinned_count, reload_ok: true}`. When no loader is configured the same shape is returned with zeros — a no-op reload is a success, not an error |

`/admin/ring` currently returns a single-member array reflecting the
self pod — real HRW membership is SHELF-20's responsibility. When
SHELF-20 lands, swap the handler body for
`state.router.membership()` without changing the JSON shape; the
`shelfctl` CLI already aligns to that contract.

## Reload semantics — replacing (decision)

On each reload the loader **diffs** the freshly-fetched pin list
against the currently-installed set and applies:

- keys present in both: left pinned (no-op);
- keys only in the current set (left the JSON): **unpinned**;
- keys only in the new set: **pinned** (when resident).

### Why replacing, not additive

`pin_list.json` is maintained as a declarative list in a config bucket
— adding or removing entries is how operators express intent. An
additive model would leak pins once an entry was deleted from the
JSON, forcing an out-of-band unpin to catch up. Replacing matches
what operators expect when they edit the list in place.

### Consequence: race with `shelfctl pin`

A manual `shelfctl pin <key>` installs an entry that is NOT in
`pin_list.json`. The next reload will unpin it. This is intentional:
if you want a key to survive reloads, add it to the JSON. The CLI is
for transient, incident-response pinning; the JSON is the source of
truth.

## SIGHUP mechanics

- On Unix we register a `tokio::signal::unix::signal(SignalKind::hangup())`
  listener in [`crate::pinlist::spawn_sighup_listener`]. Each SIGHUP
  nudges a `tokio::sync::Notify` the loader select-loop observes.
- On non-Unix platforms the listener is a no-op. The timer and
  `/admin/reload` still work — only `SIGHUP` becomes unavailable.
- Shutdown is wired via a `CancellationToken` so the loader exits
  cleanly on SIGTERM / SIGINT (see `shelfd/src/main.rs::spawn_signal_handler`).

## `pin_list.json` schema

```json
{
  "version": 1,
  "entries": [
    { "key_hex": "aa11bb22…", "pool": "rowgroup" },
    { "key_hex": "cc33dd44…", "pool": "metadata" }
  ]
}
```

- `version` — integer. v1 today. A v2 with breaking changes (TTLs,
  priority, labels) would bump this and the loader would refuse the
  unknown version rather than silently misparsing.
- `entries[].key_hex` — lower-case 64-char SHA-256 hex; same
  content-addressed key used on `/cache/:pool/:key/:range`. Malformed
  entries are logged at WARN and skipped; the rest of the list still
  applies. Counted in `skipped_missing` if the key is not resident in
  the declared pool.
- `entries[].pool` — **required**, `"metadata"` or `"rowgroup"`. The
  loader needs this to look the byte-length up in the right Foyer
  cache on pin. An unknown pool value is treated like a malformed
  key: WARN log, entry skipped.

A bare JSON array (the v0 shape used in early drafts) is **rejected**
at parse time so an operator who forgets the `{"entries": ...}`
wrapper sees a loud error instead of a silent empty load.

## Config schema

```yaml
pin_list:
  bucket: "shelf-config"            # required
  key: "shelf/pin_list.json"        # optional; default shown
  refresh_period: "15m"             # humantime; optional; default shown
  enabled: true                     # optional; default shown
```

Omitting the `pin_list:` stanza entirely is equivalent to setting
`enabled: false` — the loader is never spawned and
`POST /admin/reload` returns `503 {"error":"loader_disabled"}`.

## Acceptance evidence

- `cargo test -p shelfd --lib pinlist::` — unit tests for parsing.
- `cargo test -p shelfd --test it_admin` — black-box HTTP tests:
  `/admin/ring` shape, `/admin/pin` updates `/stats.pinned_bytes`,
  `/admin/evict` drops the entry, `/admin/reload` reports `503` when
  the loader is disabled.
- `cargo test -p shelfctl` — `--help` smoke test across every
  subcommand.

## Admission bypass path

SHELF-25's `SizeThresholdPolicy::decide` refuses inserts larger than
`admission.size_threshold_bytes` unless the entry is pinned. The pin
flag does not cross module boundaries as a global — it is threaded
through the miss path:

1. `FoyerStore::get_or_fetch(pool, key, &admission, fetch)` computes
   `ctx.pinned = self.is_pinned(&key)` before calling `admission.decide(&ctx)`.
2. `AdmissionContext { pool, key, size_bytes, pinned }` is the
   single place the flag lives. Adding a new admission backend means
   reading `ctx.pinned`; there is no other channel.
3. `SizeThresholdPolicy::decide` short-circuits to `Admit` when
   `ctx.pinned && self.pinned_bypass`.

The regression test `admission::tests::pinned_keys_bypass_size_threshold`
pins the full wiring: pin a key, evict its bytes, then issue a
`get_or_fetch` with a 32-byte payload against a 16-byte policy. Without
the bypass the bytes would be served-not-cached; with the bypass the
next `get` is a straight hit. If a future refactor forgets to plumb
`is_pinned` through the context, this test fails loudly.
