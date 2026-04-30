# SHELF-41: Public `/savings` SPA — Tailscale-style savings tile

**Status:** Draft
**Tier:** S
**Estimated effort:** S
**Depends on:** SHELF-40
**Blocks:** none

> **Sequencing note: do not start before SHELF-23 lands.** This ticket adds a new public route to `shelfd/src/http.rs` (or a sibling shim listener), which SHELF-23 currently has in-flight on `shelf-23-peer-fetch`. Resume after SHELF-23 merges to keep diffs reviewable.

## Problem (OSS-cited)

Internal Grafana is gated; nobody outside the team can see Shelf is up or what it saved. Adopters want a 1-URL "is your cache healthy and saving us money?" link to put in postmortems and procurement decks. [Tailscale's share-link pattern](https://tailscale.com/blog/share-data-securely-with-share-link/) is the public-facing comparable. Cloudflare R2 publishes aggregate egress savings ([blog](https://blog.cloudflare.com/r2-zero-egress-egress-fees/)) but not per-table. No OSS cache today exposes a public, signed, read-only savings tile.

## Goal

A static SPA at `/savings` (and a sibling `/` index) renders the SHELF-40 dollars-saved counter plus per-table breakdown, served read-only with no admin actions, suitable for embedding in public-facing dashboards.

## Approach

Single-page React (or Preact) app committed under `shelfd/web/savings/` and bundled into the `shelfd` binary via `include_dir!` (same pattern as the existing `shelfd/src/ui.rs`). New route `GET /savings` in `shelfd/src/http.rs` serves the SPA shell; data comes from a new JSON endpoint `GET /v1/savings.json` that aggregates the SHELF-40 counter into a stable wire schema:

```json
{
  "as_of": "2026-04-28T17:00:00Z",
  "window_seconds": 86400,
  "totals": { "dollars_saved": 412.30, "bytes_served_from_cache": 8.4e12, "queries_accelerated": 1842 },
  "per_table": [
    { "table": "cdp.icesheet.silver_offline_event_data_2026", "dollars_saved": 312.10, "hit_ratio": 0.86 }
  ],
  "formula_version": "1",
  "amortization_included": true
}
```

The SPA is **read-only and unauthenticated**; response is cached at 30 s on the server. The endpoint refuses to serve data if SHELF-40 has not been initialised with an amortisation value (returns HTTP 503 with a body explaining the operator must set it). UI shows a top-line "$ saved" big-number, a sparkline of the last 30 days, and a sortable per-table table with the top 50 rows. Visual style matches the existing `/ui` admin SPA but without action buttons. Build pipeline runs in CI under `shelfd/web/savings/package.json` with `pnpm build` → static assets baked into the binary at compile time.

## Acceptance criteria

- [ ] `curl http://shelfd:9090/savings` returns a 200 HTML shell; assets load without console errors in Firefox + Chrome current-stable.
- [ ] `curl http://shelfd:9090/v1/savings.json` returns a JSON document matching the schema above; schema is committed at `shelfd/web/savings/schema.json` and validated in CI.
- [ ] Without SHELF-40 amortisation set, both routes return 503 with a Retry-After header and an explanatory body.
- [ ] SPA renders correctly at viewport widths 360 px (mobile) and 1440 px (desktop).
- [ ] No admin actions are wired (no `pin`, `evict`, `reload` calls reachable from the SPA).
- [ ] First-paint latency: ≤ 200 ms for the HTML shell, ≤ 500 ms for the data fetch on a warm process.
- [ ] Lighthouse performance score ≥ 90 on a desktop emulation against a 1 K-row fixture.

## Out of scope

- Authentication / authorisation (the tile is intentionally public).
- Editing pricing constants from the UI.
- Historic backfill UI ("show me last quarter") beyond the 30-day sparkline.
- Mobile native app.
- i18n; English-only in v1.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Public exposure leaks tenant identifiers via `per_table` row names | Default tenant masking on; operators opt in to unmasked per-table rows via `config.savings.public.unmasked=true`. |
| Mis-cited number gets quoted in a procurement deck | SPA shows `formula_version` + `amortization_included: true/false` prominently above the totals. |
| SPA bundle bloats `shelfd` binary | Bundle gzipped < 200 KiB enforced by a CI check; reuse the existing `/ui` chart library if any. |
| Cross-site embedding (`<iframe>`) | Set `X-Frame-Options: SAMEORIGIN` by default; opt-in `Content-Security-Policy: frame-ancestors *` for those who want to embed. |

## Test plan

- Unit tests: Rust handlers (`/savings`, `/v1/savings.json`) for 503-without-amortisation, 200-with-amortisation, schema-shape parity with the committed JSON schema.
- Integration tests: `shelfd/tests/it_savings_spa.rs` boots the binary with a fake SHELF-40 source and curls both routes.
- Frontend tests: Playwright smoke under `shelfd/web/savings/tests/` asserts the totals render and the per-table grid is sortable.
- (If applicable) docker compose smoke: SHELF-12 + listener + savings; assert curl returns 200 and the SPA renders a non-zero $-saved.

## Open questions

- Should the SPA be served from `:9090` (admin port) or `:9092` (S3 shim port)? Recommend the admin port; the shim port is for S3 traffic only.
- Default unmasked-vs-masked: lean toward masked. Confirm with the OSS-launch reviewer.
- Where does the per-window selection live (24 h / 7 d / 30 d)? Recommend a query param `?window=7d`, default 7 d.
