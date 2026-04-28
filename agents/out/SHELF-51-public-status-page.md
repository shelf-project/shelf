# SHELF-51: Public status page (Tailscale-style)

**Status:** Draft
**Tier:** A
**Estimated effort:** S
**Depends on:** none
**Blocks:** none

> **Sequencing note: do not start before SHELF-23 lands.** This ticket adds a new public route to `shelfd/src/http.rs`, which SHELF-23 currently has in-flight on `shelf-23-peer-fetch`. Resume after SHELF-23 merges.

## Problem (OSS-cited)

Internal Grafana is gated; nobody outside the team can see Shelf is up. Adopters want a 1-URL "is your cache healthy?" link to put in postmortems. [Tailscale's share-link pattern](https://tailscale.com/blog/share-data-securely-with-share-link/) is the comparable; Cloudflare's [public status page](https://www.cloudflarestatus.com/) is another. No OSS analytical-cache today ships a public, signed, read-only status SPA.

## Goal

A static SPA at `https://shelf.<org>/` serves a Tailscale-style health overview from the same `/stats` JSON, signed read-only, with no admin actions, suitable for embedding in public postmortems and uptime-tracker pages.

## Approach

Single-page React app committed under `shelfd/web/status/`, bundled into the `shelfd` binary via `include_dir!` (same pattern as `shelfd/src/ui.rs` and SHELF-41's `/savings`). New route `GET /` (root) in `shelfd/src/http.rs` serves the SPA shell; data comes from a new public read-only endpoint `GET /v1/status.json` that aggregates a stable subset of `/stats`:

```json
{
  "as_of": "2026-04-28T17:00:00Z",
  "version": "1.0.0",
  "uptime_seconds": 86400,
  "pods": [
    { "id": "shelf-0", "healthy": true, "uptime_seconds": 86400 }
  ],
  "pools": {
    "metadata": { "hit_ratio_24h": 0.95, "p99_latency_ms": 4 },
    "rowgroup": { "hit_ratio_24h": 0.78, "p99_latency_ms": 38 }
  },
  "incidents_open": 0,
  "incidents_last_30d": 1
}
```

The `/v1/status.json` endpoint is **read-only and unauthenticated**. Response is 30 s server-cached. Optional HMAC signing (`config.status.public.hmac_secret`) attaches an `X-Shelf-Sig` header so embedders can verify integrity. The SPA is anonymised — pod names map to opaque indices; no IP addresses; no tenant labels.

UI design borrows from Tailscale's traffic-light layout: a single big-number "operational" / "degraded" / "outage" status driven by the SHELF-27 alert rules' current firing state, with rolled-up pool tiles below. No drill-down beyond pool-level. No login.

Layout cross-references:
- `shelfd/src/http.rs` — new `GET /` and `GET /v1/status.json` routes.
- `shelfd/web/status/` — Vite+React SPA, bundled at compile time.
- `shelfd/web/status/schema.json` — JSON-schema for `/v1/status.json`, validated in CI.
- `charts/shelf/values.yaml` — `status.public.enabled` gate, default false (operators opt in).

## Acceptance criteria

- [ ] `curl http://shelfd:9090/` returns a 200 HTML shell; assets load without console errors in current-stable Firefox + Chrome.
- [ ] `curl http://shelfd:9090/v1/status.json` returns a JSON document matching the committed schema.
- [ ] No admin actions are reachable from the SPA (no `pin`, `evict`, `reload`, `ring` calls).
- [ ] No tenant identifiers, IPs, or pod names leak — pod IDs are opaque indices.
- [ ] When `status.public.enabled=false`, both routes return 404 (the public surface is opt-in).
- [ ] First-paint latency: ≤ 200 ms HTML shell, ≤ 500 ms data fetch on a warm process.
- [ ] Bundled SPA gzipped < 200 KiB enforced by CI.
- [ ] Lighthouse performance ≥ 90 on a desktop emulation against a 3-pool fixture.

## Out of scope

- Authentication / authorisation (the page is intentionally public).
- Drill-down beyond pool-level.
- Historical incident timeline UI (text count only in v1).
- Cross-cluster / multi-tenant aggregation.
- Mobile native app.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Information disclosure — SPA leaks internal topology | Anonymise pod identifiers; redact tenant labels; default-off (`status.public.enabled=false`). |
| Stale "operational" badge during incidents | `incidents_open` counter sources from SHELF-27 alert rules; updated on each scrape. |
| Cross-site embedding misuse | `Content-Security-Policy: frame-ancestors 'self'` by default; opt-in `*` for embedders. |
| HMAC secret leakage | Secret stored only in env / Helm secret; never echoed in logs; rotation runbook in `docs/runbook.md`. |

## Test plan

- Unit tests: handler returns 200 / 404 based on `status.public.enabled`; JSON schema match; opaque-pod-id rendering; HMAC signing path.
- Integration tests: `shelfd/tests/it_status_page.rs` boots the binary with a fixture state, curls both routes, validates schema.
- Frontend tests: Playwright smoke under `shelfd/web/status/tests/` asserts traffic-light renders correctly across `operational / degraded / outage`.
- (If applicable) docker compose smoke: SHELF-12 + status page on; assert curl returns 200 and the SPA renders an "operational" badge.

## Open questions

- Should the page also expose the SHELF-41 dollars-saved tile inline, or stay a separate `/savings` route? Recommend separate — different audiences (procurement vs ops).
- Should the SPA include a "subscribe to incidents" button (Atom feed / webhook)? v1: no; post-v1.
- HMAC default off vs on? Recommend off in v1 (most adopters won't want signed embeds); document the upgrade path.
