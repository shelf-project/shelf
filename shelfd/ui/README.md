# shelfd UI

Embedded admin UI for the `shelfd` cache daemon. A Vite + React + TypeScript
single-page app that lives inside the Rust binary under the `ui` cargo
feature and is served same-origin at `/ui`.

## What it does

Three tabs on top of the existing `shelfctl` HTTP contract:

- **Ops** — cumulative hit rate, p95 `/cache` latency, origin fallback rate,
  per-pool capacity bars (DRAM for `metadata`, DRAM + NVMe for `rowgroup`),
  and an SLO traffic light. Polls `/stats` and `/metrics` every 5 s.
- **Admin** — HRW ring table, pin/unpin/evict form (with confirm dialog),
  pin-list reload button. Every action maps 1:1 to the same endpoint
  `shelfctl` uses.
- **Showcase** — the "what this cache is" page, with a live cumulative
  `shelf_hits_total` counter and a tiny sparkline of recent deltas.

## Develop

```bash
# From the repo root:
make ui-install          # pnpm install in shelfd/ui/
make smoke-up            # bring up shelfd on :9091 without the UI baked in
cd shelfd/ui && pnpm dev # Vite dev server on :5173 proxies to :9091
```

Override the proxy target if `shelfd` runs elsewhere:

```bash
SHELFD_ORIGIN=http://127.0.0.1:9090 pnpm --dir shelfd/ui dev
```

## Ship

```bash
# Build the SPA and rebuild shelfd with the bundle embedded:
make ui

# Or via the smoke harness (UI at http://127.0.0.1:9091/ui):
make smoke-up-ui
```

Inside Docker, pass `--build-arg SHELFD_FEATURES=ui` to either
`shelfd/Dockerfile` or `benchmarks/smoke/Dockerfile.shelfd`.

## Bundle size

Target: ≤ 60 KB gzipped JS. At scaffold time we're at ~53 KB. No
charting library, no Prom client — a ~120-line text-format parser
extracts the handful of series the Ops tab renders.

## Not doing

- Auth. Admin endpoints stay protected exactly the way they already
  are (network / reverse proxy). Fetch wrappers will grow
  `Authorization` headers when ingress auth lands.
- Charts. Grafana (`shelf-read-path`, SHELF-27) remains the observability
  surface; this UI is an operator console, not a dashboard.
- New API contract. If `shelfctl` and the browser disagree we've
  got a bigger problem than a flaky tab.
