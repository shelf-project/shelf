# SHELF-27 — Insight-first Grafana dashboard + read-path alerts

_Ticket:_ SHELF-27 (Grafana dashboard, insight-first).
_Depends on:_ SHELF-08 (Prometheus `/metrics`, OTel), SHELF-21 (chart
production readiness).
_Superseded artefact:_ `observability/dashboards/shelf-read-path.json`
(SHELF-08 starter, retained for backward-compat).

## Goal

Turn the three-panel SHELF-08 starter into a dashboard an on-call can
diagnose in ≤ 3 clicks (AGENTS.md rubric). The "is Shelf healthy right
now?" answer must live in the first viewport and render as **big
numbers, not time-series**.

## Layout

```
┌──────────────┬──────────────┬──────────────┬──────────────┐
│ Hit Ratio    │ P99 Latency  │ Miss Volume  │ Error Rate   │   ← stat row
│  overall +   │   5m         │   1m         │   5m         │     (big numbers)
│  per-pool    │              │              │              │
├──────────────┴──────────────┴──────────────┴──────────────┤
│ Hit Ratio    │ p50/95/99    │ HEAD hit /    │             │   ← drill row
│ by pool      │ by route     │ miss by pool  │             │
├──────────────┴──────────────┴──────────────┴──────────────┤
│ Origin call-rate (success vs failure)      │ Pinned Bytes │   ← origin / pin row
│                                             ├──────────────┤
│                                             │ Single-flight│
│                                             │ coalesced    │
└─────────────────────────────────────────────┴──────────────┘
```

All panels share a single datasource variable `${DS_PROMETHEUS}` of
type `datasource` (Grafana schemaVersion 39). The dashboard UID is
`shelf-read-path` — unchanged from the starter so existing folder
permissions and deep-links keep working.

## Big-number thresholds (panel traffic lights)

| Panel       | Green         | Yellow       | Red          | Source |
|-------------|---------------|--------------|--------------|--------|
| Hit Ratio   | ≥ 0.80        | 0.50–0.80    | < 0.50       | v0.5 gate = 0.71; yellow gives on-call a 10-point cushion before the informational alert fires at 0.40. |
| P99 Latency | < 50 ms       | 50–100 ms    | ≥ 100 ms     | Matches `ShelfReadPathP99Degraded` paging threshold. |
| Miss Volume | < 500 req/s   | 500–2000 r/s | ≥ 2000 r/s   | Placeholder pending Phase-0 benchmark (SHELF-18). |
| Error Rate  | < 0.5 %       | 0.5–1 %      | ≥ 1 %        | Matches `ShelfReadPathHighErrorRate` paging threshold. |

## Alerting rules

Committed at `charts/shelf/grafana/alerts/shelf-read-path.yml`.

| Alert                          | Severity | Window | Expression (abridged)                                                                                    |
|--------------------------------|----------|--------|----------------------------------------------------------------------------------------------------------|
| `ShelfReadPathHighErrorRate`   | page     | 10m    | `rate(shelf_request_seconds_count{status=~"5.."}[10m]) / rate(shelf_request_seconds_count[10m]) > 0.01`  |
| `ShelfReadPathP99Degraded`     | warn     | 10m    | `histogram_quantile(0.99, rate(shelf_request_seconds_bucket[10m])) > 0.1`                                |
| `ShelfReadPathHitRatioCollapsed` | info   | 30m    | `rate(hits[30m]) / (rate(hits[30m]) + rate(misses[30m])) < 0.4`                                          |

The `info`-severity hit-ratio alert is informational on purpose —
rotation and warm-up events legitimately trip it and we don't want to
page at 03:00 for a pod restart.

## Metric-label assumptions (the gap list)

The SHELF-27 spec asked for expressions that reference labels that are
**not yet wired** on `shelfd`'s Prometheus surface. We committed the
spec's literal expressions so the contract is visible, and we use panel
descriptions + this doc to call out the gap explicitly:

| Assumption in SHELF-27 spec                                        | Reality in `shelfd/src/metrics.rs` (2026-04-24)                                                             | Resolution                                                                                         |
|--------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `shelf_request_seconds{status=~"5.."}`                              | Histogram labels are `{path, outcome}`. `outcome` ∈ `{hit, miss, bad_request, not_found, error, ok}`. No `status` label. | Panel renders "No data" until a `status` label (or a dedicated `shelf_http_requests_total{status}`) lands. Alert evaluates to NaN and cannot page. **Follow-up:** add `status` label (or counter) in SHELF-08 obs-pass-2 / SHELF-05 obs-pass. |
| `shelf_request_seconds{route=~".*"}`                                | Dimension is emitted as `path`, not `route`.                                                                | Dashboard uses `path` directly (semantic identical) and documents the rename in the panel description. |
| `shelf_pinned_bytes`                                                | Listed as *Planned* in `shelfd/docs/metrics.md`; owning ticket SHELF-24.                                    | Panel shipped as a placeholder (`noValue` copy explains the gap). Wire when SHELF-24 emits.         |
| `shelf_origin_requests_total{verb,status}`                          | Planned under SHELF-05 obs-pass; not yet emitted.                                                           | Panel proxies "success" with `rate(shelf_misses_total)` (misses *are* the calls that reach S3) and "failure" with `shelfd_error_total{component="origin"}`. Rewire when real counter lands. |
| `shelf_singleflight_followers_total`                                | SHELF-08 emits a `shelfd.singleflight{role="follower"}` **trace event**, not a counter.                     | Panel shipped as a placeholder. Wire when a Prom counter is added (likely SHELF-06 follow-up).      |

None of these gaps block the panel from rendering — the dashboard
degrades gracefully to "No data" / `noValue` copy and every gap is
cross-referenced here so the owning ticket has a single source of
truth.

## Distribution

The chart ships both the dashboard JSON and the alert YAML as
ConfigMaps picked up by the kube-prometheus-stack Grafana sidecar:

- `charts/shelf/templates/grafana-dashboard.yaml` renders two
  ConfigMaps (`*-grafana-dashboard`, `*-grafana-alerts`) using
  `.Files.Get` over `charts/shelf/grafana/**`. Editing the JSON/YAML
  is a pure content change.
- `charts/shelf/values.yaml` exposes `grafana.enabled` (default
  `true`) plus `grafana.dashboardLabel` / `grafana.alertLabel` so
  operators running a non-default sidecar selector can rewire without
  forking the chart.

The canonical dashboard path is now
`charts/shelf/grafana/dashboards/shelf-read-path.json`. The SHELF-08
starter at `observability/dashboards/shelf-read-path.json` is retained
with an in-body `description` pointer to avoid breaking existing
bookmarks.

## Validation

- `python3 -c "import json; json.load(open('charts/shelf/grafana/dashboards/shelf-read-path.json'))"` — parses cleanly.
- `python3 -c "import yaml; yaml.safe_load(open('charts/shelf/grafana/alerts/shelf-read-path.yml'))"` — parses cleanly.
- `helm lint charts/shelf` and `helm lint charts/shelf -f charts/shelf/ci/lint-values.yaml --strict` — both clean.
- `helm template charts/shelf --kube-version 1.28.0 | grep grafana_dashboard` — emits the `grafana_dashboard: "1"` label on the dashboard ConfigMap.

## Out of scope

- `shelf-overview` / `shelf-tenant` / `shelf-trainer` dashboards — SHELF-27 is read-path only; the overview dashboard rollup is a follow-up.
- A `PrometheusRule` CRD emission (we ship plain rule YAML in a ConfigMap so clusters without `monitoring.coreos.com` can still consume the alerts).
- Runbook content — owned by SHELF-28 (the `runbook` annotation points at the SHELF-28 URL).
