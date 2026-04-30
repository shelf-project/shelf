# Shelf Grafana dashboards

This directory ships the Shelf project's Grafana dashboards as raw JSON
exports. Operators install them into their own Grafana — the shelfd
binary does **not** ship a Grafana, and the Helm chart's
`grafana-dashboard.yaml` template only emits the JSON as a `ConfigMap`
for sidecar-discovery setups (the kube-prometheus-stack convention).

## Inventory

| File | UID | Purpose | Status |
|---|---|---|---|
| `shelf-overview-v2.json` | `shelf-overview-v2` | rc.5 drain-wave overview, 15 actionable panels in 4 rows (traffic-light health, per-pool + cost, culprit tables, drain-wave feature observability). Replaces the legacy 48-panel `shelf-overview` once validated. | **Pending UI import** (rc.6 P0.5) |
| `shelf-read-path.json` | `shelf-read-path` | Per-request read-path distribution (hit/miss/peer/origin), Foyer pool stats, S3 shim throughput. | Stable |
| `shelf-mv-acceleration.json` | `shelf-mv-acceleration` | MV-aware pinning (SHELF-65) hit-ratio lift on materialized views. | Stable |
| `shelf-trainer.json` | `shelf-trainer` | Phase-4 LightGBM admission trainer (off in v1; ADR-0003). | Stable, inert in v1 |
| `shelf-tenant.json` | `shelf-tenant` | Per-tenant (A/B tag) accounting (SHELF-42). | Stable |

## Import — the manual UI path (default for cluster operators)

The dashboard JSON is a Grafana export, **not** a Kubernetes manifest.
Use Grafana's UI for the one-off cluster install:

1. Open Grafana, log in with a user that has the **Editor** role on
   the target folder.
2. Navigate to **Dashboards → Import** (or `+ → Import dashboard`).
3. Click **Upload JSON file** and pick the JSON from this directory.
4. On the next screen, set:
   - **Folder** — the team's dashboard folder. Pick the folder your
     Grafana service-account token is scoped to (Grafana 11+ blocks
     imports into the root "General" folder; see gotcha below).
   - **Datasource** — the Prometheus datasource you scrape Shelf
     metrics from. The dashboards default to a UID named
     `mimir-data`; other clusters override the datasource at
     import time.
5. Click **Import**.

The dashboard JSON's `id` field is `null`, so Grafana assigns a new
internal id on import; the **UID is preserved** from the JSON. The
UID is what permalinks (`/d/<uid>/...`) resolve, so it is stable
across imports.

## Import — the API path (for automation)

If you want to install dashboards from CI or from a `make` target,
use Grafana's HTTP API with a write-scoped service-account token.

```bash
GRAFANA="https://your-grafana.example.com"
TOKEN="<service-account-token-with-Editor-on-target-folder>"
FOLDER_UID="<your-folder-uid>"   # required on Grafana 11+

curl -fsS \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  "${GRAFANA}/api/dashboards/db" \
  --data @<(jq \
    --arg fuid "${FOLDER_UID}" \
    '{dashboard: . | (.id = null), folderUid: $fuid, overwrite: true, message: "rc.6 dashboard import"}' \
    observability/dashboards/shelf-overview-v2.json)
```

A few gotchas worth knowing:

- **Read-only tokens cannot import.** Grafana service accounts on
  the **Viewer** role hit `403` on `POST /api/dashboards/db`. The
  token MUST be from a service account on **Editor** (folder-scoped)
  or higher. Minting that token is an org-admin action — out of
  scope for this PR.
- **Default folder ("General") is locked on Grafana 11+.** The
  Dashboard App Platform (`apis/dashboard.grafana.app/v1beta1/...`)
  returns `403` for dashboards saved at the root "General" folder
  even to the creator. Always pass an explicit `folderUid`.
- **The `id` field must be `null`** on import for Grafana to mint a
  fresh internal id. The exports in this directory are already
  scrubbed; re-exports from the UI need a manual `jq '.id = null'`.
- **`overwrite: true`** lets a re-import update an existing
  dashboard at the same UID. Without it, Grafana refuses with
  `412 Precondition Failed`.

## When to bump the version field

Each dashboard JSON has a `"version": <int>` set by Grafana on save.
PR-time edits should leave `version` as it was on export — Grafana
increments it on the next save in the UI. Bumping it manually
serves no purpose and just creates merge churn.

## Datasource UIDs are environment-specific

The dashboards reference datasources by UID. The default UIDs
shipped here (`mimir-data` / `loki-data` / etc.) are the canonical
project defaults; clusters running differently named datasources
remap them at import time and the JSON does not need to be edited.

If you want to ship the JSON pre-mapped for your cluster, run
through `jq` first:

```bash
jq '(.. | objects | select(.datasource?)) |= (.datasource.uid = "your-prom-uid")' \
   observability/dashboards/shelf-overview-v2.json > /tmp/remapped.json
```

## Roadmap

- **rc.6 P0.5** (this PR): land `shelf-overview-v2.json` in the
  repo + document the import path. Manual UI import is the
  expected operator action.
- **post-rc.6**: chart-level option to materialize dashboards as
  ConfigMaps with the
  `grafana_dashboard: "1"` sidecar label. The chart already has
  `templates/grafana-dashboard.yaml` in skeleton form; lighting it
  up is gated on shipping a stable JSON set first.
