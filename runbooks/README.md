# Shelf runbooks

One file per alert + one file per operational scenario. Every runbook
follows the agent-8 Pass-3 template: **Symptom / Impact / Diagnosis /
Mitigation / Escalation / Post-incident**.

## Alert runbooks

| Alert                     | Runbook                             |
| ------------------------- | ----------------------------------- |
| `ShelfHitRateTooLow`      | `shelf-hit-rate-too-low.md`         |
| `ShelfFallThroughSurge`   | `shelf-fall-through-surge.md`       |
| `ShelfNvmeUsageHigh`      | `shelf-nvme-usage-high.md`          |
| `ShelfPodRestarting`      | `shelf-pod-restarting.md`           |
| `ShelfAdmissionModelStale`| `shelf-admission-model-stale.md`    |
| `ShelfCircuitBreakerOpen` | `circuit-breaker-open.md`           |

## Operational runbooks

- `scale-up.md` — add a shelfd pod and watch HRW rebalance
- `scale-down.md` — safely drain a pod
- `pin-table.md` — add a table to the pin list
- `unpin-table.md` — remove a table from the pin list
- `rollback-admission-model.md` — revert to the previous admission model
- `evict-poisoned-key.md` — evict a single bad key, force re-fetch
- `regional-outage.md` — S3 in our region is impaired

## Conventions

- Each runbook lists its alert in the `Alert:` header line (if applicable).
- The "Diagnosis" section has **exactly three** commands, copy-pasteable.
- The "Mitigation" section has **exactly three** progressively-safer actions.
- Escalation follows `docs/oncall.md`.

### Operator URL placeholders

Runbooks that reference Grafana, Airflow, or other operator surfaces use
shell-style placeholders rather than literal hostnames so they're
copy-paste safe across deployments. Set these once in your shell or
runbook environment:

| Placeholder              | Example                              | Used in                                                 |
| ------------------------ | ------------------------------------ | ------------------------------------------------------- |
| `${SHELF_DASHBOARD_BASE}`| `https://grafana.example.com`        | "Dashboard:" header, Grafana deep-links                 |
| `${SHELF_AIRFLOW_BASE}`  | `https://airflow.example.com`        | DAG status / trigger URLs in trainer-related runbooks   |
| `${SHELF_CONFIG_BUCKET}` | `your-shelf-prod-config`             | S3 bucket holding pin list + admission model artefacts  |

## Quick command cheat-sheet

```bash
# Shelf StatefulSet status
kubectl -n shelf get sts shelf

# Live pod stats (round-trips every pod)
kubectl -n shelf exec shelf-0 -- shelfctl stats

# HRW ring view
kubectl -n shelf exec shelf-0 -- shelfctl ring

# Trigger a pin-list reload (no restart)
kubectl -n shelf exec shelf-0 -- shelfctl reload pin-list

# Evict one key
kubectl -n shelf exec shelf-0 -- shelfctl evict <key>
```
