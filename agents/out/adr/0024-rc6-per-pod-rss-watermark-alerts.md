# ADR 0024: Per-pod shelfd RSS watermark alerts

_Status: Accepted (2026-04-30)_
_Deciders: shelfd-maintainers, shelf-sre_
_Supersedes: none_
_Superseded-by: none_

## Context

The recurring failure mode for shelfd in production has been the
**OOMKill from foyer / origin-pool / submit-queue inflation under
sustained writes** (SHELF-21e, SHELF-29, SHELF-30 traces all carry
this signature). Every such event is preceded by a measurable RSS
ramp on the affected pod: the kernel OOM-killer fires when
`container_memory_working_set_bytes` crosses the node's allocatable
ceiling, which on the deployment's node pool is **~27 GiB**
(node families like `m6a/m5a/m7a/c6a 4xlarge` expose ~27.3 GiB
allocatable to pods).

Existing alerts catch the **post-mortem** signal:

- `ShelfPodRestarting` fires when `kube_pod_container_status_restarts_total`
  registers ≥ 3 restarts in 15 m. This is too late — the OOMKill
  has already happened.
- `ShelfNvmeUsageHigh` fires on disk pressure, which is a *different*
  failure mode (Foyer admission refusal at 90 % NVMe).

We have **no pre-OOM signal** today. The Phase D auto-rollback
watcher used during cutover windows hard-codes a `pod RSS > 24 GiB
sustained 5m OR any OOMKill` trigger, but that lives outside the
PrometheusRule bundle and only runs during a cutover window — there
is no continuous on-call gate.

## Decision

Add **two watermark alerts** to the existing `shelf.health` rule
group:

| Alert | Threshold | `for:` | Severity | Operator action |
|---|---|---|---|---|
| `ShelfPodRSSWarn` | RSS > 24 GiB | 5m | warning | Investigate before adding more replica traffic. |
| `ShelfPodRSSCritical` | RSS > 25.5 GiB | 2m | critical | Scale shelf-pool +2 OR rolling-restart this pod immediately. |

Both alerts use the `max without(prometheus, dataPrometheusReplica)`
pattern so HA-Prometheus pairs (`mimir-prod-0`/`-1`,
`mimir-data-0`/`-1`) do not double-count the same series — workspace
memory `/Users/aamir/trino/AGENTS.md#observability-gotchas` codifies
this requirement after the 2026-04-24 "unhealthy pods = 4 vs actual
2" incident.

The metric is `container_memory_working_set_bytes`, which is
exposed by cAdvisor (kubelet) via kube-state-metrics — already
scraped on every Prometheus we run, so no new exporter is required.

The expression converts bytes to GiB inline (`/ (1024 * 1024 *
1024)`) so the threshold values in the rule stay human-readable
(`> 24` and `> 25.5`) and grep-friendly.

## Why two thresholds and not one

A single threshold either fires too late (only `25.5 GiB / 2m` —
lose the investigate signal) or causes alert fatigue (only
`24 GiB / 5m` paging at every traffic spike). The two-watermark
pattern matches how on-call already operates: the warning is a
**look** signal, the critical is an **act** signal. The two-minute
critical window is short enough to act before the kernel does
(observed pre-OOM ramps cross 24 → 27 GiB in 8-15 minutes).

## Why `namespace="shelf"` and not the deployment namespace

The OSS rule bundle uses `namespace="shelf"` (the chart's default
release namespace, matching `ShelfPodRestarting` immediately above
the new alerts). Operators running shelf in a different namespace
(e.g. side-by-side with another component in a shared namespace
like `alluxio`) override via Helm overlay or by editing the rule
bundle in their fork. Templating namespace through Helm is
preferable but is a larger refactor — out of scope for P1.4.

## Why no PrometheusRule template (yet)

The same templating refactor that would let us parameterize
`namespace` would also give us templated `runbook_url` and
`dashboard_url` placeholders. Both improvements together are a
clean follow-up; bundling them with P1.4 risks regressing the
existing alerts. Tracked as a docs-only follow-up.

## Validation

- `promtool check rules` (Prometheus official image, ran in CI as
  part of this PR's local validation) — `SUCCESS: 8 rules found`.
- YAML structure check via Python: every rule carries
  `summary`, `description`, `runbook_url`, `dashboard_url`
  annotations + `severity`, `service` labels. Parens / braces
  balanced.
- Dashboard URL points at `shelf-overview-v2`, the dashboard
  shipped under rc.6 P0.5 (PR #76). Operators following the link
  while the dashboard is pre-import will see Grafana's "dashboard
  not found" — the runbook covers the manual import step.

## Consequences

- **Positive**: pre-OOM signal lands in the standard PrometheusRule
  bundle; on-call gets the same auto-rollback signal that the
  Phase D watcher already trips on, without the cutover window
  scoping. Reduces the OOMKill → DNS-race → query-fail blast radius
  by ~5 minutes (the difference between "investigate at 24 GiB" and
  "react to a 137 exit code").
- **Negative**: two new pages possible per pod-day under stress.
  Acceptable — the existing `ShelfPodRestarting` already pages on
  any restart, so the volume floor is unchanged in steady-state.
  The `_RSSCritical` is the new paging surface; if it pages without
  the warn having pre-fired, that is itself a useful signal (the
  pod ramped through 1.5 GiB faster than 5 m).

## References

- `observability/alerts/shelf-prometheus-rules.yaml#shelf.health`
  — the rule group these alerts join.
- `runbooks/shelf-pod-rss-watermark.md` — to be authored as a
  follow-up; the alert annotation already references the URL so it
  resolves the day the runbook lands.
- Workspace memory `/Users/aamir/trino/AGENTS.md` — node-pool
  allocatable ceiling, Phase D auto-rollback trigger schema, HA
  Prometheus de-dup pattern.
