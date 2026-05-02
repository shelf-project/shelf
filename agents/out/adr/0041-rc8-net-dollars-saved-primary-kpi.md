# ADR-0041 — rc.8 K3: net dollars saved is the primary KPI on shelf-overview-v2

**Status**: Accepted (rc.8)
**Date**: 2026-05-02
**Supersedes**: none (extends ADR-0028)
**Tickets**: rc.8 K3 (`shelf_rc.8_roadmap_beb7f350.plan.md`)

## Context

A4 (PR #97, ADR-0028) shipped two metrics that, between them, finally make
shelf's procurement story auditable end-to-end:

- `shelf_s3_dollars_saved_net_total{region}` — counter, integer **dollar-
  micros**. Cumulative *net* dollars saved (gross S3 + data-transfer
  savings on `shelf_s3_dollars_saved_total` minus amortized shelf-pool
  cost). Anti-overclaim guard: only credited when the operator has set
  `cache.cost.amortizedDollarsPerHour` to a positive, finite number.
- `shelf_pool_amortized_dollars_per_hour` — gauge, integer dollar-micros.
  Reports the active amortization configuration; `0` means unset and the
  net counter will refuse to publish.

`shelf-overview-v2` to date has led with **hit ratio (5m)** in row 1
panel 1. That answers an operator question (am I caching anything?) but
does **not** answer the procurement question that workspace memory and
the bench narrative both keep flagging as the missing primary KPI:

> *Is shelf paying for itself?*

The bench follow-up plan (`shelf_benchmark_design_*.plan.md`) and the
v1.0.0 GA postmortem both call this out: every conversation with a
non-engineer stakeholder loops back to "what does that hit ratio mean in
dollars?" and we walk them through the same A4 metric arithmetic by hand.
The dashboard has the underlying counter (panel id 8 — `$ saved
cumulative`) but it is buried in row 2 alongside per-pool hit ratio and
peer-fetch outcomes, and it shows the **gross** counter
(`shelf_s3_dollars_saved_total`), not the **net** A4 counter that already
subtracts amortized pool cost.

## Decision

Promote A4's net counter + amortized-cost gauge to **row 1** of
`shelf-overview-v2.json` as the new top-of-dashboard band. The existing
six health-stats become row 2.

Row 1 is exactly five panels (panel ids `21..25`, plus row header
`50`):

| # | Panel                              | Metric / expression                                                                                                                | Unit          | Thresholds                                            |
|---|------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|---------------|-------------------------------------------------------|
| 1 | Net $ saved (24h)                  | `sum(increase(max without(prometheus, dataPrometheusReplica) (shelf_s3_dollars_saved_net_total{namespace="alluxio", pod=~"$pod"})[24h:1m])) / 1e6` | `currencyUSD` | red < 0 / amber 0–100 / green ≥ 100                   |
| 2 | Net $ saved (7d)                   | same, `[7d:5m]`                                                                                                                    | `currencyUSD` | red < 0 / amber 0–700 / green ≥ 700                   |
| 3 | Net $ saved (30d)                  | same, `[30d:30m]`                                                                                                                  | `currencyUSD` | red < 0 / amber 0–3000 / green ≥ 3000                 |
| 4 | Amortized pool $/hr                | `sum(max without(prometheus, dataPrometheusReplica) (shelf_pool_amortized_dollars_per_hour{namespace="alluxio", pod=~"$pod"})) / 1e6` | `currencyUSD` | amber at unset (= $0) / green > 0                     |
| 5 | Cumulative net $ saved trend       | `sum(max without(prometheus, dataPrometheusReplica) (shelf_s3_dollars_saved_net_total{namespace="alluxio", pod=~"$pod"})) / 1e6`   | `currencyUSD` | line chart (no thresholds — slope is the signal)      |

The new row inherits the dashboard-wide `$pod` template variable so
operators can scope the savings number to a single replica's shelf-pool
when they need to. The pre-existing `$ab_tag` template variable
(introduced in D2 / PR #91) is retained at the top of the templating
list; row 1 does not consume it, but row 6 still does.

The dashboard `uid` (`shelf-overview-v2`), `title`, and the `mimir-data`
datasource UID (`ddy2eykq2tfy8a`) are unchanged. Operators have both the
URL and the bookmark; we do not want to break either.

The dashboard `version` is bumped 2 → 3 to record the schema change.
Total panel count goes 20 → 25 (in 6 rows instead of 5).

## Consequences

**Positive**

- **Procurement gets a one-glance answer** to "is shelf paying for
  itself?" at three time horizons (24h cohort review, 7d weekly cohort,
  30d headline / monthly review). The 30d panel is the one that goes
  into cluster-cost decks.
- **Cumulative regression becomes visible immediately**. If A4's
  `tick()` arithmetic ever inverts (gross < amortized for a sustained
  window) the cumulative trend bends down and the 24h stat goes red
  before any operator has to read the metric directly.
- **Anti-overclaim guard is loud, not silent**. The amortized-cost
  panel reads exactly $0/hr (amber) until `cache.cost.amortizedDollars
  PerHour` is configured, and every other panel in row 1 will read $0
  while it does. An operator cannot accidentally screenshot a green
  procurement KPI from a misconfigured cluster — the amber amortized
  panel is the single point of truth that the configuration is wired.
- **Hit ratio is still the second thing you see**. Row 2 panel 1 is the
  same `Hit ratio (5m)` stat operators already reach for — moved one
  scroll-tick down, same gridPos.x, same colour scheme, same target
  thresholds. No operational muscle-memory cost.

**Negative**

- The first time an operator opens the dashboard after the rollout, all
  five row-1 panels will read `0` until either (a) they configure
  amortization, or (b) the existing 24h/7d/30d windows accumulate
  observations against the new metric. We accept the cosmetic confusion
  in exchange for the auditability the metric gives.
- Dashboard schema version bumps from 2 → 3. Anyone with a forked copy
  of v2 needs to pull this change before sharing screenshots; the row
  numbering in old screenshots no longer matches.

## Alternatives considered

1. **Grafana annotations for procurement events.**
   Too low-density. An annotation marks a moment in time but does not
   answer "are we positive right now?" without arithmetic. Rejected.

2. **Separate "Procurement" dashboard.**
   Adds operator navigation overhead — they would need to flip between
   `shelf-overview-v2` (operational) and a procurement dashboard
   (financial), and the two views would diverge on filters, time-range,
   and pod scope. The cluster cost question is *the same question* as
   the operational health question; they belong on the same surface.
   Rejected.

3. **Promote `$ saved cumulative` (panel 8, gross) without adding A4
   net.** The gross counter does not subtract amortized cost, so it
   over-claims by exactly the pool's hourly run-rate. That is precisely
   the failure mode A4 was built to prevent. Rejected.

## References

- A4 PR #97 — net dollars-saved counter + amortized-cost gauge
- ADR-0028 — `agents/out/adr/0028-rc7-net-dollars-saved.md`
- D2 PR #91 — A/B tag attribution row + `$ab_tag` template variable
- Workspace memory: bench narrative and v1.0.0 GA postmortem on the
  procurement-justification gap
- `shelf_rc.8_roadmap_beb7f350.plan.md` — K3
