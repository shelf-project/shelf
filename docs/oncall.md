# Shelf on-call

The operator contract for being on call for Shelf.

## Page policy

### What pages (immediate, 24/7)

- Any of: `ShelfHitRateTooLow`, `ShelfFallThroughSurge`,
  `ShelfPodRestarting`, `ShelfCircuitBreakerOpen`.
- Any incident that surfaces as Trino `QueryFailedEvent` with a
  stack-trace containing `ShelfFileSystem` or `ShelfPlugin` (these
  are treated as paging because per BLUEPRINT §9.5's invariant, the
  plugin MUST NOT surface a Shelf-specific error; if it does, we have
  a v1 bug).
- v0.5 gate window: any single minute of `< 60%` hit rate is a page
  (manual), because the gate is a 7-day rolling commitment.

### What does not page (Slack only)

- `ShelfNvmeUsageHigh` — warning, action within the shift.
- `ShelfAdmissionModelStale` — warning; v1 inert by default (ADR-0003).
- Chaos drill failures in staging — Slack to `#shelf-oncall`.
- Any alert with `severity: info`.

### What does not alert at all

- Transient pod restart (single restart in 15m) — kube-state-metrics
  dashboard panel only.
- Hit rate dip < 60% for < 10 m — debounced by the `for: 10m` in the
  alert.

## Rotation shape

**Phase 1-3 (pre-v0.5 through v0.5):** Single person per week, Monday
09:00 → following Monday 09:00 local (IST for the cache team).
Secondary is the next-week primary (so every person has one "shadow"
week before their "primary" week).

**Phase 4-5 (v0.5 → rep-2 cutover):** Add a 2° rota:

| Role        | Shape                                 |
|-------------|---------------------------------------|
| 1° primary  | 7-day shifts (same as Phase 1-3)      |
| 2° backup   | Same 7-day shift, paged after 15 min  |
| Eng-lead    | Always available as tertiary          |

**Phase 6+ (multi-replica):** Move to a follow-the-sun rota if the
team has members in multiple time zones. Until then, stay
single-region.

### Rotation tool

Stored in PagerDuty:

- Service: `shelf`
- Escalation policy: `shelf-production`
- Schedule: `shelf-oncall-rotation`

Changes to the rota go through a PR against this doc AND a PagerDuty
change — both must land before the next Monday 09:00.

## Escalation paths

| When                            | Who                                                           |
|---------------------------------|---------------------------------------------------------------|
| Any page                        | 1° primary on-call                                            |
| 15 min, no primary response     | 2° backup on-call                                             |
| 30 min, no resolution           | Eng-lead                                                      |
| Cross-AZ/region S3 impairment   | Incident commander (platform SRE) + eng-lead                  |
| Security incident (auth / IAM)  | Security + eng-lead (follow `security-oncall` separate policy)|
| Shelf-caused data loss          | **DO NOT HAPPEN** — Shelf is a cache; see BLUEPRINT §9. If it somehow occurs, eng-lead + CTO. |

## First-day training checklist

A new on-call rings all of these bells before they can be listed on
the rota:

- [ ] Read `BLUEPRINT.md` §5, §6, §9 (principles, architecture, ops).
- [ ] Read `agents/out/03-plan.md` §5 (risks) and §6 (SLOs).
- [ ] Read ADRs 0001, 0003, 0004, 0008, 0010 at minimum.
- [ ] Read every file in `runbooks/` once.
- [ ] Run the chaos drills in staging: `chaos/pod-kill.sh`,
      `chaos/network-partition.sh`. Observe that dashboards reflect
      the drill.
- [ ] Walk through `docs/SLO.md` and execute the PromQL for 6.4.1 on
      the live Grafana `shelf-overview` dashboard.
- [ ] Role-play: a fake `ShelfHitRateTooLow` page; exec the 3
      diagnosis commands; explain the 3 mitigation options to a peer.
- [ ] Pair on one real on-call shift as "shadow" before primary.
- [ ] PagerDuty profile + Slack `#shelf-oncall` membership.
- [ ] `kubectl` context for staging + rep-2 prod, IRSA role assumed.
- [ ] `shelfctl` installed locally.

Sign-off: 2° on-call + eng-lead initial this section of the ticket
before the new on-call's first primary shift.

## Weekly rituals

- **Monday stand-up.** Outgoing on-call walks through the previous
  week's pages, dashboard anomalies, and any deferred follow-ups.
- **Thursday 30-min review.** Go through the Grafana `shelf-overview`
  dashboard; note any SLO drift; open a ticket per drift item.
- **Weekly chaos drill.** `chaos/pod-kill.sh` in staging; once per
  month, rotate to `chaos/network-partition.sh` and
  `chaos/nvme-fill.sh`. Document the drill outcome in
  `docs/drills/YYYY-MM-DD.md`.

## Hand-off template

Paste this at the top of the weekly on-call ticket:

```
## Week of YYYY-MM-DD hand-off

Pages this week:
  - (list)

Open alerts / known degradations:
  - (list)

Config changes landed:
  - (pin list version, chart version)

Things the next on-call must know:
  - (anything that's not in a runbook yet)

Follow-up tickets:
  - (link)
```
