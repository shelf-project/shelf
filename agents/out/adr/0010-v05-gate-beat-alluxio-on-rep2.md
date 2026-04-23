# ADR 0010: v0.5 gate — Shelf must beat Alluxio on rep-2 for 7 consecutive days

_Status: Accepted (planner amendment, 2026-04-23)_
_Deciders: eng-lead, critic §4 + §8_

## Context

Shelf is being designed at the exact moment Alluxio on rep-2 started
working. Post `UfsIOManager=256` and the 3-master HA migration
(2026-04-23), Alluxio delivers ~71 % cumulative / 76 % instantaneous
hit rate with a stable concurrency cap and `GOLD_DBT` ok-rate ≥ 99.9 %.
The uncomfortable question: in this emotional state, is the team
building a 9-10 month greenfield Rust project because (a) it is truly
the right next step, or (b) the scar-tissue from recent Alluxio
incidents is dominating the decision?

A project this ambitious needs an explicit **kill-switch** — a
pre-committed metric that, if missed, retires the project rather
than iterating further. Without it, sunk-cost reasoning prevails and
an underperforming Shelf gets maintained indefinitely.

## Decision

The **v0.5 milestone** (end of Phase 1 in the rewritten roadmap) is
the go/no-go for the entire Shelf project. v0.5 runs Shelf in
production on rep-2 for the `cdp` catalog's gold/silver read path,
with Alluxio in hot-standby. For 7 consecutive days, Shelf must meet
all five:

1. Cumulative cache hit rate ≥ 71 % (Alluxio baseline from E12).
2. `GOLD_DBT` DAG ok-rate ≥ 99.9 %.
3. Rep-2 p95 query latency ≤ 120 % of Alluxio baseline.
4. Zero Shelf-attributed pages.
5. Oncall surface ≤ 50 % of Alluxio's 7-day rolling rate (measured by
   unique runbook lookups + pages + Slack incidents).

If any one of (1)-(5) is missed, the project **stops** — a 2-week
gap-analysis window to diagnose is permitted; if the gap cannot be
closed in that window, Shelf is retired and the team reinvests in
the Alluxio path we already understand.

Gate evaluation is visible in Grafana dashboard `shelf-v05-gate`, and
the eng-lead signs the gate manually via a PR to `docs/gate-v05.md`
that commits the 7-day numbers.

## Alternatives considered

- **No kill switch; iterate forever.** Rejected: violates blueprint
  principle 6 ("simpler to operate than what it replaces") implicitly,
  because by the time we notice Shelf is not simpler, years will have
  passed.
- **TPC-DS benchmark as the gate.** Rejected: TPC-DS is synthetic;
  the rep-2 workload is real. Win the real battle.
- **Only hit rate as gate.** Rejected: hit rate can be gamed with
  aggressive admission; the 5-metric gate captures "actually running
  in production."

## Consequences

- **Positive.** The team has permission to fail fast. "Cannot match
  Alluxio" is a publishable, honest outcome, not a career-ending one.
- **Positive.** Every scope cut downstream (ADR-0001 through ADR-0009)
  is justified by "does it help us hit the v0.5 gate in 10 weeks?".
  Any feature that does not is deferred.
- **Negative.** A team member may interpret the gate as adversarial
  to their work. The eng-lead owns reframing it as shared permission
  to be honest, not individual performance pressure.
- **Guardrail.** The gate numbers themselves cannot be lowered without
  a superseding ADR. No "well, 68% is basically 71%" negotiations.
