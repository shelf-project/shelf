# ADR 0007: Drop Phase 10 incremental MV refresh from Shelf roadmap

_Status: Accepted (planner amendment, 2026-04-23)_
_Deciders: eng-lead, critic §3 (extra section) + §7_

## Context

The v0.3 blueprint adds Phase 10 (8-12 weeks) — a new `shelf-mv-refresh`
service that watches Iceberg snapshot deltas, reads only delta files,
computes incremental aggregates, and commits via Iceberg `MERGE`.
Positioned as the "Firebolt aggregating-index gap closer".

The problem: this is not a cache. It is a **compute service** that
reads data, runs aggregation operators, and writes Iceberg commits.
Scope-wise it overlaps with dbt incremental models and with Trino's
native materialised-view refresh path. Including it inside Shelf:

- Doubles the project surface area
- Introduces Iceberg write semantics into what has been strictly a
  read-through cache (blueprint §14 "Shelf is not write-through")
- Forks engineering attention at exactly the moment we are trying to
  ship a cache that beats Alluxio

## Decision

Remove Phase 10 from the Shelf roadmap entirely. If the organisation
wants incremental MV refresh:

- Start it as a separate project (name TBD; `mv-refresh`, `iceshelf`,
  whatever) with its own repo, its own dependency footprint, and its
  own oncall.
- Or file a Trino TIP to add native incremental MV refresh to Trino
  itself — the natural home for the feature.

Shelf will happily cache the files that either of those projects
writes, because those files are just more Iceberg tables. That is the
whole of Shelf's interaction with them.

## Alternatives considered

- **Keep Phase 10 but descope to "read-only MV awareness".** That is
  already Phase 9; Phase 10 specifically added the refresh *service*.
  Phase 9 stays.
- **Keep Phase 10 as-is, extend timeline.** Rejected: violates
  blueprint principle 6 ("simpler to operate than what it replaces").
  We are not ready to operate a write-path service.

## Consequences

- **Positive.** Shelf stays a cache. OSS narrative is tighter.
  Engineering scope is honest. The "Firebolt gap" closure is recognised
  as a Tier-2 optional ambition (MV-aware *caching* in Phase 9), not
  an end-to-end claim.
- **Negative.** Marketing story is less aggressive; we cannot say
  "Shelf closes the Firebolt aggregating-index gap". We can say
  "Shelf + Iceberg MVs close the gap" — which is honest.
- **Guardrail.** If a "Phase 10 v2" proposal reappears in future
  planning cycles, it needs to live as a separate ADR superseding
  this one **and** show a new organisational sponsor for the compute
  service.
