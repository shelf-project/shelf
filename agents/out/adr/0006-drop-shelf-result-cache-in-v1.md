# ADR 0006: Drop `shelf-result-cache` from v1; Redis Gateway owns result caching

_Status: Accepted (planner amendment, 2026-04-23)_
_Deciders: eng-lead, critic §1.8 + §3, scientist agent §5 (question 10)_

## Context

The v0.3 blueprint introduces `shelf-result-cache` as an "independently
deployable companion binary" to `shelfd`, targeted for Phase 1.5,
caching full query results keyed on
`sha256(normalized_sql || snapshot_map)`. The COMPARISON.md already
commits to a Phase 0 Redis-backed Trino-Gateway result cache — which
covers **the same 60-70 % of BI dashboard traffic** for the same
snapshot-aware invalidation story.

Shipping both means two deployables, two oncall paths, two incident
histories, and a tempting coupling between `shelfd` and a result
cache that is nominally independent.

## Decision

Drop `shelf-result-cache` from the v1 roadmap. The COMPARISON Phase 0
Redis + Trino-Gateway result-cache plugin **is** the v1 result cache.
It runs in a different namespace, owned by the data-platform team,
and reuses the same `SnapshotWatcher` that `shelfd` will also consume
for metadata-tier snapshot tagging.

In v2+, if measurement shows a reason to fold result caching into
Shelf's DRAM tier (e.g. to co-locate with metadata for a hotter hit
path, or to avoid the Redis dependency), revisit as a new ADR.

## Alternatives considered

- **Ship both.** Rejected per critic §1.8: two cache layers to reason
  about; the "independent" binary tends to share infrastructure with
  `shelfd` as soon as convenience demands it.
- **Fold result caching into `shelfd` in v1.** Rejected: `shelfd` is
  a byte-range cache; mixing result frames changes its storage
  profile and eviction story.
- **Drop result caching entirely.** Rejected: dashboard traffic is
  ~80 % of our queries per AGENTS.md; too much value to defer.

## Consequences

- **Positive.** Scope control. `shelfd` stays a pure byte-range cache.
  Users see result-cache value on week 2 (Phase 0R) without waiting
  for Shelf v1.0.
- **Negative.** `shelf-result-cache` marketing/branding moves to
  "future v2". Docs must be clear: in v1, the result cache is Redis
  + Gateway, not a Shelf component.
- **Guardrail.** Codeowners reject any PR that introduces
  `shelf-result-cache` or its clone into the v1 repo without a
  superseding ADR.
