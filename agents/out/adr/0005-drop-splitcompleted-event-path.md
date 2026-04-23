# ADR 0005: Drop SplitCompletedEvent; use plugin-observation + operatorSummaries

_Status: Accepted (planner amendment, 2026-04-23)_
_Deciders: eng-lead, scientist agent §3.2 + §1 (row 18), critic §1.4 + §6(4)_

## Context

The v0.3 blueprint's Phase 2b-signal-2 builds row-group-level learning
on top of `EventListener#splitCompleted`, citing Trino PR #26425 as
"already enabling worker event listeners". This is **actively wrong**:

- PR #26425 was **superseded and closed** in favour of Trino PR #26436
  (merged 2025-08-19), which **removed `EventListener#splitCompleted`
  entirely** (-666 lines, 42 files).
- Upstream rationale: per-split events are too expensive for
  data-lake connectors. Replacement pointer is
  `QueryStatistics#getOperatorSummaries`, available only at
  `QueryCompletedEvent` time.

Phase 2b as designed cannot be built against shipping Trino. We must
redesign before writing a line of listener code.

## Decision

Two mechanisms replace the dead SPI path, and both are independent of
each other:

1. **Plugin-side observation (Phase 2b-signal-1), primary.** After a
   worker range-GET on a Parquet footer, `ShelfFileSystem` parses the
   footer locally, correlates row-group statistics against the
   predicate captured from `QueryCreatedEvent`, and issues row-group
   prefetches before the worker's next range-GET. No new Trino SPI is
   needed.
2. **Post-hoc learning via `QueryCompletedEvent.operatorSummaries`,
   secondary.** A nightly trainer aggregates per-operator summaries
   into `(query_sketch → likely_row_groups)` maps used by Phase 2a's
   file+footer prefetch to promote to row-group prefetch on the *next*
   matching query. Coarser than per-split events, but live on all
   shipped Trino.

Additionally, we **file a Trino TIP** for a focused, scoped split-level
cache-interest event as a long-lead-time v2 upgrade — but we ship
without waiting for it.

## Alternatives considered

- **Wait for upstream to re-introduce `splitCompleted`.** Rejected:
  upstream has decided against it; we are not going to relitigate.
- **Replicate `IcebergSplitSource` in the plugin.** Rejected:
  explicit non-goal in blueprint §7.2; would duplicate Trino
  planning logic.
- **Plugin-observation only, no learning.** Viable but abandons the
  "learn from history" story entirely. operatorSummaries is coarse
  but free; we keep it.

## Consequences

- **Positive.** Phase 2 work is unblocked and based on live-in-prod
  Trino SPI. Blueprint §13 risk row is corrected.
- **Negative.** Coarser granularity for learning (per-operator, not
  per-split). May need richer sketches to recover the signal.
- **Guardrail.** Don't start writing listener code until experiment
  E2 confirms operatorSummaries carries enough information for our
  query mix.
