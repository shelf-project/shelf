# I1 — Query-plan subplan caching (design spike)

**Status:** research spike, not for v1. Track in Tier 5 of the plan.
**Owning ticket:** `i1-subplan-caching`.
**Related:** `BLUEPRINT §7.5.4 deferred`, honest-residual §7.5.

## The honest residual this closes

Firebolt wins on *novel* ad-hoc aggregations — the first time
anyone asks `SELECT region, SUM(revenue) FROM orders GROUP BY
region WHERE year = 2026`, we miss the cache completely because
the byte surface is new. Most analytics workloads trend the other
way: the same aggregate gets re-asked with a shifted filter. H1's
MV advisor already addresses the repeated-query case. I1 addresses
the **same-group-by, different-filter** case without waiting for a
dbt PR to land.

## Core idea

Maintain a lattice of computed aggregations keyed on:

```
(table_snapshot_id, grouping_columns_sorted, filter_cols_superset)
```

and answer a query by finding the **tightest** cached aggregate
whose filter predicate is a *superset* of the incoming predicate,
then applying the residual predicate on the cached result.

Example:
- Cached: `GROUP BY region WHERE year ∈ [2020, 2026]` against
  snapshot `s42`.
- Query: `GROUP BY region WHERE year = 2026` against snapshot
  `s42`.
- Rewrite: serve from the cached lattice entry with
  `HAVING year = 2026` pushed into the aggregate row's surviving
  tuples (requires the cached aggregate to include `year` in the
  grouping key, not a raw filter).

The lattice is a **set-containment order** on predicates, not a
value-containment order on columns — we cache the *coarsest*
aggregate that still lets us answer finer queries by filtering.

## Where it lives

Two plausible homes, both viable:

| Option | Lives in | Pros | Cons |
|---|---|---|---|
| **A: Trino connector extension** | Iceberg connector fork | Uses Trino's own optimizer rules for correctness proofs; MV rewriter already understands `(table, predicate)` subsumption | Forces a connector patch, hard to upstream; couples shelf to Trino release cadence |
| **B: Shelf control-plane aggregate store** | `shelfd` + event listener | Ships in shelf's own release train; can leverage the H1 jsonPlan fingerprint substrate | Must re-implement predicate subsumption; risk of getting semantics subtly wrong |

**Recommendation:** B, layered over the existing H1 fingerprint
telemetry. The MV advisor already canonicalises filter predicates;
widening that canonicalisation to expose `(grouping_cols,
filter_cols)` dimensions is incremental work and re-uses the
verification harness H4 already built.

## Correctness guardrails

The big footgun is aggregate reuse across snapshots: answering
`2026` from a snapshot that predates the 2026 data would silently
lose rows. Non-negotiable invariants:

1. **Snapshot-pinned.** Every lattice entry names a specific
   `table_snapshot_id`. Iceberg already guarantees monotonic
   snapshot IDs; shelf must refuse reuse across snapshots.
2. **Predicate-superset verified.** The rewriter must prove the
   cached predicate is a strict superset of the incoming
   predicate. We need a formal `implies(p_cached, p_query)` check
   — for our workload that's numeric ranges + discrete `IN`
   lists, not full SMT, so a few hundred lines of Rust plus
   tests.
3. **Grouping-key preservation.** If the query groups by
   `(region, year)` and the cache entry groups by `(region)`
   only, the answer is wrong — the cache must group by at least
   the query's key.
4. **Correctness test harness.** Every rewrite must re-run the
   original query below the lattice hit, compare answers, and
   disable the lattice entry on mismatch. Cheap insurance before
   trust is earned.

## Non-goals

- No joins. The lattice is *single-table* aggregates only in v1.
  Multi-table aggregates are correctness minefields (join-order
  equivalence classes, semi-join reductions) and should land
  after the single-table version has baked for at least one
  quarter.
- No user-defined aggregates. Built-ins only (`SUM`, `COUNT`,
  `AVG` — decompose `AVG` into `SUM/COUNT` before caching —
  `MIN`, `MAX`, `APPROX_DISTINCT`).
- No streaming / micro-batch invalidation. Lattice entries die
  when the snapshot dies.

## Minimum experiment to green-light

1. Pick three recurring dashboards (3-5 queries each) from
   `trino_logs` that Firebolt's comparative runs on SF1000 won
   against shelf. Confirm the win is aggregate reuse, not raw
   compute.
2. Build the **query canonicaliser** that emits
   `(snapshot, group_cols, filter_key_superset, projections)`
   plus a proof-carrying subsumption check.
3. Offline replay: for each dashboard query, check if a coarser
   recent query could have answered it. Publish hit-rate; target
   ≥ 40 % for green-light.
4. If the offline replay crosses 40 %, ship a `shelfd` endpoint
   `POST /lattice/serve` that returns the cached aggregate for a
   jsonPlan fingerprint. Wire into the event listener behind a
   feature flag.

## Why this isn't v1

The plan's Tier 5 is explicit: I1 is "research-grade, correctness
tricky". The v1 product has to hit the TPC-DS SF1000 ship bar
without it, and the H1 advisor already hits the low-hanging
repeated-query residual. The quickest path to closing §7.5 for
the dashboards that matter is H1 + H3 + H5; I1 is what we add
when those plateau.

## Exit criteria to consider v1 ready

- Offline replay hit-rate ≥ 40 % on the shortlisted dashboards.
- Correctness harness passes 1M query pairs without a single
  mismatch.
- The feature flag can be flipped on/off per coordinator without
  a shelfd restart.
