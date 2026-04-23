# BLUEPRINT amendment — Shelf Advisor (v0.4 proposal)

_Status: draft amendment, pending scientist (agent 1) + critic (agent 2) + planner (agent 3) review before merging into `BLUEPRINT.md`._
_Author of draft: parent agent, 2026-04-24._
_Rationale: user request — "since this is our cache layer, it should give me intelligence like if some query running multiple time then it should make and schedule matview model of it with those intelligent. or make dbt something like that."_

This is a **major amendment** per `shelf/agents/README.md` → "Amendment flow": it introduces a new killer feature (§7.6), a new phase (Phase 11), and a new binary (`shelf-advisor`). It therefore requires a full agent 1 → 2 → 3 cycle. This document is the **design proposal** the chain is given as input; it is not yet merged.

---

## 1. Summary (for TL;DR table and §1 update)

Add one row to the TL;DR design-choices table in §1:

| Design choice | Why |
|---|---|
| Workload-aware MV advisor (§7.6): mines the query log, identifies repeat aggregations, proposes Iceberg MVs or dbt models, auto-schedules them | Turns Shelf from a passive cache into an active workload optimiser — the only component that sees query fingerprint + cache miss + snapshot_id + cost |

Add one bullet to the v0.4 header log:

> **v0.4 changes** (on top of v0.3): added § 7.6 *Workload-aware MV advisor* and Phase 11 *`shelf-advisor` binary*. Shelf now proposes (and optionally creates or emits-as-dbt-model) materialised views for recurring aggregation patterns, powered by query-log mining, Shelf cache-miss telemetry, and snapshot-aware cost modelling. Total timeline extends to ≈ 11 months.

---

## 2. New section — §7.6 Workload-aware MV advisor

Insert after §7.5 and before §8. Proposed text:

```markdown
### 7.6 Workload-aware MV advisor (closing the "I already knew this query was hot" gap)

Commercial warehouses (Snowflake, Redshift, BigQuery) have advisors
that watch the workload and suggest — or in Redshift's AutoMV case,
automatically create — materialised views. On Trino + Iceberg + S3
no such advisor exists. Yet **Shelf sees more of the query workload
than any other component in the stack**:

- Normalized query plan (from `QueryCreatedEvent` — §7.2)
- How many times each fingerprint ran, by tenant, by hour
  (from `cdp.trino_logs.trino_queries` and Shelf's own metrics)
- Which row groups / columns were physically read
  (from `SplitCompletedEvent` learning — §7.2 Phase 2b)
- Cache hit / miss and S3 bytes saved per fingerprint (§6)
- Current Iceberg snapshot per referenced table (§13.5 snapshot-watcher)
- Tenant budget and write-cadence tolerance (control plane)

No other tool in the stack sees the union of these signals. We turn
them into MV recommendations.

#### 7.6.1 Fingerprinting

Do **not** fingerprint on raw SQL text — literals and formatting cause
false negatives. Fingerprint on the *optimised logical plan* emitted
by Trino in `QueryMetadata.jsonPlan`:

1. Parse the jsonPlan.
2. Canonicalise: strip node IDs, replace literals with typed
   placeholders (`INT_1`, `VARCHAR_1`, …), sort commutative children.
3. Hash with SHA-256.
4. Persist into `shelfd`'s control plane keyed by tenant.

Stability requirement: only promote a fingerprint to a *candidate* if
it has been stable for `advisor.fingerprint.stable_days` (default 7)
AND its referenced tables have not undergone schema evolution in that
window. Shelf reads Iceberg `snapshot_log` to check this cheaply.

#### 7.6.2 Cost model

For each candidate fingerprint, compute on `shelf-advisor`'s nightly run:

```
runs_per_day        = count over the last 7 days, mean
bytes_scanned_raw   = sum of SplitCompletedEvent.physicalInputBytes / runs
bytes_scanned_mv    = estimated post-MV scan (aggregates usually < 1% of raw)
bytes_refresh_delta = estimated daily bytes to refresh the MV
                      (uses Phase-10 snapshot-watcher: daily delta bytes)
storage_mv          = estimated MV table size, from cardinality + schema
savings_per_day     = runs_per_day × (bytes_scanned_raw − bytes_scanned_mv)
                        − bytes_refresh_delta
                        − storage_mv × storage_cost_per_gb_per_day
net_benefit_ci95    = bootstrap over the 7-day sample; MUST be > 0 at
                       95 % confidence to proceed
```

This is the same basic form as Redshift AutoMV's net-benefit (VLDB '23);
what's unique here is that `bytes_refresh_delta` comes from Shelf's
snapshot-watcher, not from wall-clock refresh intervals — so MVs on
slowly-churning tables win over MVs on busy tables, automatically.

A candidate only becomes a *recommendation* if:
- `net_benefit_ci95 > 0`
- `runs_per_day ≥ advisor.min_runs` (default 10)
- `storage_mv ≤ advisor.max_mv_size_gb` (default 5 GB)
- No table in the fingerprint has churned schema in the last 7 days
- No overlap with an existing MV that already covers > 80 % of the
  scanned bytes (else we'd create redundant MVs)

#### 7.6.3 Output modes (user chooses per tenant)

The advisor never imposes anything. It emits recommendations; the
tenant configures what to do with them. Three modes:

1. **`recommend-only` (default)** — writes
   `advisor/recommendations/<tenant>/<YYYY-MM-DD>.yaml` and exposes
   it in a Grafana panel. Nothing is created. Ops reviews weekly.

2. **`dbt-emit`** — same YAML, plus opens a pull request against a
   configured dbt repo containing:
   - A dbt model `.sql` file (materialised as `table` or `materialized_view`
     depending on the dbt adapter's Iceberg support).
   - A `schedule.yml` entry with a refresh cadence derived from the
     target SLO.
   - A PR description containing the evidence: runs/day, bytes
     saved, candidate fingerprint summary, the exact queries that
     would rewrite onto this MV.

   This is the **recommended production default** — the user's
   existing code review and git audit become the governance layer.

3. **`auto-materialize`** — Shelf itself runs
   `CREATE MATERIALIZED VIEW` via a Trino JDBC connection (held by
   `shelf-advisor`, scoped to an `advisor_user` role with a
   locked-down set of permissions: CREATE MV in `advisor_*`
   schemas only, SELECT on referenced base tables, never DROP on
   anything not created by this user). Creations logged to the
   `shelf-advisor` audit stream.

   This mode is off by default. Enabling it requires an explicit
   tenant config + at least one signoff recorded in
   `advisor_user`'s audit trail.

#### 7.6.4 Refresh strategy

Advisor-created MVs use Iceberg materialised views, refreshed by the
Phase-10 `shelf-mv-refresh` service on the snapshot-delta path. This
is where §7.6 explicitly depends on Phase 10: without incremental
refresh, the advisor's net-benefit numbers don't hold for MVs on
frequently-written tables.

If Phase 10 is not yet shipped, the advisor still produces
recommendations but the `auto-materialize` mode is disabled and the
`dbt-emit` mode emits a warning in the PR description explaining
that refresh will be full-table until Phase 10 lands.

#### 7.6.5 Drop / deprecate path

An MV only stays valuable while its fingerprint keeps running.
`shelf-advisor` tracks MV hit rate in Shelf's control plane (§7.5.3)
and flags:

- MVs with < 5 hits/day over the last 14 days → recommendation: `DROP`.
- MVs whose base table has undergone schema evolution → recommendation:
  `DROP` (advisor will re-propose post-stabilisation).
- MVs whose refresh cost has grown > advisor.max_refresh_cost_ratio
  (default: 2× initial estimate) → recommendation: `DROP or
  RECONSIDER`.

In `auto-materialize` mode, advisor-created MVs can be auto-dropped
after a 7-day grace window and operator ack; in `dbt-emit` mode a
deprecation PR is opened.

#### 7.6.6 What we don't build in v1

- Speculative MVs nobody has asked for — Firebolt's aggregating-index
  approach. The advisor only proposes MVs that match observed query
  fingerprints.
- Predicate-pushdown MV subsumption ("a `GROUP BY region` MV can
  serve `GROUP BY region WHERE year=2026`"). Research-grade; Trino's
  built-in MV rewrite handles the common case.
- Cross-tenant MV sharing. Each tenant's advisor is scoped to its own
  query log and its own recommendations.
- A SQL UI for the advisor; we ship a CLI (`shelfctl advisor …`) and a
  Grafana panel. A web UI is a v2 conversation.

#### 7.6.7 Why Shelf owns this and not dbt / Trino / an external tool

- **dbt** is a transformation tool, not an observer. It has no
  query-log or cache-miss signal.
- **Trino** has `system.runtime.queries` but no persistent
  fingerprint store, no snapshot-aware cost model, and no place to
  put one without a TIP we'd have to write anyway.
- **Shelf already has** the query log feed (from the event listener),
  the cache miss signal, the snapshot-watcher, and the tenant config.
  The advisor is `≈ 3 000 LOC` on top of state Shelf already maintains.

The advisor is small precisely **because** Shelf already exists.
```

---

## 3. New roadmap phase — Phase 11

Append one row to the §12 roadmap table:

| Phase | Window | Scope | Success gate |
|---|---|---|---|
| **11 — `shelf-advisor` binary (§ 7.6)** | 4-6 weeks | Implement fingerprinting, cost model, three output modes (`recommend-only` default, `dbt-emit`, `auto-materialize`). Depends on Phase 10 for incremental refresh numbers. Ships as a separate binary (same pattern as `shelf-result-cache` and `shelf-mv-refresh`). | On rep-2's last 30 days of `trino_logs`, the advisor identifies ≥ 5 MV candidates whose predicted net benefit matches measured benefit within ± 20 % after being created. `dbt-emit` mode opens a well-formed PR against a test dbt repo. `auto-materialize` gated behind tenant signoff. |

Update the total timeline paragraph:

> Total ≈ 41-47 weeks (≈ 10-11 months) to Phase 11. Production value
> delivered from phase 2 onward; phases 8-10 are the "close the gap vs
> Warp Speed / Firebolt" track; phase 11 is the "Shelf is a workload
> optimiser, not just a cache" track. Phases 8, 9, 10, 11 can run in
> parallel with phase 7 if staffed.

---

## 4. §14 update — "What Shelf is NOT"

Keep existing bullets. Add:

> - **`shelf-advisor` does not execute queries or move data.** It
>   only reads the query log, emits recommendations, and in
>   `auto-materialize` mode executes DDL against a Trino coordinator
>   via JDBC under a scoped `advisor_user` role. All data motion is
>   done by Trino's own MV refresh machinery (Phase 10's
>   `shelf-mv-refresh`), not by the advisor.
> - **The advisor is not a dbt competitor.** In `dbt-emit` mode it
>   *writes* dbt code for the user to review. It does not run dbt, it
>   does not maintain dbt state, it does not replace `dbt run`.
>   Analytics teams keep their existing transformation pipeline; the
>   advisor just seeds it with MV candidates they would not have found
>   manually.

---

## 5. §6 control-plane additions

Add to §6.3 (control plane):

> - **Advisor store** — SQLite (default) or Postgres (HA) database
>   holding fingerprints, observation windows, cost estimates, and
>   recommendation history per tenant. Separate from the Raft state
>   machine (which is membership + pin list only). Owned by
>   `shelf-advisor`, readable by `shelfctl advisor`.

---

## 6. Agent-level ownership (affects `agents/README.md`)

Update the cast table in `agents/README.md`:

- Agent 4 (`shelfd-builder`): add "Phases 10, 11" to its scope row.
- Agent 6 (`trainer-builder`): add "Phase 11 (fingerprinting + cost model)" to its scope row; fingerprinting is ML-adjacent enough to fit the trainer. The DDL / dbt-emit / JDBC parts stay with agent 4.
- Agent 9 (`security-auditor`): gets a new required threat-model pass for `shelf-advisor`'s JDBC credential and the dbt-emit token (see §7 below).
- Agent 10 (`scribe`): documents the three modes and a migration path ("I already have MVs managed manually; does the advisor collide with them?").

---

## 7. Risk additions (to §13)

| Risk | Mitigation |
|---|---|
| Advisor recommends a bad MV that regresses performance | Every recommendation must carry a measurable net-benefit CI95 > 0. Auto-materialize mode tracks post-creation hit rate for 14 days; if under projection, auto-proposes `DROP`. |
| `auto-materialize` credentials abuse — someone steals the Trino JDBC creds and creates rogue objects | `advisor_user` role is scoped to `CREATE MV in advisor_* schemas` only; no DROP on objects it didn't create; all actions logged to an immutable audit stream; secret rotated monthly. Default is `recommend-only`, not `auto-materialize`. |
| `dbt-emit` GitHub / GitLab token abuse | Token scoped to a single dbt repo and a single branch prefix (`shelf-advisor/*`); cannot force-push, cannot delete, cannot merge. PR merge requires human reviewer. |
| Fingerprint drift across Trino minor upgrades | Shelf stores the Trino version alongside the fingerprint and invalidates on minor bumps that change `jsonPlan` shape; advisor warns at upgrade time. Fallback: SQL-text fingerprint as a secondary key. |
| Advisor and user-managed dbt MVs collide | Advisor reads existing MVs from the Iceberg catalog on every run; any candidate whose fingerprint already matches an existing MV with >80 % overlap is suppressed. Audit log records "suppressed (covered by existing MV X)". |
| Runaway storage from too many MVs | Per-tenant MV count cap (`advisor.max_mvs_per_tenant`, default 50) and per-tenant cumulative storage cap (default 50 GB). Exceeding either blocks new candidates and emits a warning. |

---

## 8. Open questions for the chain to settle

1. **Fingerprint substrate.** `QueryMetadata.jsonPlan` vs
   `QueryCreatedEvent.metadata.plan` vs running `EXPLAIN` offline:
   which is stable across Trino minor releases? Agent 1 to settle in
   Pass 3.6.
2. **dbt adapter reality check.** Which dbt adapters (`dbt-trino`,
   `dbt-databricks`, `dbt-snowflake`) actually let you materialise
   as an Iceberg MV today vs a plain `table`? Agent 1.
3. **Auto-drop safety.** Do we ever auto-drop in `auto-materialize`
   mode, or always require human ack? Agent 2 to red-team.
4. **State store.** Is SQLite good enough for the advisor store in
   v1, or do we need Postgres from day one for multi-tenant? Agent 3
   to decide.
5. **Does the advisor run on-cluster or as a sidecar to Trino?**
   Affects credential model. Agent 3.

---

## 9. Blast-radius statement (required by agent 9)

In `recommend-only` and `dbt-emit` modes, the advisor has **read-only**
access to:

- Trino query event stream (already consumed by §7.2 plugin)
- `cdp.trino_logs.trino_queries` (read-only role)
- Iceberg catalog metadata (read-only; uses HMS Thrift via the
  existing Trino service account)
- Shelf's own control-plane state
- The dbt repo via a narrowly-scoped PR-only token (no write to
  existing branches)

In `auto-materialize` mode, the advisor additionally has **write**
access to:

- A single Trino JDBC connection under `advisor_user`, which is
  permitted only `CREATE MATERIALIZED VIEW` in `advisor_<tenant>_*`
  schemas. No other writes. No DROP on objects not created by this
  user. All actions logged.

The advisor is **never** granted write access to base tables. It
cannot `INSERT`, `UPDATE`, `DELETE`, `DROP TABLE`, or `ALTER TABLE` on
any table in any tenant.

---

## 10. How the amendment chain consumes this doc

Agent 1 (scientist) must do:

- Pass 1: verify the Redshift AutoMV (VLDB '23), MISO (SIGMOD '22),
  BigSubs (SIGMOD '19) citations exist and say what this doc claims.
- Pass 3.6: survey MV selection / advisor literature; evaluate
  whether jsonPlan fingerprinting is stable across Trino 480 minor
  releases (actual source inspection, not hand-waving).
- Pass 5: list open research questions — e.g. is side-information
  from cache-miss bytes actually a better signal than query-log
  bytes? Nobody has measured this for Trino + Iceberg.

Agent 2 (critic) must do:

- §4 (Monday scope): what do we cut? Candidate cuts: dbt-emit mode
  (defer to v2), `auto-materialize` mode (defer indefinitely),
  fingerprint stability check (simplify).
- §7: propose edits to this section where it over-promises or
  under-specifies.
- Honesty audit: the "closing the X gap" framing in §7.4 / §7.5
  earned scrutiny; §7.6 should face the same.

Agent 3 (planner) must do:

- Break Phase 11 into tickets in §4 of `03-plan.md`.
- Add ADR for fingerprint substrate, storage substrate, refresh
  dependency on Phase 10.
- Produce the canonical `BLUEPRINT-DIFF.md` merging this file with
  scientist and critic outputs.
- Apply the final diff to `BLUEPRINT.md`, bump version to v0.4.
