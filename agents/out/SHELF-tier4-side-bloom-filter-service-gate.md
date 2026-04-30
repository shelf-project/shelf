# SHELF Tier-4 — `side_bloom.rs` + `filter_service.rs` Activation Gate

Gate-criteria memo for activating the dormant `side_bloom.rs` +
`filter_service.rs` modules. Plan §5 Tier-4 #1 marks this as
*"estimated 20–40 % rowgroup-fetch reduction on equality-pushdown
queries that lack writer-side blooms"* but explicitly gates on SHELF-52
advisor showing **>30 % of cost concentrated in no-writer-bloom
tables**. This memo records the gate-pending status and the criteria
that must hold for activation to be authorised.

## TL;DR

**Cannot evaluate the gate today.** SHELF-52 (PR #70) is a draft PR
pending review/merge; even after merge, it needs **≥ 7 days of live
recommendation output** against `cdp.trino_logs.trino_queries` to
produce a defensible "X % of cost in no-writer-bloom tables" number
(less than that and the recommendation set is dominated by whichever
queries happened to run that week, not the steady-state mix).

**Earliest re-evaluation: 2026-05-15** (T+14d after a hypothetical
2026-05-01 SHELF-52 merge — adjust if SHELF-52 actually merges later).
Until then, both `side_bloom.rs` and `filter_service.rs` remain
dormant and uninvoked.

## The lever

**20–40 % rowgroup-fetch reduction** on equality-pushdown queries
against tables that lack writer-side Parquet bloom filters.

**Mechanism.** When the Parquet writer didn't enable Parquet
bloom-write — Trino #20662 added it but most legacy tables predate it,
and external producers (data engineering, partner imports) often don't
enable it either — shelfd builds approximate Bloom filters over hot row
groups itself and exposes a `ShelfFilterService` probe endpoint
(gRPC primary, HTTP shim as fallback variant). The Trino Iceberg
plugin queries:

> *"Does row-group N for predicate P possibly match?"*

and gets back `MAYBE` or `NO`. On `NO`, Trino skips the entire
row-group fetch from S3. On `MAYBE`, Trino falls back to the existing
fetch-and-evaluate path (so false-positives only cost the existing
behaviour; correctness is preserved).

The 20–40 % range comes from plan §5 — the floor assumes equality
pushdowns on dimension-key columns where shelfd has seen ≥ a few
hundred row groups; the ceiling assumes a workload heavily dominated
by such queries.

## Current state of the dormant modules

Verified path-by-path against `origin/main` at the time of writing:

- **`shelfd/src/side_bloom.rs`** — file exists. `pub mod side_bloom;`
  is declared in `shelfd/src/lib.rs:61`. **No callers anywhere in the
  shelfd crate.** The module compiles into the binary as dead code.
- **`shelfd/src/filter_service.rs`** — file exists. `pub mod
  filter_service;` is declared in `shelfd/src/lib.rs:45`. An HTTP
  probe handler exists in `shelfd/src/http.rs` and is wired into the
  router, but **`state.filter_service` is never set in
  `shelfd/src/main.rs`** — it remains `None`, so the handler always
  takes the `None` branch and returns the "no filter service
  configured" response. End-to-end, the surface is dormant.

Activating the lever therefore needs three things in addition to the
gate-criteria below: (i) build a `ShelfFilterService` instance from
the running `side_bloom` aggregator, (ii) populate `state.filter_service`
in `main.rs`, (iii) ship the Trino-side client (gRPC or HTTP) that
issues the probe. Items (i) and (ii) are local to shelfd; (iii) is a
Trino plugin change.

## Gate (all three must hold simultaneously)

These are the criteria the orchestrator reads to authorise Tier-4
funding for this lever. **All three** must hold; if any one fails,
the lever stays gated.

1. **SHELF-52 (PR #70) merged AND running for ≥ 7 days** against live
   `cdp.trino_logs.trino_queries`. The 7-day floor is non-negotiable —
   recommendation output over a shorter window is dominated by
   week-of-month effects (dbt-run schedule, exam-period traffic).
2. **Concentration.** SHELF-52 output identifies **≥ 30 %** of total
   scan-cost concentrated in tables where the Iceberg `metadata.json`
   for the latest snapshot shows **no `bloom_filter_columns`** entry
   for the predicate column(s) used in the heaviest queries. (The
   advisor produces this set directly; it is not a separate query.)
3. **Bloom-untouchable residual.** SHELF-52's own follow-up — i.e. the
   "enable writer-side blooms" recommendation — is **not** a cheaper
   path for the same workload. In practice this means SHELF-52
   recommends "enable writer-side blooms" for some subset of the
   ≥ 30 %, but at least **30 percentage points of the total** remain
   bloom-untouchable: tables owned by external producers (no commit
   access from data-platform), or legacy snapshots that won't be
   rewritten because the table is append-only / archived.

If criterion #3 fails (i.e. SHELF-52 says "everyone can just turn on
writer-side blooms"), the cheaper path wins and this lever stays
deferred forever — the writer-side fix delivers the same 20–40 %
without a new sidecar surface or a Trino plugin change.

## Effort if gate triggers

**L (~3–4 wk)**:

- Wire `state.filter_service` in `shelfd/src/main.rs`.
- Populate `ShelfFilterService` from the `side_bloom` aggregator's
  build output (per-row-group Bloom filters, refreshed on cache
  admission).
- Add the Trino plugin gRPC client (or the HTTP-shim variant if the
  gRPC dep is judged too heavy) for the `MAYBE` / `NO` probe.
- Add Trino-side caching of probe results per query (so we don't
  re-probe the same row group N times in one scan).
- Write `THREAT_MODEL.md` per plan §8 — **mandatory** for any new
  sidecar surface; the parse / DoS / poisoning analysis below is the
  preview, not the final document.

## Sidecar threat-model preview

Mandatory references to plan §8. The full `THREAT_MODEL.md` shipped
alongside the activation PR must address each of these explicitly; the
preview is non-binding scoping.

- **Path-traversal containment.** Allowlist the four canonical
  buckets only —
  `pw-data-cdp-prod-{gold,silver,bronze,temp}-layer` — and reject any
  probe whose object key resolves outside them. Reject `..` segments
  and absolute keys defensively.
- **Parse / DoS containment.** Hard caps on attacker-controlled
  Parquet inputs:
  - `max_footer_bytes = 8 MiB`
  - `max_blob_count   = 4096`
  - `max_page_index_entries = 65 536`
  Anything above any cap fails closed (returns `MAYBE`, never panics
  / never holds the probe socket open).
- **Negative-cache poisoning containment.** Do **not** cache 4xx
  responses under the positive-result key. A 404 / 403 / 5xx must
  invalidate any cached `NO` for the same key, otherwise an attacker
  who can cause one transient 4xx pollutes future probe results.
- **PII containment.** No Iceberg `readable_metrics` JSON in any
  sidecar response — `readable_metrics` carries column-level min/max
  values which can be PII. Probe responses are limited to
  `MAYBE` / `NO` plus an opaque key.

## Status

**Gate-pending.** Re-evaluate at **T+14d after SHELF-52 merge** —
i.e. 7 days for the recommendation set to stabilise plus 7 days for
the orchestrator to read it and run the criterion-#3 check.

**Owner:** cost-plan orchestrator.
