# Admission label definition

_Owner: agent-6 (trainer builder). Status: **material** — this is Pass 0
per `shelf/agents/6-trainer-builder.md`. Review before any admission model
code is written._

This document defines, for all admission-model training runs in Shelf:

1. The target (label).
2. The prediction horizon.
3. The train/valid/test split strategy.
4. The leakage controls — both which features are forbidden and how we
   verify none are leaking.
5. The metric hierarchy (primary + guardrails).

It is the single source of truth for the trainer. A model trained against
a label defined outside this doc is rejected by agent-6.

---

## 1. Target

> **`y = 1`** iff the object key is re-read within **one hour** of the
> admission decision that is being scored. Otherwise `y = 0`.

"Object key" is exactly the content-addressed key used by `shelfd`:
`sha256(etag || rg_ordinal || offset || length)` for row groups, and the
analogous `(etag, offset, length)` triple for footer / manifest ranges.
**The label is defined on keys, not on files.** A dashboard that re-reads
row group 2 of a 5 GB Parquet file but never touches row group 3 provides
exactly one positive label (key for rg 2) and zero positive labels for
rg 3 — this is the whole point of row-group granularity (BLUEPRINT §7.1).

"Re-read" means any subsequent `GET /cache/<key>/<offset>-<len>` request,
*regardless* of whether shelfd served it from DRAM, NVMe, or fell through
to S3. We label what the workload actually asked for, not what the cache
happened to contain.

Positive class is the minority (dashboards re-hit the same row-group within
an hour; ad-hoc scans, almost by definition, do not). See §5 for why this
drives the choice of AUC-PR over AUC-ROC.

## 2. Prediction horizon

**1 hour.** Matches `P(reaccess_within_1h)` in BLUEPRINT §7.3 and the
`admission.size_threshold` fallback semantics in §6.3. Chosen because:

- Dashboard refresh cadences are tens of minutes to an hour; anything
  shorter misses the dominant positive signal.
- An hour is comfortably inside the NVMe tier's typical retention window
  (7-30 d), so a "useful" admission is one that actually earns a hit in a
  human timeframe, not one that survives forever hoping.
- It matches the canary observation window's lower bound (Agent 6 Pass 4,
  24 h) — long enough to be signal, short enough that the canary can
  distinguish a hit from a stale entry.

The horizon is **not** a tunable in v1. If we ever change it we issue a
new ADR, a new feature-order tag in `contracts/admission-model.md`, and a
new model version — not a silent training-config flip.

## 3. Split strategy

**Time-based, three-way, never random.**

Given a training window `[t0, t3)`:

| Split | Window              | Notes                                                                                 |
|-------|---------------------|---------------------------------------------------------------------------------------|
| Train | `[t0, t1)`          | Default length 21 days.                                                               |
| Valid | `[t1 + H, t2)`      | `H` = prediction horizon = 1 h. Gap avoids positive-leakage across the t1 boundary.   |
| Test  | `[t2, t3)`          | Default length 7 days. The AUC-PR we report is computed here.                         |

Random split is forbidden. A key observed on day 3 can trivially leak its
future re-access into a "training" row if the model also sees day 4 rows
sorted next to it; any CV routine that shuffles destroys the label's
meaning. The `TimeSplit` dataclass in `shelf_trainer.labels` refuses to
construct if `valid_start - train_end < horizon`.

Weekly retrain cadence. We retrain on a rolling `[t3 - 35d, t3)` window so
every production model has seen ≥ 30 days of traffic minus the
horizon-gap. E5 (`shelf/agents/out/03-plan.md` §2) established that
dashboard cohort stationarity is week-over-week stable, so weekly is
enough — we do not need the nightly cadence the blueprint originally
implied.

## 4. Leakage controls

The paranoia rule (Agent 6 Role §Paranoid):
> If "future access" looks too easy to predict, the features probably
> contain the label.

### 4.1 Feature admissibility

For each feature in BLUEPRINT §7.3 (canonical order lives in
`shelf_trainer.features.FEATURE_ORDER`), this table records whether it is
computed from events strictly before `decision_ts` (allowed) or
touches/could touch `≥ decision_ts` (forbidden).

| # | Feature                   | Allowed source window             | Verdict   | Note                                                                                     |
|---|---------------------------|-----------------------------------|-----------|------------------------------------------------------------------------------------------|
| 0 | `table_tf_7d`             | `[decision_ts - 7d, decision_ts)` | Allowed   | Access counts, strictly past.                                                            |
| 1 | `table_tu_7d`             | `[decision_ts - 7d, decision_ts)` | Allowed   | Distinct-user count, strictly past.                                                      |
| 2 | `partition_depth`         | static table metadata             | Allowed   | Schema-level, no temporal leakage.                                                       |
| 3 | `user_type`               | directory / role, stable          | Allowed   | Comes from the requester, known at decision time.                                        |
| 4 | `size_mb`                 | Iceberg manifest                  | Allowed   | Known at decision time (we're about to fetch it).                                        |
| 5 | `hour_of_day`             | `decision_ts`                     | Allowed   | Not label-correlated except via workload seasonality, which is fine.                     |
| 6 | `recency_days`            | `[−∞, decision_ts)`               | Allowed   | "Days since last partition read"; **must not** be computed with the current read counted.|
| 7 | `query_cost_rank`         | historical per-tenant percentile  | Allowed   | Rank from `[decision_ts - 30d, decision_ts)`, never including the live query.            |
| 8 | `file_is_recent`          | object `last_modified` from S3    | Allowed   | Writer-time, not reader-time.                                                            |
| 9 | `file_is_on_pin_list`     | published `pin_list.json`         | Allowed   | We use the pin list **as of** `decision_ts`, not tomorrow's.                             |

Forbidden explicitly:

- Any feature using `QueryCompletedEvent.operatorSummaries` from the
  current query. That is post-hoc: at admission time the query has not
  completed.
- Anything derived from `shelf_hits_total` / `shelf_misses_total` on the
  current key. That is the label.
- `snapshot_id` later than `decision_ts`.
- Row-group access counts inside the decision's own query (we score the
  whole query as one decision batch).

### 4.2 Runtime sanity gate

`shelf_trainer.labels.assert_no_leakage` (stub: SHELF-36) computes
per-feature point-biserial correlation with the binary label. If any
single feature clears `|r| ≥ 0.98`, we refuse the frame. An honest
feature is almost never that predictive on its own; a feature that hits
0.98 is nearly always the label in disguise.

### 4.3 Reproducibility hash

Every training frame is Parquet-materialised under
`clients/python/trainer/data/<date>/` with a sha256 over `(as_of, window,
feature_config, source_query_plan_hash)`. The hash is recorded in
`admission_v<N>.meta.json` so a promoted model can always be re-derived.

## 5. Metric hierarchy

Primary:

- **AUC-PR** on the temporal test split (§3).
  Positive class is rare; AUC-ROC would be misleading (a trivial "admit
  all" scores high). AUC-PR is the right ranking metric for the admission
  threshold shelfd actually applies (`P > 0.3`, BLUEPRINT §7.3).

Guardrails (any one failing blocks promotion even if primary improves):

- **Calibration slope** on 20 equal-frequency reliability bins. Must lie
  in `[0.8, 1.2]`. Shelfd compares `P` against a fixed threshold; a
  miscalibrated model shifts the admit rate silently.
- **Coverage** on the large-miss decision set. Must be ≥ 0.95 — the
  candidate has to actually produce a score for ≥ 95 % of eligible
  decisions, not silently fall back to size-threshold on the remainder.
- **Replay hit-rate lift** on a 7-day replay of
  `cdp.trino_logs.trino_queries` compared to size-threshold alone. Must be
  ≥ 5 pp (ADR-0003 escape-hatch threshold). Computed offline by the
  benchmarker (agent 7), not the trainer — we publish the replay delta
  we believe and agent 7 reproduces it on the gate harness.
- **p99 inference latency** on the Rust `lightgbm3` scorer on a
  Graviton3 pod. Must be < 50 µs (ADR-0003). Measured by the
  benchmarker; the trainer only records the offline-Python number as a
  hint.

If any guardrail fails, promotion is refused; the candidate artifact
stays under `admission/candidate/` with an `.audit.json` explaining
which guardrail blocked it.

---

## References

- BLUEPRINT §6.3 (control plane), §7.3 (learned admission on large
  scans).
- ADR-0003 — size-threshold + pin-list in v1; LightGBM is the v1.x
  escape hatch; **no ONNX ever**.
- Plan §2 E4 — ONNX / LightGBM latency experiment (rationale for the
  50 µs guardrail).
- Plan §4 Phase 4 tickets — SHELF-29 through SHELF-51.
