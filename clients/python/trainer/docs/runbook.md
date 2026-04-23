# Shelf trainer runbook

_Status: **stub** — section headers fixed; body filled in as each alert
ships with its first on-call touch. Owned by agent-8 (operator)._

This runbook is what the on-call engineer reads at 03:00 when the Shelf
trainer pages. Every alert name below is the Prometheus / Airflow alert
identifier that fires; if the alert name on your pager does not appear
here, something has been renamed — file a ticket against the operator
agent.

## 0. Before you touch anything

TODO (SHELF-53): traffic-light check per `AGENTS.md`'s three-click
diagnosis convention. Link to `shelf-overview` Grafana dashboard and
the `shelf-v05-gate` dashboard.

## 1. Alert: `shelf_trainer_dag_failed`

Airflow DAG `shelf-trainer` failed at a task boundary.

TODO (SHELF-54): per-task triage:

- `extract` failure — usually Trino auth / `cdp.trino_logs` schema drift.
- `train` failure — skip for v1 (stub, should never fire).
- `promote` failure — guardrails refused, or S3 write permission lost.

## 2. Alert: `shelf_trainer_guardrail_blocked_promotion`

The nightly run produced a candidate but promotion was refused.

TODO (SHELF-55): copy the `audit.json` sidecar from
`s3://<config-bucket>/shelf/admission/candidate/<version>/` and file a
link against the ML channel. **Do not** `--force` promote without a
paired eng-lead ack.

## 3. Alert: `shelf_trainer_pin_list_empty`

`pin_list.json` published with zero entries.

TODO (SHELF-56): known cause — `cdp.trino_logs.trino_queries` schema
change or 30d window had no data. Fall back to previous known-good
`pin_list.json` via S3 versioning (R-11 in `shelf/agents/out/03-plan.md`
§5).

## 4. Alert: `shelf_trainer_feature_drift`

PSI on any single feature exceeded 0.2 between the last two training
frames. V1.x only (requires a live LightGBM model).

TODO (SHELF-57): per-feature triage table.

## 5. Alert: `shelf_trainer_replay_hit_rate_regressed`

Nightly replay delta dropped below the last promoted model.

TODO (SHELF-58): this is a serious signal, not a flake. Do not silence.

## 6. Rollback: admission model

TODO (SHELF-59): `shelf-trainer rollback <prior_version>` and the
expected `/stats` admit-rate curve on shelfd pods.

## 7. Rollback: pin list

TODO (SHELF-60): S3 object-version revert of `pin_list.json` +
`shelfctl reload pin-list` on each pod.

## 8. Data backfill

TODO (SHELF-61): re-running the trainer for a specific historical date.
