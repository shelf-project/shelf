# Runbook: ShelfAdmissionModelStale

**Alert:** `ShelfAdmissionModelStale`
**Severity:** warn
**Dashboard:** https://grafana.example.internal/d/shelf-trainer

## Symptom

The admission model (LightGBM / ONNX) has not been promoted in more
than 48 hours. In v1 this alert is inert by construction — ADR-0003
ships size-threshold admission only. The alert guards the Phase-4
escape hatch in the event we ever enable a learned model.

## Impact

If the model is enabled but stale, admission decisions drift from the
current workload. Symptoms include NVMe-fill deltas, hit-rate erosion,
and occasional scan-storm false-admits. Users see no error.

## Diagnosis

```bash
# 1. Is the model actually enabled? (v1 default: no)
kubectl -n shelf exec shelf-0 -- shelfctl stats --admission | grep -E 'enabled|promoted'

# 2. What does the Airflow DAG say?
# Replace DAG URL with the example Airflow for your env.
curl -s https://airflow.example.internal/api/v1/dags/shelf_admission_model_trainer/dagRuns \
  | jq '.dag_runs[-5:] | .[] | {run_id, state, start_date, end_date}'

# 3. Is the S3 object present, and what is its LastModified?
aws s3 ls s3://example-shelf-prod-config/admission-model.onnx --human-readable
```

## Mitigation

1. **Kick the trainer DAG.** Trigger `shelf_admission_model_trainer` in
   Airflow. If it succeeds, the next periodic reload (15 min) picks it
   up. `shelfctl reload admission-model` short-circuits the wait.
2. **If the trainer is genuinely broken**, disable the model:
   `helm upgrade ... --set cache.admission.model.enabled=false`. The
   cache reverts to size-threshold-only (ADR-0003) and this alert
   stops firing.
3. **Roll back to the previous model** (see
   `rollback-admission-model.md`) if the most recent promotion was
   what broke the trainer.

## Escalation

- `warn` severity → Slack only on first fire.
- If stale for > 7 days, escalate to the ML/data-eng owner of the DAG.

## Post-incident actions

- [ ] If the DAG failed because of `cdp.trino_logs.trino_queries`
      schema drift (risk R-13), tag the schema owner and snapshot the
      schema.
- [ ] Verify the stale model didn't produce a runtime regression (hit
      rate, NVMe admit bytes) via `shelf-trainer` dashboard.
