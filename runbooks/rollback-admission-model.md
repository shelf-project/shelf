# Runbook: rollback-admission-model

**Scenario:** Revert to the previous admission model (or disable
learned admission entirely).

## Symptom

After a recent admission-model promotion:
- NVMe admit rate spiked anomalously.
- Hit rate dropped.
- `shelf-trainer` dashboard shows canary-vs-prod disagreement > 10%.
- A poisoned training-data regression was caught post-promotion.

## Impact

Rollback to a known-good model (or disabling the model entirely,
ADR-0003) returns admission decisions to a well-understood policy.
Expected recovery: hit-rate within 1 hour of warmup.

## Diagnosis

```bash
# 1. Confirm the bad promotion. Last N versions of the S3 object.
aws s3api list-object-versions \
  --bucket example-shelf-prod-config \
  --prefix admission-model.onnx \
  | jq '.Versions[:5] | .[] | {VersionId, LastModified, IsLatest}'

# 2. Verify the current model timestamp matches what shelfd reports.
kubectl -n shelf exec shelf-0 -- shelfctl stats --admission | grep promoted

# 3. Double-check the in-flight trainer isn't about to overwrite.
curl -s https://airflow.example.internal/api/v1/dags/shelf_admission_model_trainer/dagRuns?state=running | jq '.dag_runs'
```

## Mitigation

1. **Fast-roll-back via S3 version pinning.** Copy the previous
   VersionId over the current latest:
   ```bash
   aws s3api copy-object \
     --bucket example-shelf-prod-config \
     --copy-source "example-shelf-prod-config/admission-model.onnx?versionId=<GOOD_VERSION>" \
     --key admission-model.onnx
   kubectl -n shelf exec shelf-0 -- shelfctl reload admission-model
   # fan-out to all pods
   for p in $(kubectl -n shelf get pod -l app.kubernetes.io/name=shelf -o name); do
     kubectl -n shelf exec $p -c shelfd -- shelfctl reload admission-model || true
   done
   ```
2. **Disable the model entirely** (the v1 default):
   ```bash
   helm upgrade shelf charts/shelf -n shelf \
     -f charts/shelf/values-prod.yaml \
     --set cache.admission.model.enabled=false
   ```
   Cache reverts to size-threshold-only admission per ADR-0003.
3. **Pause the trainer DAG** in Airflow so it doesn't re-overwrite
   before the investigation concludes.

## Escalation

- Page ML/data-eng owner immediately.
- If the rollback does not recover hit rate within 1 h, escalate to
  eng-lead and consider treating as an `ShelfHitRateTooLow` incident.

## Post-incident actions

- [ ] Record the rollback in the incident ticket, including
      VersionId of the good model.
- [ ] Add a canary gate to the trainer so future promotions require
      `canary_disagreement_rate < 5%` before shipping.
- [ ] Verify the `ShelfAdmissionModelStale` alert is silenced while the
      model is disabled (it has an `and ... enabled == 1` guard).
