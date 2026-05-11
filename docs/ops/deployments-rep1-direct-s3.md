# Replica-1: temporary direct S3 gate (Fix 1)

When shelf LODC submit-queue overflow is **> 20 %** sustained, the shim adds latency without NVMe relief — route **rep-1** `cdp` catalog back to direct S3 until gates pass.

## Change

In `deployments-repo` branch `cicd-v2`, edit  
`values-files/data-platform-cluster/trino-replica-1-values.yaml`:

```properties
# Inside cdp.properties block:
s3.endpoint=https://s3.ap-south-1.amazonaws.com
```

(Use your region’s public endpoint.)

## Re-enable shelf on rep-1 when

1. **Fix 0** applied (LODC tuning + restart), **and**
2. `rate(shelf_lodc_drops_total{reason="submit_queue_overflow"}[5m])` **< 20 %** of miss rate on rep-2 (or canary replica) for **≥ 2 h**, **and**
3. **Frequency admission** (`policy: frequency`) deployed if part of your rollout.

## MR hygiene

- Single-property catalog edits reconcile quickly; document rollback as the inverse one-liner.
- Do not add inline YAML comments for per-field rationale — put analysis in the MR body.
