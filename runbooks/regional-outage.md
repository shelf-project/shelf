# Runbook: regional-outage

**Scenario:** S3 in our region is impaired — either the API is
returning elevated 503s or an AZ event has knocked out the origin
bucket access path.

## Symptom

- CloudWatch: S3 `5xxErrors` > 0 sustained, or `RequestLatency` > 1s p95.
- Shelf: miss rate spikes (expected); fall-through to S3 also fails.
- Users: queries start returning `GENERIC_INTERNAL_ERROR` from Trino,
  which is the real S3 error surfacing — Shelf's plugin never masks it.
- Paired AWS Health Dashboard notification for S3 in the region.

## Impact

The last line of defence (direct S3) has failed. Trino worker reads
error out. Shelf cannot serve keys it doesn't already have. In this
state, Shelf's value is exactly the keys already in its NVMe / DRAM —
**a hot cache is a stability story, not just a performance one.**

## Diagnosis

```bash
# 1. Is S3 actually down, or just slow?
time aws s3api head-bucket --bucket penpencil-cdp-prod
aws cloudwatch get-metric-statistics --region ap-south-1 --namespace AWS/S3 \
  --metric-name 5xxErrors --dimensions Name=BucketName,Value=penpencil-cdp-prod \
  Name=FilterId,Value=EntireBucket --start-time $(date -u -d '15 min ago' +%FT%T) \
  --end-time $(date -u +%FT%T) --period 60 --statistics Sum

# 2. Is only Trino affected, or every engine?
kubectl -n shelf exec shelf-0 -- shelfctl stats --origin | grep -E 'errors|latency'

# 3. Is there AWS Health guidance?
# Visit the AWS Health Dashboard or check the SSM OpsCenter entries.
aws health describe-events --region us-east-1 --filter services=S3 2>/dev/null || true
```

## Mitigation

1. **Lean on the cache.** Do nothing about Shelf. It will serve every
   already-hot key. Only misses fail, and that is the S3 outage, not
   Shelf. Communicate to stakeholders: "Dashboard queries are served
   from Shelf; long-tail ad-hoc queries may fail until S3 recovers."
2. **Degrade gracefully: disable plugin fall-through temporarily.**
   If S3 returns for-seconds-then-gone, the plugin's fall-through
   causes queries to fail on misses. A short-term config to short-fail
   the miss path keeps the cached slice healthy:
   ```bash
   # Via Trino catalog override (ArgoCD)
   fs.shelf.fail-closed-on-miss = true
   ```
   (This is the inverse of BLUEPRINT §9.5 — only use during a known
   outage.)
3. **If Shelf is serving enough of the workload**, DO NOT fall back to
   Alluxio — Alluxio hits the same impaired S3. If some reads must
   succeed and the data is cross-region replicated, flip the Trino
   catalog to the replicated bucket. Coordinate with platform.

## Escalation

- Page primary + secondary on-call + eng-lead immediately.
- Open an AWS support case for the S3 impairment if AWS Health has
  not already acknowledged.
- Incident commander from SRE.

## Post-incident actions

- [ ] Record hit-rate timeline during the outage in the incident
      ticket. Shelf's cached slice is the narrative: "Shelf absorbed
      X% of reads while S3 was down."
- [ ] Re-run `docs/capacity.md` working-set math vs observed hit rate
      during the outage — often the real working set is smaller than
      planned, in our favour.
- [ ] Update `regional-outage.md` with anything the incident taught us.
