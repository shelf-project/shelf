# Runbook: pin-table

**Scenario:** Add a table (or partition) to the pin list so Shelf
resists evicting its files under bulk scan pressure.

## Symptom

- A user reports that a known-hot dashboard table is not consistently
  hitting (hit rate < 80% for that table on `shelf-tenant`).
- Ad-hoc scans have been evicting dashboard row-groups from DRAM
  (symptom of ADR-0008's two-pool trade-off).
- Phase-6 onboarding of a new tenant includes a pre-populated pin list.

## Impact

Pinned objects bypass the size-threshold admission (ADR-0003). They are
also exempt from eviction. Over-pinning eats NVMe capacity — always
check `shelfctl stats --pinned-bytes` before adding.

## Diagnosis

```bash
# 1. Current pin list.
aws s3 cp s3://penpencil-shelf-prod-config/pin_list.json - | jq .

# 2. How much NVMe is already pinned?
kubectl -n shelf exec shelf-0 -- shelfctl stats --pinned-bytes

# 3. What's the working set of the table in question? (from trainer DB)
# Replace catalog/table accordingly.
echo "SELECT sum(scanned_bytes) FROM cdp.trino_logs.trino_queries WHERE query LIKE '%silver_offline_event%' AND create_time > now() - INTERVAL '7' DAY" \
  | trino --catalog cdp
```

## Mitigation

1. **Propose via PR** against `shelf-config` repo's `pin_list.json`.
   Keep it small — pinned bytes per pod should stay under 20% of NVMe
   per `docs/capacity.md`. Example entry:
   ```json
   {
     "pins": [
       {
         "table": "cdp.icesheet.silver_offline_event_data_2026",
         "partitions": ["event_region='MP+CG'"],
         "max_bytes_per_pod": 21474836480
       }
     ]
   }
   ```
2. **Roll out via the normal config-reload path:** after the PR merges,
   the trainer job picks up the diff and writes to S3 on the next
   cycle (or trigger it manually). `shelfctl reload pin-list` causes
   all pods to re-read within 5 s.
3. **Verify.** `shelfctl stats --pinned-bytes` rises by roughly
   `max_bytes_per_pod × replicas / replicas = max_bytes_per_pod`;
   the table's hit-rate panel on `shelf-tenant` climbs toward 100%
   within 1 hour of steady traffic.

## Escalation

- Routine: no escalation. PR + merge.
- Emergency pin (critical dashboard about to go dark): on-call may
  bypass the PR and hand-edit `pin_list.json` in S3 via `aws s3 cp`,
  then open a retrospective PR.

## Post-incident actions

- [ ] PR merged, trainer scheduled to re-evaluate in its next cycle.
- [ ] Confirm pinned bytes rose by expected amount.
- [ ] If emergency-edited, ensure the follow-up PR lands within 24 h.
