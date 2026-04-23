# Runbook: unpin-table

**Scenario:** Remove a table (or partition) from the pin list.

## Symptom

- NVMe usage is above 85% and `pinned_bytes` is > 20% of capacity
  (from `shelf-nvme-usage-high.md`).
- A table was rolled off the product (partitions archived) but is
  still in the pin list.
- A pinned entry has been flagged as low-value by the trainer
  (next-cycle diff proposes removal).

## Impact

Unpinned bytes become eligible for Foyer's S3-FIFO eviction. If the
table is still warm, some of its objects stay resident due to SIEVE
frequency counters; the rest evict. First-read-after-unpin misses can
cause a brief re-warm.

## Diagnosis

```bash
# 1. What is currently pinned?
aws s3 cp s3://penpencil-shelf-prod-config/pin_list.json - | jq '.pins[].table'

# 2. How large is the entry we'd remove?
kubectl -n shelf exec shelf-0 -- shelfctl stats --pin cdp.icesheet.silver_offline_event_data_2026

# 3. Is the table still receiving queries? Avoid an unpin-then-repin cycle.
echo "SELECT count(*) FROM cdp.trino_logs.trino_queries WHERE query LIKE '%silver_offline_event%' AND create_time > now() - INTERVAL '24' HOUR" \
  | trino --catalog cdp
```

## Mitigation

1. **Propose the diff** in `shelf-config` PR (same flow as
   `pin-table.md` — JSON schema enforced by the PR CI).
2. **Dry-run in staging** (optional but recommended if the entry is
   large): `helm upgrade ... -f values-staging.yaml` with the staging
   pin list version bumped; observe hit rate for 1 h.
3. **Reload prod.** Either `shelfctl reload pin-list` or wait for the
   15-minute periodic reload. `pinned_bytes` drops; Foyer eviction
   gradually reclaims the freed quota as cold keys age out.

## Escalation

- Routine: no escalation.
- If the unpinned table's hit rate collapses (< 40% for 15 min after
  unpin), roll back the pin-list change — `aws s3api copy-object` to
  restore the previous S3 object version.

## Post-incident actions

- [ ] PR merged; pin list diff reviewed and recorded.
- [ ] Verify `pinned_bytes` dropped by the expected amount.
- [ ] If the unpin caused a hit-rate dip, note in the trainer log and
      revise the pin-list trainer's heuristic.
