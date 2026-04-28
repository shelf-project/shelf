# Runbook: ShelfHitRateTooLow

**Alert:** `ShelfHitRateTooLow`
**Severity:** page
**Dashboard:** https://grafana.example.internal/d/shelf-overview

## Symptom

Cumulative Shelf hit rate has dropped below 60% for 10 minutes.
(Target from plan §6.4: ≥ 71%; rollback threshold: < 60% for 24h.)

## Impact

Users are not seeing errors — the plugin is falling through to S3 per
BLUEPRINT §9.5. But:

- Dashboard query p95 degrades toward raw-S3 latency.
- S3 cost rises (GET + data egress).
- If the v0.5 gate observation window is active (plan §3 Phase 1), this
  alert at 24h continuous = **kill-switch triggered** per ADR-0010.

## Diagnosis

Run, in this order:

```bash
# 1. Is this a hot-key storm or genuine cache ineffectiveness?
kubectl -n shelf exec shelf-0 -- shelfctl stats --granularity=row-group | head -40

# 2. Which pool is missing? (metadata pool missing = config drift; rowgroup missing = admission refusing)
kubectl -n shelf exec shelf-0 -- shelfctl stats --by-pool

# 3. Has someone recently shipped a plugin version, pin-list rev, or scaled the StatefulSet?
kubectl -n shelf get events --sort-by=.lastTimestamp | tail -30
```

## Mitigation

Three actions, safest first:

1. **Warm the metadata pool.** Manifest + footer misses cascade into
   row-group misses. If `pool.metadata` hit rate is < 95%, run
   `shelfctl reload pin-list` — a recent trainer promotion may have
   shed pins that hold metadata.
2. **Scale up** one pod. If the ring is < 80% NVMe headroom across the
   set, see `scale-up.md`. HRW rebalance spreads hot keys to the new
   pod within ~30s.
3. **Flip to Alluxio hot-standby.** Edit the Trino catalog property
   `fs.shelf.enabled=false` via ArgoCD. Plugin fails through to S3 for
   ongoing queries; new queries take the Alluxio path. See
   `regional-outage.md` for the full fail-back sequence.

## Escalation

- **Now (≤ 15m in):** primary on-call (see `docs/oncall.md`).
- **30m in, no recovery:** page secondary on-call + eng-lead.
- **If v0.5 gate window is active and alert held > 1h:** open an ADR
  supersession PR against `agents/out/adr/0010-v05-gate-beat-alluxio-on-rep2.md`
  and notify eng-leadership.

## Post-incident actions

- [ ] Record incident in `docs/incidents/YYYY-MM-DD-shelf-hit-rate.md`.
- [ ] If the root cause was a pin-list regression: add a golden-query
      smoke test to the trainer canary.
- [ ] If the root cause was NVMe exhaustion: update `docs/capacity.md`
      with the observed working-set size.
- [ ] If the root cause was plugin-version drift: tighten the
      ArgoCD-drift alert (risk R-17 in plan §5).
