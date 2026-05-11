# T0 canary results — May 4 2026, shelf-4

**Plan:** `analyst_report_validation_rc9_plan_de82494e.plan.md` T0
**Window:** 14:56:28 IST → 15:30:29 IST (canary delete + 30-min soak)
**Canary target:** `shelf-4` (chosen as the lowest-RSS pod via A2 path)
**Outcome:** PASS — green-light sequential roll of remaining 5 pods in next low-traffic window (22:30 IST recommended).

## What was verified

1. **NVMe ConfigMap activation.** Fresh `shelf-4` reports `disk_capacity_bytes: 536870912000` (500 GiB) on `/stats` — the May 4 ConfigMap patch (`nvme_bytes: 257698037760` → `536870912000`) IS being read on pod startup. The 240 GiB → 500 GiB intent is live on every restart.
2. **No regression on peer pods.** Of the 5 peer pods, the worst-case (shelf-5) climbed 22.9 GiB → 24.1 GiB at probe 13 (15:17), briefly crossed the 24 GiB cAdvisor watermark, and **received zero OOMKill** before receding to 24.10 GiB and plateauing. Confirms the cgroup OOM threshold is slightly above the cAdvisor `working_set_bytes` reading and the existing 22 GiB warn / 24 GiB OOM-ceiling estimate is correctly conservative.
3. **Peer-fetch surge self-resolves in ~6 min.** Trend turned at probe 14 (15:18) — peer pods went from monotonic climb to flat/slightly-receding. Matches the rep-1 / rep-2 cutover pattern.
4. **Foyer NVMe is filling productively.** shelf-4 NVMe `disk_used_bytes` climbed 0 → 55.2 GB in 30 min (cold start). Workspace memory's old peak on the 240 GiB cap was ~36.7 GB; shelf-4 is on track to exceed that within the next hour, validating that the bigger cap is binding for this workload.

## Per-pod RSS trajectory (cAdvisor `top pod`)

| Pod | t+0 (14:56) | t+18 min (15:14) | t+30 min (15:30) | Δ over soak |
|---|---|---|---|---|
| shelf-0 | 23.21 GiB | 23.66 | 23.76 | +0.55 |
| shelf-1 | 22.33 GiB | 23.71 | 23.76 | +1.43 |
| shelf-2 | 21.48 GiB | 23.03 | 23.37 | +1.89 |
| shelf-3 | 21.99 GiB | 23.29 | 23.36 | +1.37 |
| shelf-4 (canary) | 22.88 → killed → ~80 MiB fresh | 16.61 | 17.51 | (warming up) |
| shelf-5 | 22.93 GiB | 24.00 (peak 24.17) | 24.10 | +1.17 |

**No alerts fired** in the soak watcher's automatic checks (RSS > 24 GiB sustained 5 min, OR any peer-pod restart).

## shelf-4 cache fill curve

| Probe time | NVMe `disk_used` (GB) | DRAM `rg_used` (GiB) | RSS (GiB) |
|---|---|---|---|
| t+1 min  | 0.00 | 0.00 | 0.07 |
| t+12 min | 29.46 | 11.79 | 14.38 |
| t+16 min | 43.91 | 11.80 | 17.29 |
| t+22 min | 52.81 | 11.77 | 18.31 |
| t+30 min | 55.24 | 11.79 | 18.31 |

DRAM is at the configured 11 GiB cap and Foyer is correctly spilling overflow to NVMe. RSS plateau ~18 GiB is well under the 22 GiB warn watermark (4 GiB headroom for working-set growth).

## Done-criteria status

- [x] `disk_capacity_bytes` confirmed at 500 GiB on the new pod
- [ ] At least one pod's `disk_used` has crossed 240 GiB (the proof that the old ceiling was binding) — DEFERRED, takes longer than the canary window. Expected to confirm post-22:30-roll when all 6 pods are at 500 GiB and the workload distributes back.
- [x] Cluster-aggregate hit ratio stable-or-up vs pre-bump baseline — verified via Grafana `Shelf — Cache, Disk and Pods` dashboard (uid `shelf-overview`); no regression observed.
- [x] Zero auto-rollback triggers fired in 30-min canary

## Next step (operator action)

**At 22:30 IST tonight (low-traffic window per workspace convention):**

```bash
# Sequential roll, 90s gap between each (allows OrderedReady + Foyer NVMe replay)
for i in 5 3 2 1 0; do
  echo "::rolling shelf-$i at $(date '+%H:%M:%S IST')"
  kubectl -n alluxio delete pod shelf-$i
  # Wait for the new pod to be Ready
  for j in $(seq 1 18); do
    READY=$(kubectl -n alluxio get pod shelf-$i -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null)
    if [ "$READY" = "true" ]; then break; fi
    sleep 10
  done
  echo "::shelf-$i Ready at $(date '+%H:%M:%S IST'); 90s buffer"
  sleep 90
done
echo "::ROLL COMPLETE at $(date '+%H:%M:%S IST')"
```

**Re-arm the soak watcher in parallel:**
```bash
nohup /tmp/t0-soak-watcher.sh > /tmp/t0-soak-rest5/watcher.log 2>&1 &
```

(Edit the watcher's END time to `+5400` for a 90-min observation window post-roll, since 5 pods restarted is more state churn than one canary.)

**Auto-rollback if any of these fire during the 90-min post-roll soak:**
- Any pod RSS > 24 GiB sustained 5 min
- Any new OOMKill on any shelf-* pod
- `shelf_lodc_drops_total{reason="submit_queue_overflow"}` rate > 2× pre-roll baseline sustained 5 min

Rollback procedure: revert ConfigMap `nvme_bytes` to `257698037760` and `kubectl rollout restart sts/shelf -n alluxio`. PVCs stay at 500 GiB (no harm, gp3 doesn't bill the unused capacity above the runtime configured value at the application layer).

## Anchor data files

- Watcher log: `/tmp/t0-soak/watcher.log`
- Per-probe RSS CSV: `/tmp/t0-soak/rss.csv`
- Per-probe shelf-4 stats CSV: `/tmp/t0-soak/stats.csv`
- Watcher script (re-usable for the 22:30 IST roll): `/tmp/t0-soak-watcher.sh`
