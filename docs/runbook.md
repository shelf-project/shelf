# Shelf v0.5 gate runbook

SHELF-28 deliverable. Covers the v0.5 gate decision path — *is Shelf
ready to take rep-2 traffic in place of Alluxio?* — and the kill-switch.
Once v0.5 is declared green, ops switches to `[oncall.md](./oncall.md)`
and `[SLO.md](./SLO.md)` for steady-state operation.

> **Target reader.** An operator with zero prior Shelf context should
> be able to execute this runbook end-to-end in ≤ 30 minutes.

---

## 0. Pre-flight — what you need before you start


| Prereq                                               | Where                                                                    |
| ---------------------------------------------------- | ------------------------------------------------------------------------ |
| `kubectl` context pointed at the rep-2 Trino cluster | `kubectl config current-context` must print `rep-2`                      |
| 3-pod Shelf StatefulSet live in `shelf-staging`      | SHELF-21 rollout. See `[cluster-handoff.md](./cluster-handoff.md)`       |
| Grafana dashboard `shelf-read-path`                  | `charts/shelf/grafana/dashboards/shelf-read-path.json`                   |
| Alluxio baseline numbers                             | E12 output in `agents/out/experiments/E12-alluxio-baseline.md`           |
| `shelfctl` on PATH                                   | `cargo build --release -p shelfctl && cp target/release/shelfctl ~/bin/` |


The rest of this runbook assumes those are in place. If any is missing,
stop here and route back to `cluster-handoff.md`.

---

## 1. The v0.5 gate — five green criteria

The gate is a **7-day rolling observation window** on rep-2 after Shelf
takes shadow traffic. If any single criterion misses for any single day
inside the window, the window resets and the team has 48 h to close
the gap or trigger the kill-switch (§4).


| #   | Criterion                                        | Source                                                                                      | Target                                                            |
| --- | ------------------------------------------------ | ------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| 1   | Cumulative hit rate (all pools combined)         | Prom `shelf_hits_total / (shelf_hits_total + shelf_misses_total)` — "hit ratio" panel row 1 | ≥ **71 %** for 7 consecutive days (the Alluxio baseline from E12) |
| 2   | `GOLD_DBT` ok-rate                               | Airflow DAG SLA from the dbt job catalog                                                    | ≥ **99.9 %**                                                      |
| 3   | p95 query latency                                | Trino `QueryCompletedEvent` p95, 7-day window                                               | ≤ **120 %** of Alluxio baseline                                   |
| 4   | Shelf-attributed pages                           | PagerDuty filter `service=shelf`                                                            | **0** in 7 days                                                   |
| 5   | Oncall surface (pages + runbook lookups + Slack) | Manual count in a weekly log                                                                | ≤ **50 %** of Alluxio's 7-day rolling rate                        |


These are the same numbers called out in plan §6.4. The dashboard panel
**Hit ratio overall** (top-left big number) answers #1 at a glance;
**p99 latency** (top-right) is a leading indicator for #3.

### How to evaluate — 3-click path

1. Open the `shelf-read-path` dashboard, set range = `now-7d` to `now`.
2. Read the four big numbers. If any one is red, skip to §4 (kill-switch).
3. Cross-check #2 / #4 / #5 in their respective consoles
  (Airflow / PagerDuty / the Slack oncall log). If all five are green
   for 7 consecutive days, **declare v0.5 green** (§3).

---

## 2. Weekly chaos drills (required while the gate is open)

Two drills run every Monday 10:00 (staging) and every Thursday 10:00
(rep-2) while the observation window is open. Both are scripted and
must pass. Failures rewind the observation window by 24 h.


| Drill                         | Target                                                                                               | `make` target              | Runtime |
| ----------------------------- | ---------------------------------------------------------------------------------------------------- | -------------------------- | ------- |
| KEDA rotation                 | 50 % of Trino workers + 1 Shelf pod; 10 canonical queries run to completion                          | `make chaos-keda-rotation` | ~10 min |
| Pod-kill on busiest Shelf pod | Dashboard traffic continues; cumulative hit rate ≥ 80 % of Alluxio baseline during the 10-min window | `make chaos-pod-kill`      | ~12 min |


Both drills ship with a **green-in-CI smoke variant** that runs against
the `benchmarks/smoke/` docker-compose harness — no cluster required —
so drift in the assertion scripts themselves is caught on every PR:

- `make chaos-keda-rotation-smoke`
- `make chaos-pod-kill-smoke`

The smoke variants exit 0 in under 60 s on a laptop and are wired into
the `smoke.yml` workflow. **They do not prove the cluster-side
invariant**; they prove the drill scripts still parse, the expected
metrics still exist, and the threshold math is still correct.

---

## 3. Declaring v0.5 green

All five gate criteria green for 7 consecutive days + both chaos drills
green for the last 2 runs. When that lines up:

1. Tag the release: `git tag -a v0.5 -m 'Shelf v0.5 gate passed on rep-2'`.
2. File a `shelf` ticket titled `v0.5: rep-2 production on Shelf`
  summarising: 7-day hit rate, 7-day p95, page count, oncall surface
   delta. Attach the four-big-numbers screenshot.
3. Move Alluxio on rep-2 from primary to hot-standby (do **not**
  decommission — that's Phase 5 / SHELF-retire-alluxio). The flip is
   a Trino config change, not a Shelf change.
4. Page a note in `#shelf` linking the green-gate ticket. Update
  `cluster-handoff.md` status header.
5. Rotate on-call playbook from this file to `[oncall.md](./oncall.md)`.

---

## 4. The kill-switch — when to stop Shelf

The v0.5 gate is a feature, not a fear. If Shelf cannot beat stabilised
Alluxio, we kill the project **on purpose** rather than letting it die
slowly. Triggers:

- Hit-rate criterion #1 misses for two consecutive observation windows
(14 days total).
- Any **Shelf-attributed page** in the 7-day observation window.
- `GOLD_DBT` ok-rate < 99.9 % for any single day.
- p95 query latency > 120 % of Alluxio baseline for any 24-hour window
that the team cannot diagnose + close inside 72 h.
- Two consecutive chaos drills fail with the same root cause.

### Kill-switch execution (ops checklist)

```bash
# 1) Point the plugin back at Alluxio. 0-downtime config flip.
kubectl -n trino-db patch configmap trino-catalog-iceberg \
  --patch='{"data":{"fs.shelf.enabled":"false"}}'
kubectl -n trino-db rollout restart deployment/trino-coordinator deployment/trino-worker

# 2) Leave the Shelf StatefulSet running idle for 30 days so the team
#    has a diagnosable snapshot. shelfctl still works against live pods.
#    Do NOT delete the PVCs — the NVMe cache is forensic evidence.

# 3) File a postmortem. Template in docs/postmortem-template.md (TBD).
#    Include: which gate criterion failed, what we tried, why it did
#    not close, what we'd do differently in a v2.
```

### After the kill-switch

- Shelf stays on main in a maintenance-only mode; no new features.
- Alluxio goes back to full-primary on rep-2; the Alluxio rota resumes.
- The team scopes the next project (possibly "fix Alluxio harder").

---

## 5. Fast diagnosis tree

If a gate criterion is red but you don't yet know whether to trigger
the kill-switch, walk this tree. Three branches, three clicks each.

```
          ┌── Hit rate low ─────────┐
          │                         │
  Red?  ──┼── p99 latency high ─────┼── See §5.x below
          │                         │
          └── Error rate > 1 % ─────┘
```

### 5.1 Hit rate low (panel "Hit ratio overall" < 71 %)

1. Check the **per-pool** breakdown row. Is it the `metadata` pool or
  `rowgroup`? Metadata-pool cold means a recent snapshot invalidated
   everything (self-healing in < 30 min). Rowgroup cold means real
   traffic shift — page the data-eng oncall.
2. Check **pinned bytes** panel (bottom row). If `shelf_pinned_bytes`
  collapsed to 0, the pin-list loader is failing. Runbook:
   `[runbooks/shelf-fall-through-surge.md](../runbooks/shelf-fall-through-surge.md)`.
3. Check **fall-through rate** — `shelf_fallthrough_total` on the
  plugin side. If elevated, the plugin circuit breaker is open; a Shelf
   pod is sick. Runbook: `[runbooks/circuit-breaker-open.md](../runbooks/circuit-breaker-open.md)`.

If none of the above, the hit-rate drop is a real workload shift, not a
Shelf bug. Give it 2 h before escalating.

### 5.2 p99 latency high (panel "p99 latency" > 100 ms)

1. Click through to the **per-route** panel. Is it `GET /cache/`* or
  `/admin/*`? `/admin/*` slow does not matter for the gate; continue.
2. Is the spike correlated with NVMe fill > 80 %? If yes, runbook
  `[runbooks/shelf-nvme-usage-high.md](../runbooks/shelf-nvme-usage-high.md)`.
3. If latency is high but origin-request rate is **not** elevated,
  suspect DRAM pool eviction thrash. Runbook:
   `[runbooks/shelf-admission-model-stale.md](../runbooks/shelf-admission-model-stale.md)`.

### 5.3 Error rate > 1 % (alert `ShelfReadPathHighErrorRate`)

1. `kubectl -n shelf-staging logs -l app=shelf --tail=200 --prefix`
  grouped by pod. Look for 5xx or `ShelfUnavailableException` blobs.
2. If 5xx is correlated with one specific pod, `shelfctl ring` to see
  if it's still in the ring. If it is, `shelfctl evict` against a hot
   key to reproduce out-of-band, then page k8s-eng-1 to rotate.
3. If 5xx spans pods, suspect S3 throttling. Look for `503 SlowDown` in
  the origin-client span output (Tempo). Runbook:
   `[runbooks/shelf-fall-through-surge.md](../runbooks/shelf-fall-through-surge.md)`.

---

## 5.4 Pin-list replay (cold cache, hit-rate stuck < 50 % after warm-up)

> When a fresh shelfd pod (or a pool that just engine-reset) takes
> longer than the SHELF-G11 SLI to cross 50 % hit rate. We replay
> the pin-list against the affected pod so the top-N hot tables
> earn their cache lines without waiting on organic traffic.

**Pre-condition.** Confirm the pin-list itself is recent. From any
shelf pod:

```bash
kubectl -n alluxio exec shelf-2 -- /shelfctl pin status | head
# look for `entries` ≥ 5 and `last_reload_age_sec` < 1800.
```

If the loader has not refreshed in ≥ 30 min, regenerate the list
first — see "Regenerate" below.

**Replay (single pod).**

```bash
# Replay all pinned keys against shelf-2 only. Backpressure-aware:
# shelfctl streams the pin-list and lets shelfd's admission picker
# decide which entries are still worth fetching, so a mid-replay
# kill is safe.
kubectl -n alluxio exec shelf-2 -- /shelfctl pin replay --pool=metadata
kubectl -n alluxio exec shelf-2 -- /shelfctl pin replay --pool=rowgroup
```

Watch `shelf_admissions_total{pool, decision="admit"}` climb on the
**Admission & eviction policy** row in
[Shelf — Cache, Disk and Pods](https://platform-grafana.penpencil.co/d/shelf-overview).
The replay is healthy when:

- `shelf_origin_request_seconds{op="GetObject"}` p99 stays
  under 250 ms — Foyer's LODC submit queue is **not** saturated.
  If you see "lodc submit queue overflow" in `kubectl logs`, stop
  the replay; raise `pool.rowgroup.flushers` first (see
  `shelf1-oom-followup`).
- `shelf_disk_bytes_used{pool="rowgroup"}` increases only when
  `shelf_admissions_total{decision="admit"}` does — i.e. nothing
  is sneaking past the size-threshold gate.

**Regenerate (workload mix shifted).**

```bash
# Live top-50 by SUM(physicalInputBytes) × COUNT(*) over the last 7 days.
python3 tools/gen_pin_list.py \
    --trino-url      http://trino-replica-2.trino-db.svc:8080 \
    --trino-user     dbt_user \
    --top-n          50 \
    --output         s3://penpencil-cdp-temp/shelf/pin_list.json

# Emergency replay when Trino is down; uses the frozen TOP_5_PROD_TABLES
# constant in tools/gen_pin_list.py (refreshed 2026-04-27).
python3 tools/gen_pin_list.py \
    --top-5-prod \
    --output s3://penpencil-cdp-temp/shelf/pin_list.json
```

The shelfd `PinListLoader` polls every 15 min (configured via
`values-prod.yaml: cache.pinList.reloadIntervalSeconds`) so the
replay above runs against the *next* poll cycle. To force an
immediate reload without waiting, `kill -HUP` the shelfd PID inside
the pod (the daemon listens for `SIGHUP` on the pin-list channel).

**Cross-pod replay.** HRW routing means each key has exactly one
owner. Replaying against a single pod warms only its slice — for
all-pods warm-up issue the same `shelfctl pin replay` against
`shelf-0`, `shelf-1`, `shelf-2` in serial. Don't parallelise: the
origin `s3.penpencil.co` shares the same SDK pool, and three
parallel replays will trip Foyer's submit queue overflow guard
on at least one pod.

---

## 6. References

- Plan: `[agents/out/03-plan.md](../agents/out/03-plan.md)` §3 Phase 1, §4 SHELF-28, §6.4.
- ADR-0010: v0.5 gate — beat Alluxio on rep-2.
- On-call conventions: `[docs/oncall.md](./oncall.md)`.
- SLOs: `[docs/SLO.md](./SLO.md)`.
- Cluster handoff: `[docs/cluster-handoff.md](./cluster-handoff.md)`.
- Per-failure runbooks: `[runbooks/](../runbooks)`.
- Chaos drills: `[chaos/](../chaos)` + `make chaos-`* targets.

