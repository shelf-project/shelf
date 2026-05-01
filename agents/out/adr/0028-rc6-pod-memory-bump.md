# ADR 0028: rc.6 pod memory bump 32Gi → 40Gi (replay of dropped PR #80)

_Status: Accepted (2026-05-01)_
_Deciders: shelfd-maintainers, shelf-oncall_
_Supersedes: none (PR #80 / draft ADR-0027 closed during the v1.0.0 GA squash)_
_Superseded-by: none_

## Context

The Phase D rc.5 cutover watcher on the production EKS cluster saw
**shelf-3 OOMKilled at 27.81 GiB** on a 32 GiB-limit pod at
~13:51:21 UTC on 2026-04-30, with shelf-0 also at ~27 GiB and rising.
This was the first post-cutover night-load pass on rep-0 (rep-0 carries
~5.7 M queries / 7d versus rep-1 ~85 K, so this was the first time the
StatefulSet saw sustained traffic at that scale).

The 27.81 GiB OOM signature is **not container-limit pressure** — the
pod was 4 GiB short of its declared 32 GiB limit. It is
**node-allocatable pressure**: the alluxio Karpenter NodePool's
`instance-size: 4xlarge` selector permitted four instance families
(`m6a`, `m5a`, `m7a`, `c6a`), and **c6a.4xlarge** is only 32 GiB
physical (~27.3 GiB allocatable after kubelet/system reservations).
A 32 GiB pod limit on a 27 GiB-allocatable node lets the pod run
until the kernel's eviction threshold trips, which happens around
27 GiB working-set RSS and kills the pod before the container limit
fires.

This is a recurring pattern in shelfd's OOM history (Foyer 0.12 LODC
submit queue + origin pool in-flight buffers + DRAM caches stack up
faster than the kernel reclaim cycles can free). User decision
(2026-04-30): keep rep-0 on shelf, bump the pod limit rather than
roll back the cutover.

The original chart change landed as PR #80 (`feat/rc6-p0.6-pod-memory-bump-32-to-40g`,
draft ADR-0027). PR #80 was **closed** during the v1.0.0 GA squash on
2026-04-30 EOD, so the chart durability fix never reached `main`.
This ADR re-establishes that change on top of post-GA `main` (commit
`82b65bd`) and renumbers as 0028 to leave 0027 retired.

The cluster-side relief was applied via direct `kubectl` on
2026-05-01 ~10:18 IST after rep-0 was rolled back to direct S3
(MR `!17966` in deployments-repo); the rolling restart completed at
~10:53 IST with all 6 shelf pods on `m5a.4xlarge`, 0 restarts,
0 OOMKill events. This ADR closes the durability loop so future
deploys carry the 40 GiB limit without manual `set resources`.

## Decision

Raise the **production overlay** pod resource block in
`infra/penpencil/charts/shelf/values-prod.yaml`:

| Field                    | Pre   | Post  | Rationale                                                |
| ------------------------ | ----- | ----- | -------------------------------------------------------- |
| `requests.memory`        | 24Gi  | 28Gi  | Maintain single-pod-per-node by raising request floor    |
| `limits.memory`          | 32Gi  | 40Gi  | ~16 GiB headroom over the worst-case sizing-rule budget  |
| `requests.cpu`           | 4     | 4     | unchanged                                                |
| `limits.cpu`             | 8     | 8     | unchanged                                                |

Cache-pool sizing knobs (metadata DRAM, rowgroup DRAM, LODC submit
queue, origin pool) stay at their rc.5 values — those were tuned in
SHELF-21f for the 27 GiB-allocatable case. The 40 GiB limit gives the
runtime + Rust allocator + tokio stacks the headroom they need under
sustained read load without the kernel killing the pod first.

Worst-case sizing rule check at the new limit:

```
  5 GiB  metadata pool DRAM
+ 11 GiB rowgroup pool DRAM
+ 4 GiB  origin.pool.maxConnections × 32 MiB (128 × 32 MiB)
+ 1 GiB  LODC submit queue threshold
+ 3 GiB  Rust runtime / tokio stacks / jemalloc fragmentation
= 24 GiB worst case
+ 16 GiB headroom inside the 40 GiB limit
```

### Co-requirement: alluxio NodePool instance-family scope

40 GiB pod limit is **only feasible on m-family 4xlarge** nodes
(`m6a.4xlarge`, `m5a.4xlarge`, `m7a.4xlarge` — 64 GiB physical,
~58 GiB allocatable). On `c6a.4xlarge` (32 GiB physical, ~27 GiB
allocatable) the new 40 GiB limit reproduces the pre-existing OOM
mode: pods get killed at ~27 GiB on node pressure, regardless of the
declared limit.

The operator MUST drop `c6a` from the alluxio NodePool's
`karpenter.k8s.aws/instance-family` requirement list before rolling
out this overlay. The Karpenter spec lives outside this chart
(`alluxio-fix/karpenter/alluxio-provider.yaml` in the operations
workspace). The cluster-side rollout on 2026-05-01 applied the
NodePool patch first (instance-family `[m6a, m5a, m7a, c6a]` →
`[m6a, m5a, m7a]`), drained all 16 c6a nodes via a temporary
disruption-budget widen 0 → 1, then restored the budget to 0 before
patching the StatefulSet resources. The deploy runbook captures this
ordering as a hard prerequisite check.

If the NodePool change cannot be made first, fall back to the
**conservative path**: keep the 32 GiB limit and instead reduce
`cache.pools.rowgroup.dramSizeBytes` further (14 → 11 → 8 GiB)
together with `origin.pool.maxConnections` (256 → 128 → 96). That
trims the worst-case footprint at the cost of hit ratio.

## Consequences

- **One overlay knob, one ADR.** The chart change is two-line in
  `values-prod.yaml` (request 24 → 28 Gi, limit 32 → 40 Gi) plus
  the sizing-rule comment refresh. The OSS default
  (`charts/shelf/values.yaml`, 48 Gi request / 64 Gi limit) already
  exceeds 40 GiB and is already correct for generous-node clusters,
  so no OSS-side numeric edit is required.

- **Cluster-side rollout via `kubectl set resources`, not Helm.**
  Shelfd's StatefulSet is applied via direct `kubectl apply` against
  the in-cluster manifest, not a `helm upgrade` run, so this chart
  PR is durable for **future** deploys but does NOT auto-update the
  live STS. The runbook at `/tmp/shelf-mem-bump-runbook.md`
  documents the exact `kubectl -n alluxio set resources sts/shelf …`
  command for the immediate rollout.

- **Capacity planning under the new limit.** 6 pods × 40 GiB =
  240 GiB total reserved on the alluxio NodePool, vs. the previous
  6 × 32 = 192 GiB. The NodePool's existing `limits.memory: 1024Gi`
  budget covers the new total comfortably. With the m-family-only
  constraint, each pod schedules onto its own 64 GiB node; one
  Karpenter rebalance opportunity is lost (we can no longer
  consolidate two shelf pods on a single 32 GiB c6a node, but that
  was never a sustainable layout in the first place — it's the
  config that produced the 2026-04-30 OOM).

- **Rolling restart preserves NVMe state.** PVCs survive the rolling
  restart triggered by the `kubectl set resources` patch, so each
  pod's Foyer NVMe replay (~30-60 s on a warm 240 GiB pool) keeps
  the disk tier hot. DRAM is lost per pod sequentially, so the
  cluster-wide hit ratio dips during the ~10-min rolling window
  (6 pods × ~100 s including `minReadySeconds=30` and
  `startupProbe` grace).

## Validation evidence (2026-05-01 cluster apply)

- Karpenter NodePool patched at 10:21 IST: `instance-family`
  `[m6a, m5a, m7a, c6a]` → `[m6a, m5a, m7a]`.
- Disruption budget temporarily widened 0 → 1 to allow Karpenter
  drift to drain the 16 in-service c6a nodes (~20 min total).
- All 6 shelf pods landed on m5a.4xlarge by ~10:42 IST with 0
  restarts and 0 OOMKill events.
- Disruption budget restored to 0 before the resource patch.
- `kubectl set resources` applied at ~10:43 IST; STS rolling
  restart completed at ~10:53 IST.
- Post-fix RSS at +5 min: 6.0–12.5 GiB across all 6 pods (cold
  cache, NVMe replay still warming) — well under both the 22 GiB
  warn watermark and the 40 GiB limit.
- Foyer NVMe state preserved (`shelf_disk_bytes_used` ≈ 6.7 GiB
  per pod immediately post-restart, climbing on warm-up).

## Rollback signals

| Trigger | Action |
|---|---|
| Any shelf pod OOMKilled at the new 40 GiB limit (RSS at kill > 35 GiB) | Revert this overlay; investigate whether DRAM caps or LODC submit queue have grown faster than the new headroom. |
| Any shelf pod OOMKilled at < 30 GiB RSS post-rollout | Confirms a c6a.4xlarge slipped through the NodePool filter. Revert this overlay AND reapply the NodePool instance-family restriction. |
| `kube_node_status_condition{condition="MemoryPressure",status="true"}` fires on any alluxio node | Node-allocatable headroom is gone; either the NodePool slipped a c6a node in or another tenant is competing for the node. Revert overlay; resize NodePool. |
| Cluster total reserved memory on alluxio NodePool > 80 % of NodePool `limits.memory` (1024 GiB) | Pre-emptively pause further shelf-pool scale-outs at the new size; resize NodePool before adding capacity. |

## References

- **PR #80** (closed during v1.0.0 GA squash) — the original chart
  change this ADR replays.
- **rc.5 cutover exec summary** — `docs/rollout-v1/rc5-cutover-2026-04-30-exec-summary.md` §3
  (P0 backlog item: "rc6-capacity-fix-prereq").
- **SHELF-21f** — DRAM 14 → 11 GiB, origin pool 256 → 128, flushers
  4 → 8, bufferPool 256 → 384 MiB; the existing rc.5 sizing baseline
  this ADR builds on.
- **`alluxio-fix/karpenter/alluxio-provider.yaml`** (out-of-tree
  Karpenter spec) — the `instance-family` requirement block is the
  co-requirement edit point.
- **ADR-0008** — two-pool architecture (sizing-rule inputs).

## Threat model / risks

- **NodePool drift.** If a future operator re-adds `c6a` to the
  `instance-family` list (perhaps because Karpenter exhausts m-family
  spot capacity), the OOM signature reappears with no chart change.
  Add a periodic check that asserts every `workload=alluxio` node
  carries an `m6a/m5a/m7a` instance-type label, and alert on any
  c6a node that schedules into the pool. Track as a follow-up.
- **NodePool exhaustion.** With c6a removed, the m-family pool may
  hit on-demand quota / capacity limits more aggressively under
  scale-out. Coordinate with infra-eng before SHELF-pool scale-outs
  beyond 6 pods on top of the existing alluxio workload.
- **The 16 GiB headroom buys time, not safety.** Future Foyer
  upgrades (PR #22 0.22.x), W-TinyLFU (SHELF-33), and the bloom
  index (SHELF-46 / ADR-0021) all add a few hundred MiB to a few
  GiB to the worst-case sizing rule. Track each lever's RSS impact
  before flipping defaults; re-run the sizing-rule check above any
  time a new component lands inside the pod.
