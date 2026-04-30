# ADR 0027: rc.6 pod memory bump 32Gi → 40Gi

_Status: Accepted (2026-04-30)_
_Deciders: shelfd-maintainers, shelf-oncall_
_Supersedes: none_
_Superseded-by: none_

## Context

The Phase D rc.5 cutover watcher on `data-platform-cluster` saw **shelf-3
OOMKilled at 27.81 GiB** on a 32 GiB-limit pod at ~13:51:21 UTC on
2026-04-30, with shelf-0 also at 27.0 GiB and rising. This was the first
post-cutover night-load pass on rep-0 (rep-0 carries ~5.7 M queries / 7d
versus rep-1 ~85 K, so this was the first time the StatefulSet saw
sustained traffic at that scale).

The 27.81 GiB OOM signature is **not container-limit pressure** — the pod
was 4 GiB short of its declared 32 GiB limit. It is **node-allocatable
pressure**: the alluxio Karpenter NodePool's `instance-size: 4xlarge`
selector permits four instance families (`m6a`, `m5a`, `m7a`, `c6a`),
and **c6a.4xlarge** is only 32 GiB physical (~27.3 GiB allocatable after
kubelet/system reservations). A 32 GiB pod limit on a 27 GiB-allocatable
node lets the pod run until the kernel's eviction threshold trips,
which happens around 27 GiB of working-set RSS and kills the pod
before the container limit fires.

This is a recurring pattern in shelfd's OOM history (see
`shelfd/docs/runbooks/2026-04-shelf-1-oom.md` and the workspace
`AGENTS.md` notes from 2026-04-27 / 2026-04-29) — the Foyer 0.12 LODC
submit queue + origin pool in-flight buffers + DRAM caches stack up
faster than the kernel's reclaim cycles can free, and the pod dies on
node-pressure rather than at its declared limit.

User decision (2026-04-30): keep rep-0 on shelf, bump the pod limit
rather than roll back the cutover.

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
out this overlay. The Karpenter spec lives outside this chart at
`/Users/aamir/trino/alluxio-fix/karpenter/alluxio-provider.yaml`; the
edit is the four-line `In:` block at line 112-118 of that file. The
deploy runbook captures this as a hard prerequisite check.

If the NodePool change cannot be made first, fall back to the
**conservative path**: keep the 32 GiB limit and instead reduce
`cache.pools.rowgroup.dramSizeBytes` further (14 → 11 → 8 GiB)
together with `origin.pool.maxConnections` (256 → 128 → 96). That
trims the worst-case footprint at the cost of hit ratio.

## Consequences

- **One overlay knob, one ADR.** The chart change is two-line in
  `values-prod.yaml` (request 24 → 28 Gi, limit 32 → 40 Gi) plus the
  sizing-rule comment refresh. The OSS default
  (`charts/shelf/values.yaml`, 48 Gi request / 64 Gi limit) already
  exceeds 40 GiB and is already correct for generous-node clusters,
  so no OSS-side numeric edit is required.

- **Cluster-side rollout via `kubectl set resources`, not Helm.**
  Shelfd's StatefulSet is applied via direct `kubectl apply` against
  the in-cluster manifest, not a `helm upgrade` run, so this chart
  PR is durable for **future** deploys but does NOT auto-update the
  live STS. The runbook at `/tmp/shelf-mem-bump-runbook.md`
  (also captured in this PR's body) documents the exact
  `kubectl -n alluxio set resources sts/shelf …` command for the
  immediate rollout.

- **Capacity planning under the new limit.** 6 pods × 40 GiB =
  240 GiB total reserved on the alluxio NodePool, vs. the previous
  6 × 32 = 192 GiB. The NodePool's existing `limits.memory: 1024Gi`
  budget covers the new total comfortably. With the m-family-only
  constraint, each pod schedules onto its own 64 GiB node; one
  Karpenter rebalance opportunity is lost (we can no longer
  consolidate two shelf pods on a single 32 GiB c6a node, but that
  was never a sustainable layout in the first place — it's the
  config that produced today's OOM).

- **Rolling restart preserves NVMe state.** PVCs survive the rolling
  restart triggered by the `kubectl set resources` patch, so each
  pod's Foyer NVMe replay (~30-60 s on a warm 240 GiB pool) keeps
  the disk tier hot. DRAM is lost per pod sequentially, so the
  cluster-wide hit ratio dips during the ~7-min rolling window
  (4 pods × ~100 s including `minReadySeconds=30` and
  `startupProbe` grace).

- **Pairs with rc.6 capacity-fix prereq.** This satisfies the
  `rc6-capacity-fix-prereq` line item under rc.6 P0 in
  `docs/rollout-v1/rc5-cutover-2026-04-30-exec-summary.md` §3.
  The plan file flip is owned by the rc.6 orchestrator (workspace
  rule: orchestrator owns plan edits).

## Rollback signals

| Trigger | Action |
|---|---|
| Any shelf pod OOMKilled at the new 40 GiB limit (RSS at kill > 35 GiB) | Revert this overlay; investigate whether DRAM caps or LODC submit queue have grown faster than the new headroom. |
| Any shelf pod OOMKilled at < 30 GiB RSS post-rollout | Confirms a c6a.4xlarge slipped through the NodePool filter. Revert this overlay AND reapply the NodePool instance-family restriction. |
| `kube_node_status_condition{condition="MemoryPressure",status="true"}` fires on any alluxio node | Node-allocatable headroom is gone; either the NodePool slipped a c6a node in or another tenant is competing for the node. Revert overlay; resize NodePool. |
| Cluster total reserved memory on alluxio NodePool > 80 % of NodePool `limits.memory` (1024 GiB) | Pre-emptively pause further shelf-pool scale-outs at the new size; resize NodePool before adding capacity. |

## References

- **rc.5 cutover exec summary** — `docs/rollout-v1/rc5-cutover-2026-04-30-exec-summary.md` §3
  (P0 backlog item: "rc6-capacity-fix-prereq").
- **SHELF-21f** — DRAM 14 → 11 GiB, origin pool 256 → 128, flushers
  4 → 8, bufferPool 256 → 384 MiB; the existing rc.5 sizing baseline
  this ADR builds on.
- **Phase D auto-rollback watcher** — the OOMKill that triggered
  this ADR fired after the watcher's 19:36 IST soak window closed
  cleanly, so this ADR's bump is a forward fix, not a rollback.
- **`/Users/aamir/trino/alluxio-fix/karpenter/alluxio-provider.yaml`** —
  out-of-tree Karpenter spec; the `instance-family` requirement
  block is the co-requirement edit point.
- **`shelfd/docs/runbooks/2026-04-shelf-1-oom.md`** — earlier OOM
  postmortem with the same kernel-pressure signature on c6a-class
  allocatable.
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
