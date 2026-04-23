# `alluxio-2-9/` — Alluxio OSS 2.9.5, our production baseline

**This is the number we must beat.** E12 measured 71 % cumulative /
76 % instantaneous hit rate on rep-2 after the `UfsIOManager=256` patch
and the 3-master HA migration (2026-04-23). The v0.5 gate (ADR-0010)
is defined relative to this number.

## Source of truth for values

The values file committed here is a sanitised clone of our production
rep-2 Alluxio Helm values, with:

- Secrets (S3 keys, IRSA annotations) replaced with env-var references.
- Node affinity changed from rep-2 pool labels to bench-cluster labels.
- Master quorum reduced from 3 → 1 **only if** bench cluster has < 3
  nodes; default stays 3 for parity.

Do **not** re-tune for the bench environment. The whole point is to
measure what rep-2 measures.

## Critical production patches baked in

| Patch                            | Source                                        |
| -------------------------------- | --------------------------------------------- |
| `UfsIOManager=256`               | live CM patch 2026-04-23, committed in git.   |
| 3-master HA, not 1-master         | migration completed 2026-04-23.               |
| Journal on EBS gp3 200 GiB        | per rep-2 capacity plan.                      |
| Raft heartbeat timeout tuned      | per rep-2 `alluxio-site.properties`.          |

Drop any of these and the baseline is no longer E12 — flag in the run
record's `config_hash` field.

## Hit-rate calculation (same formula as Shelf)

```
cumulative_hit_rate = ClusterCacheHit / (ClusterCacheHit + ClusterCacheMiss)
```

Sampled from Alluxio's Prometheus endpoint every 10 s; 7-day rolling
window used for gate evaluation.

## TODO_SHELF-26

- Commit sanitised `values.yaml`.
- Document the exact `UfsIOManager=256` tuning.
- Pin Alluxio image digest in values file so we never accidentally
  compare two different Alluxio builds.
