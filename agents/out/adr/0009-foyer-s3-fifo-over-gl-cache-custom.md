# ADR 0009: Use Foyer's built-in S3-FIFO on NVMe; defer GL-Cache

_Status: Accepted (planner amendment, 2026-04-23)_
_Deciders: eng-lead, scientist agent §4.2, critic §6(3)_

## Context

The v0.3 blueprint specifies "GL-Cache-style group-level eviction" for
the NVMe tier (§6.1 `pool.rowgroup`). GL-Cache (FAST '23) reports +7
pp hit rate over LRB and 228× throughput — impressive, but requires
building, training, and operating a learned per-group policy. That is
6-8 weeks of engineering and a permanent ops dependency.

Foyer already ships two highly-regarded policies:

- **SIEVE** (NSDI '24) for DRAM-hot-path workloads.
- **S3-FIFO** (SOSP '23), which "FIFO queues are all you need for
  cache eviction" (CACM '24) argues beats learned policies on 10 of
  14 published datasets. Zero custom code.

## Decision

Use Foyer's built-in policies:

- **DRAM** — SIEVE (for metadata pool; row-group DRAM portion of the
  hybrid pool).
- **NVMe** — S3-FIFO.

No custom GL-Cache implementation in v1. Evaluate 3L-Cache (FAST '25)
or GL-Cache as a v1.1 upgrade only if Phase 4 replay measurements show
> 3 pp hit-rate gap vs S3-FIFO on our rep-2 traffic.

## Alternatives considered

- **GL-Cache custom impl (blueprint).** Rejected: 6-8 weeks of code
  we don't own; ML-ops dependency for a 3-7 pp ceiling on published
  traces (block I/O + CDN, not analytical lakehouse).
- **3L-Cache (FAST '25, scientist §4.3).** Strictly better than
  GL-Cache on the CPU dimension but still requires non-trivial
  integration and hyperparameter sensitivity; deferred to v1.1.
- **Plain LRU (Foyer built-in).** Rejected: S3-FIFO's "one-hit
  wonder" filtering specifically helps with long-tail ad-hoc traffic
  we know exists.

## Consequences

- **Positive.** Zero custom eviction code. Policy swap is a config
  change (Foyer's pluggable policy interface).
- **Negative.** Give up a few pp of published-trace hit-rate vs
  GL-Cache. If measurement on our trace confirms a meaningful gap,
  the policy can be swapped without changing Shelf code outside
  Foyer config.
- **Neutral.** The "FIFO is all you need" argument is strong on
  Zipfian traces; our workload is Zipfian.
