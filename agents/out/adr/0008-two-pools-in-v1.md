# ADR 0008: Two pools in v1 (metadata + bulk), not four

_Status: Accepted (planner amendment, 2026-04-23)_
_Deciders: eng-lead, critic §1.7_

## Context

The v0.3 blueprint proposes four pools: `pool.metadata` (DRAM
FrozenHot, 5 % quota), `pool.footer` (DRAM FrozenHot, 10 % quota),
`pool.rowgroup_hot` (DRAM SIEVE, rest of DRAM), and `pool.rowgroup`
(NVMe GL-Cache). Separating hot row-group DRAM from NVMe row-group is
a Firebolt-inspired idea; separating metadata from bulk is the real
differentiator.

Four pools means four byte quotas to tune, four eviction policies to
benchmark, and four failure modes when quota miscalculation starves
one tier. Alluxio's tiered-store quotas caused a real
`NodeHasDiskPressure` eviction storm on 2026-04-20 — same failure
class. We don't have evidence that separating `rowgroup_hot` from
`rowgroup` is necessary for our workload.

## Decision

Ship **two pools** in v1:

- `pool.metadata` — DRAM only, FrozenHot, 5 GiB absolute (not
  percentage). Holds `metadata.json`, manifest lists, manifests,
  Parquet footers, page indexes.
- `pool.rowgroup` — Foyer hybrid (DRAM + NVMe), S3-FIFO, using the
  remaining DRAM (~56 GiB per pod) + 500 GiB NVMe. Holds row-group
  byte-ranges.

Split `rowgroup_hot` out only if we measure (via the `trino_logs`
replay from SHELF-26) that ad-hoc bulk scans are evicting dashboard
row groups from DRAM frequently enough to move p95. That is a
v1.1 decision, not a v1 one.

## Alternatives considered

- **Four pools (blueprint).** Rejected: premature optimisation for
  the `rowgroup_hot` / `rowgroup` distinction; and the `metadata` /
  `footer` distinction is effectively already solved by both living
  in the FrozenHot DRAM pool.
- **One pool.** Rejected: critic and scientist agree pool isolation
  between metadata and bulk scan is genuinely right — Firebolt and
  Alluxio both validate it.

## Consequences

- **Positive.** Simpler tuning surface; one quota to worry about; one
  eviction policy to benchmark per tier.
- **Negative.** If a 50 GB ad-hoc scan fills the DRAM portion of
  `pool.rowgroup`, dashboard row groups evict. Mitigation: size
  threshold admission (ADR-0003) refuses ≥ 1 GiB non-pinned admits,
  and SIEVE on DRAM resists scan evictions on frequency.
- **Reversible.** Adding `pool.rowgroup_hot` later is additive; no
  breaking change.
