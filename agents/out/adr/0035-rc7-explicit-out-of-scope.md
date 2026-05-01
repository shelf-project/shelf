# ADR 0035: Explicit out-of-scope for rc.7 — eBPF page-cache, DAC eviction, Apache Ratis

*Status: Accepted (2026-05-01)*
*Deciders: shelf-maintainers*
*Supersedes: none*
*Superseded-by: none*
*Related: ADR-0001 (no embedded Raft), ADR-0009 (Foyer S3-FIFO over GL-Cache custom), ADR-0011 (ETag content-addressing), ADR-0015 (SHELF-32 Sieve eviction on rowgroup pool)*

## Context

A pattern keeps recurring on Shelf's GitHub issue tracker, in research-paper roundups, and in occasional deep-research reports landing in the workspace: a contributor (sometimes a researcher, sometimes a well-intentioned agent) reads a recent paper or upstream proposal, identifies an "obvious win" for cache performance, and proposes adopting it for Shelf. Three of these proposals come up often enough that a one-time, written-down rejection is cheaper than re-litigating each instance:

1. **eBPF-customisable Linux page cache** — the SOSP 2025 `cache_ext` work ([IBM Research publication](https://research.ibm.com/publications/cacheext-customizing-the-page-cache-with-ebpf)) presents a kernel-side mechanism for application-tuned page-cache eviction policies via BPF programs.
2. **DynamicAdaptiveClimb (DAC) eviction** — [arXiv 2511.21235](https://arxiv.org/abs/2511.21235) reports DAC outperforming SIEVE on KV / CDN traces by 1 – 3 percentage points hit ratio.
3. **Apache Ratis / Raft for cache invariants** — periodic suggestions to use [Apache Ratis](https://ratis.apache.org/) or another Raft library to maintain "consistent" cache state across the shelf-pool.

This ADR says **no** to each, with a written rejection reason and a concrete re-evaluation trigger. It is a commitment ADR (status `Accepted`, not `Proposed`): rc.7 will not investigate any of the three. Re-evaluation is welcome only when the stated trigger fires.

The point is not that any of these technologies is bad. The point is that none of them solves a problem Shelf currently has, given the design choices we have already made.

## Decision

### 1. eBPF page-cache (`cache_ext`) — explicit NO

**Why it sounds appealing.** Shelf is a userspace cache; page-cache awareness would let us collaborate with the kernel's page eviction instead of competing with it. `cache_ext` advertises exactly that: tell the kernel "this Foyer NVMe-backed mmap region is hot, don't evict it" via a BPF LSM program.

**Why we say no.**

- **K8s portability.** BPF LSM modules require a privileged DaemonSet that pins kernel and BPF-toolchain versions across the cluster. Shelf today runs unprivileged on Bottlerocket nodes provisioned by Karpenter (m5a/m6a 4xlarge); the `alluxio` NodePool image rotates on Karpenter's schedule. Adding a BPF dependency turns Shelf into a kernel-version-pinned workload — a meaningful regression in operational footprint for an OSS distribution that aims to run on whatever kernel its host already has.
- **Invalidation semantics don't match Iceberg.** `cache_ext`'s policies talk about "this page is hot, keep it"; Shelf's invalidation talks about "this ETag is stale, the new one is at a different key" (ADR-0011). The kernel's page cache has no model for content-addressing; we'd be plumbing a parallel invalidation channel through BPF programs to communicate ETag changes to the kernel. The plumbing cost dominates the policy gain.
- **Foyer + NVMe is not the bottleneck.** Production observation since v1.0.0 GA: NVMe occupancy peaks at 240 GB across the pool; per-pod RSS budget (the actual constraint) caps DRAM at ~14 – 20 GiB. The bottleneck is DRAM admission policy + LDC submit-queue, not the page-cache eviction the kernel runs underneath. `cache_ext` would target a layer that is not currently hot.

**Re-evaluation trigger.** Re-open this decision **only if** Linux page-cache eviction shows up as the top item in a `perf` flamegraph against shelfd in production for ≥ 24 h sustained, *and* the same pod's `iostat` shows NVMe read amplification > 1.5 × read traffic (i.e. the kernel is reading the same NVMe block twice). Today neither is true. Until they are, this is rejected.

### 2. DynamicAdaptiveClimb eviction — explicit NO

**Why it sounds appealing.** [arXiv 2511.21235](https://arxiv.org/abs/2511.21235) presents DynamicAdaptiveClimb (DAC) eviction with reported 1 – 3 pp hit-ratio improvements over SIEVE on KV-store and CDN traces. SIEVE is what ADR-0015 targets for the Foyer rowgroup pool (gated under the F2 P2-conditional gate), so a "better than SIEVE" claim naturally raises the question.

**Why we say no.**

- **Workload mismatch.** DAC's reported wins are on KV-store and CDN traces — workloads where access patterns shift across access-frequency tiers in ways DAC's "climb" mechanism captures. Shelf's workload is **immutable Parquet byte ranges**, indexed by `sha256(etag || ...)`. Once a row group is in the cache, it doesn't change frequency tiers; either the ETag rotates (new key, no overlap) or it doesn't. The "adaptive climb" axis DAC optimises along doesn't have a corresponding axis in Shelf's content-addressed key space.
- **F2 gate already gates SIEVE.** ADR-0015 commits to SIEVE *if* the SHELF-35 Belady replay produces ≥ 5 pp lift over the tuned S3-FIFO baseline. That replay has not yet shown the lift, and PR #22 (Foyer 0.22 — the only path that ships SIEVE in Shelf's stack) remains parked. Adding DAC as a *third* candidate widens the search without cause; the right move is to finish the SHELF-35 replay against SIEVE first.
- **A/B fairness.** Even if DAC's gains transferred (they probably don't, see workload mismatch), comparing DAC vs S3-FIFO without first comparing SIEVE vs S3-FIFO leaves us uncertain whether the win comes from DAC specifically or from "anything-better-than-S3-FIFO". One-axis-at-a-time evaluation discipline says no.

**Re-evaluation trigger.** Re-open this decision **only if** SHELF-35 replay shows SIEVE produces ≥ 5 pp lift over S3-FIFO on the Shelf workload (the F2 gate clears) *and* a follow-up replay against DAC shows ≥ 1 pp lift over SIEVE on the same trace. Even then, the implementation cost ladder is non-trivial — DAC is not in Foyer upstream and would need a custom eviction policy. The replay smoke is cheap (< 1 engineer-day); the implementation isn't, and we shouldn't pay for the latter without the former.

### 3. Apache Ratis / Raft for cache invariants — explicit NO

**Why it sounds appealing.** Distributed consensus solves "all nodes agree on state X". A cache that wants strong consistency on membership / eviction / pin-list state across pods could naturally reach for Raft, and Apache Ratis is the well-maintained Java implementation many JVM systems use.

**Why we say no.**

- **Violates ADR-0001.** ADR-0001 explicitly rejects embedded Raft in shelfd. The reasoning still holds: Raft introduces leader election, log compaction, quorum-loss handling, snapshot replication — operational surface that materially exceeds the work it would protect. The shelf-pool is a stateless cache; pod failures already gracefully degrade via SHELF-23 peer-fetch + HRW resharding.
- **No correctness gap to fill.** Cache state correctness is handled by ADR-0011 (ETag content-addressing): if two pods hold a value for key `K`, the value is byte-identical because the key is `sha256(etag || ...)` and both pods derived it from the same ETag. There is no consistency anomaly to resolve via consensus. The shelf-pool does not have a "current snapshot of authoritative state" that nodes could disagree on; it has a content-addressed cache where disagreement is structurally impossible.
- **Membership doesn't need consensus either.** SHELF-23's resolver model uses K8s headless-service DNS + drain-bit propagation. Inconsistent membership views resolve within `dns_refresh = 5 s`. Strong consistency on membership is not required for correctness (HRW is order-insensitive); eventual consistency at DNS cadence has run cleanly through the rep-1 + rep-2 cutovers without a single observed inconsistency-driven incident.

**Re-evaluation trigger.** Re-open this decision **only if** a concrete cache invariant shows up that requires linearizable agreement across pods *and* cannot be resolved by ETag content-addressing or eventually-consistent DNS-driven membership. No such invariant exists in v1.0.0; if one shows up in rc.8+ (a use case ADR-0011 doesn't cover), the conversation can restart from this paragraph. Until then, this is rejected.

## Consequences

- **Future "obvious win" proposals get pointed at this ADR first.** New contributors who come in with one of these three proposals get a one-link answer with the rejection reason and the re-evaluation trigger. This saves several review cycles per proposal.
- **rc.7 commits to the Tier-A stability spine + Tier-D ops-UX work.** No bandwidth lost to investigating any of these three.
- **Re-evaluation is genuinely welcome on the stated triggers.** This ADR is not "permanent no"; it is "no until evidence X".

## Rollback

This ADR is a commitment, not a code change; there is no rollback. To revisit any of the three rejections, open a follow-on ADR that supersedes the relevant section of this one, with the trigger evidence cited inline.

## References

- [`cache_ext`: Customizing the Page Cache with eBPF](https://research.ibm.com/publications/cacheext-customizing-the-page-cache-with-ebpf) (SOSP 2025).
- [DynamicAdaptiveClimb: arXiv 2511.21235](https://arxiv.org/abs/2511.21235).
- [Apache Ratis](https://ratis.apache.org/) — JVM Raft implementation.
- ADR-0001 — no embedded Raft (the original commitment this ADR cites for rejection 3).
- ADR-0009 — Foyer S3-FIFO over GL-Cache custom (the eviction-policy framing that bounds rejection 2).
- ADR-0011 — ETag content-addressing (the substrate that makes rejection 3 a no-brainer).
- ADR-0015 — SHELF-32 Sieve eviction on rowgroup pool (the F2-gated SIEVE candidate that rejection 2 says to finish evaluating before considering DAC).
- Workspace memory entry on F2 P2-conditional gate (PR #22 / Foyer 0.22 parked indefinitely until SHELF-35 lift evidence).
