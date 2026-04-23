# ADR 0002: Rendezvous (HRW) hashing over a 2000-vnode consistent-hash ring

_Status: Accepted (planner amendment, 2026-04-23)_
_Deciders: eng-lead, scientist agent §4.5, critic §1.2_

## Context

The v0.3 blueprint proposes a consistent-hash ring with 2000 virtual
nodes per physical node, capacity-weighted by NVMe size, with ring
membership stored in Raft. For a 3-7 pod cluster this is a 10k-20k
entry map that mutates on membership change and must be kept
byte-identical across Java plugin and Rust server (any
hash-function-implementation asymmetry turns into a silent subset of
100% miss-rate keys — a diagnosis nightmare).

Rendezvous (HRW) hashing gives the same "minimum movement on
membership change" guarantee with O(N) lookup, no ring data
structure, and a trivially-testable hash contract.

## Decision

Use Rendezvous (HRW) hashing with capacity weights.

- **Function.** `owner(key) = argmax_node (weight(node) /
  -ln((sha256(key || node_id) as u64) / max_u64))`.
- **Library.** `shelf-hashring` Rust crate + `ShelfHashRing` Java
  class, both implementing the above. Golden-vector unit tests verify
  byte-identical output across 10k random keys × 7 weighted nodes.
- **Weights.** Pulled from each pod's `/stats` endpoint
  (`capacity_bytes` / `used_bytes`); refreshed every 5 s along with
  DNS resolution.

## Alternatives considered

- **2000-vnode consistent hash.** Rejected: map-maintenance cost,
  vnode-count tuning, cross-language parity risk. Ticket's worth of
  work to introduce; weeks' worth of work to diagnose if it breaks.
- **Plain consistent hash (no vnodes).** Rejected: load imbalance on
  heterogeneous capacity is worse than HRW.
- **Jump hashing (Lamping & Veach 2014).** Rejected: doesn't support
  capacity weighting natively.

## Consequences

- **Positive.** O(N) hash-comparisons per lookup for N ≈ 10 nodes =
  sub-µs. Zero map maintenance. Cross-language parity is
  trivially-testable.
- **Negative.** Load balance is slightly worse than 2000 vnodes for
  very heterogeneous capacity; capacity-weighted HRW solves this and
  is well-known (Thaler-Ravishankar variant).
- **Neutral.** Scientist §4.5 and critic §1.2 independently converged
  on this. The existing industrial pattern (Varnish, some CDN cache
  tiers) uses HRW — this is not an experimental choice.
