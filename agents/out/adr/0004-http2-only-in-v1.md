# ADR 0004: HTTP/2 range-GET only in v1; Arrow Flight deferred

_Status: Accepted (planner amendment, 2026-04-23)_
_Deciders: eng-lead, scientist agent §2.6 + §4.6, critic §1.5_

## Context

The v0.3 blueprint proposes a hybrid data plane: HTTP/2 for < 1 MB
payloads, Arrow Flight for ≥ 1 MB. The "6 GB/s single-stream" number
cited for Flight comes from Tanveer et al. DaMoN '22 measured on
Mellanox InfiniBand hardware. EKS commodity ENIs cap at 10-25 Gbps
(≈ 1-3 GB/s per stream). Flight also introduces:

- A gRPC version skew risk between the Java plugin (Java gRPC) and
  the Rust server (Tonic). Arrow issue #35910 reports a 10-15 %
  throughput regression on a gRPC 1.46 upgrade.
- Two protocols to tune, two pool configurations, two failure modes.
- An `arrow-flight` Rust/Java dependency with its own release cadence.

No published benchmark validates the 1 MB crossover point Shelf
proposes; it's a first-principles guess.

## Decision

Ship **v1 HTTP/2 range-GET only** for all payload sizes. Keep the
`ShelfReadRequest` protobuf definition reserved for v1.x Flight use.
Revisit Flight only if a Phase 2+ EKS-measured benchmark shows ≥ 20%
throughput gain at our per-stream realistic bandwidth.

## Alternatives considered

- **Hybrid from day 1 (blueprint).** Rejected: two protocols, doubled
  tuning + benchmark cost; the "6 GB/s" marketing number is not
  achievable on EKS.
- **gRPC-only (bytes payload).** Rejected: mediocre compromise;
  loses h2 multiplexing benefits of pure HTTP/2.
- **QUIC / HTTP/3.** Rejected for v1: immature server-side support;
  head-of-line blocking wins don't help on short-RTT same-AZ paths.

## Consequences

- **Positive.** One protocol to tune, one connection pool, one
  benchmark to publish, zero gRPC-version regression risk. Matches
  the same HTTP/2 path as the S3-compat shim in §8.3 (one code path).
- **Negative.** Give up theoretical zero-copy gains on bulk row-group
  reads. Our measurement baseline (E3) will tell us if this matters.
- **Neutral.** Arrow Flight remains on the v1.x roadmap. The protobuf
  stubs remain in `protos/` for eventual re-introduction.
