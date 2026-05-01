# ADR 0032: Multi-region federated shelf-pool

*Status: Proposed (2026-05-01)*
*Deciders: shelf-maintainers*
*Supersedes: none*
*Superseded-by: none*
*Related: ADR-0001 (no embedded Raft), ADR-0002 (HRW hashing), ADR-0011 (ETag content-addressing), ADR-0012 (Trino read-path strategy)*

## Context

Today the shelf-pool serves a single AWS region. The production deployment is `ap-south-1` only, with one StatefulSet of 4 m5a.4xlarge pods fronting Trino's S3 reads. This worked for a single-region workload — but the next 6–12 months are likely to add `us-east-1` (analytics warehouse for North-America-billed traffic) and `eu-west-1` (regulatory residency requirements for EU-resident user activity). Two unrelated problems show up the moment the second region appears.

**Problem 1: locality.** A US-East Trino coordinator pulling row groups through an `ap-south-1` shelf pool buys ~150 ms of cross-region RTT for every cache hit. That dwarfs the byte-range fetch latency the cache was built to compress. The cache wins on the local hop only.

**Problem 2: disaster recovery.** A single regional shelf-pool is a single failure domain. Karpenter rotation, an AWS regional service event, an IRSA token expiry — any one of these takes the whole cache down for the whole company. v1.0.0 mitigated this with the SHELF-23 peer-fetch ring inside one region; multi-region adds a second axis the ring doesn't cover.

Two design pressures pull in opposite directions. **Strict per-region isolation** (one pool per region, no cross-region traffic) is operationally clean but loses every cross-region cache locality benefit on first-touch reads. **Globally-shared pool** (one consistent ring across regions) maximises hit rate but requires distributed consensus on membership and writes — exactly what ADR-0001 said no to.

The middle path is per-region pools that *can* fall through to a peer region on miss, with an explicit, infrequently-exercised promotion knob — not consensus.

### What the cache state actually looks like across regions

Cache keys today are content-addressed by ETag (ADR-0011): `sha256(etag || u64_le(offset) || u64_le(length) || u32_le(rg_ordinal))`. The same Iceberg snapshot read from the same S3 bucket produces the same key in every region — there is no region-coupled state in the key derivation. That is the substrate that makes a cross-region peer fall-through legal at all: a `us-east-1` shelf pod that holds a key derived from an `ap-south-1`-hosted Parquet file is structurally serving the same bytes a local origin call would, no consistency proof required.

What *does* differ across regions is the origin path: the Iceberg metadata catalog (HMS / REST), the S3 bucket geography, and the IRSA role chain. A region-aware cache must encode the origin region into the routing decision, not into the key.

## Decision

Adopt a **per-region shelf-pool topology** with three load-bearing rules:

1. **One StatefulSet per region.** Each region runs an independent shelf-pool: own headless Service, own SHELF-23 resolver/ring, own Foyer NVMe state. The pools never share Foyer state on disk.
2. **Region is part of the routing decision, not the key.** Extend `shelfd/src/router.rs` (rc.8+ implementation, not this ADR) so the HRW computation runs over `(pod_id, region)` pairs from the local region's resolver view only. Peer-fetch (SHELF-23) is restricted to same-region peers by default; cross-region peer-fetch is explicitly off behind `cache.peerFetch.crossRegion.enabled = false`.
3. **Cold origin promotion, not consensus.** Cross-region promotion is a separate offline / control-plane job — it picks hot keys from a region's `shelf_hits_by_table_total` series and replays them through the target region's `/admin/pin` endpoint. CRDT-free, eventually-consistent, and rare enough (post-launch + after major schema rewrites) that the operator can drive it manually before any automation lands.

Cross-region miss-storm fall-through is a future opt-in, not a default. If `us-east-1`'s pool is fully cold and `ap-south-1`'s pool is warm, fall-through is gated on:

```yaml
cache:
  peerFetch:
    crossRegion:
      enabled: false        # default off
      regions: []           # explicit allow-list, e.g. ["ap-south-1"]
      maxLatencyMs: 200     # circuit-breaker
      bytesPerSecondCap: 50000000  # 50 MB/s cluster-wide rate cap
```

The default is "no cross-region traffic ever". Operators turn it on consciously, replica-by-replica, and only when an active cold-start event makes the cross-region hop cheaper than direct S3.

### Architecture sketch

```
                  +------------------------+      +------------------------+
                  | Iceberg metadata (HMS) |      | Iceberg metadata (HMS) |
                  | per-region                |      | per-region                |
                  +-----------+------------+      +-----------+------------+
                              |                               |
                              v                               v
+-----------------+   +------------------+      +------------------+   +-----------------+
| Trino ap-south-1|-->| shelf-pool a-s-1 |<====>| shelf-pool u-e-1 |<--| Trino us-east-1 |
+-----------------+   +--------+---------+      +--------+---------+   +-----------------+
                               |                          |
                               v                          v
                       +---------------+         +---------------+
                       | S3 a-s-1      |         | S3 u-e-1      |
                       | (origin)      |         | (origin)      |
                       +---------------+         +---------------+

 <====>  cross-region peer-fetch, default OFF, opt-in per cache.peerFetch.crossRegion
 |       same-region SHELF-23 peer-fetch, default ON, unchanged from v1.0.0
```

Promotion is the explicit dotted line: a control-plane job replays hot keys from region A's `shelf_hits_by_table_total` into region B's `/admin/pin`. It is not part of the data plane.

### What changes in shelfd

The work splits cleanly across files; this ADR commits to the design only. rc.8+ implements:

- `shelfd/src/router.rs` — region label on `Member` + HRW computation scoped to the local region.
- `shelfd/src/peer_fetch.rs` — gate cross-region peer-fetch behind the new config + circuit breaker.
- `shelfd/src/membership.rs` — resolver fans out only to same-region peers via headless-service DNS; cross-region peers are listed via explicit static endpoints.
- `crates/shelf-cost/src/lib.rs` — region label on the cost counter (already shipped via SHELF-40 `cost.region`; this ADR locks the convention).
- `infra/penpencil/charts/shelf/values-prod.yaml` — `region:` overlay per StatefulSet (one chart-render per region).

## Alternatives considered

### A. Single global shelf-pool with consensus on membership

Rejected. Distributed Raft / Ratis on the membership view is exactly what ADR-0001 ruled out: the operational footprint of a Raft cluster (leader-election under network partitions, log compaction, quorum loss handling) is significantly heavier than the reads it would protect. Cross-region replication is a distinct problem — adding consensus to solve it is over-engineering.

### B. Globally-shared Foyer state on a shared blob store

Rejected. Foyer's NVMe layout is local-disk-coupled by design (Direct I/O, partition-pinned blocks). Any "shared Foyer" model requires a network filesystem underneath, which negates the latency win that motivates a NVMe cache in the first place.

### C. Cloudflare R2-style global object store with regional buckets

Rejected as a Shelf design path; noted as a useful precedent. R2 solves a different problem (durable origin, pay-once-egress) and its architecture leans on a Cloudflare-internal control plane Shelf doesn't have. The reference is in the See-also; the model isn't.

### D. Cross-region peer-fetch as default-on

Rejected for v1 of this design. A cold pool falling through to a far-region peer at `iceberg.metadata-cache.enabled=false` cadence (every footer + manifest read on a JOIN-heavy query) would saturate inter-region bandwidth and confuse cost attribution. Default-off, opt-in, with a circuit breaker, is the only safe shape.

## Consequences

- **rc.7 deliverable is this ADR + a 1-engineer-week local prototype.** The prototype runs `docker-compose` with two MinIO endpoints (different bucket names simulating regional buckets), two shelfd processes with `region: a` / `region: b` labels, and an nginx instance with geo-DNS-style routing for the Trino read endpoint. Smoke is byte-identity on a 5-query Iceberg replay across both regions. **rc.7 does not ship the production code.**
- **rc.8+ implementation lift.** Estimated 2 – 3 engineer-weeks for the four-file change set above plus migration tooling. The chart value `region:` becomes mandatory for any new pool deployment; existing single-region pools default to `ap-south-1` and the migration is a no-op.
- **Cost discipline preserved.** Per-region pools mean per-region cost counters out of the box (already ADR-aligned via SHELF-40). Multi-region adds *new* fixed cost (one StatefulSet per region) that must be amortized against per-region S3 GET savings — operators will see the trade-off cleanly in `shelf_net_dollars_saved_total{region}`.
- **Cross-region fall-through is a fail-open gate.** If the circuit breaker trips, shelfd falls through to direct S3 in the local region. The operator is not paged on the trip; they're paged on a sustained pattern (a follow-on alert on `shelf_cross_region_fetch_circuit_open_total`).

## Rollback

Multi-region is a forward-only change for any region that adopts it. To roll back a region:

- **Single region's pool**: scale the StatefulSet to 0; the chart's catalog-side `s3.endpoint` flips back to direct S3 (existing flag, same shape as the rep-0 May 1 revert).
- **Cross-region peer-fetch**: flip `cache.peerFetch.crossRegion.enabled=false`; no state migration; takes effect on next ConfigMap reload + rolling restart.
- **The promotion job** is a separate piece of tooling; rolling it back is `kubectl delete cronjob`.

No on-disk format change, no key-derivation change, no Trino-side change. The entire design slots in additively.

## Verification (rc.7 prototype scope)

- `docker-compose up` brings up two MinIO + two shelfd in two simulated regions.
- A Trino-side smoke runs the SHELF-22 byte-identity harness (5 canonical Iceberg queries) against each region in turn; both must produce byte-identical output.
- A second smoke explicitly enables cross-region peer-fetch via the new config; observes `shelf_peer_fetch_total{region="other"}` climb on a cold pool.
- A third smoke verifies the circuit breaker: induces 2 s artificial latency on the cross-region link, asserts `shelf_cross_region_fetch_circuit_open_total` increments and traffic falls back to direct origin.

The rc.8 production-code verification is out of scope for this ADR.

## References

- [Cloudflare R2 architecture](https://developers.cloudflare.com/r2/how-r2-works/) — precedent for regional buckets with global addressing; useful for the "regional origin, regional cache" framing, not adopted as Shelf's design path.
- [BLUEPRINT.md §regional posture](../../../BLUEPRINT.md) — single-region assumption that this ADR explicitly extends.
- ADR-0001 — no embedded Raft (justifies why we don't reach for consensus on membership).
- ADR-0002 — HRW hashing (the routing primitive the region label extends).
- ADR-0011 — ETag content-addressing (the substrate that makes cross-region peer-fetch correct without consistency proofs).
- ADR-0012 — Trino read-path strategy (the same `s3.endpoint` swap pattern is per-region in this design).
- Workspace memory entries on SHELF-23 peer-fetch and the rep-1 cross-pod cache redistribution behaviour.
