# SHELF-48: Priority lanes Phase 1 — pool-level token-bucket on origin

**Status:** Draft
**Tier:** A
**Estimated effort:** S
**Depends on:** none
**Blocks:** none

> **Sequencing note: do not start before SHELF-23 lands.** This ticket adds origin-pool fairness primitives that are exercised through `shelfd/src/http.rs` request handlers (and the S3 shim path), which SHELF-23 currently has in-flight. Resume after SHELF-23 merges.

## Problem (OSS-cited)

Backfill ETL nukes BI dashboard latency every morning by saturating the shared origin S3 connection pool. [Trino resource-groups docs](https://trino.io/docs/current/admin/resource-groups.html) provide memory and CPU fairness only — no I/O fairness on shared origin connections. Alluxio's `UfsIOManager` is also a single-pool surface; we hit this in 2026-04 and patched it (see `agents/out/03-plan.md` Phase −1) but it does not isolate tenants. No OSS cache today implements per-pool token-bucket fairness on the origin path.

## Goal

`shelfd`'s origin client allocates fetch tokens through a configurable token-bucket per Foyer pool (`metadata`, `rowgroup`), so a saturating ETL backfill on `pool.rowgroup` cannot starve `pool.metadata`'s small, fast manifest fetches.

## Approach

Phase-1 deliverable: pool-level only (not yet per-tenant — that's Phase 2 once `query_id` is plumbed via SHELF-37 + a SHELF-29-tier shim handover; out of scope for this ticket). Implement a [Bucket4j-style](https://bucket4j.com/) token bucket in pure Rust (use the `governor` crate, Apache 2.0; pin a version) with one bucket per pool. Bucket parameters configured in `shelfd/src/config.rs`:

```toml
[origin.fairness]
enabled = true
total_rate_per_second = 4096          # global ceiling on origin GETs/s
total_burst = 8192                    # global burst cap

[origin.fairness.pool.metadata]
weight = 10                           # share of total

[origin.fairness.pool.rowgroup]
weight = 1
```

Effective per-pool rate = `total_rate * weight / Σ weight`. The bucket is consulted in `shelfd/src/origin.rs::get_range` before issuing the S3 call; if no token is available, the fetch awaits up to `origin.fairness.max_wait_ms` (default 200 ms) then proceeds anyway with a `shelf_origin_throttle_blocked_ms{pool}` histogram observation. The bucket is refilled at wall-clock cadence by an internal tokio task.

Metrics:
- `shelf_origin_tokens_consumed_total{pool}`
- `shelf_origin_tokens_starved_total{pool}` (no token available within `max_wait_ms`)
- `shelf_origin_throttle_wait_seconds{pool}` histogram

The `rowgroup` pool gets default `weight=1`, `metadata` gets `weight=10`, so manifest reads are 10× preferred. Operators retune via the SHELF-38 `tune` recommendations.

## Acceptance criteria

- [ ] With ETL flooding `pool.rowgroup`, `pool.metadata` p95 origin-fetch latency stays within 1.2× of an idle baseline (synthetic load test).
- [ ] Bucket-disabled mode (`origin.fairness.enabled=false`) reverts to today's behaviour and is a no-op for metrics.
- [ ] At default weights, `pool.metadata` consumes ≥ 90 % of available tokens during contention.
- [ ] Quantitative gate: warm `pool.metadata` p99 origin-fetch latency under simultaneous ETL load on `pool.rowgroup` ≤ 50 ms (vs > 500 ms without the bucket on a 4-core dev pod).
- [ ] Token starvation increments `shelf_origin_tokens_starved_total` and the corresponding fetch still completes (no error to client; this is a fairness control, not a hard limit).
- [ ] Unit tests ≥ 10 cases (weight ratio enforcement, disabled-mode parity, max-wait timeout, refill cadence).
- [ ] Integration test under `shelfd/tests/it_priority_lanes.rs` driving a synthetic two-pool contention workload.

## Out of scope

- Per-tenant lanes (Phase 2 of the BLUEPRINT idea — depends on SHELF-37 + SHELF-29-class shim work).
- Per-prefix S3 rate limiter (separate, BLUEPRINT §9.4 / Phase 3).
- Coordinator-side queue weighting in Trino (out of repo).
- Hard rate limits (this is a *fairness* primitive, not a quota).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Tenant-label spoofing (Phase 2 concern) | Phase 1 is pool-level only, no tenant labels; spoofing is not a Phase 1 attack surface. |
| `governor` crate API churn | Pin the version; alternative is a hand-rolled atomic bucket; benchmark both. |
| Misconfiguration starves `pool.rowgroup` indefinitely | Default weights documented; min-rate floor (`pool.rowgroup.min_rate_per_second`, default 64) ensures progress. |
| Token bucket adds latency on the hit path | Bucket is consulted in `origin.rs::get_range` only — hit path is unaffected. Verified by metrics regression test. |

## Test plan

- Unit tests: weight enforcement, refill cadence, disabled-mode no-op, max-wait, min-rate floor.
- Integration tests: two-pool contention workload, asserts `pool.metadata` latency stays below SLA.
- (If applicable) docker compose smoke: SHELF-12 + a synthetic ETL load on `pool.rowgroup`; assert `shelf_origin_throttle_wait_seconds{pool="metadata"}` p95 ≤ 10 ms.

## Open questions

- Default weights `metadata=10, rowgroup=1` — too aggressive? Recommend keeping; SHELF-38 `tune` can re-recommend.
- Phase 2 (per-tenant): which ticket carries it? Recommend SHELF-48b once `query_id` plumbing is in scope.
- Should the bucket be shared across pods (e.g. via Redis) or per-pod? Per-pod in Phase 1; cross-pod fairness is a Phase 3 problem.
