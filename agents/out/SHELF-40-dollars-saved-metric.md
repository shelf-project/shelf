# SHELF-40: `shelf_s3_dollars_saved_total` Prometheus metric + audit-able formula

**Status:** Draft
**Tier:** S
**Estimated effort:** M
**Depends on:** SHELF-37
**Blocks:** SHELF-41

## Problem (OSS-cited)

Procurement decks ask "what does this thing save us?" and existing OSS caches can't answer in dollars-per-Iceberg-table. OpenCost ([opencost.io](https://www.opencost.io/)) and Karpenter's [cost reporter](https://karpenter.sh/) compute *cluster* cost; Cloudflare R2 [analytics](https://blog.cloudflare.com/r2-zero-egress-egress-fees/) shows GET savings in aggregate; nobody computes per-Iceberg-table $-saved with an audit-able, fail-closed formula. Trino's `QueryCompletedEvent` SPI ([trinodb/trino #26342](https://github.com/trinodb/trino/issues/26342)) carries `bytesReadFromCache` / `bytesReadExternally` per operator, which is the input the formula needs.

## Goal

A new Prometheus counter `shelf_s3_dollars_saved_total{tenant,table,user}` is exported by `shelfd` (or a sidecar exporter), computed from the audit-able formula below, that operators can paste straight into a procurement deck.

## Approach

Formula (commit it as a literal in code, with a unit-tested constant table):

```
shelf_$_saved =
      (hits_bytes / 1 GB)                × $0.0004           # S3 GET request data
    + (hits_count / 1000)                × $0.0004           # S3 GET request count
    + (hits_bytes_via_NAT / 1 GB)        × $0.045            # NAT egress (0 unless flagged on)
    + (hits_bytes_cross_az / 1 GB)       × $0.01             # cross-AZ data transfer
    − amortized_shelf_$cost_per_window                       # MUST subtract
```

`frac_traffic_through_NAT` and `frac_traffic_cross_az` default to 0; the metric refuses to publish a non-zero contribution from those terms unless the operator has explicitly enabled them with cluster-specific values (`config.savings.nat.fraction`, `config.savings.cross_az.fraction`). The amortised Shelf cost is computed from a single config value (`config.savings.amortized_dollars_per_hour`, default 0.0 — and if 0.0 the metric refuses to publish at all and emits a one-time WARN).

Implementation lands in a new module `shelfd/src/savings.rs` and exports the counter through the existing Prometheus registry in `shelfd/src/metrics.rs`. Inputs come from two streams: (a) per-request hit/miss bytes already counted by the SHELF-22 / SHELF-06 read path; (b) per-tenant attribution backfilled by a periodic SQL pull from the SHELF-37 log table (every 5 min) — the bridge keeps a `HashMap<(tenant, table), AtomicU64>` of bytes-saved-since-last-poll. The amortisation term is a wall-clock-based subtraction applied on each scrape.

A new shared crate `shelf-dollars-saved` under `crates/shelf-dollars-saved/` holds the formula + constants so SHELF-38 / SHELF-39 / SHELF-41 all read the same numbers (no formula drift across surfaces).

## Acceptance criteria

- [ ] `shelf_s3_dollars_saved_total{tenant,table,user}` is exported with non-decreasing values when hits accumulate.
- [ ] Without `amortized_dollars_per_hour` set, the counter stays 0 and a WARN is logged exactly once per process lifetime.
- [ ] Without explicit NAT/cross-AZ fractions, those terms contribute 0 — verified by a unit test that asserts `nat_term == 0 && cross_az_term == 0` when the config is unset.
- [ ] Formula constants are committed in `shelf-dollars-saved/src/constants.rs` with a comment citing the AWS pricing page URL and the date the constant was captured.
- [ ] Audit unit test: feed `(hits_bytes=10 GiB, hits_count=10_000, amortized=$0.10/hr, window=1 h)` and assert the output is the formula's hand-computed value to within a 0.0001 cent rounding tolerance.
- [ ] At least one quantitative gate: across a 1 M-event replay, total `dollars_saved` differs from a hand-computed reference by ≤ 0.5 %.

## Out of scope

- Public `/savings` SPA (SHELF-41).
- Currency other than USD.
- Spot vs on-demand price differentiation.
- Cross-cloud (Azure / GCP) constant tables.
- Auto-detecting NAT / cross-AZ fractions from VPC flow logs.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Over-claim: amortisation skipped → publishes inflated number | Counter refuses to publish unless `amortized_dollars_per_hour` is explicitly set; gate enforced by both unit test and a startup readiness check. |
| AWS price-list drift | Constants table dated; a CI job runs quarterly via `awspricing` MCP and opens an MR if any constant differs by ≥ 5 %. |
| Tenant-label spoofing | Tenant comes from the SHELF-37 log table's `user`/`session_properties` (source-authenticated by Trino); raw HTTP headers from the read path are never trusted. |

## Test plan

- Unit tests: each formula term in isolation; combined formula with all terms; explicit-zero behaviour for NAT/cross-AZ when fractions unset; amortisation subtraction over a synthetic 1 h window.
- Integration tests: replay a 1 M-event SHELF-37 fixture, assert `shelf_s3_dollars_saved_total` matches a committed golden number to ≤ 0.5 %.
- (If applicable) docker compose smoke: SHELF-12 + listener + savings exporter; assert the metric is non-zero and within ±20 % of expected after the 10-query smoke.

## Open questions

- Should the counter be a Prometheus `counter` (monotonic) or `gauge` (current rate)? Recommend counter — operators compute `rate(shelf_s3_dollars_saved_total[1h])`.
- Per-user cardinality: Prometheus blow-up risk. Recommend gating user label behind a feature flag, default off.
- Where does the constant table live: hard-coded or a YAML the AWS-pricing CI can rewrite? Recommend Rust constants with `// SOURCE: <url> AS_OF: 2026-04-28` comments and a `cargo xtask refresh-prices` rail.
