# `shelf-cost`

Audit-able cost model that converts shelfd cache hits into integer
**cents** of S3 + EC2 data-transfer + NAT-gateway spend avoided.
Ships the constants, the math, and the YAML config loader; wiring
into Prometheus / Grafana / `shelf-advisor` lives in those crates.

Ticket: [SHELF-40](../../shelfd/docs/design-notes/SHELF-40-dollars-saved-counter.md).

## Goals

- One source of truth for the dollar formula across `shelfd`,
  `shelf-advisor`, the SHELF-37 listener jar, and any future
  surface that wants to denominate "what shelf is saving us" in
  procurement dollars.
- Fixed-point arithmetic only. The cumulative counter must not
  drift across an `f64` mantissa over weeks of cache hits.
- Operator opt-out is a one-line YAML flip; the runtime cost is
  one fixed-point arithmetic add per cache hit (≪ 5 ns measured
  on `m6a.4xlarge`, see `benches/dollars_saved_atomic_add.rs`).

## What it costs (and where each constant came from)

The model attributes savings to four AWS line items:

| Line item                                 | Coefficient name                       | Source                                                                                                    | AS_OF   |
| ----------------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------- | ------- |
| S3 GET (Standard, Tier-1)                 | `get_request_micro_cents`              | <https://aws.amazon.com/s3/pricing/>                                                                      | 2026-04 |
| EC2 data transfer between AZs (same region) | `cross_az_micro_cents_per_byte`        | <https://aws.amazon.com/ec2/pricing/on-demand/> (Data Transfer)                                            | 2026-04 |
| EC2 data transfer within an AZ             | `same_az_micro_cents_per_byte`         | <https://aws.amazon.com/ec2/pricing/on-demand/> (Data Transfer)                                            | 2026-04 |
| NAT-gateway data processing                | `nat_processing_micro_cents_per_byte`  | <https://aws.amazon.com/vpc/pricing/>                                                                     | 2026-04 |

### Concrete values (us-east-1 + ap-south-1)

| Constant                                  | Published price          | Stored as              | Rounding residue |
| ----------------------------------------- | ------------------------ | ---------------------- | ---------------- |
| `get_request_micro_cents`                 | $0.0004 / 1 000 requests | **40 µ¢/req**          | 0 (exact)        |
| `cross_az_micro_cents_per_gib`            | $0.01 / GiB              | **1 000 000 µ¢/GiB**   | 0 (exact)        |
| `same_az_micro_cents_per_gib`             | $0.00 / GiB              | **0 µ¢/GiB**           | 0 (exact)        |
| `nat_processing_micro_cents_per_gib`      | $0.045 / GiB             | **4 500 000 µ¢/GiB**   | 0 (exact)        |

> **Why µ¢/GiB and not µ¢/byte?** AWS publishes data-transfer
> rates per GiB. Storing them per-byte forces a 1024^3 division
> in every coefficient and rounds rates < 1 µ¢/byte (which
> includes the entire AWS data-transfer table) either to `0`
> (under-charge by 100 %) or to `1` (over-charge by 1 075×). The
> µ¢/GiB unit holds every published rate exactly and pushes the
> divide-by-2^30 to `dollars_saved` time, where it lives in
> `i128` arithmetic alongside the bytes-multiply.

Both regions ship with identical published rates for these four line
items as of 2026-04 — Mumbai's regional uplift is on instance hours,
not on S3 GET / data-transfer pricing. EC2 hourly cost (e.g. the
SHELF-61 amortisation `$0.864/hr/pod` for `m6a.4xlarge` Mumbai list)
is **not** modelled here — it belongs in the operator's own values
overlay (`infra/<operator>/charts/shelf/values-prod.yaml`) so the
upstream coefficient table stays publishable without any
operator-specific identifiers.

The NAT term defaults to **off** (`nat_traversal_basis_points = 0`).
Most EKS data-platform stacks use an S3 VPC gateway endpoint
(<https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html>),
which incurs no NAT-data-processing charge; turning the term on is
an explicit operator affirmation that traffic does cross NAT in
their cluster.

## How to refresh the constants

This is a **manual quarterly refresh** until SHELF-NN automates it
behind the `awspricing` MCP. The full procedure:

1. Open <https://aws.amazon.com/s3/pricing/>. Confirm the **Standard
   Tier-1** GET price for both `us-east-1` and `ap-south-1`. AWS
   has changed the small-request tier price 3 times in the last 8
   years; if it has changed, update `coefficients::us_east_1` and
   `coefficients::ap_south_1`'s `get_request_micro_cents` field
   and bump the `AS_OF:` comment.
2. Open <https://aws.amazon.com/ec2/pricing/on-demand/> and scroll
   to **Data Transfer**. Confirm the cross-AZ rate ($0.01/GiB). If
   AWS rounds the published price to a different significant digit
   (the published number is documented as 1 ¢/GiB, but AWS has at
   times advertised 0.99 ¢ / 1.5 ¢), update
   `cross_az_micro_cents_per_byte` accordingly and bump `AS_OF:`.
3. Open <https://aws.amazon.com/vpc/pricing/> and confirm the NAT
   gateway data-processing charge ($0.045/GiB).
4. Re-run `cargo test -p shelf-cost --all-targets`. The unit tests
   in `coefficients::tests` will fail loudly if any preset constant
   drifts from the literal expected value.
5. Update `CHANGELOG.md` with a one-liner under `[Unreleased]`:
   `- chore(shelf-cost): refresh coefficients to AWS price list YYYY-QN`
6. Commit with the same DCO + author-email convention the rest of
   the repo uses.

If a price refresh requires a structural change (e.g. AWS adds a
fifth line item, or splits cross-AZ into "intra-region cross-AZ"
vs "intra-region cross-VPC"), the refresh PR also needs a design-
note update at
[`shelfd/docs/design-notes/SHELF-40-dollars-saved-counter.md`](../../shelfd/docs/design-notes/SHELF-40-dollars-saved-counter.md)
and a workspace-memory note in `~/trino/AGENTS.md`.

## How to use it

```rust
use shelf_cost::{CostModel, HitEvent, PeerAz};

let model = CostModel::for_region("ap-south-1").expect("known region");

let saved = model.dollars_saved(HitEvent::Memory {
    bytes_returned: 4 * 1024 * 1024,
    peer_az: PeerAz::SameAz,
});

println!("saved {saved}"); // -> "$0.00 (0 ¢)" — the GET avoided
                            //    rounds below 1 cent
```

Production wiring inside `shelfd` consumes the `Cents` value as a
`u64` count and fans out into the
`shelf_s3_dollars_saved_total{region, outcome}` Prometheus
counter. See the SHELF-40 design note for the full integration
diagram and the rate-helper gauge
(`shelf_s3_dollars_saved_rate_cents_per_sec`).

## What this crate is **not**

- Not a billing system. Real AWS bills include per-bucket request-
  tier mixes, S3 Inventory reads, KMS, replication, lifecycle
  transitions, and contractual / volume discounts that this crate
  makes no attempt to model.
- Not a benchmark. Benchmark numbers belong in `shelfd`'s
  benchmark harness or the SHELF-26 replay tooling.
- Not a precision counter for sub-cent values. The fractional cent
  of any single hit is dropped at the µ¢ → ¢ step. Over a million
  events the dropped residue caps at < $10, well below the
  acceptance gate (≤ 0.5 % of total).

## Licence

Apache-2.0, same as the rest of the workspace. The cost coefficients
are facts published by AWS; we record them here under fair-use as
operational data, with citations preserved per the table above.
