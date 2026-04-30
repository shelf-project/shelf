# SHELF-40 — `shelf_s3_dollars_saved_total` counter + shared cost crate

| Field   | Value                                                      |
|---------|-------------------------------------------------------------|
| Status  | Implemented (this PR).                                      |
| Owners  | aamir                                                       |
| Tickets | SHELF-40                                                    |
| Code    | `crates/shelf-cost/` + `shelfd/src/cost.rs`                  |
| Tests   | `crates/shelf-cost/tests/property.rs`, `crates/shelf-cost/benches/dollars_saved_atomic_add.rs`, `shelfd/tests/it_dollars_saved.rs` |

## TL;DR

We stand up a first-class **`$/sec`** rate dashboard that quantifies
how much shelf is saving in S3 cost in real time. The math is
deliberately narrow: every cache hit avoids exactly one S3 GET
(`$0.0004 / 1 000` requests in `us-east-1`, region-overridable) and
avoids paying for the bytes-out the pod's NIC would have shipped if
the hit had crossed an AZ boundary (`$0.01/GiB` cross-AZ; `$0.00/GiB`
same-AZ). The hard part isn't the formula — it's making the
constants citable, the breakdown auditable, and the path through
the codebase narrow enough that future cost dimensions (PUT, LIST,
KMS, S3 IA tier, contractual discounts) can plug in without
touching every call site.

The implementation lives in **two pieces**:

1. A new cargo workspace member `crates/shelf-cost/` that owns the
   formula, the citation-bearing coefficient table, and the
   serde-validated config loader. Pure data + arithmetic; no
   Prometheus, no Tokio, no AWS SDK.
2. `shelfd::cost::CostState` glue that bumps the counter on the
   `s3_shim` / `peer_fetch` hot paths and publishes the rolling
   rate gauge.

## Why a separate crate

Three downstream surfaces consume the same dollar formula:

- **`shelfd`** (this PR) — bumps the Prometheus counter in the
  read-path.
- **`shelf-advisor`** (Tier-3 follow-up, SHELF-NN) — scores
  candidate pin / MV recommendations against the same
  coefficients so the recommendations are denominated in the same
  dollars the procurement deck reads.
- **SHELF-37 listener jar** (Iceberg `bytesReadFromCache` / `bytesReadExternally`
  per query) — calls into `shelf-cost` over a thin FFI / SQL UDF
  layer so per-query `$/query` cuts on the dashboard share the
  same constants.

Centralising the formula in one library prevents the three
surfaces from drifting on coefficient values, region overrides,
or rounding behaviour. Copying the constants is a review-blocking
anti-pattern; this design note enshrines that.

## What the counter and gauge expose

### `shelf_s3_dollars_saved_total{region, outcome}`

`IntCounterVec`, unit **cents**. `region` is the literal AWS region
string the operator chose in `cache.cost.region` (default
`us-east-1`); `outcome ∈ {hit_memory, hit_disk, peer}` maps to
the local DRAM tier, the local NVMe tier, and the SHELF-23 peer
fetch path respectively. The counter is bumped exactly once per
cache hit, by an integer number of cents computed from the
formula below.

The series unit is `cents` — **not** `dollars` — because the
underlying type is `i64`. Lying to operators with a `dollars`
unit when the integer is cents would force the dashboard to
multiply by `0.01` invisibly, which is exactly the kind of
unit confusion we built the test pin against. Grafana renders
the cents-saved-this-month tile as `currencyUSD` after an
explicit `* 0.01`.

### `shelf_s3_dollars_saved_rate_cents_per_sec{region, outcome}`

`IntGaugeVec`, unit **cents/sec**, computed by an in-process
1 Hz updater task as a 60-sample sliding window over the
counter. The gauge exists so dashboards don't have to write
`rate(shelf_s3_dollars_saved_total[60s]) * 0.01` everywhere —
the `* 0.01` cents-to-dollars conversion belongs to the panel
config, not to every PromQL query.

Cardinality is bounded: typical clusters have one region + three
outcomes ⇒ ≤ 6 child series. The updater task short-circuits when
the cost wiring is disabled.

## The formula

For a single cache hit:

```
saved_µ¢ =
      get_request_µ¢                                                    (1)
    + bytes × az_rate_µ¢_per_GiB / 2^30                                  (2)
    + bytes × nat_rate_µ¢_per_GiB × nat_bps / 10_000 / 2^30              (3)

saved_¢ = saved_µ¢ / 1_000_000      (truncating; sub-cent residue dropped)
```

- (1) is **always** charged: every cache hit avoids exactly one
  S3 GET call against origin (the `s3_shim` already coalesces
  client-side range-GETs into a single logical hit at this seam,
  so we never double-charge).
- (2) is the AZ-aware data-transfer term; rate selected from
  `same_az_micro_cents_per_gib` or `cross_az_micro_cents_per_gib`
  per the `peer_az` flag on the `HitEvent`. The default `peer_az`
  the shim emits today is `SameAz` (see "AZ assumptions" below).
- (3) is the NAT-gateway data-processing term; **only** contributes
  when the operator explicitly affirms a non-zero
  `nat_traversal_basis_points`. EKS clusters using an S3 VPC
  gateway endpoint never charge NAT for S3 traffic, so the
  default is 0 and the term is silent.

All multiplications happen in `i128`; the final divide-by-`2^30`
and divide-by-`1_000_000` happen at the boundary so the model
never touches a float. `Cents` is an `i64` newtype so the
cumulative counter survives weeks of accretion without `f64`
mantissa rounding.

### Why `µ¢/GiB`, not `µ¢/byte`

The first revision of this crate stored data-transfer as
`µ¢/byte`. The unit looks natural — bytes are what we have at
the call site — but it can't represent any of the AWS
data-transfer rates without huge rounding error:

| Rate              | µ¢/byte            | Stored as `i64`         | Rounding residue |
|-------------------|--------------------|-------------------------|------------------|
| $0.01/GiB cross-AZ | 0.000_931 µ¢/byte | `0` (under-charge)       | -100 %           |
| $0.01/GiB cross-AZ | 0.000_931 µ¢/byte | `1` (over-charge)        | **+1 075×**      |
| $0.045/GiB NAT     | 0.004_19 µ¢/byte  | `0` (under-charge)       | -100 %           |

Storing `µ¢/GiB` instead lets every published rate land on the
integer grid exactly: $0.01/GiB = `1_000_000 µ¢/GiB`,
$0.045/GiB = `4_500_000 µ¢/GiB`, $0.00/GiB = `0 µ¢/GiB`.
`dollars_saved` then divides by `2^30` once, in `i128`, after
multiplying by the byte count. Zero rounding residue on the
coefficient table; the only truncation is the µ¢ → ¢ floor at
the very last step, capped at the `< $10` envelope SHELF-40's
gate accepts.

## Coefficient citations

See the `crates/shelf-cost/README.md` table. Every stored value
points at a specific AWS price-list URL with an `AS_OF` date.
The README also carries the **stale-price refresh runbook**:

1. Open <https://aws.amazon.com/s3/pricing/>. Confirm S3 Standard
   Tier-1 GET rate for `us-east-1` and `ap-south-1`.
2. Open <https://aws.amazon.com/ec2/pricing/on-demand/>. Confirm
   the **Data Transfer** cross-AZ rate.
3. Open <https://aws.amazon.com/vpc/pricing/>. Confirm the NAT
   gateway data-processing rate.
4. If anything changed: edit `crates/shelf-cost/src/coefficients.rs`,
   bump the `AS_OF:` doc-comment, run
   `cargo test -p shelf-cost --all-targets` (the `coefficients::tests`
   suite pins the values literally and will fail loudly).
5. Append a one-liner to `CHANGELOG.md`'s `[Unreleased]`:
   `chore(shelf-cost): refresh coefficients to AWS price list YYYY-QN`.
6. Sign-off + push.

If a price refresh requires a structural change (AWS adds a fifth
line item, splits cross-AZ into intra-region cross-AZ vs
cross-VPC, etc.), the refresh PR also amends this design note
and the workspace memory at `~/trino/AGENTS.md` so future agents
inherit the new shape.

## Hot-path performance

Acceptance criterion: "wiring overhead must be one atomic add per
request — verified by < 5 ns/call".

The micro-benchmark at `crates/shelf-cost/benches/dollars_saved_atomic_add.rs`
exercises the three hit-shape variants the shim emits
(`Memory{SameAz}`, `Disk{SameAz}`, `Peer{CrossAz}`). The bench
intentionally measures the **formula** in isolation — the
Prometheus counter bump in `shelfd::cost::CostState::observe` is
trivial (an inlined `IntCounterVec.with_label_values(...).inc_by(u64)`)
and is exercised end-to-end by the integration test below. The
benchmark output is not committed to CI as a hard gate; it's a
"someone broke the hot path" leading indicator that the
benchmark harness exists at all.

## Auditability

Every contribution to the counter can be reconstructed offline
from the `cache.cost` block in the rendered `shelfd.yaml`
ConfigMap + the per-region preset table in
`crates/shelf-cost/src/coefficients.rs`. The model never reads
clocks, never reads environment, never reads a remote pricing
API — the formula is **pure** w.r.t. its `(region, override
overrides…, HitEvent)` inputs. `shelfctl rep <pod>` exposes the
same `region` label downstream consumers use, so a customer's
Audit step is "look at the YAML, grep the citation, run the
arithmetic" without touching the running pod.

## AZ assumptions

The shim today wires every hit as `peer_az = SameAz` via
`shelfd::cost::DEFAULT_PEER_AZ`. This is a deliberate
**pessimistic** default: without explicit AZ-aware membership data
(SHELF-23 surfaces it via the resolver's `peer_az` per-pod
field; SHELF-20 has the wiring in place), modelling every hit as
same-AZ guarantees the counter never inflates by claiming
cross-AZ savings that didn't happen. Operators who confirm a
multi-AZ Trino-per-shelfd pairing can flip this contract via a
future per-pod override on `CostState`; a same-AZ assumption is
the conservatively-correct OSS-default.

## Interaction with SHELF-37 (Iceberg event listener)

SHELF-37 (separate ticket, separate jar) emits per-query
`bytesReadFromCache` / `bytesReadExternally` from the Trino
event listener into `cdp.trino_logs.trino_queries`. With both
shipped, a per-query / per-table `$/query` cut becomes:

```
saved_per_query = bytesReadFromCache * cross_az_rate / 2^30 / 1e6
                + (queries_count) * get_request_µ¢ / 1e6
```

This calculation is performed dashboard-side by Grafana, not in
shelfd. The SHELF-40 counter is the **rate** cut; the SHELF-37
listener is the **per-query** cut. Both share the same
coefficients via the `shelf-cost` crate (SHELF-37's UDF wiring
will link `shelf-cost` directly).

## Configuration

`cache.cost.*` block on the chart values:

| Key                                  | Type      | Default        | Notes                                                          |
|--------------------------------------|-----------|----------------|----------------------------------------------------------------|
| `cache.cost.enabled`                 | bool      | `true`         | Master switch.                                                 |
| `cache.cost.region`                  | string    | `us-east-1`    | Picks preset; supported: `us-east-1`, `ap-south-1`.            |
| `cache.cost.getRequestMicroCents`    | int       | preset         | Override.                                                      |
| `cache.cost.sameAzMicroCentsPerGib`  | int       | preset         | Override.                                                      |
| `cache.cost.crossAzMicroCentsPerGib` | int       | preset         | Override.                                                      |
| `cache.cost.natProcessingMicroCentsPerGib` | int | preset         | Override.                                                      |
| `cache.cost.natTraversalBasisPoints` | int       | `0`            | 0..=10 000. Operator affirmation that traffic crosses NAT.     |

An operator-specific overlay (e.g.
`infra/<operator>/charts/shelf/values-prod.yaml` for a Mumbai
cluster) flips `region: ap-south-1` and inherits the published
ap-south-1 preset; it leaves `natTraversalBasisPoints` at `0` if
the cluster reaches S3 via a VPC gateway endpoint, or sets it to
`10000` if NAT-egress to S3 actually applies.

## Testing

- **Unit tests** (`crates/shelf-cost/src/lib.rs`, `coefficients.rs`,
  `config.rs`) — every `HitEvent` variant + every preset constant
  pinned literally to the published AWS rate.
- **Property tests** (`crates/shelf-cost/tests/property.rs`):
  - additive identity over arbitrary hit sequences
    (`sum(individually) == sum(aggregate)`, exact);
  - `dollars_saved >= 0` on any non-negative coefficient table;
  - cross-AZ ≥ same-AZ for any byte count;
  - any negative coefficient routed through `CostConfig` is
    rejected at load by `CostConfigError::NegativeCoefficient`.
- **Micro-benchmark** (`crates/shelf-cost/benches/dollars_saved_atomic_add.rs`)
  for the < 5 ns/call gate.
- **Integration test** (`shelfd/tests/it_dollars_saved.rs`,
  `SHELF_INTEGRATION=1`) — drives the SHELF-22 shim with 100
  warm reads and asserts `shelf_s3_dollars_saved_total` advanced
  by **exactly** the cents the public formula predicts. The
  fixture overrides the NAT term to a synthetic value so the
  per-hit cents is non-zero (a 4 MiB hit with the real
  $0.045/GiB rate produces only 0.018 cents, which floors to 0
  and is unobservable). The synthetic value is clearly marked
  as a SHELF-40-test fixture, NOT a published rate, so refresh
  reviews don't mistake it for a real coefficient.
- **Metrics regression tests**
  (`shelfd::metrics::registry_exposes_documented_series` and
  `metrics_scrape_contains_documented_series_after_touch`) carry
  the two new series in `EXPOSED_SERIES`.

## Out of scope

- **Real-time AWS price refresh.** A future ticket will run the
  `awspricing` MCP on a cron and open a PR when any constant
  drifts; today the refresh is the manual quarterly runbook
  above.
- **Per-customer / contractual discount.** A 30 %–60 % EDP
  discount is real money on the procurement deck but is also a
  contract artefact, not a published rate. Operators with such
  a discount supply explicit `*MicroCents*` overrides in their
  values overlay; the citation field in the README does not
  pretend to model EDP.
- **PUT / LIST / KMS / S3 IA tier savings.** Out of scope for
  v1; the `HitEvent` enum is structured so a future
  `HitEvent::Put { … }` variant lands without touching the
  existing call sites.
- **Per-bucket attribution.** The bucket-level cut comes from
  Athena `s3_access_logs_db.*_logs_v2` tables, not from this
  counter.
