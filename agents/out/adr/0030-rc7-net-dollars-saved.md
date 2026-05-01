# ADR 0030 — rc.7 net dollars-saved metric (`shelf_s3_dollars_saved_net_total`)

- Ticket: SHELF-A4 (rc.7 measurement-substrate completion).
- Status: Accepted (rc.7 release-prep).
- Companions: ADR 0011 (cache-key spec), ADR 0022 (rc.6 cost-region chart default), ADR 0030 builds atop SHELF-40's gross counter, the operator-facing close of the procurement story.

## Context

SHELF-40 shipped `shelf_s3_dollars_saved_total` — the audit-able
gross counter that translates every cache hit into cents saved
against published AWS pricing. The gross counter answers "how
much origin S3 + cross-AZ traffic did the cache avoid?", which
is the right number for engineering hit-rate dashboards but the
**wrong** number for a procurement-facing
"is the cache earning its keep?" panel.

The gap that procurement actually cares about is *net*: gross
savings minus the amortised running cost of the shelf-pool itself
(EKS pod compute, EBS volumes, EKS control plane, NAT egress,
operator amortisation). For the canonical penpencil overlay this
amortisation is approximately:

```
6 × m5a.4xlarge × $0.864/hr (ap-south-1 on-demand)  ≈  $5.18 /hr
```

…but the exact number is operator-specific and must come from
the AWS CUR (`cost-db.cloud_costs` table) of each cluster. We
intentionally **do not** model this internally — the audit story
breaks if shelfd starts inferring "your pool costs $X/hr" from
node tags.

A naive implementation could simply expose a derived gauge

```
shelf_s3_dollars_saved_net_dollars_per_hour
  = rate(shelf_s3_dollars_saved_total[1h]) - 5.18
```

…built in PromQL on the dashboard. The reasons we don't:

1. **Anti-overclaim.** A purely PromQL-side derivation lets any
   operator publish a "net" number against an arbitrary `5.18`
   they pulled from a runbook three quarters ago. We need the
   amortisation rate to be **part of the deployed config**, surfaced
   as a Prometheus gauge, and explicitly authorised by whoever
   pushed the Helm overlay.
2. **Stale-rate detection.** A gauge at zero is trivially detectable
   on a dashboard ("net accounting dormant"); a missing PromQL
   variable is not.
3. **Counter (not gauge) for net.** Procurement wants a cumulative
   number ("the cache has saved $X net to date"), not a snapshot
   rate. Counters preserve that without a Prom recording rule.

## Decision

Add an inline accountant module to `shelfd` (same crate as the
existing SHELF-40 wiring) that:

1. **Reads** the cumulative gross from
   `shelf_s3_dollars_saved_total{region=<self>, outcome=*}` once per
   `NET_TICK_INTERVAL` (10 s).
2. **Subtracts** `amortized_dollars_per_hour × elapsed` over the
   interval, in fixed-point µ¢ arithmetic.
3. **Bumps** `shelf_s3_dollars_saved_net_total{region=<self>}` by the
   resulting cents — but **only** when the delta is positive.
   Counters cannot decrement; intervals where amortisation outpaces
   gross savings keep the net counter flat, which the dashboard
   reads as "gross > net ⇒ cache currently underwater".
4. **Always** publishes `shelf_pool_amortized_dollars_per_hour`
   (in integer cents per hour) so dashboards can detect dormant
   accounting from the gauge alone.

### Anti-overclaim guard

The accountant **refuses** to publish to the net counter if
`cache.cost.amortized_dollars_per_hour` is unset (`None`). The
companion gauge still emits `0`, which is the documented
dashboard signal for "net accounting dormant."

The same guard fires on `NaN` / `Inf` / negative values, which
fail at config load via `CostConfig::from_config`'s validation
walk (`InvalidAmortization` error).

### File / metric layout

| Path | Change |
| --- | --- |
| `crates/shelf-cost/src/config.rs` | New field `amortized_dollars_per_hour: Option<f64>` + `validated_amortized_dollars_per_hour()` accessor + `InvalidAmortization` error variant. |
| `crates/shelf-cost/src/lib.rs` | `CostModel::from_config` validates the new field at boot. |
| `shelfd/src/cost.rs` | New `NetCostAccountant` (Arc-shared, `parking_lot::Mutex` tick state, `Instant`-based monotonic clock). |
| `shelfd/src/metrics.rs` | New `S3_DOLLARS_SAVED_NET_TOTAL` (`IntCounterVec`, label `region`) + `POOL_AMORTIZED_DOLLARS_PER_HOUR` (`IntGauge`). Both added to `EXPOSED_SERIES` + the registry-touch test. |
| `shelfd/src/main.rs` | Constructs the accountant after `cost_state.spawn_rate_updater(...)` and spawns its updater task on the same shutdown token. |
| `charts/shelf/values.yaml` | New (commented-out) `cache.cost.amortizedDollarsPerHour` knob; default = unset. |
| `charts/shelf/templates/configmap-shelfd.yaml` | Conditional render to `amortized_dollars_per_hour:` snake-case key. |
| `infra/penpencil/charts/shelf/values-prod.yaml` | Sets `amortizedDollarsPerHour: 5.18` with the derivation comment + "verify against actual EKS bill" caveat. |

## Consequences

### Operator-visible

- A freshly deployed OSS cluster sees `shelf_pool_amortized_dollars_per_hour = 0`
  and **no** `shelf_s3_dollars_saved_net_total` series. Dashboards
  must alert on that case ("net accounting dormant").
- Operators who flip `cache.cost.amortizedDollarsPerHour` start
  publishing the net counter on the **next** 10-second tick
  after pod restart. Existing gross series keep their values; the
  net counter resets to 0 because it's a new label set.
- `cache.cost.amortizedDollarsPerHour` is **not** hot-reloaded —
  shelfd reads YAML at boot only, per the workspace
  no-hot-reload rule. Operators must `kubectl rollout restart sts/shelf`
  after changing the value.

### Test discipline

- `crates/shelf-cost` config tests cover: unset, explicit zero,
  positive, negative-rejected, NaN-rejected.
- `shelfd::cost` unit tests cover: unset (refuses), set-to-zero
  (publishes gross only), set-to-positive (correct subtraction),
  multiple-tick accumulation, region-label propagation through the
  metric. Tick state is exercised through the test-only
  `tick_at(gross, now)` to avoid `tokio::time::pause` ceremony.

### Cost / risk

- Wiring overhead per pod: one tokio task ticking at 10 Hz that
  performs three `IntCounterVec::with_label_values(...).get()`
  reads, an i128 multiply / divide, and at most one `inc_by` per
  tick. Negligible against the existing SHELF-40 rate updater.
- Anti-overclaim default is "silent" — same shape as the
  workspace's standing rule that operator-facing money numbers
  must be cluster-side authorised before they can render.

### Rollback

- Single Helm value: set `cache.cost.amortizedDollarsPerHour: null`
  (or remove the line) → restart pods → counter goes silent. The
  previously-published series ages out of Prometheus per its
  retention; no in-shelfd rollback path is needed.

## References

- Workspace memory bullet (Apr 30 EOD) — "shelf-pool 6 pods ×
  m5a.4xlarge ≈ $3,800/mo on EKS+EBS; SHELF-40 ships gross only,
  procurement needs net".
- `crates/shelf-cost/README.md` — citation block for the per-region
  AWS pricing constants the gross counter consumes.
- ADR 0011 — cache-key spec (orthogonal; ADR 0030 lives entirely
  in the metrics layer and does not touch cache keys).
- ADR 0022 — chart-default region knob the SHELF-40 counter uses;
  the same `region` label flows into ADR 0030's net counter.
