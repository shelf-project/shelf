# ADR-0028 — A4 (rc.7) net dollars-saved metric

| Status | Date | Author |
|---|---|---|
| Accepted (rc.7 prep) | 2026-05-01 | shelf-project |

## Context

SHELF-40 / PR #68 shipped the gross savings counter
`shelf_s3_dollars_saved_total` (in cents) — every cache hit credits
the audit-able formula in `crates/shelf-cost/`. Procurement asks the
next question — *minus the cost of running the shelf pool itself*.

Reference figure: a 6-pod shelf pool on `m5a.4xlarge` in
`ap-south-1` runs `6 × $0.864/hr ≈ $5.18/hr` on On-Demand list
price. If the gross counter is only ticking $4/hr, the cluster is
*losing* money on shelf — the gross counter alone hides that.
Cluster TCO conversations require a **net** number that subtracts
the pool's amortized cost so the cumulative reading is "money
genuinely returned to the business by running shelf".

## Decision

Ship A4 inline in `shelfd` (one new module + two new metrics + one
new config field).

### Surface

1. **`shelfd::cost::NetCostAccountant`** (new struct in the
   existing `shelfd/src/cost.rs`). Stores the operator-supplied
   amortization as integer micros, runs a per-tick subtraction on
   the gross counter, returns a clamped `i64` net delta. Anti-
   overclaim guard: unset / zero / negative / NaN /
   non-finite ⇒ `tick()` returns `None` and the net counter
   stays at zero.

2. **`shelf_s3_dollars_saved_net_total{region}` IntCounterVec** —
   cumulative net savings in **dollar-micros** (1 cent =
   10_000 µ$, 1 dollar = 1_000_000 µ$). Divide by `1e6` to render
   dollars. Counter is monotonic — per-tick negative deltas
   (gross stalled while pool burned amortization) are clamped to
   zero so the cumulative number only ever moves forward.

3. **`shelf_pool_amortized_dollars_per_hour` IntGauge** —
   amortized pool cost in **dollar-micros / hr**. **Always
   exposed** (regardless of whether the net counter publishes) so
   dashboards can flag a `0` reading as the operator-misconfig
   signal.

4. **`cache.cost.amortizedDollarsPerHour` Helm key**
   (camelCase in values.yaml; serde-snake_case
   `amortized_dollars_per_hour: Option<f64>` on
   `shelf_cost::CostConfig`). Default unset.

5. **`shelfd::cost::spawn_net_accountant()` task** — spawned from
   `main.rs` next to the existing SHELF-40 rate updater. Runs on
   a 60s ticker; reads the sum-across-labels of
   `shelf_s3_dollars_saved_total`, converts cents → dollar-micros,
   feeds the accountant, credits the clamped delta to the net
   counter, always updates the gauge. Cancels on the same
   `shutdown` token the data plane already observes.

### Why not a separate `crates/shelf-cost-net` crate

`shelf-cost` is a pure data/model library (no async, no IO, no
prometheus). The accountant needs to scrape live counter values
and bump a counter — both `shelfd` runtime concerns. Splitting
this into a third crate would force `shelf-cost` to grow a
prometheus dependency or invent a mediator trait, neither of
which buys decoupling proportional to the surface area (one
module, ~150 LOC).

### Why anti-overclaim guard via `Option<f64>` (not a default)

Procurement reads cumulative numbers, then converts them into
TCO/ROI conversations. Defaulting `amortized_dollars_per_hour` to
`0.0` would silently make every gross-savings dollar a "net
savings" dollar — the cluster appears to print money even when
it's running a `c6a.4xlarge`-induced OOM cascade. The guard
forces the operator to acknowledge the pool cost up front;
`shelf_pool_amortized_dollars_per_hour = 0` is the visible
signal that procurement should not yet trust the net counter.

This mirrors the SHELF-40 "fail loud on misconfig" pattern (the
gross counter refuses to register on negative coefficients or
unknown regions).

## Consequences

### Positive

- **Procurement ROI conversation now lives on a single counter**
  (cumulative dollar-micros / 1e6 = $) instead of operators
  having to subtract pool cost by hand from a Grafana panel.
- **Default-on, default-safe**: gross counter unchanged; net
  counter only publishes when the operator explicitly opts in.
- **Cardinality unchanged in practice**: net counter is labelled
  by `region` only (one series per cluster in the OSS default
  single-region case).
- **Zero hot-path cost**: the accountant runs on a 60 s ticker;
  the data plane never sees it.
- **Anti-overclaim by construction**: `Option<f64>` field +
  `is_publishable()` gate means there is no "default that silently
  inflates the number" failure mode.

### Negative

- One more knob operators must set to get the full ROI surface
  (mitigated by the 6-pod m5a.4xlarge reference figure documented
  in `charts/shelf/values.yaml` and `infra/penpencil/charts/shelf/values-prod.yaml`).
- The `5.18` reference figure is List-Price On-Demand — Spot /
  Reserved / EDP discounts can lower this 30–60 %. Operators must
  re-derive against their actual EKS bill before merging an
  overlay value (the chart comment explicitly calls this out).
- Counter unit (dollar-micros) differs from the gross counter
  (cents) — every dashboard PromQL expression that mixes the two
  has to apply a `× 0.0001` factor on one side. We pay this once
  in the dashboard JSON and never again.

### Rollback

| Trigger | Action |
|---|---|
| `shelf_s3_dollars_saved_net_total` shows negative growth (cumulative regression) | Bug in `tick()` arithmetic; revert by setting `cache.cost.amortizedDollarsPerHour: null` (or removing the key) — gauge will read 0 and counter stays at last value. |
| Operator sees net counter at 0 unexpectedly | Check `cache.cost.amortizedDollarsPerHour` is set; gauge `shelf_pool_amortized_dollars_per_hour` should show non-zero (in dollar-micros / hr). |
| Pod cost reference (5.18 $/hr) drifts | Update `infra/penpencil/charts/shelf/values-prod.yaml` overlay; chart re-render is a single ConfigMap reconcile (~2 min on `data-platform-cluster`) followed by `kubectl rollout restart sts/shelf` — config is read at startup, not hot-reloaded. |

## References

- **SHELF-40 / PR #68** — gross savings counter
  (`shelf_s3_dollars_saved_total`, cents). The A4 net counter
  layers on top by reading the same source-of-truth and
  subtracting the operator-supplied amortization.
- **Workspace memory bullet** "Apr 30 EOD findings on
  procurement gap" — the original framing that gross-only is
  incomplete for TCO/ROI conversations and procurement requires
  net.
- **`agents/out/adr/0024-rc6-per-pod-rss-watermark-alerts.md`** —
  precedent for a "configure-or-stay-quiet" gauge (the watermark
  alert that prefers operator silence over a wrong default).
