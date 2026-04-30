# ADR 0022: Production overlay defaults `cache.cost.region=ap-south-1`

_Status: Accepted (2026-04-30)_
_Deciders: shelfd-maintainers_
_Supersedes: none_
_Superseded-by: none_

## Context

SHELF-40 (PR #68) shipped `shelf_s3_dollars_saved_total`, a Prometheus
counter labelled by region preset. The counter is fed by
`crates/shelf-cost`, which ships per-region price coefficients
sourced from the published AWS price list (`us-east-1`, `ap-south-1`
shipped at v1.0.0-rc.5; more on demand).

The chart template at
`charts/shelf/templates/configmap-shelfd.yaml` reads
`.Values.cache.cost.region` and writes it verbatim into shelfd's
`shelfd.yaml`. The default in `charts/shelf/values.yaml` is
`region: us-east-1`, which matches the chart's "neutral OSS
defaults" convention (every example in the chart uses `us-east-1`,
the canonical AWS region).

The penpencil cluster — which is the ground-truth deploy target for
the rc.5 → 1.0.0 rollout — runs in `ap-south-1` (Mumbai). Without an
explicit override, every saved S3 GET would be priced at the
`us-east-1` coefficient, which under-counts the actual savings by
~10 % (the Mumbai / N.Virginia per-1k-GET ratio) and wires the
**wrong region label** onto every series in `shelf_s3_dollars_saved_total`.
Downstream Grafana panels and the rc.5 cutover-impact analysis
depend on that label being right.

This ADR codifies the prod overlay default and adds a CI gate so
that a nested rebase or a values-file refactor cannot silently drop
the override without the build going red.

## Decision

1. **`infra/penpencil/charts/shelf/values-prod.yaml` ships a
   `cache.cost.region: ap-south-1` override** (already committed
   under PR #68; this ADR documents the decision retroactively).
2. **The helm-lint workflow asserts the rendered manifest contains
   `region: "ap-south-1"` (quoted or unquoted) for the prod
   overlay** on every PR that touches `charts/**`, `infra/**`, or
   the workflow itself. Failure is fatal.
3. **The overlay path stays under `infra/penpencil/`**, the
   release-time `git rm` set, so the published OSS tag does not
   carry a Mumbai-specific default. Operators in other regions
   override `cache.cost.region` in their own overlay; the chart
   default (`us-east-1`) keeps the OSS-neutral story intact.

## Why nest under `cache:` rather than top-level

The chart template reads `.Values.cache.cost`. Promoting `cost:` to
the top level would require either (a) a parallel template path
that reads from both locations, or (b) a breaking config-key
rename. Both are net-negative for an rc.6 ops change with no
runtime payoff — the actual contract surface is shelfd's
`shelfd.yaml`, which has always been a flat structure regardless of
where the value lives in `values*.yaml`. We keep `cache.cost` as
the canonical key and revisit only if a future "split caching from
costing into separate top-level blocks" refactor lands.

## Consequences

- **Positive**: dollar-savings counter labelled correctly on
  ap-south-1; CI catches accidental drops of the override.
- **Negative**: the assertion is overlay-specific. Other operators
  who fork `values-prod.yaml` for a different region must update
  the regex (or, more cleanly, run their own helm-lint workflow
  scoped to their overlay file). The default chart behaviour is
  unaffected.

## References

- `crates/shelf-cost/src/coefficients.rs` — the `ap-south-1` preset
  with citations to the published AWS price list (S3 Standard GET,
  Mumbai data-transfer rates, NAT-gateway processing).
- `infra/penpencil/charts/shelf/values-prod.yaml#cache.cost` — the
  block this ADR locks in.
- `.github/workflows/helm-lint.yml` step
  "Assert prod overlay renders cost.region=ap-south-1".
