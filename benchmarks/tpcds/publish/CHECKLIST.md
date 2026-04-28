# Publication checklist

Use this file as a literal PR checklist when preparing the
SF1000 publication. Do not remove items; check them off.

## Engine runs

- `shelf-trino` — 99 queries × (10 cold + 10 warm₁ + 10 warm₂) complete.
- `trino-alluxio` — same shape.
- `starburst-warpspeed` — same shape, warmup indexes built per vendor guidance.
- `firebolt` — same shape on the Firebolt-managed cluster sized per `hardware.md`.
- Checksums of each CSV recorded in `results-sf1000.csv.sig.meta`.

## Gate math

- p50 wins for shelf on ≥ 80/99 queries vs each of the other three engines.
- `$/query` wins for shelf on ≥ 95/99 queries vs each of the other three engines.
- Every losing query has a `residuals.md` entry.

## Provenance

- Two engineers GPG-signed `results-sf1000.csv`.
- Every engine's config commit SHA is recorded under `engines/<name>/config.md`.
- Starburst list price + Firebolt FBU rates captured verbatim in `hardware.md`.

## Third-party rehearsal

- A shelf-naive engineer walked through `replication.md`.
- Their CSV is within ±10 % of the published numbers on every query.
- Any gap larger than ±10 % has a documented reason (AZ variance, spot pricing, etc.).

## Release

- PR linked from `shelf_trino_perf_research.plan.md`.
- Blog post + Grafana snapshot ready.
- `results-sf1000.csv` tagged `sf1000-2026-QX` or similar.