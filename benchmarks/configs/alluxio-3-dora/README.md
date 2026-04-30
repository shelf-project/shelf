# `alluxio-3-dora/` — Alluxio 3.x DORA, stock config

OSS peer comparison. Stock Helm chart, tuned per Alluxio's public 3.x
tuning docs only. **No internal patches carried over from 2.9.5.**
This is the number a new user would get by following Alluxio's own
quick-start.

## Values file shape

- Source: Alluxio Enterprise Helm chart, 3.x line.
- Sizing: matches our bench cluster's Shelf node group (3× NVMe-backed
  pods) so the like-for-like is fair.
- Backend: same S3 fixture bucket as every other backend (parameterised
  in `bootstrap.sh`).

## Why DORA specifically

Alluxio 3.x DORA is the upstream-recommended architecture for
object-store-backed read caching, and is the design our blueprint
positions against (see `COMPARISON.md` §Alluxio 3). Running it gives
reviewers confidence we benchmarked the right Alluxio variant, not
just the old 2.9.

## What we will *not* do

- No private Alluxio Enterprise flags. Every setting must be documented
  in the public Alluxio 3.x docs.
- No cross-tuning from our 2.9.5 knowledge. DORA is architecturally
  different; reusing 2.9.5 values would be wrong and would invalidate
  the comparison.

## TODO_SHELF-26

- Commit stock `values.yaml` matching upstream chart version.
- Verify DORA worker image matches the chart's appVersion.
- Document known DORA gotchas (e.g. worker-side UFS fallback path).
