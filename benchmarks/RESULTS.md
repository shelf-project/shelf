# Shelf benchmark results

_Authoritative aggregate summary. One row per `(release_tag, backend, benchmark)`.
Numbers are copied here by CI after a green nightly full run; never
hand-edited. Raw data links to `results/<date>/<backend>/<run-id>.json`._

Status: **EMPTY AT LAUNCH**. Rows appear as each benchmark produces its
first real run after Phase 1 (SHELF-26) lands.

---

## How this file is populated

1. Nightly `.github/workflows/bench.yml` finishes a full run.
2. The `publish-results` job appends one row per `(tag, backend, bench)`
   to the table below, sorted `tag desc, backend, bench`.
3. The same job opens a PR titled `results: <tag>` for manual review.
4. On merge, the raw Parquet + JSON artefacts are uploaded to the
   results bucket (see `results/README.md`).

If you are editing this file by hand, you are doing it wrong.

---

## Benchmark results

| release_tag | backend          | benchmark    | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | hit rate | $/query | raw |
| ----------- | ---------------- | ------------ | -------- | -------- | -------- | ---------- | -------- | ------- | --- |
| _TBD_       | _TBD_            | _TBD_        | —        | —        | —        | —          | —        | —       | —   |

_Empty until SHELF-26 lands. See `README.md` for the reproducibility
contract that every row above must satisfy._

---

## v0.5 gate board (ADR-0010)

This sub-table is the **kill-switch**. It is the single view the eng-lead
signs off on before Phase 2 can begin. The gate is evaluated from the
7-day rolling window of the `replay` benchmark on rep-2.

| date (UTC) | backend | 7-day hit rate | GOLD_DBT ok% | p95 vs Alluxio | Shelf-caused pages | Oncall surface | Verdict |
| ---------- | ------- | -------------- | ------------ | -------------- | ------------------ | -------------- | ------- |
| _TBD_      | shelf   | —              | —            | —              | —                  | —              | pending |

Targets (all five must hold for 7 consecutive days):

- 7-day cumulative hit rate ≥ 71 %
- `GOLD_DBT` DAG ok-rate ≥ 99.9 %
- p95 ≤ 120 % of Alluxio baseline
- Shelf-attributed pages = 0
- Oncall surface ≤ 50 % of Alluxio's 7-day rolling rate

If any cell drops below target, the verdict column reads `FAIL: <metric>`
and the 2-week gap-analysis window starts (see ADR-0010).

---

## Changelog

| date       | tag      | notes                                          |
| ---------- | -------- | ---------------------------------------------- |
| 2026-04-23 | v0.0     | Scaffold only. No real results yet.             |
