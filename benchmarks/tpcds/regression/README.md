# TPC-DS regression gate (F4)

The tag-time gate runs shelf-only SF100 and fails if any query
regresses more than:

- **10 %** for a general query
- **5 %** for a "parity" query (Track E committed wins)

## Files

- [`check_regression.py`](./check_regression.py) — compares a fresh
  `runner/run.py` CSV against `baseline/shelf-sf100.csv`.
- [`../baseline/shelf-sf100.csv`](../baseline/shelf-sf100.csv) — the
  frozen baseline. Refresh by replacing the file and landing a PR
  whose title starts with `chore(bench): refresh SF100 baseline`.

## Refreshing the baseline

```bash
python benchmarks/tpcds/runner/run.py \
  --engine shelf --sf 100 \
  --out benchmarks/tpcds/baseline/shelf-sf100.csv \
  --warm-state mixed --timeout 900
```

The CSV is intentionally checked in — small enough (~15 KB) that
review-time diffs are meaningful and large enough to capture the
99-query surface.

## Why SF100 and not SF1000

SF1000 takes 2-4 hours on the self-hosted runner; SF100 catches
>90 % of regressions in under 20 minutes. The full SF1000 run lives
on a separate nightly workflow (TBD) and backfills the public
scorecard in `cost/summary.md`.
