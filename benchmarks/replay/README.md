# `replay/` — 7-day rep-2 trino_queries replay

See [SPEC.md](SPEC.md) for the authoritative specification.

- `run.sh` — runner (scaffolding).
- `schema.json` — JSON Schema Draft 2020-12 (includes the `gate` object).
- `SPEC.md` — goal, workload, method, metrics, reporting, reproduce cmd.

## Why this benchmark matters

This is the **v0.5 kill-switch**, per ADR-0010. It is the one
benchmark whose pass/fail single-handedly decides whether Shelf
continues as a project.

The five gate metrics (all must hold for 7 consecutive days):

1. Cumulative hit rate ≥ 71 %.
2. `<your_critical_dag>` ok-rate ≥ 99.9 %.
3. p95 ≤ 120 % of Alluxio baseline.
4. Shelf-attributed pages = 0.
5. Oncall surface ≤ 50 % of Alluxio's 7-day rolling rate.

The gate is evaluated only at `--speed=2x`. Other speeds are reported
but marked `verdict: n/a`.

## Quick invocation

```bash
./run.sh --backend=shelf    --days=7 --speed=2x
./run.sh --backend=shelf    --days=1 --speed=10x  # smoke; verdict=n/a
```

Outputs land under `../results/<YYYY-MM-DD>/<backend>/replay-<run_id>.json`.
