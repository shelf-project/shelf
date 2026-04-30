# `spot-churn/` — kill 50 % of workers every 5 min

See [SPEC.md](SPEC.md) for the authoritative specification.

- `run.sh` — runner (scaffolding).
- `schema.json` — JSON Schema Draft 2020-12.
- `SPEC.md` — goal, workload, method, metrics, reporting, reproduce cmd.

## Quick invocation

```bash
./run.sh --backend=shelf
./run.sh --backend=shelf --warmup-min=5 --run-min=10   # dev smoke
```

Outputs land under `../results/<YYYY-MM-DD>/<backend>/spot-churn-<run_id>.json`.

## What success looks like

Phase 3 gate (plan §3): Shelf hit rate stays ≥ 65 % under this chaos
pattern. BLUEPRINT §10.3 claim to validate: Shelf ≥ 75 %, fs.cache
≈ 20 % after sustained churn.
