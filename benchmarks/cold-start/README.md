# `cold-start/` — 2 → 20 worker scale-up TTFQ

See [SPEC.md](SPEC.md) for the authoritative specification.

- `run.sh` — runner (scaffolding).
- `schema.json` — JSON Schema Draft 2020-12 for the result JSON.
- `SPEC.md` — goal, workload, method, metrics, reporting, reproduce cmd.

## Quick invocation

```bash
./run.sh --backend=shelf
./run.sh --backend=fs-cache --cycles=1   # faster smoke
```

Outputs land under `../results/<YYYY-MM-DD>/<backend>/cold-start-<run_id>.json`.

## What success looks like

Phase 2 gate (plan §6.5): Shelf TTFQ p95 ≤ 3 s with
`ShelfPrefetchListener` enabled. Claim in `BLUEPRINT.md` §10.2 to be
validated or refuted by this benchmark: Shelf ≈ 1-2 s, fs.cache ≈ 15-40 s,
raw S3 ≈ 8-15 s.
