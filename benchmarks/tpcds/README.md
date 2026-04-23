# `tpcds/` — TPC-DS @ 1 TB

See [SPEC.md](SPEC.md) for the authoritative specification.

- `run.sh` — runner (scaffolding; emits a schema-valid skeleton record).
- `schema.json` — JSON Schema Draft 2020-12 for the result JSON.
- `SPEC.md` — goal, workload, method, metrics, reporting, reproduce cmd.

## Quick invocation

```bash
./run.sh --backend=shelf --scale=1tb --iterations=3
./run.sh --backend=shelf --profile=smoke   # 3 queries, CI-safe
```

Outputs land under `../results/<YYYY-MM-DD>/<backend>/tpcds-<run_id>.json`.

## Relation to the v0.5 gate

TPC-DS is **not** the v0.5 gate. See ADR-0010 — the gate is
`replay/` on rep-2. TPC-DS is the number we publish for external
reviewers and the launch blog.
