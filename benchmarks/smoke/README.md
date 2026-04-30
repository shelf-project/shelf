# SHELF-12 · Docker-Compose Smoke Harness

A minimal, self-contained stack that proves the Phase-0/1 read path
end-to-end:

```
trino-coordinator (480) ─┬─▶ shelf-trino-plugin
                         │         │
                         │         └─▶ shelfd (Rust)
                         │                  │
                         └────────▶ MinIO ◀─┘   ← iceberg-warehouse bucket
```

The harness lives entirely under `benchmarks/smoke/` so it is trivial
to run, cheap to throw away, and impossible to confuse with the
production deploy.

## Quick start

```bash
make -C benchmarks/smoke build        # build the shelfd image
make -C benchmarks/smoke up           # compose up + wait healthy
make -C benchmarks/smoke smoke        # cold + warm + assert
make -C benchmarks/smoke down         # clean up (removes the minio volume)
```

Or from the repo root:

```bash
make smoke          # whole loop (assumes `make smoke-up` already ran)
make smoke-down
make smoke-logs
```

## What it checks

1. Trino 480 + Iceberg (Hadoop catalog on MinIO) answers all 10
   canonical queries — see `seed/queries/`.
2. Running them a second time produces byte-identical output (no
   non-determinism from an Iceberg split reshuffle, nothing cached
   wrong).
3. `shelfd`'s `/metrics` endpoint shows
   `shelf_hits_total{pool="metadata"}` **or**
   `shelf_hits_total{pool="rowgroup"}` strictly greater on the warm
   run than the cold run. This is the deferred conformance gate on
   SHELF-15 (footer prefetch) and SHELF-20 (HRW routing).

## Scope / non-goals

See `docs/SHELF-12-design-notes.md`. Short version:

- Single-pod shelfd. Pod rotation on SHELF-20 is **code-path
  verified** (the plugin's `MembershipResolver` still runs), not
  **fleet-scale verified**.
- One Trino coordinator, no workers. Good enough to exercise the
  plugin FS factory + prefetch listener; not a scheduler test.
- ~1000 rows total. The harness is for correctness, not throughput.
- No cold-start, no spot-churn, no TPC-DS. Those live in
  `benchmarks/{cold-start,spot-churn,tpcds}`.

## Results layout

`run-smoke.sh` writes under `benchmarks/smoke/results/`:

```
results/
├── cold/NN.txt                 # per-query output on the cold run
├── warm/NN.txt                 # per-query output on the warm run
├── metrics-after-cold.txt      # shelfd /metrics after cold queries
└── metrics-after-warm.txt      # shelfd /metrics after warm queries
```

Results are `.gitignore`d — run locally to reproduce.
