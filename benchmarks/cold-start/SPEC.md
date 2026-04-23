# Cold-start benchmark — specification

_Measures time-to-first-query (TTFQ) when Trino scales from 2 → 20
workers. Phase 2 gate metric (plan §6.5)._

- **Status.** Scaffolding (v0.0).
- **Owner.** bench-harness team (agent 7) + trino-plugin-eng-1.
- **References.** `BLUEPRINT.md` §10.2, plan §6.5, SHELF-26.

---

## Goal

Quantify the "cold-start tax" — the penalty each backend pays when
Trino elastic-scales worker count by 10× and new workers have empty
local caches. This is the dashboard-user-visible effect of a scale-up,
and the primary reason the blueprint argues against per-worker caches
(shared Shelf vs fs.cache's N-cold-caches).

Gate: Phase 2's `ShelfPrefetchListener` must bring Shelf's TTFQ p95
to ≤ 3 s after 10× scale-up.

## Workload

- **Dataset.** Same 1 TB Iceberg TPC-DS fixture as `tpcds/`, OR the
  `replay/` fixture (1-day rep-2 slice). Either works; SPEC.md
  defaults to TPC-DS for portability.
- **Query set.** 20 canonical dashboard queries (committed under
  `queries/dashboard-20.sql` — SHELF-26). Each is a selective filter
  over one TPC-DS fact table — the pattern that exposes per-worker
  cache cold-starts.
- **Scale event.** Trino workers 2 → 20 via Karpenter (or the bench
  cluster's equivalent). Scale completes when all 20 pods are `Ready`
  from Kubernetes' viewpoint.

## Method

1. Steady-state warm-up: issue the 20 dashboard queries against a
   2-worker Trino cluster until cumulative hit rate ≥ 80 %.
2. Trigger scale-up (`kubectl scale` or Karpenter event).
3. At the moment the 20th worker goes `Ready`, start issuing the 20
   dashboard queries in parallel (concurrency 20).
4. For each query, record wall-clock from submission to first row
   returned (`TTFQ` = time-to-first-result).
5. Repeat the whole cycle 3× and report per-backend TTFQ quantiles.

**Failure handling.** A query that does not return a row within 60 s
is recorded as `failed=true` with the TTFQ clamped to 60 s — the
alternative (drop from the record) would hide a real failure mode.

## Metrics (definitions + thresholds)

| Metric           | Definition                                              | Threshold (plan §6.5)           |
| ---------------- | ------------------------------------------------------- | --------------------------------- |
| `latency_ns_p50` | Median TTFQ across 20 queries × 3 cycles.               | Reported.                         |
| `latency_ns_p95` | 95th percentile TTFQ.                                   | **Shelf: ≤ 3 s** (gate).          |
| `latency_ns_p99` | 99th percentile TTFQ.                                   | Reported.                         |
| `latency_ns_p999`| 99.9th percentile TTFQ.                                 | Reported.                         |
| `hit_rate`       | Cumulative on the new 18 workers during the first 60 s. | Reported per backend.             |
| `bytes_read`     | Bytes read by the 20-query fan-out.                     | Reported.                         |
| `bytes_admitted` | Bytes admitted into cache during the 60-s window.       | Reported.                         |
| `scale_up_latency_seconds` | Time from `kubectl scale` to 20th worker Ready.| Reported (infra-fairness control).|

Expected values (BLUEPRINT §10.2):

- Shelf: ≈ 1-2 s.
- fs.cache: ≈ 15-40 s (cold per-worker caches).
- raw S3: ≈ 8-15 s.

These are *claims*, not guarantees; this benchmark is designed to
confirm or refute them.

## Reporting format

One JSON per run at:

```
results/<YYYY-MM-DD>/<backend>/cold-start-<run_id>.json
```

Validates against `schema.json`. Publisher aggregates into
`RESULTS.md` as one row per `(release_tag, backend, benchmark=cold-start)`.

## Reproducibility command

```bash
git clone https://github.com/<org>/shelf && cd shelf/benchmarks
export AWS_PROFILE=shelf-bench AWS_REGION=ap-south-1
make env-up
./bootstrap.sh --apply --scale=1tb
./cold-start/run.sh --backend=shelf    --apply
./cold-start/run.sh --backend=fs-cache --apply
./cold-start/run.sh --backend=raw-s3   --apply
python3 -m tools.aggregate results/$(date +%F)/*/cold-start-*.json
./cleanup.sh --apply
make env-down
```

Expected wall-clock per backend: ~30 min (warm-up + 3 scale cycles).

## What this benchmark is not

- **Not a scalability test.** It does not measure steady-state QPS at
  20 workers.
- **Not a correctness test.** Assumes the query set is correct; wrong
  results are out of scope for this benchmark.

## TODO_SHELF-26 / TODO_SHELF-28

- Commit the 20 dashboard queries under `queries/dashboard-20.sql`.
- Wire the Karpenter scale event into `run.sh` (SHELF-28 chaos driver).
- Record per-worker readiness timestamps so we can plot TTFQ
  vs "workers ready" curves.
