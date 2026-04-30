# Spot-churn benchmark — specification

_Kills 50 % of Trino workers every 5 min for 1 h while steady
dashboard load runs. Measures hit-rate degradation curve._

- **Status.** Scaffolding (v0.0).
- **Owner.** bench-harness team (agent 7) + sre-1.
- **References.** `BLUEPRINT.md` §10.3, plan §3 Phase 3 gate, SHELF-28.

---

## Goal

Quantify how each backend behaves during the level of worker churn we
see on rep-2 in practice. This is the differentiator Shelf claims most
loudly: a shared cache (Shelf) should barely notice, whereas
per-worker caches (fs.cache) collapse because every new worker is
cold.

Phase 3 gate (plan §3): Shelf hit rate stays ≥ 65 % under this chaos
pattern.

## Workload

- **Dataset.** 1 TB TPC-DS Iceberg fixture or `replay/` fixture;
  either works — the benchmark is about steady dashboard *load*, not
  query mix.
- **Query generator.** `k6` script issuing the 20 dashboard queries at
  a constant rate (10 QPS) for the full 60 minutes.
- **Chaos pattern.** Every 5 minutes, select 50 % of Trino worker pods
  uniformly at random and `kubectl delete pod --grace-period=10`.
  (Spot interruption proxy. Real spot interruption gives 2 min; 10 s
  is a stricter test.)
- **Shelf pods.** Untouched. Shelf runs on on-demand NVMe per plan §3
  Phase 5. The benchmark measures *Trino* worker churn, not Shelf
  churn.
- **Duration.** 60 min steady state. 12 chaos events total.

## Method

1. Warm-up: 20 min at 10 QPS with a stable worker count. Record
   baseline hit rate.
2. Start the chaos loop: every 5 min, delete 50 % of worker pods.
3. Throughout, sample:
   - Cumulative hit rate every 10 s.
   - Query latency per request.
   - Failed queries (count + reason).
4. Stop after 60 min of steady-state load (i.e. chaos loop runs for
   50 min of the 60; last 10 min is post-chaos recovery).
5. Produce the hit-rate-over-time curve and the quantile summary.

**Failure handling.** Query failures are counted, not retried. A
backend that relies on worker-side retries to "recover" during churn
is *not* what the user experiences.

## Metrics (definitions + thresholds)

| Metric                 | Definition                                           | Threshold                         |
| ---------------------- | ---------------------------------------------------- | --------------------------------- |
| `latency_ns_p50`       | Median query latency over the full 60 min.           | Reported.                         |
| `latency_ns_p95`       | 95th percentile over 60 min.                         | Reported.                         |
| `latency_ns_p99`       | 99th percentile.                                     | Reported.                         |
| `latency_ns_p999`      | 99.9th percentile.                                   | Reported.                         |
| `hit_rate`             | Cumulative hit rate at t = 60 min.                   | **Shelf ≥ 65 %** (plan §3 P3).    |
| `hit_rate_floor`       | Minimum cumulative hit rate at any 10 s sample.       | **Shelf ≥ 55 %**.                 |
| `bytes_read`           | Sum of scanned bytes over the run.                   | Reported.                         |
| `bytes_admitted`       | Bytes admitted into cache over the run.              | Reported.                         |
| `failed_queries_total` | Query failures over the whole 60 min.                | **All backends: 0** under chaos.  |
| `dollars_per_query`    | As per tpcds SPEC.                                   | Reported.                         |

Expected values (BLUEPRINT §10.3):

- Shelf: hit rate stays ≥ 75 %.
- fs.cache: drops to ~20 %.

## Reporting format

One JSON per run at:

```
results/<YYYY-MM-DD>/<backend>/spot-churn-<run_id>.json
```

Validates against `schema.json`. `samples[]` array holds the 10-s
hit-rate time series; `summary` holds the quantile table.

## Reproducibility command

```bash
git clone https://github.com/<org>/shelf && cd shelf/benchmarks
export AWS_PROFILE=shelf-bench AWS_REGION=ap-south-1
make env-up
./bootstrap.sh --apply --scale=1tb
./spot-churn/run.sh --backend=shelf    --apply
./spot-churn/run.sh --backend=fs-cache --apply
python3 -m tools.aggregate results/$(date +%F)/*/spot-churn-*.json
./cleanup.sh --apply
make env-down
```

Expected wall-clock per backend: ~80 min (20 min warm-up + 60 min run).

## What this benchmark is not

- **Not a network partition test.** Partitions are a separate failure
  mode; Shelf's pod-kill test here does not model them.
- **Not Shelf pod churn.** Shelf survives churn because it does not
  experience churn in our design; that is a feature, not a cheat.
- **Not an Alluxio-fairness statement about Alluxio-3 DORA's workers.**
  DORA's worker pods are also on-demand in the baseline; the benchmark
  only churns Trino workers.

## TODO_SHELF-26 / TODO_SHELF-28

- `k6` script with the 20-query rotation at 10 QPS.
- Chaos driver (SHELF-28) that honours "uniformly at random, 50 %,
  every 5 min" invariant and records the delete timestamps into the
  result record.
- Grafana panel for live hit-rate curve during the run (SHELF-27).
