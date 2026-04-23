# Replay benchmark — specification

_Replays 7 days of `cdp.trino_logs.trino_queries` from rep-2 against
each backend at 2× real-time. **This is the v0.5 kill-switch.**_

- **Status.** Scaffolding (v0.0).
- **Owner.** bench-harness team (agent 7) + data-eng-1 (SHELF-26).
- **References.** `BLUEPRINT.md` §10.4, plan §3 Phase 1 (v0.5 gate),
  `ADR-0010`, SHELF-26.

---

## Goal

Answer the question the whole project lives or dies by: on our real
workload (rep-2's last 7 days), can Shelf match or beat Alluxio OSS
2.9.5 across five metrics simultaneously, for 7 consecutive days?

Concretely, this benchmark produces the numbers in the `v0.5 gate
board` of `RESULTS.md`. Its correctness is the reason we run it *at
all* — TPC-DS says nothing about our dashboard cohort; this does.

## Workload

- **Dataset.** The Iceberg tables that rep-2 actually read during a
  given 7-day window, copied (or referenced) in the bench cluster's
  fixture bucket. Bytes of data ≈ 5-20 TiB depending on the week.
- **Trace source.** `cdp.trino_logs.trino_queries` filtered on
  `replica = 'rep-2'` between two pinned timestamps. Trace snapshot ID
  is recorded in the result.
- **Replay speed.** 2× real-time (i.e. 7 days compressed to 3.5 days
  of wall clock on the bench cluster). Replay speed is a knob; 1×, 2×,
  and 10× profiles exist. The gate is evaluated only at 2×.
- **Query mix.** Every query is replayed verbatim as issued by the
  original user, including user, catalog, resource group. DDL and DML
  queries are filtered out.
- **Cluster shape.** Same shape as rep-2's prod Trino on the day of
  the trace (worker count + instance type). Scaled down only if the
  bench AWS account has insufficient quota, in which case the scale
  factor is recorded in `cluster_shape.scale_factor` and the run is
  flagged `partial=true`.

## Method

1. `replay/prep.sh` (SHELF-26) materialises the trace + Iceberg
   fixtures into the bench cluster. Deterministic; idempotent.
2. `run.sh --backend=<b> --days=7 --speed=2x`:
   - Reset the backend's cache to empty.
   - For each query in the trace, issue it via Trino JDBC at the
     scheduled wall-clock offset (scaled by replay speed).
   - Record: latency, hit rate snapshot, scanned bytes, admitted bytes,
     $/query, failure.
3. Every 10 s, sample cumulative hit rate and GOLD_DBT-equivalent
   ok-rate (computed over the dbt subset of the trace).
4. Run ends when the last query of the trace is issued.

## Metrics (definitions + thresholds from plan §6.4 = ADR-0010)

Five **gate metrics** (all must hold for 7 consecutive days — the
primary evidence for passing v0.5):

| Metric                     | Definition                                                 | Threshold              |
| -------------------------- | ---------------------------------------------------------- | ---------------------- |
| `hit_rate_7d_cumulative`   | `hits / (hits+misses)` over the 7-day replay.              | **≥ 71 %**             |
| `gold_dbt_ok_rate`         | Ok-rate of dbt queries in the trace (catalog=`cdp_dbt`).   | **≥ 99.9 %**           |
| `latency_ns_p95_vs_alluxio`| Shelf p95 / Alluxio p95. Alluxio is the baseline run.      | **≤ 1.20**             |
| `shelf_caused_pages`       | PagerDuty pages attributed to Shelf during the window.     | **= 0**                |
| `oncall_surface_ratio`     | Shelf oncall surface / Alluxio oncall surface, 7-day roll. | **≤ 0.50**             |

Also reported (informational, not gating):

| Metric             | Definition                                                  |
| ------------------ | ----------------------------------------------------------- |
| `latency_ns_p50`   | Median query latency over the 7-day replay.                 |
| `latency_ns_p95`   | 95th percentile.                                             |
| `latency_ns_p99`   | 99th percentile.                                             |
| `latency_ns_p999`  | 99.9th percentile.                                           |
| `bytes_read`       | Total bytes scanned.                                         |
| `bytes_admitted`   | Total bytes written into cache.                              |
| `dollars_per_query`| See tpcds SPEC.                                              |

**Kill-switch rule.** If any one of the five gate metrics misses, the
verdict in `RESULTS.md` is `FAIL: <metric>`. ADR-0010 then triggers a
2-week gap-analysis window. No "68 % is basically 71 %" negotiations.

## Reporting format

One JSON per run at:

```
results/<YYYY-MM-DD>/<backend>/replay-<run_id>.json
```

Validates against `schema.json`. Plus a canonical aggregation pointer
at:

```
results/latest/replay-rep2-7d-<backend>.json
```

The gate evaluator (`tools.gate`) consumes `results/latest/` and
emits the `RESULTS.md` v0.5 gate row.

## Reproducibility command

```bash
git clone https://github.com/<org>/shelf && cd shelf/benchmarks
export AWS_PROFILE=shelf-bench AWS_REGION=ap-south-1
make env-up
./bootstrap.sh --apply --scale=1tb
./replay/prep.sh   --from="2026-04-16T00:00:00Z" --to="2026-04-23T00:00:00Z" --apply
./replay/run.sh    --backend=shelf    --days=7 --speed=2x --apply
./replay/run.sh    --backend=alluxio-2-9 --days=7 --speed=2x --apply
python3 -m tools.gate results/$(date +%F)/shelf/replay-*.json \
                       results/$(date +%F)/alluxio-2-9/replay-*.json
./cleanup.sh --apply
make env-down
```

Expected wall-clock: ~3.5 days per backend at 2×. **This is the one
benchmark that does not fit in a 90-min reproduction budget** — the
90-min target is for TPC-DS smoke + 1-day replay (see
`docs/reproducing.md` step 4).

## Why 2× real-time and not 10×

1× hides the cache's steady-state pressure (traffic arrives at the
same rate the cache can absorb it). 10× becomes a throughput test
that exposes worker CPU, not cache behaviour. 2× was chosen because
it stress-tests the cache slightly above prod without distorting the
workload mix. If we ever want a 10× run, it is reported separately
and **never** used for gate evaluation.

## What this benchmark is not

- **Not a live-traffic canary.** Shadow traffic on rep-2 (SHELF-13) is
  a separate mechanism.
- **Not a correctness test.** Query correctness is assumed; if a
  backend returns wrong results, `replay` will mark failures but cannot
  diagnose silent wrong-answer bugs. That is SHELF-12 / integration
  tests.
- **Not synthetic.** Trace comes from `cdp.trino_logs.trino_queries`.
  If the schema evolves, SHELF-26 tracker documents the migration.

## TODO_SHELF-26

- `prep.sh` that materialises Iceberg manifests + fixtures.
- Python replay driver with configurable speed knob.
- Gate evaluator `tools.gate` producing `RESULTS.md` row.
- Trace snapshot ID persistence so a historical run is byte-identical
  to reproduce.
