# TPC-DS @ 1 TB — specification

_Authoritative spec for the TPC-DS benchmark. This file is the source of
truth; `run.sh` implements it; `schema.json` constrains its output._

- **Status.** Scaffolding (v0.0). Runner is a no-op that writes a
  valid-but-empty record.
- **Owner.** bench-harness team (agent 7).
- **References.** `BLUEPRINT.md` §10.1, plan §6 (SLOs).

---

## Goal

Produce the *public* headline numbers for Shelf: p50 / p95 / p99 / p99.9
query latency, $/query, cache hit rate, and warm-up time to 80 % hit
rate, on a standard 1 TB Iceberg TPC-DS dataset, across five backends.

This benchmark's audience is external reviewers and the launch blog.
The v0.5 kill-switch is NOT decided by TPC-DS (see ADR-0010) — that is
the `replay/` benchmark's job. TPC-DS is credibility, not gate.

## Workload

- **Dataset.** TPC-DS scale factor 1 000 (≈ 1 TB raw) written as Iceberg
  v2 tables in Parquet. 24 TPC-DS tables + 99 queries. Generator:
  `tpcds-kit` pinned at commit `<TODO_SHELF-26>`.
- **Query set.** All 99 TPC-DS queries, unmodified.
- **Concurrency.** 1 (serial). TPC-DS throughput variants (`TPC-DS
  TPC-H Benchmark/Throughput`) are out of scope for v1.
- **Iterations.** 3 cold + 3 warm runs per query. Cold = cache flushed
  between runs. Warm = cache retained.
- **Cluster shape.** 3 Trino workers + 3 Shelf nodes + 1 driver (all
  pinned in `env/variables.tf`). Same shape for every backend.

## Method

1. `bootstrap.sh --backends=<list>` installs every backend into its
   own namespace with its own config from `configs/<backend>/`.
2. `run.sh --backend=<one>` executes:
   - Cache flush (backend-specific; see `configs/<backend>/README.md`).
   - For each of 99 queries:
     - Issue via Trino JDBC.
     - Record wall-clock latency, bytes scanned (from
       `QueryStatistics.getScanStatistics().getTotalInputBytes()`),
       bytes admitted to cache, and the backend's hit-rate snapshot.
   - Warm-up curve: every 100 ms sample cumulative hit rate, record
     the first timestamp at which it crosses 80 %.
3. Output JSON validates against `schema.json`.
4. Any backend that cannot serve a query completes the run with that
   query marked `failed=true` rather than aborting — we want the full
   matrix even when one backend breaks.

## Metrics (definitions + thresholds)

| Metric           | Definition                                                 | Threshold (from plan §6)          |
| ---------------- | ---------------------------------------------------------- | ---------------------------------- |
| `latency_ns_p50` | Median wall-clock of the query, per backend, warm runs.    | No absolute — comparative.         |
| `latency_ns_p95` | 95th percentile, warm runs.                                | Shelf ≤ 120 % of Alluxio (v0.5 gate, measured on *replay*; TPC-DS is reported, not gated). |
| `latency_ns_p99` | 99th percentile.                                           | Reported.                          |
| `latency_ns_p999`| 99.9th percentile.                                         | Reported (quality bar: never drop from the report). |
| `hit_rate`       | `hits / (hits + misses)` over the full warm run.           | Reported.                          |
| `bytes_read`     | Sum of `getTotalInputBytes()` across all queries.          | Reported.                          |
| `bytes_admitted` | Bytes written into the cache during the run.               | Shelf vs Alluxio: compared only.   |
| `dollars_per_query` | `(ec2_hr + s3_get_cost) * elapsed_hr / queries`.         | Reported; leaderboard lower-is-better. |
| `warm_up_seconds` | First time cumulative hit rate ≥ 80 %.                    | Target: Shelf ≤ 2 min; fs.cache expected > 10 min. |
| `regression_p95` | Compared to previous tagged release.                       | CI gate: > 10 % regression fails PR. |
| `regression_p99` | Compared to previous tagged release.                       | CI gate: > 5 % regression fails PR. |

## Reporting format

Every `run.sh` invocation writes one JSON file per run at:

```
results/<YYYY-MM-DD>/<backend>/tpcds-<run_id>.json
```

Validating against `schema.json`. The file contains one record per
(query_id, iteration, cold|warm). The publisher job then aggregates
into `RESULTS.md` as a single row per `(release_tag, backend,
benchmark=tpcds)` using warm-run quantiles across all 99 queries.

Sample table row (illustrative, empty at launch):

| release_tag | backend         | p50  | p95  | p99   | p99.9 | hit_rate | $/query | raw |
| ----------- | --------------- | ---- | ---- | ----- | ----- | -------- | ------- | --- |
| v0.5.0-rc1  | shelf           | —    | —    | —     | —     | —        | —       | [raw](...) |
| v0.5.0-rc1  | alluxio-2-9     | —    | —    | —     | —     | —        | —       | [raw](...) |

## Reproducibility command

From a clean AWS account and laptop:

```bash
git clone https://github.com/<org>/shelf && cd shelf/benchmarks
export AWS_PROFILE=shelf-bench AWS_REGION=ap-south-1
make env-up
./bootstrap.sh --apply --scale=1tb
./tpcds/run.sh --backend=shelf    --scale=1tb --iterations=3 --apply
./tpcds/run.sh --backend=alluxio-2-9 --scale=1tb --iterations=3 --apply
./tpcds/run.sh --backend=fs-cache --scale=1tb --iterations=3 --apply
./tpcds/run.sh --backend=raw-s3   --scale=1tb --iterations=3 --apply
./tpcds/run.sh --backend=alluxio-3-dora --scale=1tb --iterations=3 --apply
python3 -m tools.aggregate results/$(date +%F)/*/tpcds-*.json
./cleanup.sh --apply
make env-down
```

Expected wall-clock: ≤ 6 h for the full 5-backend × 99-query × 6-iteration
matrix on the default cluster shape. Budget confirmed against TPC-DS
elapsed time ranges from the Alluxio 3.x public benchmarks (ADR-0010
context).

## What this benchmark is not

- **Not the v0.5 gate.** ADR-0010 is evaluated on `replay/`, not here.
- **Not a throughput test.** Concurrency = 1.
- **Not a TCO study.** `$/query` includes only EC2 + S3 GET; does not
  include engineering time, support, or data transfer. Stated clearly
  in the launch blog.

## TODO_SHELF-26

- Pin `tpcds-kit` generator commit.
- Commit the 99-query SQL files adapted for Iceberg v2.
- Wire `dollars_per_query` computation (requires a priced copy of
  `env/outputs.tf::cluster_shape`).
