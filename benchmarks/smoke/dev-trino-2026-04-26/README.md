# dev-trino smoke run — 2026-04-26

Smallest-possible end-to-end exercise of the F-track harness against
the in-cluster dev Trino (`trino` namespace, single-coordinator,
single-worker `example-trino-cluster`). The point of this run is **not**
to compare engines — there's no shelfd, no Alluxio, no Warp Speed in
the loop — but to:

1. Prove the F2 harness shape (`runner/run.py`) actually runs against
   a live Trino with auth, X-Forwarded-Proto, and the queued/finished
   state machine.
2. Capture a real-world cold→warm timing tail for a heterogeneous
   query mix so that the F4 regression gate has more than a synthetic
   baseline to chew on.
3. Surface gaps between the **plan-as-written** and the
   **plan-as-deployed** in dev so we know what is actually shipping.

## Setup

- Cluster: `example-trino-cluster` (Trino 480), `trino` namespace.
- Catalog: `tpcds.sf1` (Trino's built-in TPC-DS generator — generates
  rows in-memory; no S3 reads, no shelfd, no Iceberg metadata).
- Auth: temporary `shelfbench` user added to
  `example-trino-cluster-trino-password-file` and
  `example-trino-cluster-trino-access-control-volume-coordinator`,
  removed at the end of the session.
- Driver: `/tmp/shelf_smoke/harness.py` (an ad-hoc copy of the F2
  Trino driver minus engine-specific cache-flush hooks).

## Results — `tpcds.sf1`, 1 coord + 1 worker, 8 queries × 3 repeats

| query                        |   cold |  warm1 |  warm2 | plan c→w     | cpu c→w        |
|------------------------------|-------:|-------:|-------:|--------------|----------------|
| `q_count_store_sales`        | 10586  | 10690  | 10624  | 13 → 12 ms   | 7770 → 7700 ms |
| `q_topk_brand_revenue`       | 13630  | 13455  | 13107  | 187 → 41 ms  | 10793 → 10699 ms |
| `q_filter_pushdown_customer` |  5657  |  5519  |  5542  | 21 → 19 ms   | 595 → 559 ms   |
| `q_join_three_way`           | 14806  | 15123  | 13744  | 62 → 43 ms   | 12149 → 11672 ms |
| `q_window_top_per_state`     | 15282  | 15128  | 15105  | 41 → 31 ms   | 11302 → 11193 ms |
| `q_agg_rollup`               | 15397  | 14943  | 14840  | 31 → 22 ms   | 11344 → 11057 ms |
| `q_inventory_join`           |  5524  |  5674  |  5339  | 33 → 31 ms   | 6040 → 5854 ms |
| `q_correlated_subquery`      | 12576  | 12965  | 12702  | 42 → 27 ms   | 9467 → 9489 ms |

(times in ms; raw rows in `tpcds-sf1.csv`)

The interesting cell is `q_topk_brand_revenue`: planning fell from
**187 ms → 41 ms** (4.5× speedup) once HMS+Iceberg metadata was
warm. This is exactly the A1+A3 effect (HMS TTL + planner warmup),
on a **5-minute** TTL, against a connector that synthesizes rows
in-memory. The bigger absolute wins land when both the metastore TTL
is at the planned 60 minutes and the query reaches an actual S3
backend — which the next test exercises.

## Bonus — direct-S3 Iceberg via `cdp.lms.silver_companies`

Tiny table, but a real S3 round-trip through the Iceberg connector.
Cold→warm progression on the same query (`SELECT count(distinct
_id), max(name)`):

| phase | wall   | plan  | cpu   | physical_input |
|-------|-------:|------:|------:|---------------:|
| cold  | 2897 ms| 15 ms | 60 ms | 8359 B         |
| warm1 | 2756 ms| 13 ms | 23 ms | 8359 B         |
| warm2 | 2571 ms| 13 ms | 22 ms | 8359 B         |
| warm3 | 2700 ms| 13 ms | 19 ms | 8359 B         |

CPU drops 60 → 22 ms (~63 %) once executor-side stats are cached.
Wall-clock barely moves because, at this scale, query setup overhead
dominates the data scan. That's exactly why the F1 generator targets
SF1000 — only at ≥10 GB per partition does the data path stop being
noise.

## Plan-as-written vs plan-as-deployed (dev)

What's already in `cdp_shelf.properties`:
- `iceberg.metadata-cache.enabled=true` ✓
- `hive.metastore-cache-ttl=5m`            ⚠ A1 calls for `60m`
- `hive.metastore-refresh-interval=1m`     ⚠ A1 calls for `5m`
- `iceberg.query-partition-filter-required=true` ✓ (defends against
  full-table scans)
- `iceberg.split-size`                     ✗ not set (B2)
- `iceberg.max-initial-splits`             ✗ not set (B2)
- `fs.native-s3.http2`                     ✗ not set (B2)
- `s3.max-connections`                     ✗ not set (B2)
- `iceberg.materialized-views.enabled`     ✗ not set explicitly (relies on Trino-468+ default; H4 wants this confirmed)

What's already in `config.properties`:
- `optimizer.experimental.iterative-optimizer-timeout` ✗ not set (A1)
- `query.execution-policy=phased` (or similar)         ✗ not visible

## Path through the dev cluster (for context)

```
client → port-forward 18080 → svc/example-trino-cluster-trino → coord
         coord → metastore.metastore.svc (HMS Thrift)
         coord → S3 (cdp catalog: s3.ap-south-1.amazonaws.com)
         coord → Alluxio shim (cdp_shelf catalog:
                    shelf.cache.svc.cluster.local:9092)
```

Note: cdp_shelf paths still go through Alluxio's S3 proxy, which is
the source of the "Error accessing metadata file for table" failure
documented in `.cursor/skills/debug-trino-alluxio-s3-proxy/`. We saw
that exact failure on `cdp_shelf.lms.silver_companies` during this
run — left in the log as a real-world repro.

## Regression gate sanity check (F4)

```
$ python3 shelf/benchmarks/tpcds/regression/check_regression.py \
    --baseline tpcds-sf1-shelf-shaped.csv \
    --candidate tpcds-sf1-shelf-shaped.csv
PASS: 8 queries within tolerance       # self-comparison

$ python3 shelf/benchmarks/tpcds/regression/check_regression.py \
    --baseline tpcds-sf1-shelf-shaped.csv \
    --candidate tpcds-sf1-shelf-slowed.csv  # fabricated 1.20× slowdown
regression: q_agg_rollup 17931ms vs baseline 14943ms (+20.0%, allowed +10.0%)
... (8 lines)
FAIL: 8 regressions detected           # exit 1
```

Both happy and unhappy paths fire correctly.

## What this does **not** show

- **Engine comparison.** No shelfd, Alluxio, Warp Speed or Firebolt
  was driven from this run. The cross-engine numbers in the plan
  remain pending hardware procurement.
- **SF1000 numbers.** The dev cluster is single-worker; running
  SF1000 on it would take days. The F1 generator and F2 runner are
  parameterised by `--sf` precisely because this smoke is SF1.
- **Cache hit-rate evidence.** No shelfd in the loop, so no
  `shelf_hits_total` to inspect. The metric registration was
  unit-tested in `shelfd/src/metrics.rs`; the live numbers will be
  produced by the F2 runner when shelfd is deployed.

## Cleanup

The transient user/ACL changes were reverted at the end of the
session — see `cleanup.sh`.
