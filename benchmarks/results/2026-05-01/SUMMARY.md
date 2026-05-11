# Bench results — 2026-05-01

Six runs landed today across two harnesses. All records are
schema-valid against `benchmarks/replay/schema.json`.

## Headline: TPC-H SF1 cluster bench (Shelf vs raw S3)


| metric   | shelf         | raw-S3        |
| -------- | ------------- | ------------- |
| p50 wall | **2380.7 ms** | **2268.0 ms** |
| p95 wall | **3060.2 ms** | **2522.3 ms** |
| p99 wall | **3652.6 ms** | **2525.5 ms** |


**6 TPC-H queries × 3 reps × 2 backends, 4-worker dev Trino, real AWS S3 origin.** Per-query (warm-avg):


| query                 | shelf   | raw-S3  | warm delta           |
| --------------------- | ------- | ------- | -------------------- |
| q01 pricing summary   | 2253 ms | 2326 ms | -3 % (shelf wins)    |
| q03 shipping priority | 2794 ms | 2460 ms | +14 % (shelf loses)  |
| q06 forecasting       | 2177 ms | 2385 ms | -9 % (shelf wins)    |
| q10 returned items    | 2362 ms | 2107 ms | +12 % (shelf loses)  |
| q12 shipping modes    | 2298 ms | 2181 ms | +5 % (shelf neutral) |
| q14 promotion effect  | 2297 ms | 2252 ms | +2 % (shelf neutral) |


**Honest interpretation**: at SF1 on a same-region (intra-VPC) S3 origin, Shelf's HTTP hop adds modest overhead that the cache cannot quite recover. SF1 is dominated by Trino's planning + first-query JVM warmup; the Iceberg metadata + Parquet footer reads (which Shelf caches well) are a small fraction of total wall time at this scale. Shelf's network-savings advantage shows up at higher SFs and on cross-region origins, neither of which was tested here.

## All 6 records


| Run                 | Backend | Harness                     | p50       | p95       | p99       | Hit rate                      | Origin bytes |
| ------------------- | ------- | --------------------------- | --------- | --------- | --------- | ----------------------------- | ------------ |
| smoke (TPC-H-shape) | shelf   | docker-compose              | 4.8 ms    | 9.0 ms    | 9.1 ms    | 88.2 %                        | 77 KB        |
| smoke (TPC-H-shape) | raw-s3  | docker-compose              | 6.3 ms    | 10.5 ms   | 12.8 ms   | n/a                           | n/a          |
| TPC-H SF1 (laptop)  | shelf   | docker-compose              | 410.6 ms  | 1303.0 ms | 1574.2 ms | 86.3 %                        | 615 MB       |
| TPC-H SF1 (laptop)  | raw-s3  | docker-compose              | 300.0 ms  | 782.0 ms  | 1233.8 ms | n/a                           | n/a          |
| TPC-H SF1 (cluster) | shelf   | dev Trino + prod shelf-pool | 2380.7 ms | 3060.2 ms | 3652.6 ms | metric scrape gap (see below) | n/a          |
| TPC-H SF1 (cluster) | raw-s3  | dev Trino → AWS S3          | 2268.0 ms | 2522.3 ms | 2525.5 ms | n/a                           | n/a          |


### Metric scrape gap on the cluster shelf record

The shelf record's `summary.hit_rate` and `summary.bytes_read` show 0 because shelfd ships in a **distroless container** (no `/bin/sh`), and the harness's `kubectl exec ... -- wget` to scrape `/metrics` failed silently. Shelf was definitely active — verified post-run via port-forward of `shelf-0:9090/metrics` which showed `shelf_origin_request_bytes_total{bucket="pw-data-cdp-prod-temp",op="get_range",outcome="ok"} 131 GB` cumulative including our run's contribution. The wall-time numbers are correct; only the hit-rate and origin-byte fields in the record are unmeasured. Field `cluster_shape.partial=true` flags the record so consumers don't aggregate it into hit-rate roll-ups.

To capture hit-rate next run, either pre-create a sidecar curl pod in `alluxio` ns and have the harness `kubectl exec` it, or have the harness run `kubectl port-forward` per shelf pod (the slow but reliable path).

## How the cluster bench was unblocked

Earlier today the cluster shelf path was blocked at the IAM layer:

- Shelf-pool IRSA (`data-platform-alluxio-role`) reads `pw-data-cdp-prod-`* only.
- Dev HMS's IAM key writes dev-temp only.
- → no bucket all three components can use.

The unblock: **point dev Trino's bench catalogs at prod HMS** (`thrift://trino-prod-metastore.penpencil.co:9083`) instead of dev HMS. Prod HMS accepts `pw-data-cdp-prod-temp` as schema location, shelf-pool IRSA reads it, and dev Trino can write to it via the operator-provided AWS keys.

Caveats:

- The bench schema (`bench_iceberg._shelf_bench_tpch_sf1`) was created in **prod HMS**, briefly visible to prod replicas. Used a `_shelf_bench_…` prefix so it's clearly throw-away and easy to grep for.
- Schema dropped via `DROP SCHEMA … CASCADE` BEFORE the helm rollback so no debris stays in prod HMS.
- Verified: `SELECT schema_name FROM bench_iceberg.information_schema.schemata WHERE schema_name='_shelf_bench_tpch_sf1'` returns 0 rows post-drop.

## Cluster state on completion

- Helm rev 675 = rollback to 670 (= original rev 665, untouched)
- Catalog ConfigMap: 6 original catalogs only (no `bench_iceberg`, no `bench_iceberg_shelf`, no `shelfbench` user)
- Worker count: KEDA-controlled, scaling back to min 1 / max 3 / fallback 1
- Auth: original PASSWORD-only with Ranger access control
- Prod HMS: bench schema dropped, no debris
- S3: `s3://pw-data-cdp-prod-temp/warehouse/benchmark/_shelf_bench_tpch_sf1.db/` removed via `aws s3 rm --recursive`
- Dev `pw-data-cdp-dev-temp/warehouse/benchmark/` from earlier run also cleaned

Private operator artefacts (helm values w/ AWS keys, bcrypt password file, snapshot YAMLs) live in `/Users/aamir/trino/.cursor-private/bench-snapshots/` — never committed.

## Vendor comparison framing

- StarRocks's published TPC-DS SF1000 99-query sum on Trino+Iceberg (5 × m6id.4xlarge, ~2552 s total, see `docs/VENDOR-COMPARE.md`) is the closest "no-cache" reference. Per-query average ≈ 25 s on SF1000.
- Our cluster TPC-H SF1 raw-S3 baseline is ~2.3 s p50 across 6 queries — roughly the same per-query order of magnitude as a 1000× smaller workload. The non-linearity is expected: at SF1 a large chunk of wall time is Trino planning + exchange manager setup, not data scan; at SF1000 those overheads disappear into the scan time.
- Shelf's cluster numbers at SF1 are within 5–14 % of raw-S3 on individual warm queries. At SF100/SF1000 (where data scan dominates and Iceberg metadata reads are amortised), the network-savings advantage of caching should compound. Confirming that needs a higher-SF run, which is gated on either:
  - More fixture generation time (~2 hr for SF100, half-day for SF1000), and
  - Either grant DevOps for IAM expansion to remove the prod-HMS workaround dependency, or accept the prod-HMS-via-bench-catalog pattern as a recurring operator runbook.

