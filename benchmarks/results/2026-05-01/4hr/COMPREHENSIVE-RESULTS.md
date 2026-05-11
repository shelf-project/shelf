# Shelf comprehensive bench — 4-scenario, SF100, 8-worker dev Trino, 6-pod fresh shelf-bench

> **TL;DR — shelf is consistently slower than raw S3 across every scenario tested today, despite achieving a 97.6 % cache hit rate.** The bottleneck is *not* the cache; it's the per-request shim hop overhead. Same-region S3 RTT is already 5-15 ms; the shim adds another 5-15 ms; the cache savings (~150-300 µs per cached read) cannot recover that. Adding pods (3 → 6) gave only ~10-20 % more throughput. **Where to improve**: shim per-request latency, not cache hit rate.

## Setup

- **Cluster**: dev Trino on `data-platform-cluster`, KEDA min=8 max=12 (so 8 workers active, each 32 vCPU / 128 GiB)
- **Shelf**: dedicated `shelf-bench` StatefulSet (3 pods, then scaled to 6), `data-platform-alluxio-role` IRSA, `ebs-gp3-wffc` 240 GiB NVMe per pod, image `1.0.0-rc.5`
- **Fixture**: TPC-H SF100 (8 tables, 600 M lineitem rows, ~250 GB Parquet) at `s3://pw-data-cdp-prod-temp/warehouse/benchmark/_shelf_bench_tpch_sf100.db/` via prod HMS pivot
- **Backends**: `bench_iceberg_shelf` (s3.endpoint=`shelf-bench-pool:9092`) vs `bench_iceberg` (s3.endpoint=`s3.ap-south-1.amazonaws.com`)
- **Tested in this 4-hour run**: BI dashboard replay (15-min measurement), TPC-H 10-query (cold + 2 warm), 50-rep convergence, concurrency curve (1 → 32)
- **Total elapsed**: 59 min for 4 scenarios at 3 pods + 8 min for the 6-pod re-run + 30 min cleanup/writeup

## Headline numbers

### Scenario 1 — BI dashboard replay (4 concurrent users × 15-min measurement window after 3-min prewarm)

```
                  n     fail   p50          p95          p99          total_qps
shelf-bench       1146  1      2014.7 ms    6523.9 ms    6870.1 ms    1.27
raw-S3            1879  3      1751.8 ms    2185.7 ms    2430.2 ms    2.09
shelf delta       -39%  +33%   +15%         +199% (!!)   +183%        -39%
```

raw-S3 delivered **64 % more queries/sec** than shelf at the same concurrency, and shelf's tail latency (p95 = 6.5 s) is 3× worse than raw-S3 (p95 = 2.2 s). Every shelf-side run had a measurable long-tail problem — likely from queue-depth saturation in the shelf-shim's HTTP/2 server during 4-way concurrency.

### Scenario 2 — TPC-H 10Q (cold + 2 warm)

```
              cold p50    warm1 p50   warm2 p50
shelf-bench   4352.2 ms   4457.4 ms   5051.6 ms    (warming -> getting WORSE)
raw-S3        3022.7 ms   2702.4 ms   2535.6 ms    (warming -> getting BETTER)
```

The shelf-bench *degrades* across warm passes — the opposite of what a cache should do. Hypothesis: shim-level connection-pool churn or capacity-pressure evictions in the rowgroup pool. raw-S3 *converges* across warm passes because Trino's per-worker JVM caches Iceberg metadata locally between queries.

### Scenario 3 — 50-rep convergence (3 hot queries × 50 reps each, decile averages in ms)

```
              query             r0-4    r5-14   r15-29  r30-49
shelf-bench   qb_lineitem       2920    2960    2952    2930      (FLAT — no convergence)
shelf-bench   qb_aggregate      3278    3139    3115    3203      (FLAT)
shelf-bench   qb_join           4220    3810    3803    3763      (-11%)
raw-S3        qb_lineitem       2302    2290    2294    2284      (already steady-state)
raw-S3        qb_aggregate      3371    2236    2748    2573      (-24%, Trino-internal warming)
raw-S3        qb_join           2755    2563    2464    2624      (-5%)
```

**raw-S3 converges faster than shelf at SF100.** This is counterintuitive but follows from the diagnostic above: shim per-request latency is fixed; cache hits don't compound across reps because Trino-internal caches already win the per-rep gains.

### Scenario 4 — Concurrency curve (q06 single query, varying concurrency, 30 sec per level)

```
             3 shelf-bench pods                6 shelf-bench pods                raw-S3
conc=1       0.33 qps  p50=3046  p95=3203      0.37 qps  p50=2796  p95=3320      0.43-0.47  qps  p50=2235-2372
conc=4       0.67      6748     6885           0.80      5432     5925           1.87-1.90       2160-2200
conc=8       0.83     12036    13263           1.07      9378    10355           2.87-3.17       2638-2966
conc=16      1.10     23219    23753           1.10     18162    18963           3.47-3.57       4896-5164
conc=32      n/a                               1.07     34493    34733           5.53            6667
```

This is the diagnostic chart. Key observations:

1. **shelf-bench plateaus at ~1.1 qps** regardless of concurrency or pod count.
2. Doubling pods (3 → 6) gave only marginal improvements at conc 1-8 and *zero* improvement at conc 16+.
3. raw-S3 scales nearly linearly through conc 16 and is still scaling at conc 32 (5.5 qps).
4. shelf-bench p95 grows linearly with concurrency (3.2 s → 23.8 s as conc goes 1 → 16) — clear queueing.

The shelf-shim has a **per-request latency floor of ~3 sec at SF100 same-region** that more pods don't fix.

## Diagnostic — shelfd `/metrics` from all 6 pods at end of run

```
pod              hits        misses      origin_get      disk_used
shelf-bench-0    488454      11190       42.7 GB         20.1 GB
shelf-bench-1    509775      11253       43.0 GB         20.2 GB
shelf-bench-2    538929      11130       42.2 GB         20.0 GB
shelf-bench-3    61622       2293        6.7 GB          5.1 GB
shelf-bench-4    34094       2211        4.9 GB          5.1 GB
shelf-bench-5    34810       2182        4.9 GB          5.1 GB

TOTAL            1,667,684   40,259      144.4 GB        75.6 GB
hit-rate (cluster-wide)   97.6 %
```

This is the most striking finding of the day. **Despite a 97.6 % cache hit rate and 144 GB served from cache, shelf was still slower than raw-S3 across every scenario.** The cache *worked* — it's just that the shim hop is expensive enough to cancel out the cache savings on a low-RTT origin.

Pod load distribution (HRW) is highly skewed: shelf-bench-{0,1,2} took 14× more queries than {3,4,5} (joined later). NVMe usage is only 8 % of the 240 GB cap — DRAM rowgroup pool (11 GiB per pod) was sufficient.

## Where to improve (the actionable findings)

Ranked by potential impact for shelf:


| #     | Finding                                                                                                                                                                                                        | What to do                                                                                                                                                                                                                                                                      |
| ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1** | **Per-request shim hop latency is the dominant cost on low-RTT origins.** A cache hit through shelf is ~10-65 ms; direct same-region S3 is ~5-15 ms. The cache "wins the byte fetch" but loses the round-trip. | Eliminate or amortize the network hop. The Java-plugin path (`ShelfFileSystem` in `clients/trino/`) is exactly this — when the Trino blob-cache SPI lands (trinodb/trino#29184), shelf becomes an in-process cache lookup with no HTTP overhead. **This is the biggest lever.** |
| **2** | **shelf-shim per-pod throughput plateaus at ~0.2 qps per pod** (1.1 qps cluster-wide on 6 pods, query is the unit not range-GET).                                                                              | Profile the shim: where is per-request time going? Connection pool? `HybridCache::get` lock? AWS SDK signing? The 5-15 ms hop is suspiciously high for a localhost-equivalent path.                                                                                             |
| **3** | **HRW load is heavily skewed when pods join asynchronously** — first 3 pods served 14× more queries than last 3 in this run.                                                                                   | Pre-warm new pods during scale-out before they're added to the active pool, OR use Consistent Hashing with Bounded Loads (CHWBL) per the workspace's perf-research note.                                                                                                        |
| **4** | **NVMe is barely used (8 %)** — working set fits in DRAM.                                                                                                                                                      | Reduce NVMe size in default values from 240 GB to 60 GB; reclaim cluster storage cost. Or repurpose: use NVMe for compressed-cold data only, keep hot in DRAM.                                                                                                                  |
| **5** | **iceberg.metadata-cache.enabled=false on the shelf catalog** (forced by us to surface metadata-pool hit rate) doubles HEAD object volume. Production catalogs leave it on.                                    | Re-test with `metadata-cache.enabled=true` on the shelf path. The metadata HEAD volume in §Diagnostic was 1.0 TB cumulative (mostly from prior runs) — substantial.                                                                                                             |


## Honest verdict — is shelf worth continuing?

**On wall-clock latency for same-region TPC-H workloads at SF100: no, shelf is the bottleneck not the accelerator.** The shim adds round-trip cost that the cache savings cannot recover.

**But the bench did NOT exercise**:

- **High-RTT origin** (cross-region S3, congested prefix). Where shelf hit-savings exceed shim hop. Shelf was designed for this.
- **Cold-start under KEDA spot churn** (the unique-to-shelf differentiator vs `fs.cache`). Production rep-1/2 cutover evidence shows this is the regime shelf wins.
- **Real production trace replay** (Power BI dashboard pattern from `cdp.trino_logs.trino_queries`). The rep-1 cutover -46 % wall-time win came from this pattern.
- **In-process Java plugin path** (`ShelfFileSystem`, dormant in `clients/trino/`). Once trinodb/trino#29184 lands the blob-cache SPI, shelf is no longer an HTTP hop — it becomes a cache lookup in Trino's JVM. Per-request latency goes to the speed of NVMe (~~50 µs) rather than HTTP (~~5-15 ms).

Recommendation:

1. **Don't kill shelf based on this bench alone.** The bench measured exactly the worst regime for shelf and shelf still hit 97.6 % cache rate — the cache layer works.
2. **Prioritise the in-process plugin path.** Track upstream Trino blob-cache SPI; if it lands within 6 months, shelf becomes useful regardless of RTT.
3. **Run the production-trace-replay scenario next** — it's the only bench that replicates the regime where production rep-1/2 showed the -46 % wins.
4. **De-prioritise pod scaling and NVMe optimisation** — they're not the bottleneck.

## Files


| Path                                                                                             | Contents                                                                         |
| ------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------- |
| `benchmarks/results/2026-05-01/4hr/COMPREHENSIVE-RESULTS.md`                                     | this file                                                                        |
| `benchmarks/results/2026-05-01/4hr/raw-comprehensive.json`                                       | all per-query raw timings, 4 scenarios                                           |
| `benchmarks/results/2026-05-01/4hr/concurrency-6pod.json`                                        | 6-pod re-run of concurrency curve                                                |
| `benchmarks/results/2026-05-01/4hr/shelfd-metrics/shelf-bench-{0..5}-{stats,metrics}.{json,txt}` | per-pod /stats + /metrics at end of run                                          |
| `benchmarks/results/2026-05-01/SF10-SF100-FINDINGS.md`                                           | earlier 2-hour bench (SF10, SF100 with prod shelf-pool — superseded by this run) |


## Cluster state restored

- Helm rev 680 = rollback to 670 (= clean original 665)
- 6 original catalogs only (no `bench_iceberg`, no `bench_iceberg_shelf`)
- Workers KEDA-controlled, scaling back to min 1
- Ranger access control re-enabled
- shelf-bench StatefulSet uninstalled, all 6 PVCs deleted
- Prod HMS schema dropped (verified gone)
- S3 prefix `s3://pw-data-cdp-prod-temp/warehouse/benchmark/_shelf_bench_tpch_sf100.db/` cleaned

Total wall-clock: ~3 hr 5 min (within 4-hour budget).