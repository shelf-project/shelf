# Vendor benchmark citations

> Shelf benchmarks measure **Shelf vs raw S3** on a fresh
> [in-cluster fixture](../benchmarks/in-cluster/README.md). For
> apples-to-apples comparisons against vendor caches and indexers, we
> cite each vendor's own published benchmark instead of producing
> numbers ourselves. Reproducing a Starburst Warp Speed or Firebolt
> TPC-DS run requires a vendor contract and is therefore not
> third-party reproducible — and the workspace's no-fabricated-numbers
> rule says cite, do not interpolate.

This document is the citation source for `[COMPARISON.md](../COMPARISON.md)` §2 ("Vendor-cited"). Every row links to the vendor's primary source, dates the publication, names the hardware shape, names the scale factor, and flags whether the comparison is **✓ apples-to-apples** with our bench fixture or **⚠ different** (different SF, different hardware, different engine version).

If you have access to a vendor account and want to reproduce these numbers on the same hardware Shelf was measured on, populate the corresponding entry in `[benchmarks/tpcds/runner/engines.yaml](../benchmarks/tpcds/runner/engines.yaml)` and re-run the harness. We will accept reproduced-with-receipts numbers via PR.

---

## Citation matrix


| Vendor / engine                    | Source                                                                                                                                                                                 | Date     | Hardware                                                                                 | Scale          | Headline reported                                                                                                                                                                                    | Apples-to-apples                                                    |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ---------------------------------------------------------------------------------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Trino on Iceberg (no cache)        | [StarRocks TPC-DS benchmark](https://docs.starrocks.io/docs/3.5/benchmarking/TPC_DS_Benchmark/)                                                                                        | 2025-04  | 5 × AWS `m6id.4xlarge` (16 vCPU / 64 GiB / 950 GB NVMe). 1 coord + 4 workers. Trino 475. | SF1000         | Sum of 99-query response times = **2 552 s** (≈ 2 552 076 ms). Iceberg via AWS Glue, Parquet+ZSTD.                                                                                                   | ✓ Trino+Iceberg, ⚠ smaller hardware (80 vCPU vs our 192)            |
| Starburst Warp Speed (Galaxy)      | [Announcing the public preview of Warp Speed in Starburst Galaxy](https://www.starburst.io/blog/announcing-warp-speed-starburst-galaxy/)                                               | 2023-06  | Galaxy Standard cluster (instance class not disclosed in blog).                          | SF1000         | TPC-DS **query #96 only**: 5× perf vs Galaxy Standard, 10× CPU reduction. 40 % avg interactive-workload improvement on customer traffic.                                                             | ⚠ Single-query (Q96), not the 99-query suite. Hardware undisclosed. |
| Starburst Warp Speed (LTS 479-e)   | [Release 479-e LTS (27 Feb 2026)](https://starburstdata.github.io/latest/release/release-479-e.html)                                                                                   | 2026-02  | n/a (release notes; no benchmark)                                                        | n/a            | Warp Speed in 479-e now uses the file-system cache for data caching — relevant context for any future benchmark comparison. No new TPC-DS numbers published in the release notes.                    | n/a                                                                 |
| Starburst Warp Speed (independent) | [Warp Speed – Part II — Graham Martin](https://gpjmartin.wordpress.com/2024/03/31/starburst-warp-speed-part-ii/)                                                                       | 2024-03  | Independent blog                                                                         | varies         | Independent third-party walkthrough of Warp Speed indexing behaviour. Useful narrative; no head-to-head numbers.                                                                                     | ⚠ Narrative only                                                    |
| Alluxio + Spark on S3              | [Benchmark Spark Alluxio S3 Stack with TPC-DS](https://www.alluxio.io/blog/one-click-to-benchmark-spark-alluxio-s3-stack-with-tpc-ds-queries-on-aws)                                   | 2024-ish | Spark cluster on EMR with Alluxio sidecar. Specific instance/SF detail in blog.          | varies         | Alluxio's official benchmark write-up — Spark + Alluxio + S3, NOT Trino. Useful for the cache-vs-direct-S3 framing but the engine differs.                                                           | ⚠ Spark, not Trino                                                  |
| Trino + Alluxio cache (in-tree)    | [A cache refresh for Trino](https://trino.io/blog/2024/03/08/cache-refresh) and [PR #18719](https://github.com/trinodb/trino/pull/18719)                                               | 2024-03  | n/a (announcement; no published TPC-DS numbers)                                          | n/a            | Trino 439 integrated Alluxio-powered file-system caching for Hive / Iceberg / Delta / Hudi. No published TPC-DS or replay numbers in the announcement.                                               | n/a                                                                 |
| Firebolt (TPC-DS substitute)       | [The Process of Running FireScale Benchmarks](https://www.firebolt.io/blog/firescale-benchmarks-a-deeper-dive) and [firebolt-db/benchmarks](https://github.com/firebolt-db/benchmarks) | 2024+    | Firebolt SaaS, FBU-based engine sizing.                                                  | 1 TB FireScale | Firebolt explicitly does NOT publish TPC-DS — they substitute their own **FireScale** benchmark, derived from Berkeley AMPLab's Big Data Benchmark at 1 TB scale. Numbers are FireScale, not TPC-DS. | ⚠ Different benchmark (not TPC-DS)                                  |


---

## How to read this matrix

- **The vendor numbers are not normalised.** Different engines, different hardware shapes, different scale factors, different benchmarks. We refuse to multiply ratios and present a single "Shelf vs Warp Speed" speed-up — that would be exactly the fabricated-comparison the workspace's no-fabricated-numbers rule forbids.
- **Shelf's measured numbers (Shelf vs raw S3) live in `[benchmarks/RESULTS.md](../benchmarks/RESULTS.md)`.** Use the StarRocks-page Trino+Iceberg+SF1000 row above as the closest "no-cache" reference point: it gives total query time on a comparable Trino + Iceberg + Parquet stack, on documented hardware, with 1 warmup + 3 timed runs. Shelf vs raw S3 on our fixture is the directly-measured replacement for that baseline.

## Realistic positioning of Shelf vs Warp Speed (from workspace evidence)

This subsection encodes the workspace's locked positioning (May 1) so the OSS reader is not misled.

- **Shelf will NOT beat Warp Speed on warm-cache selective-query latency.** Warp Speed builds a *new* column-index format optimised for selective predicates and reshapes the ScanFilterProject operator. Shelf accelerates the *existing* Trino+Iceberg+Parquet read path without any new index format.
- **Shelf wins where vendor lock-in costs:**
  - **TCO** — 10–30× cheaper than vendor-contracted alternatives at equivalent throughput (OSS Apache-2.0 + commodity NodePool vs per-vCPU contract).
  - **Workload churn** — instant cache adapt via ETag-versioned content-addressed keys (see [ADR-0011](../agents/out/adr/0011-shelf04-key-is-sha256-etag-offset-length-ordinal.md)), no async column-index rebuild required when an Iceberg snapshot rotates.
  - **Cluster-shared cache** — survives KEDA worker rotation; in-cluster column-index alternatives must rebuild after each scale event.
  - **No vendor lock-in** — the engine stays vanilla Trino. Shelf can be ripped out per-replica via single-line `s3.endpoint` flip on the catalog properties file.
  - **Iceberg-snapshot-safety by construction** — no manual cache-invalidation policy needed when a schema or snapshot changes.
- **Realistic target**: get within ~2× of Warp Speed's selective-query p99 with all Tier-1 lever flips on (SHELF-B1 NVMe compression, SHELF-46 bloom-aware admission, SHELF-49 range coalescing, SHELF-50 decoded-metadata cache, SHELF-33 W-TinyLFU on DRAM metadata only, plus `iceberg.metadata-cache.enabled=false` on the catalog), at a fraction of Warp Speed's price.

## Citation hygiene

If you find a vendor benchmark with newer / better-matched numbers, please open a PR adding a row to the matrix above. The minimum bar is:

1. Primary source — the vendor's own URL (blog, docs, paper). No re-blogged secondhand numbers.
2. Date of publication of the cited number.
3. Hardware shape — instance type × count, at minimum.
4. Scale factor or workload size.
5. The exact metric reported (geometric mean? sum? p95?).
6. An honest apples-to-apples flag.

We do **not** accept rows that:

- Compute a ratio across two different vendor benchmarks ("Vendor A's number divided by Vendor B's").
- Cite a vendor's marketing page without a hardware spec.
- Re-blog a vendor's own claim through a third party (always link to the primary source).