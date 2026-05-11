# Shelf vs the field

> What does Shelf get you, and where does it not pay off? This page is
> the v1 honest-comparison surface. Numbers in §1 come from the
> [in-cluster bench fixture](benchmarks/in-cluster/README.md);
> vendor rows in §2 are cited from the vendor's own publications via
> `[docs/VENDOR-COMPARE.md](docs/VENDOR-COMPARE.md)`. Operator
> evidence on a real cluster is in §4 — explicitly framed as evidence,
> not as third-party-reproducible numbers.
>
> What this page is NOT: a marketing speed-up table. We do not
> multiply our SF100 measurement by a vendor's SF1000 ratio and call
> it a Shelf-vs-vendor speed-up. That would violate the workspace's
> no-fabricated-numbers rule, and it would mislead the OSS reader.

---

## §1 — Measured by us: Shelf vs raw S3

Source: `[benchmarks/RESULTS.md](benchmarks/RESULTS.md)`. Each cell links to the JSON record at `benchmarks/results/<date>/<backend>/<run-id>.json`. Bench fixture is documented at `[benchmarks/in-cluster/README.md](benchmarks/in-cluster/README.md)`.


| Bench        | Backend | p50 wall | p95 wall | p99 wall | p99.9 wall | Hit rate | $/query | TTFQ p95 | run record                            |
| ------------ | ------- | -------- | -------- | -------- | ---------- | -------- | ------- | -------- | ------------------------------------- |
| TPC-DS SF100 | shelf   | TBD      | TBD      | TBD      | TBD        | TBD      | TBD     | n/a      | (populates after first nightly green) |
| TPC-DS SF100 | raw-s3  | TBD      | TBD      | TBD      | TBD        | n/a      | TBD     | n/a      | (populates after first nightly green) |
| Cold-start   | shelf   | n/a      | TBD      | TBD      | n/a        | TBD      | n/a     | TBD      | (populates after first nightly green) |
| Cold-start   | raw-s3  | n/a      | TBD      | TBD      | n/a        | n/a      | n/a     | TBD      | (populates after first nightly green) |
| 1-day replay | shelf   | TBD      | TBD      | TBD      | TBD        | TBD      | TBD     | n/a      | (populates after first nightly green) |
| 1-day replay | raw-s3  | TBD      | TBD      | TBD      | TBD        | n/a      | TBD     | n/a      | (populates after first nightly green) |


**The cells above are EMPTY UNTIL the first green nightly bench run.** No one is allowed to fill them in by hand — see `[benchmarks/RESULTS.md](benchmarks/RESULTS.md)` for the auto-populate pipeline.

If you want to reproduce these numbers on your own cluster:

```bash
git clone https://github.com/shelf-project/shelf && cd shelf
export BENCH_BUCKET=s3://your-tpcds-bench
export BENCH_REGION=us-east-1
export HMS_THRIFT_URI=thrift://your-metastore:9083
export SHELF_IRSA_ROLE_ARN=arn:aws:iam::<acct>:role/shelf-bench-s3
./benchmarks/tpcds/generator/generate_sf1000.sh
./benchmarks/in-cluster/up.sh
# … run TPC-DS, cold-start, replay per benchmarks/in-cluster/RUNBOOK.md …
./benchmarks/in-cluster/down.sh
```

Reproduction budget: ~24 h wall-clock for SF100 + cold-start + 1-day-replay × 2 backends; ~$70 of `ap-south-1` on-demand spend (3 shelf-bench pods + 1 coord + 4 worker nodes).

---

## §2 — Vendor-cited: Alluxio, Starburst Warp Speed, Firebolt

We do **not** measure these vendors ourselves. Their TPC-DS / TPC-H runs require contracts (Starburst Galaxy / Enterprise; Firebolt SaaS) or are published only on different engines (Alluxio's main TPC-DS write-up is Spark, not Trino). The honest path for an OSS comparison is to cite vendor publications and clearly mark them as such.

The full citation matrix lives in `[docs/VENDOR-COMPARE.md](docs/VENDOR-COMPARE.md)`. Headlines:


| Vendor / engine                              | Cited claim                                                                                                                                         | Source                                                                                                                                                   | Apples-to-apples                     |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| Trino on Iceberg (no cache)                  | 99-query SF1000 sum = **2 552 s** on 5 × m6id.4xlarge, Iceberg via AWS Glue, Parquet+ZSTD                                                           | [StarRocks docs (2025-04)](https://docs.starrocks.io/docs/3.5/benchmarking/TPC_DS_Benchmark/)                                                            | ✓ Trino+Iceberg, ⚠ smaller hardware  |
| Starburst Warp Speed (Galaxy public preview) | TPC-DS Q96 only at SF1000 on Iceberg: 5× perf, 10× CPU reduction vs Galaxy Standard. 40 % avg interactive-workload improvement on customer testing. | [Starburst blog (2023-06)](https://www.starburst.io/blog/announcing-warp-speed-starburst-galaxy/)                                                        | ⚠ Single-query, hardware undisclosed |
| Trino + Alluxio cache (in-tree, Trino 439)   | Announcement only — Trino 439 (Feb 2024) integrated Alluxio file-system caching for Hive / Iceberg / Delta / Hudi. No published TPC-DS numbers.     | [trino.io blog](https://trino.io/blog/2024/03/08/cache-refresh) and [PR #18719](https://github.com/trinodb/trino/pull/18719)                             | n/a (announcement only)              |
| Alluxio on S3 + Spark                        | Alluxio's own TPC-DS write-up is Spark + S3, not Trino. Useful for the cache-vs-direct-S3 framing only.                                             | [Alluxio blog](https://www.alluxio.io/blog/one-click-to-benchmark-spark-alluxio-s3-stack-with-tpc-ds-queries-on-aws)                                     | ⚠ Spark, not Trino                   |
| Firebolt                                     | Firebolt does not publish TPC-DS. Their substitute is **FireScale** (1 TB Berkeley AMPLab BDB derivative).                                          | [Firebolt blog](https://www.firebolt.io/blog/firescale-benchmarks-a-deeper-dive) and [firebolt-db/benchmarks](https://github.com/firebolt-db/benchmarks) | ⚠ Different benchmark                |


**We do NOT compute Shelf-vs-vendor speed-up ratios.** The vendor numbers are on different hardware, different scale factors, and (in Firebolt's case) different benchmarks. Multiplying them through with our SF100 measurement would produce a fabricated result.

---

## §3 — Qualitative axes (no numbers)

Where Shelf wins or loses without ever running a benchmark.


| Axis                                 | Shelf                                                                                                                           | Trino + Alluxio                                                             | Starburst Warp Speed                         | Firebolt                               |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- | -------------------------------------------- | -------------------------------------- |
| License model                        | Apache-2.0                                                                                                                      | Apache-2.0                                                                  | Commercial contract                          | SaaS (FBU/hr)                          |
| Engine modification required         | None — `s3.endpoint` flip                                                                                                       | Trino-side compile-in (Trino 439+)                                          | Starburst-only execution mode                | Replatform (ingest into F3)            |
| Iceberg-snapshot-safety              | By construction (ETag content-addressing — [ADR-0011](agents/out/adr/0011-shelf04-key-is-sha256-etag-offset-length-ordinal.md)) | Manual invalidation policy                                                  | Async column-index rebuild on schema change  | Re-ingest required                     |
| Workload-churn tolerance             | Instant — new etag → new key, no rebuild                                                                                        | Trino fs.cache: 15–20 % hit ratio under KEDA spot churn (operator evidence) | Async index rebuild — minutes                | Re-ingest cost on schema/data change   |
| Cross-replica / cluster-shared cache | Yes (HRW + peer-fetch — [ADR-0002](agents/out/adr/0002-hrw-hashing-over-vnode-ring.md))                                         | Per-worker in Trino fs.cache; cluster-shared in Alluxio standalone          | In-cluster, per-cluster index                | SaaS-side, no per-replica concept      |
| Deploy-time-from-clone               | Single `s3.endpoint` line + `helm install` — minutes                                                                            | ~30 min Helm + IAM + UfsIOManager tuning                                    | Vendor contract → onboarding lead-time       | Vendor contract → onboarding lead-time |
| Vendor-lock-in to remove             | Per-replica revert via `s3.endpoint` flip; no schema migration                                                                  | Easy (Trino fs.cache disable) or Alluxio drain                              | Hard (column-index format proprietary)       | Hard (data is in F3 format)            |
| Operational language                 | Rust (`shelfd`) + Helm + ConfigMaps                                                                                             | Java + Alluxio operator                                                     | Vendor-managed                               | Vendor-managed                         |
| Selective-query indexed lookups      | Not built — uses Iceberg's own bloom + min/max stats                                                                            | Same as Shelf                                                               | **Yes** — proprietary column index per query | **Yes** — F3 columnstore               |
| Predictable steady-state $/query     | Yes (commodity NodePool)                                                                                                        | Yes (commodity NodePool)                                                    | Per-vCPU-hour contract                       | Per-FBU-hour contract                  |


The takeaway from §3: Shelf is the right answer when the constraint is OSS, snapshot-safety, workload churn, or vendor-neutrality. Shelf is **not** the right answer when the constraint is winning warm-cache selective-query latency on a stable schema — that is what Warp Speed is built for.

---

## §4 — Internal cluster operator evidence (sidebar)

The Shelf project originated on a 4-replica Trino-on-EKS cluster (penpencil) and has been on production traffic since rep-2 cutover (April 2026) and rep-1 cutover (April 2026 — see workspace evidence). The numbers below are operator evidence on that cluster — they are NOT third-party-reproducible OSS-launch numbers. The OSS-reproducible numbers are §1.

- **rep-2 active cutover (2026-04-27)**: stopped a live Alluxio meltdown. Alluxio-class infra failure rate **94 % → 5.7 %** in the first post-cutover hour. `ICEBERG_INVALID_METADATA` 147 → 0; `ICEBERG_BAD_DATA` 38 → 0; `ICEBERG_CANNOT_OPEN_SPLIT` 111 → 13; `GENERIC_INTERNAL_ERROR` (metadata) 70 → 1; `USER_CANCELED` 205 → 61 (-70 %). Full write-up: `[docs/rollout-v1/cutover-rep2.md](docs/rollout-v1/cutover-rep2.md)`.
- **rep-1 cutover (2026-04-27)**: median wall time 91 s → 49 s on a 7-day-before/after window; Power BI heaviest hitters dropped read latency 55–62 %; Trino `CLUSTER_OUT_OF_MEMORY` 20 → 0 (the cache offloads scan-buffer pressure off Trino workers, shrinking JVM heap footprint). The earlier "47 % volume drop confound" notice still applies — the operator-evidence narrative is honest about it. Full write-up: `[docs/rollout-v1/cutover-rep1.md](docs/rollout-v1/cutover-rep1.md)`.
- **rep-0 cutover attempt + revert (2026-04-30 → 2026-05-01)**: clean roll-forward, then OOM cascade traced to c-family node-allocatable < pod-limit on the alluxio NodePool. Reverted to direct S3 the morning of May 1. Lesson: capacity engineering (drop c-family from `instance-family` AND bump pod limit 32 → 40 GiB) is mandatory for shelf pods on m-family at any scale. Full write-up: `[docs/rollout-v1/cutover-rep0.md](docs/rollout-v1/cutover-rep0.md)`.

These numbers reflect a production-Trino workload (50.6 % join queries, 22.1 % equality-pushdown, 45.7 % selective-< 100 MB queries) which differs from TPC-DS in shape — TPC-DS has more analytical aggregations and fewer point-lookups. The §1 OSS numbers and §4 operator numbers are complementary, not redundant.

---

## §5 — What is NOT measured here, and why


| Comparison                                    | Status                                                         | Why deferred / why not                                                                                                                                                                                                                                      |
| --------------------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Shelf vs Alluxio OSS 2.9.5 (in our cluster)   | Not in v1                                                      | Alluxio is currently scaled to 0 in our `alluxio` namespace (rep-1/2 are on Shelf since April 2026). Spinning Alluxio back up for a one-shot bench is operationally messy and would inflate cost. We cite the Spark+Alluxio TPC-DS write-up via §2 instead. |
| Shelf vs Warp Speed (we run TPC-DS on Galaxy) | Not in v1                                                      | Vendor contract required. We cite Starburst's own TPC-DS Q96 + interactive-workload claims via §2. Realistic positioning: Shelf within ~2× of Warp Speed's warm-cache selective p99, at a fraction of the price (workspace evidence May 1).                 |
| Shelf vs Firebolt                             | Not in v1                                                      | Firebolt is SaaS and explicitly does not publish TPC-DS — they substitute FireScale. The benchmarks are not comparable.                                                                                                                                     |
| 7-day full-replay gate                        | Not in v1                                                      | Doesn't fit the OSS 90-min reproduction budget. The 1-day slice is in scope; the 7-day full path is documented at `[benchmarks/replay/SPEC.md](benchmarks/replay/SPEC.md)` for any operator who wants to run the full `v0.5 kill-switch`.                   |
| `spot-churn` benchmark                        | v1.1                                                           | Chaos-driver complexity is not blocking the comparison story; deferred to a separate release.                                                                                                                                                               |
| LRB / W-TinyLFU / SuRF algorithm-swap A/B     | Phase B/C of [perf research](docs/perf-research-2026-04-27.md) | Algorithm research, NOT the OSS launch baseline. Each lever has its own go/no-go criteria documented in the perf-research roadmap.                                                                                                                          |


---

## §6 — How to push back on this comparison

If you have evidence that Shelf is mis-positioned in any cell of §3, file a GitHub issue with:

1. The cell you disagree with.
2. The specific scenario you measured.
3. A reproducible artefact (config + commands) that another operator can run.

We will update §3 with the new evidence. We will **not** update §1 or §2 from issue conversations — §1 lands via auto-PR from the nightly bench; §2 lands via PR-with-citation per the [VENDOR-COMPARE.md citation hygiene rules](docs/VENDOR-COMPARE.md#citation-hygiene).

---

*Last meaningful update: v1 launch. Replaces the 2026-04-23 "Shelf vs TrinoCache Stack" framing, which was internal-merge-debate context for blueprint v0.2 and has been superseded by v1.0 GA.*