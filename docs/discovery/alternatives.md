# Shelf vs alternatives — when each tool wins

Honest comparison of Shelf against the other options for caching Trino-on-Iceberg-on-S3 reads. Numbers come from measured runs on the project's origin Trino-on-EKS cluster (4 replicas, ~250 K queries/day, KEDA-scaled spot workers); your mileage will vary, but the trade-off shape generalises.

## TL;DR — the matrix

| | Shelf 1.0 | Alluxio OSS 2.9 | Alluxio EE 3.x | Starburst Warp Speed | Native `fs.cache` | No cache |
|---|---|---|---|---|---|---|
| **License** | Apache 2.0 | Apache 2.0 | Commercial | Commercial (Starburst-only) | Apache 2.0 (Trino) | n/a |
| **Trino + Iceberg + S3 fit** | Native | Generic — known sharp edges on Iceberg metadata | Native | Native | Generic | n/a |
| **Granularity** | Row-group / footer (sub-MB) | File | Block (configurable) | Row-group | File | n/a |
| **Cache key** | Content-addressed (S3 ETag) — Iceberg-snapshot-safe by construction | Path + ttl | Path + ttl | Path + version | Path + mtime | n/a |
| **Multi-replica share** | Yes — one HRW ring across all Trino replicas | Yes — Alluxio cluster | Yes | Per-replica | Per-replica | n/a |
| **Spot-worker friendly** | Yes — cache lives on dedicated pool, decoupled from KEDA | Yes — same pattern | Yes | Yes | **No** — cache dies with the worker | n/a |
| **Deploy footprint** | 1 Helm chart, 3 pods | 1 Helm chart, master + workers + proxy | Same as OSS | Bundled with Starburst | Trino-only | n/a |
| **Operator surface** | `s3.endpoint` config line + ServiceMonitor | Mount table + IAM scope-write + connection pool | Same as OSS + per-table TTLs | Starburst console | Trino properties | n/a |
| **Failure mode on cache outage** | **Fail-open** to direct S3 (no error) | Fail-closed (queries fail) without explicit fallback | Same as OSS unless `worker.s3.redirect.enabled` | Fail-open in Starburst | Cache miss → S3 (transparent) | n/a |
| **Supply chain** | cosign + SBOM + SLSA v1.0 | cosign on releases | Same | Vendor-signed | n/a | n/a |
| **Best when** | You're on OSS Trino, Iceberg, S3, want measured 2× latency cuts | You need shared cache for Spark + Trino simultaneously | You need Alluxio + EE-only features (S3 SDK v2, worker redirect) | You're already on Starburst | Single-replica, stable workers | Cost-bound, bursty workloads |
| **Worst when** | Non-S3 backend, no temporal locality | Trino-Iceberg specifically — metadata-pool saturation | Cost — commercial license | Vendor lock-in | KEDA / spot — cold cache on rotation | Steady hot tables (you pay S3 forever) |

## Shelf vs Alluxio OSS 2.9.x — the most common comparison

Both are open-source, both proxy S3, both run as a separate cache layer in front of Trino. The differences come from architectural choices that matter under sustained Iceberg load:

### Where Alluxio struggles on Trino + Iceberg specifically

The user-facing symptom is queries failing with `Error processing metadata for table` / `Malformed Parquet file` (`Expected magic number: PAR1 or PARE got: <0–3 bytes>`) / `ICEBERG_BAD_DATA` / Avro `Length is negative` or `Invalid sync!` — looks like data corruption but it's not. The root cause is connection-pool saturation: Alluxio's S3 proxy gates read concurrency on `alluxio.underfs.io.threads` (default 36 in OSS 2.9.5). Under sustained load, the pool empties, the proxy returns truncated bodies, and Trino's Parquet reader sees garbage where the magic number should be.

Workarounds exist (raise `underfs.io.threads` to 256, scale workers, scale proxies) but the architectural ceiling of `connections × workers × proxies` ultimately bites. The two features that lift the ceiling — `alluxio.underfs.s3.sdk.version=2` and `alluxio.worker.s3.redirect.enabled` — are **EE-only**, not in OSS.

### Where Shelf is structurally different

Shelf's read path doesn't have a shared connection pool gating throughput. Each Foyer pool (metadata DRAM-only, rowgroup DRAM+NVMe) reads/writes locally; the only S3 connection is on miss, and the AWS SDK v1 `aws-sdk-s3` crate manages its pool independently per pod. The `Connection refused` / pool-exhaustion failure class doesn't exist in Shelf's design.

Cache keys are content-addressed via the S3 ETag — `sha256(etag || offset || length || rg_ordinal)` — so Iceberg snapshot churn produces new keys automatically. There are no TTLs to tune, no invalidation queue to drain, no stale-read class of bugs. Alluxio handles snapshot-safety via path + TTL, which means you either set TTL aggressively (more S3 reads) or accept stale reads on snapshot rotation.

### When Alluxio still wins

- **You have multiple compute engines on the same cache.** Spark + Trino + Presto + Hive all reading the same hot tables — Alluxio's cluster model amortises the cache across all of them. Shelf is single-engine (Trino-shaped, S3-protocol).
- **You need explicit per-table TTL / pin policies.** Alluxio's mount table is more declarative than Shelf's pin list.
- **You want Alluxio's POSIX FUSE mount.** Shelf doesn't expose one (it's an S3 endpoint, not a filesystem).
- **Existing Alluxio investment.** If you're already on Alluxio and your symptoms aren't the metadata-pool class above, the migration cost may exceed the benefit.

## Shelf vs Starburst Warp Speed

Warp Speed is Starburst's commercial cache layer, bundled with their Trino distribution. It's row-group-granular like Shelf, plan-aware like Shelf, and fast on Iceberg.

The difference is **availability**: Warp Speed only runs inside Starburst Enterprise / Galaxy. If you're on OSS Trino (Trino 480 from `trinodb/trino`), Warp Speed isn't an option. If you're on Starburst, Warp Speed is the default; Shelf would be a sidegrade with no clear advantage.

Shelf's architecture is heavily inspired by Warp Speed's published design notes (row-group keys, plan-aware prefetch, fail-open). The difference is that Shelf is open-source and protocol-level (any S3-compatible engine can use it), while Warp Speed is engine-integrated and proprietary.

## Shelf vs Trino native `fs.cache`

Trino ships a built-in filesystem cache (`fs.cache.enabled=true`) that caches files locally on each worker. It's the simplest possible answer — properties-only, no separate deployment.

It works well if **your workers are stable**. If they're not — KEDA-scaled, spot-priced, rotating frequently — the cache dies with the worker. Empirically on a 4-replica spot-priced cluster, native `fs.cache` hit ratio sits at 15–20 %; the same workload on Shelf sits at 70–85 % because the cache lives on a separate StatefulSet that doesn't rotate with KEDA.

Use native `fs.cache` if your workers are stable and your scale is small. Use Shelf if you have any worker churn at all.

## Shelf vs no cache (just S3)

Sometimes the right answer is no cache. Specifically:

- **Cost-bound workloads** where the S3 GET cost is much smaller than the storage cost of a cache.
- **One-shot scans** where data is read once and discarded.
- **Tables that change every query** — temporal locality is zero.

For these, S3 itself is the cache (it scales perfectly and you only pay for what you fetch). Shelf adds infrastructure cost (3 pods, ~80 GiB RAM, 600 GiB NVMe) that doesn't pay back.

## Decision tree

```
Are you on Trino + Iceberg + S3?
├── No → none of these apply; pick a cache for your stack
└── Yes →
    ├── Are your workers stable (no KEDA, no spot churn)?
    │   ├── Yes → native fs.cache is the simplest answer; reach for Shelf only if measured insufficient
    │   └── No → on to the next branch
    │
    ├── Are you on Starburst?
    │   ├── Yes → Warp Speed is bundled and supported
    │   └── No → on to the next branch
    │
    ├── Do you have temporal locality (same tables / partitions read repeatedly)?
    │   ├── No → no cache will help; tune queries / partition keys instead
    │   └── Yes → on to the next branch
    │
    ├── Do you have other compute engines (Spark, Hive, Presto) on the same cache?
    │   ├── Yes → Alluxio's cluster model amortises better; evaluate Shelf if Alluxio metadata-pool issues bite
    │   └── No → Shelf is the focused answer
```

## Citations

The numbers in this document come from real measured cluster runs, not vendor benchmarks:

- [README.md "Real-world impact"](../../README.md#real-world-impact) — first-hour cutover measurements on the origin cluster.
- [docs/rollout-v1/](../rollout-v1/) — full rollout narrative across 4 replicas, including a contemporaneous record of what worked and what broke.
- Alluxio OSS metadata-pool saturation: [GitHub issue draft](https://github.com/Alluxio/alluxio/issues) (search for `underfs.io.threads`).
- Trino native `fs.cache` design: [trinodb/trino](https://github.com/trinodb/trino) — search the docs for `fs.cache`.
- Starburst Warp Speed published design: [Starburst blog](https://www.starburst.io/blog/) — search for "Warp Speed".
