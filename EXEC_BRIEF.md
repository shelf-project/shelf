# Shelf — what it is, why it matters

*A 5-minute brief.*

---

## TL;DR

Trino on top of S3-backed Iceberg spends most of its wall-clock on the
**round trip to S3** for the same byte ranges, query after query.
**Shelf** is a small Rust service that sits between Trino and S3 and
remembers those byte ranges so the second query doesn't pay for them
twice.

Three sentences:

1. **Shelf is a content-addressed read cache for Iceberg-on-S3.** Same
   Parquet row group, same hash, served once.
2. **It's open source, single binary, no per-vCPU license.** The
   competitors above the line are commercial (Starburst Warp Speed,
   Firebolt) or single-tier (Alluxio).
3. **The plan and the engineering are done.** Production benchmark
   numbers against Warp Speed / Alluxio / Firebolt are gated on
   workload availability, not on engineering.

---

## A simple example

A dashboard query that runs every 5 minutes against an Iceberg table:

```sql
SELECT brand, sum(revenue)
FROM   sales
WHERE  sold_date BETWEEN '2026-04-01' AND '2026-04-30'
GROUP  BY brand
ORDER  BY 2 DESC
LIMIT  10
```

Without a cache, every 5 minutes Trino:

1. Asks the metastore "where does the `sales` table live?" → ~50 ms
2. Reads the Iceberg manifest from S3 → ~80 ms
3. Reads each Parquet file's footer + page index from S3 → ~200 ms × N
   files = several seconds
4. Reads the actual data row groups for April → ~1 s
5. Computes the answer → ~0.3 s

Most of that time is "talking to S3 about things that haven't changed
since the last run." Shelf turns those round trips into local DRAM /
NVMe lookups.

---

## How Shelf is different

### vs. Alluxio (the closest comparison)

Alluxio is a distributed filesystem cache that remembers files by
*path*. Shelf differs on three things that matter:

1. **Content-addressed, not path-addressed.** When the same Parquet row
   group is referenced by two snapshots, Alluxio caches it twice;
   Shelf caches it once.
2. **Two pools, not one.** Iceberg footers (~64 KiB) and data row
   groups (~32 MiB) have very different access patterns. Shelf keeps
   metadata pinned in DRAM and demotes data row groups to NVMe;
   Alluxio treats them as the same kind of byte.
3. **Plan-fingerprint telemetry.** Shelf reports
   `shelf_queries_served_total` per fingerprint, so it's clear *which
   dashboards* are getting a fast path, not just "x % of bytes were
   warm."

### vs. Starburst Warp Speed

Warp Speed is a closed-source Galaxy add-on that auto-builds range,
bitmap, and lookup indexes on hot columns and caches data alongside
them. Shelf differs on:

1. **Open source vs. proprietary.** Warp Speed is a per-vCPU license
   fee on top of Galaxy / Starburst Enterprise. Shelf is Apache-2.0;
   it deploys onto the cluster you already pay for.
2. **Trino-version-portable.** Warp Speed binds to Starburst's Trino
   fork. Shelf works with stock Trino through a small event-listener
   plugin.
3. **Side blooms are *learned*, not built.** Warp Speed's bitmap
   indexes are constructed at ingest. Shelf builds 1 MiB blooms per
   `(file_etag, rowgroup, column)` from the first scans it observes —
   so the index "warms up" with the workload, not before it.

### vs. Firebolt

Firebolt is a different category. It's a cloud-native columnar database
— you stop using Trino and Iceberg, re-ingest into Firebolt's F3
format, and query through Firebolt's engine. Shelf differs in kind,
not in degree:

1. **No data migration.** Shelf reads the existing Iceberg tables on
   the existing S3 bucket.
2. **Same query language.** Trino SQL keeps working; nothing in dbt
   models or BI tools changes.
3. **No lock-in.** Shelf can be turned off in a config flag.

### vs. raw Trino + S3

Trino has a small filesystem cache (`fs.cache`), but it's per-worker,
path-addressed, and gets evicted when the worker pod restarts. The
"Why Shelf" section of [`README.md`](./README.md) is the technical
answer.

---

## How Shelf works, in one paragraph

Trino's S3 client doesn't talk to S3 directly — it talks to Shelf,
which speaks the S3 protocol. Shelf hashes every byte range it sees
(`sha256(etag || offset || length || rowgroup_ordinal)`) and keeps the
hot ones in DRAM (metadata: footers, page indexes, manifests) or NVMe
(data row groups). When two queries ask for the same row group, the
second one is served from local memory. A small plugin in the Trino
coordinator tells Shelf *what query is running* so Shelf can pre-warm
the bytes it knows will be needed before the worker even asks. That's
the whole idea.

---

## Status

- **Architecture / design** — done. See [`BLUEPRINT.md`](./BLUEPRINT.md)
  and ADRs under `agents/out/adr/`.
- **Core caching engine (`shelfd`)** — done. Two pools, eviction,
  telemetry, peer failover, MV registry.
- **Trino plugin (event listener, row-group skip)** — done. Side
  blooms, batch probe, plan fingerprint.
- **Materialised-view advisor + auto-pinning** — done. Nightly advisor
  + dbt-emit + MV pin watcher.
- **Helm chart + smoke compose** — done.
- **Public TPC-DS SF1000 benchmark vs. Warp Speed / Alluxio /
  Firebolt** — pending; requires a benchmark cluster + vendor accounts.

For the cluster-side hand-off ledger (what was rolled out where, what
remained on the cluster operator's side), see
[`docs/rollout-v1/cluster-handoff.md`](./docs/rollout-v1/cluster-handoff.md).

---

## Where to go deeper (in this order)

1. [`README.md`](./README.md) — one-pager, technical
2. [`BLUEPRINT.md`](./BLUEPRINT.md) — full design
3. [`COMPARISON.md`](./COMPARISON.md) — Shelf vs. competitor matrix
4. [`benchmarks/tpcds/publish/README.md`](./benchmarks/tpcds/publish/README.md)
   — the gate that decides what numbers are published
