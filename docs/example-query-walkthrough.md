# Shelf — Worked Example: What gets cached for `SELECT ... LIMIT 10`

A concrete walkthrough showing exactly which bytes Shelf caches for a tiny query against a partitioned Iceberg table. Uses dummy file names, real-world layout, and byte-level offsets for clarity.

---

## Setup — `cdp.gold.gold_ratings`, partitioned by `date`

```sql
CREATE TABLE cdp.gold.gold_ratings (
  user_id    BIGINT,
  content_id BIGINT,
  rating     DOUBLE,
  device     VARCHAR,
  created_at TIMESTAMP,
  date       DATE
)
WITH (
  partitioning = ARRAY['date'],
  format = 'PARQUET',
  location = 's3a://pw-data-cdp-prod-gold-layer/gold/gold_ratings'
);
```

### S3 layout (dummy, but realistic)

```
s3a://pw-data-cdp-prod-gold-layer/gold/gold_ratings/
│
├── metadata/
│   ├── v00042.metadata.json                     ← current snapshot pointer (~12 KB)
│   ├── snap-7384029384-1.avro                   ← snapshot list (~8 KB)
│   ├── 1a2b3c-m0.avro                           ← manifest list (~6 KB)
│   ├── 1a2b3c-m1.avro                           ← manifest #1 (~80 KB, lists data files)
│   └── 1a2b3c-m2.avro                           ← manifest #2 (~80 KB)
│
└── data/
    ├── date=2026-04-26/
    │   ├── 00031-aaa.parquet  (480 MB)
    │   └── 00032-bbb.parquet  (510 MB)
    │
    ├── date=2026-04-27/
    │   ├── 00040-ccc.parquet  (495 MB)
    │   └── 00041-ddd.parquet  (488 MB)
    │
    └── date=2026-04-28/                          ← the partition we want
        ├── 00050-eee.parquet  (512 MB)   etag=9F2A86B0…
        ├── 00051-fff.parquet  (498 MB)   etag=4C7D11A2…
        └── 00052-ggg.parquet  (505 MB)   etag=BB1100FF…
```

Each Parquet file internally:

```
00050-eee.parquet  (512 MB total, etag=9F2A86B0…)
  bytes 0……4                  PAR1 magic
  bytes 4……4_194_307          Row Group 0   (~4 MB, rows 0..999_999)
  bytes 4_194_308…8_388_611   Row Group 1   (~4 MB, rows 1M..1.999M)
  bytes 8_388_612…12_582_915  Row Group 2   (~4 MB, rows 2M..2.999M)
  …
  bytes 536_804_864…536_870_903   Footer (~64 KB)
  bytes 536_870_904…536_870_911   footer-length (4 B) + PAR1 magic (4 B)
```

---

## The query

```sql
SELECT user_id, rating
FROM cdp.gold.gold_ratings
WHERE date = '2026-04-28'
LIMIT 10;
```

10 rows. Sounds tiny, right? Let's see what Trino actually has to read.

### Step 0 — What Trino does **not** read (key insight)

Because `date` is a partition column:

- Iceberg's manifest tells the planner *"only files under `date=2026-04-28/` qualify"*
- Files in `date=2026-04-26/` and `date=2026-04-27/` are **never opened** — not by Trino, not by shelfd, not from S3
- That's **partition pruning**, free, before any Parquet is touched

So out of ~~3 GB of data files in those 3 partitions, only the 3 files in `date=2026-04-28/` (~~1.5 GB) are even candidates.

### Step 1 — Iceberg metadata reads (planner phase)

Trino has to walk the Iceberg snapshot tree to know what files exist:


| #   | What it reads                                                                            | From        | Bytes  | Pool     |
| --- | ---------------------------------------------------------------------------------------- | ----------- | ------ | -------- |
| 1   | `v00042.metadata.json`                                                                   | `metadata/` | ~12 KB | metadata |
| 2   | `snap-7384029384-1.avro`                                                                 | `metadata/` | ~8 KB  | metadata |
| 3   | `1a2b3c-m0.avro` (manifest list)                                                         | `metadata/` | ~6 KB  | metadata |
| 4   | `1a2b3c-m1.avro` (manifest, contains data file paths + partition values + min/max stats) | `metadata/` | ~80 KB | metadata |
| 5   | `1a2b3c-m2.avro`                                                                         | `metadata/` | ~80 KB | metadata |


Total: **~186 KB of metadata reads** going through shelfd. Each becomes a separate cache entry.

> Note: by default Trino's JVM-local `MemoryFileSystemCache` would shadow shelf for these warm reads. To actually see them in the dashboard, the catalog runs with `iceberg.metadata-cache.enabled=false`.

### Step 2 — Parquet footer reads (planner asks each file "what row groups have data?")

For each of the 3 candidate files, Trino does the standard Parquet footer dance — **two range reads per file**:


| #   | File              | Range read                  | Bytes | What it gets                                                                    |
| --- | ----------------- | --------------------------- | ----- | ------------------------------------------------------------------------------- |
| 6   | 00050-eee.parquet | `bytes=-8` (suffix range)   | 8 B   | footer length + magic                                                           |
| 7   | 00050-eee.parquet | `bytes=536804864-536870903` | 64 KB | full footer (schema, row-group offsets, min/max stats per column per row group) |
| 8   | 00051-fff.parquet | `bytes=-8`                  | 8 B   |                                                                                 |
| 9   | 00051-fff.parquet | `bytes=…`                   | 64 KB |                                                                                 |
| 10  | 00052-ggg.parquet | `bytes=-8`                  | 8 B   |                                                                                 |
| 11  | 00052-ggg.parquet | `bytes=…`                   | 64 KB |                                                                                 |


Total: **~192 KB across 6 footer entries**, all in the `metadata` pool.

### Step 3 — Row-group reads (worker phase, with `LIMIT 10` short-circuit)

Now the planner sees the schema: only `user_id` and `rating` are projected. It also sees `LIMIT 10`.

Trino's split scheduler picks the **smallest** row group from one file (say row group 0 of `00050-eee.parquet`) and starts reading. Parquet is **columnar**, so even within row group 0, only the `**user_id` and `rating` column chunks** are read, not all 6 columns.

Inside row group 0:

```
Row Group 0 internal layout (~4 MB total, rows 0..999_999)
  user_id    column chunk:    bytes 4……  ~800 KB
  content_id column chunk:    bytes …    ~700 KB
  rating     column chunk:    bytes …    ~600 KB
  device     column chunk:    bytes …    ~900 KB        ← skipped
  created_at column chunk:    bytes …    ~900 KB        ← skipped
  date       column chunk:    bytes …    ~6 KB           ← skipped (partition column, value is constant)
```

Trino issues two byte-range GETs:


| #   | File              | Range read              | Column        | Bytes   |
| --- | ----------------- | ----------------------- | ------------- | ------- |
| 12  | 00050-eee.parquet | `bytes=4-819203`        | user_id chunk | ~800 KB |
| 13  | 00050-eee.parquet | `bytes=1519204-2119203` | rating chunk  | ~600 KB |


Trino starts decoding rows. After reading just the first **data page** of each column chunk (~64 KB worth ≈ ~8 000 rows), it has 10 rows, hits `LIMIT 10`, **stops the split**, and never asks for the next page or any other row group.

Total row-group bytes actually fetched: **~1.4 MB across 2 column-chunk entries**, in the `rowgroup` pool.

(In practice Parquet readers may pre-fetch a bit more, but the principle holds: only what `LIMIT 10` actually needed.)

### Step 4 — Files **00051-fff** and **00052-ggg** are never opened past the footer

Why? Because the Trino split scheduler queues splits but stops dispatching once `LIMIT 10` is satisfied by the first split. Their footers are cached (we read them in Step 2), but no row groups from those files are touched.

---

## What is in Shelf's cache after this one query

Total **10 cache entries**, one per byte range Trino actually asked for:


| #   | Pool     | Key (`sha256(etag‖offset‖length‖rg_ord)` → first 6 hex) | Object                 | Range                                         | Size   |
| --- | -------- | ------------------------------------------------------- | ---------------------- | --------------------------------------------- | ------ |
| 1   | metadata | `a13f02…`                                               | v00042.metadata.json   | full                                          | 12 KB  |
| 2   | metadata | `e8b714…`                                               | snap-7384029384-1.avro | full                                          | 8 KB   |
| 3   | metadata | `4c0029…`                                               | 1a2b3c-m0.avro         | full                                          | 6 KB   |
| 4   | metadata | `7711aa…`                                               | 1a2b3c-m1.avro         | full                                          | 80 KB  |
| 5   | metadata | `9088ee…`                                               | 1a2b3c-m2.avro         | full                                          | 80 KB  |
| 6   | metadata | `d0aa17…`                                               | 00050-eee.parquet      | bytes 536804864–536870903 (footer)            | 64 KB  |
| 7   | metadata | `2c7e44…`                                               | 00051-fff.parquet      | footer                                        | 64 KB  |
| 8   | metadata | `883b91…`                                               | 00052-ggg.parquet      | footer                                        | 64 KB  |
| 9   | rowgroup | `e7c4a1…`                                               | 00050-eee.parquet      | bytes 4–819203 (user_id col chunk, RG0)       | 800 KB |
| 10  | rowgroup | `f12d80…`                                               | 00050-eee.parquet      | bytes 1519204–2119203 (rating col chunk, RG0) | 600 KB |


**Total cached for "10 rows": ~1.78 MB** — split as ~378 KB metadata + ~1.4 MB row-group bytes.

That's it. Out of the 1.5 GB of Parquet files in the partition, **0.12 % is cached**. Out of the entire 3 GB table across 3 partitions, **0.06 % is cached**.

---

## Same query 5 minutes later — different user

```sql
SELECT user_id, rating FROM cdp.gold.gold_ratings WHERE date='2026-04-28' LIMIT 10;
```

```
Trino → shelfd: GET v00042.metadata.json
                                 → key a13f02… → HIT (DRAM, ~50 µs)
Trino → shelfd: GET 1a2b3c-m1.avro
                                 → key 7711aa… → HIT (DRAM, ~50 µs)
Trino → shelfd: GET 00050-eee.parquet bytes=-8
                                 → key d0aa17… → HIT (DRAM)
Trino → shelfd: GET 00050-eee.parquet bytes=536804864-…  (footer)
                                 → key d0aa17… → HIT (DRAM)
Trino → shelfd: GET 00050-eee.parquet bytes=4-819203     (user_id RG0)
                                 → key e7c4a1… → HIT (NVMe, ~3 ms)
Trino → shelfd: GET 00050-eee.parquet bytes=1519204-…    (rating RG0)
                                 → key f12d80… → HIT (NVMe, ~3 ms)

Origin S3 GETs: 0
Wall time:  ~6 s (cold)  →  ~1.5 s (warm)
```

## Slightly different query — same partition, different filter

```sql
SELECT user_id, rating FROM cdp.gold.gold_ratings
WHERE date='2026-04-28' AND device = 'ios'
LIMIT 10;
```

- Steps 1–11 (metadata + 3 footers): **all HIT** (already cached)
- Step 12–13: also need to read the `device` column chunk to filter on it → **MISS** for the new column chunk → fetched once, cached
- Result: ~600–900 KB of incremental bytes fetched from S3, then permanently cached too

This is the cumulative compounding behaviour — the more queries hit the same partition, the more of its row groups settle into Shelf, asymptotically approaching what the working set actually demands (not what the table contains).

## What happens when Iceberg compacts the partition

Tomorrow morning a `OPTIMIZE` job rewrites `date=2026-04-28/` into one big file `00099-zzz.parquet` with a brand-new etag.

```
Old keys:  d0aa17 (00050 footer), 2c7e44, 883b91, e7c4a1 (RG0 user_id), f12d80 (RG0 rating)
New keys:  derived from etag of 00099-zzz.parquet → completely different sha256 prefixes
```

The **old 5 entries become unreachable orphans** — no manifest points to those files anymore, so no query asks for them, so they sit in NVMe until Foyer evicts them on capacity. **Zero invalidation logic, zero risk of stale reads** — this is the ETag-keyed safety guarantee in action.

---

## TL;DR for this query


| Stat                               | Value                                                 |
| ---------------------------------- | ----------------------------------------------------- |
| Rows returned to user              | 10                                                    |
| Partitions scanned                 | 1 (out of 3 candidates pruned to 1)                   |
| Files opened                       | 1 (out of 3 in the partition)                         |
| Row groups read                    | 1 (out of ~128 in the file)                           |
| Column chunks read                 | 2 (out of 6 in the row group)                         |
| Bytes read from S3 (cold)          | ~1.78 MB                                              |
| Bytes read from S3 (warm, 2nd run) | 0                                                     |
| Cache entries created              | 10 (5 Iceberg metadata + 3 footers + 2 column chunks) |
| Total cached bytes for this query  | ~1.78 MB                                              |


The "magic" isn't really magic — it's **Iceberg + Parquet doing the projection/partition/row-group pruning for you, and Shelf just caching the tiny byte ranges that survived the funnel**.