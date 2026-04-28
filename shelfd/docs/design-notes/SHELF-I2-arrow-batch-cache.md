# I2 — Pool::Arrow record-batch cache (design spike)

**Status:** research spike, v2 material. Track only; do not ship in
v1.
**Owning ticket:** `i2-arrow-batch-cache`.
**Related:** `BLUEPRINT §E9 defer: in-DRAM Arrow record-batch cache`,
`§8.1 Arrow Flight data plane`.

## The question

How much of a warm-query's wall-clock is spent *decoding* Parquet
column chunks to Arrow on the Trino worker? If the answer is
"≥ 20 %", then a third shelf pool that serves already-decoded
Arrow record batches would uncork a gear that neither Warp Speed
nor Alluxio has. If the answer is "≤ 5 %", the whole idea is a
memory budget sink for a rounding-error win.

Track F's flame graphs — not this design note — answer the
question. I2 ships only if F says yes.

## The proposed pool

`Pool::Arrow` sits alongside `Pool::Metadata` and `Pool::RowGroup`:

| | Metadata | RowGroup | Arrow |
|---|---|---|---|
| Unit | Puffin / manifests / page index | Parquet rowgroup byte range | Arrow `RecordBatch` for a column chunk |
| Tier | DRAM | DRAM + NVMe | DRAM only |
| Admission | pin-list + size threshold | admission policy | **fingerprint-matched hot columns only** |
| Byte budget | ~8 GiB | ~3 TiB NVMe + 64 GiB DRAM | ≤ 32 GiB DRAM |
| Consumer | Iceberg connector | Parquet reader | Trino operator (new SPI) |

The key design choice is **what to admit**. Naive caching of every
column chunk would 3-5× the memory bill without a matching win;
the only economical version is "pre-decode the columns the H1
fingerprint telemetry says are hot for the top-20 fingerprints".

## Admission shape

```
admit(column_chunk) iff
    fingerprint_weight(column_chunk) ≥ ADMISSION_THRESHOLD
  ∧ is_primitive_or_dict_type(column_chunk.schema)
  ∧ projected_in_top_N_fingerprints(column_chunk.column)
```

`fingerprint_weight` comes out of the E7 canonical-plan counter
(`shelf_queries_served_total` + `shelf_bytes_saved_total`).
Admission is conservative on purpose — we'd rather have 30 % DRAM
utilisation than evict a hot batch mid-query.

## The SPI problem

Trino's connector SPI today exposes bytes from a `TrinoFileSystem`
and leaves Parquet decoding to its own reader. To skip that decode
step we need a new SPI that lets shelf's blob-cache plugin
short-circuit with a `RecordBatch` stream. The candidates:

1. **SHELF-29 BlobCacheManager (trinodb/trino#29184).** The PR
   currently handles byte-level caching; it would need an Arrow
   extension. Tracked already in Track C.
2. **Arrow Flight between shelfd and Trino.** Heavier; a protocol
   change rather than an SPI extension. `BLUEPRINT §8.1` keeps
   this strictly v2+.
3. **Trino fork.** Off the table — we want shelf to work against
   upstream binaries so the TPC-DS SF1000 story is reproducible
   on any cluster.

**Recommendation:** block I2 on #1 landing, then design the Arrow
extension as a follow-up.

## Memory + latency math

For our top-20 fingerprints (from the H1 advisor's cost model),
the hot column set is ~18 columns averaging 40 MB/column/snapshot
after Arrow encoding. That's roughly 720 MB per snapshot; at 10
live snapshots across the warehouse that's ~7 GB. Budget the pool
at 32 GB to leave headroom for hot-set growth and keep it DRAM
only — NVMe-tier Arrow would defeat the point (decode cost
returns).

Latency budget we need to beat: Parquet decode on a 1 MB chunk
with LZ4 is ~1.2 ms on a Graviton3 worker; an Arrow `RecordBatch`
of the same shape is a zero-copy pointer hand-off. Ship criterion:
shelf warm p50 drops by ≥ 150 ms on the top-20 fingerprints.

## Correctness

Arrow batches are derived data: a lossy encoding of the Parquet
source. Two invariants:

1. **Column-chunk content hash must match.** Admit only when the
   Arrow batch was decoded from the *exact* Parquet bytes that
   shelf currently holds for `(file_etag, rg_ordinal, column)`.
   On Parquet eviction/replacement, the Arrow entry must be
   invalidated in the same transaction.
2. **Schema/dictionary pinning.** Dictionary-encoded columns must
   ship their dictionaries together; otherwise a Trino worker
   that looked up `Pool::Arrow` without the dictionary would
   produce garbled strings.

The v2 `MvRegistry` pattern (H5) is the right model: a small,
single-writer registry mapping Parquet keys to live Arrow batches
so eviction stays consistent.

## Why this isn't v1

Three hard blockers, each independently sufficient:

1. **No SPI.** Until SHELF-29 lands + extends, Trino cannot
   consume Arrow from a cache.
2. **F-track evidence missing.** We have no flame graph from
   SF1000 saying Parquet decode is 20 % of warm wall-clock. Until
   we do, building Arrow cache is speculative memory.
3. **Operational complexity.** A third pool needs its own
   admission, eviction, and telemetry story. Shipping that
   without an evidence-backed win slows every other track.

## Experiment plan (6 months out)

1. After F1-F4 goes green, run SF1000 on the shelf-only cluster
   and `perf record` + `async-profiler` the Trino workers.
   Aggregate the Parquet decode bar.
2. If decode < 10 % of warm wall-clock, close I2 as unviable.
3. If decode is 10-20 %, keep I2 parked; revisit on the next
   hardware generation (Arrow wins scale with SIMD width).
4. If decode ≥ 20 %, promote I2 to a ticketed feature with a
   detailed ADR: SPI dependency, admission policy, memory budget,
   rollback plan.
