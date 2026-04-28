# SHELF E2 — zstd on Pool::Metadata

## Goal

Increase the *effective* capacity of `Pool::Metadata` without
increasing the DRAM footprint, by storing Iceberg/Hive metadata
objects (metadata.json, manifest lists, Avro-deserialisable
manifests, Puffin stats) compressed.

## What shipped

`shelfd/src/compression.rs`:

- `encode(Bytes) -> Bytes` — tagged-frame encoder. 1-byte header
  distinguishes compressed (`0x5A`) vs uncompressed-fallback
  (`0x00`). Falls back to uncompressed for inputs smaller than
  `MIN_COMPRESS_BYTES = 256` or when zstd produced no benefit.
- `decode(Bytes) -> Bytes` — inverse, errors on unknown header.
- `inspect(Bytes) -> CompressionOutcome` — observability hook used
  by the benchmark.
- 7 unit tests covering: round-trip, JSON ≥4× compression, random
  bytes refusing to inflate, empty inputs, corrupt headers, small
  inputs, inspect classification.
- Feature flag `zstd_metadata` defined in `shelfd/Cargo.toml`
  (default **off**). The feature flag controls whether the store
  wraps entries; the `compression` module itself is always compiled
  so it can be benchmarked out-of-band.

## Expected savings

On the 7-day rep-2 trace (`benchmarks/trino_logs/fixtures/rep2/`),
metadata payloads are dominated by:

| Shape | Size (p50) | Expected zstd ratio |
| --- | --- | --- |
| `metadata.json` | 60 KiB | 6–10× (very repetitive JSON) |
| manifest-list (Avro) | 4 KiB | 1.5–2× (already self-indexed) |
| manifest Avro | 120 KiB | 3–5× |
| Puffin stats | 2 KiB | 1.2× (already encoded) |

Weighted average across the trace: **≈ 4.5×**. A 4 GiB metadata
pool therefore behaves like ~18 GiB, which should compress the
cold-manifest long tail into DRAM for the first time.

## Wiring plan (behind `zstd_metadata` feature)

`FoyerStore::open` inserts a compression wrapper around the
metadata pool's weighter:

```rust
#[cfg(feature = "zstd_metadata")]
let metadata_cache = foyer::CacheBuilder::new(metadata_capacity as usize)
    .with_weighter(|_k: &Key, v: &Bytes| v.len())
    .build();
```

The weighter already counts **stored** bytes, so compressed entries
take their compressed length against the capacity — exactly the
shape we want.

`get` / `get_or_fetch` decode on read:

```rust
#[cfg(feature = "zstd_metadata")]
let bytes = compression::decode(&stored).map_err(Error::from)?;
```

`insert` / `admit` encode on write:

```rust
#[cfg(feature = "zstd_metadata")]
let stored = compression::encode(&bytes).map_err(Error::from)?;
```

Both code paths emit:

- `shelf_compression_outcome_total{pool, outcome}` where
  `outcome ∈ {compressed, skipped_small, skipped_incompressible}`
- `shelf_compression_ratio_x100` histogram labelled by pool so we
  can observe the distribution of savings in prod.

## Benchmark harness (new)

`benchmarks/compression/` runs the encoder over a sampled metadata
corpus (collected from the rep-2 prewarm dry-run) and reports:

| Column | Meaning |
| --- | --- |
| `kind` | metadata / manifest-list / manifest / puffin / other |
| `p50_bytes` | pre-compression payload size |
| `p50_ratio_x100` | `(orig - encoded) / orig` × 100 |
| `encode_us_p50` | zstd level-3 encode latency per payload |
| `decode_us_p50` | zstd decode latency per payload |
| `effective_capacity_x` | projected expansion factor |

Gate: `effective_capacity_x ≥ 2.5` and `decode_us_p50 ≤ 50 µs` on
rep-2. If either fails, we do not flip the feature on for the soak.

## Risk + rollback

- **Risk**: decode adds CPU cost on every read. Mitigated by
  (a) falling back to uncompressed for small inputs; (b) level-3
  zstd decode is ~1 GiB/s per core, so even a 1 MiB metadata read
  is <1 ms; (c) the feature is off by default.
- **Rollback**: clear the `zstd_metadata` feature flag and redeploy.
  Mixed old/new entries decode cleanly because the tagged-frame
  header distinguishes them.

## Action items (tracked)

1. [ ] Wire `#[cfg(feature = "zstd_metadata")]` into
   `FoyerStore::{insert, get, admit}` per the snippets above.
2. [ ] Add `compression::CompressionOutcome` metrics emission.
3. [ ] Land `benchmarks/compression/` harness + fixture corpus.
4. [ ] Run the gate on rep-2's 7-day corpus.
5. [ ] If gated, flip `zstd_metadata` on for rep-2 during the soak
   window and observe hit-rate uplift in
   `shelf_hits_total{pool="metadata"}`.

## Why we are not shipping the wiring in this session

The `compression` primitive is the expensive part to get right
(tagged-frame header, graceful fallback on incompressibility). The
store wiring is a few dozen cfg-gated lines that need to land with
the benchmark harness, so both can be reviewed together. Shipping
the primitive + feature flag today unblocks the benchmark work.
