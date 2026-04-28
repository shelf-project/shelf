# SHELF-G2 — Shelf-learned side bloom filters

> Status: **primitives landed**, producer/consumer wiring pending.
> Ships: `shelfd::side_bloom::SideBloom`, `SideBloomKey`.

## Shape

Classic bloom with Kirsch-Mitzenmacher double hashing. Sized
from `(expected_items, fpp)`; defaults match BLUEPRINT §7.4.2
(`n = 10 M`, `fpp = 0.01`). Footprint is capped at ~12 MiB per
filter; operators who want the 1 MiB BLUEPRINT target pick
FPP 0.05 or cap `expected_items` lower at config time.

One filter per `(file_etag, row_group_ordinal, column)`.

## Pipeline (deferred)

1. **Admission hook** — on admission of row group R of file F
   for allowlisted column C, sample values off the Parquet
   data page stream and feed `SideBloom::insert`. Fires async
   after admission so the read path isn't blocked; budgeted at
   10-30 ms/row group per BLUEPRINT.
2. **Storage** — the finished filter lands in `Pool::Metadata`
   under `SideBloomKey::cache_key()`; zstd (E2) compresses the
   sparse regions for ~2-3x capacity.
3. **Query** — the G4 `ShelfFilterService::SideBloom` impl
   reads the filter via `FoyerStore::get` and calls
   `SideBloom::contains`.

Steps 1 and 3 depend on D3 (page-index cache: needed to walk
data pages cheaply) and G4 (the service itself, now scaffolded).

## Allowlist

Columns are only ever indexed if they appear in the nightly
`trino_logs` top-N `WHERE column = value` predicate extraction.
The allowlist is a pinned metadata key at
`Pool::Metadata` `allowlist/side-bloom.json`, refreshed by the
HMS poller's sibling CronJob. Columns outside the allowlist
have zero footprint.

## Correctness

- `inserted_values_are_always_present` — no false negatives.
- `fpp_within_target` — at FPP 0.01 with n=1000, the observed
  false-positive rate over 10 000 random non-members stays
  under 4 % (a 4x slack on the 1 % target keeps the test
  stable on CI).
- `sizing_respects_caps` — adversarial cardinality is clamped
  to ≤ 16 MiB.

## Why not `bloomfilter`-the-crate?

A single-file stdlib-only implementation with `DefaultHasher`
keeps build times low and avoids pulling in a new dependency
for a ~150-line primitive. If we ever need SIMD-accelerated
lookups the external crate is an easy drop-in — the
`SideBloom` struct's bit-vector is byte-identical to the
`bloomfilter` layout, so a migration is a module rename.
