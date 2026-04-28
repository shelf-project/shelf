# SHELF-16 — Row-group byte-range key extension

- Status: **landed** (key extension + plumbing, SHELF-16a)
- Delivery level for this ticket: **SCAFFOLDED**
- Follow-up: **SHELF-16b — Parquet footer TCompactProtocol parser**
  (landed; see
  [`SHELF-16b-parquet-footer-parser.md`](./SHELF-16b-parquet-footer-parser.md))

## Problem

Until SHELF-04 we keyed Shelf cache entries on `(etag, offset, length)`.
That is enough to avoid cross-file collisions, but it does **not**
distinguish two reads of the _same byte range_ that logically belong to
different row groups. In a pathological case — two Parquet files written
by the same job at the same second with the same size end up with
identical ETag-equivalents and a Trino reader asks for offset 64 KiB,
length 4 KiB on each — the cache would serve the first file's bytes for
the second file's read. The bug is quiet: Trino gets data that is the
right _shape_ but the wrong content.

SHELF-04 extended the key to
`sha256(etag || le_u64(offset) || le_u64(length) || le_u32(rg_ordinal))`
and SHELF-16 is where the plugin side actually _uses_ the new field.

## Invariant

> Every range read issued by the plugin is keyed under the ordinal of
> the row group that fully contains it. Ranges that do not map to a
> known row group (footer reads, non-Parquet files, pre-footer-parse
> Parquet reads) are keyed under ordinal `0`, which is the plugin's
> canonical "unknown" sentinel.

The consequence: a read against `(file X, offset, length, rg 2)` and a
read against `(file X, offset, length, rg 3)` always hit distinct cache
keys. The acceptance test
`io.shelf.client.KeyTest#keysDifferByRowGroupOrdinal` pins that line.

## Pieces that shipped in SHELF-16a

### Extended golden-vector fixture

`shelfd/tests/fixtures/shelf04_golden_vectors.txt` grew from 4 to 17
entries. The new inputs cover:

- the same `(etag, offset, length)` under three distinct ordinals
  (`0`, `1`, `7`) — drops `rg_ordinal` from the key preimage and the
  test explodes on both sides simultaneously;
- `offset = u64::MAX / 2` with ordinals `0` and `255` — exercises the
  upper half of the LE `u64` encoding and flips an entire `u32` byte;
- `length = 1` with ordinal `65_535` (u16 ceiling);
- `length = 16 MiB` with ordinal `4096` (row-group-count scale);
- multipart-form ETag with ordinals `0` and `2`;
- an 8-byte ASCII ETag with every ordinal in `0..=3`.

Both `shelfd::store::key_tests::golden_vectors_match_fixture` and
`io.shelf.client.KeyTest#goldenVectorsMatchSharedFixture` diff the same
fixture file, so any algorithm drift breaks both builds immediately.

### `RowGroupIndex` abstraction

```java
interface RowGroupIndex {
    int ordinalFor(long offset, long length);
    boolean hasKnownOrdinals();
    static RowGroupIndex constantZero() { ... }
}
```

- `ConstantOrdinalIndex` — stateless, ordinal = 0 for every range;
  returned by `RowGroupIndex.constantZero()`; the default for every
  non-Parquet path.
- `ParquetFooterIndex` — wraps a parsed row-group list and answers
  `ordinalFor` with a linear scan over the sorted list. Ranges that
  straddle two row groups return `0` (the unknown sentinel); a single
  key is strictly better than one that never hits the cache, but the
  ordinal is still conservative.

### Per-range keying in `ShelfInputStream`

Constructor signature changed from
`(delegate, fetcher, breaker, endpoint, pool, contentKey, length)` to
`(delegate, fetcher, breaker, endpoint, pool, etag, index, length)`.
Each `read()` now derives its own key:

```java
int rgOrdinal = index.ordinalFor(position, want);
String contentKey = Key.fromTuple(etag, position, want, rgOrdinal).toHex();
byte[] bytes = fetcher.rangeGet(endpoint, pool, contentKey, position, want);
```

`ShelfInputFile` feeds in the `etag` (bytes derived from
`lastModified + "-" + length` until SHELF-07's HEAD endpoint lands) and
threads through the `RowGroupIndex`. The fail-open envelope is
unchanged.

### Routing stays file-level

Membership lookup still uses a single _file-level_ key
(`Key.fromTuple(etag, 0, length, 0)`). Routing per row-group would
fragment the working set across too many pods and defeat locality. The
file-level routing key is never exposed on the wire — only the
per-range keys are — so this decision is invisible to `shelfd`.

### SHELF-15 footer prefetch realignment

`ShelfFileSystem#maybePrefetchFooter` now derives the footer key from
the actual byte range `[length - window, length)` rather than the
file-level range. That is the _only_ range the prefetch emits, so the
foreground footer read's per-range key is guaranteed to match. Without
this change, SHELF-15 would collapse to a 0% hit ratio under SHELF-16.

## What was deferred: SHELF-16b

`ParquetFooterIndex.fromFooter(byte[] footerBytes, long fileLength)`
**ships as a scaffold** that always returns `Optional.empty()`. The
TCompactProtocol reader (zigzag varints, nested struct header state
machine, list-of-struct decoding to reach `row_groups[*].file_offset`,
`total_compressed_size`, `ordinal`) is ~200 lines of careful code; the
risk profile under this ticket's time budget pushed it to a dedicated
follow-up:

> **SHELF-16b — Hand-rolled Parquet TCompactProtocol footer reader.**
> Implement `ParquetFooterIndex.fromFooter` against Parquet spec
> §FileMetaData; turn on the `parseFooter_*` tests in
> `RowGroupIndexTest`; wire `ShelfFileSystem` to feed captured footer
> bytes into `ParquetFooterIndex.fromFooter` when they arrive from the
> SHELF-15 prefetch path. No wire-format or `shelfd` change.

SHELF-16a is independently useful even before SHELF-16b lands: the
key now includes the ordinal, so when a future write path (SHELF-17
rewrite, SHELF-18 NVMe) starts landing bytes under real ordinals, the
keys already match. The plugin simply returns `0` for the ordinal
until the parser ships.

## Testing matrix

| Test                                                        | Asserts                                                            |
| ----------------------------------------------------------- | ------------------------------------------------------------------ |
| `shelfd::store::key_tests::golden_vectors_match_fixture`    | Rust ↔ fixture parity for 17 entries including ordinal variants    |
| `io.shelf.client.KeyTest#goldenVectorsMatchSharedFixture`   | Java ↔ fixture parity for the same 17 entries                      |
| `io.shelf.client.KeyTest#keysDifferByRowGroupOrdinal`       | `(file X, rg 2) ≠ (file X, rg 3)` — SHELF-16 acceptance line       |
| `io.shelf.client.RowGroupIndexTest#constantZero_*`          | Sentinel returns 0 everywhere and is a singleton                   |
| `io.shelf.client.RowGroupIndexTest#parquetFooterIndex_*`    | Row-group range lookup, sorting, overlap rejection, empty handling |
| `io.shelf.client.RowGroupIndexTest#fromFooter_scaffold*`    | SHELF-16a scaffold contract: never throws, always `Optional.empty` |
| `ShelfInputStreamTest#contentKeyDiffersBetweenRowGroupOrdinals` | On-wire contentKey changes between rg#0 and rg#1 reads         |

## Out of scope for SHELF-16

- Page-level (row-group page) granularity — tracked as SHELF-23.
- Feeding the captured footer bytes from SHELF-15 prefetch into
  `ParquetFooterIndex.fromFooter` — blocked on SHELF-16b landing the
  parser first.
- Swapping the `(lastModified, length)` ETag-equivalent for the real
  S3 ETag via SHELF-07's HEAD endpoint — format-compatible, landed
  separately.
