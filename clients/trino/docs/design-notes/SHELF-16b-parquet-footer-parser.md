# SHELF-16b — Parquet footer TCompactProtocol reader

- Status: **landed**
- Depends on: SHELF-16a (row-group key extension + `RowGroupIndex` scaffold)

## Problem

SHELF-16a shipped the end-to-end plumbing for row-group-aware cache
keys — including the `ParquetFooterIndex` type, its sorted row-group
list, `ordinalFor()` linear scan, the `ConstantOrdinalIndex`
fallback, and integration through `ShelfInputStream` / `ShelfInputFile`.
What it deliberately did not ship was the actual footer decoder:
`ParquetFooterIndex.fromFooter(byte[], long)` always returned
`Optional.empty()`, which forced every Parquet read to fall through to
the constant-zero ordinal namespace.

SHELF-16b replaces that stub with a hand-rolled Thrift TCompactProtocol
reader that walks the Parquet `FileMetaData` struct and extracts each
row group's `(file_offset, total_compressed_size, ordinal)` tuple.

## Non-goals

- Any Thrift service / RPC support. We decode a single Parquet struct;
  the reader is not a general-purpose Thrift runtime.
- Any Thrift generated-code dependency. Libthrift, parquet-format, and
  parquet-thrift would each add 200–500 KiB of runtime dependency for
  code paths that Parquet's own layout has not changed in years. We
  hand-roll the ~80 lines of varint / field-header decoding that we
  actually need.
- Parquet Modular Encryption. An encrypted footer (`PARE` magic) is
  detected and treated as "unparseable, fall through to constant-zero."
  Support lands separately if we ever need to cache encrypted Iceberg.

## Parser shape

Two new files under `src/main/java/io/shelf/client/`:

| File                          | Role                                                                                                                              |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `CompactProtocolReader.java`  | Package-private TCompactProtocol cursor. Exposes `readVarint32/64`, `readZigzag32/64`, `readFieldHeader`, `enterList/Struct`, `skipField`, and a nested `ThriftParseException`. ~260 lines. |
| `ParquetFooterIndex.java`     | Extends the SHELF-16a scaffold with `fromFooter()`. Descends into `row_groups[*]` only; skips every other top-level field. ~450 lines total. |

The decoder understands exactly the type codes Parquet footers use
(`BOOL_TRUE`, `BOOL_FALSE`, `BYTE`, `I16`, `I32`, `I64`, `DOUBLE`,
`BINARY`, `LIST`, `SET`, `MAP`, `STRUCT`), handles the delta-encoded
field-id nibble, and tracks an `ArrayDeque` of parent-struct field ids
so `enterStruct` / `exitStruct` restore correct delta reference frames.

### FileMetaData walk

```
FileMetaData {
  1: version           (i32)  — skipped
  2: schema            (list<struct>) — skipped
  3: num_rows          (i64)  — skipped
  4: row_groups        (list<struct>) — DESCEND HERE
  5: key_value_metadata(list<struct>) — skipped
  6: created_by        (string) — skipped
  7: column_orders     (list<struct>) — skipped
  8: encryption_algorithm (struct) — skipped
  9: footer_signing_key_metadata (binary) — skipped
}
```

For each `RowGroup`:

```
RowGroup {
  1: columns                (list<struct>)  — needed for fallbacks
  2: total_byte_size        (i64) — uncompressed, ignored
  3: num_rows               (i64) — ignored
  4: sorting_columns        (list<struct>) — skipped
  5: file_offset            (i64, optional) — captured when present
  6: total_compressed_size  (i64, optional) — captured when present
  7: ordinal                (i16, optional) — captured when present
}
```

### Fallback priorities

Per the Parquet spec, row-group `file_offset`, `total_compressed_size`,
and `ordinal` are all optional. Pre-2020 writers (Parquet <= 1.10,
Spark <= 2.4.x fast path) routinely omit them. The reader applies the
same fallbacks Parquet's own readers use:

1. **`file_offset`** → `columns[0].file_offset` → `min(columns[0].meta_data.data_page_offset, columns[0].meta_data.dictionary_page_offset)`.
2. **`total_compressed_size`** → `sum(columns[*].meta_data.total_compressed_size)`.
3. **`ordinal`** → index of this row group in the parsed list
   (post-sort-by-offset). Parsed-list index, not wire order, so the
   fallback is stable under writer reordering.

A row group that fails *every* fallback (no columns, no page offsets,
no column sizes) fails the whole parse → `Optional.empty()`.

## Fail-open contract

The single biggest requirement of the footer reader is that it must
never throw into Trino. The caller (`ShelfInputFile`) already knows
how to degrade: if `fromFooter` returns empty, the file uses
`RowGroupIndex.constantZero()` and keys fall into the pre-SHELF-16
"unknown ordinal" namespace. The invariant is:

> `ParquetFooterIndex.fromFooter(footerBytes, fileLength)` terminates
> with either `Optional.of(index)` or `Optional.empty()`, regardless
> of what `footerBytes` contains. The only exception that ever escapes
> is `IllegalArgumentException` for `fileLength < 0`, which is a
> programmer error at the call site, not a data bug.

Concretely, the top-level `try { ... } catch (RuntimeException e)` in
`fromFooter` absorbs:

| Failure class                                 | Example trigger                                           |
| --------------------------------------------- | --------------------------------------------------------- |
| `ThriftParseException`                        | Malformed field header, unknown type code, varint overflow|
| `ArrayIndexOutOfBoundsException`              | Buggy pre-parse magic / length checks let a malformed blob through |
| `ArithmeticException`                         | `Math.addExact` on row-group end overflows `long`         |
| `IllegalArgumentException` (record / index)   | Negative offset, zero size, overlapping row groups        |
| `NullPointerException`                        | Defensive — no code path should produce one, but we don't want to be wrong|

Every branch logs at `Level.FINE` so production operators can grep for
`Parquet footer parse failed` without taking any action; the plugin's
default log level stays `INFO`.

## Bounds discipline

Every read goes through `CompactProtocolReader.ensureAvailable(n)`,
which uses `long` arithmetic to detect end-of-buffer — a malicious
`int n = 0x7fffffff` still fails cleanly instead of wrapping around.
Varints are capped at 10 bytes for u64 and 5 bytes for u32; the reader
rejects any that exceed that cap rather than looping to exhaustion.
`ParquetFooterIndex.fromFooter` also rejects:

- `footerBytes.length < 8` (magic + length trailer wouldn't fit);
- declared `footer_length <= 0` or `> footerBytes.length - 8`;
- any row group whose `fileOffset + totalCompressedSize` exceeds the
  supplied `fileLength` (catch-all sanity check against a truncated
  object).

## Testing

### Hand-built blobs (deterministic)

Ten tests in `ParquetFooterIndexTest` drive the parser via a
`CompactProtocolWriter` test helper (`src/test/java/io/shelf/client/CompactProtocolWriter.java`).
They cover the happy path, `PARE` encrypted footer, bad magic,
truncated footer, row groups past `fileLength`, column-based
fallbacks for `file_offset` / `total_compressed_size`, page-offset
fallbacks when `ColumnChunk.file_offset` is absent, list-index
fallback for missing `ordinal`, and top-level field skipping.

### Real Parquet file (belt-and-braces)

`fromFooter_extractsRowGroupOffsets_fromRealParquetFile` writes a real
multi-row-group Parquet file using `parquet-hadoop:1.14.1` (test
scope) via `LocalOutputFile` — deliberately **without** going through
`HadoopInputFile` on the read side, because parquet-hadoop's reader
drags in `UserGroupInformation.getCurrentUser()` which calls the
JDK 25-removed `Subject.getSubject(AccessControlContext)`. Our own
reader is therefore the ground truth; the test asserts the writer
produced ≥ 2 row groups and that every parsed tuple lives inside
`(file_header, footer_start)` with dense 0..N-1 ordinals.

### Re-enabled contract tests

`RowGroupIndexTest.fromFooter_scaffoldReturnsEmpty` continues to
pin fail-open for bad inputs (empty tail, 0-length footer). It used
to double as the SHELF-16a scaffold assertion; with SHELF-16b landed
it reads as a regression fence: whatever happens, garbage in →
`Optional.empty()`, never a throw.

## Dependencies touched

`clients/trino/pom.xml` gained three **test-scope only** artifacts:

- `org.apache.parquet:parquet-hadoop:1.14.1`
- `org.apache.parquet:parquet-common:1.14.1`
- `org.apache.hadoop:hadoop-common:3.4.0`
- `org.apache.hadoop:hadoop-mapreduce-client-core:3.4.0`

All four are `<scope>test</scope>` and are never packaged into the
shaded plugin JAR. Hadoop's transitive zoo (YARN, HDFS, Jetty,
Jersey, Curator, Kerby, Bouncycastle, reload4j) is excluded; the
test plane needs only `Configuration` and `Path`. `slf4j-reload4j`
is explicitly excluded to keep parquet-hadoop's `log4j-over-slf4j`
bridge from fighting Hadoop's backend binder at class-init time.

## Parquet spec references

- [Apache Parquet `parquet.thrift`](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)
  — canonical `FileMetaData` / `RowGroup` / `ColumnChunk` / `ColumnMetaData` definitions.
- [Apache Thrift compact protocol spec](https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)
  — field header nibble encoding, zigzag varints, list/struct framing.
- [Parquet file layout](https://parquet.apache.org/docs/file-format/)
  — `[4-byte header magic][row groups][footer Thrift blob][4-byte footer length LE][4-byte trailer magic]`.

## What SHELF-16b does *not* do

- Actually feed footer bytes into `fromFooter` from the SHELF-15
  prefetch path — tracked as SHELF-16c. The parser contract is frozen,
  so this is a pure wiring change that touches `ShelfFileSystem` /
  `ShelfInputFile`.
- Page-level granularity (cache keys that include the page index
  within a row group) — tracked as SHELF-23.
- Parquet Modular Encryption support (`PARE`) — unscoped.
