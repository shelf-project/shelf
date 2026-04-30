# ADR 0011: SHELF-04 key = sha256(etag || le_u64(offset) || le_u64(length) || le_u32(rg_ordinal))

_Status: Accepted (2026-04-23)_
_Deciders: rust-engineer-1, rust-engineer-2, trino-plugin-eng-1_
_Supersedes: none_
_Superseded-by: none_

## Context

Every byte that transits `shelfd` is addressed by a single 32-byte content
key. That key is derived on three machines &mdash; the Trino worker JVM,
the `shelfd` Axum handler, and the `shelfctl` CLI &mdash; and must agree
byte-for-byte across all three. A mismatch of a single byte produces a
silent cache miss that looks, in metrics, like a cold-cache workload:
the worst failure mode we have because it is invisible in graphs. We
therefore pin the algorithm, the input order, the integer encoding, and
the expected digests in an ADR.

Secondary requirement: the derivation must be cheap (Parquet plugin
may derive ~10^4 keys per query) and must use only stdlib / existing
workspace deps.

## Decision

```
key = sha256(
        etag                                 // raw bytes, no quotes stripped
     || offset.to_le_bytes()                 // u64, 8 bytes, little-endian
     || length.to_le_bytes()                 // u64, 8 bytes, little-endian
     || rg_ordinal.to_le_bytes()             // u32, 4 bytes, little-endian
     )
```

Inputs:

- **etag**: the `ETag` header byte-for-byte as S3 returned it. Quotes
  included if S3 included them. Treated as an opaque version token; not
  required to be a cryptographic hash.
- **offset**: zero-based byte offset into the object.
- **length**: range length in bytes. Must be `> 0`. Zero is rejected at
  the API boundary on both sides (Rust `Error::InvalidKey`, Java
  `IllegalArgumentException`).
- **rg_ordinal**: row-group ordinal, zero-based. `0` for non-columnar
  payloads (manifests, footers, page indexes) so the function covers
  every pool with one signature.

Encoding invariant: **little-endian** everywhere. Rust's
`u64::to_le_bytes()` and `u32::to_le_bytes()`, Java's `ByteBuffer` with
`ByteOrder.LITTLE_ENDIAN`, Python's `struct.pack("<Q", ...)` and
`struct.pack("<I", ...)` all produce identical byte layouts.

Output: the 32-byte sha256 digest, rendered as 64 lowercase hex
characters when expressed as a string (HTTP paths, Prometheus
exemplars, log lines).

## Consequences

- **Cross-language agreement is testable.** A shared fixture file
  `shelfd/tests/fixtures/shelf04_golden_vectors.txt` holds the hex
  digest of four canonical input tuples. Both `shelfd::store::key_tests`
  and `io.shelf.client.KeyTest` load that file and diff against their
  own computation. A one-byte drift fails CI instantly on both sides.

- **The fixture is the source of truth.** Changing any line in the
  fixture without a new ADR is a protocol break: existing caches would
  have to be flushed and every plugin version bumped in lockstep.
  The generator lives at `tools/gen_shelf04_golden.py` and uses Python
  stdlib only so the fixture stays auditable by a reviewer who trusts
  neither Rust nor Java.

- **Multipart-ETag caveat is documented in both Rust and Java.** S3's
  ETag is not a cryptographic object hash; we pass it as opaque
  version bytes. Callers who need integrity verification must layer it
  on top (e.g. via the Iceberg metadata's `record_count` + content
  hashes), not via the cache key.

- **SHA-256 is overkill for collision avoidance but free in practice.**
  Modern CPUs execute one SHA-256 block in < 500 ns; at ~10^4 derivations
  per query this costs microseconds, well below any S3 RTT. We considered
  BLAKE3 (faster) but rejected it because every supported JVM ships
  `MessageDigest.getInstance("SHA-256")` natively while BLAKE3 would add
  a plugin dependency and a classloader footprint.

- **`rg_ordinal` is a u32, not a u16.** A Parquet file is technically
  limited to `Integer.MAX_VALUE` row groups; we keep the full 32-bit
  range so the key survives pathological cases (e.g. a badly-partitioned
  CDC sink with millions of tiny row groups).

- **Length-0 is rejected.** It has no meaning in a cache that stores
  byte ranges and historically has been the source of silent cache
  misses during plugin refactors. We fail loud.

## Test surface

- `shelfd::store::key_tests::golden_vectors_match_fixture` &mdash; Rust
  side.
- `io.shelf.client.KeyTest#goldenVectorsMatchSharedFixture` &mdash; Java
  side.
- `tools/gen_shelf04_golden.py` emits the fixture bytes; CI verifies
  the generator's output equals the committed file.

## References

- `shelfd/src/store.rs` (`Key`, `key_from_tuple`, `key_tests`)
- `clients/trino/src/main/java/io/shelf/client/Key.java`
- `clients/trino/src/test/java/io/shelf/client/KeyTest.java`
- `shelfd/tests/fixtures/shelf04_golden_vectors.txt`
- `tools/gen_shelf04_golden.py`
- `agents/out/03-plan.md` §4 SHELF-04
