#!/usr/bin/env python3
"""Regenerate the SHELF-04 golden-vector fixture.

The fixture lives at
    shelfd/tests/fixtures/shelf04_golden_vectors.txt
and is loaded by both the Rust `store::key_tests` and the Java
`io.shelf.client.KeyTest` suite. Because the two languages must agree
byte-for-byte, we keep the generator language-neutral (Python with the
stdlib only) and commit the output.

Usage:
    python3 tools/gen_shelf04_golden.py > shelfd/tests/fixtures/shelf04_golden_vectors.txt

The list of input tuples must be kept in lockstep with the `GOLDEN_INPUTS`
constants in the Rust and Java tests. If you need a new vector, add it
in all three places in the same commit.
"""
from __future__ import annotations

import hashlib
import struct
import sys

# Kept in lockstep with:
#   - shelfd/src/store.rs::GOLDEN_INPUTS
#   - clients/trino/src/test/java/io/shelf/client/KeyTest::GOLDEN_INPUTS
GOLDEN_INPUTS: list[tuple[str, int, int, int]] = [
    # -- SHELF-04 baseline --
    ('"9f8e6e48a1f7e2c3b5d41234567890ab"', 0,           8_192,  0),
    ('"aa11bb22cc33dd44ee55ff6677889900"', 536_854_528, 65_536, 0),
    ('"aa11bb22cc33dd44ee55ff6677889900"', 536_854_528, 65_536, 3),
    ('"d41d8cd98f00b204e9800998ecf8427e-7"', 1,         1,      42),
    # -- SHELF-16: row-group ordinal variants --
    # Same (etag, offset, length), three distinct rg ordinals (0, 1, 7).
    ('"rg-ordinal-sweep"',              4_096,                      131_072,         0),
    ('"rg-ordinal-sweep"',              4_096,                      131_072,         1),
    ('"rg-ordinal-sweep"',              4_096,                      131_072,         7),
    # Offset = u64::MAX / 2 with ordinal 0 and 255 (upper half of offset
    # space exercises every byte lane of the LE u64 encoding).
    ('"big-offset"',                    9_223_372_036_854_775_807,  16,              0),
    ('"big-offset"',                    9_223_372_036_854_775_807,  16,              255),
    # Length = 1 byte with ordinal = u16 ceiling (65_535).
    ('"single-byte"',                   0,                          1,               65_535),
    # Length = 16 MiB with ordinal 4_096 (rg-count scale).
    ('"row-group-xl"',                  0,                          16 * 1024 * 1024, 4_096),
    # Multipart-form ETag with ordinals 0 and 2. The literal value
    # includes both outer double-quotes and the `-N` multipart suffix
    # marker; we treat it as opaque bytes.
    ('""-multipart"',                  0,                          4_096,           0),
    ('""-multipart"',                  0,                          4_096,           2),
    # ASCII-only 8-byte ETag (no surrounding quotes — 8 bytes exactly),
    # every ordinal in 0..=3 to pin the hot-path ordinals.
    ('shelf16b',                          2_048,                      8_192,           0),
    ('shelf16b',                          2_048,                      8_192,           1),
    ('shelf16b',                          2_048,                      8_192,           2),
    ('shelf16b',                          2_048,                      8_192,           3),
]

HEADER = """# SHELF-04 golden vectors — shared by Rust and Java key tests.
#
# Each non-empty, non-comment line is the lowercase-hex sha256 output
# of `key_from_tuple(etag, offset, length, rg_ordinal)` for the input
# tuple at the same position in GOLDEN_INPUTS (shelfd/src/store.rs)
# and GOLDEN_INPUTS (clients/trino/src/test/java/io/shelf/client/KeyTest.java).
#
# Changing any line here changes the on-disk cache layout. See ADR-0011.
# Regenerate by running `python3 tools/gen_shelf04_golden.py` (kept
# deliberately language-neutral so the fixture remains auditable).
"""


def digest(etag: str, offset: int, length: int, ordinal: int) -> str:
    h = hashlib.sha256()
    h.update(etag.encode("utf-8"))
    h.update(struct.pack("<Q", offset))
    h.update(struct.pack("<Q", length))
    h.update(struct.pack("<I", ordinal))
    return h.hexdigest()


def main() -> int:
    sys.stdout.write(HEADER)
    for etag, offset, length, ordinal in GOLDEN_INPUTS:
        sys.stdout.write(digest(etag, offset, length, ordinal) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
