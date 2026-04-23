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

GOLDEN_INPUTS: list[tuple[str, int, int, int]] = [
    ('"9f8e6e48a1f7e2c3b5d41234567890ab"', 0,           8_192,  0),
    ('"aa11bb22cc33dd44ee55ff6677889900"', 536_854_528, 65_536, 0),
    ('"aa11bb22cc33dd44ee55ff6677889900"', 536_854_528, 65_536, 3),
    ('"d41d8cd98f00b204e9800998ecf8427e-7"', 1,         1,      42),
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
