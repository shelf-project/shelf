"""Content-key parity tests with Rust ``shelfd::store::key_from_tuple``.

Inputs come from :mod:`shelf.tools.gen_shelf04_golden` (loaded by
file-path since ``shelf/tools/`` isn't a Python package); expected
outputs come from the shared fixture. Drift on any implementation
fails here.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from shelf_replay.key import content_key

REPO = Path(__file__).resolve().parents[3]
GOLDEN_FIXTURE = REPO / "shelfd" / "tests" / "fixtures" / "shelf04_golden_vectors.txt"
GOLDEN_GEN = REPO / "tools" / "gen_shelf04_golden.py"


def _load_golden_inputs():
    spec = importlib.util.spec_from_file_location(
        "shelf_golden_gen", GOLDEN_GEN
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("shelf_golden_gen", mod)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod.GOLDEN_INPUTS


def test_content_key_deterministic():
    # Stable across runs — if this changes we have broken the wire contract.
    assert (
        content_key("etag-x", 0, 0, 16)
        == content_key("etag-x", 0, 0, 16)
    )


def test_rg_ordinal_changes_key():
    a = content_key("etag-x", 0, 0, 16)
    b = content_key("etag-x", 1, 0, 16)
    assert a != b


def test_offset_changes_key():
    a = content_key("etag-x", 0, 0, 16)
    b = content_key("etag-x", 0, 1, 16)
    assert a != b


def test_length_changes_key():
    a = content_key("etag-x", 0, 0, 16)
    b = content_key("etag-x", 0, 0, 32)
    assert a != b


def test_golden_fixture_parity():
    """Every vector in the shared fixture must reproduce here."""

    if not GOLDEN_FIXTURE.exists() or not GOLDEN_GEN.exists():
        pytest.skip(f"golden sources missing: {GOLDEN_FIXTURE} / {GOLDEN_GEN}")

    expected = [
        line.strip()
        for line in GOLDEN_FIXTURE.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    inputs = _load_golden_inputs()
    assert len(inputs) == len(expected), (
        f"fixture length {len(expected)} != inputs length {len(inputs)}"
    )
    for (etag, offset, length, ordinal), want in zip(inputs, expected):
        got = content_key(etag, ordinal, offset, length)
        assert got == want, (
            f"drift on vector etag={etag!r} offset={offset} "
            f"length={length} ordinal={ordinal}\n"
            f"  python: {got}\n  fixture: {want}"
        )
