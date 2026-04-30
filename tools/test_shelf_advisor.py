"""Unit tests for ``shelf_advisor.py`` (H1)."""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import tempfile

import pytest

from shelf_advisor import (
    MIN_RUNS_PER_DAY,
    MIN_STABLE_DAYS,
    QueryRow,
    main,
    recommend,
)


def _rows(fp: str, n_per_day: int, days: int, *,
          bytes_raw: int = 100_000_000, bytes_mv: int = 1_000_000,
          tables: tuple[str, ...] = ("iceberg.a.events",)) -> list[QueryRow]:
    start = dt.date(2026, 4, 1)
    out: list[QueryRow] = []
    for d in range(days):
        day = start + dt.timedelta(days=d)
        for _ in range(n_per_day):
            out.append(QueryRow(
                fingerprint=fp,
                canonical_plan="{sorted-plan}",
                observed_at=day,
                bytes_scanned_raw=bytes_raw,
                bytes_scanned_mv_est=bytes_mv,
                elapsed_ms=1000,
                tables=tables,
            ))
    return out


def test_recommends_big_win() -> None:
    rows = _rows("fp-win", n_per_day=20, days=14)
    recs = recommend(rows)
    assert len(recs) == 1
    r = recs[0]
    assert r.fingerprint == "fp-win"
    assert r.runs_per_day == pytest.approx(20.0)
    assert r.bytes_saved_per_day > 0
    assert r.tables == ("iceberg.a.events",)


def test_skips_when_no_savings() -> None:
    rows = _rows("fp-nope", n_per_day=50, days=14,
                 bytes_raw=1_000_000, bytes_mv=1_000_000)
    recs = recommend(rows)
    assert recs == []


def test_skips_when_runs_too_few() -> None:
    # Under MIN_RUNS_PER_DAY: advisor must stay silent.
    rows = _rows("fp-thin", n_per_day=MIN_RUNS_PER_DAY - 1, days=14)
    assert recommend(rows) == []


def test_skips_when_unstable() -> None:
    # Less than MIN_STABLE_DAYS of history: not a candidate yet.
    rows = _rows("fp-young", n_per_day=20, days=MIN_STABLE_DAYS - 1)
    assert recommend(rows) == []


def test_main_emits_json(tmp_path: pathlib.Path) -> None:
    rows = _rows("fp-win", n_per_day=20, days=14)
    input_path = tmp_path / "queries.jsonl"
    with input_path.open("w") as f:
        for r in rows:
            f.write(json.dumps({
                "fingerprint": r.fingerprint,
                "canonical_plan": r.canonical_plan,
                "observed_at": r.observed_at.isoformat(),
                "bytes_scanned_raw": r.bytes_scanned_raw,
                "bytes_scanned_mv_est": r.bytes_scanned_mv_est,
                "elapsed_ms": r.elapsed_ms,
                "tables": list(r.tables),
            }) + "\n")
    out = tmp_path / "out.json"
    exit_code = main(["--input", str(input_path), "--out", str(out)])
    assert exit_code == 0
    payload = json.loads(out.read_text())
    assert payload["recommendations"]
    assert payload["recommendations"][0]["fingerprint"] == "fp-win"
