"""Unit tests for the correctness-diff runner — no live Trino required."""

from __future__ import annotations

import pathlib
from typing import Any, Iterator

import pytest

from correctness_diff.runner import Runner, _canonical_hash, _render


@pytest.fixture
def tmp_query_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    d = tmp_path / "queries"
    d.mkdir()
    (d / "01-simple.sql.tmpl").write_text(
        "SELECT * FROM {catalog}.{schema}.{table} WHERE x = {x}\n",
        encoding="utf-8",
    )
    (d / "02-agg.sql.tmpl").write_text(
        "SELECT count(*) FROM {catalog}.{schema}.{table}\n",
        encoding="utf-8",
    )
    return d


def test_canonical_hash_is_order_independent() -> None:
    rows_1 = [(1, "a"), (2, "b"), (3, "c")]
    rows_2 = [(3, "c"), (1, "a"), (2, "b")]
    h1, c1, _ = _canonical_hash(iter(rows_1), preview_rows=0)
    h2, c2, _ = _canonical_hash(iter(rows_2), preview_rows=0)
    assert h1 == h2
    assert c1 == c2 == 3


def test_canonical_hash_detects_row_change() -> None:
    rows_1 = [(1, "a"), (2, "b")]
    rows_2 = [(1, "a"), (2, "B")]  # one byte different
    h1, _, _ = _canonical_hash(iter(rows_1), preview_rows=0)
    h2, _, _ = _canonical_hash(iter(rows_2), preview_rows=0)
    assert h1 != h2


def test_canonical_hash_detects_missing_row() -> None:
    rows_1 = [(1, "a"), (2, "b"), (3, "c")]
    rows_2 = [(1, "a"), (2, "b")]
    h1, c1, _ = _canonical_hash(iter(rows_1), preview_rows=0)
    h2, c2, _ = _canonical_hash(iter(rows_2), preview_rows=0)
    assert h1 != h2
    assert c1 == 3 and c2 == 2


def test_render_substitutes_catalog_and_bindings(tmp_query_dir: pathlib.Path) -> None:
    out = _render(
        tmp_query_dir / "01-simple.sql.tmpl",
        {"schema": "default", "table": "events", "x": 42},
        catalog="iceberg_direct",
    )
    assert "iceberg_direct.default.events" in out
    assert "x = 42" in out


def test_render_raises_on_missing_placeholder(tmp_query_dir: pathlib.Path) -> None:
    with pytest.raises(KeyError):
        _render(
            tmp_query_dir / "01-simple.sql.tmpl",
            {"schema": "default", "table": "events"},  # missing x
            catalog="c",
        )


class _FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = list(rows)
        self._pos = 0

    def execute(self, sql: str) -> None:
        # Ignore session SET statements; otherwise the cursor just
        # stands ready to stream the pre-seeded rows.
        self._last_sql = sql

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows = self._rows[self._pos:]
        self._pos = len(self._rows)
        return rows

    def fetchmany(self, n: int) -> list[tuple[Any, ...]]:
        chunk = self._rows[self._pos : self._pos + n]
        self._pos += len(chunk)
        return chunk


class _FakeConn:
    def __init__(self, rows_by_sql: dict[str, list[tuple[Any, ...]]]) -> None:
        self._rows_by_sql = rows_by_sql
        self._cursor: _FakeCursor | None = None

    def cursor(self) -> _FakeCursor:
        # Seed a fresh cursor each call. The runner issues a SET
        # SESSION then the real query; we re-seed on the SELECT.
        self._cursor = _FakeCursor([])
        return self._cursor

    def close(self) -> None:  # noqa: D401 — protocol
        pass


def _conn_factory(rows_by_catalog: dict[str, list[tuple[Any, ...]]]):
    # Each Runner._execute call opens one connection per side; the
    # factory inspects the SQL via its cursor and returns the right
    # rows for that catalog.
    def factory(trino_cfg):
        # The config is the same for both sides, so differentiation
        # must come from the cursor.execute sql.
        class _SelectiveCursor(_FakeCursor):
            def execute(self, sql: str) -> None:
                super().execute(sql)
                if sql.strip().upper().startswith("SELECT"):
                    catalog = sql.split(".")[0].split()[-1]
                    self._rows = list(rows_by_catalog.get(catalog, []))
                    self._pos = 0

        class _Conn(_FakeConn):
            def cursor(self) -> _FakeCursor:
                self._cursor = _SelectiveCursor([])
                return self._cursor

        return _Conn({})

    return factory


def test_runner_reports_match_when_sides_agree(tmp_query_dir: pathlib.Path) -> None:
    rows = [(1, "a"), (2, "b")]
    config = {
        "trino": {"host": "localhost", "port": 8080},
        "catalog_a": "iceberg_direct",
        "catalog_b": "iceberg",
        "schema": "default",
        "bindings": {"table": "events", "x": 1},
        "execution": {"fetch_chunk_rows": 100},
        "replica": "rep-0",
    }
    runner = Runner(
        config,
        tmp_query_dir,
        connection_factory=_conn_factory({"iceberg_direct": rows, "iceberg": rows}),
    )
    report = runner.run()
    assert report.all_match
    assert len(report.queries) == 2
    assert all(q.match for q in report.queries)


def test_runner_reports_mismatch_when_sides_differ(tmp_query_dir: pathlib.Path) -> None:
    rows_a = [(1, "a"), (2, "b")]
    rows_b = [(1, "a"), (2, "B")]
    config = {
        "trino": {"host": "localhost", "port": 8080},
        "catalog_a": "iceberg_direct",
        "catalog_b": "iceberg",
        "schema": "default",
        "bindings": {"table": "events", "x": 1},
        "execution": {"fetch_chunk_rows": 100},
        "replica": "rep-2",
    }
    runner = Runner(
        config,
        tmp_query_dir,
        connection_factory=_conn_factory({"iceberg_direct": rows_a, "iceberg": rows_b}),
    )
    report = runner.run()
    assert not report.all_match
    diverged = [q for q in report.queries if not q.match]
    assert diverged, "expected at least one diverged query"
    assert diverged[0].hash_a != diverged[0].hash_b
