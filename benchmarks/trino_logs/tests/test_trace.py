"""Trace loader + predicate-extraction tests."""

from __future__ import annotations

import json

import pytest

from shelf_replay.trace import load_trace
from shelf_replay.types import PredicateTerm


def test_load_fixture_trace(fixture_dir):
    entries = load_trace(fixture_dir / "trace.jsonl")
    # q-01..q-05 plus the SHELF-26a join-shape q-06.
    assert len(entries) == 6
    assert entries[0].query_id == "q-01"
    assert entries[0].day == "2026-04-16"
    assert entries[0].tables[0].fqn == "warehouse.your_schema.your_events_table"


def test_predicate_simple_eq(tmp_path):
    trace = tmp_path / "t.jsonl"
    trace.write_text(
        json.dumps(
            {
                "query_id": "q",
                "query_date": "2026-04-16T00:00:00Z",
                "query": "SELECT x FROM t WHERE event_region = 'MP+CG'",
                "tables": [],
            }
        )
        + "\n"
    )
    entries = load_trace(trace)
    assert entries[0].predicate == (
        PredicateTerm(column="event_region", op="=", value="MP+CG"),
    )


def test_predicate_and_combinator(tmp_path):
    trace = tmp_path / "t.jsonl"
    trace.write_text(
        json.dumps(
            {
                "query_id": "q",
                "query_date": "2026-04-16T00:00:00Z",
                "query": "SELECT x FROM t WHERE a = 1 AND b < 10 AND c IN (2, 4)",
                "tables": [],
            }
        )
        + "\n"
    )
    entries = load_trace(trace)
    pred = entries[0].predicate
    assert pred is not None
    assert set(p.op for p in pred) == {"=", "<", "in"}
    in_term = next(p for p in pred if p.op == "in")
    assert in_term.value == (2, 4)


def test_predicate_unsupported_returns_none(tmp_path):
    # A top-level OR across *different* columns still poisons the whole
    # predicate — callers must fall through to file granularity.
    trace = tmp_path / "t.jsonl"
    trace.write_text(
        json.dumps(
            {
                "query_id": "q",
                "query_date": "2026-04-16T00:00:00Z",
                "query": "SELECT x FROM t WHERE a = 1 OR b = 2",
                "tables": [],
            }
        )
        + "\n"
    )
    entries = load_trace(trace)
    assert entries[0].predicate is None


# -----------------------------------------------------------------------
# SHELF-26a: join / subquery / OR shapes — extracted conservatively
# instead of poisoning the whole predicate. See
# ``docs/SHELF-26a-predicate-extraction.md`` for the contract.
# -----------------------------------------------------------------------


def _load_one(tmp_path, sql):
    trace = tmp_path / "t.jsonl"
    trace.write_text(
        json.dumps(
            {
                "query_id": "q",
                "query_date": "2026-04-16T00:00:00Z",
                "query": sql,
                "tables": [],
            }
        )
        + "\n"
    )
    return load_trace(trace)[0]


def test_extracts_predicate_from_join_with_fact_table_alias(tmp_path):
    """Two-table JOIN with alias-qualified WHERE terms on both sides.

    Pre-SHELF-26a this poisoned the entire predicate (join ⇒ ``None``).
    Post-SHELF-26a we attach ``table_alias`` to each term so the scanner
    can filter them per :class:`TableRef`.
    """

    entry = _load_one(
        tmp_path,
        "SELECT f.x "
        "FROM fact f JOIN dim d ON f.dim_id = d.id "
        "WHERE f.event_date = '2026-04-17' AND d.name = 'X'",
    )
    pred = entry.predicate
    assert pred is not None
    by_alias = {(t.column, t.table_alias): t for t in pred}
    assert by_alias[("event_date", "f")].value == "2026-04-17"
    assert by_alias[("name", "d")].value == "X"
    # Alias map is recorded so the scanner can resolve f→fact, d→dim.
    alias_map = dict(entry.table_aliases)
    assert alias_map["f"] == "fact"
    assert alias_map["d"] == "dim"


def test_scalar_subquery_falls_through_only_for_affected_term(tmp_path):
    """``a = 1 AND b = (SELECT max(c) FROM t)`` keeps only the ``a = 1`` term."""

    entry = _load_one(
        tmp_path,
        "SELECT x FROM t WHERE a = 1 AND b = (SELECT max(c) FROM audit)",
    )
    assert entry.predicate == (
        PredicateTerm(column="a", op="=", value=1, table_alias=None),
    )


def test_in_subquery_falls_through_only_for_affected_term(tmp_path):
    """``region IN (SELECT ... )`` is dropped; the date conjunct survives."""

    entry = _load_one(
        tmp_path,
        "SELECT x FROM t "
        "WHERE region IN (SELECT region FROM r) "
        "AND event_date = DATE '2026-04-17'",
    )
    pred = entry.predicate
    assert pred is not None
    assert len(pred) == 1
    assert pred[0].column == "event_date"
    assert pred[0].op == "="
    assert pred[0].value == "2026-04-17"


def test_or_over_same_column_collapses_to_in(tmp_path):
    """``region = 'MP+CG' OR region = 'UP'`` → one IN term."""

    entry = _load_one(
        tmp_path,
        "SELECT x FROM t WHERE region = 'MP+CG' OR region = 'UP'",
    )
    assert entry.predicate == (
        PredicateTerm(
            column="region", op="in", value=("MP+CG", "UP"), table_alias=None
        ),
    )


def test_or_across_columns_returns_none(tmp_path):
    """``a = 1 OR b = 2`` cannot be collapsed — predicate falls through."""

    entry = _load_one(tmp_path, "SELECT x FROM t WHERE a = 1 OR b = 2")
    assert entry.predicate is None


def test_cte_predicate_extracted_from_outer_select(tmp_path):
    """CTE: extract the outermost ``SELECT``'s WHERE and ignore the CTE body."""

    entry = _load_one(
        tmp_path,
        "WITH x AS (SELECT * FROM t) "
        "SELECT * FROM x WHERE event_date = DATE '2026-04-17'",
    )
    pred = entry.predicate
    assert pred is not None
    assert len(pred) == 1
    assert pred[0].column == "event_date"
    assert pred[0].value == "2026-04-17"


def test_unbound_column_returns_none(tmp_path):
    """Prefixed column whose alias is not in the FROM list ⇒ ``None``."""

    entry = _load_one(
        tmp_path,
        "SELECT x FROM a JOIN b ON a.id = b.id WHERE unknown.col = 1",
    )
    assert entry.predicate is None


def test_column_alias_resolution_is_case_insensitive(tmp_path):
    """``FROM T AS F WHERE f.region = 'MP'`` lowers the alias to ``'f'``."""

    entry = _load_one(
        tmp_path, "SELECT x FROM T AS F WHERE f.region = 'MP'"
    )
    assert entry.predicate == (
        PredicateTerm(
            column="region", op="=", value="MP", table_alias="f"
        ),
    )
    # The alias map carries the resolution ``f -> t`` (both lowered).
    assert dict(entry.table_aliases)["f"] == "t"


def test_trace_missing_required_key(tmp_path):
    trace = tmp_path / "bad.jsonl"
    trace.write_text(json.dumps({"query_id": "q"}) + "\n")
    with pytest.raises(ValueError, match="missing required key"):
        load_trace(trace)
