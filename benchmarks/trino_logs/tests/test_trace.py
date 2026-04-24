"""Trace loader + predicate-extraction tests."""

from __future__ import annotations

import json

import pytest

from shelf_replay.trace import load_trace
from shelf_replay.types import PredicateTerm


def test_load_fixture_trace(fixture_dir):
    entries = load_trace(fixture_dir / "trace.jsonl")
    assert len(entries) == 5
    assert entries[0].query_id == "q-01"
    assert entries[0].day == "2026-04-16"
    assert entries[0].tables[0].fqn == "cdp.icesheet.silver_events_2026"


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
    # A subquery or OR conjunct poisons the whole predicate — callers
    # must fall through to file granularity.
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


def test_trace_missing_required_key(tmp_path):
    trace = tmp_path / "bad.jsonl"
    trace.write_text(json.dumps({"query_id": "q"}) + "\n")
    with pytest.raises(ValueError, match="missing required key"):
        load_trace(trace)
