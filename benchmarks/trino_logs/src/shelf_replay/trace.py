"""Trace loader for ``cdp.trino_logs.trino_queries`` dumps.

Accepts JSONL (one JSON object per line) as the canonical format. CSV
is converted by upstream tooling — the `trino_queries` schema includes
nested columns (`plan`) that don't round-trip cleanly through CSV, so
JSONL is the source of truth.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

import sqlglot
from sqlglot import exp

from .types import PredicateTerm, TableRef, TraceEntry


def load_trace(path: str | Path) -> list[TraceEntry]:
    """Load a JSONL trace file into :class:`TraceEntry` records."""

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"trace not found: {p}")
    entries: list[TraceEntry] = []
    with p.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as e:
                raise ValueError(f"{p}:{line_no}: invalid JSON: {e}") from e
            entries.append(_parse_entry(obj, line_no, p))
    return entries


def _parse_entry(obj: dict, line_no: int, path: Path) -> TraceEntry:
    try:
        query_id = obj["query_id"]
        sql = obj["query"]
    except KeyError as e:
        raise ValueError(f"{path}:{line_no}: missing required key {e}") from e

    query_date = _parse_timestamp(obj.get("query_date"))

    # ``tables`` in the trace is expected to be a list of dicts; it is
    # the canonical binding between the query and its Iceberg snapshot.
    # We do not attempt to re-derive it from the SQL — the trace
    # exporter fills it from ``cdp.trino_logs.trino_queries``.
    tables = tuple(_parse_table_refs(obj.get("tables", [])))
    predicate = _extract_predicate(sql)

    return TraceEntry(
        query_id=query_id,
        query_date=query_date,
        catalog=obj.get("catalog"),
        schema=obj.get("schema"),
        sql=sql,
        tables=tables,
        predicate=predicate,
        wall_time_millis=_int_or_none(obj.get("wall_time_millis")),
        physical_input_bytes=_int_or_none(obj.get("physical_input_bytes")),
    )


def _parse_timestamp(raw: object) -> datetime:
    if raw is None:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)
    if isinstance(raw, str):
        # Accept both ISO-8601 Z form and Trino's space-separated form.
        s = raw.replace("Z", "+00:00").replace(" ", "T", 1)
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
    raise TypeError(f"unsupported query_date type: {type(raw).__name__}")


def _parse_table_refs(raw: Iterable) -> Iterator[TableRef]:
    for t in raw:
        if not isinstance(t, dict):
            continue
        try:
            yield TableRef(
                catalog=t["catalog"],
                schema=t["schema"],
                table=t["table"],
                snapshot_id=int(t["snapshot_id"]),
            )
        except (KeyError, TypeError, ValueError):
            continue


def _extract_predicate(sql: str) -> tuple[PredicateTerm, ...] | None:
    """Best-effort WHERE-clause extraction via ``sqlglot``.

    Returns ``None`` when the predicate cannot be flattened to a pure
    conjunction of ``column OP literal`` terms. Callers treat ``None``
    as "prune at file granularity only, do not row-group-prune".
    """

    try:
        parsed = sqlglot.parse_one(sql, dialect="trino")
    except sqlglot.errors.ParseError:
        return None
    if not isinstance(parsed, exp.Select):
        return None

    where = parsed.args.get("where")
    if where is None:
        return tuple()
    conjuncts = _flatten_and(where.this)
    terms: list[PredicateTerm] = []
    for c in conjuncts:
        term = _term_from(c)
        if term is None:
            return None  # one join / subquery / OR poisons the lot
        terms.append(term)
    return tuple(terms)


def _flatten_and(node: exp.Expression) -> list[exp.Expression]:
    if isinstance(node, exp.And):
        return _flatten_and(node.left) + _flatten_and(node.right)
    return [node]


_OP_MAP = {
    exp.EQ: "=",
    exp.NEQ: "!=",
    exp.LT: "<",
    exp.LTE: "<=",
    exp.GT: ">",
    exp.GTE: ">=",
}


def _term_from(node: exp.Expression) -> PredicateTerm | None:
    cls = type(node)
    op = _OP_MAP.get(cls)
    if op is not None:
        col = _col_name(node.this)
        val = _literal_value(node.expression)
        if col is None or val is _UNBOUND:
            return None
        return PredicateTerm(column=col, op=op, value=val)
    if isinstance(node, exp.In):
        col = _col_name(node.this)
        vals: list = []
        for expr in node.expressions:
            v = _literal_value(expr)
            if v is _UNBOUND:
                return None
            vals.append(v)
        if col is None or not vals:
            return None
        return PredicateTerm(column=col, op="in", value=tuple(vals))
    return None


_UNBOUND = object()


def _col_name(node: exp.Expression | None) -> str | None:
    if isinstance(node, exp.Column):
        return node.name.lower() if node.name else None
    return None


def _literal_value(node: exp.Expression | None):
    if node is None:
        return _UNBOUND
    if isinstance(node, exp.Literal):
        if node.is_int:
            return int(node.this)
        if node.is_number:
            return float(node.this)
        return str(node.this)
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Null):
        return None
    return _UNBOUND


def _int_or_none(v) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
