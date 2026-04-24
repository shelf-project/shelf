"""Trace loader for ``cdp.trino_logs.trino_queries`` dumps.

Accepts JSONL (one JSON object per line) as the canonical format. CSV
is converted by upstream tooling — the `trino_queries` schema includes
nested columns (`plan`) that don't round-trip cleanly through CSV, so
JSONL is the source of truth.

Predicate extraction (the ``WHERE`` → :class:`PredicateTerm` step) lives
in :mod:`.predicates`; this module only owns I/O + :class:`TraceEntry`
construction.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from .predicates import extract_predicate
from .types import TableRef, TraceEntry


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
    predicate, alias_map = extract_predicate(sql)

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
        table_aliases=tuple(sorted(alias_map.items())),
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


def _int_or_none(v) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
