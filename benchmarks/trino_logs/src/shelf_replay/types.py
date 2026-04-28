"""Data model shared across trace loading, scanning, and simulation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class PredicateTerm:
    """A single conjunct of a query's WHERE clause.

    We keep this deliberately tiny: (column, op, literal, table_alias).
    ``table_alias`` is ``None`` for bare single-table predicates (back-compat
    with pre-SHELF-26a extraction); for multi-table queries it is the
    lower-cased alias used in the SQL column prefix (e.g. ``f`` for
    ``f.region``). The scanner uses :attr:`TraceEntry.table_aliases` to
    resolve the alias back to a base table name at scan time — see
    ``docs/SHELF-26a-predicate-extraction.md``.
    """

    column: str
    op: str
    value: Any
    table_alias: str | None = None

    def __post_init__(self) -> None:
        if self.op not in _ALLOWED_OPS:
            raise ValueError(f"unsupported predicate op: {self.op!r}")


_ALLOWED_OPS = frozenset({"=", "<", "<=", ">", ">=", "!=", "in"})


@dataclass(frozen=True)
class TableRef:
    catalog: str
    schema: str
    table: str
    snapshot_id: int

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass(frozen=True)
class TraceEntry:
    """One row of ``cdp.trino_logs.trino_queries`` plus extracted metadata."""

    query_id: str
    query_date: datetime
    catalog: str | None
    schema: str | None
    sql: str
    tables: tuple[TableRef, ...]
    predicate: tuple[PredicateTerm, ...] | None
    wall_time_millis: int | None = None
    physical_input_bytes: int | None = None
    # Lowered (alias, base-table-name) pairs harvested from FROM + JOINs.
    # Empty for single-table queries where alias == table. The scanner
    # uses this to decide which PredicateTerms apply to each TableRef.
    table_aliases: tuple[tuple[str, str], ...] = ()

    @property
    def day(self) -> str:
        return self.query_date.strftime("%Y-%m-%d")


@dataclass(frozen=True)
class RowGroup:
    ordinal: int
    compressed_bytes: int
    uncompressed_bytes: int
    row_count: int
    column_stats: dict[str, "ColumnStats"] = field(default_factory=dict)


@dataclass(frozen=True)
class ColumnStats:
    min: Any
    max: Any
    null_count: int
    has_stats: bool = True


@dataclass(frozen=True)
class DataFile:
    path: str
    file_size_in_bytes: int
    partition: dict[str, Any]
    record_count: int
    etag: str


@dataclass(frozen=True)
class ScanResult:
    """Per-query scan accounting.

    ``rg_entries`` is the ordered stream of ``(file_path, rg_ordinal,
    offset, length, etag)`` tuples that the cache simulator replays.
    """

    query_id: str
    day: str
    files_scanned: int
    scanned_bytes_file_level: int
    files_after_partition_prune: int
    scanned_bytes_rg_level: int
    rg_count: int
    rg_pruning_unsupported_columns: int
    rg_entries: tuple[tuple[str, int, int, int, str], ...]

    @property
    def rg_over_file_ratio(self) -> float:
        if self.scanned_bytes_file_level == 0:
            return 0.0
        return self.scanned_bytes_rg_level / self.scanned_bytes_file_level
