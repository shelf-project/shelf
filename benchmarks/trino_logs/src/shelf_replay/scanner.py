"""File-level and row-group-level scan accounting.

This is the core of E5. Given a trace entry and a manifest index, we
compute:

- ``scanned_bytes_file_level`` — sum of ``DataFile.file_size_in_bytes``
  after partition-predicate pruning.
- ``scanned_bytes_rg_level`` — sum of row-group ``total_byte_size``
  after applying the query predicate against row-group column stats.

The ratio ``rg / file`` is the answer to "what fraction of bytes
would Shelf's row-group-granular cache admit if it pruned the way
Trino prunes at scan time?"
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq

from .manifest import ManifestIndex, PartitionField, TableSnapshot
from .types import (
    ColumnStats,
    DataFile,
    PredicateTerm,
    RowGroup,
    ScanResult,
    TableRef,
    TraceEntry,
)


def scan_query(
    entry: TraceEntry,
    manifest_index: ManifestIndex,
) -> ScanResult:
    """Compute file- and row-group-level scanned bytes for one query."""

    files_scanned = 0
    scanned_bytes_file_level = 0
    files_after_partition_prune = 0
    scanned_bytes_rg_level = 0
    rg_count = 0
    rg_unsupported = 0
    rg_entries: list[tuple[str, int, int, int, str]] = []

    for table_ref in entry.tables:
        snapshot = manifest_index.get(table_ref)
        if snapshot is None:
            continue

        for data_file in snapshot.data_files:
            files_scanned += 1
            scanned_bytes_file_level += data_file.file_size_in_bytes

            if not _partition_keep(data_file, snapshot, entry.predicate):
                continue
            files_after_partition_prune += 1

            row_groups = _load_row_groups(manifest_index, data_file)
            if not row_groups:
                # Fall through to file granularity conservatively.
                scanned_bytes_rg_level += data_file.file_size_in_bytes
                continue

            current_offset = 0
            for rg in row_groups:
                rg_count += 1
                keep, unsupported = _rg_keep(rg, entry.predicate, snapshot)
                if unsupported:
                    rg_unsupported += unsupported
                if keep:
                    scanned_bytes_rg_level += rg.compressed_bytes
                    rg_entries.append(
                        (
                            data_file.path,
                            rg.ordinal,
                            current_offset,
                            rg.compressed_bytes,
                            data_file.etag,
                        )
                    )
                current_offset += rg.compressed_bytes

    return ScanResult(
        query_id=entry.query_id,
        day=entry.day,
        files_scanned=files_scanned,
        scanned_bytes_file_level=scanned_bytes_file_level,
        files_after_partition_prune=files_after_partition_prune,
        scanned_bytes_rg_level=scanned_bytes_rg_level,
        rg_count=rg_count,
        rg_pruning_unsupported_columns=rg_unsupported,
        rg_entries=tuple(rg_entries),
    )


def _partition_keep(
    data_file: DataFile,
    snapshot: TableSnapshot,
    predicate: tuple[PredicateTerm, ...] | None,
) -> bool:
    """True when this DataFile survives partition-level pruning."""

    if predicate is None or not predicate:
        return True
    partition_cols = {p.name.lower() for p in snapshot.partition_spec}
    for term in predicate:
        if term.column.lower() not in partition_cols:
            continue
        part_value = data_file.partition.get(term.column) or data_file.partition.get(
            term.column.lower()
        )
        if part_value is None:
            # Identity partition with a null value is not prunable.
            continue
        if not _compare(part_value, term.op, term.value):
            return False
    return True


def _rg_keep(
    rg: RowGroup,
    predicate: tuple[PredicateTerm, ...] | None,
    snapshot: TableSnapshot,
) -> tuple[bool, int]:
    """Return ``(keep, unsupported_column_count)`` for this row group.

    A row group is pruned when ANY conjunct demonstrably excludes all
    rows (``max < lower`` or ``min > upper``). Missing stats or
    unsupported ops count toward ``unsupported`` and never prune.
    """

    if predicate is None:
        return (True, 0)

    partition_cols = {p.name.lower() for p in snapshot.partition_spec}
    unsupported = 0
    for term in predicate:
        col = term.column.lower()
        if col in partition_cols:
            # Already enforced at partition layer.
            continue
        stats = rg.column_stats.get(col)
        if stats is None or not stats.has_stats:
            unsupported += 1
            continue
        if not _range_keep(stats, term):
            return (False, unsupported)
    return (True, unsupported)


def _range_keep(stats: ColumnStats, term: PredicateTerm) -> bool:
    mn, mx = stats.min, stats.max
    if mn is None or mx is None:
        return True
    op, v = term.op, term.value
    try:
        if op == "=":
            return mn <= v <= mx
        if op == "!=":
            return True
        if op == "<":
            return mn < v
        if op == "<=":
            return mn <= v
        if op == ">":
            return mx > v
        if op == ">=":
            return mx >= v
        if op == "in":
            return any(mn <= x <= mx for x in v)
    except TypeError:
        return True
    return True


def _compare(lhs, op: str, rhs) -> bool:
    try:
        if op == "=":
            return lhs == rhs
        if op == "!=":
            return lhs != rhs
        if op == "<":
            return lhs < rhs
        if op == "<=":
            return lhs <= rhs
        if op == ">":
            return lhs > rhs
        if op == ">=":
            return lhs >= rhs
        if op == "in":
            return lhs in rhs
    except TypeError:
        return True
    return True


def _load_row_groups(
    manifest_index: ManifestIndex, data_file: DataFile
) -> tuple[RowGroup, ...]:
    local_path = manifest_index.resolve_file(data_file)
    return _row_groups_for(str(local_path))


@lru_cache(maxsize=4096)
def _row_groups_for(path: str) -> tuple[RowGroup, ...]:
    """Read row-group metadata via pyarrow. Cached per file for sweep speed."""

    try:
        pf = pq.ParquetFile(path)
    except FileNotFoundError:
        return tuple()
    except Exception:
        return tuple()
    meta = pf.metadata
    if meta is None:
        return tuple()
    result: list[RowGroup] = []
    for rg_ord in range(meta.num_row_groups):
        rg_meta = meta.row_group(rg_ord)
        col_stats: dict[str, ColumnStats] = {}
        for col_ord in range(rg_meta.num_columns):
            col_meta = rg_meta.column(col_ord)
            col_name = str(col_meta.path_in_schema).lower()
            stats = col_meta.statistics
            if stats is None or not stats.has_min_max:
                col_stats[col_name] = ColumnStats(
                    min=None, max=None, null_count=0, has_stats=False
                )
            else:
                col_stats[col_name] = ColumnStats(
                    min=stats.min,
                    max=stats.max,
                    null_count=stats.null_count or 0,
                    has_stats=True,
                )
        compressed = sum(
            int(rg_meta.column(i).total_compressed_size)
            for i in range(rg_meta.num_columns)
        )
        result.append(
            RowGroup(
                ordinal=rg_ord,
                compressed_bytes=compressed,
                uncompressed_bytes=int(rg_meta.total_byte_size),
                row_count=int(rg_meta.num_rows),
                column_stats=col_stats,
            )
        )
    return tuple(result)


def clear_footer_cache() -> None:
    """Reset the Parquet footer LRU — useful in tests."""

    _row_groups_for.cache_clear()


def scan_all(
    trace: Iterable[TraceEntry], manifest_index: ManifestIndex
) -> list[ScanResult]:
    return [scan_query(e, manifest_index) for e in trace]
