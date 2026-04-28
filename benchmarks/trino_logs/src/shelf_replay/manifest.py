"""Manifest index reader.

The on-disk format is a JSON directory:

    manifests/
      index.json                      # top-level table-snapshot index
      <catalog>.<schema>.<table>/
        <snapshot_id>.json            # list of DataFile entries

``index.json``::

    {
      "tables": [
        {
          "catalog": "cdp",
          "schema": "icesheet",
          "table": "silver_offline_event_data_2026",
          "snapshot_id": 1234,
          "partition_spec": [{"field": "event_region", "transform": "identity"}],
          "entries_file": "cdp.icesheet.silver_offline_event_data_2026/1234.json"
        }
      ]
    }

Each ``entries_file`` is a JSON list of :class:`DataFile`-compatible
records — ``path``, ``file_size_in_bytes``, ``partition``,
``record_count``, ``etag``.

This format is intentionally decoupled from Iceberg's Avro manifests so
the harness is trivially testable and so the `scripts/export-manifests.py`
bridge can be implemented / evolved without touching the analyzer.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .types import DataFile, TableRef


@dataclass(frozen=True)
class PartitionField:
    name: str
    transform: str  # identity | year | month | day | hour | bucket | truncate


@dataclass(frozen=True)
class TableSnapshot:
    ref: TableRef
    partition_spec: tuple[PartitionField, ...]
    data_files: tuple[DataFile, ...]


class ManifestIndex:
    """Lookup-by-``TableRef`` over an on-disk manifest directory."""

    def __init__(self, root: Path, snapshots: dict[TableRef, TableSnapshot]):
        self._root = root
        self._snapshots = snapshots

    @classmethod
    def load(cls, root: str | Path) -> "ManifestIndex":
        root_path = Path(root)
        index_path = root_path / "index.json"
        if not index_path.exists():
            raise FileNotFoundError(f"manifest index not found: {index_path}")
        with index_path.open("r", encoding="utf-8") as fh:
            top = json.load(fh)

        snapshots: dict[TableRef, TableSnapshot] = {}
        for t in top.get("tables", []):
            ref = TableRef(
                catalog=t["catalog"],
                schema=t["schema"],
                table=t["table"],
                snapshot_id=int(t["snapshot_id"]),
            )
            spec = tuple(
                PartitionField(name=p["field"], transform=p.get("transform", "identity"))
                for p in t.get("partition_spec", [])
            )
            entries_path = root_path / t["entries_file"]
            with entries_path.open("r", encoding="utf-8") as fh:
                raw_entries = json.load(fh)
            data_files = tuple(
                DataFile(
                    path=e["path"],
                    file_size_in_bytes=int(e["file_size_in_bytes"]),
                    partition=dict(e.get("partition", {})),
                    record_count=int(e.get("record_count", 0)),
                    etag=str(e["etag"]),
                )
                for e in raw_entries
            )
            snapshots[ref] = TableSnapshot(
                ref=ref, partition_spec=spec, data_files=data_files
            )
        return cls(root_path, snapshots)

    @property
    def root(self) -> Path:
        return self._root

    def get(self, ref: TableRef) -> TableSnapshot | None:
        return self._snapshots.get(ref)

    def tables(self) -> list[TableSnapshot]:
        return list(self._snapshots.values())

    def resolve_file(self, data_file: DataFile) -> Path:
        """Map a :class:`DataFile`'s logical path to a local filesystem path.

        Synthetic fixtures ship real Parquet files under
        ``manifests/files/<filename>``; the ``path`` field of each
        :class:`DataFile` is a plain filename and resolves relative to
        ``<root>/files/``. Real rep-2 exports set ``path`` to the full
        S3 URI; those are resolved by a side-car
        ``files.index.json`` that the export script writes.
        """

        # Real rep-2 paths (s3:// or http(s)://) require the side-car index.
        if "://" in data_file.path:
            return self._resolve_via_sidecar(data_file)
        return self._root / "files" / data_file.path

    def _resolve_via_sidecar(self, data_file: DataFile) -> Path:
        sidecar = self._root / "files.index.json"
        if not sidecar.exists():
            raise FileNotFoundError(
                f"remote DataFile path {data_file.path!r} requires "
                f"{sidecar} produced by scripts/export-manifests.py"
            )
        with sidecar.open("r", encoding="utf-8") as fh:
            mapping = json.load(fh)
        local = mapping.get(data_file.path)
        if local is None:
            raise KeyError(
                f"no local mapping for DataFile path {data_file.path!r}"
            )
        return self._root / local
