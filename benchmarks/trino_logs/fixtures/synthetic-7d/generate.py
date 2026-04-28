"""Generate the deterministic synthetic 7-day fixture.

Run::

    python3 fixtures/synthetic-7d/generate.py

Produces (all relative to this directory):

* ``manifests/index.json`` — top-level manifest index.
* ``manifests/<fqn>/<snapshot>.json`` — per-snapshot DataFile lists.
* ``manifests/files/*.parquet`` — real Parquet files with two row groups
  each, engineered so row-group statistics enable predicate pushdown.
* ``trace.jsonl`` — 5 queries across 5 distinct days.
* ``sim-configs.json`` — cache-simulation matrix.

The file layout, row-group stats, and query predicates are picked so
the E5 ratios are hand-derivable for tests — see
``tests/test_golden_e5.py``.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

FIXTURE = Path(__file__).resolve().parent
MANIFEST_ROOT = FIXTURE / "manifests"
FILES_DIR = MANIFEST_ROOT / "files"


def _etag_for(path: Path) -> str:
    h = hashlib.md5()
    h.update(path.read_bytes())
    return h.hexdigest()


def _write_parquet(name: str, table: pa.Table, row_group_size: int) -> Path:
    path = FILES_DIR / name
    pq.write_table(
        table,
        path,
        row_group_size=row_group_size,
        compression="snappy",
        # Emit column-chunk statistics for every column we might
        # predicate on.
        write_statistics=True,
        # Force a small data-page size so multiple row groups land
        # even with modest row counts.
        data_page_size=4096,
    )
    return path


def _build_silver_events() -> list[dict]:
    """Table 1: ``cdp.icesheet.silver_events_2026`` — 3 partitions.

    Partition column: ``event_region``. Each file has two row groups;
    row-group 0 holds ``user_id`` 0..49 and row-group 1 holds
    ``user_id`` 50..99, which lets a ``user_id = 42`` predicate prune
    the second row group of every file.
    """

    entries: list[dict] = []
    for region in ("MP+CG", "UP", "DL"):
        uid = list(range(100))
        table = pa.table(
            {
                "user_id": pa.array(uid, type=pa.int64()),
                "event_name": pa.array(
                    [f"evt-{region}-{i}" for i in uid], type=pa.string()
                ),
            }
        )
        name = f"silver_events__{region.replace('+', '_')}.parquet"
        path = _write_parquet(name, table, row_group_size=50)
        entries.append(
            {
                "path": path.name,
                "file_size_in_bytes": path.stat().st_size,
                "partition": {"event_region": region},
                "record_count": 100,
                "etag": _etag_for(path),
            }
        )
    return entries


def _build_daily_revenue() -> list[dict]:
    """Table 2: ``cdp.gold.daily_revenue`` — unpartitioned.

    Two files. File A has revenue 0..999, file B has revenue 1_000..1_999.
    Each file has two row groups (first 50 rows, last 50). A predicate
    ``revenue > 1000`` keeps **only file B**, both row groups.
    ``revenue < 100`` keeps **only file A, row group 0**.
    """

    entries: list[dict] = []
    for idx, start in enumerate((0, 1000)):
        rev = list(range(start, start + 100))
        table = pa.table(
            {
                "revenue": pa.array(rev, type=pa.int64()),
                "country": pa.array([f"c-{i % 10}" for i in rev], type=pa.string()),
            }
        )
        name = f"daily_revenue__part{idx}.parquet"
        path = _write_parquet(name, table, row_group_size=50)
        entries.append(
            {
                "path": path.name,
                "file_size_in_bytes": path.stat().st_size,
                "partition": {},
                "record_count": 100,
                "etag": _etag_for(path),
            }
        )
    return entries


def _build_page_events() -> list[dict]:
    """Table 3: ``cdp.bronze.page_events`` — partitioned by event_date."""

    entries: list[dict] = []
    for date in ("2026-04-16", "2026-04-17", "2026-04-18", "2026-04-19"):
        ids = list(range(200))
        table = pa.table(
            {
                "event_id": pa.array(ids, type=pa.int64()),
                "page": pa.array([f"p-{i % 20}" for i in ids], type=pa.string()),
            }
        )
        name = f"page_events__{date}.parquet"
        path = _write_parquet(name, table, row_group_size=100)
        entries.append(
            {
                "path": path.name,
                "file_size_in_bytes": path.stat().st_size,
                "partition": {"event_date": date},
                "record_count": 200,
                "etag": _etag_for(path),
            }
        )
    return entries


def _write_index_and_entries(
    table_records: list[tuple[dict, list[dict]]]
) -> None:
    tables_meta: list[dict] = []
    for table_meta, entries in table_records:
        rel = (
            f"{table_meta['catalog']}.{table_meta['schema']}.{table_meta['table']}/"
            f"{table_meta['snapshot_id']}.json"
        )
        out_path = MANIFEST_ROOT / rel
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(entries, fh, indent=2)
        tables_meta.append({**table_meta, "entries_file": rel})
    with (MANIFEST_ROOT / "index.json").open("w", encoding="utf-8") as fh:
        json.dump({"tables": tables_meta}, fh, indent=2)


def _write_trace() -> list[dict]:
    trace = [
        {
            "query_id": "q-01",
            "query_date": "2026-04-16T09:15:00Z",
            "query": (
                "SELECT user_id FROM cdp.icesheet.silver_events_2026 "
                "WHERE event_region='MP+CG' AND user_id = 42"
            ),
            "catalog": "cdp",
            "schema": "icesheet",
            "tables": [
                {
                    "catalog": "cdp",
                    "schema": "icesheet",
                    "table": "silver_events_2026",
                    "snapshot_id": 1001,
                }
            ],
            "wall_time_millis": 820,
            "physical_input_bytes": 12_345,
        },
        {
            "query_id": "q-02",
            "query_date": "2026-04-17T11:40:00Z",
            "query": "SELECT * FROM cdp.gold.daily_revenue WHERE revenue > 1000",
            "catalog": "cdp",
            "schema": "gold",
            "tables": [
                {
                    "catalog": "cdp",
                    "schema": "gold",
                    "table": "daily_revenue",
                    "snapshot_id": 2001,
                }
            ],
            "wall_time_millis": 400,
        },
        {
            "query_id": "q-03",
            "query_date": "2026-04-18T14:10:00Z",
            "query": (
                "SELECT * FROM cdp.bronze.page_events "
                "WHERE event_date = '2026-04-17'"
            ),
            "catalog": "cdp",
            "schema": "bronze",
            "tables": [
                {
                    "catalog": "cdp",
                    "schema": "bronze",
                    "table": "page_events",
                    "snapshot_id": 3001,
                }
            ],
            "wall_time_millis": 300,
        },
        {
            "query_id": "q-04",
            "query_date": "2026-04-20T08:05:00Z",
            "query": "SELECT * FROM cdp.icesheet.silver_events_2026",
            "catalog": "cdp",
            "schema": "icesheet",
            "tables": [
                {
                    "catalog": "cdp",
                    "schema": "icesheet",
                    "table": "silver_events_2026",
                    "snapshot_id": 1001,
                }
            ],
            "wall_time_millis": 1200,
        },
        {
            "query_id": "q-05",
            "query_date": "2026-04-22T16:55:00Z",
            "query": "SELECT * FROM cdp.gold.daily_revenue WHERE revenue < 100",
            "catalog": "cdp",
            "schema": "gold",
            "tables": [
                {
                    "catalog": "cdp",
                    "schema": "gold",
                    "table": "daily_revenue",
                    "snapshot_id": 2001,
                }
            ],
            "wall_time_millis": 380,
        },
        # q-06 exercises SHELF-26a: a JOIN-shaped WHERE where the
        # fact-side terms (``s.event_region`` and ``s.user_id``) must
        # row-group-prune silver_events_2026 exactly as q-01 does,
        # while the dim-side ``r.revenue > 1000`` is ignored at scan
        # time because the trace only binds silver_events_2026.
        {
            "query_id": "q-06",
            "query_date": "2026-04-24T10:00:00Z",
            "query": (
                "SELECT s.user_id FROM cdp.icesheet.silver_events_2026 s "
                "JOIN cdp.gold.daily_revenue r ON s.user_id = r.revenue "
                "WHERE s.event_region = 'MP+CG' AND s.user_id = 42 "
                "AND r.revenue > 1000"
            ),
            "catalog": "cdp",
            "schema": "icesheet",
            "tables": [
                {
                    "catalog": "cdp",
                    "schema": "icesheet",
                    "table": "silver_events_2026",
                    "snapshot_id": 1001,
                }
            ],
            "wall_time_millis": 900,
        },
    ]
    with (FIXTURE / "trace.jsonl").open("w", encoding="utf-8") as fh:
        for row in trace:
            fh.write(json.dumps(row))
            fh.write("\n")
    return trace


def _write_sim_configs() -> None:
    payload = {
        "configs": [
            {
                "name": "baseline-512g",
                "capacity_bytes": 512 * (1 << 30),
                "policy": "lru",
                "size_threshold_bytes": 1 << 30,
                "pinned_bypass": True,
                "pin_list": [],
            },
            {
                "name": "tight-16k",
                "capacity_bytes": 16 * 1024,
                "policy": "lru",
                "size_threshold_bytes": 1 << 30,
                "pinned_bypass": True,
                "pin_list": [],
            },
            {
                "name": "threshold-256b",
                "capacity_bytes": 512 * (1 << 30),
                "policy": "size-only",
                "size_threshold_bytes": 256,
                "pinned_bypass": True,
                "pin_list": [],
            },
        ]
    }
    with (FIXTURE / "sim-configs.json").open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def _write_expected(entries_by_table: dict[str, list[dict]]) -> None:
    """Hand-derived per-day expectations.

    File-level bytes are fixed by the Parquet writer's output size. We
    capture the exact values observed at generation time so tests
    verify against a concrete number. Row-group ratios are captured
    as observed (real Parquet stats) but cross-checked by
    ``tests/test_pipeline.py``'s invariants (ratio <= 1, narrow
    predicates produce ratio < 1).
    """

    # Intentionally empty on generation; populated after first run.
    # See ``tests/test_golden_e5.py::test_regenerate_expected`` for
    # how to refresh. The CLI's ``replay-rep2-7d`` gracefully handles
    # the missing file.
    pass


def main() -> None:
    MANIFEST_ROOT.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)

    silver = _build_silver_events()
    revenue = _build_daily_revenue()
    page_events = _build_page_events()

    _write_index_and_entries(
        [
            (
                {
                    "catalog": "cdp",
                    "schema": "icesheet",
                    "table": "silver_events_2026",
                    "snapshot_id": 1001,
                    "partition_spec": [{"field": "event_region", "transform": "identity"}],
                },
                silver,
            ),
            (
                {
                    "catalog": "cdp",
                    "schema": "gold",
                    "table": "daily_revenue",
                    "snapshot_id": 2001,
                    "partition_spec": [],
                },
                revenue,
            ),
            (
                {
                    "catalog": "cdp",
                    "schema": "bronze",
                    "table": "page_events",
                    "snapshot_id": 3001,
                    "partition_spec": [{"field": "event_date", "transform": "identity"}],
                },
                page_events,
            ),
        ]
    )
    _write_trace()
    _write_sim_configs()
    _write_expected(
        {
            "cdp.icesheet.silver_events_2026": silver,
            "cdp.gold.daily_revenue": revenue,
            "cdp.bronze.page_events": page_events,
        }
    )
    print(f"wrote fixture under {FIXTURE}")


if __name__ == "__main__":
    main()
