"""Pin-list generator (stub).

Emits ``pin_list.json`` for shelfd / Trino-plugin hot-reload. Per Agent 6
Pass 5 and ADR-0003, v1 admission is size-threshold + pin-list; the pin
list is the *only* training artifact that ships in v1.

Algorithm (v1 default):

1. Query ``your_query_log_table`` for three windows:
   * last 7 days
   * last 30 days
   * last 90 days
2. For each window, compute per-table access frequency
   (``scanned_bytes × wall_time × frequency``), rank, and take top-N.
3. Emit the **intersection** across all three windows. A table missing
   from any window is excluded — we want persistent hot tables, not
   this-week spikes.
4. Exclude tables whose owner is in ``settings.pin_list_exclude_users``
   (``airflow_user``, ``dbt_user`` by default): ETL writers churn files
   constantly and pinning them wastes NVMe.
5. Emit JSON schema ``{table, partitions?, reason, score}`` per
   BLUEPRINT §6.3 / Agent 6 Pass 5.

Output path: ``s3://<config-bucket>/<config-prefix>pin_list.json``.
Consumer: shelfd ``SHELF-24`` (pin-list loader + SIGHUP reload).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl


@dataclass(frozen=True, slots=True)
class PinEntry:
    """Schema for a single row in pin_list.json.

    ``partitions`` is optional: if omitted, the entire table is pinned.
    If present, each string is a single partition predicate (e.g.
    ``"event_region='MP+CG'"``) — shelfd pins only those partitions.
    """

    table: str
    partitions: tuple[str, ...] | None
    reason: str
    score: float


def generate_pin_list(
    query_log: pl.DataFrame,
    *,
    as_of: datetime,
    top_n: int,
    exclude_users: frozenset[str],
) -> list[PinEntry]:
    """Compute the intersection-ranked pin list.

    Not implemented — see SHELF-37.
    """
    del query_log, as_of, top_n, exclude_users
    raise NotImplementedError(
        "SHELF-37: pin-list generator (7/30/90d intersection, ETL exclusion)."
    )


def write_pin_list(entries: list[PinEntry], *, dest: Path) -> None:
    """Serialise ``entries`` as ``pin_list.json`` to ``dest``.

    JSON shape::

        {
          "pins": [
            {"table": "...", "partitions": [...], "reason": "...", "score": 12.3},
            ...
          ],
          "generated_at": "2026-04-23T00:00:00Z",
          "schema_version": 1
        }
    """
    del entries, dest
    raise NotImplementedError("SHELF-38: write pin_list.json (S3 + local).")


def to_dict(entry: PinEntry) -> dict[str, Any]:
    """Helper: stable JSON projection of a :class:`PinEntry`."""
    del entry
    raise NotImplementedError("SHELF-39: PinEntry -> JSON projection.")
