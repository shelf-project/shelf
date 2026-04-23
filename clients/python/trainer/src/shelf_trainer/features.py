"""Feature extraction for the admission model (stub).

The v1.x LightGBM feature set is fixed by BLUEPRINT §7.3 and matches the
ONNX MLP originally proposed there; changing it without updating
``contracts/admission-model.md`` is a bug.

Feature order (index matters — it maps 1:1 to the LightGBM booster's
feature index and to shelfd's Rust ``lightgbm3`` caller):

0. ``table_tf_7d``            — table access frequency over the last 7 days.
1. ``table_tu_7d``            — distinct users touching the table in 7 days.
2. ``partition_depth``        — depth of the partition spec (0 = unpartitioned).
3. ``user_type``              — categorical: 0=dashboard, 1=adhoc, 2=etl.
4. ``size_mb``                — size of the candidate object (MB).
5. ``hour_of_day``            — 0-23 at decision time (UTC).
6. ``recency_days``           — days since the object's partition was last read.
7. ``query_cost_rank``        — per-tenant rank (0..1) of the query's historical cost.
8. ``file_is_recent``         — 1 iff the file was written in the last 24 h, else 0.
9. ``file_is_on_pin_list``    — 1 iff the object's table/partition is pinned.

The helper :func:`feature_order` returns this tuple so callers cannot
silently reorder. The actual extraction (joining
``cdp.trino_logs.trino_queries`` with Iceberg manifest metadata) is a stub.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl

FEATURE_ORDER: tuple[str, ...] = (
    "table_tf_7d",
    "table_tu_7d",
    "partition_depth",
    "user_type",
    "size_mb",
    "hour_of_day",
    "recency_days",
    "query_cost_rank",
    "file_is_recent",
    "file_is_on_pin_list",
)


def feature_order() -> tuple[str, ...]:
    """Return the canonical feature order (index-stable)."""
    return FEATURE_ORDER


def extract_features(
    query_log: pl.DataFrame,
    *,
    as_of: datetime,
    pin_list: frozenset[str],
) -> pl.DataFrame:
    """Extract the 10-feature admission frame.

    Parameters
    ----------
    query_log:
        Raw rows from ``cdp.trino_logs.trino_queries`` (or a sample thereof).
    as_of:
        Decision timestamp. Features are computed *as if* scoring at this
        instant — nothing in the feature vector may depend on events at or
        after ``as_of`` (leakage control; see ``docs/labels.md``).
    pin_list:
        Set of ``schema.table`` identifiers currently pinned. Used to
        derive ``file_is_on_pin_list``.

    Returns
    -------
    polars.DataFrame
        One row per candidate (key, size) pair, columns exactly
        :data:`FEATURE_ORDER` in order, plus any trailing label/id columns
        the caller wants. Numeric dtypes only for the 10 features.
    """
    del query_log, as_of, pin_list
    raise NotImplementedError(
        "SHELF-34: feature extraction from cdp.trino_logs.trino_queries (BLUEPRINT §7.3)."
    )
