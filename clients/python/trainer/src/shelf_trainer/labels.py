"""Label construction and leakage controls (stub).

The one-pager at ``docs/labels.md`` is the source of truth. Summary:

- **Target:** ``y = 1`` iff the object key is re-read within
  ``PREDICTION_HORIZON`` after its first read inside the training window.
- **Prediction horizon:** 1 hour (matches BLUEPRINT §7.3 "P(reaccess<1h)").
- **Split:** time-based; never random. Train on days ``[t0, t1)``, validate
  on ``[t1, t2)``, test on ``[t2, t3)``. ``t2 - t1`` ≥ horizon to avoid
  near-boundary leakage.
- **Leakage controls:** no feature may read events at or after the decision
  timestamp. Reject frames where label-feature correlation is implausibly
  high (sanity-gate).

This module is a stub. Do not reimplement in the caller — always route
through :func:`build_training_frame`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import polars as pl

PREDICTION_HORIZON: timedelta = timedelta(hours=1)


@dataclass(frozen=True, slots=True)
class TimeSplit:
    """A three-way time-ordered split.

    Invariant: ``train_end <= valid_start`` and
    ``valid_end <= test_start`` and ``valid_start - train_end >= horizon``.
    """

    train_start: datetime
    train_end: datetime
    valid_start: datetime
    valid_end: datetime
    test_start: datetime
    test_end: datetime
    horizon: timedelta = PREDICTION_HORIZON

    def __post_init__(self) -> None:
        if not (self.train_start < self.train_end <= self.valid_start):
            raise ValueError("train_start < train_end <= valid_start violated")
        if not (self.valid_start < self.valid_end <= self.test_start):
            raise ValueError("valid_start < valid_end <= test_start violated")
        if self.valid_start - self.train_end < self.horizon:
            raise ValueError("valid_start - train_end must be >= horizon to avoid label leakage")


def build_training_frame(
    query_log: pl.DataFrame,
    *,
    split: TimeSplit,
) -> pl.DataFrame:
    """Join features with labels for the ``split.train_*`` window.

    Returns a frame with columns ``[*feature_order, y, decision_ts]``.
    Labels are computed by joining each decision row against future reads
    in ``(decision_ts, decision_ts + split.horizon]``.

    Raises ``NotImplementedError`` — the real join logic lives in SHELF-35.
    """
    del query_log, split
    raise NotImplementedError("SHELF-35: time-based leakage-safe label join (see docs/labels.md).")


def assert_no_leakage(frame: pl.DataFrame, *, max_point_biserial: float = 0.98) -> None:
    """Sanity-gate: refuse training frames with absurd label-feature correlation.

    A point-biserial correlation between any single feature and the binary
    label that is above ``max_point_biserial`` is almost always a leaked
    feature. The real implementation raises ``LeakageError``; here it stubs.
    """
    del frame, max_point_biserial
    raise NotImplementedError("SHELF-36: leakage sanity gate on training frame.")
