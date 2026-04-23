"""Tests for :mod:`shelf_trainer.labels`."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from shelf_trainer.labels import PREDICTION_HORIZON, TimeSplit


def test_horizon_is_one_hour() -> None:
    assert timedelta(hours=1) == PREDICTION_HORIZON


def test_timesplit_valid() -> None:
    base = datetime(2026, 4, 1)
    split = TimeSplit(
        train_start=base,
        train_end=base + timedelta(days=7),
        valid_start=base + timedelta(days=7, hours=2),
        valid_end=base + timedelta(days=9),
        test_start=base + timedelta(days=9),
        test_end=base + timedelta(days=10),
    )
    assert split.horizon == PREDICTION_HORIZON


def test_timesplit_rejects_leakage_window() -> None:
    base = datetime(2026, 4, 1)
    with pytest.raises(ValueError, match="leakage"):
        TimeSplit(
            train_start=base,
            train_end=base + timedelta(days=7),
            valid_start=base + timedelta(days=7, minutes=5),
            valid_end=base + timedelta(days=9),
            test_start=base + timedelta(days=9),
            test_end=base + timedelta(days=10),
        )


@pytest.mark.skip(reason="TODO SHELF-35: build_training_frame label join.")
def test_build_training_frame() -> None:
    raise AssertionError("unreachable")


@pytest.mark.skip(reason="TODO SHELF-36: assert_no_leakage sanity gate.")
def test_assert_no_leakage() -> None:
    raise AssertionError("unreachable")
