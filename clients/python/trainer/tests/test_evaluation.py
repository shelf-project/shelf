"""Tests for :mod:`shelf_trainer.evaluation`."""

from __future__ import annotations

import pytest


@pytest.mark.skip(reason="TODO SHELF-40: AUC-PR on held-out temporal split.")
def test_auc_pr_primary_metric() -> None:
    raise AssertionError("unreachable")


@pytest.mark.skip(reason="TODO SHELF-41: calibration slope sanity.")
def test_calibration_slope() -> None:
    raise AssertionError("unreachable")


@pytest.mark.skip(reason="TODO SHELF-42: coverage metric on large-miss path.")
def test_coverage() -> None:
    raise AssertionError("unreachable")
