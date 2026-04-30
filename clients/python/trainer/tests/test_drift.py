"""Tests for :mod:`shelf_trainer.drift`."""

from __future__ import annotations

import pytest


@pytest.mark.skip(reason="TODO SHELF-43: PSI drift monitor (Phase 4+).")
def test_compute_drift_flags_distribution_shift() -> None:
    raise AssertionError("unreachable")


@pytest.mark.skip(reason="TODO SHELF-44: PSI known-value regression.")
def test_psi_known_values() -> None:
    raise AssertionError("unreachable")
