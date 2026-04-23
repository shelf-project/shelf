"""Tests for the v1 size-threshold admission policy.

These are **not** skipped — size-threshold is the only real v1 logic, so
it has real coverage. Everything else in :mod:`shelf_trainer` is stubbed
and tested only for "raises NotImplementedError with a ticket ID".
"""

from __future__ import annotations

import pytest

from shelf_trainer.models.size_threshold import SizeThresholdAdmission

_1_GIB = 1 << 30
_1_MIB = 1 << 20


class TestSizeThresholdAdmission:
    def test_small_object_admitted(self) -> None:
        policy = SizeThresholdAdmission()
        assert policy.admit(size=64 * _1_MIB, pinned=False) is True

    def test_at_threshold_refused(self) -> None:
        policy = SizeThresholdAdmission()
        assert policy.admit(size=_1_GIB, pinned=False) is False

    def test_above_threshold_refused(self) -> None:
        policy = SizeThresholdAdmission()
        assert policy.admit(size=2 * _1_GIB, pinned=False) is False

    def test_pinned_large_object_admitted_by_default(self) -> None:
        policy = SizeThresholdAdmission()
        assert policy.admit(size=5 * _1_GIB, pinned=True) is True

    def test_pinned_ignored_when_bypass_disabled(self) -> None:
        policy = SizeThresholdAdmission(pinned_bypass=False)
        assert policy.admit(size=5 * _1_GIB, pinned=True) is False

    def test_zero_size_admitted(self) -> None:
        policy = SizeThresholdAdmission()
        assert policy.admit(size=0, pinned=False) is True

    def test_negative_size_raises(self) -> None:
        policy = SizeThresholdAdmission()
        with pytest.raises(ValueError, match="non-negative"):
            policy.admit(size=-1, pinned=False)

    def test_non_positive_threshold_raises(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            SizeThresholdAdmission(size_threshold_bytes=0)

    def test_custom_threshold(self) -> None:
        policy = SizeThresholdAdmission(size_threshold_bytes=256 * _1_MIB)
        assert policy.admit(size=128 * _1_MIB, pinned=False) is True
        assert policy.admit(size=512 * _1_MIB, pinned=False) is False
