"""Tests for :mod:`shelf_trainer.promotion`."""

from __future__ import annotations

import pytest


@pytest.mark.skip(reason="TODO SHELF-45: promotion guardrail matrix.")
def test_promotion_passes_when_guardrails_met() -> None:
    raise AssertionError("unreachable")


@pytest.mark.skip(reason="TODO SHELF-45: promotion refuses on sub-5pp lift.")
def test_promotion_refuses_small_lift() -> None:
    raise AssertionError("unreachable")


@pytest.mark.skip(reason="TODO SHELF-45: promotion refuses on p99 > 50us.")
def test_promotion_refuses_slow_candidate() -> None:
    raise AssertionError("unreachable")


@pytest.mark.skip(reason="TODO SHELF-46: S3 alias flip audit log.")
def test_promote_flips_alias() -> None:
    raise AssertionError("unreachable")


@pytest.mark.skip(reason="TODO SHELF-47: rollback semantics.")
def test_rollback_to_prior() -> None:
    raise AssertionError("unreachable")
