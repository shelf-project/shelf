"""Tests for :mod:`shelf_trainer.config`. Mostly stubs for now."""

from __future__ import annotations

import pytest

from shelf_trainer.config import TrainerSettings


def test_defaults_are_sane() -> None:
    s = TrainerSettings()
    assert s.pin_list_top_n == 200
    assert 0.0 <= s.canary_fraction <= 1.0
    assert s.promote_hit_rate_delta_pp >= 5.0
    assert s.promote_p99_latency_us_max <= 50.0
    assert "airflow_user" in s.pin_list_exclude_users


@pytest.mark.skip(reason="TODO SHELF-52: env-var override matrix + .env loading test.")
def test_env_var_override() -> None:
    raise AssertionError("unreachable")
