"""Tests for :mod:`shelf_trainer.models.lightgbm_admission`."""

from __future__ import annotations

import pytest

from shelf_trainer.features import FEATURE_ORDER
from shelf_trainer.models.lightgbm_admission import LightGBMAdmissionTrainer


def test_trainer_stores_feature_order() -> None:
    trainer = LightGBMAdmissionTrainer(feature_order=FEATURE_ORDER)
    assert trainer.feature_order == FEATURE_ORDER


@pytest.mark.skip(reason="TODO SHELF-29: LightGBM train() — Phase 4 gated.")
def test_train_fits_booster() -> None:
    raise AssertionError("unreachable")


@pytest.mark.skip(reason="TODO SHELF-30: save() publishes to admission/candidate/.")
def test_save_publishes_artifact() -> None:
    raise AssertionError("unreachable")


@pytest.mark.skip(reason="TODO SHELF-31: predict() offline scoring parity.")
def test_predict_matches_rust_lightgbm3() -> None:
    raise AssertionError("unreachable")
