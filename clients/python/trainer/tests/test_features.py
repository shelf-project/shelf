"""Tests for :mod:`shelf_trainer.features`."""

from __future__ import annotations

import pytest

from shelf_trainer.features import FEATURE_ORDER, feature_order


def test_feature_order_is_stable_tuple() -> None:
    assert isinstance(feature_order(), tuple)
    assert feature_order() == FEATURE_ORDER
    assert len(FEATURE_ORDER) == 10


def test_feature_names_match_blueprint_7_3() -> None:
    expected = {
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
    }
    assert set(FEATURE_ORDER) == expected


@pytest.mark.skip(reason="TODO SHELF-34: feature-extraction join against trino_queries.")
def test_extract_features_basic() -> None:
    raise AssertionError("unreachable")
