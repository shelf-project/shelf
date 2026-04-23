"""Feature-distribution drift monitor (stub).

Compares the distribution of each feature in the most-recent training
frame against the distribution of the same feature *at serve time* in
shelfd's emitted admission-decision log. Purpose: catch silent model
regressions where yesterday's model looked fine but today's training
frame is unrepresentative.

Approach (v1.x, when LightGBM ships):

- Per-feature Population Stability Index (PSI) with 10 quantile bins.
- Threshold: PSI ≥ 0.2 on any feature -> alert.
- Categorical features use chi-square instead of PSI.

Not wired in v1 (no model to monitor). Retained as a skeleton so
Phase 4 has an obvious landing zone.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True, slots=True)
class DriftReport:
    """Per-feature drift summary."""

    feature: str
    psi: float
    alert: bool


def compute_drift(
    training_frame: pl.DataFrame,
    serving_frame: pl.DataFrame,
    *,
    psi_alert_threshold: float = 0.2,
) -> list[DriftReport]:
    """Return a per-feature drift report."""
    del training_frame, serving_frame, psi_alert_threshold
    raise NotImplementedError("SHELF-43: PSI drift monitor (Phase 4+, gated on LightGBM landing).")


def population_stability_index(
    baseline: pl.Series,
    candidate: pl.Series,
    *,
    n_bins: int = 10,
) -> float:
    """PSI between ``baseline`` and ``candidate`` on ``n_bins`` quantile bins."""
    del baseline, candidate, n_bins
    raise NotImplementedError("SHELF-44: PSI computation.")
