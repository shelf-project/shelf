"""Evaluation metrics for the admission candidate (stub).

Metric hierarchy (per Agent 6 Pass 0 and ``docs/labels.md``):

- **Primary:** AUC-PR on the held-out *temporal* test split. Average
  precision captures performance on the rare positive class
  (re-access within 1 h is the minority label).
- **Guardrails:**
    * *Calibration* — reliability-diagram slope on binned predictions.
      A model whose 0.3 bucket hits ≠ ~30 % in practice breaks the
      admission threshold that shelfd compares against.
    * *Coverage* — fraction of eligible large-miss decisions the model
      actually scored (vs silently fell back to size-threshold).
    * *Replay delta* — simulated NVMe hit-rate delta vs size-threshold
      baseline on 7-day replay. ADR-0003 promote gate is ≥ 5 pp.

All four are computed; all four go in ``admission_v<N>.meta.json``; only
the primary is used for ordering candidates.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True, slots=True)
class EvaluationReport:
    """Container for a full candidate evaluation."""

    auc_pr: float
    calibration_slope: float
    coverage: float
    replay_hit_rate_delta_pp: float


def evaluate(predictions: pl.DataFrame, labels: pl.Series) -> EvaluationReport:
    """Compute AUC-PR, calibration, coverage, and replay delta."""
    del predictions, labels
    raise NotImplementedError(
        "SHELF-40: evaluation metrics (AUC-PR, calibration, coverage, replay delta)."
    )


def calibration_slope(predictions: pl.Series, labels: pl.Series, *, n_bins: int = 20) -> float:
    """Reliability-diagram slope on ``n_bins`` equal-frequency bins."""
    del predictions, labels, n_bins
    raise NotImplementedError("SHELF-41: calibration slope via reliability bins.")


def coverage(predictions: pl.Series) -> float:
    """Fraction of eligible rows the model scored (vs fallback)."""
    del predictions
    raise NotImplementedError("SHELF-42: decision coverage of candidate.")
