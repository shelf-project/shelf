"""LightGBM admission model (v1.x escape hatch per ADR-0003).

**Status:** stub. ADR-0003 rejects the v0.3 blueprint's ONNX MLP and
designates a LightGBM binary classifier (predicting
``P(reaccess_within_1h)`` on the 10 features listed in BLUEPRINT §7.3)
as the *only* v1.x learned-admission candidate. shelfd would load it
through the Rust ``lightgbm3`` binding; the exported artifact is a
plain ``.txt`` booster dump, **not** ONNX.

A candidate model only ships if, on a 30-day replay (Phase 4):

* hit-rate lift over size-threshold is ≥ 5 pp, **and**
* p99 inference latency on the large-miss path is < 50 µs.

Everything here raises ``NotImplementedError`` until those gates are
measured. Do not fill this in before Phase 4.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl


@dataclass(frozen=True)
class LightGBMAdmissionArtifact:
    """Container for a trained LightGBM admission model + its metadata."""

    booster_path: Path
    meta_path: Path
    feature_order: tuple[str, ...]
    training_data_hash: str
    trained_at_iso: str


class LightGBMAdmissionTrainer:
    """Trainer for the v1.x LightGBM admission model (stub).

    The real implementation will:

    1. Consume a leakage-safe training frame from :mod:`shelf_trainer.labels`.
    2. Fit a LightGBM binary classifier with
       ``objective="binary"``, ``metric="average_precision"``.
    3. Export the booster via ``booster.save_model(path)`` (text format).
    4. Emit ``<booster>.meta.json`` with feature order + normalisation
       constants + training-data hash + eval metrics (AUC-PR, calibration,
       coverage).

    See ``docs/labels.md`` for the label definition and split strategy.
    """

    def __init__(self, *, feature_order: tuple[str, ...]) -> None:
        self.feature_order = feature_order

    def train(self, frame: pl.DataFrame) -> LightGBMAdmissionArtifact:
        """Fit the LightGBM booster on ``frame`` and return the artifact paths.

        Not implemented until Phase 4 replay benchmark passes ADR-0003's
        escape-hatch thresholds.
        """
        raise NotImplementedError(
            "SHELF-29: LightGBM admission trainer (see ADR-0003; Phase 4 replay gate)."
        )

    def save(self, artifact: LightGBMAdmissionArtifact) -> None:
        """Publish the artifact to the config bucket's ``admission/candidate/`` key."""
        raise NotImplementedError(
            "SHELF-30: publish LightGBM artifact to admission/candidate/ in S3."
        )

    def predict(self, features: pl.DataFrame) -> pl.Series:
        """Score ``features`` and return ``P(reaccess_within_1h)``.

        Used by the offline replay harness (Phase 4) to compute hit-rate
        lift vs size-threshold. The production scorer is Rust/``lightgbm3``.
        """
        raise NotImplementedError("SHELF-31: LightGBM offline scoring for replay harness.")

    @classmethod
    def load(cls, artifact_dir: Path) -> LightGBMAdmissionTrainer:
        """Load a previously-saved artifact for offline scoring."""
        del artifact_dir
        raise NotImplementedError("SHELF-32: load LightGBM artifact from disk.")

    def to_meta(self) -> dict[str, Any]:
        """Serialise metadata sidecar to match contracts/admission-model.md."""
        raise NotImplementedError(
            "SHELF-33: emit admission_v<N>.meta.json matching contracts/admission-model.md."
        )
