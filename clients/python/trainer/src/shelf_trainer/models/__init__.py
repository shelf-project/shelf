"""Admission models.

The only v1-production model is :class:`size_threshold.SizeThresholdAdmission`.
The LightGBM trainer in :mod:`shelf_trainer.models.lightgbm_admission` is a
v1.x escape hatch per ADR-0003 and is stubbed out.
"""

from __future__ import annotations

from shelf_trainer.models.size_threshold import SizeThresholdAdmission

__all__ = ["SizeThresholdAdmission"]
