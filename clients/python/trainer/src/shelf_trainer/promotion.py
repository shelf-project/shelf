"""Candidate -> production promotion with guardrails (stub).

Implements Agent 6 Pass 4: every promotion is an explicit, logged action;
there is no "cron line" that silently swaps models. A promotion is valid
iff **all** of the following hold (ADR-0003):

1. Replay-benchmark hit-rate delta ≥ ``promote_hit_rate_delta_pp``
   (default 5 pp) over size-threshold baseline.
2. p99 offline inference latency < ``promote_p99_latency_us_max`` µs
   (default 50 µs).
3. Canary 24-hour observation shows hit-rate and admit-rate within
   guardrails of the prior production model (no silent regression).
4. Coverage ≥ ``promote_coverage_min`` (default 0.95).

Rollback is the same machinery run backwards: swap the
``admission/production/`` alias back to the last known-good artifact and
emit a structured audit log entry.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from shelf_trainer.evaluation import EvaluationReport


@dataclass(frozen=True, slots=True)
class PromotionDecision:
    """Structured outcome of one promotion attempt."""

    promoted: bool
    candidate_version: str
    previous_version: str
    reason: str
    decided_at: datetime


def evaluate_promotion(
    candidate: EvaluationReport,
    *,
    prior_production: EvaluationReport,
    hit_rate_delta_pp_min: float,
    p99_latency_us_candidate: float,
    p99_latency_us_max: float,
    coverage_min: float,
) -> PromotionDecision:
    """Apply the four promotion guardrails and return a decision."""
    del (
        candidate,
        prior_production,
        hit_rate_delta_pp_min,
        p99_latency_us_candidate,
        p99_latency_us_max,
        coverage_min,
    )
    raise NotImplementedError("SHELF-45: promotion guardrails (ADR-0003 thresholds; 24h canary).")


def promote(candidate_version: str) -> PromotionDecision:
    """Flip the ``admission/production/`` S3 alias to ``candidate_version``."""
    del candidate_version
    raise NotImplementedError("SHELF-46: S3 alias flip + audit log.")


def rollback(to_version: str) -> PromotionDecision:
    """Revert ``admission/production/`` to ``to_version``."""
    del to_version
    raise NotImplementedError("SHELF-47: rollback to prior production artifact.")
