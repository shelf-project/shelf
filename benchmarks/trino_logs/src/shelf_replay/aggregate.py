"""Aggregations for CSV / JSON reporting (E5 median + P90 ratios)."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Iterable

from .types import ScanResult


def quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if q <= 0:
        return sorted_values[0]
    if q >= 1:
        return sorted_values[-1]
    idx = q * (len(sorted_values) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = idx - lo
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac


@dataclass(frozen=True)
class DayAggregate:
    day: str
    query_count: int
    total_file_bytes: int
    total_rg_bytes: int
    median_ratio: float
    p90_ratio: float

    @property
    def overall_ratio(self) -> float:
        if self.total_file_bytes == 0:
            return 0.0
        return self.total_rg_bytes / self.total_file_bytes


def aggregate_by_day(scans: Iterable[ScanResult]) -> list[DayAggregate]:
    by_day: dict[str, list[ScanResult]] = {}
    for s in scans:
        by_day.setdefault(s.day, []).append(s)

    out: list[DayAggregate] = []
    for day in sorted(by_day):
        bucket = by_day[day]
        ratios = sorted(
            s.rg_over_file_ratio for s in bucket if s.scanned_bytes_file_level > 0
        )
        total_file = sum(s.scanned_bytes_file_level for s in bucket)
        total_rg = sum(s.scanned_bytes_rg_level for s in bucket)
        out.append(
            DayAggregate(
                day=day,
                query_count=len(bucket),
                total_file_bytes=total_file,
                total_rg_bytes=total_rg,
                median_ratio=median(ratios) if ratios else 0.0,
                p90_ratio=quantile(ratios, 0.90),
            )
        )
    return out
