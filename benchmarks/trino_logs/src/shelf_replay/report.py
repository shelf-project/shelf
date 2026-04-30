"""CSV + JSON writers for the analyse / simulate commands."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .aggregate import DayAggregate
from .simulate import SimResult
from .types import ScanResult, TraceEntry


def write_per_query_csv(
    out_dir: Path,
    trace: list[TraceEntry],
    scans: list[ScanResult],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "per-query.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "query_id",
                "day",
                "catalog",
                "schema",
                "files_scanned",
                "scanned_bytes_file_level",
                "files_after_partition_prune",
                "scanned_bytes_rg_level",
                "rg_over_file_ratio",
                "rg_count",
                "rg_pruning_unsupported_columns",
                "wall_time_millis",
                "physical_input_bytes",
            ]
        )
        for entry, scan in zip(trace, scans):
            writer.writerow(
                [
                    entry.query_id,
                    scan.day,
                    entry.catalog or "",
                    entry.schema or "",
                    scan.files_scanned,
                    scan.scanned_bytes_file_level,
                    scan.files_after_partition_prune,
                    scan.scanned_bytes_rg_level,
                    f"{scan.rg_over_file_ratio:.6f}",
                    scan.rg_count,
                    scan.rg_pruning_unsupported_columns,
                    entry.wall_time_millis if entry.wall_time_millis is not None else "",
                    entry.physical_input_bytes
                    if entry.physical_input_bytes is not None
                    else "",
                ]
            )
    return path


def write_per_day_csv(out_dir: Path, aggregates: list[DayAggregate]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "per-day.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "day",
                "query_count",
                "total_file_bytes",
                "total_rg_bytes",
                "overall_ratio",
                "median_ratio",
                "p90_ratio",
            ]
        )
        for a in aggregates:
            writer.writerow(
                [
                    a.day,
                    a.query_count,
                    a.total_file_bytes,
                    a.total_rg_bytes,
                    f"{a.overall_ratio:.6f}",
                    f"{a.median_ratio:.6f}",
                    f"{a.p90_ratio:.6f}",
                ]
            )
    return path


def write_sim_csv(out_dir: Path, sim: SimResult) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    safe = sim.config.name.replace("/", "_")
    path = out_dir / f"sim-{safe}.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["timestamp", "cumulative_hit_rate"])
        for ts, hr in sim.hit_rate_curve:
            writer.writerow([ts, f"{hr:.6f}"])
    return path


def write_summary_json(
    out_dir: Path,
    trace: list[TraceEntry],
    scans: list[ScanResult],
    aggregates: list[DayAggregate],
    sims: Iterable[SimResult],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "summary.json"
    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "trace": {
            "queries": len(trace),
            "days": sorted({e.day for e in trace}),
        },
        "scan_totals": {
            "scanned_bytes_file_level": sum(s.scanned_bytes_file_level for s in scans),
            "scanned_bytes_rg_level": sum(s.scanned_bytes_rg_level for s in scans),
            "rg_count": sum(s.rg_count for s in scans),
        },
        "per_day": [asdict(a) | {"overall_ratio": a.overall_ratio} for a in aggregates],
        "simulations": [
            {
                "config": {
                    "name": s.config.name,
                    "capacity_bytes": s.config.capacity_bytes,
                    "policy": s.config.policy,
                    "size_threshold_bytes": s.config.size_threshold_bytes,
                    "pinned_bypass": s.config.pinned_bypass,
                    "pin_list_size": len(s.config.pin_list),
                },
                "hits": s.hits,
                "misses": s.misses,
                "bytes_hit": s.bytes_hit,
                "bytes_miss": s.bytes_miss,
                "admitted_bytes": s.admitted_bytes,
                "rejected_by_threshold": s.rejected_by_threshold,
                "evicted_bytes": s.evicted_bytes,
                "hit_rate": s.hit_rate,
            }
            for s in sims
        ],
    }
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=False)
    return path
