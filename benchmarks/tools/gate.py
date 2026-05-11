#!/usr/bin/env python3
"""benchmarks/tools/gate.py — v0.5 gate evaluator (ADR-0010).

Reads one Shelf-backend replay record and one Alluxio-backend replay
record (the v0.5 gate is *comparative* — Shelf p95 vs Alluxio p95 — so
both records are required for a defensible verdict).

Inputs:
    --shelf <path-to-replay-record-json>      (backend=shelf)
    --baseline <path-to-replay-record-json>   (backend=alluxio-2-9 by default)
    --pages-shelf-attributed N                (count from PagerDuty / oncall log)
    --oncall-surface-shelf S                  (Shelf oncall-surface-ratio)
    --oncall-surface-baseline S               (Baseline oncall-surface-ratio)

Per ADR-0010 / SPEC.md, all 5 gate metrics must hold:
    hit_rate_7d_cumulative   >= 0.71
    gold_dbt_ok_rate         >= 0.999
    latency_ns_p95_vs_alluxio<= 1.20
    shelf_caused_pages       == 0
    oncall_surface_ratio     <= 0.50

Outputs:
    1. JSON to stdout: { "verdict": "pass|fail|n/a", "failed_metrics":[...], ... }
    2. RESULTS.md row to stdout if --emit-row is set.
    3. Exit code 0 (pass), 1 (fail), 2 (n/a or invalid input).
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from pathlib import Path
from typing import Any


# Thresholds — ADR-0010 / replay/SPEC.md §Metrics.
GATE_THRESHOLDS = {
    "hit_rate_7d_cumulative": 0.71,
    "gold_dbt_ok_rate": 0.999,
    "latency_ns_p95_vs_alluxio": 1.20,
    "shelf_caused_pages": 0,
    "oncall_surface_ratio": 0.50,
}


@dataclasses.dataclass
class GateInput:
    shelf_record: dict[str, Any]
    baseline_record: dict[str, Any]
    pages_shelf_attributed: int
    oncall_surface_shelf: float
    oncall_surface_baseline: float


@dataclasses.dataclass
class GateResult:
    verdict: str  # "pass" | "fail" | "n/a"
    failed_metrics: list[str]
    metrics: dict[str, float | int | None]
    notes: list[str]


def evaluate(inp: GateInput) -> GateResult:
    notes: list[str] = []
    if inp.shelf_record.get("benchmark") != "replay":
        return GateResult("n/a", [], {}, ["shelf record is not a replay benchmark"])
    if inp.baseline_record.get("benchmark") != "replay":
        return GateResult("n/a", [], {}, ["baseline record is not a replay benchmark"])
    if inp.shelf_record.get("backend") != "shelf":
        return GateResult("n/a", [], {}, ["shelf record's backend is not 'shelf'"])
    if inp.shelf_record.get("trace", {}).get("speed") != "2x":
        return GateResult("n/a", [], {}, ["gate is only evaluated at speed=2x"])
    if inp.baseline_record.get("trace", {}).get("speed") != "2x":
        return GateResult("n/a", [], {}, ["baseline must also be speed=2x"])

    failed: list[str] = []

    hit_rate = float(inp.shelf_record["summary"]["hit_rate"])
    gold_dbt_ok = float(inp.shelf_record["gate"]["gold_dbt_ok_rate"])
    shelf_p95 = int(inp.shelf_record["summary"]["latency_ns_p95"])
    baseline_p95 = int(inp.baseline_record["summary"]["latency_ns_p95"])
    if baseline_p95 == 0:
        return GateResult("n/a", [], {}, ["baseline p95 is zero — invalid"])
    p95_ratio = shelf_p95 / baseline_p95

    if hit_rate < GATE_THRESHOLDS["hit_rate_7d_cumulative"]:
        failed.append("hit_rate_7d_cumulative")
    if gold_dbt_ok < GATE_THRESHOLDS["gold_dbt_ok_rate"]:
        failed.append("gold_dbt_ok_rate")
    if p95_ratio > GATE_THRESHOLDS["latency_ns_p95_vs_alluxio"]:
        failed.append("latency_ns_p95_vs_alluxio")
    if inp.pages_shelf_attributed > GATE_THRESHOLDS["shelf_caused_pages"]:
        failed.append("shelf_caused_pages")
    surface_ratio = (
        inp.oncall_surface_shelf / inp.oncall_surface_baseline
        if inp.oncall_surface_baseline > 0
        else 0.0
    )
    if surface_ratio > GATE_THRESHOLDS["oncall_surface_ratio"]:
        failed.append("oncall_surface_ratio")

    metrics = {
        "hit_rate_7d_cumulative": round(hit_rate, 6),
        "gold_dbt_ok_rate": round(gold_dbt_ok, 6),
        "latency_ns_p95_vs_alluxio": round(p95_ratio, 6),
        "shelf_caused_pages": inp.pages_shelf_attributed,
        "oncall_surface_ratio": round(surface_ratio, 6),
    }

    return GateResult(
        verdict="pass" if not failed else "fail",
        failed_metrics=failed,
        metrics=metrics,
        notes=notes,
    )


def render_results_md_row(
    result: GateResult,
    shelf_record: dict[str, Any],
    *,
    date_utc: str,
    backend: str = "shelf",
) -> str:
    """Render the v0.5 gate-board row for benchmarks/RESULTS.md."""
    m = result.metrics
    if result.verdict == "pass":
        verdict_cell = "**PASS**"
    elif result.verdict == "fail":
        verdict_cell = "**FAIL: " + ", ".join(result.failed_metrics) + "**"
    else:
        verdict_cell = "n/a"

    def fmt_pct(x: float | int | None) -> str:
        if x is None:
            return "—"
        return f"{float(x) * 100:.2f} %"

    def fmt_ratio(x: float | int | None) -> str:
        if x is None:
            return "—"
        return f"{float(x):.2f}"

    return (
        f"| {date_utc} | {backend} | "
        f"{fmt_pct(m.get('hit_rate_7d_cumulative'))} | "
        f"{fmt_pct(m.get('gold_dbt_ok_rate'))} | "
        f"{fmt_ratio(m.get('latency_ns_p95_vs_alluxio'))} | "
        f"{int(m.get('shelf_caused_pages') or 0)} | "
        f"{fmt_ratio(m.get('oncall_surface_ratio'))} | "
        f"{verdict_cell} |"
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="v0.5 gate evaluator (ADR-0010).")
    p.add_argument("--shelf", required=True, type=Path)
    p.add_argument("--baseline", required=True, type=Path)
    p.add_argument("--pages-shelf-attributed", type=int, default=0)
    p.add_argument("--oncall-surface-shelf", type=float, default=0.0)
    p.add_argument("--oncall-surface-baseline", type=float, default=1.0)
    p.add_argument("--emit-row", action="store_true",
                   help="Print a RESULTS.md gate-board row to stdout instead of JSON.")
    args = p.parse_args(argv)

    if not args.shelf.is_file():
        print(f"ERROR: shelf record not found: {args.shelf}", file=sys.stderr)
        return 2
    if not args.baseline.is_file():
        print(f"ERROR: baseline record not found: {args.baseline}", file=sys.stderr)
        return 2

    with args.shelf.open("r", encoding="utf-8") as fh:
        shelf_record = json.load(fh)
    with args.baseline.open("r", encoding="utf-8") as fh:
        baseline_record = json.load(fh)

    inp = GateInput(
        shelf_record=shelf_record,
        baseline_record=baseline_record,
        pages_shelf_attributed=args.pages_shelf_attributed,
        oncall_surface_shelf=args.oncall_surface_shelf,
        oncall_surface_baseline=args.oncall_surface_baseline,
    )

    result = evaluate(inp)

    if args.emit_row:
        date_utc = shelf_record.get("timestamp", "")[:10] or "(no-date)"
        print(render_results_md_row(result, shelf_record, date_utc=date_utc))
    else:
        print(json.dumps(dataclasses.asdict(result), indent=2))

    if result.verdict == "pass":
        return 0
    if result.verdict == "fail":
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
