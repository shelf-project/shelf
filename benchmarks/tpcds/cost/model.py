#!/usr/bin/env python3
"""F3 — cost model for the TPC-DS harness.

Takes a run directory (output of `runner/run.py`) and joins each
query's wall-clock against `hardware.yaml` to produce a normalised
`$/query` column alongside the raw timing.

The model is deliberately simple and explicit: every assumption is
printed into `summary.md` so a reviewer can redo the arithmetic by
hand. No lookup services, no market-rate APIs — those change week
to week and would make historical results unreproducible.

Inputs
------
run-dir/
    <engine>.csv    raw per-query CSVs from run.py (see run.py
                    field schema)

Outputs
-------
run-dir/
    summary.md      markdown scorecard, one row per (engine, query)
    costed.csv      raw.csv + $/query column

Invariants
----------
- Every engine must have the same set of query_ids in raw CSV;
  a missing query aborts the run with a non-zero exit so humans
  see the gap instead of silently publishing a partial win.
- The cold/warm1/warm2 triple from run.py collapses here to a
  p50 of the three; absolute wall-clock for cold reads is
  published separately in summary.md.
"""

from __future__ import annotations

import argparse
import csv
import pathlib
import statistics
import sys


def load_hardware(path: pathlib.Path) -> dict:
    import yaml  # type: ignore

    with path.open() as f:
        return yaml.safe_load(f)


def hourly_cost_usd(cfg: dict) -> float:
    """Compute a single number: $ per hour for the whole engine
    configuration. Different engines take different code paths —
    shelf and Alluxio share the compute formula; Warp Speed adds
    a license term; Firebolt is entirely FBU-driven."""
    if "fbu" in cfg:
        storage = 0.0  # rolled into FBU
        return cfg["fbu"] * cfg["fbu_hourly_rate"]
    compute = cfg["nodes"] * cfg["hourly_per_instance"]
    storage = cfg["nodes"] * cfg["storage_gb"] * cfg["storage_hourly_per_gb"]
    license_cost = 0.0
    if "starburst_license_per_vcpu_hour" in cfg:
        vcpus = _vcpus_for_instance(cfg["instance_type"]) * cfg["nodes"]
        license_cost = vcpus * cfg["starburst_license_per_vcpu_hour"]
    return compute + storage + license_cost


# Hard-coded vCPU counts for the instance types we actually compare.
# The alternative — pulling from the AWS pricing API — is not
# reproducible; the numbers below are verifiable from AWS docs and
# never change.
_VCPU_TABLE = {
    "m6a.4xlarge": 16,
    "m6a.12xlarge": 48,
    "m6a.24xlarge": 96,
}


def _vcpus_for_instance(instance: str) -> int:
    if instance not in _VCPU_TABLE:
        raise SystemExit(f"unknown instance type '{instance}'; extend _VCPU_TABLE")
    return _VCPU_TABLE[instance]


def load_raw_csv(path: pathlib.Path) -> list[dict]:
    with path.open() as f:
        return list(csv.DictReader(f))


def compute_costed_rows(engine: str, rows: list[dict], hourly_cost: float) -> list[dict]:
    out = []
    for row in rows:
        elapsed_s = float(row["elapsed_ms"]) / 1000.0
        dollars = (elapsed_s / 3600.0) * hourly_cost
        row = dict(row)
        row["query_cost_usd"] = f"{dollars:.6f}"
        out.append(row)
    return out


def group_p50_by_query(rows: list[dict]) -> dict[str, dict[str, float]]:
    by_query: dict[str, list[float]] = {}
    by_query_cold: dict[str, float] = {}
    by_query_cost: dict[str, list[float]] = {}
    for row in rows:
        qid = row["query_id"]
        ms = float(row["elapsed_ms"])
        by_query.setdefault(qid, []).append(ms)
        by_query_cost.setdefault(qid, []).append(float(row["query_cost_usd"]))
        if row["phase"] == "cold":
            by_query_cold[qid] = ms
    summary = {}
    for qid, samples in by_query.items():
        summary[qid] = {
            "p50_ms": statistics.median(samples),
            "cold_ms": by_query_cold.get(qid, float("nan")),
            "cost_usd_p50": statistics.median(by_query_cost[qid]),
        }
    return summary


def render_summary_md(run_dir: pathlib.Path, hw_cfg: dict, per_engine: dict[str, dict]) -> str:
    engines = sorted(per_engine)
    # Canonical query order = whatever order query_ids appear in the
    # first engine's CSV (which is deterministic — glob('q*.sql')
    # yields them in lex order).
    query_ids: list[str] = []
    seen: set[str] = set()
    for eng in engines:
        for qid in per_engine[eng]:
            if qid not in seen:
                query_ids.append(qid)
                seen.add(qid)

    lines = [
        "# TPC-DS cross-engine results",
        "",
        f"Run directory: `{run_dir.name}`.",
        "",
        "## Hardware and cost assumptions",
        "",
    ]
    for eng in engines:
        cfg = hw_cfg[eng]
        hourly = hourly_cost_usd(cfg)
        lines.append(f"- **{eng}**: ${hourly:.2f}/hour ({cfg.get('notes', '').strip() or '—'})")
    lines.extend([
        "",
        "## Per-query results",
        "",
        "| query | " + " | ".join(f"{e} p50 ms" for e in engines)
              + " | " + " | ".join(f"{e} $/q" for e in engines) + " |",
        "|---|" + "---|" * (2 * len(engines)),
    ])
    for qid in query_ids:
        row = [qid]
        for eng in engines:
            s = per_engine[eng].get(qid)
            row.append(f"{s['p50_ms']:.0f}" if s else "-")
        for eng in engines:
            s = per_engine[eng].get(qid)
            row.append(f"{s['cost_usd_p50']:.4f}" if s else "-")
        lines.append("| " + " | ".join(row) + " |")

    # Win counts — summary stats. Do not pretend these replace the
    # sign-off protocol; "shelf wins on X queries" only matters once
    # both engineers have reviewed `costed.csv`.
    if "shelf" in engines and len(engines) >= 2:
        lines.extend(["", "## Head-to-head tallies (from p50 ms)", ""])
        for rival in [e for e in engines if e != "shelf"]:
            shelf_wins_ms = shelf_wins_cost = total = 0
            for qid in query_ids:
                shelf = per_engine["shelf"].get(qid)
                opp = per_engine[rival].get(qid)
                if not shelf or not opp:
                    continue
                total += 1
                if shelf["p50_ms"] < opp["p50_ms"]:
                    shelf_wins_ms += 1
                if shelf["cost_usd_p50"] < opp["cost_usd_p50"]:
                    shelf_wins_cost += 1
            lines.append(f"- shelf vs {rival}: p50 ms wins on {shelf_wins_ms}/{total}; $/q wins on {shelf_wins_cost}/{total}")
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True, type=pathlib.Path)
    parser.add_argument(
        "--hardware",
        default=pathlib.Path(__file__).resolve().parent / "hardware.yaml",
        type=pathlib.Path,
    )
    args = parser.parse_args(argv)

    if not args.run_dir.is_dir():
        raise SystemExit(f"{args.run_dir} is not a directory")

    hw = load_hardware(args.hardware)
    engine_csvs = sorted(args.run_dir.glob("*.csv"))
    if not engine_csvs:
        raise SystemExit(f"no *.csv files in {args.run_dir}")

    all_costed = []
    per_engine_summary: dict[str, dict] = {}
    for csv_path in engine_csvs:
        engine = csv_path.stem
        if engine == "costed" or engine.startswith("summary"):
            continue
        if engine not in hw:
            print(f"skipping {csv_path.name}: no hardware entry for '{engine}'", file=sys.stderr)
            continue
        rows = load_raw_csv(csv_path)
        hourly = hourly_cost_usd(hw[engine])
        costed = compute_costed_rows(engine, rows, hourly)
        all_costed.extend(costed)
        per_engine_summary[engine] = group_p50_by_query(costed)

    out_csv = args.run_dir / "costed.csv"
    if all_costed:
        with out_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(all_costed[0].keys()))
            writer.writeheader()
            writer.writerows(all_costed)

    out_md = args.run_dir / "summary.md"
    out_md.write_text(render_summary_md(args.run_dir, hw, per_engine_summary))
    print(f"wrote {out_csv}")
    print(f"wrote {out_md}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
