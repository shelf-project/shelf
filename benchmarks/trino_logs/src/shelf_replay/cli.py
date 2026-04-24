"""``shelf-replay`` entry point.

Subcommands
-----------

``analyze``
    Produce ``per-query.csv``, ``per-day.csv`` (E5 ratios), and
    ``summary.json``. Reads the trace + manifest directory only; does
    not simulate.

``simulate``
    Run a matrix of cache configs against the same trace and emit one
    ``sim-<name>.csv`` per config plus the full ``summary.json``.

``replay-rep2-7d``
    Convenience target. Runs both subcommands against the committed
    synthetic fixture and asserts the E5 ratios match the golden
    values. This is what ``make replay-rep2-7d`` drives.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import __version__
from .aggregate import aggregate_by_day
from .manifest import ManifestIndex
from .report import (
    write_per_day_csv,
    write_per_query_csv,
    write_sim_csv,
    write_summary_json,
)
from .scanner import clear_footer_cache, scan_all
from .simulate import SimConfig, simulate
from .trace import load_trace


def _cmd_analyze(args: argparse.Namespace) -> int:
    trace = load_trace(args.trace)
    manifest_index = ManifestIndex.load(args.manifest_dir)
    scans = scan_all(trace, manifest_index)
    aggregates = aggregate_by_day(scans)
    out = Path(args.out)
    write_per_query_csv(out, trace, scans)
    write_per_day_csv(out, aggregates)
    write_summary_json(out, trace, scans, aggregates, sims=[])
    if not args.quiet:
        _print_e5(aggregates)
    return 0


def _cmd_simulate(args: argparse.Namespace) -> int:
    trace = load_trace(args.trace)
    manifest_index = ManifestIndex.load(args.manifest_dir)
    scans = scan_all(trace, manifest_index)
    aggregates = aggregate_by_day(scans)
    configs = _load_sim_configs(args.configs)
    out = Path(args.out)
    sims = [simulate(zip(trace, scans), cfg) for cfg in configs]
    for sim in sims:
        write_sim_csv(out, sim)
    write_per_query_csv(out, trace, scans)
    write_per_day_csv(out, aggregates)
    write_summary_json(out, trace, scans, aggregates, sims)
    if not args.quiet:
        _print_e5(aggregates)
        _print_sim(sims)
    return 0


def _cmd_replay_rep2_7d(args: argparse.Namespace) -> int:
    fixture = Path(args.fixture)
    trace_path = fixture / "trace.jsonl"
    manifest_dir = fixture / "manifests"
    configs_path = fixture / "sim-configs.json"
    expected_path = fixture / "expected.json"
    out = Path(args.out)

    trace = load_trace(trace_path)
    manifest_index = ManifestIndex.load(manifest_dir)
    clear_footer_cache()
    scans = scan_all(trace, manifest_index)
    aggregates = aggregate_by_day(scans)

    configs = _load_sim_configs(configs_path) if configs_path.exists() else []
    sims = [simulate(zip(trace, scans), cfg) for cfg in configs]

    write_per_query_csv(out, trace, scans)
    write_per_day_csv(out, aggregates)
    for sim in sims:
        write_sim_csv(out, sim)
    write_summary_json(out, trace, scans, aggregates, sims)

    if expected_path.exists():
        if not _verify_expected(expected_path, aggregates):
            return 1
    if not args.quiet:
        _print_e5(aggregates)
        _print_sim(sims)
        print(f"\n[OK] wrote {out}/per-query.csv, per-day.csv, summary.json")
    return 0


def _verify_expected(path: Path, aggregates: list) -> bool:
    with path.open("r", encoding="utf-8") as fh:
        expected = json.load(fh)
    tolerance = float(expected.get("tolerance", 1e-6))
    by_day = {a.day: a for a in aggregates}
    ok = True
    for day_entry in expected.get("per_day", []):
        day = day_entry["day"]
        got = by_day.get(day)
        if got is None:
            print(f"[FAIL] missing day in output: {day}", file=sys.stderr)
            ok = False
            continue
        for field in ("median_ratio", "p90_ratio", "overall_ratio"):
            want = float(day_entry.get(field, -1))
            if want < 0:
                continue
            have = float(getattr(got, field))
            if abs(have - want) > tolerance:
                print(
                    f"[FAIL] {day}.{field}: want {want:.6f} got {have:.6f}",
                    file=sys.stderr,
                )
                ok = False
    return ok


def _load_sim_configs(path: str | Path) -> list[SimConfig]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"sim configs not found: {p}")
    with p.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)
    configs: list[SimConfig] = []
    for c in raw.get("configs", []):
        configs.append(
            SimConfig(
                name=c["name"],
                capacity_bytes=int(c.get("capacity_bytes", 512 * (1 << 30))),
                policy=str(c.get("policy", "lru")),
                size_threshold_bytes=int(c.get("size_threshold_bytes", 1 << 30)),
                pinned_bypass=bool(c.get("pinned_bypass", True)),
                pin_list=frozenset(c.get("pin_list", [])),
            )
        )
    return configs


def _print_e5(aggregates) -> None:
    print("E5 — median / P90 rg/file ratio per day")
    print(f"  {'day':<12} {'queries':>8} {'median':>10} {'P90':>10} {'overall':>10}")
    for a in aggregates:
        print(
            f"  {a.day:<12} {a.query_count:>8} "
            f"{a.median_ratio:>10.4f} {a.p90_ratio:>10.4f} {a.overall_ratio:>10.4f}"
        )


def _print_sim(sims) -> None:
    if not sims:
        return
    print("\nSimulations")
    print(f"  {'config':<24} {'hits':>8} {'misses':>8} {'hit-rate':>10}")
    for s in sims:
        print(
            f"  {s.config.name:<24} {s.hits:>8} {s.misses:>8} {s.hit_rate:>10.4f}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="shelf-replay",
        description="Offline analysis harness for cdp.trino_logs.trino_queries (SHELF-26).",
    )
    parser.add_argument("-V", "--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("-q", "--quiet", action="store_true")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_an = sub.add_parser("analyze", help="Produce per-query + per-day CSVs (E5).")
    p_an.add_argument("--trace", required=True)
    p_an.add_argument("--manifest-dir", required=True)
    p_an.add_argument("--out", required=True)
    p_an.set_defaults(func=_cmd_analyze)

    p_sim = sub.add_parser("simulate", help="Run a matrix of cache configs.")
    p_sim.add_argument("--trace", required=True)
    p_sim.add_argument("--manifest-dir", required=True)
    p_sim.add_argument("--configs", required=True)
    p_sim.add_argument("--out", required=True)
    p_sim.set_defaults(func=_cmd_simulate)

    p_rp = sub.add_parser(
        "replay-rep2-7d",
        help="Run analyze + simulate against a fixture and verify expected numbers.",
    )
    p_rp.add_argument("--fixture", required=True)
    p_rp.add_argument("--out", required=True)
    p_rp.set_defaults(func=_cmd_replay_rep2_7d)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
