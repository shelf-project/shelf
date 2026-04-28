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
from .prewarm import run_prewarm
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

    p_pw = sub.add_parser(
        "prewarm",
        help=(
            "Online prewarm: issue real range-GETs to a running shelfd "
            "for every unique row-group byte-range the trace touched. "
            "Used as a rollout pre-req (see docs/rollout-v1.md §4)."
        ),
    )
    p_pw.add_argument("--trace", required=True, help="Path to per-replica trace file.")
    p_pw.add_argument("--manifest-dir", required=True, help="Iceberg manifest dir for the trace window.")
    p_pw.add_argument("--endpoint", required=True, help="shelfd S3 shim URL, e.g. http://shelfd:9092")
    p_pw.add_argument("--bucket", required=True, help="Iceberg warehouse bucket name.")
    p_pw.add_argument("--replica", required=True, help="Replica tag for result JSON (rep-0/1/2/3).")
    p_pw.add_argument("--concurrency", type=int, default=32)
    p_pw.add_argument("--limit", type=int, default=None, help="Cap issued requests; for dry runs.")
    p_pw.add_argument("--timeout", type=float, default=10.0, help="Per-request timeout in seconds.")
    p_pw.add_argument("--out", default=None, help="Optional JSON summary output path.")
    p_pw.set_defaults(func=_cmd_prewarm)

    args = parser.parse_args(argv)
    return int(args.func(args))


def _cmd_prewarm(args: argparse.Namespace) -> int:
    report = run_prewarm(
        trace_path=args.trace,
        manifest_dir=args.manifest_dir,
        endpoint=args.endpoint,
        bucket=args.bucket,
        replica=args.replica,
        concurrency=args.concurrency,
        limit=args.limit,
        per_request_timeout=args.timeout,
    )
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        from dataclasses import asdict

        out_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")
    if not args.quiet:
        hr = report.hit_ratio_from_outcomes
        print(
            f"[OK] prewarm {report.replica}: "
            f"{report.successes}/{report.requests_issued} success "
            f"({report.success_ratio:.1%}); "
            f"hit-ratio from outcomes = {'n/a' if hr is None else f'{hr:.3f}'}; "
            f"elapsed {report.elapsed_s:.1f}s"
        )
    # Rollout runbook: success_ratio < 0.95 is a warning; < 0.50 is a
    # "do not proceed with cutover" signal. We surface this via exit
    # code so the Makefile target can gate on it.
    if report.success_ratio < 0.50:
        return 2
    if report.success_ratio < 0.95:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
