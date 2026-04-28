#!/usr/bin/env python3
"""F4 — regression gate check.

Compares a fresh shelf-only SF100 run CSV (output of `runner/run.py`)
against a committed baseline (`baseline/shelf-sf100.csv`). Fails the
release if p50 wall-clock for any query regresses more than a
per-query tolerance.

The default tolerance is 10 % per query. A stricter 5 % applies to
the "parity" set — queries that Track E claims wins on — so we
catch quiet regressions on the queries we've committed to winning.

Absent baseline or absent results for a query emit a warning, not
a failure. This is deliberate: the first run establishes the
baseline, and a dropped query from the upstream `bootstrap.sh`
shouldn't brick the gate.
"""

from __future__ import annotations

import argparse
import csv
import pathlib
import statistics
import sys


# Queries Track E claims wins on. Updated as the harness fills in.
PARITY_QUERIES = frozenset([
    "q03", "q07", "q20", "q40", "q55",
])


def load_medians(path: pathlib.Path) -> dict[str, float]:
    by_query: dict[str, list[float]] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            if row.get("engine") not in (None, "", "shelf"):
                continue
            try:
                ms = float(row["elapsed_ms"])
            except (KeyError, ValueError):
                continue
            by_query.setdefault(row["query_id"], []).append(ms)
    return {q: statistics.median(v) for q, v in by_query.items()}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True, type=pathlib.Path)
    parser.add_argument("--candidate", required=True, type=pathlib.Path)
    parser.add_argument("--tolerance", type=float, default=0.10,
                        help="allowed regression ratio for non-parity queries (default 10%)")
    parser.add_argument("--parity-tolerance", type=float, default=0.05,
                        help="allowed regression ratio for parity queries (default 5%)")
    args = parser.parse_args(argv)

    if not args.baseline.is_file():
        print("warn: no baseline file; skipping gate", file=sys.stderr)
        return 0
    baseline = load_medians(args.baseline)
    if not baseline:
        print("warn: baseline has no rows; skipping gate", file=sys.stderr)
        return 0

    candidate = load_medians(args.candidate)
    if not candidate:
        print("error: candidate CSV has no shelf rows", file=sys.stderr)
        return 2

    failures = 0
    for qid, base in sorted(baseline.items()):
        cand = candidate.get(qid)
        if cand is None:
            print(f"warn: {qid} missing from candidate; skipping", file=sys.stderr)
            continue
        ratio = (cand - base) / max(base, 1e-6)
        tolerance = args.parity_tolerance if qid in PARITY_QUERIES else args.tolerance
        if ratio > tolerance:
            failures += 1
            print(
                f"regression: {qid} {cand:.0f}ms vs baseline {base:.0f}ms "
                f"(+{ratio * 100:.1f}%, allowed +{tolerance * 100:.1f}%)",
                file=sys.stderr,
            )
        else:
            sign = "+" if ratio >= 0 else ""
            print(f"ok: {qid} {cand:.0f}ms vs baseline {base:.0f}ms ({sign}{ratio * 100:.1f}%)")

    if failures:
        print(f"FAIL: {failures} regressions detected", file=sys.stderr)
        return 1
    print(f"PASS: {len(candidate)} queries within tolerance")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
