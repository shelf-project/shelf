"""SHELF-35 replay harness CLI.

Usage
-----

::

    # Synthetic smoke (no Trino dependency, deterministic):
    python -m tools.replay.main \\
        --synthetic \\
        --capacity-mb 14000 \\
        --policies lru,fifo,s3fifo,belady \\
        --output agents/out/SHELF-35/replay-$(date +%F).tsv

    # Real production trace (operator exports CSV from Trino first):
    python -m tools.replay.main \\
        --trace /tmp/trace_30d.csv \\
        --capacity-mb 14000 \\
        --policies lru,fifo,s3fifo,belady \\
        --output agents/out/SHELF-35/replay-$(date +%F).tsv

The harness is hermetic: same input CSV + same ``--seed`` (synthetic
mode) + same policy set ⇒ byte-identical TSV. Operators check this
into ``agents/out/SHELF-35/`` so a future agent can diff against an
older run.

Capacities can be swept by repeating ``--capacity-mb``:

::

    --capacity-mb 1000 --capacity-mb 5000 --capacity-mb 14000

— one TSV row per ``(policy, capacity)`` pair.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

from .policies import build_policy
from .simulator import simulate, write_tsv_row
from .trace import Access, load_from_trino_csv, load_synthetic


def _parse_capacities(values: List[str]) -> List[int]:
    out: List[int] = []
    for v in values:
        n = int(v)
        if n <= 0:
            raise argparse.ArgumentTypeError(f"capacity must be > 0 MB; got {v}")
        out.append(n * 1024 * 1024)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="shelf.tools.replay",
        description="SHELF-35 — Belady oracle replay harness over Trino traces.",
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--trace",
        help="Path to a Trino-exported trace CSV. See "
        "tools/replay/sql/extract_trace_30d.sql for the schema.",
    )
    src.add_argument(
        "--synthetic",
        action="store_true",
        help="Use a deterministic Zipfian synthetic trace. For unit tests + smoke.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Synthetic-mode RNG seed (default 0).",
    )
    parser.add_argument(
        "--n-queries",
        type=int,
        default=10_000,
        help="Synthetic-mode query count (default 10_000).",
    )
    parser.add_argument(
        "--n-tables",
        type=int,
        default=200,
        help="Synthetic-mode distinct-table count (default 200).",
    )
    parser.add_argument(
        "--policies",
        default="lru,fifo,s3fifo,belady",
        help="Comma-separated list of policy names (default: lru,fifo,s3fifo,belady).",
    )
    parser.add_argument(
        "--capacity-mb",
        action="append",
        default=None,
        help="Capacity in MiB. Repeatable; one TSV row per (policy, capacity). "
        "Default: 14000 (= 14 GiB DRAM target per shelf pod, workspace memory).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output TSV path. Created if absent; schema is in tools/replay/simulator.py.",
    )
    args = parser.parse_args(argv)

    capacities = _parse_capacities(args.capacity_mb or ["14000"])
    policy_names = [p.strip() for p in args.policies.split(",") if p.strip()]
    if not policy_names:
        parser.error("--policies must list at least one policy")

    if args.synthetic:
        trace: list[Access] = load_synthetic(
            seed=args.seed,
            n_queries=args.n_queries,
            n_tables=args.n_tables,
        )
    else:
        trace = load_from_trino_csv(args.trace)
    print(f"loaded {len(trace)} accesses across {len({a.object_id for a in trace})} keys", file=sys.stderr)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    is_first = True
    for policy_name in policy_names:
        for capacity in capacities:
            policy = build_policy(policy_name, trace)
            stats = simulate(trace, policy, capacity)
            write_tsv_row(str(out_path), stats, header=is_first)
            is_first = False
            print(
                f"policy={stats.policy:<8} capacity_mb={capacity // (1024 * 1024):>6} "
                f"hit_ratio={stats.hit_ratio:.4f} byte_miss_ratio={stats.byte_miss_ratio:.4f} "
                f"bypassed={stats.bypassed:>6} evictions={stats.evictions:>8}",
                file=sys.stderr,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
