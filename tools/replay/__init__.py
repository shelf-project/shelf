"""SHELF-35 — Belady oracle replay harness.

Drives algorithm-comparison experiments over Trino query traces sourced
from ``your_query_log_table``. Produces per-algorithm hit-ratio,
byte-miss-ratio, and the Belady-MIN upper bound — the precondition for
every learned-policy lever (SHELF-26 / SHELF-31 / SHELF-32 / SHELF-33 /
SHELF-36) per the Shelf algorithmic optimization roadmap.

Public API
----------

>>> from shelf.tools.replay import simulate, LRU, BeladyMin, load_synthetic
>>> trace = load_synthetic(seed=0, n_queries=1000)
>>> stats = simulate(trace, LRU(), capacity_bytes=10 * 1024 * 1024)
>>> stats.hit_ratio   # noqa: F841
0.42  # for example

The CLI (``python -m shelf.tools.replay.main ...``) emits one TSV row
per (algorithm, capacity) tuple under
``agents/out/SHELF-35/replay-<algo>-<date>.tsv`` per the plan's
"Output frozen per algorithm" rule.
"""

from .policies import (  # noqa: F401
    BeladyMin,
    FIFO,
    LRU,
    Policy,
    S3FIFO,
)
from .simulator import SimStats, simulate  # noqa: F401
from .trace import Access, load_from_trino_csv, load_synthetic  # noqa: F401

__all__ = [
    "Access",
    "BeladyMin",
    "FIFO",
    "LRU",
    "Policy",
    "S3FIFO",
    "SimStats",
    "load_from_trino_csv",
    "load_synthetic",
    "simulate",
]
