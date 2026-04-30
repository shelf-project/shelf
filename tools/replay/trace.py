"""Trace loaders for the SHELF-35 replay harness.

Trace shape
-----------

The simulator consumes a flat list of [`Access`][.] tuples ordered by
ascending timestamp. Each access represents a Trino query reading a
specific cacheable object. Because ``your_query_log_table`` does
**not** record per-split paths (Trino's ``SplitCompletedEvent`` was
removed upstream in PR #26436, merged 2025-08-19, per ADR-0005), the
honest v1 trace operates at **(query, table_fqn)** granularity:

    object_id = "<catalog>.<schema>.<table>"
    size_bytes = physicalInputBytes for that (query, table) pair

This is coarser than the file-level granularity Belady-MIN would model
ideally, but it is the only granularity ``trino_queries`` honestly
supports. SHELF-35b (file-level synthesis on top of an Iceberg
``$files`` join) is the documented upgrade path; the v1 simulator
results are still useful for differentiating LRU / FIFO / Sieve /
W-TinyLFU at table-level granularity, which is where the dominant
production hot-key skew lives anyway (see workspace memory: rep-2
Metabase queries hashing to a single shelf pod).

Loaders
-------

Two paths are supported:

1. ``load_from_trino_csv`` — production path. The operator runs the
   SQL in ``sql/extract_trace_30d.sql`` against Trino and exports the
   result as CSV. The harness consumes that CSV without needing live
   Trino access — keeps the simulator hermetic for unit tests, CI, and
   replay reproducibility (the same CSV always produces the same
   stats).

2. ``load_synthetic`` — deterministic Zipfian generator for unit
   tests + smoke. Models a heavy-tailed access distribution over a
   configurable number of tables and queries. Seeded for repro.
"""
from __future__ import annotations

import csv
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


@dataclass(frozen=True)
class Access:
    """One cacheable object access in the replay trace.

    Attributes
    ----------
    timestamp_ms
        IST-converted millisecond timestamp. Trino's ``query_date`` is
        UTC; the loader converts to IST so per-day rollups align with
        the workspace's IST-default reporting convention.
    object_id
        Stable identifier the simulator uses as the cache key. v1 =
        ``<catalog>.<schema>.<table>``; v2 (future, post-SHELF-35b)
        will be ``<s3_path>#<row_group_ordinal>``.
    size_bytes
        Object size in bytes. For the v1 (table, query) granularity
        this is ``physicalInputBytes`` for that pair — i.e., how much
        the query actually read of that table after predicate
        pushdown.
    query_id
        Optional Trino query id; useful for joining back against the
        per-query latency histogram when computing the p99 model.
    """

    timestamp_ms: int
    object_id: str
    size_bytes: int
    query_id: str | None = None


def load_from_trino_csv(path: str | Path) -> List[Access]:
    """Read a Trino-exported CSV trace.

    Expected header (case-insensitive):

        ``timestamp_ms,object_id,size_bytes,query_id``

    ``query_id`` is optional — empty values are treated as ``None``.
    Rows are returned in input order; the caller is expected to have
    sorted by ``timestamp_ms`` in the source query (the SQL in
    ``sql/extract_trace_30d.sql`` does so).

    Returns an empty list if the file is empty.
    """
    rows: List[Access] = []
    p = Path(path)
    with p.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            # DictReader's keys are the header row; lowercase to be
            # forgiving about column-naming style across exports.
            row = {(k or "").strip().lower(): v for k, v in raw.items()}
            if not row.get("object_id"):
                # Skip empty trailing rows / malformed lines.
                continue
            rows.append(
                Access(
                    timestamp_ms=int(row["timestamp_ms"]),
                    object_id=row["object_id"],
                    size_bytes=int(row["size_bytes"]),
                    query_id=row.get("query_id") or None,
                )
            )
    return rows


def load_synthetic(
    seed: int = 0,
    n_queries: int = 10_000,
    n_tables: int = 200,
    zipf_alpha: float = 1.1,
    base_size_bytes: int = 10 * 1024 * 1024,
    size_jitter: float = 0.5,
) -> List[Access]:
    """Generate a deterministic synthetic trace for tests + smoke.

    Models a Zipfian-distributed table popularity (``zipf_alpha`` ~ 1.1
    matches the empirical heavy-tail observed in production rep-2 +
    rep-1 traffic — workspace memory: a handful of tables drive the
    bulk of physicalInputBytes). Sizes are jittered around
    ``base_size_bytes`` to model real Iceberg ``optimize_data_files``
    output (target file size ~128 MiB, but tables vary by 0.5×–2×).

    The trace is reproducible: ``load_synthetic(seed=0, ...) ==
    load_synthetic(seed=0, ...)`` byte-for-byte.
    """
    rng = random.Random(seed)

    # Zipfian probabilities over the n_tables population.
    weights = [1.0 / (i**zipf_alpha) for i in range(1, n_tables + 1)]
    total = sum(weights)
    probs = [w / total for w in weights]
    # Pre-build the cumulative distribution so we sample by bisect.
    cum = []
    acc = 0.0
    for p in probs:
        acc += p
        cum.append(acc)

    table_sizes = {
        f"cdp.bench.t_{i:04d}": max(
            1,
            int(base_size_bytes * (1.0 + rng.uniform(-size_jitter, size_jitter))),
        )
        for i in range(n_tables)
    }

    accesses: List[Access] = []
    t = 0
    for q in range(n_queries):
        # Sample a table by Zipf cumulative.
        u = rng.random()
        # Binary search the cumulative table.
        lo, hi = 0, n_tables - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if u <= cum[mid]:
                hi = mid
            else:
                lo = mid + 1
        table = f"cdp.bench.t_{lo:04d}"
        # Inter-arrival jitter ~ exp(mean=1s); deterministic via rng.
        gap_ms = max(1, int(-math.log(max(rng.random(), 1e-6)) * 1000))
        t += gap_ms
        accesses.append(
            Access(
                timestamp_ms=t,
                object_id=table,
                size_bytes=table_sizes[table],
                query_id=f"q_{q:08d}",
            )
        )
    return accesses


def write_csv(rows: Iterable[Access], path: str | Path) -> None:
    """Write a trace to CSV. Inverse of [`load_from_trino_csv`]."""
    p = Path(path)
    with p.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["timestamp_ms", "object_id", "size_bytes", "query_id"])
        for r in rows:
            w.writerow([r.timestamp_ms, r.object_id, r.size_bytes, r.query_id or ""])
