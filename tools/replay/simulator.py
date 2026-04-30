"""Cache simulator core for SHELF-35.

The simulator is intentionally tiny: walk the trace, ask the policy on
each access, maintain a ``cached: Dict[key, size_bytes]`` map, and
track count + byte-level stats. No threading, no I/O — the inner loop
is hot enough on a 30-day trace (~1 M accesses) that anything else
would be a needless complication.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from .policies import BeladyMin, Policy
from .trace import Access


@dataclass
class SimStats:
    """Per-policy outcome summary.

    All counts are integer; ratios are derived properties so the TSV
    writer and the CLI both see the same canonical numbers.
    """

    policy: str
    capacity_bytes: int
    accesses: int = 0
    hits: int = 0
    misses: int = 0
    bytes_requested: int = 0
    bytes_hit: int = 0
    bytes_miss: int = 0
    bypassed: int = 0
    """Accesses where the object exceeded capacity (admit returned
    False). Counted as misses by hit-ratio but tracked separately so
    operators can spot pathological size distributions."""
    evictions: int = 0
    bytes_evicted: int = 0

    @property
    def hit_ratio(self) -> float:
        return self.hits / self.accesses if self.accesses else 0.0

    @property
    def byte_hit_ratio(self) -> float:
        return self.bytes_hit / self.bytes_requested if self.bytes_requested else 0.0

    @property
    def byte_miss_ratio(self) -> float:
        return self.bytes_miss / self.bytes_requested if self.bytes_requested else 0.0


def simulate(
    accesses: Sequence[Access],
    policy: Policy,
    capacity_bytes: int,
) -> SimStats:
    """Run the trace through `policy` against a fixed-capacity cache.

    Returns the per-policy [`SimStats`][.]. Both the count- and
    byte-level numbers are populated so the harness can answer the
    "8× origin-bandwidth" framing the plan attributes to Vimeo /
    Varnish — that's the byte view, distinct from the count view that
    most academic papers report.
    """
    policy.reset()
    cached: Dict[str, int] = {}
    used: int = 0
    stats = SimStats(policy=policy.name, capacity_bytes=capacity_bytes)
    is_belady = isinstance(policy, BeladyMin)

    for idx, access in enumerate(accesses):
        if is_belady:
            policy.step(idx)  # type: ignore[attr-defined]
        stats.accesses += 1
        stats.bytes_requested += access.size_bytes
        if access.object_id in cached:
            stats.hits += 1
            stats.bytes_hit += access.size_bytes
            policy.on_hit(access)
            continue
        stats.misses += 1
        stats.bytes_miss += access.size_bytes
        if not policy.admit(access, cached, capacity_bytes):
            stats.bypassed += 1
            continue
        # Evict until the new access fits.
        while used + access.size_bytes > capacity_bytes:
            victim = policy.select_victim(cached)
            if victim is None:
                # Policy refused to evict — bypass admission.
                break
            v_size = cached.pop(victim)
            used -= v_size
            stats.evictions += 1
            stats.bytes_evicted += v_size
            policy.on_evict(victim)
        if used + access.size_bytes > capacity_bytes:
            stats.bypassed += 1
            continue
        cached[access.object_id] = access.size_bytes
        used += access.size_bytes
        policy.on_admit(access)

    return stats


def write_tsv_row(path: str, stats: SimStats, *, header: bool = False) -> None:
    """Append (or create) a TSV row per [`SimStats`][.].

    Schema:

        policy\\tcapacity_bytes\\taccesses\\thits\\tmisses\\tbytes_requested\\
        bytes_hit\\tbytes_miss\\tbypassed\\tevictions\\tbytes_evicted\\thit_ratio\\
        byte_hit_ratio\\tbyte_miss_ratio

    The ``hit_ratio`` / ``byte_hit_ratio`` / ``byte_miss_ratio`` columns
    are formatted to 6 decimals so two TSVs differing in the 7th
    decimal still diff cleanly under ``diff -u``.
    """
    cols = (
        "policy",
        "capacity_bytes",
        "accesses",
        "hits",
        "misses",
        "bytes_requested",
        "bytes_hit",
        "bytes_miss",
        "bypassed",
        "evictions",
        "bytes_evicted",
        "hit_ratio",
        "byte_hit_ratio",
        "byte_miss_ratio",
    )
    row = (
        stats.policy,
        stats.capacity_bytes,
        stats.accesses,
        stats.hits,
        stats.misses,
        stats.bytes_requested,
        stats.bytes_hit,
        stats.bytes_miss,
        stats.bypassed,
        stats.evictions,
        stats.bytes_evicted,
        f"{stats.hit_ratio:.6f}",
        f"{stats.byte_hit_ratio:.6f}",
        f"{stats.byte_miss_ratio:.6f}",
    )
    mode = "w" if header else "a"
    with open(path, mode, encoding="utf-8") as fh:
        if header:
            fh.write("\t".join(cols) + "\n")
        fh.write("\t".join(str(c) for c in row) + "\n")
