"""Cache hit-rate simulator over scanner output.

Replays the ordered ``(content_key, size)`` stream through a pure-
Python Foyer-equivalent:

- **LRU** eviction backed by :class:`collections.OrderedDict` — the
  simplest policy that matches v0 Foyer DRAM behaviour.
- **Size-threshold** admission per SHELF-25: reject inserts larger
  than ``size_threshold_bytes`` unless the key is in the pin list.
- **Pin list** bypasses the size threshold and is never evicted.

The output is a :class:`SimResult` with cumulative hit-rate curves
sampled at 10-second intervals in simulated replay time (wall-clock
offset of each query mapped through ``speed`` = 1× for the offline
analysis).
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

from .key import content_key
from .types import ScanResult, TraceEntry


@dataclass(frozen=True)
class SimConfig:
    name: str
    capacity_bytes: int = 512 * (1 << 30)  # 512 GiB
    policy: str = "lru"  # or "size-only"
    size_threshold_bytes: int = 1 << 30  # 1 GiB
    pinned_bypass: bool = True
    pin_list: frozenset[str] = frozenset()


@dataclass
class SimResult:
    config: SimConfig
    hits: int = 0
    misses: int = 0
    bytes_hit: int = 0
    bytes_miss: int = 0
    admitted_bytes: int = 0
    rejected_by_threshold: int = 0
    evicted_bytes: int = 0
    # (timestamp_iso, cumulative_hit_rate)
    hit_rate_curve: list[tuple[str, float]] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        t = self.total
        return self.hits / t if t else 0.0


class _LruCache:
    __slots__ = ("_entries", "_used", "_capacity", "_pinned")

    def __init__(self, capacity_bytes: int, pinned: frozenset[str]):
        self._entries: "OrderedDict[str, int]" = OrderedDict()
        self._used = 0
        self._capacity = capacity_bytes
        self._pinned = pinned

    def touch(self, key: str) -> bool:
        if key in self._entries:
            self._entries.move_to_end(key)
            return True
        return False

    def insert(self, key: str, size: int) -> int:
        evicted = 0
        while self._used + size > self._capacity and self._entries:
            oldest_key = next(iter(self._entries))
            if oldest_key in self._pinned:
                # Cannot evict pinned — move to end to simulate exemption.
                self._entries.move_to_end(oldest_key)
                if all(k in self._pinned for k in self._entries):
                    break
                continue
            oldest_size = self._entries.pop(oldest_key)
            self._used -= oldest_size
            evicted += oldest_size
        self._entries[key] = size
        self._used += size
        return evicted


def simulate(
    paired: Iterable[tuple[TraceEntry, ScanResult]],
    config: SimConfig,
    curve_bucket_seconds: int = 10,
) -> SimResult:
    """Replay one config across the (entry, scan) stream in trace order."""

    result = SimResult(config=config)
    cache = _LruCache(config.capacity_bytes, config.pin_list)

    last_bucket: int | None = None
    for entry, scan in paired:
        ts = int(entry.query_date.timestamp())
        bucket = ts - (ts % curve_bucket_seconds) if ts > 0 else 0

        for (_path, rg_ord, offset, size, etag) in scan.rg_entries:
            key = content_key(etag, rg_ord, offset, size)
            if cache.touch(key):
                result.hits += 1
                result.bytes_hit += size
                continue

            result.misses += 1
            result.bytes_miss += size

            if _admit(key, size, config):
                evicted = cache.insert(key, size)
                result.admitted_bytes += size
                result.evicted_bytes += evicted
            else:
                result.rejected_by_threshold += 1

        if last_bucket is None or bucket != last_bucket:
            if ts > 0:
                iso = datetime.fromtimestamp(bucket, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                result.hit_rate_curve.append((iso, result.hit_rate))
            last_bucket = bucket

    return result


def _admit(key: str, size: int, config: SimConfig) -> bool:
    if key in config.pin_list and config.pinned_bypass:
        return True
    if config.policy == "size-only":
        return size <= config.size_threshold_bytes
    # "lru" policy admits everything under the threshold; size-threshold
    # admission is the same gate for both policies in v0.
    return size <= config.size_threshold_bytes
