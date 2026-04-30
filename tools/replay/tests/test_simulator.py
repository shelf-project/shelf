"""Unit tests for the SHELF-35 simulator core.

These tests are independent of any live Trino / S3 / cluster
infrastructure — they only require Python's stdlib + the modules in
``tools/replay/``. Run with ``python -m unittest -v tools.replay.tests``.
"""
from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tools.replay.policies import LRU, FIFO, S3FIFO, BeladyMin, build_policy
from tools.replay.simulator import simulate, write_tsv_row
from tools.replay.trace import Access, load_synthetic, load_from_trino_csv, write_csv


def _trace(*pairs: tuple[str, int]) -> list[Access]:
    """Convenience: list[(object_id, size_bytes)] -> list[Access]."""
    return [
        Access(timestamp_ms=i, object_id=oid, size_bytes=sz, query_id=f"q{i}")
        for i, (oid, sz) in enumerate(pairs)
    ]


class TestLRU(unittest.TestCase):
    def test_warm_then_evict(self) -> None:
        # Capacity holds two items; third forces eviction of A (LRU).
        trace = _trace(("A", 10), ("B", 10), ("A", 10), ("C", 10), ("B", 10), ("A", 10))
        stats = simulate(trace, LRU(), capacity_bytes=20)
        # A miss B miss A hit C miss(evict B) B miss(evict A) A miss(evict C)
        self.assertEqual(stats.misses, 5)
        self.assertEqual(stats.hits, 1)
        self.assertEqual(stats.evictions, 3)

    def test_promotion_on_hit_changes_victim(self) -> None:
        # If A is touched between B's insert and the eviction, B becomes
        # the LRU victim — proving promotion-on-hit works.
        trace = _trace(("A", 10), ("B", 10), ("A", 10), ("C", 10))
        stats = simulate(trace, LRU(), capacity_bytes=20)
        # A miss; B miss; A hit (LRU is now B); C miss → evicts B.
        self.assertEqual(stats.misses, 3)
        self.assertEqual(stats.hits, 1)
        self.assertEqual(stats.evictions, 1)


class TestFIFO(unittest.TestCase):
    def test_no_promotion_on_hit(self) -> None:
        # Identical trace to TestLRU.test_promotion_on_hit_changes_victim
        # but FIFO ignores the A re-touch — it still evicts A first.
        trace = _trace(("A", 10), ("B", 10), ("A", 10), ("C", 10), ("B", 10))
        stats = simulate(trace, FIFO(), capacity_bytes=20)
        # A miss; B miss; A hit; C miss → evicts A; B hit.
        self.assertEqual(stats.misses, 3)
        self.assertEqual(stats.hits, 2)


class TestS3FIFO(unittest.TestCase):
    def test_promotion_via_freq(self) -> None:
        # Promotion threshold = 1: a re-touch promotes A from small to
        # main, so the next eviction lands on B (still in small-q).
        trace = _trace(("A", 10), ("B", 10), ("A", 10), ("C", 10))
        stats = simulate(trace, S3FIFO(promotion_threshold=1), capacity_bytes=20)
        # A miss; B miss; A hit (freq=1, promotes); C miss → evicts B.
        self.assertEqual(stats.misses, 3)
        self.assertEqual(stats.hits, 1)


class TestBeladyMin(unittest.TestCase):
    def test_evicts_furthest_future(self) -> None:
        # B is touched again at idx 5 (far); A at idx 4 (near). On the
        # eviction at idx 3 (C miss, capacity 2), Belady should evict B.
        trace = _trace(
            ("A", 10),  # idx 0
            ("B", 10),  # idx 1
            # C miss at idx 2 forces an eviction. A's next is idx 3 ; B's
            # next is idx 5. Belady evicts B (furthest).
            ("C", 10),  # idx 2
            ("A", 10),  # idx 3 (hit if we kept A)
            ("C", 10),  # idx 4 (hit)
            ("B", 10),  # idx 5 (miss — we evicted it)
        )
        b = BeladyMin()
        b.prepare(trace)
        stats = simulate(trace, b, capacity_bytes=20)
        # idx 0: miss; idx 1: miss; idx 2: miss(evict B); idx 3: hit;
        # idx 4: hit; idx 5: miss.
        self.assertEqual(stats.hits, 2)
        self.assertEqual(stats.misses, 4)

    def test_belady_is_optimal_lower_bound(self) -> None:
        # Belady-MIN must produce >= hits as LRU on the same trace.
        # (Equality is OK; Belady can never lose to LRU on miss count.)
        trace = load_synthetic(seed=7, n_queries=2_000, n_tables=50)
        b = BeladyMin()
        b.prepare(trace)
        capacity = 30 * 10 * 1024 * 1024  # 300 MiB
        belady_stats = simulate(trace, b, capacity)
        lru_stats = simulate(trace, LRU(), capacity)
        self.assertGreaterEqual(belady_stats.hits, lru_stats.hits)


class TestSimulator(unittest.TestCase):
    def test_capacity_zero_admits_nothing(self) -> None:
        trace = _trace(("A", 10), ("A", 10), ("A", 10))
        stats = simulate(trace, LRU(), capacity_bytes=0)
        self.assertEqual(stats.hits, 0)
        self.assertEqual(stats.misses, 3)
        self.assertEqual(stats.bypassed, 3)

    def test_oversized_object_bypasses(self) -> None:
        trace = _trace(("A", 100), ("B", 10))
        stats = simulate(trace, LRU(), capacity_bytes=50)
        # A is 100 bytes, exceeds capacity; B fits.
        self.assertEqual(stats.bypassed, 1)
        self.assertEqual(stats.hits, 0)
        self.assertEqual(stats.misses, 2)

    def test_byte_ratios(self) -> None:
        trace = _trace(("A", 10), ("A", 30), ("A", 50))
        stats = simulate(trace, LRU(), capacity_bytes=100)
        # All three are A; capacity > size; first miss, two hits.
        self.assertEqual(stats.hits, 2)
        self.assertEqual(stats.bytes_hit, 80)
        self.assertEqual(stats.bytes_miss, 10)
        self.assertAlmostEqual(stats.byte_hit_ratio, 80 / 90, places=6)

    def test_synthetic_trace_is_deterministic(self) -> None:
        a = load_synthetic(seed=42, n_queries=100, n_tables=10)
        b = load_synthetic(seed=42, n_queries=100, n_tables=10)
        self.assertEqual(a, b)

    def test_csv_round_trip(self) -> None:
        trace = load_synthetic(seed=3, n_queries=50, n_tables=8)
        with TemporaryDirectory() as tmp:
            p = Path(tmp) / "trace.csv"
            write_csv(trace, p)
            loaded = load_from_trino_csv(p)
        self.assertEqual(trace, loaded)

    def test_tsv_writer_produces_stable_schema(self) -> None:
        trace = _trace(("A", 10), ("A", 10))
        stats = simulate(trace, LRU(), capacity_bytes=100)
        with TemporaryDirectory() as tmp:
            p = Path(tmp) / "out.tsv"
            write_tsv_row(str(p), stats, header=True)
            txt = p.read_text(encoding="utf-8")
        head, body = txt.strip().splitlines()
        cols = head.split("\t")
        self.assertIn("policy", cols)
        self.assertIn("hit_ratio", cols)
        self.assertEqual(len(body.split("\t")), len(cols))


class TestPolicyFactory(unittest.TestCase):
    def test_known_policies(self) -> None:
        trace = load_synthetic(seed=0, n_queries=100, n_tables=10)
        for name in ("lru", "fifo", "s3fifo", "belady"):
            p = build_policy(name, trace)
            self.assertEqual(p.name, name)

    def test_unknown_policy_raises(self) -> None:
        with self.assertRaises(ValueError):
            build_policy("magic", [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
