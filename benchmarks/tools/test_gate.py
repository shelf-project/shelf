"""Unit tests for benchmarks/tools/gate.py.

Run with:
    python3 -m unittest benchmarks/tools/test_gate.py
or pytest:
    pytest -q benchmarks/tools/test_gate.py
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

# Make `gate` importable from both `python3 -m unittest benchmarks.tools.test_gate`
# (cwd = repo root) and `python3 -m unittest test_gate` (cwd = tools/).
sys.path.insert(0, str(Path(__file__).parent))

from gate import GateInput, evaluate, render_results_md_row  # noqa: E402


def _replay_record(
    *,
    backend: str,
    hit_rate: float,
    gold_dbt_ok: float,
    p95_ns: int,
    speed: str = "2x",
) -> dict:
    return {
        "benchmark": "replay",
        "backend": backend,
        "summary": {
            "latency_ns_p50": p95_ns // 2,
            "latency_ns_p95": p95_ns,
            "latency_ns_p99": p95_ns * 2,
            "latency_ns_p999": p95_ns * 4,
            "hit_rate": hit_rate,
            "bytes_read": 1_000_000,
            "bytes_admitted": 500_000,
            "dollars_per_query": 0.001,
        },
        "trace": {
            "speed": speed,
            "source_table": "cdp.trino_logs.trino_queries",
            "snapshot_id": "0" * 19,
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-01-08T00:00:00Z",
            "replica": "rep-2",
            "query_count": 1000,
        },
        "gate": {
            "hit_rate_7d_cumulative": hit_rate,
            "gold_dbt_ok_rate": gold_dbt_ok,
            "latency_ns_p95_vs_alluxio": None,
            "shelf_caused_pages": 0,
            "oncall_surface_ratio": None,
            "verdict": "pending",
            "failed_metrics": [],
        },
    }


class GateEvaluation(unittest.TestCase):
    def test_pass_all_thresholds(self):
        shelf = _replay_record(backend="shelf", hit_rate=0.85, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        baseline = _replay_record(backend="alluxio-2-9", hit_rate=0.40, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        inp = GateInput(
            shelf_record=shelf,
            baseline_record=baseline,
            pages_shelf_attributed=0,
            oncall_surface_shelf=10.0,
            oncall_surface_baseline=100.0,  # ratio = 0.10 ≤ 0.50
        )
        result = evaluate(inp)
        self.assertEqual(result.verdict, "pass")
        self.assertEqual(result.failed_metrics, [])
        self.assertAlmostEqual(result.metrics["latency_ns_p95_vs_alluxio"], 1.0, places=6)
        self.assertAlmostEqual(result.metrics["hit_rate_7d_cumulative"], 0.85, places=6)

    def test_fail_hit_rate_below_threshold(self):
        shelf = _replay_record(backend="shelf", hit_rate=0.50, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        baseline = _replay_record(backend="alluxio-2-9", hit_rate=0.40, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        result = evaluate(GateInput(shelf, baseline, 0, 1.0, 100.0))
        self.assertEqual(result.verdict, "fail")
        self.assertIn("hit_rate_7d_cumulative", result.failed_metrics)

    def test_fail_p95_regression(self):
        shelf = _replay_record(backend="shelf", hit_rate=0.85, gold_dbt_ok=1.0, p95_ns=2_000_000_000)
        baseline = _replay_record(backend="alluxio-2-9", hit_rate=0.40, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        # ratio = 2.0 > 1.20
        result = evaluate(GateInput(shelf, baseline, 0, 1.0, 100.0))
        self.assertEqual(result.verdict, "fail")
        self.assertIn("latency_ns_p95_vs_alluxio", result.failed_metrics)
        self.assertAlmostEqual(result.metrics["latency_ns_p95_vs_alluxio"], 2.0, places=6)

    def test_fail_dbt_ok_rate(self):
        shelf = _replay_record(backend="shelf", hit_rate=0.85, gold_dbt_ok=0.95, p95_ns=1_000_000_000)
        baseline = _replay_record(backend="alluxio-2-9", hit_rate=0.40, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        result = evaluate(GateInput(shelf, baseline, 0, 1.0, 100.0))
        self.assertEqual(result.verdict, "fail")
        self.assertIn("gold_dbt_ok_rate", result.failed_metrics)

    def test_fail_pages(self):
        shelf = _replay_record(backend="shelf", hit_rate=0.85, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        baseline = _replay_record(backend="alluxio-2-9", hit_rate=0.40, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        result = evaluate(GateInput(shelf, baseline, 1, 1.0, 100.0))
        self.assertEqual(result.verdict, "fail")
        self.assertIn("shelf_caused_pages", result.failed_metrics)

    def test_na_when_speed_is_not_2x(self):
        shelf = _replay_record(backend="shelf", hit_rate=0.85, gold_dbt_ok=1.0, p95_ns=1_000_000_000, speed="10x")
        baseline = _replay_record(backend="alluxio-2-9", hit_rate=0.40, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        result = evaluate(GateInput(shelf, baseline, 0, 1.0, 100.0))
        self.assertEqual(result.verdict, "n/a")

    def test_results_md_row_pass(self):
        shelf = _replay_record(backend="shelf", hit_rate=0.85, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        baseline = _replay_record(backend="alluxio-2-9", hit_rate=0.40, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        result = evaluate(GateInput(shelf, baseline, 0, 1.0, 100.0))
        row = render_results_md_row(result, shelf, date_utc="2026-05-01")
        self.assertIn("**PASS**", row)
        self.assertIn("85.00 %", row)

    def test_results_md_row_fail_lists_failed_metrics(self):
        shelf = _replay_record(backend="shelf", hit_rate=0.50, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        baseline = _replay_record(backend="alluxio-2-9", hit_rate=0.40, gold_dbt_ok=1.0, p95_ns=1_000_000_000)
        result = evaluate(GateInput(shelf, baseline, 0, 1.0, 100.0))
        row = render_results_md_row(result, shelf, date_utc="2026-05-01")
        self.assertIn("**FAIL", row)
        self.assertIn("hit_rate_7d_cumulative", row)


if __name__ == "__main__":
    unittest.main()
