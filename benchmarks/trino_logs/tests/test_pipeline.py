"""End-to-end pipeline tests against the synthetic fixture."""

from __future__ import annotations

from shelf_replay.aggregate import aggregate_by_day
from shelf_replay.manifest import ManifestIndex
from shelf_replay.scanner import clear_footer_cache, scan_all, scan_query
from shelf_replay.trace import load_trace


def _run(fixture_dir):
    trace = load_trace(fixture_dir / "trace.jsonl")
    manifest = ManifestIndex.load(fixture_dir / "manifests")
    clear_footer_cache()
    return trace, manifest, scan_all(trace, manifest)


def test_partition_pruning_drops_two_of_three_files(fixture_dir):
    """Q1 filters by event_region='MP+CG' — only 1 of 3 files survives."""

    trace, manifest, scans = _run(fixture_dir)
    q1 = next(i for i, e in enumerate(trace) if e.query_id == "q-01")
    scan = scans[q1]
    assert scan.files_scanned == 3
    assert scan.files_after_partition_prune == 1


def test_row_group_pruning_keeps_one_of_two(fixture_dir):
    """Q1's user_id=42 predicate keeps only the first row group (user_id 0..49)."""

    trace, manifest, scans = _run(fixture_dir)
    q1 = next(i for i, e in enumerate(trace) if e.query_id == "q-01")
    scan = scans[q1]
    # The surviving file has two row groups; exactly one should be
    # kept by the row-group scanner.
    assert scan.rg_count == 2
    assert len(scan.rg_entries) == 1
    _, ordinal, *_ = scan.rg_entries[0]
    assert ordinal == 0


def test_unpredicated_full_scan_keeps_all_row_groups(fixture_dir):
    """Q4 has no WHERE clause and must survive all row groups in the table."""

    trace, manifest, scans = _run(fixture_dir)
    q4 = next(i for i, e in enumerate(trace) if e.query_id == "q-04")
    scan = scans[q4]
    assert scan.files_after_partition_prune == 3
    assert scan.rg_count == 6
    assert len(scan.rg_entries) == 6


def test_ratio_monotone_invariant(fixture_dir):
    """For every query, rg_bytes <= file_bytes."""

    _, _, scans = _run(fixture_dir)
    for s in scans:
        assert s.scanned_bytes_rg_level <= s.scanned_bytes_file_level
        assert 0.0 <= s.rg_over_file_ratio <= 1.0


def test_narrow_predicates_reduce_scanned_bytes(fixture_dir):
    """Narrow predicates (Q1, Q5) must prune more than the full scan (Q4)."""

    _, _, scans = _run(fixture_dir)
    by_id = {s.query_id: s for s in scans}
    assert by_id["q-01"].rg_over_file_ratio < by_id["q-04"].rg_over_file_ratio
    assert by_id["q-05"].rg_over_file_ratio < by_id["q-04"].rg_over_file_ratio


def test_aggregate_by_day_matches_expected(fixture_dir):
    """The E5 golden numbers committed in expected.json must reproduce."""

    import json

    expected_path = fixture_dir / "expected.json"
    expected = json.loads(expected_path.read_text())
    tol = float(expected["tolerance"])

    _, _, scans = _run(fixture_dir)
    days = {a.day: a for a in aggregate_by_day(scans)}
    for item in expected["per_day"]:
        day = item["day"]
        agg = days[day]
        assert abs(agg.median_ratio - item["median_ratio"]) <= tol, day
        assert abs(agg.p90_ratio - item["p90_ratio"]) <= tol, day
        assert abs(agg.overall_ratio - item["overall_ratio"]) <= tol, day
