"""Cache simulator tests."""

from __future__ import annotations

from shelf_replay.manifest import ManifestIndex
from shelf_replay.scanner import clear_footer_cache, scan_all
from shelf_replay.simulate import SimConfig, simulate
from shelf_replay.trace import load_trace


def _pairs(fixture_dir):
    trace = load_trace(fixture_dir / "trace.jsonl")
    manifest = ManifestIndex.load(fixture_dir / "manifests")
    clear_footer_cache()
    scans = scan_all(trace, manifest)
    return list(zip(trace, scans))


def test_replaying_same_query_twice_yields_hits(fixture_dir):
    """Q1 + Q4 both scan silver_events — Q4 should hit Q1's cached RG."""

    pairs = _pairs(fixture_dir)
    config = SimConfig(name="baseline", capacity_bytes=1 << 30)
    result = simulate(pairs, config)
    # Q1 admits 1 RG; Q4 reads 6 RGs, one of which is Q1's.
    assert result.hits >= 1
    assert result.misses >= 1


def test_tiny_capacity_evicts_before_revisit(fixture_dir):
    """A too-small cache evicts Q1's RG before Q4 revisits it."""

    pairs = _pairs(fixture_dir)
    config = SimConfig(
        name="tiny", capacity_bytes=256, size_threshold_bytes=1 << 30
    )
    result = simulate(pairs, config)
    assert result.hits == 0
    assert result.evicted_bytes > 0


def test_size_threshold_rejects_everything(fixture_dir):
    """Size threshold below the smallest RG admits nothing."""

    pairs = _pairs(fixture_dir)
    config = SimConfig(
        name="threshold",
        capacity_bytes=1 << 30,
        size_threshold_bytes=1,  # smaller than any real row group
    )
    result = simulate(pairs, config)
    assert result.hits == 0
    assert result.rejected_by_threshold > 0
    assert result.admitted_bytes == 0


def test_pin_list_bypasses_size_threshold(fixture_dir):
    """A pinned key is admitted even if it exceeds the size threshold."""

    pairs = _pairs(fixture_dir)
    trace, scans = zip(*pairs)
    # Pin the single row group Q1 reads.
    pinned_entry = scans[0].rg_entries[0]
    from shelf_replay.key import content_key

    pinned_key = content_key(
        pinned_entry[4], pinned_entry[1], pinned_entry[2], pinned_entry[3]
    )
    config = SimConfig(
        name="pinned",
        capacity_bytes=1 << 30,
        size_threshold_bytes=1,
        pinned_bypass=True,
        pin_list=frozenset({pinned_key}),
    )
    result = simulate(pairs, config)
    # At least one admission via the pin-list path.
    assert result.admitted_bytes >= pinned_entry[3]
