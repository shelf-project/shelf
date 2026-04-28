"""Unit tests for ``mv_pin_watcher.py`` (H3)."""

from __future__ import annotations

import hashlib
import json
import pathlib

import pytest

from mv_pin_watcher import (
    MvEvent,
    _enumerate_mv_files,
    _load_backfill,
    _load_events,
    process,
)


def _sha(uri: str) -> str:
    return hashlib.sha256(("mv:" + uri).encode()).hexdigest()


def test_enumerate_mv_files_covers_current_snapshot() -> None:
    metadata = {
        "current-snapshot-id": 99,
        "snapshots": [
            {"snapshot-id": 98, "manifest-list": "s3://a/m-old.avro"},
            {"snapshot-id": 99, "manifest-list": "s3://a/m-new.avro"},
        ],
        "_mv_watcher_files": ["s3://a/d1.parquet", "s3://a/d2.parquet"],
    }
    files = _enumerate_mv_files(metadata)
    assert files == [
        "s3://a/m-new.avro",
        "s3://a/d1.parquet",
        "s3://a/d2.parquet",
    ]


def test_process_pins_every_file_on_every_replica() -> None:
    calls: list[tuple[str, bytes]] = []

    def fake_post(url: str, body: bytes) -> int:
        calls.append((url, body))
        return 200

    events = [
        MvEvent(
            nl_id=1,
            db_name="analytics",
            tbl_name="top_ten",
            event_type="CREATE_MATERIALIZED_VIEW",
            metadata_location="s3://a/metadata.json",
        )
    ]
    metadata = {
        "current-snapshot-id": 1,
        "snapshots": [{"snapshot-id": 1, "manifest-list": "s3://a/m.avro"}],
        "_mv_watcher_files": ["s3://a/d.parquet"],
    }
    results = process(
        events,
        shelfd_urls=["http://shelfd-a:9000", "http://shelfd-b:9000"],
        sha256_key=_sha,
        http_post=fake_post,
        metadata_loader=lambda _: metadata,
    )
    # 3 files × 2 shelfd replicas = 6 pin requests.
    assert len(results) == 6
    assert all(r.ok for r in results)
    # Every URL hit `/admin/pin` and carried the right JSON shape —
    # in particular the `mv_name` field that feeds shelfd's H5
    # `MvRegistry`. Missing it would keep the per-MV counters flat.
    for url, body in calls:
        assert url.endswith("/admin/pin")
        doc = json.loads(body)
        assert doc["pool"] == "metadata"
        assert len(doc["key_hex"]) == 64
        assert doc["mv_name"] == "analytics.top_ten"


def test_load_backfill_synthesizes_events(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "backfill.jsonl"
    path.write_text(
        json.dumps({
            "db_name": "analytics",
            "tbl_name": "top_ten",
            "metadata_location": "s3://a/metadata.json",
        }) + "\n"
        + json.dumps({
            "db_name": "analytics",
            "tbl_name": "weekly_sales",
            "metadata_location": "s3://a/weekly.json",
        }) + "\n"
    )
    events = _load_backfill(path)
    assert len(events) == 2
    # Backfill rows must use the BACKFILL sentinel and nl_id=0 so
    # the watcher's notification-log cursor stays untouched.
    assert all(e.event_type == "BACKFILL" for e in events)
    assert all(e.nl_id == 0 for e in events)
    assert {e.tbl_name for e in events} == {"top_ten", "weekly_sales"}


def test_load_events_parses_jsonl(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(json.dumps({
        "nl_id": 5,
        "db_name": "d",
        "tbl_name": "mv",
        "event_type": "CREATE_MATERIALIZED_VIEW",
        "metadata_location": "s3://a/metadata.json",
    }) + "\n")
    events = _load_events(path)
    assert events[0].nl_id == 5
    assert events[0].event_type == "CREATE_MATERIALIZED_VIEW"
