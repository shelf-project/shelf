"""Unit tests for ``dbt_emit.py`` (H2)."""

from __future__ import annotations

import json
import pathlib
import subprocess

import pytest

from dbt_emit import _slug, emit


@pytest.fixture
def sample_payload(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "recs.json"
    path.write_text(json.dumps({
        "recommendations": [
            {
                "fingerprint": "abcdef0123456789",
                "canonical_plan": "{sorted-plan-goes-here}",
                "runs_per_day": 42.5,
                "bytes_saved_per_day": 3.21e12,
                "storage_cost_per_day": 0.01,
                "refresh_cost_per_day": 0.02,
                "net_benefit_per_day_bytes": 3.2e12,
                "net_benefit_ci95_bytes": 2.9e12,
                "tables": ["iceberg.a.events"],
                "first_seen": "2026-04-01",
                "last_seen": "2026-04-14",
            }
        ]
    }))
    return path


def test_slug_is_stable() -> None:
    assert _slug("abcdef0123456789") == "shelf_mv_abcdef0123"
    assert _slug("abcdef0123xxxxxxx") == "shelf_mv_abcdef0123"


def test_emit_writes_model_files(sample_payload: pathlib.Path, tmp_path: pathlib.Path) -> None:
    repo = tmp_path / "dbt-repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    (repo / "README.md").write_text("# test")
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "init"], check=True)

    recs = json.loads(sample_payload.read_text())["recommendations"]
    emit(recs, repo, commit=False)

    model_dir = repo / "models" / "shelf_materialized"
    assert (model_dir / "shelf_mv_abcdef0123.sql").is_file()
    assert (model_dir / "shelf_mv_abcdef0123.yml").is_file()
    sql = (model_dir / "shelf_mv_abcdef0123.sql").read_text()
    assert "materialized_view" in sql
    assert "fingerprint abcdef0123" in sql or "abcdef0123456789" in sql


def test_emit_skips_when_empty(tmp_path: pathlib.Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    out = emit([], repo)
    assert out == repo / "models" / "shelf_materialized"
    assert not out.exists()
