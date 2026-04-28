"""Shared pytest fixtures."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "synthetic-7d"


@pytest.fixture(scope="session", autouse=True)
def _ensure_fixture_exists() -> None:
    """Regenerate the Parquet fixture if missing — keeps tests self-bootstrapping."""

    files_dir = FIXTURE / "manifests" / "files"
    if files_dir.exists() and any(files_dir.glob("*.parquet")):
        return
    subprocess.check_call([sys.executable, str(FIXTURE / "generate.py")])


@pytest.fixture(scope="session")
def fixture_dir() -> Path:
    return FIXTURE
