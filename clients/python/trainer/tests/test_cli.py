"""Tests for :mod:`shelf_trainer.cli`.

Every non-trivial command is a stub that must raise ``NotImplementedError``
with a ``SHELF-<nn>:`` ticket prefix, so ops can trace who owns each
missing slice. These tests assert that shape explicitly; they would catch
a lazy ``pass`` / silent return in place of a real implementation.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from shelf_trainer import __version__
from shelf_trainer.cli import app

runner = CliRunner()


def test_version_command() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_help_lists_all_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in ("pin-list", "train-admission", "promote", "rollback", "version"):
        assert cmd in result.stdout


@pytest.mark.parametrize(
    ("argv", "ticket_prefix"),
    [
        (["pin-list", "--dry-run"], "SHELF-48"),
        (["train-admission", "--dry-run"], "SHELF-49"),
        (["promote", "v42"], "SHELF-50"),
        (["rollback", "v41"], "SHELF-51"),
    ],
)
def test_stub_commands_raise_with_ticket_id(argv: list[str], ticket_prefix: str) -> None:
    result = runner.invoke(app, argv)
    assert result.exit_code != 0
    exc = result.exception
    assert isinstance(exc, NotImplementedError)
    assert ticket_prefix in str(exc)
