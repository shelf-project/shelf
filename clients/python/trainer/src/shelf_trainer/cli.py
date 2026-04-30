"""Shelf trainer CLI.

Four entry points, all wired to stubs in this skeleton:

* ``shelf-trainer pin-list``         — emit ``pin_list.json`` (Agent 6 Pass 5).
* ``shelf-trainer train-admission``  — train LightGBM candidate (v1.x,
  ADR-0003 escape hatch; Phase 4).
* ``shelf-trainer promote``          — flip ``admission/production/`` alias
  after guardrails pass.
* ``shelf-trainer rollback``         — revert to a previous production artifact.

This is a skeleton: every subcommand raises ``NotImplementedError`` with
its owning ticket. The CLI surface itself is stable and matches what the
Airflow DAG in ``infra/dag/shelf-trainer-dag.py`` invokes.
"""

from __future__ import annotations

import typer

from shelf_trainer import __version__
from shelf_trainer.config import TrainerSettings

app = typer.Typer(
    name="shelf-trainer",
    help="Shelf nightly trainer: pin-list + (v1.x) LightGBM admission.",
    no_args_is_help=True,
    add_completion=False,
)


@app.callback()
def _main_callback() -> None:
    """Shared state hook (reserved for --log-level / --config overrides)."""


@app.command("version")
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)


@app.command("pin-list")
def pin_list_cmd(
    top_n: int = typer.Option(
        None,
        "--top-n",
        help="Override settings.pin_list_top_n.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Compute but do not publish pin_list.json.",
    ),
) -> None:
    """Generate pin_list.json from your_query_log_table."""
    settings = TrainerSettings()
    resolved_top_n = top_n if top_n is not None else settings.pin_list_top_n
    raise NotImplementedError(
        f"SHELF-48: wire pin-list CLI to shelf_trainer.pin_list "
        f"(top_n={resolved_top_n}, dry_run={dry_run})."
    )


@app.command("train-admission")
def train_admission_cmd(
    window_days: int = typer.Option(
        30,
        "--window-days",
        help="Training-window length in days (matches Phase 4 replay default).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Fit + evaluate but do not publish to admission/candidate/.",
    ),
) -> None:
    """Train a LightGBM admission candidate (v1.x, ADR-0003 escape hatch)."""
    _ = TrainerSettings()
    raise NotImplementedError(
        f"SHELF-49: wire train-admission CLI to shelf_trainer.models.lightgbm_admission "
        f"(window_days={window_days}, dry_run={dry_run})."
    )


@app.command("promote")
def promote_cmd(
    candidate_version: str = typer.Argument(
        ...,
        help="Artifact version under admission/candidate/ to promote.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Skip guardrail check (ops break-glass only; audit-logged).",
    ),
) -> None:
    """Promote candidate_version to admission/production/ after guardrails."""
    _ = TrainerSettings()
    raise NotImplementedError(
        f"SHELF-50: wire promote CLI to shelf_trainer.promotion "
        f"(candidate_version={candidate_version!r}, force={force})."
    )


@app.command("rollback")
def rollback_cmd(
    to_version: str = typer.Argument(
        ...,
        help="Prior production artifact version to roll back to.",
    ),
) -> None:
    """Revert admission/production/ to ``to_version``."""
    _ = TrainerSettings()
    raise NotImplementedError(
        f"SHELF-51: wire rollback CLI to shelf_trainer.promotion.rollback "
        f"(to_version={to_version!r})."
    )


if __name__ == "__main__":
    app()
