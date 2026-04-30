"""Airflow DAG skeleton for the Shelf trainer.

Single daily trigger, three serial tasks:

    extract  ->  train  ->  promote

All three tasks are stubs: they invoke ``shelf-trainer`` CLI subcommands
that currently raise ``NotImplementedError`` with ticket IDs. The DAG
shape itself is the deliverable — agent-8 (operator) wires the actual
scheduling, alerting, and retries against this skeleton.

Per ADR-0003:

- ``extract`` pulls the last-30-day slice of ``your_query_log_table``
  needed by both pin-list generation and LightGBM training.
- ``train`` produces a LightGBM candidate (v1.x escape hatch). In pure v1
  the ``train`` task is a no-op — pin-list only ships — but we keep the
  task shape so enabling LightGBM is a one-line flip, not a DAG rewrite.
- ``promote`` applies the guardrails (ADR-0003: ≥5 pp replay lift,
  <50 µs p99) and either flips the ``admission/production/`` S3 alias or
  records a structured refusal under ``admission/candidate/<v>/audit.json``.

The pin-list job runs inside ``extract`` for v1; once LightGBM lands it
moves to its own task (and this DAG grows to five tasks).

Not scaffolded here:

- Alert wiring (``on_failure_callback`` → PagerDuty). SHELF-62.
- Retry policy tuned to Trino transient failures. SHELF-63.
- Backfill semantics (S3 object versioning on ``pin_list.json``). SHELF-64.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # Airflow is not a runtime dep of the trainer package.
    from airflow import DAG

DAG_ID = "shelf-trainer"
OWNER = "cache-team"
SCHEDULE = "@daily"
START_DATE = datetime(2026, 5, 1)


def _cmd(subcommand: str, *args: str) -> str:
    """Build a ``shelf-trainer`` invocation string for BashOperator."""
    quoted = " ".join(args)
    return f"uv run --directory /opt/shelf-trainer shelf-trainer {subcommand} {quoted}".strip()


def build_dag() -> DAG:
    """Construct the DAG. Import-time Airflow usage kept inside this function
    so unit tests / ruff can parse the module without Airflow installed.

    Wiring real Airflow: SHELF-65.
    """
    from airflow import DAG  # noqa: PLC0415
    from airflow.operators.bash import BashOperator  # noqa: PLC0415

    default_args: dict[str, Any] = {
        "owner": OWNER,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": False,
    }

    dag = DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description="Nightly Shelf trainer: extract -> train -> promote.",
        schedule=SCHEDULE,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=["shelf", "cache", "trainer"],
    )

    with dag:
        extract = BashOperator(
            task_id="extract",
            bash_command=_cmd("pin-list", "--dry-run"),
            doc_md=(
                "Pulls the 7/30/90d trino_queries slice and emits "
                "pin_list.json (v1 default). Stub: SHELF-48."
            ),
        )

        train = BashOperator(
            task_id="train",
            bash_command=_cmd("train-admission"),
            doc_md=(
                "Trains the LightGBM admission candidate (v1.x, ADR-0003). "
                "In pure v1 this task is a no-op. Stub: SHELF-49."
            ),
        )

        promote = BashOperator(
            task_id="promote",
            bash_command=_cmd("promote", "{{ ti.xcom_pull(task_ids='train') }}"),
            doc_md=(
                "Applies guardrails and promotes candidate -> production if all "
                "ADR-0003 thresholds pass. Stub: SHELF-50."
            ),
        )

        extract >> train >> promote

    return dag


# Airflow discovers DAGs by scanning module globals for DAG instances.
# At import time in this skeleton we do *not* instantiate the DAG — Airflow
# is not pinned as a runtime dep. The operator agent enables this line in
# the production image:
#
#     dag = build_dag()
#
# SHELF-65 is the ticket that flips it on.
