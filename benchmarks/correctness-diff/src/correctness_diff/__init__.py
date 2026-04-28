"""shelf-correctness-diff — row-level parity between Shelf and S3-direct.

Library surface is intentionally tiny:

- :class:`Runner` — executes the diff and returns a :class:`RunReport`.
- :class:`RunReport` — a dataclass the CLI serialises to JSON.

The CLI lives in :mod:`correctness_diff.cli` so operators can write
``python -m correctness_diff`` in their cron definitions without
importing internals.
"""

from .runner import QueryReport, Runner, RunReport

__all__ = ["QueryReport", "Runner", "RunReport"]
