"""Core diff runner for the correctness harness.

Algorithm (per query, for each side of the diff):

1. Render the ``.sql.tmpl`` file with ``str.format_map`` using the
   config's ``bindings`` plus the catalog name for this side. A
   missing placeholder raises :class:`KeyError` at load time, which
   the CLI surfaces as exit-code 2 (harness configuration bug).
2. Submit the query to the Trino coordinator using a per-side
   connection. The connection sets ``query_max_run_time`` at session
   level before running the query.
3. Stream the result set in chunks; canonicalise by sorting the row
   tuples with a stable key and updating a SHA-256 hash column-by-
   column. Keep the first ``diff_preview_rows`` rows from each side
   verbatim so divergence can be pretty-printed.
4. Compare the two hashes. Equal hashes + equal row counts ⇒ match.

The canonical form is "sorted tuples of stringified column values
joined with ``\\x1f`` (ASCII unit separator) per row, and rows joined
with ``\\x1e`` (record separator)". ASCII unit/record separators are
chosen deliberately so any column value containing them would be
a Trino-side payload-encoding bug (and thus *should* diverge).

This module is deliberately sync + blocking — correctness-diff
workloads are I/O-bound on Trino's side, and one query at a time
makes the cron log trivially readable. Parallelism is wall-clock
noise at query-count = 5.
"""

from __future__ import annotations

import dataclasses
import hashlib
import logging
import pathlib
import time
from typing import Any, Iterable, Mapping

import trino.dbapi
import trino.exceptions

LOGGER = logging.getLogger(__name__)

_UNIT_SEP = "\x1f"
_RECORD_SEP = "\x1e"


@dataclasses.dataclass
class QueryReport:
    """One query's result across both sides of the diff."""

    name: str
    match: bool
    row_count_a: int
    row_count_b: int
    hash_a: str
    hash_b: str
    elapsed_s_a: float
    elapsed_s_b: float
    diff_preview: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    error: str | None = None


@dataclasses.dataclass
class RunReport:
    """Full harness output for one invocation."""

    replica: str
    started_at: float
    ended_at: float
    queries: list[QueryReport]

    @property
    def all_match(self) -> bool:
        return all(q.match and q.error is None for q in self.queries)


class Runner:
    """Orchestrates a single correctness-diff run.

    The constructor accepts a fully-materialised config dict (loaded
    from YAML by the CLI) so this class is unit-testable without a
    real filesystem.
    """

    def __init__(
        self,
        config: Mapping[str, Any],
        query_dir: pathlib.Path,
        *,
        now: float | None = None,
        connection_factory=None,
    ) -> None:
        self._config = dict(config)
        self._query_dir = pathlib.Path(query_dir)
        self._now = now if now is not None else time.time
        self._connection_factory = connection_factory or _default_connection

    # ----- Public API -----

    def run(self) -> RunReport:
        started = self._now()
        reports = [self._run_one(path) for path in sorted(self._query_dir.glob("*.sql.tmpl"))]
        ended = self._now()
        return RunReport(
            replica=str(self._config.get("replica", "unknown")),
            started_at=started,
            ended_at=ended,
            queries=reports,
        )

    # ----- Internals -----

    def _run_one(self, template_path: pathlib.Path) -> QueryReport:
        name = template_path.stem.removesuffix(".sql")
        bindings = dict(self._config.get("bindings") or {})
        bindings["schema"] = self._config.get("schema", "default")
        sql_a = _render(template_path, bindings, catalog=self._config["catalog_a"])
        sql_b = _render(template_path, bindings, catalog=self._config["catalog_b"])

        try:
            hash_a, count_a, sample_a, elapsed_a = self._execute(sql_a)
            hash_b, count_b, sample_b, elapsed_b = self._execute(sql_b)
        except trino.exceptions.TrinoExternalError as err:
            LOGGER.exception("Trino rejected query %s", name)
            return QueryReport(
                name=name,
                match=False,
                row_count_a=-1,
                row_count_b=-1,
                hash_a="",
                hash_b="",
                elapsed_s_a=0.0,
                elapsed_s_b=0.0,
                error=str(err),
            )

        match = (hash_a == hash_b) and (count_a == count_b)
        preview: list[dict[str, Any]] = []
        if not match:
            preview = _diff_preview(sample_a, sample_b)
            LOGGER.error(
                "Divergence in %s: A=%d rows (hash %s…), B=%d rows (hash %s…)",
                name,
                count_a,
                hash_a[:10],
                count_b,
                hash_b[:10],
            )
        return QueryReport(
            name=name,
            match=match,
            row_count_a=count_a,
            row_count_b=count_b,
            hash_a=hash_a,
            hash_b=hash_b,
            elapsed_s_a=elapsed_a,
            elapsed_s_b=elapsed_b,
            diff_preview=preview,
        )

    def _execute(self, sql: str) -> tuple[str, int, list[tuple[Any, ...]], float]:
        trino_cfg = self._config["trino"]
        exec_cfg = self._config.get("execution") or {}
        max_run_time = exec_cfg.get("query_max_run_time", "5m")
        chunk_rows = int(exec_cfg.get("fetch_chunk_rows", 10_000))
        preview_rows = int(exec_cfg.get("diff_preview_rows", 20))

        started = self._now()
        conn = self._connection_factory(trino_cfg)
        try:
            cursor = conn.cursor()
            cursor.execute(f"SET SESSION query_max_run_time = '{max_run_time}'")
            cursor.fetchall()
            cursor.execute(sql)
            rows_iter = _fetch_chunks(cursor, chunk_rows)
            digest, count, sample = _canonical_hash(rows_iter, preview_rows)
        finally:
            try:
                conn.close()
            except Exception:  # close is best-effort
                LOGGER.debug("connection close raised", exc_info=True)
        elapsed = self._now() - started
        return digest, count, sample, elapsed


# ----- Module-level helpers (unit-tested without a Trino server) -----


def _render(template_path: pathlib.Path, bindings: Mapping[str, Any], *, catalog: str) -> str:
    raw = template_path.read_text(encoding="utf-8")
    env = {"catalog": catalog, **bindings}
    try:
        return raw.format_map(_StrictMapping(env))
    except KeyError as err:
        missing = err.args[0]
        raise KeyError(
            f"query template {template_path.name} references placeholder "
            f"{{{missing}}} which is not present in config.bindings"
        ) from None


class _StrictMapping(dict):
    def __missing__(self, key):  # noqa: D401 — dict protocol
        raise KeyError(key)


def _fetch_chunks(cursor, chunk_rows: int) -> Iterable[tuple[Any, ...]]:
    while True:
        rows = cursor.fetchmany(chunk_rows)
        if not rows:
            return
        yield from rows


def _canonical_hash(rows: Iterable[tuple[Any, ...]], preview_rows: int) -> tuple[str, int, list[tuple[Any, ...]]]:
    """Compute SHA-256 of the sorted, unit-separator-joined rows.

    Sorting is done on stringified tuples, which is sufficient because
    identical rows from two Trino catalogs produce identical strings
    under the python ``repr``/``str`` of each column value. The tiny
    per-column ``repr`` cost dominates in-memory wall-clock compared
    to the network round-trip to Trino.
    """
    encoded: list[str] = []
    for row in rows:
        encoded.append(_UNIT_SEP.join(_stringify(col) for col in row))
    encoded.sort()
    digest = hashlib.sha256()
    for row_str in encoded:
        digest.update(row_str.encode("utf-8"))
        digest.update(_RECORD_SEP.encode("utf-8"))
    sample = [tuple(row.split(_UNIT_SEP)) for row in encoded[:preview_rows]]
    return digest.hexdigest(), len(encoded), sample


def _stringify(value: Any) -> str:
    # ``repr`` is deterministic across CPython minor versions for the
    # value types Trino returns (int, float, str, bytes, datetime,
    # decimal, list, dict, None). We're not portable to PyPy or Jython
    # on purpose — the harness runs in our container image.
    if value is None:
        return "NULL"
    return repr(value)


def _diff_preview(sample_a: list[tuple[Any, ...]], sample_b: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    seen_a = set(sample_a)
    seen_b = set(sample_b)
    only_a = sorted(seen_a - seen_b)
    only_b = sorted(seen_b - seen_a)
    out: list[dict[str, Any]] = []
    for row in only_a[:10]:
        out.append({"side": "A_only", "row": list(row)})
    for row in only_b[:10]:
        out.append({"side": "B_only", "row": list(row)})
    return out


def _default_connection(trino_cfg: Mapping[str, Any]):
    return trino.dbapi.connect(
        host=trino_cfg["host"],
        port=int(trino_cfg.get("port", 8080)),
        user=trino_cfg.get("user", "shelf-correctness-diff"),
        http_scheme=trino_cfg.get("http_scheme", "http"),
    )
