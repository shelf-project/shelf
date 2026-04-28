"""CLI entry point for shelf-correctness-diff.

Designed to be friendly to a container image + cron job:

- Single ``--config`` flag points at the YAML; everything else is in
  the file so the cron definition stays stable across replicas.
- Non-zero exit means "operator should investigate"; rollout runbook
  treats any non-zero exit during a canary window as an immediate
  rollback signal. Specifically:

      exit 0  -> all queries matched
      exit 1  -> at least one query diverged
      exit 2  -> harness configuration bug (rendering, missing file,
                 unparseable YAML) — NOT a cache-correctness signal,
                 but still needs operator attention.
      exit 3  -> Trino unreachable / per-query timeout — re-run may
                 succeed; rollout runbook treats 3 consecutive
                 exit-3s as a canary failure.

The JSON blob written to ``output_dir/<timestamp>.json`` is also
written (or symlinked on POSIX) to ``output_dir/latest.json`` so
cron-job log tails can grep against a stable filename without
timestamp math.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import pathlib
import sys
import time

import yaml

from .runner import Runner, RunReport


LOGGER = logging.getLogger("shelf-correctness-diff")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="shelf-correctness-diff",
        description="Row-level diff between Shelf-backed and S3-direct Trino catalogs.",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to YAML config (see config.example.yaml).",
    )
    parser.add_argument(
        "--replica",
        default=os.environ.get("REPLICA"),
        help="Trino replica identifier to tag results with (overrides config.replica).",
    )
    parser.add_argument(
        "--queries-dir",
        default=None,
        help="Override the queries directory (default: <config_dir>/queries).",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Python logging level (default: INFO).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":%(message)s}',
    )

    try:
        config_path = pathlib.Path(args.config).resolve()
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        if args.replica:
            config["replica"] = args.replica
        queries_dir = (
            pathlib.Path(args.queries_dir).resolve()
            if args.queries_dir
            else config_path.parent / "queries"
        )
        if not queries_dir.exists():
            _log(LOGGER, logging.ERROR, "queries_dir_missing", dir=str(queries_dir))
            return 2
    except (OSError, yaml.YAMLError, KeyError) as err:
        _log(LOGGER, logging.ERROR, "config_load_failed", error=str(err))
        return 2

    try:
        report = Runner(config, queries_dir).run()
    except KeyError as err:
        # Template placeholder missing / bad bindings — harness bug.
        _log(LOGGER, logging.ERROR, "template_binding_missing", error=str(err))
        return 2
    except ConnectionError as err:
        # Trino unreachable.
        _log(LOGGER, logging.ERROR, "trino_unreachable", error=str(err))
        return 3
    except Exception as err:  # pragma: no cover — defensive
        _log(LOGGER, logging.ERROR, "harness_crashed", error=str(err))
        return 2

    output_dir = pathlib.Path(config.get("output_dir", "results"))
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    out_path = output_dir / f"{ts}.json"
    out_path.write_text(
        json.dumps(dataclasses.asdict(report), indent=2, default=_json_default),
        encoding="utf-8",
    )
    latest = output_dir / "latest.json"
    try:
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(out_path.name)
    except OSError:
        # Fallback for filesystems without symlink support (rare in
        # our containers; guarded just in case).
        latest.write_text(out_path.read_text(encoding="utf-8"), encoding="utf-8")

    _emit_summary(report, out_path)
    return 0 if report.all_match else 1


def _emit_summary(report: RunReport, out_path: pathlib.Path) -> None:
    summary = {
        "replica": report.replica,
        "all_match": report.all_match,
        "query_count": len(report.queries),
        "diverged": [q.name for q in report.queries if not q.match],
        "errored": [q.name for q in report.queries if q.error],
        "output": str(out_path),
    }
    _log(LOGGER, logging.INFO, "run_complete", **summary)


def _log(logger: logging.Logger, level: int, event: str, **fields) -> None:
    # Emit a JSON-serialisable payload; logger's format string wraps
    # this in the outer `{"ts":...,"msg":...}` envelope.
    payload = {"event": event, **fields}
    logger.log(level, json.dumps(payload, default=_json_default))


def _json_default(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return str(value)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
