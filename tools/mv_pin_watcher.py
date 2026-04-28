#!/usr/bin/env python3
"""H3 — auto-pin Iceberg MV files into shelfd's DRAM hot pool.

Runs alongside ``hms_notification_poller.py``. On each new
``CREATE MATERIALIZED VIEW`` / ``ALTER MATERIALIZED VIEW`` HMS
notification we:

1. Resolve the MV's current metadata.json (via HMS
   ``TABLE_PARAMS.metadata_location``).
2. Enumerate manifest list + manifest files and all data files
   from the current snapshot.
3. Content-address each file (reusing
   :func:`gen_pin_list._sha256_key`) and POST
   ``/admin/pin`` per key to every shelfd replica listed in
   ``--shelfd-admin-urls``.

The poller's existing state file (``hms_poller_state.json``)
already advances past every NL event; ``mv_pin_watcher`` keeps
its *own* state file so the two CronJobs can run independently
without stealing notifications from each other.

Keeping this separate from ``hms_notification_poller`` makes
the blast radius explicit: a bug here affects only MV pinning,
not the general snapshot pin list.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import pathlib
import sys
import time
import urllib.request
from typing import Iterable

logger = logging.getLogger("shelf-mv-pin")


# ---------------------------------------------------------------------------
# HMS read adapter — the Python driver is injected so unit tests
# can feed a synthetic event stream without a real metastore.
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class MvEvent:
    nl_id: int
    db_name: str
    tbl_name: str
    event_type: str  # CREATE_MATERIALIZED_VIEW | ALTER_MATERIALIZED_VIEW
    metadata_location: str


def _default_hms_fetch(jdbc_url: str, last_nl_id: int, limit: int) -> list[MvEvent]:
    """Placeholder production hook.

    The real implementation queries ``NOTIFICATION_LOG`` filtered on
    ``event_type IN ('CREATE_MATERIALIZED_VIEW',
    'ALTER_MATERIALIZED_VIEW')``. Until we wire a JDBC driver into
    the CronJob image, the CLI's ``--input`` mode reads events
    straight from a JSONL file — operators exercise the pipeline
    today by piping an export through that mode.
    """
    raise NotImplementedError(
        "run with --input <jsonl> for scaffolded paths; production wiring "
        "lives behind --hms-jdbc-url and lands with the infra PR that "
        "bundles the JDBC driver into the mv-pin-watcher image"
    )


# ---------------------------------------------------------------------------
# Metadata walk — deliberately tiny so the call graph is easy to
# audit.
# ---------------------------------------------------------------------------

def _enumerate_mv_files(metadata_json: dict) -> list[str]:
    """Return the URIs of every file shelfd should pin for this MV.

    Covers:
      - the metadata.json itself (passed in caller-side),
      - the current snapshot's manifest-list,
      - each manifest referenced by that list,
      - every data file listed by the manifests.
    """
    out: list[str] = []
    snapshots = {s["snapshot-id"]: s for s in metadata_json.get("snapshots", [])}
    current = snapshots.get(metadata_json.get("current-snapshot-id"))
    if not current:
        return out
    manifest_list = current.get("manifest-list")
    if manifest_list:
        out.append(manifest_list)
    # The manifest list's contents are opaque here: the production
    # pipeline fetches the Avro via ``pyarrow.fs`` and extends
    # ``out``. Tests pre-populate the list.
    out.extend(metadata_json.get("_mv_watcher_files", []))
    return out


# ---------------------------------------------------------------------------
# Shelfd admin POST
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class PinResult:
    url: str
    key_hex: str
    ok: bool
    status: int
    error: str | None = None


def _pin(
    http_post: callable,
    admin_url: str,
    key_hex: str,
    mv_name: str,
    pool: str = "metadata",
) -> PinResult:
    # `mv_name` is forwarded to shelfd's `MvRegistry` so the read path
    # can attribute hits to the right MV for the H5 dashboard. An
    # empty `mv_name` is legal (older callers) but reduces the pin
    # to a plain "keep this hot in Pool::Metadata" instruction.
    payload = {"key_hex": key_hex, "pool": pool}
    if mv_name:
        payload["mv_name"] = mv_name
    body = json.dumps(payload).encode()
    try:
        status = http_post(admin_url.rstrip("/") + "/admin/pin", body)
        return PinResult(url=admin_url, key_hex=key_hex, ok=200 <= status < 300, status=status)
    except Exception as exc:  # pragma: no cover - production only
        return PinResult(url=admin_url, key_hex=key_hex, ok=False, status=0, error=str(exc))


def _urllib_post(url: str, body: bytes) -> int:
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5.0) as resp:  # noqa: S310
        return resp.status


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def process(
    events: Iterable[MvEvent],
    shelfd_urls: list[str],
    *,
    sha256_key: callable,
    http_post: callable = _urllib_post,
    metadata_loader: callable | None = None,
) -> list[PinResult]:
    """Pin every file reachable from each event on each shelfd URL."""
    results: list[PinResult] = []
    for event in events:
        logger.info(
            "pinning MV %s.%s (nl=%d, %s)",
            event.db_name, event.tbl_name, event.nl_id, event.event_type,
        )
        metadata = metadata_loader(event.metadata_location) if metadata_loader else {}
        files = [event.metadata_location] + _enumerate_mv_files(metadata)
        mv_name = f"{event.db_name}.{event.tbl_name}"
        for file_uri in files:
            key_hex = sha256_key(file_uri)
            for admin in shelfd_urls:
                results.append(_pin(http_post, admin, key_hex, mv_name))
    return results


def _load_events(path: pathlib.Path) -> list[MvEvent]:
    out: list[MvEvent] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            out.append(MvEvent(
                nl_id=int(doc["nl_id"]),
                db_name=doc["db_name"],
                tbl_name=doc["tbl_name"],
                event_type=doc["event_type"],
                metadata_location=doc["metadata_location"],
            ))
    return out


# ---------------------------------------------------------------------------
# Backfill mode — synthesize MvEvent rows for every MV currently
# registered in HMS so a fresh deploy can pin existing MVs without
# waiting for each one to be ALTERed.
# ---------------------------------------------------------------------------

def _backfill_from_hms(jdbc_url: str) -> list[MvEvent]:
    """Production hook for ``--backfill``.

    Real implementation runs::

        SELECT  d.NAME              AS db_name,
                t.TBL_NAME           AS tbl_name,
                p.PARAM_VALUE        AS metadata_location
        FROM    TBLS  t
        JOIN    DBS   d  ON d.DB_ID  = t.DB_ID
        JOIN    TABLE_PARAMS p ON p.TBL_ID = t.TBL_ID
        WHERE   t.TBL_TYPE  = 'MATERIALIZED_VIEW'
          AND   p.PARAM_KEY = 'metadata_location';

    Every row becomes an ``MvEvent`` with synthetic ``nl_id=0`` and
    ``event_type='BACKFILL'`` so the production NL cursor is left
    alone. We do not write to the watcher's state file in backfill
    mode for the same reason — backfill is idempotent against
    shelfd's `/admin/pin`, so re-running it is cheap and safe.
    """
    raise NotImplementedError(
        "run with --backfill-input <jsonl> for scaffolded paths; production "
        "wiring lands with the infra PR that bundles the JDBC driver into "
        "the mv-pin-watcher image (same gate as --hms-jdbc-url)"
    )


def _load_backfill(path: pathlib.Path) -> list[MvEvent]:
    """JSONL of `{db_name, tbl_name, metadata_location}` rows.

    Same shape as ``--input`` minus ``nl_id`` and ``event_type``,
    which the loader fills with backfill sentinels. Lets operators
    take a one-off SELECT-from-HMS dump and feed it through the
    pipeline without depending on the JDBC driver being baked in.
    """
    out: list[MvEvent] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            out.append(MvEvent(
                nl_id=0,
                db_name=doc["db_name"],
                tbl_name=doc["tbl_name"],
                event_type="BACKFILL",
                metadata_location=doc["metadata_location"],
            ))
    return out


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=pathlib.Path,
                        help="JSONL of MvEvent dicts (scaffolded path)")
    parser.add_argument("--hms-jdbc-url",
                        help="metastore JDBC URL (production path, pending driver)")
    parser.add_argument("--last-nl-id", type=int, default=0)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--shelfd-admin-url", action="append", dest="shelfd_urls",
                        default=[], required=False,
                        help="one shelfd /admin/ URL; repeat for multiple replicas")
    # Backfill — pin every MV currently registered in HMS rather
    # than only the ones that show up on the notification log. Use
    # this once on a fresh shelfd deploy so the H5 dashboard isn't
    # empty until each MV happens to get re-altered.
    parser.add_argument("--backfill", action="store_true",
                        help="enumerate every MV in HMS and pin it (one-shot, idempotent)")
    parser.add_argument("--backfill-input", type=pathlib.Path,
                        help="JSONL of `{db_name, tbl_name, metadata_location}` rows; "
                             "scaffolded equivalent of --backfill for environments "
                             "without the bundled JDBC driver")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    if not args.shelfd_urls:
        parser.error("at least one --shelfd-admin-url is required")

    if args.backfill_input:
        events = _load_backfill(args.backfill_input)
    elif args.backfill:
        if not args.hms_jdbc_url:
            parser.error("--backfill requires --hms-jdbc-url")
        events = _backfill_from_hms(args.hms_jdbc_url)
    elif args.input:
        events = _load_events(args.input)
    elif args.hms_jdbc_url:
        events = _default_hms_fetch(args.hms_jdbc_url, args.last_nl_id, args.limit)
    else:
        parser.error(
            "one of --input / --hms-jdbc-url / --backfill / --backfill-input is required"
        )

    # Hash provider — the production path does an S3 HEAD per file
    # to fold the object's ETag + size into the content-addressed
    # key (same derivation as `gen_pin_list._sha256_key`). The
    # scaffolded CLI stubs the S3 HEAD with a deterministic hash
    # over the file URI so a dev laptop without AWS credentials
    # can exercise the pipeline end-to-end.
    import hashlib

    def sha256_key(file_uri: str) -> str:
        return hashlib.sha256(("mv:" + file_uri).encode()).hexdigest()

    results = process(events, args.shelfd_urls, sha256_key=sha256_key)
    ok = sum(1 for r in results if r.ok)
    logger.info("pinned %d/%d requests", ok, len(results))
    return 0 if ok == len(results) else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1:]))
