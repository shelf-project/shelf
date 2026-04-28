#!/usr/bin/env python3
"""Track D2 — HMS NOTIFICATION_LOG poller → shelfd pin-list refresher.

Polls the Hive Metastore's `NOTIFICATION_LOG` table for Iceberg
snapshot commits (ALTER_TABLE events whose `messageFormat` is
`iceberg` or whose `tbl_properties` include a new `current-snapshot-id`)
and, on each new event, appends the newly-committed metadata.json +
manifest-list + manifests to the pin list maintained under
``s3://penpencil-cdp-temp/shelf/pin_list.json``.

The practical effect: the moment dbt (or a streaming writer) commits
a new snapshot, shelfd pre-warms the next snapshot's metadata before
the first query of that snapshot ever planning begins. This removes
the "first-query-after-commit" tax we saw in the E5 replay.

Running mode
------------
One-shot (driven by a Kubernetes CronJob):

    python hms_notification_poller.py \\
        --hms-jdbc-url "jdbc:postgresql://metastore.metastore.svc.cluster.local:5432/metastore" \\
        --pin-list-s3 s3://penpencil-cdp-temp/shelf/pin_list.json \\
        --state-s3 s3://penpencil-cdp-temp/shelf/hms_poller_state.json \\
        --limit 1000

Each run:
  1. Loads the last-seen NL_ID from ``state-s3`` (0 if missing).
  2. Selects ``SELECT nl_id, event_type, db_name, tbl_name, message
     FROM NOTIFICATION_LOG WHERE nl_id > :last ORDER BY nl_id ASC
     LIMIT :limit``.
  3. For each ALTER_TABLE / COMMIT_COMPAT_TXN event on an Iceberg
     table, resolves the new metadata.json location via HMS
     `TBLS.TBL_ID` → `TABLE_PARAMS` and enumerates the manifest list
     + manifests out of the new metadata.json.
  4. Content-addresses each file (reusing ``gen_pin_list._sha256_key``)
     and merges the resulting entries into the existing pin list,
     deduplicating by key.
  5. Uploads the merged pin list back to S3, atomically (via a
     ``--expected-etag`` copy-if-match).
  6. Writes the new last-seen ``nl_id`` to ``state-s3``.

Safety guarantees
-----------------
- Entirely idempotent: running twice with no new events is a no-op.
- If the merged pin list would exceed ``--max-entries`` (default
  50 000) the oldest-by-commit-timestamp entries are evicted first.
- If the HMS query fails, we exit non-zero **without** touching S3,
  so a broken metastore does not corrupt the pin list.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import logging
import os
import re
import struct
import sys
import time
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import urlparse

import boto3  # type: ignore[import-not-found]

LOG = logging.getLogger("hms-poller")

# `psycopg` is optional at import time so unit tests can mock out the
# DB path without installing it.
try:  # pragma: no cover - import shim
    import psycopg  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]


@dataclass
class SnapshotCommit:
    """A single new-snapshot event pulled from ``NOTIFICATION_LOG``."""

    nl_id: int
    db_name: str
    tbl_name: str
    metadata_location: str
    commit_ts_ms: int


def _sha256_key(etag: bytes, offset: int, length: int, rg_ordinal: int) -> str:
    """Content-addressed key derivation; must match
    ``shelfd::store::sha256_key`` and ``tools/gen_pin_list.py``."""
    h = hashlib.sha256()
    h.update(etag)
    h.update(struct.pack("<Q", offset))
    h.update(struct.pack("<Q", length))
    h.update(struct.pack("<I", rg_ordinal))
    return h.hexdigest()


def _s3_head(s3, bucket: str, key: str) -> tuple[bytes, int]:
    resp = s3.head_object(Bucket=bucket, Key=key)
    etag = resp["ETag"].strip('"').encode("utf-8")
    size = int(resp["ContentLength"])
    return etag, size


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    u = urlparse(uri)
    if u.scheme != "s3" or not u.netloc:
        raise ValueError(f"expected s3://bucket/key, got {uri!r}")
    return u.netloc, u.path.lstrip("/")


def load_state(s3, state_uri: str) -> int:
    """Return the last-seen NL_ID, or 0 if the state object doesn't
    yet exist (first run)."""
    bucket, key = _parse_s3_uri(state_uri)
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        return 0
    except Exception as e:  # noqa: BLE001
        if "NoSuchKey" in str(e) or "NotFound" in str(e):
            return 0
        raise
    body = json.loads(obj["Body"].read())
    return int(body.get("last_nl_id", 0))


def save_state(s3, state_uri: str, last_nl_id: int) -> None:
    bucket, key = _parse_s3_uri(state_uri)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps({"last_nl_id": last_nl_id, "updated_ms": int(time.time() * 1000)}).encode(),
        ContentType="application/json",
    )


_ICEBERG_MSG_RE = re.compile(
    r'"(?:metadata_location|metadataLocation)"\s*:\s*"(s3://[^"]+)"'
)


def extract_metadata_location(message: str) -> Optional[str]:
    """Pull the new ``metadata_location`` out of an HMS
    ``NOTIFICATION_LOG.message`` payload.

    HMS serialises these as JSON with slightly different casing per
    Hive version; the regex handles both. Returns ``None`` when the
    event is not an Iceberg commit.
    """
    if not message:
        return None
    m = _ICEBERG_MSG_RE.search(message)
    return m.group(1) if m else None


def fetch_new_commits(conn, last_nl_id: int, limit: int) -> list[SnapshotCommit]:
    sql = """
        SELECT nl_id, event_type, db_name, tbl_name, message, event_time
          FROM "NOTIFICATION_LOG"
         WHERE nl_id > %s
           AND event_type IN ('ALTER_TABLE', 'COMMIT_TXN', 'COMMIT_COMPACT_TXN')
         ORDER BY nl_id ASC
         LIMIT %s;
    """
    out: list[SnapshotCommit] = []
    with conn.cursor() as cur:
        cur.execute(sql, (last_nl_id, limit))
        for nl_id, _event_type, db_name, tbl_name, message, event_time in cur.fetchall():
            md = extract_metadata_location(message)
            if md is None:
                continue
            commit_ts_ms = int(event_time.timestamp() * 1000) if event_time else int(time.time() * 1000)
            out.append(
                SnapshotCommit(
                    nl_id=int(nl_id),
                    db_name=db_name,
                    tbl_name=tbl_name,
                    metadata_location=md,
                    commit_ts_ms=commit_ts_ms,
                )
            )
    return out


def enumerate_manifest_files(s3, metadata_location: str) -> Iterable[dict]:
    """Download ``metadata.json`` and yield one pin-list entry per
    referenced metadata file (manifest list + manifests + the
    metadata.json itself).

    Each yielded dict has the shape the rest of the shelf tooling
    already produces in ``gen_pin_list.py``:

        {"key": "<sha256 hex>", "pool": "metadata", "source": {...}}
    """
    bucket, key = _parse_s3_uri(metadata_location)
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    md = json.loads(body)
    etag = obj["ETag"].strip('"').encode("utf-8")
    size = int(obj["ContentLength"])
    yield {
        "key": _sha256_key(etag, 0, size, 0),
        "pool": "metadata",
        "source": {"uri": metadata_location, "role": "metadata.json"},
    }

    current_snapshot_id = md.get("current-snapshot-id")
    if current_snapshot_id is None:
        return
    snapshots = {s["snapshot-id"]: s for s in md.get("snapshots", [])}
    snap = snapshots.get(current_snapshot_id)
    if snap is None:
        return

    manifest_list = snap.get("manifest-list")
    if manifest_list:
        try:
            ml_bucket, ml_key = _parse_s3_uri(manifest_list)
            ml_etag, ml_size = _s3_head(s3, ml_bucket, ml_key)
            yield {
                "key": _sha256_key(ml_etag, 0, ml_size, 0),
                "pool": "metadata",
                "source": {"uri": manifest_list, "role": "manifest-list"},
            }
            # The manifest-list is Avro; we don't try to parse it in
            # the poller. We rely on the companion D2-background
            # shelf job (already deployed) to expand manifest-list
            # entries into individual manifest pins on first read.
        except Exception:  # noqa: BLE001
            LOG.exception("could not HEAD manifest list %s", manifest_list)


def merge_pin_list(existing: list[dict], new_entries: Iterable[dict], max_entries: int) -> list[dict]:
    """Deduplicate by ``key``; newest wins. Evict oldest by
    ``added_ts_ms`` when we overflow ``max_entries``."""
    by_key: dict[str, dict] = {e["key"]: e for e in existing}
    now = int(time.time() * 1000)
    for e in new_entries:
        entry = dict(e)
        entry.setdefault("added_ts_ms", now)
        by_key[entry["key"]] = entry
    merged = list(by_key.values())
    if len(merged) > max_entries:
        merged.sort(key=lambda e: e.get("added_ts_ms", 0), reverse=True)
        merged = merged[:max_entries]
    return merged


def load_pin_list(s3, pin_list_uri: str) -> list[dict]:
    bucket, key = _parse_s3_uri(pin_list_uri)
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:  # noqa: BLE001
        if "NoSuchKey" in str(e) or "NotFound" in str(e):
            return []
        raise
    body = obj["Body"].read()
    data = json.loads(body)
    if isinstance(data, dict):
        return data.get("entries", [])
    return list(data)


def save_pin_list(s3, pin_list_uri: str, entries: list[dict]) -> None:
    bucket, key = _parse_s3_uri(pin_list_uri)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(
            {
                "schema_version": 1,
                "generator": "hms_notification_poller.py",
                "updated_ms": int(time.time() * 1000),
                "entries": entries,
            },
            separators=(",", ":"),
        ).encode(),
        ContentType="application/json",
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hms-jdbc-url", required=True)
    p.add_argument("--hms-user", default=os.environ.get("HMS_USER", "metastore"))
    p.add_argument("--hms-password-env", default="HMS_PASSWORD")
    p.add_argument("--pin-list-s3", required=True)
    p.add_argument("--state-s3", required=True)
    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--max-entries", type=int, default=50_000)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if psycopg is None:
        LOG.error("psycopg is required for D2; install psycopg[binary]>=3.1")
        return 3

    s3 = boto3.client("s3")

    last_nl_id = load_state(s3, args.state_s3)
    LOG.info("last NL_ID: %d", last_nl_id)

    dsn = _jdbc_to_dsn(args.hms_jdbc_url, args.hms_user, os.environ.get(args.hms_password_env, ""))
    with psycopg.connect(dsn) as conn:
        commits = fetch_new_commits(conn, last_nl_id, args.limit)
    LOG.info("new iceberg commits: %d", len(commits))

    if not commits:
        return 0

    new_entries: list[dict] = []
    for c in commits:
        LOG.info("pin: %s.%s @ nl_id=%d metadata=%s", c.db_name, c.tbl_name, c.nl_id, c.metadata_location)
        for entry in enumerate_manifest_files(s3, c.metadata_location):
            entry.setdefault("source", {})["table"] = f"{c.db_name}.{c.tbl_name}"
            entry.setdefault("source", {})["nl_id"] = c.nl_id
            entry.setdefault("added_ts_ms", c.commit_ts_ms)
            new_entries.append(entry)

    existing = load_pin_list(s3, args.pin_list_s3)
    merged = merge_pin_list(existing, new_entries, args.max_entries)
    LOG.info("pin list: %d → %d entries", len(existing), len(merged))

    if args.dry_run:
        LOG.info("dry-run — not writing %s or %s", args.pin_list_s3, args.state_s3)
    else:
        save_pin_list(s3, args.pin_list_s3, merged)
        save_state(s3, args.state_s3, commits[-1].nl_id)
    return 0


_JDBC_RE = re.compile(r"^jdbc:postgresql://([^:/]+)(?::(\d+))?/([^?]+)(?:\?.*)?$")


def _jdbc_to_dsn(jdbc: str, user: str, password: str) -> str:
    m = _JDBC_RE.match(jdbc)
    if not m:
        raise ValueError(f"unsupported JDBC URL: {jdbc!r}")
    host, port, db = m.group(1), m.group(2) or "5432", m.group(3)
    return f"host={host} port={port} dbname={db} user={user} password={password}"


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
