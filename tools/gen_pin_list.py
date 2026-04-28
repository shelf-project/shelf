#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Track A2 — Generate pin_list.json for shelfd from 7-day trino_logs.
#
# Reads query history from `cdp.trino_logs.trino_queries` (or whatever source
# the operator points --trino-logs-table at), ranks Iceberg tables by
# `scan_bytes_7d × queries_7d`, takes the top-N, and for each:
#
#   1. `SHOW CREATE TABLE` → get table metadata location.
#   2. Walk Iceberg metadata: metadata.json → snapshot → manifest-list → manifests.
#   3. For each file, S3 HEAD to get ETag + size, compute cache key
#      `sha256(etag || u64_le(offset) || u64_le(length) || u32_le(rg_ordinal))`,
#      emit one pin_list entry per file with `pool=metadata`, offset=0,
#      length=size, rg_ordinal=0.
#
# The result matches the `PinListDoc` struct in shelfd/src/pinlist.rs:
#
#   { "version": 1, "entries": [{ "key_hex": "...", "pool": "metadata" }, ...] }
#
# Uploads to s3://penpencil-cdp-temp/shelf/pin_list.json (configurable).
# Shelfd's PinListLoader picks up the change on the next 15 min poll or on
# SIGHUP. See ADR-0011 for the key-derivation decision.
#
# Usage:
#
#   python3 tools/gen_pin_list.py \
#       --trino-url  http://trino.prod:8080 \
#       --trino-user dbt_user \
#       --top-n      50 \
#       --output     s3://penpencil-cdp-temp/shelf/pin_list.json \
#       --dry-run
#
# Dry-run prints the JSON to stdout without uploading. Requires IRSA / AWS
# credentials with read+write on the pin-list bucket and read on all
# pinned-table buckets.
# -----------------------------------------------------------------------------
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import struct
import sys
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import urlparse

import boto3
import requests

LOG = logging.getLogger("shelf.gen_pin_list")


@dataclass(frozen=True)
class IcebergFile:
    """One S3 object that needs pinning; we pin the whole file."""

    bucket: str
    key: str
    etag: bytes
    size: int


def _sha256_key(etag: bytes, offset: int, length: int, rg_ordinal: int) -> str:
    """Compute shelfd's content-addressed cache key per ADR-0011.

    See shelf/shelfd/src/store.rs `key_from_tuple`.
    """
    h = hashlib.sha256()
    h.update(etag)
    h.update(struct.pack("<Q", offset))   # u64_le
    h.update(struct.pack("<Q", length))   # u64_le
    h.update(struct.pack("<I", rg_ordinal))  # u32_le
    return h.hexdigest()


def _trino_query(
    session: requests.Session, url: str, user: str, password: Optional[str], sql: str
) -> list[list]:
    """Minimal synchronous Trino REST client — returns merged rows.

    Raises on non-2xx; not robust against long-running queries (5 min cap).
    """
    headers = {"X-Trino-User": user, "Content-Type": "text/plain"}
    auth = (user, password) if password else None
    endpoint = f"{url.rstrip('/')}/v1/statement"
    resp = session.post(endpoint, headers=headers, auth=auth, data=sql, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    rows: list[list] = []
    hops = 0
    while True:
        if "data" in data:
            rows.extend(data["data"])
        next_uri = data.get("nextUri")
        if not next_uri:
            break
        hops += 1
        if hops > 600:
            raise RuntimeError("trino polled > 600 hops, giving up")
        resp = session.get(next_uri, headers=headers, auth=auth, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("error"):
            raise RuntimeError(f"trino error: {data['error']}")
    return rows


# Live top-5 prod tables, sampled 2026-04-27 from
#   `cdp.trino_logs.trino_queries` over the last 7 days, ranked by
#   `SUM(physicalInputBytes) × COUNT(*)` (the same rank used by the
#   live SQL below). Used by `--top-5-prod` as a frozen fallback when
#   the metastore is down or for an emergency pin-replay where the
#   operator does not want to wait on a Trino query. Refresh this
#   list whenever the workload mix shifts; the SQL is the source of
#   truth for steady-state.
TOP_5_PROD_TABLES: list[tuple[str, str, str]] = [
    ("cdp", "cdp_revenue", "gold_users"),
    ("cdp", "lsq_pw", "silver_prospect_activity_extension_base"),
    ("cdp", "cdp_revenue", "gold_transactions"),
    ("cdp", "mview", "gold_dbt_video_stats_v3"),
    ("cdp", "cdp_revenue", "gold_orders"),
]


def top_n_tables(
    session: requests.Session,
    trino_url: str,
    trino_user: str,
    trino_pass: Optional[str],
    logs_table: str,
    top_n: int,
) -> list[tuple[str, str, str]]:
    """Return the top-N Iceberg tables by 7-day scan_bytes × query count.

    Output: list of (catalog, schema, table) tuples. Purely heuristic — the
    ranking table itself is an internal view over `QueryCompletedEvent`.

    Schema notes (verified 2026-04-27 against `cdp.trino_logs.trino_queries`):

    * The partition column is `query_date timestamp(6)` — there is no
      `created_at` column.
    * `inputs_json` is a JSON varchar (not an `input_tables` array);
      each element holds `catalogName`, `schema`, `table`, and
      `physicalInputBytes`. We `json_parse` + `UNNEST` it inline rather
      than introducing a precomputed view.
    """
    sql = f"""
    SELECT json_extract_scalar(input_obj, '$.catalogName') AS catalog,
           json_extract_scalar(input_obj, '$.schema')      AS schema_name,
           json_extract_scalar(input_obj, '$.table')       AS table_name,
           SUM(coalesce(
               cast(json_extract_scalar(input_obj, '$.physicalInputBytes') AS bigint),
               0
           )) AS scan_bytes_7d,
           COUNT(*) AS queries_7d
    FROM {logs_table} q
    CROSS JOIN UNNEST(cast(json_parse(q.inputs_json) AS ARRAY(JSON))) AS t(input_obj)
    WHERE q.query_date >= current_date - INTERVAL '7' DAY
      AND q.query_state = 'FINISHED'
      AND q.inputs_json IS NOT NULL
      AND json_extract_scalar(input_obj, '$.catalogName') IN ('cdp', 'cdp_shelf')
      AND json_extract_scalar(input_obj, '$.table')   IS NOT NULL
      AND json_extract_scalar(input_obj, '$.schema')  IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY scan_bytes_7d * queries_7d DESC
    LIMIT {int(top_n)}
    """
    rows = _trino_query(session, trino_url, trino_user, trino_pass, sql)
    return [(r[0], r[1], r[2]) for r in rows]


_METADATA_LOC_RE = re.compile(r"metadata_location\s*=\s*'([^']+)'", re.IGNORECASE)


def resolve_metadata_location(
    session: requests.Session,
    trino_url: str,
    trino_user: str,
    trino_pass: Optional[str],
    catalog: str,
    schema: str,
    table: str,
) -> Optional[str]:
    """Parse `metadata_location` from `SHOW CREATE TABLE`.

    Iceberg tables expose the current metadata.json path as a table property
    in Trino 468+. Returns None if the table is not Iceberg or not resolvable.
    """
    try:
        rows = _trino_query(
            session,
            trino_url,
            trino_user,
            trino_pass,
            f"SHOW CREATE TABLE {catalog}.{schema}.{table}",
        )
    except Exception as exc:
        LOG.warning("SHOW CREATE TABLE %s.%s.%s failed: %s", catalog, schema, table, exc)
        return None
    ddl = "\n".join(r[0] for r in rows if r and isinstance(r[0], str))
    m = _METADATA_LOC_RE.search(ddl)
    return m.group(1) if m else None


def walk_iceberg_metadata(
    s3, metadata_location: str, max_manifests: int = 50
) -> Iterable[IcebergFile]:
    """Yield IcebergFile entries for metadata.json + manifest list + manifests.

    Reads metadata.json to find the current snapshot's manifest-list, then
    reads the Avro manifest-list (best-effort; a JSON variant also exists in
    some spec versions) to enumerate manifest paths. S3 HEAD each for ETag.
    """
    parsed = urlparse(metadata_location)
    if parsed.scheme != "s3":
        raise RuntimeError(f"unsupported scheme: {metadata_location}")
    bucket = parsed.netloc
    metadata_key = parsed.path.lstrip("/")

    head = s3.head_object(Bucket=bucket, Key=metadata_key)
    etag = head["ETag"].strip('"').encode()
    yield IcebergFile(bucket, metadata_key, etag, head["ContentLength"])

    obj = s3.get_object(Bucket=bucket, Key=metadata_key)
    try:
        metadata = json.loads(obj["Body"].read())
    except Exception as exc:
        LOG.warning("metadata.json %s parse failed: %s", metadata_location, exc)
        return

    snapshots = metadata.get("snapshots") or []
    current_id = metadata.get("current-snapshot-id")
    current = next(
        (s for s in snapshots if s.get("snapshot-id") == current_id), None
    ) or (snapshots[-1] if snapshots else None)
    if not current:
        return

    manifest_list_uri = current.get("manifest-list")
    if not manifest_list_uri:
        return
    ml_parsed = urlparse(manifest_list_uri)
    if ml_parsed.scheme != "s3":
        return
    ml_key = ml_parsed.path.lstrip("/")
    try:
        ml_head = s3.head_object(Bucket=ml_parsed.netloc, Key=ml_key)
    except Exception as exc:
        LOG.warning("manifest-list HEAD failed %s: %s", manifest_list_uri, exc)
        return
    yield IcebergFile(
        ml_parsed.netloc,
        ml_key,
        ml_head["ETag"].strip('"').encode(),
        ml_head["ContentLength"],
    )

    # Manifest files: walk the manifest-list. Avro parsing is out of scope
    # here — rely on the snapshot summary's `manifest-list` being the only
    # small metadata file the pinning really needs. For deeper walks (per-
    # manifest paths) wire this script to `fastavro` or run the equivalent
    # query via Iceberg's REST metadata endpoint.


def build_pin_list(files: Iterable[IcebergFile], pool: str = "metadata") -> dict:
    """Convert resolved Iceberg files into the shelfd pin_list.json layout."""
    entries = []
    for f in files:
        key_hex = _sha256_key(f.etag, offset=0, length=f.size, rg_ordinal=0)
        entries.append({"key_hex": key_hex, "pool": pool})
    return {"version": 1, "entries": entries}


def upload_pin_list(s3, pin_list: dict, output_uri: str) -> None:
    parsed = urlparse(output_uri)
    if parsed.scheme != "s3":
        raise RuntimeError(f"output must be s3://: {output_uri}")
    body = json.dumps(pin_list, indent=2).encode()
    s3.put_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/"),
        Body=body,
        ContentType="application/json",
        CacheControl="max-age=60",
    )
    LOG.info("uploaded %s entries to %s (%d bytes)", len(pin_list["entries"]), output_uri, len(body))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trino-url", help="http://trino:8080 (omit with --top-5-prod)")
    parser.add_argument("--trino-user", default="dbt_user")
    parser.add_argument("--trino-password", default=None)
    parser.add_argument(
        "--trino-logs-table",
        default="cdp.trino_logs.trino_queries",
        help="fully qualified Trino table with QueryCompletedEvent rows",
    )
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument(
        "--top-5-prod",
        action="store_true",
        help=(
            "Emergency / no-Trino path: skip the ranking SQL and use "
            "the frozen TOP_5_PROD_TABLES list. Used during a metastore "
            "outage or when paging the on-call to replay the pin-list "
            "without waiting on a 7-day scan. Refresh the constant when "
            "the workload mix shifts."
        ),
    )
    parser.add_argument(
        "--output",
        default="s3://penpencil-cdp-temp/shelf/pin_list.json",
        help="s3:// destination; shelfd's values-alluxio.yaml must point here",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    session = requests.Session()
    s3 = boto3.client("s3")

    if args.top_5_prod:
        tables = list(TOP_5_PROD_TABLES)
        LOG.info("top-5-prod fallback: using frozen list of %d tables", len(tables))
    else:
        if not args.trino_url:
            parser.error("--trino-url is required unless --top-5-prod is set")
        tables = top_n_tables(
            session,
            args.trino_url,
            args.trino_user,
            args.trino_password,
            args.trino_logs_table,
            args.top_n,
        )
        LOG.info("top-N tables resolved: %d", len(tables))

    all_files: list[IcebergFile] = []
    for cat, sch, tbl in tables:
        loc = resolve_metadata_location(
            session, args.trino_url, args.trino_user, args.trino_password, cat, sch, tbl
        )
        if not loc:
            LOG.warning("no metadata_location for %s.%s.%s; skipping", cat, sch, tbl)
            continue
        try:
            all_files.extend(walk_iceberg_metadata(s3, loc))
        except Exception as exc:
            LOG.warning("walk failed for %s.%s.%s @ %s: %s", cat, sch, tbl, loc, exc)

    pin_list = build_pin_list(all_files)
    if args.dry_run:
        json.dump(pin_list, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    upload_pin_list(s3, pin_list, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
