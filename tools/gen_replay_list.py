#!/usr/bin/env python3
"""Generate a replay list of S3 paths for shelf cache pre-warm (Stage 3a).

The output format is a flat JSON array of `{bucket, key, size_estimate,
access_count, table}` entries, sorted by `access_count DESC`. The companion
tool ``replay_pinlist.py`` consumes this file and issues HTTP GETs through the
shelf S3 shim (port 9092) to fault every entry into the rowgroup/metadata
pools, flipping cold-start cost from O(window) to O(seconds).

This is **not** the same artifact as ``gen_pin_list.py``. That tool emits
shelfd's strict ``key_hex``/``pool`` PinListDoc consumed by ``PinListLoader``
to lock files into the cache (sha256 content-addressed, never evicted). This
tool emits S3 paths for *replay* prewarm — entries are populated by serving
real GET traffic and may be evicted under pressure.

Source of paths
---------------
``cdp.trino_logs.trino_queries`` (Iceberg, queried via Trino REST) is the only
honest source of "what shelf actually saw" since ``physicalInputBytes`` is
recorded per (catalog, schema, table) in ``inputs_json``. Trino does not
publish a ``trino_splits`` table; per-split paths are not retrievable after
the fact. We fall back to the always-read planning paths instead:

* ``metadata.json`` (current snapshot's metadata) — resolved via
  ``SHOW CREATE TABLE`` ``metadata_location`` property.
* The current snapshot's ``manifest_list`` Avro path — resolved via the
  Iceberg ``$snapshots`` system table.
* All ``manifests`` for the current snapshot — resolved via the Iceberg
  ``$manifests`` system table.

These three layers are read on every query that touches a table (planning
phase) regardless of predicate pushdown, so warming them eliminates the
"first-touch" S3 stall that drives the cold-cache thundering herd post
helm upgrade / pod restart. Data files are intentionally NOT included —
predicate pushdown means data-file reads are query-specific and should not
be eagerly warmed (the working set would explode past pool capacity).

CLI
---

    python3 gen_replay_list.py \\
        --replica rep-3 \\
        --catalog cdp \\
        --days 7 \\
        --top 10000 \\
        --out /tmp/replay-rep3.json

Use ``--source trino`` (default) or ``--source grafana-mysql`` to switch
between the Iceberg ``trino_queries`` (richer ``inputs_json``) and the
MySQL-backed mirror behind the Grafana datasource (faster but coarser —
no per-table breakdown). The Trino path is the only one that produces
useful output; the Grafana path is a fallback for when Trino is unhealthy.

Auth
----
Trino host/port/user/password and Grafana service-account token are read
from ``~/.cursor/mcp.json`` (``mcp-trino`` and ``grafana`` server blocks).
No credentials are accepted on the CLI — keeping them off argv per the
"never store credentials in files, commits, or notes" workspace rule.

Read-only: this tool issues only ``SHOW CREATE TABLE`` and ``SELECT``
against Trino. It never mutates the cluster.
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import ssl
import sys
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

LOG = logging.getLogger("shelf.gen_replay_list")

DEFAULT_MCP_JSON = os.path.expanduser("~/.cursor/mcp.json")
DEFAULT_LOGS_TABLE = "cdp.trino_logs.trino_queries"


# ---------------------------------------------------------------------------
# Auth + thin Trino REST client (synchronous, polls nextUri until done).
# ---------------------------------------------------------------------------


@dataclass
class TrinoCreds:
    host: str
    port: int
    user: str
    password: str | None
    scheme: str = "https"
    insecure: bool = True

    @property
    def base(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"

    @property
    def auth_header(self) -> str | None:
        if not self.password:
            return None
        token = base64.b64encode(f"{self.user}:{self.password}".encode()).decode()
        return f"Basic {token}"


def load_trino_creds(mcp_json_path: str = DEFAULT_MCP_JSON) -> TrinoCreds:
    with open(mcp_json_path) as f:
        cfg = json.load(f)
    env = cfg["mcpServers"]["mcp-trino"]["env"]
    return TrinoCreds(
        host=env["TRINO_HOST"],
        port=int(env["TRINO_PORT"]),
        user=env["TRINO_USER"],
        password=env.get("TRINO_PASSWORD"),
        scheme=env.get("TRINO_SCHEME", "https"),
        insecure=env.get("TRINO_SSL_INSECURE", "true").lower() == "true",
    )


def load_grafana_token(mcp_json_path: str = DEFAULT_MCP_JSON) -> str:
    with open(mcp_json_path) as f:
        cfg = json.load(f)
    return cfg["mcpServers"]["grafana"]["env"]["GRAFANA_SERVICE_ACCOUNT_TOKEN"]


def _trino_ssl_ctx(creds: TrinoCreds) -> ssl.SSLContext | None:
    if creds.scheme != "https":
        return None
    ctx = ssl.create_default_context()
    if creds.insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def trino_query(
    creds: TrinoCreds,
    sql: str,
    catalog: str | None = None,
    schema: str | None = None,
    timeout: int = 60,
    poll_cap: int = 600,
) -> tuple[list[str], list[list[Any]]]:
    """Run a Trino SELECT and return (column_names, rows).

    Polls ``nextUri`` until the statement completes. Raises ``RuntimeError``
    on Trino-side error or after ``poll_cap`` hops (defensive against
    stuck queries).
    """
    headers: dict[str, str] = {
        "X-Trino-User": creds.user,
        "Content-Type": "text/plain",
    }
    if catalog:
        headers["X-Trino-Catalog"] = catalog
    if schema:
        headers["X-Trino-Schema"] = schema
    if creds.auth_header:
        headers["Authorization"] = creds.auth_header

    ctx = _trino_ssl_ctx(creds)
    req = Request(
        f"{creds.base}/v1/statement",
        data=sql.encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(req, context=ctx, timeout=timeout) as r:
            data = json.loads(r.read())
    except HTTPError as e:
        raise RuntimeError(f"trino submit HTTP {e.code}: {e.read().decode()[:500]}")
    except URLError as e:
        raise RuntimeError(f"trino submit URLError: {e}")

    cols: list[str] | None = None
    rows: list[list[Any]] = []
    hops = 0
    while True:
        if cols is None and "columns" in data:
            cols = [c["name"] for c in data["columns"]]
        if "data" in data:
            rows.extend(data["data"])
        nxt = data.get("nextUri")
        err = data.get("error")
        if err:
            raise RuntimeError(f"trino error: {err.get('message')}")
        if not nxt:
            break
        hops += 1
        if hops > poll_cap:
            raise RuntimeError(f"trino polled > {poll_cap} hops, giving up")
        try:
            with urlopen(
                Request(nxt, headers=headers), context=ctx, timeout=timeout
            ) as r:
                data = json.loads(r.read())
        except HTTPError as e:
            raise RuntimeError(f"trino poll HTTP {e.code}: {e.read().decode()[:500]}")
    return (cols or []), rows


# ---------------------------------------------------------------------------
# Top-N tables by access volume.
# ---------------------------------------------------------------------------


@dataclass
class TableRank:
    catalog: str
    schema: str
    table: str
    queries: int
    scan_bytes: int

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


def replica_to_environment(replica: str) -> str:
    """Map ``rep-N`` / ``replica-N`` / ``replicaN`` to ``replicaN``."""
    m = re.match(r"^(?:rep|replica)-?(\d+)$", replica)
    if not m:
        raise ValueError(f"unrecognized --replica value: {replica!r}")
    return f"replica{m.group(1)}"


def top_tables_via_trino(
    creds: TrinoCreds,
    logs_table: str,
    catalog_filter: str,
    environment: str,
    days: int,
    top_n: int,
) -> list[TableRank]:
    """Rank input tables by ``SUM(physicalInputBytes) * COUNT(*)`` over a window.

    Filters on:
    * ``query_state = 'FINISHED'`` (failed queries don't tell us shelf demand)
    * ``environment = '<replica>'`` (per-replica pin lists; e.g. rep-3 has its
      own working set distinct from rep-0)
    * Catalog match on the *input* (ignores the catalog the query was issued
      under, since dbt sometimes runs on cdp_shelf and reads cdp tables)
    """
    sql = f"""
SELECT json_extract_scalar(input_obj, '$.catalogName') AS catalog,
       json_extract_scalar(input_obj, '$.schema')      AS schema_name,
       json_extract_scalar(input_obj, '$.table')       AS table_name,
       COUNT(*)                                        AS queries_in_window,
       SUM(coalesce(
           cast(json_extract_scalar(input_obj, '$.physicalInputBytes') AS bigint),
           0
       )) AS scan_bytes_in_window
FROM {logs_table} q
CROSS JOIN UNNEST(cast(json_parse(q.inputs_json) AS ARRAY(JSON))) AS t(input_obj)
WHERE q.query_date >= current_date - INTERVAL '{int(days)}' DAY
  AND q.query_state = 'FINISHED'
  AND q.environment = '{environment}'
  AND q.inputs_json IS NOT NULL
  AND json_extract_scalar(input_obj, '$.catalogName') = '{catalog_filter}'
  AND json_extract_scalar(input_obj, '$.schema')      IS NOT NULL
  AND json_extract_scalar(input_obj, '$.table')       IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY scan_bytes_in_window * queries_in_window DESC
LIMIT {int(top_n)}
"""
    cols, rows = trino_query(creds, sql, catalog="cdp", schema="trino_logs")
    LOG.info("top_tables_via_trino: %d rows", len(rows))
    out: list[TableRank] = []
    for r in rows:
        out.append(
            TableRank(
                catalog=r[0],
                schema=r[1],
                table=r[2],
                queries=int(r[3]) if r[3] is not None else 0,
                scan_bytes=int(r[4]) if r[4] is not None else 0,
            )
        )
    return out


def top_tables_via_grafana_mysql(token: str, environment: str, days: int) -> list[TableRank]:
    """Fallback: aggregate query count per replica from the MySQL mirror.

    The MySQL ``trino_queries`` table does NOT carry ``inputs_json``, so this
    path cannot return per-table breakdown. It returns a single synthetic
    ``TableRank`` with ``schema='?'`` ``table='?'`` and the replica's total
    query count — useful for sanity-checking that the Trino-side rank pulled
    the right window, and as a hard-block signal if Trino is down.
    """
    grafana_url = "https://platform-grafana.penpencil.co"
    ds_uid = "fejomnfupqf40a"
    body = {
        "queries": [
            {
                "refId": "A",
                "datasource": {"uid": ds_uid, "type": "mysql"},
                "rawSql": (
                    "SELECT environment AS replica, "
                    "COUNT(*) AS queries_in_window "
                    "FROM trino_queries "
                    "WHERE STR_TO_DATE(LEFT(query_id,15),'%Y%m%d_%H%i%s') > "
                    f"  DATE_SUB(NOW(), INTERVAL {int(days)} DAY) "
                    f"  AND environment = '{environment}' "
                    "  AND query_state = 'FINISHED' "
                    "GROUP BY environment"
                ),
                "format": "table",
            }
        ],
        "from": f"now-{int(days)}d",
        "to": "now",
    }
    req = Request(
        f"{grafana_url}/api/ds/query",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-Grafana-Org-Id": "1",
        },
    )
    with urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read())
    frames = data.get("results", {}).get("A", {}).get("frames", [])
    if not frames or not frames[0].get("data", {}).get("values"):
        LOG.warning("grafana-mysql fallback returned no rows")
        return []
    queries_in_window = int(frames[0]["data"]["values"][1][0])
    LOG.info(
        "grafana-mysql fallback: %s saw %d FINISHED queries in last %dd",
        environment,
        queries_in_window,
        days,
    )
    return [
        TableRank(
            catalog="?",
            schema="?",
            table="?",
            queries=queries_in_window,
            scan_bytes=0,
        )
    ]


# ---------------------------------------------------------------------------
# Iceberg metadata path resolution (no boto3 — uses Trino system tables).
# ---------------------------------------------------------------------------


_METADATA_LOC_RE = re.compile(r"metadata_location\s*=\s*'([^']+)'", re.IGNORECASE)


@dataclass
class S3Path:
    bucket: str
    key: str

    @classmethod
    def parse(cls, uri: str) -> "S3Path | None":
        p = urlparse(uri)
        if p.scheme not in ("s3", "s3a"):
            return None
        return cls(bucket=p.netloc, key=p.path.lstrip("/"))


@dataclass
class TableMetadataPaths:
    table: TableRank
    metadata_json: S3Path | None = None
    manifest_list: S3Path | None = None
    manifests: list[S3Path] = field(default_factory=list)


def resolve_metadata_paths(creds: TrinoCreds, t: TableRank) -> TableMetadataPaths:
    """Resolve metadata.json + manifest-list + manifests for one table.

    Each step is best-effort: a failure on one layer does not abort the
    others. A table that has no current snapshot (newly created, never
    written) yields only the metadata.json path.
    """
    paths = TableMetadataPaths(table=t)

    # 1. metadata.json via SHOW CREATE TABLE
    try:
        _, rows = trino_query(
            creds, f'SHOW CREATE TABLE {t.catalog}.{t.schema}."{t.table}"'
        )
        ddl = "\n".join(r[0] for r in rows if r and isinstance(r[0], str))
        m = _METADATA_LOC_RE.search(ddl)
        if m:
            paths.metadata_json = S3Path.parse(m.group(1))
    except Exception as exc:
        LOG.warning("SHOW CREATE TABLE %s failed: %s", t.fqn, exc)

    # 2. current snapshot's manifest_list via $snapshots system table
    try:
        _, rows = trino_query(
            creds,
            f'SELECT manifest_list FROM {t.catalog}.{t.schema}."{t.table}$snapshots" '
            "ORDER BY committed_at DESC LIMIT 1",
        )
        if rows and rows[0] and rows[0][0]:
            paths.manifest_list = S3Path.parse(rows[0][0])
    except Exception as exc:
        LOG.warning("$snapshots query for %s failed: %s", t.fqn, exc)

    # 3. all manifests for the current snapshot via $manifests
    try:
        _, rows = trino_query(
            creds,
            f'SELECT path FROM {t.catalog}.{t.schema}."{t.table}$manifests"',
        )
        for r in rows:
            if r and r[0]:
                p = S3Path.parse(r[0])
                if p:
                    paths.manifests.append(p)
    except Exception as exc:
        LOG.warning("$manifests query for %s failed: %s", t.fqn, exc)

    return paths


# ---------------------------------------------------------------------------
# Output assembly.
# ---------------------------------------------------------------------------


def assemble_replay_entries(per_table: list[TableMetadataPaths]) -> list[dict]:
    """Flatten resolved paths into the replay-list output schema.

    Sorted by ``access_count DESC`` so replay starts with the highest-value
    paths and produces visible hit-rate gains early in a window.
    """
    entries: list[dict] = []
    for tmp in per_table:
        ac = tmp.table.queries
        fqn = tmp.table.fqn

        def _emit(p: S3Path | None) -> None:
            if p is None:
                return
            entries.append(
                {
                    "bucket": p.bucket,
                    "key": p.key,
                    "size_estimate": None,
                    "access_count": ac,
                    "table": fqn,
                }
            )

        _emit(tmp.metadata_json)
        _emit(tmp.manifest_list)
        for mp in tmp.manifests:
            _emit(mp)

    entries.sort(key=lambda e: (-e["access_count"], e["table"], e["key"]))
    return entries


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a per-replica S3 replay list for shelf prewarm.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--replica",
        required=True,
        help="rep-N (where N is 0..3). Filters trino_queries.environment.",
    )
    parser.add_argument(
        "--catalog",
        default="cdp",
        help="Iceberg catalog whose paths we want to warm (default: cdp).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Look-back window in days (default: 7).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10000,
        help="Max output entries (default: 10000). Tables resolved = top/N "
        "where N is rough avg manifests/table; pass --top-tables to control.",
    )
    parser.add_argument(
        "--top-tables",
        type=int,
        default=200,
        help="Max distinct tables to resolve metadata for (default: 200).",
    )
    parser.add_argument(
        "--source",
        default="trino",
        choices=("trino", "grafana-mysql"),
        help="Source for the table-rank query (default: trino).",
    )
    parser.add_argument(
        "--logs-table",
        default=DEFAULT_LOGS_TABLE,
        help=f"Fully qualified Trino logs table (default: {DEFAULT_LOGS_TABLE}).",
    )
    parser.add_argument(
        "--mcp-json",
        default=DEFAULT_MCP_JSON,
        help="Path to MCP config holding Trino + Grafana creds.",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output JSON path. Use - for stdout.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    environment = replica_to_environment(args.replica)
    LOG.info(
        "replica=%s -> environment=%s, catalog=%s, days=%d, top=%d, top_tables=%d",
        args.replica,
        environment,
        args.catalog,
        args.days,
        args.top,
        args.top_tables,
    )

    if args.source == "trino":
        creds = load_trino_creds(args.mcp_json)
        t0 = time.monotonic()
        ranks = top_tables_via_trino(
            creds,
            args.logs_table,
            args.catalog,
            environment,
            args.days,
            args.top_tables,
        )
        LOG.info("ranking %d tables in %.1fs", len(ranks), time.monotonic() - t0)

        per_table: list[TableMetadataPaths] = []
        for i, t in enumerate(ranks):
            try:
                tmp = resolve_metadata_paths(creds, t)
            except Exception as exc:
                LOG.warning("resolve %s failed: %s", t.fqn, exc)
                continue
            per_table.append(tmp)
            if (i + 1) % 25 == 0:
                LOG.info("resolved metadata for %d/%d tables", i + 1, len(ranks))

        entries = assemble_replay_entries(per_table)
    else:
        token = load_grafana_token(args.mcp_json)
        ranks = top_tables_via_grafana_mysql(token, environment, args.days)
        if not ranks:
            LOG.error("grafana-mysql fallback returned no rows; aborting")
            return 2
        LOG.warning(
            "grafana-mysql source produces NO per-table breakdown; "
            "output will be empty. Use --source trino for a usable replay list."
        )
        entries = []

    if len(entries) > args.top:
        LOG.info("trimming entries %d -> %d (--top)", len(entries), args.top)
        entries = entries[: args.top]

    payload = json.dumps(entries, indent=2)
    if args.out == "-":
        sys.stdout.write(payload)
        sys.stdout.write("\n")
    else:
        with open(args.out, "w") as f:
            f.write(payload)
            f.write("\n")
        LOG.info(
            "wrote %d entries (%d distinct tables) to %s",
            len(entries),
            len({e["table"] for e in entries}),
            args.out,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
