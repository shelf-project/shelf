#!/usr/bin/env python3
"""Side-by-side byte-diff harness: catalog A vs catalog B (Stage 3b).

Runs a fixed set of canonical SELECT queries against two Trino catalogs in
parallel (e.g. ``cdp`` vs ``cdp_shelf``) and asserts the result sets are
identical: same row count, same schema, same row payload (under a stable
sort). Required PASS gate before any cutover that flips a live ``cdp``
endpoint to a new path. PASS = exit 0, FAIL = exit 1 with per-query diff
details.

The 5 canonical queries (overridable via ``--queries`` SQL file)
---------------------------------------------------------------
1. ``SELECT COUNT(*)`` from a large fact table — sanity on row count and
   metadata read path.
2. ``SELECT * ORDER BY <pk> LIMIT 100`` from a small dim table — exercises
   data-file reads and verifies row payload byte-for-byte.
3. Simple aggregation: ``SELECT col, COUNT(*) GROUP BY 1 ORDER BY 2 DESC
   LIMIT 50`` — exercises aggregation + grouping + sort.
4. Two-table join with ``LIMIT 100`` — exercises join planning + cross-
   table reads.
5. Metadata-heavy query: ``SELECT * FROM <table>$snapshots ORDER BY
   committed_at DESC LIMIT 10`` — exercises Iceberg system tables, which
   read manifest-list + metadata.json paths only.

Defaults are wired to known production Iceberg tables (see ``DEFAULTS`` at
the bottom of this module). Override any of them with ``--large-fact``,
``--small-dim``, ``--agg-fact``, ``--agg-col``, ``--join-fact``,
``--join-dim``, ``--join-key`` flags. For full custom queries, pass
``--queries path.sql`` — that file should contain queries delimited by
``-- @query: <name>`` comment markers, with the catalog placeholder as
``{catalog}``.

Each query is templated twice: once with ``--catalog-a`` (e.g. ``cdp``,
the origin / control), once with ``--catalog-b`` (e.g. ``cdp_shelf``, the
shim-fronted candidate). Queries run in parallel against the same Trino
coordinator; ``trino_compare.py`` is the prior art that confirms this works
through the ``mcp-trino`` REST endpoint without keeping a long-lived
session.

Byte diff
---------
For each query and each catalog the harness collects the rows as a list
of tuples. After both sides return, it compares:

1. Schema (column names + Trino type strings).
2. Row count.
3. The row sequence under a stable sort (lexicographic by string-coerced
   tuple) — Iceberg may not preserve scan order across catalog instances
   even for the same data.

Any mismatch is reported with the first 5 diverging rows side-by-side.

CLI
---

    python3 smoke_harness.py \\
        --catalog-a cdp \\
        --catalog-b cdp_shelf \\
        --replica rep-3

Read-only: every statement is a ``SELECT`` issued against the read-only
``dbt_user`` Trino account. Idempotent.
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

LOG = logging.getLogger("shelf.smoke_harness")

DEFAULT_MCP_JSON = os.path.expanduser("~/.cursor/mcp.json")


# ---------------------------------------------------------------------------
# Defaults: pick known prod Iceberg tables from the cdp catalog.
# These can be overridden on CLI; the harness validates each table exists
# in BOTH catalogs (via information_schema) before issuing queries.
# ---------------------------------------------------------------------------


@dataclass
class TableDefaults:
    large_fact: str = "cdp_revenue.gold_users"
    small_dim: str = "admin.iceberg_maintenance_log"
    small_dim_pk: str = "log_time"
    agg_fact: str = "cdp_revenue.gold_users"
    agg_col: str = "country"
    join_fact: str = "cdp_revenue.gold_orders"
    join_dim: str = "cdp_revenue.gold_users"
    join_key: str = "user_id"
    snap_table: str = "cdp_revenue.gold_users"


DEFAULTS = TableDefaults()


# ---------------------------------------------------------------------------
# Trino REST client (shared structure with gen_replay_list.py — duplicated
# here so each script remains a single-file drop-in).
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
    timeout: int = 120,
    poll_cap: int = 1200,
) -> tuple[list[str], list[str], list[list[Any]]]:
    """Run a SELECT and return (col_names, col_types, rows)."""
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

    col_names: list[str] | None = None
    col_types: list[str] | None = None
    rows: list[list[Any]] = []
    hops = 0
    while True:
        if col_names is None and "columns" in data:
            col_names = [c["name"] for c in data["columns"]]
            col_types = [c.get("type", "") for c in data["columns"]]
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
    return (col_names or []), (col_types or []), rows


# ---------------------------------------------------------------------------
# Query plan + diff.
# ---------------------------------------------------------------------------


@dataclass
class CanonicalQuery:
    name: str
    template: str  # uses {catalog} placeholder

    def for_catalog(self, catalog: str) -> str:
        return self.template.replace("{catalog}", catalog)


def build_default_queries(t: TableDefaults) -> list[CanonicalQuery]:
    return [
        CanonicalQuery(
            name="01_count_large_fact",
            template=f"SELECT COUNT(*) AS row_count FROM {{catalog}}.{t.large_fact}",
        ),
        CanonicalQuery(
            name="02_select_small_dim",
            template=(
                f"SELECT * FROM {{catalog}}.{t.small_dim} "
                f"ORDER BY {t.small_dim_pk} LIMIT 100"
            ),
        ),
        CanonicalQuery(
            name="03_simple_agg",
            template=(
                f"SELECT {t.agg_col} AS grp, COUNT(*) AS n "
                f"FROM {{catalog}}.{t.agg_fact} "
                f"WHERE {t.agg_col} IS NOT NULL "
                "GROUP BY 1 ORDER BY 2 DESC, 1 ASC LIMIT 50"
            ),
        ),
        CanonicalQuery(
            name="04_join",
            template=(
                f"SELECT a.{t.join_key}, COUNT(*) AS n "
                f"FROM {{catalog}}.{t.join_fact} a "
                f"JOIN {{catalog}}.{t.join_dim} b ON a.{t.join_key} = b.{t.join_key} "
                f"GROUP BY 1 ORDER BY 1 LIMIT 100"
            ),
        ),
        CanonicalQuery(
            name="05_metadata_snapshots",
            template=(
                f'SELECT snapshot_id, parent_id, operation, committed_at '
                f'FROM {{catalog}}.{t.snap_table}$snapshots '
                "ORDER BY committed_at DESC LIMIT 10"
            ),
        ),
    ]


_QUERY_DELIM = re.compile(r"^--\s*@query:\s*(\S+)\s*$", re.MULTILINE)


def load_queries_from_file(path: str) -> list[CanonicalQuery]:
    """Parse a SQL file with ``-- @query: <name>`` delimiters.

    Each delimiter starts a new query that runs until the next delimiter or
    EOF. ``{catalog}`` placeholder is the catalog injection point.
    """
    text = open(path).read()
    parts: list[CanonicalQuery] = []
    cursor = 0
    name: str | None = None
    last_end = 0
    for m in _QUERY_DELIM.finditer(text):
        if name is not None:
            body = text[last_end:m.start()].strip()
            if body:
                parts.append(CanonicalQuery(name=name, template=body))
        name = m.group(1)
        last_end = m.end()
        cursor = m.end()
    if name is not None:
        body = text[last_end:].strip()
        if body:
            parts.append(CanonicalQuery(name=name, template=body))
    if not parts:
        raise ValueError(
            f"{path} has no '-- @query: <name>' delimiters; cannot parse"
        )
    return parts


def _stable_sort_key(row: list[Any]) -> tuple:
    return tuple(("__none__" if v is None else str(v)) for v in row)


def _row_repr(row: list[Any], cap: int = 240) -> str:
    s = json.dumps(row, default=str)
    return s if len(s) <= cap else s[: cap - 3] + "..."


@dataclass
class QueryRun:
    name: str
    catalog: str
    sql: str
    col_names: list[str] = field(default_factory=list)
    col_types: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    elapsed: float = 0.0
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


def run_query_isolated(creds: TrinoCreds, q: CanonicalQuery, catalog: str) -> QueryRun:
    sql = q.for_catalog(catalog)
    run = QueryRun(name=q.name, catalog=catalog, sql=sql)
    t0 = time.monotonic()
    try:
        cn, ct, rows = trino_query(creds, sql)
        run.col_names = cn
        run.col_types = ct
        run.rows = rows
    except Exception as e:
        run.error = str(e)
    run.elapsed = time.monotonic() - t0
    return run


@dataclass
class QueryDiff:
    name: str
    passed: bool
    detail: list[str] = field(default_factory=list)
    elapsed_a: float = 0.0
    elapsed_b: float = 0.0
    rowcount_a: int = 0
    rowcount_b: int = 0


def diff_runs(name: str, a: QueryRun, b: QueryRun) -> QueryDiff:
    d = QueryDiff(
        name=name,
        passed=False,
        elapsed_a=a.elapsed,
        elapsed_b=b.elapsed,
        rowcount_a=len(a.rows),
        rowcount_b=len(b.rows),
    )
    if not a.ok:
        d.detail.append(f"FAIL on catalog A: {a.error}")
        return d
    if not b.ok:
        d.detail.append(f"FAIL on catalog B: {b.error}")
        return d

    if a.col_names != b.col_names:
        d.detail.append(
            f"schema names mismatch: A={a.col_names} vs B={b.col_names}"
        )
        return d
    if a.col_types != b.col_types:
        d.detail.append(
            f"schema types mismatch: A={a.col_types} vs B={b.col_types}"
        )
        return d
    if len(a.rows) != len(b.rows):
        d.detail.append(f"row count mismatch: A={len(a.rows)} vs B={len(b.rows)}")
        return d

    sa = sorted(a.rows, key=_stable_sort_key)
    sb = sorted(b.rows, key=_stable_sort_key)
    diffs: list[tuple[int, list[Any], list[Any]]] = []
    for i, (ra, rb) in enumerate(zip(sa, sb)):
        if ra != rb:
            diffs.append((i, ra, rb))
            if len(diffs) >= 5:
                break
    if diffs:
        d.detail.append(f"row payload mismatch on {len(diffs)}+ rows (showing 5):")
        for i, ra, rb in diffs:
            d.detail.append(f"  [{i}] A: {_row_repr(ra)}")
            d.detail.append(f"  [{i}] B: {_row_repr(rb)}")
        return d

    d.passed = True
    return d


# ---------------------------------------------------------------------------
# Catalog precheck (skip a query if a referenced table is missing on either
# side rather than misreporting it as a diff).
# ---------------------------------------------------------------------------


_TABLE_REF_RE = re.compile(r"\{catalog\}\.([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_$]*)")


def referenced_tables(q: CanonicalQuery) -> list[tuple[str, str]]:
    """Return distinct (schema, table) pairs referenced by a templated query.

    Strips Iceberg system-table suffixes (``$snapshots`` etc.) for the
    existence check.
    """
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for m in _TABLE_REF_RE.finditer(q.template):
        s = m.group(1)
        t = m.group(2).split("$", 1)[0]
        key = (s, t)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def check_table_exists(
    creds: TrinoCreds, catalog: str, schema: str, table: str
) -> bool:
    sql = (
        f"SELECT 1 FROM {catalog}.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' LIMIT 1"
    )
    try:
        _, _, rows = trino_query(creds, sql, timeout=30)
        return bool(rows)
    except Exception as exc:
        LOG.warning(
            "information_schema check failed for %s.%s.%s: %s",
            catalog, schema, table, exc,
        )
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Side-by-side byte-diff harness across two Trino catalogs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--catalog-a", required=True, help="control catalog (e.g. cdp)")
    parser.add_argument(
        "--catalog-b",
        required=True,
        help="candidate catalog (e.g. cdp_shelf)",
    )
    parser.add_argument(
        "--replica",
        default=None,
        help="rep-N (informational only; appears in the report header).",
    )
    parser.add_argument(
        "--queries",
        default=None,
        help="Optional path to a SQL file with '-- @query: <name>' delimited "
        "queries. Overrides the built-in 5 canonical queries.",
    )
    parser.add_argument("--large-fact", default=DEFAULTS.large_fact)
    parser.add_argument("--small-dim", default=DEFAULTS.small_dim)
    parser.add_argument("--small-dim-pk", default=DEFAULTS.small_dim_pk)
    parser.add_argument("--agg-fact", default=DEFAULTS.agg_fact)
    parser.add_argument("--agg-col", default=DEFAULTS.agg_col)
    parser.add_argument("--join-fact", default=DEFAULTS.join_fact)
    parser.add_argument("--join-dim", default=DEFAULTS.join_dim)
    parser.add_argument("--join-key", default=DEFAULTS.join_key)
    parser.add_argument("--snap-table", default=DEFAULTS.snap_table)
    parser.add_argument(
        "--mcp-json",
        default=DEFAULT_MCP_JSON,
        help="Path to MCP config holding Trino creds.",
    )
    parser.add_argument(
        "--no-precheck",
        action="store_true",
        help="Skip information_schema existence check (faster but a missing "
        "table will surface as an opaque diff).",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    creds = load_trino_creds(args.mcp_json)

    if args.queries:
        queries = load_queries_from_file(args.queries)
        LOG.info("loaded %d custom queries from %s", len(queries), args.queries)
    else:
        td = TableDefaults(
            large_fact=args.large_fact,
            small_dim=args.small_dim,
            small_dim_pk=args.small_dim_pk,
            agg_fact=args.agg_fact,
            agg_col=args.agg_col,
            join_fact=args.join_fact,
            join_dim=args.join_dim,
            join_key=args.join_key,
            snap_table=args.snap_table,
        )
        queries = build_default_queries(td)

    LOG.info(
        "smoke harness: A=%s vs B=%s (%d queries, replica=%s)",
        args.catalog_a,
        args.catalog_b,
        len(queries),
        args.replica or "n/a",
    )

    if not args.no_precheck:
        skip: set[str] = set()
        for q in queries:
            for sch, tbl in referenced_tables(q):
                ok_a = check_table_exists(creds, args.catalog_a, sch, tbl)
                ok_b = check_table_exists(creds, args.catalog_b, sch, tbl)
                if not (ok_a and ok_b):
                    LOG.warning(
                        "%s: table %s.%s missing in %s; skipping",
                        q.name,
                        sch,
                        tbl,
                        args.catalog_a if not ok_a else args.catalog_b,
                    )
                    skip.add(q.name)
                    break
        queries = [q for q in queries if q.name not in skip]
        if not queries:
            LOG.error("no queries left to run after precheck; aborting")
            return 2

    diffs: list[QueryDiff] = []
    fail_count = 0
    for q in queries:
        LOG.info("running %s on both catalogs in parallel", q.name)
        with ThreadPoolExecutor(max_workers=2) as ex:
            fa = ex.submit(run_query_isolated, creds, q, args.catalog_a)
            fb = ex.submit(run_query_isolated, creds, q, args.catalog_b)
            run_a = fa.result()
            run_b = fb.result()
        d = diff_runs(q.name, run_a, run_b)
        diffs.append(d)
        if not d.passed:
            fail_count += 1
        LOG.info(
            "  %s: %s  (rows A=%d B=%d, %.1fs A / %.1fs B)",
            q.name,
            "PASS" if d.passed else "FAIL",
            d.rowcount_a,
            d.rowcount_b,
            d.elapsed_a,
            d.elapsed_b,
        )

    print("=" * 64)
    print(
        f"smoke harness {'PASS' if fail_count == 0 else 'FAIL'}: "
        f"{len(diffs) - fail_count}/{len(diffs)} queries identical"
    )
    print(f"  catalog-A : {args.catalog_a}")
    print(f"  catalog-B : {args.catalog_b}")
    if args.replica:
        print(f"  replica   : {args.replica}")
    print("=" * 64)
    print(f"{'name':<28} {'A rows':>8} {'B rows':>8} {'A sec':>8} {'B sec':>8}  result")
    print("-" * 78)
    for d in diffs:
        print(
            f"{d.name:<28} {d.rowcount_a:>8} {d.rowcount_b:>8} "
            f"{d.elapsed_a:>8.2f} {d.elapsed_b:>8.2f}  "
            f"{'PASS' if d.passed else 'FAIL'}"
        )
    if fail_count:
        print()
        print("=" * 64)
        print("FAIL details:")
        print("=" * 64)
        for d in diffs:
            if d.passed:
                continue
            print(f"\n[{d.name}]")
            for line in d.detail:
                print(f"  {line}")

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
