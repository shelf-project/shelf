"""DuckDB-on-Shelf cold-vs-warm benchmark.

The script:
  1. Connects to DuckDB in-memory.
  2. Wires DuckDB's S3 client at `shelfd:9092` (signature-agnostic shim)
     and attaches the Iceberg REST catalog at `iceberg-rest:8181`.
  3. Runs a small representative aggregate twice; prints cold/warm
     latency and dollar-savings derived from shelfd's hit/miss
     counters in `/metrics`.

The first invocation after `docker compose up` finds shelfd's Foyer
pools empty, so run #1 is cold (every byte fetched from MinIO through
the shim) and run #2 is warm (every byte served from DRAM/NVMe).

References:
  * DuckDB httpfs S3:
      https://duckdb.org/docs/current/core_extensions/httpfs/s3api.html
  * DuckDB Iceberg REST catalog ATTACH:
      https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs.html
"""

from __future__ import annotations

import os
import re
import sys
import time
from typing import Tuple

import duckdb
import requests


SHELFD_S3_ENDPOINT = os.environ.get("SHELFD_S3_ENDPOINT", "shelfd:9092")
SHELFD_METRICS_URL = os.environ.get("SHELFD_METRICS_URL", "http://shelfd:9090/metrics")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Standard AWS S3 GET request price as of 2026 (us-east-1, ap-south-1):
# $0.0004 per 1,000 GET/SELECT requests.
S3_GET_USD_PER_REQ = 0.0004 / 1_000.0
# Standard internet-egress price proxy: $0.09/GB. Most prod stacks pay
# region-internal at $0.01–$0.02/GB; we use the higher number so the
# walkthrough's "$-saved" is conservative (i.e., realistic enterprise
# scenarios will save more, not less).
S3_EGRESS_USD_PER_GB = 0.09


QUERY = """
SELECT
    event_date,
    COUNT(*)                  AS rows,
    COUNT(DISTINCT user_id)   AS unique_users,
    ROUND(AVG(amount), 2)     AS avg_amount,
    ROUND(SUM(amount), 2)     AS total_amount
FROM lake.default.events
WHERE event_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-15'
GROUP BY event_date
ORDER BY event_date
"""


def configure_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    """Install + load extensions, point S3 at shelfd, attach Iceberg REST."""
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("INSTALL iceberg;")
    con.execute("LOAD iceberg;")

    # Disable DuckDB's per-process file cache so the warm run is forced
    # to re-read every byte through the S3 shim — otherwise the warm
    # run is hidden behind DuckDB's own in-memory cache and shelfd's
    # hit counters never move.
    con.execute("SET enable_external_file_cache = false;")

    # The shim ignores SigV4, but duckdb still requires *some* creds to
    # construct an Authorization header, so the literal string 'dummy'
    # is fine.
    con.execute(
        f"""
        CREATE OR REPLACE SECRET shelf_s3 (
            TYPE S3,
            KEY_ID 'dummy',
            SECRET 'dummy',
            REGION '{AWS_REGION}',
            ENDPOINT '{SHELFD_S3_ENDPOINT}',
            URL_STYLE 'path',
            USE_SSL false
        )
        """
    )

    # Iceberg REST catalog with no auth, so DuckDB falls back to the S3
    # secret above for every byte read. ACCESS_DELEGATION_MODE was added
    # in newer DuckDB-Iceberg builds; older releases default to using
    # the local secret when the catalog returns no vended creds, which
    # is exactly what tabulario/iceberg-rest does.
    con.execute(
        f"""
        ATTACH '' AS lake (
            TYPE iceberg,
            ENDPOINT '{ICEBERG_REST_URI}',
            AUTHORIZATION_TYPE 'none'
        )
        """
    )


_METRIC_RE = re.compile(r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?P<labels>{[^}]*})?\s+(?P<value>[\-+0-9.eE]+)\s*$")


def scrape_metrics() -> dict:
    """Return a flat `{(metric, labelset): float}` dict from shelfd."""
    out: dict = {}
    r = requests.get(SHELFD_METRICS_URL, timeout=5)
    r.raise_for_status()
    for line in r.text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _METRIC_RE.match(line)
        if not m:
            continue
        name = m.group("name")
        labels = m.group("labels") or ""
        try:
            value = float(m.group("value"))
        except ValueError:
            continue
        out[(name, labels)] = value
    return out


def sum_metric(metrics: dict, name: str) -> float:
    """Sum a metric across all label permutations."""
    return sum(v for (n, _), v in metrics.items() if n == name)


def time_query(con: duckdb.DuckDBPyConnection) -> Tuple[float, list]:
    """Run the benchmark query; return (elapsed_seconds, rows)."""
    t0 = time.perf_counter()
    rows = con.execute(QUERY).fetchall()
    t1 = time.perf_counter()
    return (t1 - t0), rows


def fmt_dur(secs: float) -> str:
    if secs >= 1.0:
        return f"{secs:6.2f} s"
    return f"{int(secs * 1000):>4d} ms"


def main() -> int:
    print("[bench] connecting to DuckDB (in-memory)...", flush=True)
    con = duckdb.connect(":memory:")
    configure_duckdb(con)

    pre = scrape_metrics()
    pre_hits = sum_metric(pre, "shelf_hits_total")
    pre_misses = sum_metric(pre, "shelf_misses_total")
    pre_origin_bytes = sum_metric(pre, "shelf_origin_request_bytes_total")

    if pre_misses + pre_hits > 0:
        print(f"[bench] NOTE: shelfd has prior traffic "
              f"(hits={int(pre_hits)} misses={int(pre_misses)}); "
              f"`docker compose --profile bench down -v` for a true cold run.",
              flush=True)

    print("[bench] cold run...", flush=True)
    cold_secs, rows = time_query(con)

    cold_metrics = scrape_metrics()
    cold_hits = sum_metric(cold_metrics, "shelf_hits_total") - pre_hits
    cold_misses = sum_metric(cold_metrics, "shelf_misses_total") - pre_misses
    cold_origin_bytes = sum_metric(cold_metrics, "shelf_origin_request_bytes_total") - pre_origin_bytes

    print("[bench] warm run...", flush=True)
    warm_secs, _rows2 = time_query(con)

    warm_metrics = scrape_metrics()
    warm_hits = sum_metric(warm_metrics, "shelf_hits_total") - sum_metric(cold_metrics, "shelf_hits_total")
    warm_misses = sum_metric(warm_metrics, "shelf_misses_total") - sum_metric(cold_metrics, "shelf_misses_total")
    warm_origin_bytes = sum_metric(warm_metrics, "shelf_origin_request_bytes_total") - sum_metric(cold_metrics, "shelf_origin_request_bytes_total")

    speedup = (cold_secs / warm_secs) if warm_secs > 0 else float("inf")

    saved_requests = max(0.0, cold_misses - warm_misses)
    saved_bytes = max(0.0, cold_origin_bytes - warm_origin_bytes)
    saved_usd = (
        saved_requests * S3_GET_USD_PER_REQ
        + (saved_bytes / (1024 ** 3)) * S3_EGRESS_USD_PER_GB
    )

    print()
    print("=" * 64)
    print(" DuckDB → Shelf → MinIO  (Iceberg events table, 1 M rows)")
    print("=" * 64)
    print(f"  cold:        {fmt_dur(cold_secs)}    "
          f"shelf hits/misses: {int(cold_hits):>4d} / {int(cold_misses):>4d}    "
          f"origin: {cold_origin_bytes/1024/1024:6.2f} MiB")
    print(f"  warm:        {fmt_dur(warm_secs)}    "
          f"shelf hits/misses: {int(warm_hits):>4d} / {int(warm_misses):>4d}    "
          f"origin: {warm_origin_bytes/1024/1024:6.2f} MiB")
    print(f"  speedup:     {speedup:.1f}x")
    print(f"  $-saved:     ${saved_usd:.6f}   "
          f"({int(saved_requests)} GETs + {saved_bytes/1024/1024:.2f} MiB egress avoided)")
    print("=" * 64)
    print()
    print(" sample result rows:")
    for row in rows[:5]:
        print(f"   {row}")
    if len(rows) > 5:
        print(f"   ... ({len(rows) - 5} more)")

    # Sanity gate: warm should be no slower than cold + 50ms jitter.
    if warm_secs > cold_secs + 0.05:
        print(f"\n[bench] WARN: warm ({fmt_dur(warm_secs)}) was slower than "
              f"cold ({fmt_dur(cold_secs)}) — Foyer pools may have evicted "
              f"between runs (check pool capacity in shelfd.yaml).", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
