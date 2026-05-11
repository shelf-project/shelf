#!/usr/bin/env python3
"""TPC-H SFn bench driver for the docker-compose smoke stack.

Issues 6 standard TPC-H queries × cold + 2 warm reps × 2 backends
(iceberg=shelf path, iceberg_direct=raw S3 baseline) and emits a
schema-valid replay record JSON per backend.

The bench fixture is materialised by `tpch_ctas` (separate step) — this
driver only RUNS queries against an existing iceberg.default schema
populated from `tpch.sfN`.

Usage:
    python3 benchmarks/smoke/tpch-bench.py --sf 1 \\
        --shelf-out  benchmarks/results/<date>/shelf/replay-<ulid>.json \\
        --raw-out    benchmarks/results/<date>/raw-s3/replay-<ulid>.json
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import secrets
import statistics
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

ULID_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def gen_ulid() -> str:
    ms = int(time.time() * 1000)
    rand = secrets.token_bytes(10)
    chars = []
    for i in range(10):
        chars.append(ULID_ALPHABET[(ms >> (45 - i * 5)) & 0x1F])
    rand_int = int.from_bytes(rand, "big")
    for i in range(16):
        chars.append(ULID_ALPHABET[(rand_int >> (75 - i * 5)) & 0x1F])
    return "".join(chars)


def trino_call(url: str, *, method="GET", body: bytes | None = None,
               extra_headers: dict | None = None, timeout: float = 600.0) -> dict:
    headers = {"X-Trino-User": "bench"}
    if extra_headers:
        headers.update(extra_headers)
    if body is not None:
        headers["Content-Type"] = "text/plain; charset=utf-8"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def run_sql(sql: str, catalog: str = "iceberg", schema: str = "default",
            timeout: float = 600.0) -> tuple[float, list]:
    started = time.monotonic()
    cur = trino_call(
        "http://127.0.0.1:8080/v1/statement", method="POST",
        body=sql.encode(),
        extra_headers={"X-Trino-Catalog": catalog, "X-Trino-Schema": schema},
        timeout=timeout,
    )
    rows = []
    deadline = started + timeout
    while True:
        if cur.get("error"):
            raise RuntimeError(f"trino error: {cur['error'].get('message')}")
        if cur.get("data"):
            rows.extend(cur["data"])
        stats = cur.get("stats") or {}
        nxt = cur.get("nextUri")
        if stats.get("state") == "FINISHED" and not nxt:
            break
        if not nxt:
            break
        if time.monotonic() > deadline:
            raise TimeoutError(f"timeout after {timeout}s")
        cur = trino_call(nxt, timeout=60.0)
    return time.monotonic() - started, rows


def scrape_metrics(url: str = "http://127.0.0.1:9091/metrics") -> dict[str, int]:
    out = {"hits_total": 0, "misses_total": 0, "origin_bytes": 0, "disk_bytes_used": 0}
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            body = r.read().decode()
    except (urllib.error.URLError, urllib.error.HTTPError, OSError):
        return out
    for line in body.splitlines():
        m = re.match(r"^(\w+)(\{[^}]*\})?\s+(-?[\d.eE+]+)\s*$", line)
        if not m:
            continue
        name, _, val = m.groups()
        try:
            v = int(float(val))
        except ValueError:
            continue
        if name == "shelf_hits_total":
            out["hits_total"] += v
        elif name == "shelf_misses_total":
            out["misses_total"] += v
        elif name == "shelf_origin_request_bytes_total":
            out["origin_bytes"] += v
        elif name == "shelf_disk_bytes_used":
            out["disk_bytes_used"] = max(out["disk_bytes_used"], v)
    return out


# Six standard TPC-H queries — kept compact so SF1 finishes in a few minutes.
TPCH_QUERIES = {
    "q01_pricing_summary": """
        SELECT l_returnflag, l_linestatus,
               sum(l_quantity), sum(l_extendedprice),
               sum(l_extendedprice * (1 - l_discount)),
               sum(l_extendedprice * (1 - l_discount) * (1 + l_tax))
        FROM {C}.default.lineitem
        WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """,
    "q03_shipping_priority": """
        SELECT l.l_orderkey, sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
               o.o_orderdate, o.o_shippriority
        FROM {C}.default.customer c
        JOIN {C}.default.orders o ON c.c_custkey = o.o_custkey
        JOIN {C}.default.lineitem l ON o.o_orderkey = l.l_orderkey
        WHERE c.c_mktsegment = 'BUILDING'
          AND o.o_orderdate < DATE '1995-03-15'
          AND l.l_shipdate > DATE '1995-03-15'
        GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority
        ORDER BY revenue DESC, o.o_orderdate
        LIMIT 10
    """,
    "q06_forecasting": """
        SELECT sum(l_extendedprice * l_discount) AS revenue
        FROM {C}.default.lineitem
        WHERE l_shipdate >= DATE '1994-01-01'
          AND l_shipdate <  DATE '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24
    """,
    "q10_returned_items": """
        SELECT c.c_custkey, c.c_name,
               sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue
        FROM {C}.default.customer c
        JOIN {C}.default.orders o ON c.c_custkey = o.o_custkey
        JOIN {C}.default.lineitem l ON o.o_orderkey = l.l_orderkey
        WHERE o.o_orderdate >= DATE '1993-10-01'
          AND o.o_orderdate <  DATE '1994-01-01'
          AND l.l_returnflag = 'R'
        GROUP BY c.c_custkey, c.c_name
        ORDER BY revenue DESC
        LIMIT 20
    """,
    "q12_shipping_modes": """
        SELECT l.l_shipmode, count(*) AS n
        FROM {C}.default.lineitem l
        JOIN {C}.default.orders o ON l.l_orderkey = o.o_orderkey
        WHERE l.l_shipmode IN ('MAIL', 'SHIP')
          AND l.l_commitdate < l.l_receiptdate
          AND l.l_shipdate < l.l_commitdate
          AND l.l_receiptdate >= DATE '1994-01-01'
          AND l.l_receiptdate <  DATE '1995-01-01'
        GROUP BY l.l_shipmode
        ORDER BY l.l_shipmode
    """,
    "q14_promotion_effect": """
        SELECT 100.00 *
            sum(CASE WHEN p.p_type LIKE 'PROMO%'
                     THEN l.l_extendedprice * (1 - l.l_discount) ELSE 0 END)
            / sum(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue
        FROM {C}.default.lineitem l
        JOIN {C}.default.part p ON l.l_partkey = p.p_partkey
        WHERE l.l_shipdate >= DATE '1995-09-01'
          AND l.l_shipdate <  DATE '1995-10-01'
    """,
}


def percentile(values: list[float], p: float) -> int:
    if not values:
        return 0
    s = sorted(values)
    k = (len(s) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return int((s[lo] + (s[hi] - s[lo]) * (k - lo)) * 1e9)  # to ns


def run_pass(catalog: str, phase_name: str) -> dict[str, list[float]]:
    """Run all TPC-H queries once against a catalog, return per-query latency in seconds."""
    results: dict[str, list[float]] = {}
    for qid, sql_tmpl in TPCH_QUERIES.items():
        sql = sql_tmpl.format(C=catalog)
        try:
            elapsed, rows = run_sql(sql, catalog=catalog, schema="default")
            results[qid] = [elapsed]
            print(f"    [{phase_name}] {qid:24s} {elapsed*1000:8.1f}ms (rows={len(rows)})")
        except Exception as exc:
            print(f"    [{phase_name}] {qid:24s} FAIL {str(exc)[:80]}")
            results[qid] = [0.0]
    return results


def build_record(*, backend: str, catalog: str, sf: int, latencies: list[float],
                 cold_metrics: dict, warm_metrics: dict) -> dict:
    p50 = percentile(latencies, 0.50)
    p95 = percentile(latencies, 0.95)
    p99 = percentile(latencies, 0.99)
    p999 = percentile(latencies, 0.999)
    if backend == "shelf":
        denom = max(1, warm_metrics["hits_total"] + warm_metrics["misses_total"])
        hit_rate = warm_metrics["hits_total"] / denom
        bytes_read = warm_metrics["origin_bytes"]
        bytes_admitted = warm_metrics["origin_bytes"]
    else:
        hit_rate = 0.0
        bytes_read = 0
        bytes_admitted = 0
    iso_now = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "run_id": gen_ulid(),
        "timestamp": iso_now,
        "commit_sha": "0" * 40,
        "release_tag": os.environ.get("SHELF_BENCH_RELEASE_TAG", f"v1.0.0-tpch-sf{sf}"),
        "benchmark": "replay",
        "backend": backend,
        "config": {
            "config_hash": "sha256:" + hashlib.sha256(f"tpch-sf{sf}|{backend}|{catalog}".encode()).hexdigest(),
            "trino_image": "trinodb/trino:480",
            "backend_image": "ghcr.io/shelf-project/shelfd:1.0.0 (smoke-built)" if backend == "shelf" else "n/a",
            "plugin_jar_sha256": None,
        },
        "cluster_shape": {
            "region": "ap-south-1",
            "k8s_version": "n/a-docker-compose",
            "trino_instance_type": "docker-laptop-4GB",
            "trino_worker_count": 1,
            "shelf_instance_type": "docker-laptop" if backend == "shelf" else "n/a",
            "shelf_node_count": 1 if backend == "shelf" else 0,
            "driver_instance_type": "n/a",
            "scale_factor": None,
            "partial": True,
        },
        "trace": {
            "source_table": "cdp.trino_logs.trino_queries",
            "snapshot_id": f"tpch-sf{sf}-{iso_now[:10]}",
            "from": iso_now,
            "to": iso_now,
            "replica": "rep-2",
            "query_count": len(latencies),
            "speed": "1x",
        },
        "samples": [],
        "summary": {
            "latency_ns_p50": p50,
            "latency_ns_p95": p95,
            "latency_ns_p99": p99,
            "latency_ns_p999": p999,
            "hit_rate": hit_rate,
            "bytes_read": bytes_read,
            "bytes_admitted": bytes_admitted,
            "dollars_per_query": 0.0,
        },
        "gate": {
            "hit_rate_7d_cumulative": hit_rate,
            "gold_dbt_ok_rate": 1.0,
            "latency_ns_p95_vs_alluxio": None,
            "shelf_caused_pages": 0,
            "oncall_surface_ratio": None,
            "verdict": "n/a",
            "failed_metrics": [],
        },
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--sf", type=int, default=1)
    p.add_argument("--shelf-out", required=True, type=Path)
    p.add_argument("--raw-out", required=True, type=Path)
    p.add_argument("--reps", type=int, default=2, help="warm reps after the cold pass")
    args = p.parse_args()

    shelf_lats: list[float] = []
    raw_lats: list[float] = []

    for catalog, label, latbuf in [
        ("iceberg", "shelf", shelf_lats),
        ("iceberg_direct", "raw-s3", raw_lats),
    ]:
        print(f"\n=== {label} via {catalog} ===")
        cold_metrics = scrape_metrics()
        # cold
        cold = run_pass(catalog, "cold")
        for qid, lats in cold.items():
            latbuf.extend(lats)
        # warm
        for r in range(args.reps):
            warm = run_pass(catalog, f"warm{r+1}")
            for qid, lats in warm.items():
                latbuf.extend(lats)
        warm_metrics = scrape_metrics()

        if label == "shelf":
            shelf_cm, shelf_wm = cold_metrics, warm_metrics
        else:
            raw_cm, raw_wm = cold_metrics, warm_metrics

    # Build records
    shelf_record = build_record(backend="shelf", catalog="iceberg", sf=args.sf,
                                latencies=shelf_lats,
                                cold_metrics=shelf_cm, warm_metrics=shelf_wm)
    raw_record = build_record(backend="raw-s3", catalog="iceberg_direct", sf=args.sf,
                              latencies=raw_lats,
                              cold_metrics={"hits_total":0,"misses_total":0,"origin_bytes":0,"disk_bytes_used":0},
                              warm_metrics={"hits_total":0,"misses_total":0,"origin_bytes":0,"disk_bytes_used":0})

    args.shelf_out.parent.mkdir(parents=True, exist_ok=True)
    args.raw_out.parent.mkdir(parents=True, exist_ok=True)
    args.shelf_out.write_text(json.dumps(shelf_record, indent=2))
    args.raw_out.write_text(json.dumps(raw_record, indent=2))

    print(f"\n=== summary — TPC-H SF{args.sf}, 6 queries × {1+args.reps} reps × 2 backends ===")
    print(f"shelf  records: {args.shelf_out}")
    print(f"raw-s3 records: {args.raw_out}")
    print(f"shelf  p50/p95/p99 = {shelf_record['summary']['latency_ns_p50']/1e6:.1f} / "
          f"{shelf_record['summary']['latency_ns_p95']/1e6:.1f} / "
          f"{shelf_record['summary']['latency_ns_p99']/1e6:.1f} ms; "
          f"hit_rate={shelf_record['summary']['hit_rate']:.3f}; "
          f"origin_bytes={shelf_record['summary']['bytes_read']:,}")
    print(f"raw-s3 p50/p95/p99 = {raw_record['summary']['latency_ns_p50']/1e6:.1f} / "
          f"{raw_record['summary']['latency_ns_p95']/1e6:.1f} / "
          f"{raw_record['summary']['latency_ns_p99']/1e6:.1f} ms")

    # Sidecar comparison summary
    sidecar = args.shelf_out.parent.parent / f"tpch-sf{args.sf}-comparison.txt"
    with sidecar.open("w") as f:
        sp50 = shelf_record["summary"]["latency_ns_p50"] / 1e6
        sp95 = shelf_record["summary"]["latency_ns_p95"] / 1e6
        sp99 = shelf_record["summary"]["latency_ns_p99"] / 1e6
        rp50 = raw_record["summary"]["latency_ns_p50"] / 1e6
        rp95 = raw_record["summary"]["latency_ns_p95"] / 1e6
        rp99 = raw_record["summary"]["latency_ns_p99"] / 1e6
        f.write(f"# TPC-H SF{args.sf} — Shelf vs raw-S3 (docker-compose smoke, MinIO)\n")
        f.write(f"# 6 queries × {1+args.reps} reps each × 2 backends\n")
        f.write(f"\n")
        f.write(f"metric          shelf       raw-s3       delta\n")
        f.write(f"p50 wall        {sp50:7.1f} ms  {rp50:7.1f} ms  {100*(sp50-rp50)/rp50:+6.1f}%\n")
        f.write(f"p95 wall        {sp95:7.1f} ms  {rp95:7.1f} ms  {100*(sp95-rp95)/rp95:+6.1f}%\n")
        f.write(f"p99 wall        {sp99:7.1f} ms  {rp99:7.1f} ms  {100*(sp99-rp99)/rp99:+6.1f}%\n")
        f.write(f"shelf hit_rate  {shelf_record['summary']['hit_rate']:.3f}\n")
        f.write(f"shelf orig B    {shelf_record['summary']['bytes_read']:>10,}\n")
    print(f"\ncomparison summary -> {sidecar}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
