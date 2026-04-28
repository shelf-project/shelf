"""Run a partitioned scan twice through Shelf and time each run.

This is the reference example for PyIceberg + Shelf. Two things matter:

1. The catalog is loaded with ``s3.endpoint=http://shelfd:9092``. PyIceberg
   forwards every ``s3.*`` property into PyArrow's ``S3FileSystem`` via
   ``pyiceberg.io.pyarrow._initialize_fs``, so all manifest and Parquet
   reads transit Shelf's signature-agnostic shim.

2. Path-style addressing is mandatory. Shelf's shim only speaks path-style;
   virtual-hosted-style would resolve the bucket via DNS. PyIceberg's
   property is ``s3.force-virtual-addressing`` (NOT ``s3.path-style-access`` —
   that name belongs to Java Iceberg and PyIceberg silently ignores it).
   Default is ``False`` (= path-style), which is what we want; we still
   set it explicitly below for documentation value.

Output: two timings + a delta of Shelf's hit/miss counters across the runs.
A working setup shows the second run faster than the first and the hit
counter materially higher.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

import requests


SHELF_ENDPOINT = os.environ["SHELF_ENDPOINT"]                # http://shelfd:9092
SHELFD_METRICS_URL = os.environ["SHELFD_METRICS_URL"]        # http://shelfd:9090/metrics
REST_URI = os.environ["ICEBERG_REST_URI"]
WAREHOUSE = f"s3://{os.environ['WAREHOUSE_BUCKET']}/"

ROW_FILTER = "date = '2024-01-15'"
TABLE = "demo.events"


def _scrape_counters() -> dict[str, float]:
    """Return current values of shelf_hits_total and shelf_misses_total.

    Counters are returned summed across all label sets — the bench only
    cares about the cluster-wide cold->warm delta, not per-pool slicing.
    """
    out = {"shelf_hits_total": 0.0, "shelf_misses_total": 0.0}
    try:
        resp = requests.get(SHELFD_METRICS_URL, timeout=5)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[bench] WARN: could not scrape {SHELFD_METRICS_URL}: {exc}", flush=True)
        return out
    for line in resp.text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        for name in out:
            if line.startswith(name + "{") or line.startswith(name + " "):
                try:
                    out[name] += float(line.rsplit(" ", 1)[1])
                except ValueError:
                    pass
                break
    return out


def _fresh_catalog() -> Any:
    """Build a brand-new catalog handle so PyArrow's S3 client cache cannot
    accidentally serve a cached request from a previous run within this
    process. We want every read to hit shelfd — that's the whole point.
    """
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "demo",
        **{
            "type": "rest",
            "uri": REST_URI,
            "warehouse": WAREHOUSE,
            "s3.endpoint": SHELF_ENDPOINT,
            "s3.access-key-id": "shelf-shim-ignores-this",
            "s3.secret-access-key": "shelf-shim-ignores-this",
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
            "s3.force-virtual-addressing": "false",
        },
    )


def _run_once(label: str) -> tuple[int, float, dict[str, float]]:
    cat = _fresh_catalog()
    tbl = cat.load_table(TABLE)

    pre = _scrape_counters()
    t0 = time.perf_counter()
    df = tbl.scan(row_filter=ROW_FILTER).to_pandas()
    elapsed = time.perf_counter() - t0
    post = _scrape_counters()

    delta = {k: post[k] - pre[k] for k in post}
    print(
        f"[{label}] rows={len(df):>6}  cols={len(df.columns)}  "
        f"elapsed={elapsed * 1000:7.1f} ms  "
        f"shelf_hits+={int(delta['shelf_hits_total'])}  "
        f"shelf_misses+={int(delta['shelf_misses_total'])}",
        flush=True,
    )
    return len(df), elapsed, delta


def main() -> int:
    print(f"[bench] table={TABLE} filter={ROW_FILTER!r} endpoint={SHELF_ENDPOINT}", flush=True)

    rows1, t1, delta1 = _run_once("cold")
    rows2, t2, delta2 = _run_once("warm")

    if rows1 != rows2:
        print(f"[bench] FAIL: row count mismatch cold={rows1} warm={rows2}", flush=True)
        return 2

    speedup = (t1 / t2) if t2 > 0 else float("inf")
    print(
        f"[bench] summary: cold={t1 * 1000:.1f} ms  warm={t2 * 1000:.1f} ms  "
        f"speedup={speedup:.2f}x  "
        f"hit_delta_cold={int(delta1['shelf_hits_total'])}  "
        f"hit_delta_warm={int(delta2['shelf_hits_total'])}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
