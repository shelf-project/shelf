"""Run a Daft `where -> groupby -> agg` query against an Iceberg table,
twice, with all S3 traffic pointed at shelfd's S3-compat shim.

What this script proves:

  1. Daft's native S3 client can talk to shelfd's signature-agnostic
     shim with `endpoint_url=http://shelfd:9092` + dummy creds +
     `force_virtual_addressing=False` (path style).
  2. Run #1 is cold: every Iceberg manifest, Parquet footer, and row
     group is fetched from MinIO through shelfd, populating the
     metadata + rowgroup pools.
  3. Run #2 is warm: the same byte ranges are served out of shelfd's
     Foyer cache. We confirm by scraping `shelf_hits_total` /
     `shelf_misses_total` from `/metrics` before and after each run.

Output is a small results table written to stdout. If shelfd's metrics
endpoint is unreachable we still print timings.
"""

from __future__ import annotations

import os
import re
import sys
import time
from typing import Dict

import requests
from pyiceberg.catalog import load_catalog

import daft
from daft import col
from daft.io import IOConfig, S3Config


SHELFD_HOST = os.environ.get("SHELFD_HOST", "shelfd")
SHELFD_DATA_PORT = int(os.environ.get("SHELFD_DATA_PORT", "9090"))
SHELFD_SHIM_PORT = int(os.environ.get("SHELFD_SHIM_PORT", "9092"))
METRICS_URL = f"http://{SHELFD_HOST}:{SHELFD_DATA_PORT}/metrics"
SHIM_URL = f"http://{SHELFD_HOST}:{SHELFD_SHIM_PORT}"

MINIO_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
WAREHOUSE = f"s3://{os.environ.get('WAREHOUSE_BUCKET', 'iceberg-warehouse')}/"
TABLE_NAME = os.environ.get("TABLE_NAME", "default.orders")


# --- shelf metrics helpers --------------------------------------------------

# We extract aggregate counters from the Prometheus text-format /metrics
# response. Shelf exposes:
#   shelf_hits_total{pool="metadata|rowgroup", ...}
#   shelf_misses_total{pool="metadata|rowgroup", ...}
# We sum across all label combinations because per-table cardinality
# detail isn't needed for a 2-run cold/warm demo.
_METRIC_LINE = re.compile(
    r"^(?P<name>shelf_hits_total|shelf_misses_total)\{[^}]*\}\s+(?P<value>[0-9.eE+-]+)\s*$"
)


def fetch_metrics() -> Dict[str, float]:
    counters: Dict[str, float] = {"shelf_hits_total": 0.0, "shelf_misses_total": 0.0}
    try:
        resp = requests.get(METRICS_URL, timeout=2.0)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        print(f"[bench] WARN: could not scrape {METRICS_URL}: {exc}", flush=True)
        return counters
    for line in resp.text.splitlines():
        m = _METRIC_LINE.match(line.strip())
        if not m:
            continue
        counters[m.group("name")] = counters.get(m.group("name"), 0.0) + float(
            m.group("value")
        )
    return counters


def diff_counters(before: Dict[str, float], after: Dict[str, float]) -> Dict[str, float]:
    return {k: after.get(k, 0.0) - before.get(k, 0.0) for k in {*before, *after}}


# --- query setup ------------------------------------------------------------

def build_io_config() -> IOConfig:
    """Daft IOConfig pointed at shelfd's S3-compat shim.

    Verified against docs.daft.ai + the live Daft 0.7.5 binary
    (Apr 2026):
      - `endpoint_url` -> shim base URL.
      - `key_id`/`access_key` -> shim is signature-agnostic but the
        SDK still needs *something* to sign with.
      - `use_ssl=False` -> the shim speaks HTTP. (Daft removed
        `verify_ssl` and `check_hostname_ssl` in 0.7 with the
        switch to rustls/AWS-LC — see Eventual-Inc/Daft#4530. With
        `use_ssl=False` no TLS is negotiated, so neither knob is
        relevant here.)
      - `force_virtual_addressing=False` -> path-style. The shim
        parses `<endpoint>/<bucket>/<key>`; virtual-host style would
        need wildcard DNS we don't have on the docker network.
      - `region_name="us-east-1"` -> default for MinIO.
    """
    return IOConfig(
        s3=S3Config(
            endpoint_url=SHIM_URL,
            key_id="dummy",
            access_key="dummy",
            use_ssl=False,
            force_virtual_addressing=False,
            region_name="us-east-1",
        )
    )


def load_iceberg_table():
    """Load the PyIceberg Table object via the REST catalog.

    PyIceberg only needs read access to the catalog + metadata.json
    here, so we point it at MinIO directly. The actual data-file
    reads happen inside Daft, which uses the IOConfig above and
    therefore goes through shelfd.
    """
    catalog = load_catalog(
        "daft_example",
        **{
            "type": "rest",
            "uri": REST_URI,
            "warehouse": WAREHOUSE,
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.environ.get(
                "AWS_SECRET_ACCESS_KEY", "minioadmin"
            ),
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
            "s3.path-style-access": "true",
        },
    )
    return catalog.load_table(TABLE_NAME)


def run_query(table, io_config: IOConfig) -> int:
    """Execute the demo query, returning the materialised row count.

    The query: `SUM(amount), COUNT(*) GROUP BY region WHERE status =
    'O'`. We force materialisation with `.collect()` so the timed
    section actually performs the scan + agg, not just plan
    construction.
    """
    df = (
        daft.read_iceberg(table, io_config=io_config)
        .where(col("status") == "O")
        .groupby("region")
        .agg(
            col("amount").sum().alias("total_amount"),
            col("order_id").count().alias("n_orders"),
        )
        .sort("region")
    )
    materialised = df.collect()
    rows = materialised.to_pydict()
    print("[bench] result:", flush=True)
    n = len(rows.get("region", []))
    for i in range(n):
        print(
            f"  region={rows['region'][i]!r}  "
            f"total_amount={rows['total_amount'][i]:.2f}  "
            f"n_orders={rows['n_orders'][i]}",
            flush=True,
        )
    return n


def wait_for_shim(timeout_s: float = 30.0) -> None:
    """Poll shelfd's /healthz before doing any timed work."""
    health = f"http://{SHELFD_HOST}:{SHELFD_DATA_PORT}/healthz"
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            r = requests.get(health, timeout=1.0)
            if r.status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(0.5)
    raise RuntimeError(f"shelfd never became healthy at {health}: {last_err}")


def main() -> int:
    print(f"[bench] shim={SHIM_URL}  metrics={METRICS_URL}", flush=True)
    print(f"[bench] table={TABLE_NAME}  warehouse={WAREHOUSE}", flush=True)

    wait_for_shim()
    table = load_iceberg_table()
    io_config = build_io_config()

    timings = []
    for run_idx in (1, 2):
        before = fetch_metrics()
        start = time.perf_counter()
        n_groups = run_query(table, io_config)
        elapsed = time.perf_counter() - start
        after = fetch_metrics()
        delta = diff_counters(before, after)
        label = "cold" if run_idx == 1 else "warm"
        timings.append((label, elapsed, n_groups, delta))
        print(
            f"[bench] run {run_idx} ({label}): {elapsed:.3f}s  "
            f"groups={n_groups}  "
            f"shelf_hits+={delta.get('shelf_hits_total', 0):.0f}  "
            f"shelf_misses+={delta.get('shelf_misses_total', 0):.0f}",
            flush=True,
        )

    print("\n=== Daft + Shelf example results ===", flush=True)
    print(f"{'run':<6} {'elapsed (s)':<14} {'shelf hits':<12} {'shelf misses':<14}")
    for label, elapsed, _, delta in timings:
        print(
            f"{label:<6} {elapsed:<14.3f} "
            f"{delta.get('shelf_hits_total', 0):<12.0f} "
            f"{delta.get('shelf_misses_total', 0):<14.0f}"
        )

    cold = timings[0][1]
    warm = timings[1][1]
    if warm < cold:
        speedup = cold / warm if warm > 0 else float("inf")
        print(f"\nwarm/cold speedup: {speedup:.2f}x", flush=True)
    else:
        print(
            "\nWARN: warm run was not faster than cold. Likely causes: "
            "result set fits in OS page cache; metadata-only scan; "
            "shelfd metrics not yet wired for this query path.",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
