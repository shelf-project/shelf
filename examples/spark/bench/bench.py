"""Spark-on-Shelf cold-vs-warm benchmark.

The script:
  1. Builds a SparkSession wired to the Iceberg REST catalog (`lake`)
     with Iceberg's `S3FileIO` pointed at shelfd's S3 shim
     (`http://shelfd:9092`). The `fs.s3a.*` keys are also wired so
     ANY non-Iceberg `s3a://` reads route through shelfd too — that
     mirrors the production pattern Trino + Spark deployments use.
  2. Runs the warmup SQL so JVM codegen + Iceberg client bootstrap
     do not contaminate the cold timing.
  3. Evicts both shelf pools through the admin endpoint so the cold
     run is unambiguously cache-cold (the warmup queries above touch
     just one partition, so most of the bench file set is still
     untouched anyway — eviction is belt-and-braces).
  4. Runs the bench SQL twice and prints cold/warm latency, hit/miss
     deltas from `/metrics`, and a back-of-envelope $ savings.

Doc references used to choose configuration keys:

  * Iceberg AWS S3 properties (`s3.endpoint`, `s3.path-style-access`,
    `s3.access-key-id`, `s3.secret-access-key`, `client.region`):
        https://iceberg.apache.org/docs/latest/aws/
  * Hadoop S3A authentication (anonymous credential provider, used
    here so unsigned requests reach the signature-agnostic shim):
        https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/tools/hadoop-aws/index.html#Authentication_properties
  * Hadoop S3A path-style access (mandatory for the shim — the shim
    routes path-style only):
        https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration
"""

from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from typing import Tuple

import requests

from pyspark.sql import SparkSession


SHELFD_S3_ENDPOINT = os.environ.get("SHELFD_S3_ENDPOINT", "http://shelfd:9092")
SHELFD_METRICS_URL = os.environ.get("SHELFD_METRICS_URL", "http://shelfd:9090/metrics")
SHELFD_ADMIN_URL = os.environ.get("SHELFD_ADMIN_URL", "http://shelfd:9090/admin")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
WAREHOUSE_BUCKET = os.environ.get("WAREHOUSE_BUCKET", "warehouse")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

WARMUP_SQL = Path("/work/spark-warmup.sql").read_text()
BENCH_SQL = Path("/work/spark-bench.sql").read_text()

# Standard AWS S3 GET request price (us-east-1 / ap-south-1):
# $0.0004 per 1,000 GET/SELECT requests.
S3_GET_USD_PER_REQ = 0.0004 / 1_000.0
# Conservative cross-AZ / inter-VPC egress proxy: $0.09/GB. Most prod
# stacks pay $0.01–$0.02/GB region-internal; we use the higher number
# so the walkthrough's "$-saved" stays conservative.
S3_EGRESS_USD_PER_GB = 0.09


def split_sql(text: str) -> list[str]:
    """Split a multi-statement SQL file on `;`, dropping comments + blanks."""
    statements: list[str] = []
    cleaned: list[str] = []
    for raw_line in text.splitlines():
        # Strip out -- comments BEFORE the split so a trailing `;` in
        # a comment doesn't fragment the statement.
        line = re.sub(r"--.*$", "", raw_line)
        cleaned.append(line)
    blob = "\n".join(cleaned)
    for chunk in blob.split(";"):
        stmt = chunk.strip()
        if stmt:
            statements.append(stmt)
    return statements


def build_spark() -> SparkSession:
    """Configure SparkSession → Iceberg REST → shelfd → MinIO."""
    builder = (
        SparkSession.builder.appName("shelf-spark-example")
        # Iceberg SQL extensions for procedure calls (REWRITE, etc.).
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # The `tabulario/spark-iceberg` image ships
        # `/opt/spark/conf/spark-defaults.conf` with a pre-baked
        # catalog named `demo` pointing at `http://rest:8181`. Make
        # sure Spark does NOT try to resolve that default before the
        # bench has set up the `lake` catalog: every analyzer pass
        # (e.g. `ReplaceCurrentLike`) calls `currentCatalog`, which
        # eagerly initialises the default. Pin our `lake` catalog as
        # the default to short-circuit it.
        .config("spark.sql.defaultCatalog", "lake")
        # Iceberg `lake` catalog → REST endpoint → S3FileIO.
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "rest")
        .config("spark.sql.catalog.lake.uri", ICEBERG_REST_URI)
        .config("spark.sql.catalog.lake.warehouse", f"s3://{WAREHOUSE_BUCKET}/")
        .config(
            "spark.sql.catalog.lake.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        # Iceberg AWS S3 properties — point Iceberg's data-file reads
        # at the shelfd shim. The shim is signature-agnostic so the
        # access-key/secret values can be any non-empty string; the
        # SDK still requires them to construct a SigV4 Authorization
        # header before the shim ignores it.
        .config("spark.sql.catalog.lake.s3.endpoint", SHELFD_S3_ENDPOINT)
        .config("spark.sql.catalog.lake.s3.path-style-access", "true")
        .config("spark.sql.catalog.lake.s3.access-key-id", "shelf-demo")
        .config("spark.sql.catalog.lake.s3.secret-access-key", "shelf-demo")
        .config("spark.sql.catalog.lake.client.region", AWS_REGION)
        # `fs.s3a.*` for any non-Iceberg `s3a://` reads (debug paths,
        # `SHOW FILES`, manual `read.parquet("s3a://...")` from a
        # follow-on notebook). The shim ignores SigV4, so the
        # AnonymousAWSCredentialsProvider is the most honest choice
        # here — it tells s3a not to attach an Authorization header
        # at all, which matches what the shim actually expects.
        .config("spark.hadoop.fs.s3a.endpoint", SHELFD_S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
        # Keep logs quiet — Spark's INFO floods the bench output and
        # buries the cold/warm summary line.
        .config("spark.log.level", "WARN")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.adaptive.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def evict_shelf() -> None:
    """Best-effort 'cold cache' nudge.

    `shelfd`'s `/admin/evict` is a per-key endpoint (it expects a
    JSON `{key_hex, pool}` body) — there is no "clear pool"
    operation in v0.5. For a fresh `docker compose up`, every Foyer
    pool is already empty, so this function is a no-op against a
    just-booted daemon. We keep the function (and the `[bench] cold
    cache assumed empty` log line) so the cold/warm flow is
    obvious; a future shelfd may grow a `/admin/clear?pool=...`
    endpoint and this is the natural place to call it.
    """
    print(
        "[bench] cold cache assumed empty (fresh shelfd boot — there is no "
        "v0.5 'clear pool' admin endpoint)",
        flush=True,
    )


_METRIC_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?P<labels>\{[^}]*\})?\s+(?P<value>[\-+0-9.eE]+)\s*$"
)


def scrape_metrics() -> dict:
    out: dict = {}
    r = requests.get(SHELFD_METRICS_URL, timeout=5)
    r.raise_for_status()
    for line in r.text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _METRIC_RE.match(line)
        if not m:
            continue
        try:
            out[(m.group("name"), m.group("labels") or "")] = float(m.group("value"))
        except ValueError:
            continue
    return out


def sum_metric(metrics: dict, name: str) -> float:
    return sum(v for (n, _), v in metrics.items() if n == name)


def sum_origin_get_bytes(metrics: dict) -> float:
    """Sum origin GET bytes across all label permutations.

    `shelf_origin_request_bytes_total{op,outcome,bucket}` is
    Foyer's authoritative byte-volume counter. We only count the
    `get_range` op (the byte-pulling read path); HEAD bytes are
    metadata-only and `get_range_conditional` 304s carry no body.
    """
    total = 0.0
    for (name, labels), value in metrics.items():
        if name != "shelf_origin_request_bytes_total":
            continue
        if 'op="get_range"' in labels and 'outcome="ok"' in labels:
            total += value
    return total


def time_query(spark: SparkSession, statements: list[str]):
    """Execute every statement; return (elapsed_seconds, last_rows)."""
    rows = []
    t0 = time.perf_counter()
    for stmt in statements:
        df = spark.sql(stmt)
        # Force materialisation so the timing actually covers I/O,
        # not just plan construction.
        rows = df.collect()
    t1 = time.perf_counter()
    return (t1 - t0), rows


def fmt_dur(secs: float) -> str:
    if secs >= 1.0:
        return f"{secs:6.2f} s"
    return f"{int(secs * 1000):>4d} ms"


def main() -> int:
    print("[bench] building SparkSession (this can take ~10–15 s on a cold "
          "JVM)...", flush=True)
    spark = build_spark()

    print("[bench] running warmup (JVM + Iceberg client bootstrap)...", flush=True)
    warmup_secs, _ = time_query(spark, split_sql(WARMUP_SQL))
    print(f"[bench] warmup completed in {fmt_dur(warmup_secs)}", flush=True)

    print("[bench] forcing a cold cache...", flush=True)
    evict_shelf()

    pre = scrape_metrics()
    pre_hits = sum_metric(pre, "shelf_hits_total")
    pre_misses = sum_metric(pre, "shelf_misses_total")
    pre_origin = sum_origin_get_bytes(pre)

    print("[bench] cold run...", flush=True)
    bench_stmts = split_sql(BENCH_SQL)
    cold_secs, rows = time_query(spark, bench_stmts)

    cold_metrics = scrape_metrics()
    cold_hits = sum_metric(cold_metrics, "shelf_hits_total") - pre_hits
    cold_misses = sum_metric(cold_metrics, "shelf_misses_total") - pre_misses
    cold_origin = sum_origin_get_bytes(cold_metrics) - pre_origin

    print("[bench] warm run...", flush=True)
    warm_secs, _ = time_query(spark, bench_stmts)

    warm_metrics = scrape_metrics()
    warm_hits = sum_metric(warm_metrics, "shelf_hits_total") - sum_metric(cold_metrics, "shelf_hits_total")
    warm_misses = sum_metric(warm_metrics, "shelf_misses_total") - sum_metric(cold_metrics, "shelf_misses_total")
    warm_origin = sum_origin_get_bytes(warm_metrics) - sum_origin_get_bytes(cold_metrics)

    speedup = (cold_secs / warm_secs) if warm_secs > 0 else float("inf")
    saved_requests = max(0.0, cold_misses - warm_misses)
    saved_bytes = max(0.0, cold_origin - warm_origin)
    saved_usd = (
        saved_requests * S3_GET_USD_PER_REQ
        + (saved_bytes / (1024 ** 3)) * S3_EGRESS_USD_PER_GB
    )

    print()
    print("=" * 72)
    print("  Spark → Shelf → MinIO   (Iceberg `lake.demo.events`, 1 M rows)")
    print("=" * 72)
    print(
        f"  cold:        {fmt_dur(cold_secs)}    "
        f"shelf hits/misses: {int(cold_hits):>5d} / {int(cold_misses):>5d}    "
        f"origin: {cold_origin / 1024 / 1024:6.2f} MiB"
    )
    print(
        f"  warm:        {fmt_dur(warm_secs)}    "
        f"shelf hits/misses: {int(warm_hits):>5d} / {int(warm_misses):>5d}    "
        f"origin: {warm_origin / 1024 / 1024:6.2f} MiB"
    )
    print(f"  speedup:     {speedup:.1f}x")
    print(
        f"  $-saved:     ${saved_usd:.6f}   "
        f"({int(saved_requests)} GETs + {saved_bytes / 1024 / 1024:.2f} MiB egress avoided)"
    )
    print("=" * 72)
    print()
    print("  sample result rows:")
    for row in rows[:5]:
        print(f"    {row}")
    if len(rows) > 5:
        print(f"    ... ({len(rows) - 5} more)")

    # One-line machine-parseable summary for run.sh's grep.
    print(
        f"\nSUMMARY: cold={cold_secs:.2f}s | warm={warm_secs:.2f}s | "
        f"speedup={speedup:.1f}x | $-saved=${saved_usd:.4f}",
        flush=True,
    )

    if warm_secs > cold_secs + 0.05:
        print(
            f"\n[bench] WARN: warm ({fmt_dur(warm_secs)}) was slower than "
            f"cold ({fmt_dur(cold_secs)}) — Foyer pools may have evicted "
            f"between runs (check pool capacity in shelfd.yaml).",
            file=sys.stderr,
        )

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
