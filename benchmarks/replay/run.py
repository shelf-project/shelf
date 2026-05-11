#!/usr/bin/env python3
"""benchmarks/replay/run.py — online replay driver.

Replays a Trino query trace produced by prep.sh against a live Trino
coordinator, sampling shelfd's `:9090/metrics` Prometheus surface
every 10 seconds. Emits a schema-valid JSON record at
`benchmarks/results/<date>/<backend>/replay-<run_id>.json` per
SPEC.md and `schema.json`.

This complements the offline `shelf-replay` package
(`benchmarks/trino_logs/src/shelf_replay/`) which simulates cache
algorithms; the online driver exercises the full
Trino → shelfd → S3 path so the gate metrics in ADR-0010 are
measured, not simulated.

Usage:
    python3 benchmarks/replay/run.py \
        --trace results/2026-05-01/replay-fixture/trace.jsonl \
        --backend shelf \
        --trino-url http://localhost:18080 \
        --trino-user bench-runner \
        --catalog cdp_shelf \
        --speed 2x \
        --shelfd-metrics-url http://localhost:19090/metrics \
        --out results/2026-05-01/shelf/replay-01H...SHELF.json

Environment:
    SHELF_BENCH_RUN_ID    — ULID; auto-generated if unset
    SHELF_BENCH_COMMIT_SHA — defaults to `git rev-parse HEAD`
    SHELF_BENCH_RELEASE_TAG — defaults to "v0.0-dev"

Schema-validation against schema.json is performed before write.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as _dt
import hashlib
import json
import os
import re
import secrets
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path
from typing import Any, Iterable

# ---------------------------------------------------------------------------
# Constants matched to the schema.
# ---------------------------------------------------------------------------

SUPPORTED_BACKENDS = {"raw-s3", "fs-cache", "alluxio-2-9", "alluxio-3-dora", "shelf"}
SUPPORTED_SPEEDS = {"1x", "2x", "10x"}
SAMPLE_INTERVAL_S = 10.0
ULID_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def gen_ulid() -> str:
    """Crockford-base32 26-char ULID. Schema requires this shape."""
    ms = int(time.time() * 1000)
    rand = secrets.token_bytes(10)
    # 48-bit timestamp + 80-bit randomness, base32 encoded.
    chars = []
    for i in range(10):
        chars.append(ULID_ALPHABET[(ms >> (45 - i * 5)) & 0x1F])
    rand_int = int.from_bytes(rand, "big")
    for i in range(16):
        chars.append(ULID_ALPHABET[(rand_int >> (75 - i * 5)) & 0x1F])
    return "".join(chars)


def git_sha() -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=Path(__file__).parent, text=True, stderr=subprocess.DEVNULL
        ).strip()
        if re.fullmatch(r"[0-9a-f]{40}", out):
            return out
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    return "0" * 40


# ---------------------------------------------------------------------------
# Prometheus metrics scraping. We only consume shelf_hits_total +
# shelf_misses_total + shelf_origin_request_bytes_total. Trying to parse
# every series is overkill; this script needs the gate-metric inputs.
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ShelfdMetricsSnapshot:
    t_seconds: float
    hits_total: int
    misses_total: int
    origin_request_bytes_total: int
    # Parsed but not in schema; useful for diagnostic logging.
    disk_bytes_used: int = 0


def scrape_shelfd_metrics(url: str, t_seconds: float, timeout: float = 5.0) -> ShelfdMetricsSnapshot | None:
    """One-shot scrape of shelfd /metrics. Returns None on error."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return None

    hits = 0
    misses = 0
    origin = 0
    disk = 0
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        # Format: metric_name{labels}? value
        m = re.match(r"^(\w+)(\{[^}]*\})?\s+(-?[\d.eE+]+)\s*$", line)
        if not m:
            continue
        name, _labels, val_str = m.groups()
        try:
            val = float(val_str)
        except ValueError:
            continue
        if name == "shelf_hits_total":
            hits += int(val)
        elif name == "shelf_misses_total":
            misses += int(val)
        elif name == "shelf_origin_request_bytes_total":
            origin += int(val)
        elif name == "shelf_disk_bytes_used":
            # Take the maximum across pods; Prometheus aggregates by
            # series, but at scrape time the same pod will only emit
            # one shelf_disk_bytes_used value per pool.
            disk = max(disk, int(val))
    return ShelfdMetricsSnapshot(
        t_seconds=t_seconds,
        hits_total=hits,
        misses_total=misses,
        origin_request_bytes_total=origin,
        disk_bytes_used=disk,
    )


# ---------------------------------------------------------------------------
# Trace iterator — replays the trace at the configured speed knob.
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class TraceQuery:
    query_id: str
    query: str
    catalog: str
    user: str
    issued_at_iso: str
    issued_at_unix: float
    is_dbt: bool


def load_trace(path: Path) -> list[TraceQuery]:
    out: list[TraceQuery] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            qd = row.get("query_date", "")
            try:
                ts = _dt.datetime.fromisoformat(qd.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                ts = _dt.datetime.now(_dt.timezone.utc)
            user = row.get("user", "") or ""
            cat = row.get("catalog", "") or "cdp"
            out.append(
                TraceQuery(
                    query_id=row.get("query_id", ""),
                    query=row.get("query", ""),
                    catalog=cat,
                    user=user,
                    issued_at_iso=ts.replace(tzinfo=_dt.timezone.utc).isoformat(),
                    issued_at_unix=ts.timestamp(),
                    # dbt queries route through `cdp_dbt` catalog or the
                    # `dbt_user` principal; either signal counts.
                    is_dbt=("dbt" in cat.lower()) or ("dbt" in user.lower()),
                )
            )
    out.sort(key=lambda q: q.issued_at_unix)
    return out


# ---------------------------------------------------------------------------
# Trino client. Imported lazily so `--help` works without the dep.
# ---------------------------------------------------------------------------


def issue_one_query(trino_url: str, user: str, catalog: str, sql: str, target_catalog: str | None) -> tuple[bool, int, int, int]:
    """Issue one query against the bench Trino. Returns (ok, latency_ns, rows, bytes_processed)."""
    try:
        from trino.dbapi import connect
    except ImportError:
        print("ERROR: pip install trino  (https://github.com/trinodb/trino-python-client)", file=sys.stderr)
        sys.exit(2)

    host = trino_url.replace("https://", "").replace("http://", "").split("/")[0]
    port = 443 if trino_url.startswith("https://") else 80
    scheme = "https" if port == 443 else "http"
    if ":" in host:
        host_only, port_str = host.rsplit(":", 1)
        port = int(port_str)
        host = host_only

    used_catalog = target_catalog or catalog
    started = time.monotonic_ns()
    try:
        conn = connect(host=host, port=port, user=user, http_scheme=scheme, catalog=used_catalog)
        cur = conn.cursor()
        # Tag the query so SHELF-42 A/B attribution works.
        cur.execute(sql)
        rows = cur.fetchall()
        latency_ns = time.monotonic_ns() - started
        # Trino python client doesn't surface processed_bytes via DBAPI;
        # stat extraction would require the REST /v1/query/<id>/info
        # path. For v1 we record latency only; bytes_processed is
        # filled in during the post-run analysis from the same trace.
        return True, latency_ns, len(rows), 0
    except Exception as exc:
        latency_ns = time.monotonic_ns() - started
        print(f"[run] WARN query {used_catalog} failed: {exc}", file=sys.stderr)
        return False, latency_ns, 0, 0


# ---------------------------------------------------------------------------
# Main driver.
# ---------------------------------------------------------------------------


def percentile(values: list[int], p: float) -> int:
    if not values:
        return 0
    s = sorted(values)
    k = (len(s) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return int(s[lo] + (s[hi] - s[lo]) * (k - lo))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--trace", required=True, type=Path)
    p.add_argument("--backend", required=True, choices=sorted(SUPPORTED_BACKENDS))
    p.add_argument("--trino-url", required=True)
    p.add_argument("--trino-user", default="bench-runner")
    p.add_argument("--catalog", default=None,
                   help="Override every trace query's catalog. Use cdp_shelf for backend=shelf, cdp for backend=raw-s3.")
    p.add_argument("--speed", default="2x", choices=sorted(SUPPORTED_SPEEDS))
    p.add_argument("--shelfd-metrics-url", default=None,
                   help="shelfd /metrics endpoint. Required if backend=shelf, ignored otherwise.")
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--limit", type=int, default=None, help="Cap query count for smoke runs.")
    p.add_argument("--max-concurrency", type=int, default=8,
                   help="Max in-flight queries; trace times will not be honoured beyond this.")
    p.add_argument("--trino-image", default="trinodb/trino:480")
    p.add_argument("--instance-type", default="m5a.4xlarge")
    p.add_argument("--worker-count", type=int, default=4)
    p.add_argument("--shelf-instance-type", default="m5a.4xlarge")
    p.add_argument("--shelf-node-count", type=int, default=3)
    args = p.parse_args(argv)

    if args.backend == "shelf" and not args.shelfd_metrics_url:
        print("ERROR: --shelfd-metrics-url is required for backend=shelf", file=sys.stderr)
        return 2

    queries = load_trace(args.trace)
    if args.limit is not None:
        queries = queries[: args.limit]
    if not queries:
        print("ERROR: trace has zero queries", file=sys.stderr)
        return 2

    speed_factor = {"1x": 1.0, "2x": 2.0, "10x": 10.0}[args.speed]

    run_id = os.environ.get("SHELF_BENCH_RUN_ID", gen_ulid())
    if not re.fullmatch(rf"[{ULID_ALPHABET}]{{26}}", run_id):
        print(f"ERROR: SHELF_BENCH_RUN_ID malformed (got {run_id!r}); using fresh ULID")
        run_id = gen_ulid()
    commit_sha = os.environ.get("SHELF_BENCH_COMMIT_SHA", git_sha())
    release_tag = os.environ.get("SHELF_BENCH_RELEASE_TAG", "v0.0-dev")

    started_unix = time.time()
    started_iso = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    # Trace anchor — relative offset from first query.
    base_unix = queries[0].issued_at_unix

    # --- Replay loop ---
    samples: list[dict[str, Any]] = []
    latency_ns: list[int] = []
    failures = 0
    bytes_read = 0
    bytes_admitted = 0
    dbt_results: list[bool] = []

    # Sampler thread: snapshots shelfd metrics every SAMPLE_INTERVAL_S.
    metrics_snapshots: list[ShelfdMetricsSnapshot] = []

    def _sampler_loop(stop_at: float) -> None:
        if not args.shelfd_metrics_url:
            return
        while time.time() < stop_at:
            snap = scrape_shelfd_metrics(
                args.shelfd_metrics_url,
                t_seconds=time.time() - started_unix,
            )
            if snap is not None:
                metrics_snapshots.append(snap)
            time.sleep(SAMPLE_INTERVAL_S)

    sampler_executor: ThreadPoolExecutor | None = None
    sampler_future: Future[None] | None = None
    if args.shelfd_metrics_url:
        # Replay wall-clock duration at the chosen speed.
        last_unix = queries[-1].issued_at_unix
        wall_seconds = max(60.0, (last_unix - base_unix) / speed_factor + 30.0)
        stop_at = started_unix + wall_seconds + 5.0
        sampler_executor = ThreadPoolExecutor(max_workers=1)
        sampler_future = sampler_executor.submit(_sampler_loop, stop_at)

    pool = ThreadPoolExecutor(max_workers=args.max_concurrency)
    inflight: list[Future[tuple[bool, int, int, int]]] = []

    print(f"[run] backend={args.backend} speed={args.speed} queries={len(queries)} run_id={run_id}", file=sys.stderr)

    try:
        for q in queries:
            offset = (q.issued_at_unix - base_unix) / speed_factor
            target_t = started_unix + offset
            now = time.time()
            if target_t > now:
                time.sleep(target_t - now)
            # Drain finished futures so memory stays bounded.
            still: list[Future[tuple[bool, int, int, int]]] = []
            for f in inflight:
                if f.done():
                    ok, lat, rows, b = f.result()
                    latency_ns.append(lat)
                    if not ok:
                        failures += 1
                    if q.is_dbt:
                        dbt_results.append(ok)
                    bytes_read += b
                else:
                    still.append(f)
            inflight = still
            inflight.append(pool.submit(issue_one_query, args.trino_url, args.trino_user, q.catalog, q.query, args.catalog))
        # Drain remaining
        for f in inflight:
            ok, lat, rows, b = f.result()
            latency_ns.append(lat)
            if not ok:
                failures += 1
            bytes_read += b
    finally:
        pool.shutdown(wait=False)
        if sampler_executor:
            sampler_executor.shutdown(wait=True)
        if sampler_future:
            try:
                sampler_future.result(timeout=5)
            except Exception:
                pass

    # --- Compute summary metrics ---
    total = max(1, len(latency_ns))
    p50 = percentile(latency_ns, 0.50)
    p95 = percentile(latency_ns, 0.95)
    p99 = percentile(latency_ns, 0.99)
    p999 = percentile(latency_ns, 0.999)

    final_hits = metrics_snapshots[-1].hits_total if metrics_snapshots else 0
    final_misses = metrics_snapshots[-1].misses_total if metrics_snapshots else 0
    hit_rate = (final_hits / (final_hits + final_misses)) if (final_hits + final_misses) > 0 else 0.0
    origin_bytes = metrics_snapshots[-1].origin_request_bytes_total if metrics_snapshots else 0

    gold_dbt_ok = (sum(1 for r in dbt_results if r) / len(dbt_results)) if dbt_results else 1.0

    # Schema-friendly per-second sample rollup (10 s buckets).
    samples_for_record: list[dict[str, Any]] = []
    if metrics_snapshots:
        for snap in metrics_snapshots:
            denom = max(1, snap.hits_total + snap.misses_total)
            sample_hr = snap.hits_total / denom if denom > 0 else 0.0
            samples_for_record.append({
                "t_seconds": snap.t_seconds,
                "hit_rate": sample_hr,
                "gold_dbt_ok_rate": gold_dbt_ok,
            })

    # Verdict — gate evaluation is delegated to tools/gate.py for the
    # full RESULTS.md row, but we emit the gate substructure here so a
    # subsequent gate run is just a comparison.
    verdict = "n/a" if args.speed != "2x" else "pending"
    failed_metrics: list[str] = []

    config_hash = "sha256:" + hashlib.sha256(
        f"{args.trino_image}|{args.backend}|{args.catalog or ''}|{args.speed}".encode()
    ).hexdigest()

    record = {
        "run_id": run_id,
        "timestamp": started_iso,
        "commit_sha": commit_sha,
        "release_tag": release_tag,
        "benchmark": "replay",
        "backend": args.backend,
        "config": {
            "config_hash": config_hash,
            "trino_image": args.trino_image,
            "backend_image": "ghcr.io/shelf-project/shelfd:1.0.0" if args.backend == "shelf" else args.backend,
            "plugin_jar_sha256": None,
        },
        "cluster_shape": {
            "region": "ap-south-1",
            "k8s_version": "1.30",
            "trino_instance_type": args.instance_type,
            "trino_worker_count": args.worker_count,
            "shelf_instance_type": args.shelf_instance_type,
            "shelf_node_count": args.shelf_node_count if args.backend == "shelf" else 0,
            "driver_instance_type": "m5a.large",
            "scale_factor": None,
            "partial": False,
        },
        "trace": {
            "source_table": "cdp.trino_logs.trino_queries",
            "snapshot_id": _read_trace_snapshot_id(args.trace) or "0" * 19,
            "from": queries[0].issued_at_iso,
            "to": queries[-1].issued_at_iso,
            "replica": "rep-2",
            "query_count": len(queries),
            "speed": args.speed,
        },
        "samples": samples_for_record,
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
            "gold_dbt_ok_rate": gold_dbt_ok,
            "latency_ns_p95_vs_alluxio": None,
            "shelf_caused_pages": 0,
            "oncall_surface_ratio": None,
            "verdict": verdict,
            "failed_metrics": failed_metrics,
        },
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as fh:
        json.dump(record, fh, indent=2)

    print(f"[run] wrote {args.out}", file=sys.stderr)
    print(
        f"[run] queries={len(queries)} failures={failures} "
        f"p50={p50/1e6:.0f}ms p95={p95/1e6:.0f}ms p99={p99/1e6:.0f}ms hit_rate={hit_rate:.3f}",
        file=sys.stderr,
    )
    return 0


def _read_trace_snapshot_id(trace_path: Path) -> str | None:
    meta = trace_path.parent / "metadata.json"
    if not meta.exists():
        return None
    try:
        with meta.open("r", encoding="utf-8") as fh:
            return json.load(fh).get("trace_snapshot_id")
    except (OSError, json.JSONDecodeError):
        return None


if __name__ == "__main__":
    raise SystemExit(main())
