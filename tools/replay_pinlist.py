#!/usr/bin/env python3
"""Replay a pin/replay list of S3 paths through a shelf S3 shim (Stage 3a).

Reads JSON output from ``gen_replay_list.py`` (a flat array of
``{bucket, key, size_estimate, access_count, table}`` records, sorted by
``access_count DESC``) and issues an HTTP ``GET`` per entry against a shelf
shim endpoint, e.g.::

    http://shelf-pool.shelf.svc.cluster.local:9092/<bucket>/<key>

The shim is signature-agnostic by design (per SHELF-22) and forwards
non-cached entries to the origin S3, populating both DRAM and disk pools as a
side effect. This is the cold-start prewarm path that flips a fresh shelf
pool from "empty cache, every request misses" to ">=70% hit ratio after first
12 h" in a few minutes instead of a few hours.

Hit/miss classification
-----------------------
shelfd's ``/cache/...`` endpoints emit no ``X-Cache-Status`` header today,
and the shim does not either. We therefore infer cache outcome from
**response time** with thresholds calibrated to the live cluster:

* < 10 ms  -> ``hit_ram`` (Foyer DRAM hit)
* 10-200 ms -> ``hit_disk`` (Foyer NVMe hit)
* >= 200 ms -> ``miss`` (origin fetch round-trip)

These thresholds match the Grafana ``Shelf — Cache, Disk and Pods`` panel
and the SHELF-A4 outcome distribution. They are intentionally conservative —
a slow NVMe hit will be misclassified as a miss, biasing the post-warm hit
ratio downward (i.e. reporting is pessimistic, not optimistic).

If a future shelf build sets ``X-Shelf-Cache: hit_ram|hit_disk|miss`` on the
response, the inference is overridden by the header value.

CLI
---

    python3 replay_pinlist.py \\
        --pinlist /tmp/replay-rep3.json \\
        --shelf-endpoint shelf-pool.shelf.svc.cluster.local:9092 \\
        --concurrency 20

Use ``--dry-run`` to print the request plan without issuing anything.
Read-only against the cluster: GETs are idempotent and the only side effect
is the cache fill that is, in fact, the goal.
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

LOG = logging.getLogger("shelf.replay_pinlist")


@dataclass
class ReplayResult:
    bucket: str
    key: str
    table: str
    status: int
    bytes_read: int
    fill_seconds: float
    outcome: str  # hit_ram | hit_disk | miss | error_5xx | error_other
    error: str | None = None


def classify_outcome(elapsed: float, status: int, header: str | None) -> str:
    if status >= 500:
        return "error_5xx"
    if status == 404:
        return "not_found"
    if status >= 400:
        return "error_other"
    if header:
        h = header.strip().lower()
        if h in ("hit_ram", "hit_memory"):
            return "hit_ram"
        if h in ("hit_disk", "hit_nvme"):
            return "hit_disk"
        if h in ("miss",):
            return "miss"
    if elapsed < 0.010:
        return "hit_ram"
    if elapsed < 0.200:
        return "hit_disk"
    return "miss"


def fetch_one(
    endpoint: str,
    entry: dict,
    timeout: int,
    chunk_bytes_cap: int | None,
) -> ReplayResult:
    """Issue one GET. Drains the response body up to ``chunk_bytes_cap``.

    We deliberately drain the body — shelfd considers the request "fulfilled"
    only after the response is fully streamed; without draining, the cache
    fill on miss may be aborted mid-flight and the next request re-misses.
    ``chunk_bytes_cap`` lets callers cap RAM use against unexpectedly large
    Iceberg manifests; ``None`` means read everything.
    """
    bucket = entry["bucket"]
    key = entry["key"]
    table = entry.get("table", "?")
    safe_key = quote(key, safe="/=,-_.+")
    url = f"http://{endpoint}/{bucket}/{safe_key}"
    req = Request(url, method="GET", headers={"User-Agent": "shelf-replay/1"})
    t0 = time.monotonic()
    try:
        with urlopen(req, timeout=timeout) as resp:
            status = resp.status
            cache_hdr = resp.headers.get("X-Shelf-Cache") or resp.headers.get(
                "X-Cache-Status"
            )
            total = 0
            while True:
                buf = resp.read(64 * 1024)
                if not buf:
                    break
                total += len(buf)
                if chunk_bytes_cap is not None and total >= chunk_bytes_cap:
                    break
            elapsed = time.monotonic() - t0
            outcome = classify_outcome(elapsed, status, cache_hdr)
            return ReplayResult(
                bucket=bucket,
                key=key,
                table=table,
                status=status,
                bytes_read=total,
                fill_seconds=elapsed,
                outcome=outcome,
            )
    except HTTPError as e:
        elapsed = time.monotonic() - t0
        cache_hdr = e.headers.get("X-Shelf-Cache") if e.headers else None
        outcome = classify_outcome(elapsed, e.code, cache_hdr)
        body = ""
        try:
            body = e.read().decode(errors="replace")[:200]
        except Exception:
            pass
        return ReplayResult(
            bucket=bucket,
            key=key,
            table=table,
            status=e.code,
            bytes_read=0,
            fill_seconds=elapsed,
            outcome=outcome,
            error=body or str(e),
        )
    except URLError as e:
        elapsed = time.monotonic() - t0
        return ReplayResult(
            bucket=bucket,
            key=key,
            table=table,
            status=0,
            bytes_read=0,
            fill_seconds=elapsed,
            outcome="error_other",
            error=f"URLError: {e.reason}",
        )
    except Exception as e:
        elapsed = time.monotonic() - t0
        return ReplayResult(
            bucket=bucket,
            key=key,
            table=table,
            status=0,
            bytes_read=0,
            fill_seconds=elapsed,
            outcome="error_other",
            error=f"{type(e).__name__}: {e}",
        )


@dataclass
class Summary:
    total: int = 0
    counts: dict[str, int] = field(default_factory=dict)
    bytes_total: int = 0
    fill_times: list[float] = field(default_factory=list)
    elapsed_seconds: float = 0.0
    errors_sample: list[ReplayResult] = field(default_factory=list)

    def record(self, r: ReplayResult) -> None:
        self.total += 1
        self.counts[r.outcome] = self.counts.get(r.outcome, 0) + 1
        self.bytes_total += r.bytes_read
        self.fill_times.append(r.fill_seconds)
        if r.outcome.startswith("error") and len(self.errors_sample) < 10:
            self.errors_sample.append(r)

    def percentile(self, p: float) -> float:
        if not self.fill_times:
            return 0.0
        return statistics.quantiles(self.fill_times, n=100)[int(p) - 1]

    def hit_rate_after_warm(self) -> float:
        """Hits over (hits + misses), excluding errors and 404s."""
        hits = self.counts.get("hit_ram", 0) + self.counts.get("hit_disk", 0)
        misses = self.counts.get("miss", 0)
        denom = hits + misses
        return (hits / denom) if denom else 0.0

    def render(self, header_extra: str = "") -> str:
        if not self.fill_times:
            return "no requests issued\n"
        sorted_t = sorted(self.fill_times)

        def pct(p: float) -> float:
            if not sorted_t:
                return 0.0
            idx = max(0, min(len(sorted_t) - 1, int(p / 100 * len(sorted_t))))
            return sorted_t[idx]

        rps = self.total / self.elapsed_seconds if self.elapsed_seconds else 0.0
        mb = self.bytes_total / (1024 * 1024)
        mb_per_s = mb / self.elapsed_seconds if self.elapsed_seconds else 0.0
        lines = [
            "=" * 64,
            "shelf replay summary",
            "=" * 64,
        ]
        if header_extra:
            lines.append(header_extra)
        lines.extend(
            [
                f"total requests   : {self.total}",
                f"elapsed wall     : {self.elapsed_seconds:.2f}s "
                f"({rps:.1f} req/s)",
                f"bytes read total : {mb:.1f} MiB ({mb_per_s:.1f} MiB/s)",
                "",
                "outcome breakdown:",
            ]
        )
        for k in (
            "hit_ram",
            "hit_disk",
            "miss",
            "not_found",
            "error_5xx",
            "error_other",
        ):
            v = self.counts.get(k, 0)
            pct_v = (v / self.total * 100) if self.total else 0.0
            lines.append(f"  {k:<11} : {v:>6}  ({pct_v:5.1f}%)")
        lines.extend(
            [
                "",
                f"hit ratio (post-warm, excl. errors+404): "
                f"{self.hit_rate_after_warm()*100:.1f}%",
                "",
                "fill time (seconds):",
                f"  p50  : {pct(50):.3f}",
                f"  p95  : {pct(95):.3f}",
                f"  p99  : {pct(99):.3f}",
                f"  max  : {sorted_t[-1]:.3f}",
            ]
        )
        if self.errors_sample:
            lines.append("")
            lines.append("error sample (first 10):")
            for e in self.errors_sample:
                msg = (e.error or "")[:120].replace("\n", " ")
                lines.append(
                    f"  status={e.status} {e.bucket}/{e.key[:60]}  -> {msg}"
                )
        lines.append("=" * 64)
        return "\n".join(lines) + "\n"


def replay(
    entries: list[dict],
    endpoint: str,
    concurrency: int,
    timeout: int,
    chunk_bytes_cap: int | None,
    progress_every: int,
) -> Summary:
    summary = Summary()
    t_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [
            ex.submit(fetch_one, endpoint, e, timeout, chunk_bytes_cap)
            for e in entries
        ]
        for i, fut in enumerate(as_completed(futures), 1):
            r = fut.result()
            summary.record(r)
            if progress_every and i % progress_every == 0:
                LOG.info(
                    "  ... %d/%d done (hits=%d misses=%d errors=%d)",
                    i,
                    len(entries),
                    summary.counts.get("hit_ram", 0)
                    + summary.counts.get("hit_disk", 0),
                    summary.counts.get("miss", 0),
                    summary.counts.get("error_5xx", 0)
                    + summary.counts.get("error_other", 0),
                )
    summary.elapsed_seconds = time.monotonic() - t_start
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay a shelf prewarm list against the S3 shim.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--pinlist",
        required=True,
        help="JSON output from gen_replay_list.py (or compatible schema).",
    )
    parser.add_argument(
        "--shelf-endpoint",
        required=True,
        help="host:port for the shelf S3 shim (e.g. "
        "shelf-pool.shelf.svc.cluster.local:9092).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Parallel GETs (default: 20). Match shelf maxConnections / 2.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Per-request timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--max-bytes-per-object",
        type=int,
        default=64 * 1024 * 1024,
        help="Cap body drained per request, bytes (default: 64 MiB). "
        "Iceberg metadata + manifest files fit comfortably; this caps "
        "any accidental data-file entry from blowing memory.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Issue only the first N entries (0 = all, default).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=200,
        help="Log progress every N completions (default: 200).",
    )
    parser.add_argument(
        "--summary-out",
        default=None,
        help="Optional path to also write the summary text (in addition to "
        "stdout).",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    with open(args.pinlist) as f:
        entries = json.load(f)
    if not isinstance(entries, list):
        LOG.error("pinlist is not a JSON array: %s", args.pinlist)
        return 2
    if args.limit and len(entries) > args.limit:
        entries = entries[: args.limit]

    LOG.info(
        "loaded %d entries from %s, endpoint=%s, concurrency=%d",
        len(entries),
        args.pinlist,
        args.shelf_endpoint,
        args.concurrency,
    )

    if args.dry_run:
        for e in entries[:20]:
            print(
                f"GET http://{args.shelf_endpoint}/{e['bucket']}/{e['key']}  "
                f"# table={e.get('table','?')} access_count={e.get('access_count',0)}"
            )
        if len(entries) > 20:
            print(f"... and {len(entries) - 20} more")
        return 0

    summary = replay(
        entries,
        args.shelf_endpoint,
        args.concurrency,
        args.timeout,
        args.max_bytes_per_object,
        args.progress_every,
    )
    header = (
        f"endpoint        : {args.shelf_endpoint}\n"
        f"pinlist         : {args.pinlist} ({len(entries)} entries)\n"
        f"concurrency     : {args.concurrency}"
    )
    text = summary.render(header)
    sys.stdout.write(text)
    if args.summary_out:
        with open(args.summary_out, "w") as f:
            f.write(text)

    if summary.counts.get("error_5xx", 0) + summary.counts.get("error_other", 0) > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
