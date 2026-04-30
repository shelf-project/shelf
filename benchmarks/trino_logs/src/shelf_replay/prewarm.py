"""Online prewarm of a shelfd pool from a recorded Trino trace.

Contrast with :mod:`shelf_replay.simulate`, which is **offline**: it
runs cache algorithms in memory against the trace and reports hit
ratios. ``prewarm`` is **online**: it issues real HTTP range-GETs to
a running shelfd (via its S3-compatibility shim) so the shelfd pool
actually warms up before a replica cutover.

Flow:

1. Reuse the :mod:`shelf_replay.scanner` pipeline to produce the
   ``(file_path, offset, length)`` stream that production Trino
   would have issued for those queries.
2. Dedupe and issue concurrent ``GET bucket/{file_path}`` with
   ``Range: bytes=offset-end`` header at the shelfd S3 shim
   endpoint. Shelfd's s3_shim turns these into origin fetches into
   its Foyer row-group pool.
3. Track success / failure / hit-vs-miss ratios (parsed from the
   optional ``X-Shelf-Outcome`` response header if present; absent
   → counted as "unknown" without failing the prewarm).
4. Emit a JSON summary so the rollout runbook's T-24h step can log
   "60 % hit ratio, 2.1 M requests issued, 0 errors" and move on.

This module is intentionally *not* a general-purpose HTTP load
tester — the goal is to warm shelfd's NVMe against the specific
byte-ranges production will ask for, using the same code path
(s3_shim → FoyerStore::get_or_fetch) that production will use at
cutover. That shared-path property is what lets us trust the
warm-up: if the prewarm hits 60 % at rate R, the first 5 min of
cutover will observe ≈ 60 % hit ratio at rate ≥ R.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import json
import logging
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterable

from .manifest import ManifestIndex
from .scanner import scan_all
from .trace import load_trace

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class PrewarmReport:
    replica: str
    endpoint: str
    requests_issued: int
    successes: int
    client_errors: int
    server_errors: int
    bytes_requested: int
    elapsed_s: float
    outcome_counts: dict[str, int]

    @property
    def success_ratio(self) -> float:
        if self.requests_issued == 0:
            return 0.0
        return self.successes / self.requests_issued

    @property
    def hit_ratio_from_outcomes(self) -> float | None:
        hits = self.outcome_counts.get("hit", 0)
        misses = self.outcome_counts.get("miss", 0)
        total = hits + misses
        if total == 0:
            return None
        return hits / total


def run_prewarm(
    trace_path: str | Path,
    manifest_dir: str | Path,
    endpoint: str,
    bucket: str,
    replica: str,
    *,
    concurrency: int = 32,
    limit: int | None = None,
    per_request_timeout: float = 10.0,
    dedupe: bool = True,
) -> PrewarmReport:
    """Warm shelfd for the byte-ranges the trace asked for.

    ``endpoint`` is a URL like ``http://shelfd:9092`` (no trailing
    slash; trailing slashes will be stripped). ``bucket`` is the
    Iceberg warehouse bucket shelfd proxies for.

    Parallelism is process-local; if you need to spread warming
    across multiple nodes, run one ``make prewarm`` per node with
    disjoint trace slices. The PodDisruptionBudget + anti-affinity
    on the shelfd StatefulSet means warmup is distributed across
    pods automatically.
    """

    endpoint = endpoint.rstrip("/")
    trace = load_trace(trace_path)
    manifest_index = ManifestIndex.load(manifest_dir)
    scans = scan_all(trace, manifest_index)
    ranges = _extract_unique_ranges(scans, dedupe=dedupe)
    if limit is not None:
        ranges = ranges[: max(0, limit)]

    LOGGER.info(
        '{"event":"prewarm_start","replica":%s,"range_count":%d,"endpoint":%s}',
        json.dumps(replica),
        len(ranges),
        json.dumps(endpoint),
    )

    started = time.monotonic()
    requests_issued = 0
    successes = 0
    client_errors = 0
    server_errors = 0
    bytes_requested = 0
    outcome_counts: dict[str, int] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(_issue_one, endpoint, bucket, path, offset, length, per_request_timeout)
            for path, offset, length in ranges
        ]
        for future in concurrent.futures.as_completed(futures):
            status, length, outcome = future.result()
            requests_issued += 1
            bytes_requested += length
            if 200 <= status < 300:
                successes += 1
            elif 400 <= status < 500:
                client_errors += 1
            else:
                server_errors += 1
            outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1

    elapsed = time.monotonic() - started
    report = PrewarmReport(
        replica=replica,
        endpoint=endpoint,
        requests_issued=requests_issued,
        successes=successes,
        client_errors=client_errors,
        server_errors=server_errors,
        bytes_requested=bytes_requested,
        elapsed_s=elapsed,
        outcome_counts=outcome_counts,
    )
    LOGGER.info(
        '{"event":"prewarm_done","replica":%s,"requests":%d,"successes":%d,"errors":%d,"hit_ratio":%s,"elapsed_s":%.1f}',
        json.dumps(replica),
        report.requests_issued,
        report.successes,
        report.client_errors + report.server_errors,
        "null" if report.hit_ratio_from_outcomes is None else f"{report.hit_ratio_from_outcomes:.3f}",
        report.elapsed_s,
    )
    return report


def _extract_unique_ranges(
    scans: Iterable, *, dedupe: bool
) -> list[tuple[str, int, int]]:
    """Extract the flat ``(file_path, offset, length)`` stream from scans.

    When ``dedupe=True`` (the default for prewarming), identical
    ``(file_path, offset, length)`` triples are collapsed — repeating
    them wouldn't teach shelfd anything new, and 7 days of trace
    typically hits each row-group many times.
    """
    ranges: list[tuple[str, int, int]] = []
    seen: set[tuple[str, int, int]] = set()
    for scan in scans:
        for path, _ordinal, offset, length, _etag in scan.rg_entries:
            key = (path, int(offset), int(length))
            if dedupe:
                if key in seen:
                    continue
                seen.add(key)
            ranges.append(key)
    return ranges


def _issue_one(
    endpoint: str,
    bucket: str,
    path: str,
    offset: int,
    length: int,
    timeout: float,
) -> tuple[int, int, str]:
    """Issue one range-GET; return (status_code, bytes_consumed, outcome).

    Errors are translated to ``(status, 0, "error")`` so the executor
    loop never raises — prewarm treats per-request errors as telemetry,
    not fatalities. The rollout runbook reads the aggregate error-rate,
    not per-request failures.
    """
    url = f"{endpoint}/{bucket}/{path.lstrip('/')}"
    end = offset + length - 1
    req = urllib.request.Request(url, method="GET")
    req.add_header("Range", f"bytes={offset}-{end}")
    req.add_header("X-Shelf-Client", "shelf-replay-prewarm")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            outcome = resp.headers.get("X-Shelf-Outcome", "unknown")
            data = resp.read()
            return resp.status, len(data), outcome
    except urllib.error.HTTPError as err:
        return err.code, 0, "error"
    except (urllib.error.URLError, TimeoutError):
        return 0, 0, "error"
    except OSError:
        return 0, 0, "error"
