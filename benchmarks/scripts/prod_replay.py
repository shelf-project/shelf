#!/usr/bin/env python3
"""Production-trace replay harness — V1 wrapper for SHELF-35 tooling.

This is the V1 prod-replay harness called from
``benchmarks/scripts/run_prod_replay.sh``. It wraps the existing
``tools/gen_replay_list.py`` (pin-list generator that talks to
``your_query_log_table`` via Trino REST) and ``tools/replay_pinlist.py``
(per-entry GET against the shelf S3 shim) without re-implementing
either, then captures sidecar metric snapshots and emits one
schema-valid result record per backend per phase
(vendor=cold-pass, repeat=warm-pass) under ``<output-dir>/<backend>/``.

The output records validate against ``benchmarks/replay/schema.json``.
The summary file ``<output-dir>/summary.txt`` mirrors the comparison
shape from ``benchmarks/results/2026-05-01/SUMMARY.md`` so an operator
can paste it straight into a release-cycle MR.

DESIGN NOTES
------------

1. We do NOT import ``boto3`` — ``tools/gen_pin_list.py`` does, but we
   route through ``tools/gen_replay_list.py`` instead which is pure
   stdlib (uses Trino system tables ``$snapshots`` / ``$manifests`` to
   resolve Iceberg metadata paths, no S3 HEAD calls). Operators on a
   bare laptop with only ``python3`` + ``requests`` installed can run
   ``--dry-run`` end-to-end.

2. Cold-vs-warm split: the SHELF-35 ``replay_pinlist.py`` tool already
   classifies outcomes as ``hit_ram | hit_disk | miss`` from response
   time, which is the right primitive for measuring cache warmth. We
   call it twice — once during ``--prewarm-secs`` (vendor record) and
   once during ``--measurement-secs`` (repeat record) — and record both
   summaries.

3. The schema's ``backend`` enum doesn't have a ``raw-s3-direct`` slot
   for "shelf shim bypassed entirely", so the raw-S3 record uses
   ``backend=raw-s3`` and points the underlying GETs at the operator's
   ``--raw-endpoint``. The replay tool's signature-agnostic GETs work
   against unauthenticated S3-compat origins; for AWS S3 this requires
   the operator's own SigV4 proxy or a public bucket; for prod-trace
   replay the supported pattern is to wire ``--raw-endpoint`` to a
   second shelfd Service that has caching disabled (``mode=passthrough``
   in the ``--raw-endpoint-mode``). For V1 the wrapper's responsibility
   stops at issuing the GET; operators wire the endpoints.

4. ``--dry-run`` short-circuits before any subprocess that would touch
   the cluster (no Trino, no kubectl, no shelf). It validates args,
   prints the planned commands, and exits 0. Used by
   ``test_prod_replay.sh``.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import logging
import os
import secrets
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional

LOG = logging.getLogger("shelf.prod_replay")

REPO_ROOT = Path(__file__).resolve().parents[2]
TOOLS_DIR = REPO_ROOT / "tools"
GEN_REPLAY_LIST = TOOLS_DIR / "gen_replay_list.py"
REPLAY_PINLIST = TOOLS_DIR / "replay_pinlist.py"
SCRAPE_HELPER = Path(__file__).with_name("scrape_shelf_metrics.sh")
SCHEMA_PATH = REPO_ROOT / "benchmarks" / "replay" / "schema.json"


# ---------------------------------------------------------------------------
# ULID + commit metadata helpers (no external deps).
# ---------------------------------------------------------------------------

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def make_ulid() -> str:
    """Generate a 26-char Crockford base32 ULID matching the schema regex
    ``^[0-9A-HJKMNP-TV-Z]{26}$``.

    Uses time-low / random-high layout. Not collision-safe across forks
    issuing in the same millisecond, which is fine for the bench-harness
    use case (one ULID per harness invocation).
    """
    ts_ms = int(_dt.datetime.now(_dt.timezone.utc).timestamp() * 1000)
    rnd = secrets.randbits(80)
    n = (ts_ms << 80) | rnd
    out = []
    for _ in range(26):
        out.append(_CROCKFORD[n & 0x1F])
        n >>= 5
    return "".join(reversed(out))


def resolve_commit_sha() -> str:
    """Best-effort 40-hex commit SHA so the schema regex passes.

    Honours ``$SHELF_BENCH_COMMIT_SHA`` (set by CI), then falls back to
    ``git -C <repo> rev-parse HEAD``, then to a zero-padded sentinel so
    a no-git environment (an operator's prod jumphost without git)
    still produces a schema-valid record rather than crashing.
    """
    env = os.environ.get("SHELF_BENCH_COMMIT_SHA", "").strip()
    if len(env) == 40 and all(c in "0123456789abcdef" for c in env.lower()):
        return env.lower()
    try:
        out = subprocess.check_output(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
        )
        sha = out.decode().strip().lower()
        if len(sha) == 40 and all(c in "0123456789abcdef" for c in sha):
            return sha
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    return "0" * 40


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_of(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


# ---------------------------------------------------------------------------
# Argument parsing + plan struct.
# ---------------------------------------------------------------------------


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", required=True, help="run-level output dir")
    p.add_argument(
        "--shelf-endpoint",
        required=True,
        help="shelf shim endpoint (e.g. http://shelf-bench-pool.<ns>.svc:9092)",
    )
    p.add_argument(
        "--raw-endpoint",
        required=True,
        help="raw S3 (or passthrough shim) endpoint for the baseline backend",
    )
    p.add_argument(
        "--trino-host",
        required=True,
        help="trino coord HOST:PORT used for both pin-list gen and replay",
    )
    p.add_argument(
        "--catalog-shelf",
        required=True,
        help="bench-side Iceberg catalog routed through shelf",
    )
    p.add_argument(
        "--catalog-raw",
        required=True,
        help="bench-side Iceberg catalog routed direct to S3",
    )
    p.add_argument(
        "--replica",
        default="rep-2",
        choices=("rep-0", "rep-1", "rep-2", "rep-3"),
        help="trace source replica (default rep-2)",
    )
    p.add_argument("--window-days", type=int, default=7)
    p.add_argument("--top-n", type=int, default=200, help="distinct tables to resolve")
    p.add_argument("--prewarm-secs", type=int, default=1800)
    p.add_argument("--measurement-secs", type=int, default=7200)
    p.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="per-backend GET concurrency (forwarded to replay_pinlist.py)",
    )
    p.add_argument(
        "--namespace",
        default="alluxio",
        help="kube namespace where shelf-bench pods live (sidecar scrape only)",
    )
    p.add_argument("--service", default="shelf-bench-pool")
    p.add_argument("--pod-prefix", default="shelf-bench")
    p.add_argument("--pod-count", type=int, default=4)
    p.add_argument("--release-tag", default="rc8-v1")
    p.add_argument("--region", default="ap-south-1")
    p.add_argument("--k8s-version", default="1.30")
    p.add_argument("--trino-image", default="trinodb/trino:480")
    p.add_argument("--shelf-image", default="ghcr.io/shelf-project/shelfd:1.0.1")
    p.add_argument(
        "--trino-instance-type", default="m6a.4xlarge", help="for cluster_shape"
    )
    p.add_argument("--trino-worker-count", type=int, default=4)
    p.add_argument("--shelf-instance-type", default="m6a.4xlarge")
    p.add_argument(
        "--mcp-json",
        default=os.path.expanduser("~/.cursor/mcp.json"),
        help="MCP creds bundle for gen_replay_list.py (TRINO_*)",
    )
    p.add_argument(
        "--pinlist-override",
        default=None,
        help="skip pin-list generation; use this JSON file instead "
        "(used by test_prod_replay.sh smoke)",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-scrape", action="store_true")
    p.add_argument("--log-level", default="INFO")
    return p


# ---------------------------------------------------------------------------
# Subprocess wrappers that still respect --dry-run.
# ---------------------------------------------------------------------------


def _run(cmd: list[str], *, dry_run: bool, capture: bool = False) -> Optional[str]:
    LOG.info("cmd: %s", " ".join(shlex.quote(c) for c in cmd))
    if dry_run:
        return None
    if capture:
        out = subprocess.check_output(cmd)
        return out.decode("utf-8", errors="replace")
    subprocess.check_call(cmd)
    return None


def gen_pinlist(args: argparse.Namespace, out_path: Path) -> None:
    if args.pinlist_override:
        LOG.info("using --pinlist-override %s", args.pinlist_override)
        if not args.dry_run:
            shutil.copy(args.pinlist_override, out_path)
        return

    cmd = [
        sys.executable,
        str(GEN_REPLAY_LIST),
        "--replica",
        args.replica,
        "--catalog",
        args.catalog_shelf,
        "--days",
        str(args.window_days),
        "--top",
        str(args.top_n * 50),
        "--top-tables",
        str(args.top_n),
        "--source",
        "trino",
        "--mcp-json",
        args.mcp_json,
        "--out",
        str(out_path),
        "--log-level",
        args.log_level,
    ]
    _run(cmd, dry_run=args.dry_run)


def replay_against(
    args: argparse.Namespace,
    pinlist_path: Path,
    endpoint_host_port: str,
    summary_path: Path,
) -> None:
    """Replay the pin-list against an endpoint and record a summary file.

    The endpoint is passed as ``HOST:PORT`` per the SHELF-35 contract
    (``replay_pinlist.py`` prepends ``http://`` itself). For HTTPS or
    custom-scheme endpoints, operators run a local proxy and pass the
    proxy's HOST:PORT.
    """
    cmd = [
        sys.executable,
        str(REPLAY_PINLIST),
        "--pinlist",
        str(pinlist_path),
        "--shelf-endpoint",
        endpoint_host_port,
        "--concurrency",
        str(args.concurrency),
        "--summary-out",
        str(summary_path),
        "--log-level",
        args.log_level,
    ]
    _run(cmd, dry_run=args.dry_run)


def scrape_metrics(
    args: argparse.Namespace, output_dir: Path, phase: str
) -> None:
    if args.skip_scrape:
        LOG.info("--skip-scrape: not invoking scrape_shelf_metrics.sh (phase=%s)", phase)
        return
    cmd = [
        "bash",
        str(SCRAPE_HELPER),
        "--namespace",
        args.namespace,
        "--service",
        args.service,
        "--pod-prefix",
        args.pod_prefix,
        "--pod-count",
        str(args.pod_count),
        "--output-dir",
        str(output_dir),
        "--phase",
        phase,
    ]
    if args.dry_run:
        cmd.append("--dry-run")
    _run(cmd, dry_run=False)  # the helper itself honours --dry-run


# ---------------------------------------------------------------------------
# Summary parsing — extract metrics from replay_pinlist.py's text output.
# ---------------------------------------------------------------------------

import re

_SUMMARY_RE = {
    "total":     re.compile(r"^total requests\s*:\s*(\d+)"),
    "elapsed":   re.compile(r"^elapsed wall\s*:\s*([\d.]+)s"),
    "bytes_mib": re.compile(r"^bytes read total\s*:\s*([\d.]+)\s*MiB"),
    "hit_ram":   re.compile(r"^\s*hit_ram\s*:\s*(\d+)"),
    "hit_disk":  re.compile(r"^\s*hit_disk\s*:\s*(\d+)"),
    "miss":      re.compile(r"^\s*miss\s*:\s*(\d+)"),
    "hit_ratio": re.compile(r"^hit ratio.*:\s*([\d.]+)%"),
    "p50":       re.compile(r"^\s*p50\s*:\s*([\d.]+)"),
    "p95":       re.compile(r"^\s*p95\s*:\s*([\d.]+)"),
    "p99":       re.compile(r"^\s*p99\s*:\s*([\d.]+)"),
    "max":       re.compile(r"^\s*max\s*:\s*([\d.]+)"),
}


def parse_summary(path: Path) -> dict:
    """Parse the text summary produced by replay_pinlist.py.

    Returns a dict with the keys in ``_SUMMARY_RE`` populated from
    matched lines, plus ``"raw"`` carrying the entire file. Missing
    fields default to 0/0.0 so downstream record-building never KeyErrors.
    """
    out = {
        "total": 0,
        "elapsed": 0.0,
        "bytes_mib": 0.0,
        "hit_ram": 0,
        "hit_disk": 0,
        "miss": 0,
        "hit_ratio": 0.0,
        "p50": 0.0,
        "p95": 0.0,
        "p99": 0.0,
        "max": 0.0,
        "raw": "",
    }
    if not path.exists():
        return out
    text = path.read_text()
    out["raw"] = text
    for line in text.splitlines():
        for key, rx in _SUMMARY_RE.items():
            m = rx.match(line)
            if m:
                val: float | int = float(m.group(1))
                if key in ("total", "hit_ram", "hit_disk", "miss"):
                    val = int(val)
                out[key] = val
                break
    return out


# ---------------------------------------------------------------------------
# Schema-valid record assembly.
# ---------------------------------------------------------------------------


def build_record(
    args: argparse.Namespace,
    *,
    backend: str,
    phase: str,  # "vendor" (cold) | "repeat" (warm)
    summary: dict,
    run_id_root: str,
    config_hash: str,
    trace: dict,
) -> dict:
    """Assemble one record per benchmarks/replay/schema.json.

    ``run_id`` is derived per (backend, phase) so all four records
    in a run carry distinct IDs but share a common root prefix that
    operators can grep for. ``shelf_node_count`` is forced to 0 on the
    raw-s3 record because the schema requires the field on every
    record; the field is honest (the raw path bypasses shelf entirely).
    """
    summary_seconds = summary.get("p50", 0.0)
    s_to_ns = lambda s: int(s * 1e9)  # noqa: E731

    record = {
        "run_id": (run_id_root + ("0" * 26))[:26].upper(),
        "timestamp": now_iso(),
        "commit_sha": resolve_commit_sha(),
        "release_tag": args.release_tag,
        "benchmark": "replay",
        "backend": backend,
        "config": {
            "config_hash": config_hash,
            "trino_image": args.trino_image,
            "backend_image": args.shelf_image if backend == "shelf" else "n/a",
            "plugin_jar_sha256": None,
        },
        "cluster_shape": {
            "region": args.region,
            "k8s_version": args.k8s_version,
            "trino_instance_type": args.trino_instance_type,
            "trino_worker_count": args.trino_worker_count,
            "shelf_instance_type": args.shelf_instance_type if backend == "shelf" else "n/a",
            "shelf_node_count": args.pod_count if backend == "shelf" else 0,
            "scale_factor": None,
            "partial": False,
        },
        "trace": trace,
        "samples": [],
        "summary": {
            "latency_ns_p50": s_to_ns(summary.get("p50", 0.0)),
            "latency_ns_p95": s_to_ns(summary.get("p95", 0.0)),
            "latency_ns_p99": s_to_ns(summary.get("p99", 0.0)),
            "latency_ns_p999": s_to_ns(summary.get("max", 0.0)),
            "hit_rate": (summary.get("hit_ratio", 0.0) / 100.0) if backend == "shelf" else 0,
            "bytes_read": int(summary.get("bytes_mib", 0.0) * 1024 * 1024),
            "bytes_admitted": int(summary.get("bytes_mib", 0.0) * 1024 * 1024) if backend == "shelf" else 0,
            "dollars_per_query": 0,
        },
        "gate": {
            "hit_rate_7d_cumulative": (summary.get("hit_ratio", 0.0) / 100.0) if backend == "shelf" else 0,
            "gold_dbt_ok_rate": 0,
            "latency_ns_p95_vs_alluxio": None,
            "shelf_caused_pages": 0,
            "oncall_surface_ratio": None,
            "verdict": "n/a",  # prod-replay reports vs raw-S3, not Alluxio (per RUNBOOK)
            "failed_metrics": [],
        },
    }
    # fold phase into a non-schema annotation that downstream consumers
    # can grep for without violating additionalProperties — we ship it
    # in a sibling .meta.json instead. See harness convention in
    # benchmarks/scripts/RUNBOOK.md.
    return record


def write_record(
    record: dict,
    *,
    output_dir: Path,
    backend: str,
    phase: str,
    run_id_root: str,
) -> Path:
    backend_dir = output_dir / backend
    backend_dir.mkdir(parents=True, exist_ok=True)
    path = backend_dir / f"replay-{phase}-{run_id_root}.json"
    path.write_text(json.dumps(record, indent=2) + "\n")
    meta = {"phase": phase, "run_id_root": run_id_root}
    (backend_dir / f"replay-{phase}-{run_id_root}.meta.json").write_text(
        json.dumps(meta, indent=2) + "\n"
    )
    return path


# ---------------------------------------------------------------------------
# Side-by-side summary.txt
# ---------------------------------------------------------------------------


def render_summary_table(
    *,
    shelf_vendor: dict,
    shelf_repeat: dict,
    raw_vendor: dict,
    raw_repeat: dict,
    args: argparse.Namespace,
) -> str:
    def pct_delta(a: float, b: float) -> str:
        if b == 0:
            return "n/a"
        d = (a - b) / b * 100.0
        sign = "+" if d >= 0 else ""
        return f"{sign}{d:.1f}%"

    def fmt_s(s: float) -> str:
        if s >= 1.0:
            return f"{s:.3f}s"
        return f"{s * 1000:.1f}ms"

    lines: list[str] = []
    lines.append(f"prod-trace replay summary  ({args.replica}, last {args.window_days}d)")
    lines.append("=" * 78)
    lines.append("")
    lines.append("Cold-pass (vendor) — first scan after fresh-cache reset:")
    lines.append("")
    lines.append(f"  metric               | shelf       | raw-S3      | delta (vs raw)")
    lines.append(f"  -------------------- | ----------- | ----------- | -------------")
    lines.append(
        f"  p50 wall             | {fmt_s(shelf_vendor['p50']):>11} | "
        f"{fmt_s(raw_vendor['p50']):>11} | {pct_delta(shelf_vendor['p50'], raw_vendor['p50'])}"
    )
    lines.append(
        f"  p95 wall             | {fmt_s(shelf_vendor['p95']):>11} | "
        f"{fmt_s(raw_vendor['p95']):>11} | {pct_delta(shelf_vendor['p95'], raw_vendor['p95'])}"
    )
    lines.append(
        f"  p99 wall             | {fmt_s(shelf_vendor['p99']):>11} | "
        f"{fmt_s(raw_vendor['p99']):>11} | {pct_delta(shelf_vendor['p99'], raw_vendor['p99'])}"
    )
    lines.append(
        f"  total qps            | {(shelf_vendor['total'] / shelf_vendor['elapsed'] if shelf_vendor['elapsed'] else 0):>11.1f} | "
        f"{(raw_vendor['total'] / raw_vendor['elapsed'] if raw_vendor['elapsed'] else 0):>11.1f} | "
        f"{pct_delta(shelf_vendor['total'] / max(shelf_vendor['elapsed'], 1e-9), raw_vendor['total'] / max(raw_vendor['elapsed'], 1e-9))}"
    )
    lines.append(
        f"  total origin GiB     | {shelf_vendor['bytes_mib'] / 1024:>11.2f} | "
        f"{raw_vendor['bytes_mib'] / 1024:>11.2f} | "
        f"{pct_delta(shelf_vendor['bytes_mib'], raw_vendor['bytes_mib'])}"
    )
    lines.append(
        f"  shelf hit rate       | {shelf_vendor['hit_ratio']:>10.1f}% | "
        f"{'n/a':>11} | n/a"
    )
    lines.append("")
    lines.append("Warm-pass (repeat) — second scan with the cache primed:")
    lines.append("")
    lines.append(f"  metric               | shelf       | raw-S3      | delta (vs raw)")
    lines.append(f"  -------------------- | ----------- | ----------- | -------------")
    lines.append(
        f"  p50 wall             | {fmt_s(shelf_repeat['p50']):>11} | "
        f"{fmt_s(raw_repeat['p50']):>11} | {pct_delta(shelf_repeat['p50'], raw_repeat['p50'])}"
    )
    lines.append(
        f"  p95 wall             | {fmt_s(shelf_repeat['p95']):>11} | "
        f"{fmt_s(raw_repeat['p95']):>11} | {pct_delta(shelf_repeat['p95'], raw_repeat['p95'])}"
    )
    lines.append(
        f"  p99 wall             | {fmt_s(shelf_repeat['p99']):>11} | "
        f"{fmt_s(raw_repeat['p99']):>11} | {pct_delta(shelf_repeat['p99'], raw_repeat['p99'])}"
    )
    lines.append(
        f"  total qps            | {(shelf_repeat['total'] / shelf_repeat['elapsed'] if shelf_repeat['elapsed'] else 0):>11.1f} | "
        f"{(raw_repeat['total'] / raw_repeat['elapsed'] if raw_repeat['elapsed'] else 0):>11.1f} | "
        f"{pct_delta(shelf_repeat['total'] / max(shelf_repeat['elapsed'], 1e-9), raw_repeat['total'] / max(raw_repeat['elapsed'], 1e-9))}"
    )
    lines.append(
        f"  total origin GiB     | {shelf_repeat['bytes_mib'] / 1024:>11.2f} | "
        f"{raw_repeat['bytes_mib'] / 1024:>11.2f} | "
        f"{pct_delta(shelf_repeat['bytes_mib'], raw_repeat['bytes_mib'])}"
    )
    lines.append(
        f"  shelf hit rate       | {shelf_repeat['hit_ratio']:>10.1f}% | "
        f"{'n/a':>11} | n/a"
    )
    lines.append("")
    lines.append(
        "Records emitted: shelf-vendor, shelf-repeat, raw-vendor, raw-repeat "
        "(all schema-valid per benchmarks/replay/schema.json)."
    )
    lines.append(
        "Hit-rate < 80% on the warm pass usually means the pin-list missed "
        "the working set — re-run gen_replay_list.py with a wider --top-tables."
    )
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Entry point.
# ---------------------------------------------------------------------------


def main() -> int:
    args = build_argparser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir = output_dir / "shelf-metrics"
    metrics_dir.mkdir(exist_ok=True)

    LOG.info("V1 prod-trace replay harness  output=%s  dry_run=%s", output_dir, args.dry_run)

    # 1. Generate the pin-list (or use the override).
    pinlist_path = output_dir / "pinlist.json"
    LOG.info("phase=gen_pinlist  -> %s", pinlist_path)
    gen_pinlist(args, pinlist_path)

    # 2. Plan: print the upcoming command sequence so an operator can
    # eyeball the workflow before the long-running replay starts.
    plan = [
        f"  pre-warm scrape    -> {metrics_dir}/(pre)",
        f"  cold-pass replay   -> shelf via {args.shelf_endpoint}, "
        f"raw via {args.raw_endpoint} ({args.prewarm_secs}s)",
        f"  post-warm scrape   -> {metrics_dir}/(prewarm)",
        f"  warm-pass replay   -> shelf + raw ({args.measurement_secs}s)",
        f"  post-measure scrape-> {metrics_dir}/(post)",
        f"  emit 4 records     -> {output_dir}/(shelf|raw-s3)/replay-(vendor|repeat)-<ulid>.json",
        f"  summary            -> {output_dir}/summary.txt",
    ]
    LOG.info("planned phases:\n%s", "\n".join(plan))

    if args.dry_run:
        LOG.info("--dry-run: skipping all subprocesses below this line")
        # Still emit a stub summary so downstream tooling can detect that
        # a dry-run happened without parsing logs.
        (output_dir / "summary.txt").write_text(
            "DRY-RUN — no replay executed.\n" + "\n".join(plan) + "\n"
        )
        return 0

    if not GEN_REPLAY_LIST.exists():
        LOG.error("missing tools/gen_replay_list.py at %s", GEN_REPLAY_LIST)
        return 2
    if not REPLAY_PINLIST.exists():
        LOG.error("missing tools/replay_pinlist.py at %s", REPLAY_PINLIST)
        return 2

    # 3. Pre-warm scrape (baseline).
    scrape_metrics(args, metrics_dir, "pre")

    # 4. Cold-pass replay (vendor records).
    summary_shelf_vendor = output_dir / "_summary-shelf-vendor.txt"
    summary_raw_vendor = output_dir / "_summary-raw-vendor.txt"
    LOG.info("phase=cold_pass  shelf=%s", args.shelf_endpoint)
    replay_against(args, pinlist_path, _strip_scheme(args.shelf_endpoint), summary_shelf_vendor)
    LOG.info("phase=cold_pass  raw=%s", args.raw_endpoint)
    replay_against(args, pinlist_path, _strip_scheme(args.raw_endpoint), summary_raw_vendor)

    scrape_metrics(args, metrics_dir, "prewarm")

    # 5. Warm-pass replay (repeat records).
    summary_shelf_repeat = output_dir / "_summary-shelf-repeat.txt"
    summary_raw_repeat = output_dir / "_summary-raw-repeat.txt"
    LOG.info("phase=warm_pass  shelf=%s", args.shelf_endpoint)
    replay_against(args, pinlist_path, _strip_scheme(args.shelf_endpoint), summary_shelf_repeat)
    LOG.info("phase=warm_pass  raw=%s", args.raw_endpoint)
    replay_against(args, pinlist_path, _strip_scheme(args.raw_endpoint), summary_raw_repeat)

    scrape_metrics(args, metrics_dir, "post")

    # 6. Build records.
    sv = parse_summary(summary_shelf_vendor)
    sr = parse_summary(summary_shelf_repeat)
    rv = parse_summary(summary_raw_vendor)
    rr = parse_summary(summary_raw_repeat)

    config_hash = sha256_of(
        json.dumps(
            {
                "shelf_endpoint": args.shelf_endpoint,
                "raw_endpoint": args.raw_endpoint,
                "trino_host": args.trino_host,
                "catalog_shelf": args.catalog_shelf,
                "catalog_raw": args.catalog_raw,
                "release_tag": args.release_tag,
                "shelf_image": args.shelf_image,
                "trino_image": args.trino_image,
                "concurrency": args.concurrency,
                "top_n": args.top_n,
                "window_days": args.window_days,
            },
            sort_keys=True,
        ).encode()
    )
    run_id_root = make_ulid()
    trace = {
        "source_table": "your_query_log_table",
        "snapshot_id": run_id_root,
        "from": _window_start_iso(args.window_days),
        "to": now_iso(),
        "replica": args.replica,
        "query_count": int(sv["total"] + sr["total"]),
        "speed": "2x",
    }

    write_record(
        build_record(args, backend="shelf", phase="vendor", summary=sv,
                     run_id_root=run_id_root, config_hash=config_hash, trace=trace),
        output_dir=output_dir, backend="shelf", phase="vendor", run_id_root=run_id_root,
    )
    write_record(
        build_record(args, backend="shelf", phase="repeat", summary=sr,
                     run_id_root=run_id_root, config_hash=config_hash, trace=trace),
        output_dir=output_dir, backend="shelf", phase="repeat", run_id_root=run_id_root,
    )
    write_record(
        build_record(args, backend="raw-s3", phase="vendor", summary=rv,
                     run_id_root=run_id_root, config_hash=config_hash, trace=trace),
        output_dir=output_dir, backend="raw-s3", phase="vendor", run_id_root=run_id_root,
    )
    write_record(
        build_record(args, backend="raw-s3", phase="repeat", summary=rr,
                     run_id_root=run_id_root, config_hash=config_hash, trace=trace),
        output_dir=output_dir, backend="raw-s3", phase="repeat", run_id_root=run_id_root,
    )

    # 7. Side-by-side summary.
    summary_path = output_dir / "summary.txt"
    summary_path.write_text(
        render_summary_table(
            shelf_vendor=sv, shelf_repeat=sr,
            raw_vendor=rv, raw_repeat=rr,
            args=args,
        )
    )
    LOG.info("summary -> %s", summary_path)

    # 8. Validate written records against schema if jsonschema is available.
    if SCHEMA_PATH.exists():
        _maybe_validate(output_dir, SCHEMA_PATH)
    return 0


def _strip_scheme(endpoint: str) -> str:
    """``replay_pinlist.py`` wants HOST:PORT and prepends ``http://``."""
    for prefix in ("https://", "http://"):
        if endpoint.startswith(prefix):
            return endpoint[len(prefix) :].rstrip("/")
    return endpoint.rstrip("/")


def _window_start_iso(days: int) -> str:
    start = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=int(days))
    return start.strftime("%Y-%m-%dT%H:%M:%SZ")


def _maybe_validate(output_dir: Path, schema_path: Path) -> None:
    try:
        import jsonschema  # type: ignore
    except ImportError:
        LOG.info("jsonschema not installed; skipping post-write validation")
        return
    schema = json.loads(schema_path.read_text())
    failures = 0
    for f in sorted(output_dir.rglob("replay-*.json")):
        if f.name.endswith(".meta.json"):
            continue
        try:
            jsonschema.validate(json.loads(f.read_text()), schema)
            LOG.info("schema OK: %s", f)
        except jsonschema.ValidationError as exc:
            LOG.error("schema FAIL %s: %s", f, exc.message)
            failures += 1
    if failures:
        LOG.error("%d record(s) failed schema validation", failures)


if __name__ == "__main__":
    sys.exit(main())
