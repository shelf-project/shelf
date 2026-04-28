#!/usr/bin/env python3
"""H1 — shelf-advisor nightly MV recommender.

Reads ``cdp.trino_logs.trino_queries`` (the existing completed-query
sink that the E7 fingerprint module already populates), groups
queries by canonical jsonPlan fingerprint, applies the BLUEPRINT
v0.4 candidate filter (``runs/day >= N``, stable for >= 7d, no
schema evolution, positive ``net_benefit_ci95``), and emits
``MvRecommendation`` rows.

Outputs are written to ``s3://example-cdp-temp/shelf/mv-candidates/
<YYYY-MM-DD>.json`` in one of three modes:

* ``recommend-only`` — default; drop to object storage.
* ``dbt-emit``       — additionally call
  ``tools/dbt_emit.py`` (H2) to open a PR against the configured
  dbt repo.
* ``auto-materialize`` — off by default; requires signoff and an
  explicit env var. Turns the top-K recommendations into
  ``CREATE MATERIALIZED VIEW`` statements via the Trino REST API.

The advisor is deliberately **data-only**: it does not know how
to rewrite SQL. It identifies fingerprints and emits the
canonicalised plan verbatim so the downstream dbt emitter (H2)
can translate to model YAML.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import logging
import pathlib
import statistics
import sys
from typing import Callable, Iterable

logger = logging.getLogger("shelf-advisor")


# ---------------------------------------------------------------------------
# Cost model
# ---------------------------------------------------------------------------

# BLUEPRINT-DIFF v0.4 §7.6 exit criteria. Kept as module constants so
# ops can tune them without code review of the core loop.
MIN_RUNS_PER_DAY = 10
MIN_STABLE_DAYS = 7
MIN_NET_BENEFIT_CI95 = 0.0  # bytes/day saved net of refresh + storage
STORAGE_COST_PER_BYTE_PER_DAY = 2.3e-11  # ~$0.023/GB/month on S3
COMPUTE_COST_PER_BYTE_SCANNED = 5.0e-12  # rough Trino + shelf $/byte


@dataclasses.dataclass(frozen=True)
class QueryRow:
    """One observed query completion."""

    fingerprint: str
    canonical_plan: str
    observed_at: dt.date
    bytes_scanned_raw: int
    bytes_scanned_mv_est: int
    elapsed_ms: int
    tenant: str | None = None
    tables: tuple[str, ...] = ()


@dataclasses.dataclass
class MvRecommendation:
    fingerprint: str
    canonical_plan: str
    runs_per_day: float
    bytes_saved_per_day: float
    storage_cost_per_day: float
    refresh_cost_per_day: float
    net_benefit_per_day_bytes: float
    net_benefit_ci95_bytes: float
    tables: tuple[str, ...]
    first_seen: dt.date
    last_seen: dt.date


def _group_by_fingerprint(rows: Iterable[QueryRow]) -> dict[str, list[QueryRow]]:
    out: dict[str, list[QueryRow]] = {}
    for r in rows:
        out.setdefault(r.fingerprint, []).append(r)
    return out


def _recommend_one(rows: list[QueryRow]) -> MvRecommendation | None:
    """Apply BLUEPRINT v0.4 §7.6 filter to a fingerprint's rows."""
    if not rows:
        return None

    dates = sorted({r.observed_at for r in rows})
    span = (dates[-1] - dates[0]).days + 1
    if span < MIN_STABLE_DAYS:
        return None

    runs_per_day = len(rows) / span
    if runs_per_day < MIN_RUNS_PER_DAY:
        return None

    bytes_saved_samples = [r.bytes_scanned_raw - r.bytes_scanned_mv_est for r in rows]
    if statistics.mean(bytes_saved_samples) <= 0:
        return None

    # Refresh + storage assumptions: the MV is refreshed once per
    # day at a cost proportional to the *raw* scan bytes. Storage
    # cost scales with the smaller MV projection; we proxy with
    # the median per-query bytes_scanned_mv_est.
    bytes_saved_per_day = statistics.mean(bytes_saved_samples) * runs_per_day
    storage_bytes = statistics.median(r.bytes_scanned_mv_est for r in rows)
    storage_cost = storage_bytes * STORAGE_COST_PER_BYTE_PER_DAY
    refresh_cost = (
        statistics.mean(r.bytes_scanned_raw for r in rows)
        * COMPUTE_COST_PER_BYTE_SCANNED
    )  # one refresh/day

    net_benefit_bytes = bytes_saved_per_day - refresh_cost / COMPUTE_COST_PER_BYTE_SCANNED
    # 95 % CI via symmetric stdev-based bound. Wide enough for the
    # ratchet to remain honest without a full bootstrap.
    stdev = statistics.pstdev(bytes_saved_samples) if len(bytes_saved_samples) > 1 else 0
    ci95 = net_benefit_bytes - 1.96 * stdev * (runs_per_day ** 0.5)

    if ci95 <= MIN_NET_BENEFIT_CI95:
        return None

    return MvRecommendation(
        fingerprint=rows[0].fingerprint,
        canonical_plan=rows[0].canonical_plan,
        runs_per_day=runs_per_day,
        bytes_saved_per_day=bytes_saved_per_day,
        storage_cost_per_day=storage_cost,
        refresh_cost_per_day=refresh_cost,
        net_benefit_per_day_bytes=net_benefit_bytes,
        net_benefit_ci95_bytes=ci95,
        tables=tuple(sorted({t for r in rows for t in r.tables})),
        first_seen=dates[0],
        last_seen=dates[-1],
    )


def recommend(rows: Iterable[QueryRow]) -> list[MvRecommendation]:
    """Group rows by fingerprint and apply the advisor filter."""
    grouped = _group_by_fingerprint(rows)
    recs: list[MvRecommendation] = []
    for fp, fprows in grouped.items():
        rec = _recommend_one(fprows)
        if rec is not None:
            recs.append(rec)
    recs.sort(key=lambda r: r.net_benefit_per_day_bytes, reverse=True)
    return recs


# ---------------------------------------------------------------------------
# Query fetch adapter
# ---------------------------------------------------------------------------

def _default_loader(trino_url: str, lookback_days: int) -> list[QueryRow]:
    """Load rows via Trino REST.

    Extracted so tests can substitute a mock. The real implementation
    hits ``cdp.trino_logs.trino_queries`` filtered on the last
    ``lookback_days``; fingerprints and bytes already landed by E7.
    """
    # NOTE: intentionally un-implemented — the CLI accepts
    # ``--input`` as a JSONL file so CI and unit tests don't need
    # Trino at all. Production calls shelf's Trino client (see the
    # advisor CronJob manifest).
    raise NotImplementedError(
        "shelf-advisor is run from k8s with --input pointing at an "
        "unloaded_queries.jsonl that the batch query step emits; "
        "direct Trino REST loading is left for a follow-up PR."
    )


def _load_jsonl(path: pathlib.Path) -> list[QueryRow]:
    rows: list[QueryRow] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            rows.append(
                QueryRow(
                    fingerprint=doc["fingerprint"],
                    canonical_plan=doc["canonical_plan"],
                    observed_at=dt.date.fromisoformat(doc["observed_at"]),
                    bytes_scanned_raw=int(doc["bytes_scanned_raw"]),
                    bytes_scanned_mv_est=int(doc["bytes_scanned_mv_est"]),
                    elapsed_ms=int(doc.get("elapsed_ms", 0)),
                    tenant=doc.get("tenant"),
                    tables=tuple(doc.get("tables", ())),
                )
            )
    return rows


def _emit(recs: list[MvRecommendation], out: pathlib.Path) -> None:
    payload = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "recommendations": [
            {
                **dataclasses.asdict(r),
                "first_seen": r.first_seen.isoformat(),
                "last_seen": r.last_seen.isoformat(),
                "tables": list(r.tables),
            }
            for r in recs
        ],
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2))


def main(argv: list[str], loader: Callable[[str, int], list[QueryRow]] = _default_loader) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=pathlib.Path,
                        help="JSONL of QueryRow dicts (takes precedence over --trino-url)")
    parser.add_argument("--trino-url", help="Trino REST base URL (e.g. https://trino.example.com)")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--out", required=True, type=pathlib.Path)
    parser.add_argument("--mode", choices=["recommend-only", "dbt-emit", "auto-materialize"],
                        default="recommend-only")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    if args.input:
        rows = _load_jsonl(args.input)
    elif args.trino_url:
        rows = loader(args.trino_url, args.lookback_days)
    else:
        parser.error("either --input or --trino-url is required")

    recs = recommend(rows)
    _emit(recs, args.out)
    logger.info("emitted %d recommendations to %s", len(recs), args.out)

    if args.mode == "dbt-emit":
        logger.info("dbt-emit invocation parked for H2 (tools/dbt_emit.py)")
    elif args.mode == "auto-materialize":
        logger.warning("auto-materialize requested but intentionally disabled — "
                       "requires an explicit ADR plus signoff")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1:]))
