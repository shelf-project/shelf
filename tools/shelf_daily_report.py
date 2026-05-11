#!/usr/bin/env python3
"""
SHELF — daily ops report: pull key Prometheus/Mimir series (+ optional Trino CSV),
evaluate against tools/reports/success_metrics.json, emit JSON + Markdown.

Usage:
  export GRAFANA_URL=https://platform-grafana.example.com
  export GRAFANA_TOKEN=...   # service account with datasources:query
  export GRAFANA_PROM_DS_UID=ddy2eykq2tfy8a   # mimir-data example
  python3 tools/shelf_daily_report.py [--date YYYY-MM-DD] [--out-dir tools/reports]

Trino block is optional:
  export TRINO_LOGS_CSV=/path/to/export.csv   # pre-exported aggregates
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def grafana_query(
    base_url: str,
    token: str,
    datasource_uid: str,
    promql: str,
    start_s: int,
    end_s: int,
) -> dict[str, Any]:
    url = base_url.rstrip("/") + "/api/ds/query"
    body = {
        "queries": [
            {
                "refId": "A",
                "datasource": {"type": "prometheus", "uid": datasource_uid},
                "expr": promql,
                "intervalMs": 60000,
                "maxDataPoints": 500,
            }
        ],
        "from": str(start_s * 1000),
        "to": str(end_s * 1000),
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.load(resp)


def instant_value(resp: dict[str, Any]) -> float | None:
    """Best-effort: first instant vector value."""
    try:
        for res in resp.get("results", {}).values():
            frames = res.get("frames", [])
            for fr in frames:
                data = fr.get("data", {})
                values = data.get("values", [])
                if values and len(values) > 1 and values[1]:
                    return float(values[1][-1])
    except (IndexError, TypeError, ValueError):
        pass
    return None


def traffic_light(value: float | None, spec: dict[str, Any], higher_is_better: bool) -> str:
    if value is None:
        return "AMBER"
    if higher_is_better:
        gmin = float(spec["green_min"])
        amin = float(spec["amber_min"])
        if value >= gmin:
            return "GREEN"
        if value >= amin:
            return "AMBER"
        return "RED"
    gmax = float(spec["green_max"])
    amax = float(spec["amber_max"])
    if value <= gmax:
        return "GREEN"
    if value <= amax:
        return "AMBER"
    return "RED"


def reverse_light(value: float | None, spec: dict[str, Any]) -> str:
    """Lower is better (invert thresholds as green_min / amber_min on upper bound)."""
    if value is None:
        return "AMBER"
    if value <= float(spec.get("green_max", 0)):
        return "GREEN"
    if value <= float(spec.get("amber_max", 0)):
        return "AMBER"
    return "RED"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--out-dir", type=Path, default=Path(__file__).resolve().parent / "reports")
    ap.add_argument("--metrics", type=Path, default=Path(__file__).resolve().parent / "reports" / "success_metrics.json")
    args = ap.parse_args()

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    thresholds = load_json(args.metrics)

    report: dict[str, Any] = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "report_date": args.date,
        "prometheus": {},
        "trino": {},
    }
    prev_file = out_dir / f"{(date.fromisoformat(args.date) - timedelta(days=1)).isoformat()}.json"

    g_url = os.environ.get("GRAFANA_URL", "").strip()
    g_tok = os.environ.get("GRAFANA_TOKEN", "").strip()
    ds_uid = os.environ.get("GRAFANA_PROM_DS_UID", "").strip()
    end = datetime.fromisoformat(args.date + "T23:59:59")
    start = datetime.fromisoformat(args.date + "T00:00:00")
    start_s, end_s = int(start.timestamp()), int(end.timestamp())

    if g_url and g_tok and ds_uid:
        pm = thresholds.get("prometheus", {})
        try:
            if "lodc_drop_ratio_vs_miss" in pm:
                q = pm["lodc_drop_ratio_vs_miss"]["expr_hint"]
                v = instant_value(grafana_query(g_url, g_tok, ds_uid, q, start_s, end_s))
                report["prometheus"]["lodc_drop_ratio_vs_miss"] = {
                    "value": v,
                    "light": reverse_light(
                        v,
                        {
                            "green_max": pm["lodc_drop_ratio_vs_miss"]["green_max"],
                            "amber_max": pm["lodc_drop_ratio_vs_miss"]["amber_max"],
                        },
                    ),
                }
            if "rolling_hit_ratio_rowgroup_bps" in pm:
                q = pm["rolling_hit_ratio_rowgroup_bps"]["expr_hint"]
                v = instant_value(grafana_query(g_url, g_tok, ds_uid, q, start_s, end_s))
                report["prometheus"]["rolling_hit_ratio_rowgroup_bps"] = {
                    "value": v,
                    "light": traffic_light(
                        v,
                        {
                            "green_min": pm["rolling_hit_ratio_rowgroup_bps"]["green_min"],
                            "amber_min": pm["rolling_hit_ratio_rowgroup_bps"]["amber_min"],
                        },
                        higher_is_better=True,
                    ),
                }
            if "origin_get_mb_per_s" in pm:
                q = pm["origin_get_mb_per_s"]["expr_hint"]
                v = instant_value(grafana_query(g_url, g_tok, ds_uid, q, start_s, end_s))
                report["prometheus"]["origin_get_mb_per_s"] = {
                    "value": v,
                    "light": traffic_light(
                        v,
                        {
                            "green_min": pm["origin_get_mb_per_s"]["green_min"],
                            "amber_min": pm["origin_get_mb_per_s"]["amber_min"],
                        },
                        higher_is_better=True,
                    ),
                }
        except urllib.error.HTTPError as e:
            report["prometheus"]["_error"] = f"grafana HTTP {e.code}: {e.read()[:500]!r}"
        except Exception as e:  # noqa: BLE001
            report["prometheus"]["_error"] = str(e)
    else:
        report["prometheus"]["_skipped"] = "Set GRAFANA_URL, GRAFANA_TOKEN, GRAFANA_PROM_DS_UID"

    # Optional Trino CSV: columns fail_rate, iceberg_cannot_open_split_per_1k
    tcsv = os.environ.get("TRINO_LOGS_CSV", "").strip()
    if tcsv and Path(tcsv).is_file():
        report["trino"]["note"] = "Loaded TRINO_LOGS_CSV — extend parser for your export shape"
    else:
        report["trino"]["_skipped"] = "Set TRINO_LOGS_CSV for Trino aggregates or query cdp.trino_logs.trino_queries separately"

    out_json = out_dir / f"{args.date}.json"
    with out_json.open("w") as f:
        json.dump(report, f, indent=2)

    # Markdown summary
    lines = [
        f"# Shelf daily report — {args.date} (UTC column `generated_at` in JSON)",
        "",
        "## Prometheus / Mimir",
    ]
    for k, v in report.get("prometheus", {}).items():
        if k.startswith("_"):
            lines.append(f"- **{k[1:]}**: {v}")
        else:
            lines.append(f"- **{k}**: {json.dumps(v)}")
    lines.extend(["", "## Trino"])
    for k, v in report.get("trino", {}).items():
        lines.append(f"- **{k}**: {v}")

    if prev_file.is_file():
        lines.extend(["", "## Delta vs previous day", f"- Previous file: `{prev_file.name}` (diff not automated — compare JSON)"])

    md_path = out_dir / f"{args.date}.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {out_json} and {md_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
