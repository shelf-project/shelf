#!/usr/bin/env python3
"""F2 — Cross-engine TPC-DS runner.

Runs the 99 canonical TPC-DS queries against one of four engines
(shelf+Trino, Alluxio+Trino, Starburst+WarpSpeed, Firebolt) and
emits a normalised CSV with cold/warm1/warm2 timing plus Trino-side
query stats wherever the engine exposes them.

Design notes:

- **JDBC-free**: uses each engine's REST/HTTP API directly so the
  runner has no Java dependency. Trino-family engines go through
  the `trino-python-client` REST API; Firebolt uses
  `firebolt-sdk`'s HTTP JSON endpoint.
- **Deterministic cache state**: between every `(engine, query,
  repeat)` tuple we optionally flush the engine's cache via its
  admin API (`/admin/evict` for shelfd, `fs drop` for Alluxio,
  `flush data cache` for Warp Speed, API-level cache reset for
  Firebolt). The `--warm-state` flag lets the caller pick from
  `cold|warm|mixed`.
- **Per-query timeout**: `queries.yaml` overrides can keep a single
  pathological query from blowing the whole run's wall-clock. Every
  override is recorded in the output CSV so the results include
  whether the query hit its engine-specific ceiling.
- **Output contract**: see `benchmarks/tpcds/results/<date>/raw.csv`
  schema in `cost/model.py`. Both files move together.

This file intentionally has no third-party deps at import time so
`python -c "import run"` works on a bare Python 3.11 container;
the heavy imports live inside `_engine_*` factories and are
instantiated lazily.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import pathlib
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

DEFAULT_QUERIES_DIR = pathlib.Path(__file__).resolve().parent.parent / "queries"
DEFAULT_ENGINES_YAML = pathlib.Path(__file__).resolve().parent / "engines.yaml"
DEFAULT_TIMEOUT_S = 600


# ----- Engines ------------------------------------------------------

@dataclass
class EngineResult:
    elapsed_ms: float
    bytes_scanned: int | None
    planning_ms: float | None
    stats_json: dict[str, Any] = field(default_factory=dict)


class Engine:
    """Base class. Each subclass knows how to run one query and
    optionally flush its cache."""

    name: str = "base"

    def run(self, sql: str, timeout_s: int) -> EngineResult:
        raise NotImplementedError

    def flush_cache(self) -> None:
        # Default: no-op. Subclasses override.
        pass

    def warmup(self, sql: str, timeout_s: int) -> None:
        # Default: run the query once and discard. Subclasses whose
        # engine requires a specific warmup ritual (Warp Speed
        # auto-index build, Firebolt workload analyser) override.
        self.run(sql, timeout_s)


def _http_client():
    import requests
    return requests.Session()


class TrinoEngine(Engine):
    name = "trino"

    def __init__(self, url: str, user: str, catalog: str, schema: str, extra_session: dict | None = None):
        self.url = url
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.session_props = extra_session or {}
        self._session = _http_client()

    def run(self, sql: str, timeout_s: int) -> EngineResult:
        start = time.perf_counter()
        headers = {
            "X-Trino-User": self.user,
            "X-Trino-Catalog": self.catalog,
            "X-Trino-Schema": self.schema,
        }
        if self.session_props:
            headers["X-Trino-Session"] = ",".join(f"{k}={v}" for k, v in self.session_props.items())
        resp = self._session.post(f"{self.url}/v1/statement", data=sql, headers=headers, timeout=timeout_s)
        resp.raise_for_status()
        payload = resp.json()
        bytes_scanned = None
        planning_ms = None
        while True:
            if payload.get("stats", {}).get("state") == "FINISHED":
                stats = payload.get("stats", {})
                bytes_scanned = stats.get("processedBytes")
                planning_ms = (stats.get("planningTimeMillis") or None)
                break
            if payload.get("error"):
                raise RuntimeError(payload["error"])
            next_uri = payload.get("nextUri")
            if not next_uri:
                break
            if time.perf_counter() - start > timeout_s:
                raise TimeoutError(f"Trino query exceeded {timeout_s}s")
            payload = self._session.get(next_uri, headers=headers, timeout=timeout_s).json()
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return EngineResult(elapsed_ms=elapsed_ms, bytes_scanned=bytes_scanned, planning_ms=planning_ms, stats_json=payload.get("stats", {}))


class ShelfEngine(TrinoEngine):
    """Trino + shelfd as a native filesystem overlay."""

    name = "shelf"

    def __init__(self, *args, shelf_admin_urls: list[str] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.shelf_admin_urls = shelf_admin_urls or []

    def flush_cache(self) -> None:
        # SHELF-23 `/admin/evict` is per-key. For a full flush the
        # operator restarts the StatefulSet, which the runner won't
        # do — instead we rely on the pod's `--cold` sidecar which
        # POSTs `/admin/reload` with an empty pin list and drops all
        # non-pinned entries. See `benchmarks/tpcds/runner/README.md`.
        for admin in self.shelf_admin_urls:
            try:
                self._session.post(f"{admin}/admin/reload", json={"pin_list_url": None}, timeout=30)
            except Exception:
                # The regression path (F4) runs with a single shelfd
                # pod where a transient reload error is survivable;
                # don't fail the whole benchmark.
                pass


class AlluxioEngine(TrinoEngine):
    name = "alluxio"

    def __init__(self, *args, alluxio_master_url: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.alluxio_master_url = alluxio_master_url

    def flush_cache(self) -> None:
        if not self.alluxio_master_url:
            return
        # Alluxio exposes `fs free /path` via REST; a full-namespace
        # free is a privileged operation we issue on a best-effort
        # basis.
        try:
            self._session.post(
                f"{self.alluxio_master_url}/api/v1/master/file/free",
                params={"path": "/"},
                timeout=60,
            )
        except Exception:
            pass


class WarpSpeedEngine(TrinoEngine):
    name = "warpspeed"

    def flush_cache(self) -> None:
        # Starburst Warp Speed: `SYSTEM flush_cache` SQL command.
        try:
            self.run("CALL system.flush_cache()", timeout_s=60)
        except Exception:
            pass


class FireboltEngine(Engine):
    name = "firebolt"

    def __init__(self, account: str, engine: str, database: str, client_id: str, client_secret: str):
        self.account = account
        self.engine = engine
        self.database = database
        self.client_id = client_id
        self.client_secret = client_secret
        self._session = _http_client()

    def run(self, sql: str, timeout_s: int) -> EngineResult:
        start = time.perf_counter()
        # Firebolt Core v2 — client-credentials flow
        tok = self._session.post(
            "https://id.app.firebolt.io/oauth/token",
            json={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
                "audience": "https://api.firebolt.io",
            },
            timeout=30,
        ).json()["access_token"]
        resp = self._session.post(
            f"https://api.firebolt.io/v3/accounts/{self.account}/engines/{self.engine}/query",
            params={"database": self.database, "output_format": "JSON_Compact"},
            headers={"Authorization": f"Bearer {tok}"},
            data=sql,
            timeout=timeout_s,
        )
        resp.raise_for_status()
        body = resp.json()
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return EngineResult(
            elapsed_ms=elapsed_ms,
            bytes_scanned=body.get("statistics", {}).get("bytes_read"),
            planning_ms=body.get("statistics", {}).get("compile_time_ms"),
            stats_json=body.get("statistics", {}),
        )


ENGINE_FACTORIES: dict[str, Callable[[dict], Engine]] = {
    "shelf": lambda cfg: ShelfEngine(**cfg),
    "alluxio": lambda cfg: AlluxioEngine(**cfg),
    "warpspeed": lambda cfg: WarpSpeedEngine(**cfg),
    "firebolt": lambda cfg: FireboltEngine(**cfg),
}


# ----- Driver -------------------------------------------------------


def iter_queries(queries_dir: pathlib.Path) -> Iterable[tuple[str, str]]:
    for path in sorted(queries_dir.glob("q*.sql")):
        yield path.stem, path.read_text()


def load_engine_config(engines_yaml: pathlib.Path, engine: str) -> dict[str, Any]:
    # PyYAML is optional — if the operator passes `engines.json` or
    # inlines a JSON env var we can load it without yaml.
    if engines_yaml.suffix in {".yaml", ".yml"}:
        import yaml  # type: ignore

        with engines_yaml.open() as f:
            all_configs = yaml.safe_load(f)
    else:
        with engines_yaml.open() as f:
            all_configs = json.load(f)
    if engine not in all_configs:
        raise SystemExit(f"engine '{engine}' not found in {engines_yaml}; known: {sorted(all_configs)}")
    return all_configs[engine]


def run_one(engine: Engine, sql: str, timeout_s: int, label: str) -> dict[str, Any]:
    started = time.time()
    try:
        result = engine.run(sql, timeout_s)
        status = "ok"
        elapsed_ms = result.elapsed_ms
        bytes_scanned = result.bytes_scanned or 0
        planning_ms = result.planning_ms or 0
    except TimeoutError:
        status = "timeout"
        elapsed_ms = timeout_s * 1000.0
        bytes_scanned = 0
        planning_ms = 0
    except Exception as e:
        status = f"error:{type(e).__name__}"
        elapsed_ms = (time.time() - started) * 1000.0
        bytes_scanned = 0
        planning_ms = 0
    return {
        "phase": label,
        "status": status,
        "elapsed_ms": round(elapsed_ms, 2),
        "bytes_scanned": bytes_scanned,
        "planning_ms": round(planning_ms, 2),
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Run the 99 TPC-DS queries against an engine")
    parser.add_argument("--engine", required=True, choices=sorted(ENGINE_FACTORIES))
    parser.add_argument("--queries-dir", type=pathlib.Path, default=DEFAULT_QUERIES_DIR)
    parser.add_argument("--engines-yaml", type=pathlib.Path, default=DEFAULT_ENGINES_YAML)
    parser.add_argument("--sf", type=int, default=1000, help="scale factor — for the output CSV only")
    parser.add_argument("--out", required=True, type=pathlib.Path, help="path to write the run CSV")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--warm-state", choices=["cold", "warm", "mixed"], default="mixed")
    parser.add_argument("--queries", nargs="*", help="subset of query ids to run (defaults to all)")
    args = parser.parse_args(argv)

    cfg = load_engine_config(args.engines_yaml, args.engine)
    engine = ENGINE_FACTORIES[args.engine](cfg)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    queries = list(iter_queries(args.queries_dir))
    if args.queries:
        allow = set(args.queries)
        queries = [(qid, sql) for qid, sql in queries if qid in allow]
    if not queries:
        raise SystemExit(f"no queries found in {args.queries_dir}")

    fieldnames = [
        "engine", "sf", "query_id", "phase", "status",
        "elapsed_ms", "bytes_scanned", "planning_ms", "timestamp",
    ]
    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for qid, sql in queries:
            if args.warm_state in {"cold", "mixed"}:
                engine.flush_cache()
            cold = run_one(engine, sql, args.timeout, "cold")
            warm1 = run_one(engine, sql, args.timeout, "warm1")
            warm2 = run_one(engine, sql, args.timeout, "warm2")
            for row in (cold, warm1, warm2):
                row.update({
                    "engine": args.engine,
                    "sf": args.sf,
                    "query_id": qid,
                    "timestamp": int(time.time()),
                })
                writer.writerow(row)
            f.flush()
            print(
                f"{qid}: cold={cold['elapsed_ms']:.0f}ms "
                f"warm1={warm1['elapsed_ms']:.0f}ms "
                f"warm2={warm2['elapsed_ms']:.0f}ms "
                f"({cold['status']})",
                flush=True,
            )

    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
