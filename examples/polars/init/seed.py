"""Seed a small Iceberg table into MinIO via PyIceberg.

Creates `demo.events` at `s3://warehouse/demo/events`, writes ~200k
rows of synthetic event data, and saves the resulting metadata.json
location to `/shared/metadata_path.txt` so `bench.py` can scan the
table without needing access to the SqlCatalog used here.

The seeder talks to MinIO directly (not through shelfd) on purpose:
the cold-vs-warm comparison in `bench.py` is the only side that
should exercise the cache.
"""
from __future__ import annotations

import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog


ENDPOINT = os.environ["MINIO_ENDPOINT"]
BUCKET = os.environ.get("WAREHOUSE_BUCKET", "warehouse")
SHARED = Path(os.environ.get("SHARED_DIR", "/shared"))
WAREHOUSE = f"s3://{BUCKET}/"
N_ROWS = int(os.environ.get("SEED_ROWS", "200000"))

S3_OPTS = {
    "s3.endpoint": ENDPOINT,
    "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
    "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
    "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
}

EVENT_TYPES = ["click", "view", "purchase", "signup", "logout"]
COUNTRIES = ["IN", "US", "GB", "DE", "BR", "JP", "FR", "AU"]


def build_events(n: int) -> pa.Table:
    """~200k rows is small enough to seed in seconds yet big enough
    to write a few row groups, so the cold→warm delta is observable."""
    random.seed(42)
    base = datetime(2026, 1, 1)
    user_ids = [random.randint(1, 5_000) for _ in range(n)]
    types = [random.choice(EVENT_TYPES) for _ in range(n)]
    countries = [random.choice(COUNTRIES) for _ in range(n)]
    amounts = [round(random.uniform(0, 999), 2) for _ in range(n)]
    ts = [base + timedelta(seconds=random.randint(0, 60 * 60 * 24 * 90)) for _ in range(n)]
    return pa.table(
        {
            "user_id":    pa.array(user_ids,  type=pa.int64()),
            "event_type": pa.array(types,     type=pa.string()),
            "country":    pa.array(countries, type=pa.string()),
            "amount":     pa.array(amounts,   type=pa.float64()),
            "ts":         pa.array(ts,        type=pa.timestamp("us")),
        }
    )


def main() -> int:
    SHARED.mkdir(parents=True, exist_ok=True)
    print(f"[seed] warehouse={WAREHOUSE} endpoint={ENDPOINT}", flush=True)

    catalog = SqlCatalog(
        "demo",
        **{
            "uri": f"sqlite:///{SHARED}/catalog.db",
            "warehouse": WAREHOUSE,
            **S3_OPTS,
        },
    )

    try:
        catalog.create_namespace("demo")
        print("[seed] created namespace demo", flush=True)
    except Exception as exc:
        print(f"[seed] namespace demo already exists: {exc}", flush=True)

    name = "demo.events"
    try:
        catalog.drop_table(name)
        print(f"[seed] dropped existing {name}", flush=True)
    except Exception:
        pass

    print(f"[seed] generating {N_ROWS} rows...", flush=True)
    arrow = build_events(N_ROWS)
    print(f"[seed] schema: {arrow.schema}", flush=True)

    table = catalog.create_table(name, schema=arrow.schema)
    table.append(arrow)
    table.refresh()

    metadata_path = table.metadata_location
    out = SHARED / "metadata_path.txt"
    out.write_text(metadata_path + "\n")
    print(f"[seed] metadata: {metadata_path}", flush=True)
    print(f"[seed] wrote {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
