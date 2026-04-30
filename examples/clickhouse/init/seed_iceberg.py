"""Seed the demo.events Iceberg table into MinIO.

The table layout matches the bench query in bench.sql:
    SELECT count(*), avg(value)
    FROM iceberg('http://shelfd:9092/warehouse/demo/events', 'dummy', 'dummy')
    WHERE date = '2024-01-15'

Schema:
    id     bigint
    ts     timestamp (UTC)
    date   string         (YYYY-MM-DD, used by the WHERE clause)
    value  double

Volume: ~30k rows spread across 30 days starting 2024-01-01, written as a
single Iceberg V2 snapshot. Deterministic via random.seed(2024).
"""

from __future__ import annotations

import os
import random
import sys
from datetime import datetime, timedelta, timezone

import pyarrow as pa

ENDPOINT = os.environ["AWS_ENDPOINT_URL"]
BUCKET = os.environ.get("WAREHOUSE_BUCKET", "warehouse")
WAREHOUSE = f"s3://{BUCKET}/"
REST_URI = os.environ["ICEBERG_REST_URI"]

NAMESPACE = "demo"
TABLE = f"{NAMESPACE}.events"
ROWS_PER_DAY = 1_000
DAYS = 30
START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)


def build_events_table() -> pa.Table:
    random.seed(2024)
    ids: list[int] = []
    timestamps: list[datetime] = []
    dates: list[str] = []
    values: list[float] = []
    next_id = 1
    for day in range(DAYS):
        day_start = START_DATE + timedelta(days=day)
        date_str = day_start.date().isoformat()
        for _ in range(ROWS_PER_DAY):
            seconds = random.randint(0, 24 * 3600 - 1)
            ts = day_start + timedelta(seconds=seconds)
            ids.append(next_id)
            timestamps.append(ts)
            dates.append(date_str)
            values.append(round(random.uniform(0.0, 1000.0), 4))
            next_id += 1

    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("ts", pa.timestamp("us", tz="UTC")),
            ("date", pa.string()),
            ("value", pa.float64()),
        ]
    )
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "ts": pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
            "date": pa.array(dates, type=pa.string()),
            "value": pa.array(values, type=pa.float64()),
        },
        schema=schema,
    )


def main() -> int:
    from pyiceberg.catalog import load_catalog

    print(
        f"[seed] warehouse={WAREHOUSE} endpoint={ENDPOINT} rest={REST_URI}",
        flush=True,
    )

    catalog = load_catalog(
        "ch_example",
        **{
            "type": "rest",
            "uri": REST_URI,
            "warehouse": WAREHOUSE,
            "s3.endpoint": ENDPOINT,
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
            "s3.path-style-access": "true",
        },
    )

    try:
        catalog.create_namespace(NAMESPACE)
        print(f"[seed] created namespace {NAMESPACE}", flush=True)
    except Exception as exc:
        print(f"[seed] namespace {NAMESPACE} already exists: {exc}", flush=True)

    arrow_table = build_events_table()
    print(
        f"[seed] writing {TABLE} ({arrow_table.num_rows} rows across {DAYS} days)",
        flush=True,
    )

    try:
        catalog.drop_table(TABLE)
        print(f"[seed]   dropped existing {TABLE}", flush=True)
    except Exception:
        pass

    iceberg_table = catalog.create_table(TABLE, schema=arrow_table.schema)
    iceberg_table.append(arrow_table)

    metadata_location = iceberg_table.metadata_location
    print(f"[seed] table metadata at {metadata_location}", flush=True)
    print("[seed] done", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
