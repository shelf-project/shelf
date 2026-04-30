"""Seed a 1 M-row partitioned Iceberg `events` table into MinIO.

Mirrors the schema used by the sibling DuckDB example so the same
warmup/bench SQL works against either engine:

    event_id    BIGINT
    user_id     BIGINT
    event_date  DATE        (partition key, identity transform)
    event_type  STRING
    amount      DOUBLE

Defaults: 1_000_000 rows distributed across `EVENTS_DAYS` (default 30)
calendar days, identity-partitioned on `event_date`. PyIceberg's
writer fans the rows into one Parquet data file per partition value,
which gives Spark exactly one footer + one row group per file × 30
files to walk on the cold run.

Deterministic via random.seed(12).
"""

from __future__ import annotations

import os
import random
import sys
from datetime import date, timedelta

import pyarrow as pa


ENDPOINT = os.environ["AWS_ENDPOINT_URL"]
BUCKET = os.environ.get("WAREHOUSE_BUCKET", "warehouse")
WAREHOUSE = f"s3://{BUCKET}/"
REST_URI = os.environ["ICEBERG_REST_URI"]

ROWS = int(os.environ.get("EVENTS_ROWS", "1000000"))
DAYS = int(os.environ.get("EVENTS_DAYS", "30"))
START_DATE = date(2026, 4, 1)

EVENT_TYPES = ["click", "view", "purchase", "signup", "share"]


def build_events_table(n_rows: int, n_days: int) -> pa.Table:
    """Generate a deterministic synthetic events Arrow table."""
    random.seed(12)

    event_ids = list(range(1, n_rows + 1))
    user_ids = [random.randint(1, 50_000) for _ in range(n_rows)]
    day_offsets = [random.randint(0, n_days - 1) for _ in range(n_rows)]
    event_dates = [START_DATE + timedelta(days=d) for d in day_offsets]
    types = [random.choice(EVENT_TYPES) for _ in range(n_rows)]
    amounts = [round(random.uniform(0.5, 999.99), 2) for _ in range(n_rows)]

    schema = pa.schema(
        [
            ("event_id", pa.int64()),
            ("user_id", pa.int64()),
            ("event_date", pa.date32()),
            ("event_type", pa.string()),
            ("amount", pa.float64()),
        ]
    )
    return pa.table(
        {
            "event_id": pa.array(event_ids, type=pa.int64()),
            "user_id": pa.array(user_ids, type=pa.int64()),
            "event_date": pa.array(event_dates, type=pa.date32()),
            "event_type": pa.array(types, type=pa.string()),
            "amount": pa.array(amounts, type=pa.float64()),
        },
        schema=schema,
    )


def main() -> int:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import (
        DateType,
        DoubleType,
        LongType,
        NestedField,
        StringType,
    )

    print(
        f"[seed] warehouse={WAREHOUSE} endpoint={ENDPOINT} rest={REST_URI} "
        f"rows={ROWS} days={DAYS}",
        flush=True,
    )

    catalog = load_catalog(
        "spark_demo",
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
        catalog.create_namespace("demo")
        print("[seed] created namespace demo", flush=True)
    except Exception as exc:
        print(f"[seed] namespace demo already exists: {exc}", flush=True)

    # `required=False` matches PyArrow's default nullable schema —
    # PyIceberg's writer rejects a "required" Iceberg field if the
    # source Arrow column is nullable, even when no NULLs exist.
    iceberg_schema = Schema(
        NestedField(1, "event_id", LongType(), required=False),
        NestedField(2, "user_id", LongType(), required=False),
        NestedField(3, "event_date", DateType(), required=False),
        NestedField(4, "event_type", StringType(), required=False),
        NestedField(5, "amount", DoubleType(), required=False),
    )
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=3,
            field_id=1000,
            transform=IdentityTransform(),
            name="event_date",
        )
    )

    name = "demo.events"
    try:
        catalog.drop_table(name)
        print(f"[seed] dropped existing {name}", flush=True)
    except Exception:
        pass

    print(f"[seed] generating {ROWS:,} rows across {DAYS} days...", flush=True)
    table = build_events_table(ROWS, DAYS)

    iceberg_table = catalog.create_table(
        name,
        schema=iceberg_schema,
        partition_spec=partition_spec,
    )
    iceberg_table.append(table)

    print(
        f"[seed] wrote {table.num_rows:,} rows to {name} "
        f"(identity-partitioned on event_date)",
        flush=True,
    )
    print("[seed] done", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
