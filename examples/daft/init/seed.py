"""Seed a tiny Iceberg table for the Daft example.

Writes `default.orders` into MinIO via the Iceberg REST catalog. The
schema is intentionally simple — one filterable string column
(`status`), one groupable string column (`region`), and one numeric
column (`amount`) — so `bench.py` can run a representative
`where → groupby → agg` plan.

Deterministic via `random.seed(12)`. Idempotent: drops the table if it
already exists before recreating.
"""

from __future__ import annotations

import os
import random
import sys
from datetime import date, timedelta

import pyarrow as pa


ENDPOINT = os.environ["AWS_ENDPOINT_URL"]
BUCKET = os.environ.get("WAREHOUSE_BUCKET", "iceberg-warehouse")
WAREHOUSE = f"s3://{BUCKET}/"
REST_URI = os.environ["ICEBERG_REST_URI"]
N_ROWS = int(os.environ.get("SEED_ROWS", "50000"))

REGIONS = ["AMERICAS", "EMEA", "APAC", "MEA"]
STATUSES = ["O", "F", "P"]


def build_orders_table(n_rows: int) -> pa.Table:
    random.seed(12)
    order_ids = list(range(1, n_rows + 1))
    cust_ids = [random.randint(1, 5_000) for _ in range(n_rows)]
    regions = [random.choice(REGIONS) for _ in range(n_rows)]
    statuses = [random.choice(STATUSES) for _ in range(n_rows)]
    amounts = [round(random.uniform(10.0, 9_999.99), 2) for _ in range(n_rows)]
    base = date(2024, 1, 1)
    order_dates = [
        (base + timedelta(days=random.randint(0, 720))).isoformat()
        for _ in range(n_rows)
    ]

    schema = pa.schema(
        [
            ("order_id", pa.int64()),
            ("cust_id", pa.int64()),
            ("region", pa.string()),
            ("status", pa.string()),
            ("amount", pa.float64()),
            ("order_date", pa.string()),
        ]
    )
    return pa.table(
        {
            "order_id": pa.array(order_ids, type=pa.int64()),
            "cust_id": pa.array(cust_ids, type=pa.int64()),
            "region": pa.array(regions, type=pa.string()),
            "status": pa.array(statuses, type=pa.string()),
            "amount": pa.array(amounts, type=pa.float64()),
            "order_date": pa.array(order_dates, type=pa.string()),
        },
        schema=schema,
    )


def main() -> int:
    from pyiceberg.catalog import load_catalog

    print(
        f"[seed] warehouse={WAREHOUSE} endpoint={ENDPOINT} rest={REST_URI} "
        f"rows={N_ROWS}",
        flush=True,
    )

    # Seed talks to MinIO directly (path-style) — shelfd is not on the
    # write path. The bench reads through shelfd to exercise the cache.
    catalog = load_catalog(
        "daft_example",
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
        catalog.create_namespace("default")
        print("[seed] created namespace default", flush=True)
    except Exception as exc:
        print(f"[seed] namespace default already exists: {exc}", flush=True)

    table_name = "default.orders"
    arrow_table = build_orders_table(N_ROWS)
    print(f"[seed] writing {table_name} ({arrow_table.num_rows} rows)", flush=True)

    try:
        catalog.drop_table(table_name)
        print(f"[seed]   dropped existing {table_name}", flush=True)
    except Exception:
        pass

    t = catalog.create_table(table_name, schema=arrow_table.schema)
    t.append(arrow_table)

    print("[seed] done", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
