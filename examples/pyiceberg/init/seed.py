"""Seed a small partitioned Iceberg table for the bench.

Important detail: the *seed* path connects directly to MinIO, not to
shelfd. Shelf is only fronting the read path in this example, so writes
land on MinIO via the REST catalog (which itself uses S3FileIO with
path-style access).

Layout:
    namespace : demo
    table     : demo.events
    partition : days(date)
    rows      : ~70 000 across 7 dates 2024-01-13 ... 2024-01-19
    columns   : ts (timestamp), date (date), user_id (long),
                event_type (string), payload (string)

Deterministic via ``random.seed(12)``.
"""

from __future__ import annotations

import os
import random
import sys
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa


ENDPOINT = os.environ["AWS_ENDPOINT_URL"]
BUCKET = os.environ.get("WAREHOUSE_BUCKET", "iceberg-warehouse")
WAREHOUSE = f"s3://{BUCKET}/"
REST_URI = os.environ["ICEBERG_REST_URI"]

DATES = [date(2024, 1, 13) + timedelta(days=i) for i in range(7)]
ROWS_PER_DATE = 10_000
EVENT_TYPES = ["page_view", "click", "purchase", "signup", "logout"]


def build_table() -> pa.Table:
    random.seed(12)
    ts: list[datetime] = []
    dt: list[date] = []
    user_id: list[int] = []
    event_type: list[str] = []
    payload: list[str] = []

    for d in DATES:
        midnight = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        for _ in range(ROWS_PER_DATE):
            secs = random.randint(0, 86_399)
            ts.append(midnight + timedelta(seconds=secs))
            dt.append(d)
            user_id.append(random.randint(1, 50_000))
            event_type.append(random.choice(EVENT_TYPES))
            payload.append("p" + "x" * random.randint(8, 64))

    schema = pa.schema(
        [
            ("ts", pa.timestamp("us", tz="UTC")),
            ("date", pa.date32()),
            ("user_id", pa.int64()),
            ("event_type", pa.string()),
            ("payload", pa.string()),
        ]
    )
    return pa.table(
        {
            "ts": pa.array(ts, type=pa.timestamp("us", tz="UTC")),
            "date": pa.array(dt, type=pa.date32()),
            "user_id": pa.array(user_id, type=pa.int64()),
            "event_type": pa.array(event_type, type=pa.string()),
            "payload": pa.array(payload, type=pa.string()),
        },
        schema=schema,
    )


def main() -> int:
    from pyiceberg.catalog import load_catalog

    print(f"[seed] warehouse={WAREHOUSE} endpoint={ENDPOINT} rest={REST_URI}", flush=True)

    # The seed catalog points at MinIO directly so the writer commits manifests
    # straight to the origin. The bench will load a *separate* catalog handle
    # whose `s3.endpoint` is shelfd.
    catalog = load_catalog(
        "demo",
        **{
            "type": "rest",
            "uri": REST_URI,
            "warehouse": WAREHOUSE,
            "s3.endpoint": ENDPOINT,
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
            # PyIceberg's S3 property is `s3.force-virtual-addressing`, not
            # `s3.path-style-access` (a Java-Iceberg name that PyIceberg silently
            # ignores). Default is False; we set it explicitly so the intent is
            # readable and survives any future default flip.
            "s3.force-virtual-addressing": "false",
        },
    )

    try:
        catalog.create_namespace("demo")
        print("[seed] created namespace demo", flush=True)
    except Exception as exc:
        print(f"[seed] namespace demo already exists: {exc}", flush=True)

    table = build_table()
    print(f"[seed] built arrow table: rows={table.num_rows} bytes={table.nbytes}", flush=True)

    name = "demo.events"
    try:
        catalog.drop_table(name)
        print(f"[seed]   dropped existing {name}", flush=True)
    except Exception:
        pass

    # Unpartitioned: keeps the seed simple. Iceberg's predicate pushdown still
    # uses Parquet column-min/max stats from the manifest, so `date = '...'`
    # still avoids scanning unrelated row groups — and the cold/warm cache
    # signal we want is dominated by manifest + footer + first-row-group reads,
    # not partition pruning.
    iceberg_tbl = catalog.create_table(name, schema=table.schema)

    # Append in 7 chunks (one per date) so we get 7 Parquet files instead of 1
    # giant blob. This makes the row-group skipping observable in cold/warm
    # numbers without any partitioning machinery.
    for d in DATES:
        mask = pa.compute.equal(table["date"], pa.scalar(d, type=pa.date32()))
        chunk = table.filter(mask)
        iceberg_tbl.append(chunk)
        print(f"[seed]   appended date={d.isoformat()} rows={chunk.num_rows}", flush=True)
    print(f"[seed] wrote {name}: {table.num_rows} rows across {len(DATES)} files", flush=True)
    print("[seed] done", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
