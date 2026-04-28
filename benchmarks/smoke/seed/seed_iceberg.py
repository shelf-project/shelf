"""Seed three tiny Iceberg tables into MinIO via the REST catalog.

Trino reads the same REST catalog via `iceberg.catalog.type=rest`
(see config/trino/etc/catalog/iceberg.properties).

Tables:
  * default.nation       - 25 rows (TPC-H nations)
  * default.region       - 5 rows (TPC-H regions)
  * default.orders_small - 1_000 synthetic rows

Deterministic: `random.seed(12)`.
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

REGIONS = [
    (0, "AFRICA",      "lar deposits. blithely final packages cajole."),
    (1, "AMERICA",     "hs use ironic, even requests. s"),
    (2, "ASIA",        "ges. thinly even pinto beans ca"),
    (3, "EUROPE",      "ly final courts cajole furiously final excuse"),
    (4, "MIDDLE EAST", "uickly special accounts cajole carefully blithely close"),
]

NATIONS = [
    (0,  "ALGERIA",        0, "haggle. carefully final deposits"),
    (1,  "ARGENTINA",      1, "al foxes promise slyly according to"),
    (2,  "BRAZIL",         1, "y alongside of the pending deposits."),
    (3,  "CANADA",         1, "eas hang ironic, silent packages."),
    (4,  "EGYPT",          4, "y above the carefully unusual theodolites"),
    (5,  "ETHIOPIA",       0, "ven packages wake quickly."),
    (6,  "FRANCE",         3, "refully final requests. regular, ironi"),
    (7,  "GERMANY",        3, "l platelets. regular accounts x-ray"),
    (8,  "INDIA",          2, "ss excuses cajole slyly."),
    (9,  "INDONESIA",      2, "slyly express asymptotes."),
    (10, "IRAN",           4, "efully alongside of the slyly final"),
    (11, "IRAQ",           4, "nic deposits boost atop the quickly final"),
    (12, "JAPAN",          2, "ously. final, express gifts"),
    (13, "JORDAN",         4, "ic deposits are blithely about"),
    (14, "KENYA",          0, " pending excuses haggle furiously"),
    (15, "MOROCCO",        0, "rns. blithely bold courts among the"),
    (16, "MOZAMBIQUE",     0, "s. ironic, unusual asymptotes wake"),
    (17, "PERU",           1, "platelets. blithely pending depend"),
    (18, "CHINA",          2, "c dependencies. furiously express"),
    (19, "ROMANIA",        3, "ular asymptotes are about the furious"),
    (20, "SAUDI ARABIA",   4, "ts. silent requests haggle."),
    (21, "VIETNAM",        2, "hely enticingly express accounts."),
    (22, "RUSSIA",         3, " requests against the platelets use"),
    (23, "UNITED KINGDOM", 3, "eans boost carefully special requests."),
    (24, "UNITED STATES",  1, "y final packages. slow foxes cajole quickly."),
]

def build_region_table() -> pa.Table:
    cols = list(zip(*REGIONS))
    schema = pa.schema([("r_regionkey", pa.int32()), ("r_name", pa.string()), ("r_comment", pa.string())])
    return pa.Table.from_arrays([pa.array(c, type=f) for c, f in zip(cols, schema.types)], schema=schema)

def build_nation_table() -> pa.Table:
    cols = list(zip(*NATIONS))
    schema = pa.schema([("n_nationkey", pa.int32()), ("n_name", pa.string()), ("n_regionkey", pa.int32()), ("n_comment", pa.string())])
    return pa.Table.from_arrays([pa.array(c, type=f) for c, f in zip(cols, schema.types)], schema=schema)

def build_orders_table(n_rows: int = 1000) -> pa.Table:
    random.seed(12)
    statuses = ["O", "F", "P"]
    orderkeys = list(range(1, n_rows + 1))
    custkeys  = [random.randint(1, 100) for _ in range(n_rows)]
    totprices = [round(random.uniform(900, 500_000), 2) for _ in range(n_rows)]
    orderdates = [(date(1996, 1, 1) + timedelta(days=random.randint(0, 2000))).isoformat() for _ in range(n_rows)]
    stats      = [random.choice(statuses) for _ in range(n_rows)]
    schema = pa.schema([
        ("o_orderkey",    pa.int64()),
        ("o_custkey",     pa.int64()),
        ("o_orderstatus", pa.string()),
        ("o_totalprice",  pa.float64()),
        ("o_orderdate",   pa.string()),
    ])
    return pa.table({
        "o_orderkey":    pa.array(orderkeys,  type=pa.int64()),
        "o_custkey":     pa.array(custkeys,   type=pa.int64()),
        "o_orderstatus": pa.array(stats,      type=pa.string()),
        "o_totalprice":  pa.array(totprices,  type=pa.float64()),
        "o_orderdate":   pa.array(orderdates, type=pa.string()),
    }, schema=schema)

def main() -> int:
    from pyiceberg.catalog import load_catalog

    print(f"[seed] warehouse={WAREHOUSE} endpoint={ENDPOINT} rest={REST_URI}", flush=True)

    catalog = load_catalog(
        "smoke",
        **{
            "type":                 "rest",
            "uri":                  REST_URI,
            "warehouse":            WAREHOUSE,
            "s3.endpoint":          ENDPOINT,
            "s3.access-key-id":     os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.region":            os.environ.get("AWS_REGION", "us-east-1"),
            "s3.path-style-access": "true",
        },
    )

    try:
        catalog.create_namespace("default")
        print("[seed] created namespace default", flush=True)
    except Exception as exc:
        print(f"[seed] namespace default already exists: {exc}", flush=True)

    specs = [
        ("default.region",       build_region_table()),
        ("default.nation",       build_nation_table()),
        ("default.orders_small", build_orders_table(1000)),
    ]

    for name, table in specs:
        print(f"[seed] writing {name} ({table.num_rows} rows)", flush=True)
        try:
            catalog.drop_table(name)
            print(f"[seed]   dropped existing {name}", flush=True)
        except Exception:
            pass
        t = catalog.create_table(name, schema=table.schema)
        t.append(table)

    print("[seed] done", flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())
