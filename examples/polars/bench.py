"""Polars + Shelf cold/warm benchmark.

Reads the metadata path written by `init/seed.py`, then runs the same
filter → group_by → aggregate lazy chain twice through Shelf's S3
shim. The first run is a cold read (shelfd fetches every Parquet
range from MinIO); the second hits Foyer's DRAM/NVMe cache.

storage_options keys come from the PyIceberg FileIO docs, NOT the
Polars / object_store keys you'd use with `scan_parquet`:

  https://py.iceberg.apache.org/configuration/#fileio

Specifically:

  * s3.endpoint              — http://shelfd:9092 (the SHELF-22 shim)
  * s3.access-key-id         — dummy works; the shim ignores SigV4
  * s3.secret-access-key     — dummy works; same reason
  * s3.region                — required by the AWS SDK
  * s3.force-virtual-addressing — defaults False, which means
                              path-style is used whenever
                              `s3.endpoint` is set. That's exactly
                              what the shim accepts (`/<bucket>/<key>`).

We force `reader_override="pyiceberg"` so PyIceberg's FileIO does
the I/O. The `native` reader path goes through Polars' Rust
object_store binding and uses a different (object_store-style) key
scheme, which would trip people up; pinning to pyiceberg keeps the
example honest about which keys the shim actually sees.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import polars as pl


SHARED = Path(os.environ.get("SHARED_DIR", "/shared"))
SHELFD_ENDPOINT = os.environ["SHELFD_ENDPOINT"]


def storage_options(endpoint: str) -> dict[str, str]:
    return {
        "s3.endpoint":          endpoint,
        "s3.access-key-id":     os.environ["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "s3.region":            os.environ.get("AWS_REGION", "us-east-1"),
    }


def build_query(metadata_path: str, endpoint: str) -> pl.LazyFrame:
    return (
        pl.scan_iceberg(
            metadata_path,
            storage_options=storage_options(endpoint),
            reader_override="pyiceberg",
        )
        .filter(pl.col("amount") > 100)
        .group_by(["country", "event_type"])
        .agg(
            pl.len().alias("events"),
            pl.col("amount").sum().alias("revenue"),
            pl.col("user_id").n_unique().alias("uniques"),
        )
        .sort(["country", "event_type"])
    )


def time_run(lf: pl.LazyFrame) -> tuple[float, int]:
    t0 = time.perf_counter()
    df = lf.collect()
    return time.perf_counter() - t0, df.height


def main() -> int:
    metadata_path = (SHARED / "metadata_path.txt").read_text().strip()
    print(f"[bench] table metadata: {metadata_path}")
    print(f"[bench] reading via shelfd shim: {SHELFD_ENDPOINT}")
    print()

    cold, n = time_run(build_query(metadata_path, SHELFD_ENDPOINT))
    print(f"[bench] cold:  {cold * 1000:8.1f} ms   ({n} groups)")

    warm, n = time_run(build_query(metadata_path, SHELFD_ENDPOINT))
    print(f"[bench] warm:  {warm * 1000:8.1f} ms   ({n} groups)")

    speedup = cold / warm if warm > 0 else float("inf")
    print()
    print(f"shelfd cold→warm speedup: {speedup:.2f}x")
    print(f"summary: cold: {cold:.2f}s | warm: {warm * 1000:.0f}ms")
    return 0


if __name__ == "__main__":
    sys.exit(main())
