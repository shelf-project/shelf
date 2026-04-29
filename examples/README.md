# Shelf — engine examples

The same Shelf binary in front of seven different OSS query engines,
each in a self-contained Docker Compose stack you can run end-to-end
in about five minutes. Every example boots `shelfd` from this
checkout, seeds an Apache Iceberg table on MinIO, points the engine
at `shelfd:9092` (Shelf's signature-agnostic S3 read shim), and runs
the same query twice — cold (cache empty, every byte fetched from
MinIO) and warm (every byte served from Foyer DRAM/NVMe). The shape
is identical across engines because the shim is — by design — an
engine-agnostic drop-in for `s3.endpoint`.

## The seven examples

| Example | What it shows | Cold → warm (Wave 1) | Engine-specific knob |
| --- | --- | --- | --- |
| [`spark/`](spark/) | Apache Spark 3.5 + Iceberg `S3FileIO` + Hadoop S3A, both wired to shelfd. | ~2.1s → ~410 ms (3.6–5.2×) | `spark.sql.defaultCatalog=lake` (avoid `tabulario/spark-iceberg`'s baked-in default catalog pointing at `rest:8181`). |
| [`duckdb/`](duckdb/) | DuckDB 1.3 `httpfs` + `iceberg` extension via a `CREATE SECRET` block. | ~790 ms → ~125 ms (6.4×) | `SET enable_external_file_cache = false;` so the bench measures shelfd's cache, not DuckDB's local one. |
| [`polars/`](polars/) | Polars `pl.scan_iceberg` with PyIceberg's `PyArrowFileIO` doing the I/O. | ~1.2s → ~250 ms (4.88×) | `reader_override="pyiceberg"` — the default `native` reader uses `object_store` with a different (translated) key scheme. |
| [`daft/`](daft/) | Daft 0.7 `read_iceberg` with its native Rust S3 client (rustls/AWS-LC). | ~810 ms → ~90 ms (8.83×) | `force_virtual_addressing=False`; the SSL toggles `verify_ssl` / `check_hostname_ssl` were dropped in 0.7+ and now raise `not implemented`. |
| [`clickhouse/`](clickhouse/) | ClickHouse 24.12 `iceberg('http://shelfd:9092/...')` table function. | ~610 ms → ~390 ms (1.56×) | `config.d/00-shelf-example.xml` disables ClickHouse's Iceberg metadata cache so the warm pass actually exercises shelfd. |
| [`starrocks/`](starrocks/) | StarRocks 3.2 external Iceberg catalog through the StarRocks AWS S3 client. | ~4.2s → ~1.2s (pending live validation — see `starrocks/VALIDATION_NOTES.md`) | `client.factory=com.starrocks.connector.iceberg.IcebergAwsClientFactory` — without it, the dummy `aws.s3.access_key`/`secret_key` are silently ignored and StarRocks falls back to the default credentials chain. |
| [`pyiceberg/`](pyiceberg/) | PyIceberg 0.7 reading via PyArrow's `S3FileSystem`, the Iceberg spec author's own client. | ~57 ms → ~17 ms (3.39×) | `s3.force-virtual-addressing=false` — **not** `s3.path-style-access`; PyIceberg silently ignores the Java-Iceberg name. |

The cold/warm numbers come from the per-example READMEs and
`VALIDATION_NOTES.md` runs on a single Apple-Silicon dev box,
2026-04-28. Absolute timings will drift on different hardware; the
warm-faster-than-cold shape is what the
[`.github/workflows/multi-engine.yml`](../.github/workflows/multi-engine.yml)
CI gate guards on every PR.

## The four config keys, by engine

Every engine routes through Shelf via the same four S3-client
properties: an endpoint URL, a placeholder access-key id, a
placeholder secret, and a path-style-addressing toggle. The shim is
signature-agnostic — Shelf never validates SigV4 — so the credentials
are deliberately dummy strings. The exact spelling of the four keys
is what differs.

| Engine | Endpoint URL | Access-key id | Secret access-key | Path-style toggle |
| --- | --- | --- | --- | --- |
| Spark (Iceberg `S3FileIO`) | `spark.sql.catalog.<cat>.s3.endpoint` | `spark.sql.catalog.<cat>.s3.access-key-id` | `spark.sql.catalog.<cat>.s3.secret-access-key` | `spark.sql.catalog.<cat>.s3.path-style-access=true` |
| Spark (Hadoop S3A) | `spark.hadoop.fs.s3a.endpoint` | (use `AnonymousAWSCredentialsProvider`) | (same) | `spark.hadoop.fs.s3a.path.style.access=true` |
| DuckDB (`CREATE SECRET`) | `ENDPOINT '127.0.0.1:9092'` | `KEY_ID 'dummy'` | `SECRET 'dummy'` | `URL_STYLE 'path'` |
| Polars (via PyIceberg `storage_options`) | `s3.endpoint` | `s3.access-key-id` | `s3.secret-access-key` | `s3.force-virtual-addressing=false` *(inverted)* |
| Daft (`S3Config`) | `endpoint_url` | `key_id` | `access_key` | `force_virtual_addressing=False` *(inverted)* |
| ClickHouse (`iceberg(url, key, secret)`) | first positional arg in the `iceberg(...)` call | second positional arg | third positional arg | auto-detected from URL host |
| StarRocks (`CREATE EXTERNAL CATALOG`) | `aws.s3.endpoint` | `aws.s3.access_key` | `aws.s3.secret_key` | `aws.s3.enable_path_style_access=true` |
| PyIceberg (`load_catalog` properties) | `s3.endpoint` | `s3.access-key-id` | `s3.secret-access-key` | `s3.force-virtual-addressing=false` *(inverted)* |

The "inverted" rows are the trap: PyIceberg, Polars (which goes
through PyIceberg), and Daft express path-style as the *negation* of
virtual-host-style addressing. Setting `s3.path-style-access=true`
on a PyIceberg catalog has no effect — PyIceberg ignores the Java
name and falls through to the default, which happens to also be
path-style, so the misconfiguration "works" by accident until
something changes the default. The PyIceberg example sets the
correct key explicitly so the wire shape is unambiguous.

## Caveats by engine

Each example surfaced a sharp edge during validation. None of these
are Shelf bugs — they're engine-side defaults that hide whether the
cache is actually doing work — but every single one would silently
turn the "warm pass" into an engine-local cache hit instead of a
shelfd hit. Worth knowing before you swap an example into your own
stack.

- **Spark — `tabulario/spark-iceberg`'s baked-in default catalog.**
  The image ships a `spark-defaults.conf` with
  `spark.sql.defaultCatalog=demo` pointing at `http://rest:8181`.
  Spark's analyzer eagerly initialises the default catalog on the
  first query, so even an unrelated `lake.demo.events` query
  trips `UnknownHostException: rest` before it gets to the
  `lake` catalog. The bench overrides
  `spark.sql.defaultCatalog=lake`. Drivers you write yourself need
  to do the same.
- **DuckDB — `enable_external_file_cache`.** DuckDB 1.x caches HTTP
  range reads in-process by default. With it on, the warm run
  finishes in ~25 ms but never even calls shelfd — the demo would
  measure DuckDB, not Shelf. The bench sets
  `SET enable_external_file_cache = false;` to keep the read path
  honest.
- **Polars — `reader_override="pyiceberg"`.** Polars'
  `pl.scan_iceberg` has two reader paths: the default `native`
  one (Rust `object_store`) and `pyiceberg` (PyArrow
  `S3FileSystem`). The `native` path translates the
  `storage_options` keys to `object_store`'s scheme internally;
  `pyiceberg` passes them through verbatim. We pin to `pyiceberg`
  so the example is honest about which keys the shim sees on the
  wire.
- **Daft — dropped SSL toggles in 0.7+.** Daft's `S3Config`
  documented `verify_ssl` and `check_hostname_ssl` through 0.6.6;
  the 0.7 binary switched the HTTP stack to rustls/AWS-LC and
  passing those keys now raises `not implemented` ([Eventual-Inc/Daft#4530](https://github.com/Eventual-Inc/Daft/issues/4530)).
  The example uses `use_ssl=False` (the shim is plain HTTP) and
  no SSL toggles.
- **ClickHouse — Iceberg metadata cache.** ClickHouse 24.x caches
  Iceberg metadata files (`metadata.json`, manifest lists,
  manifests) in a JVM-local cache. The warm run would hit that
  cache before reaching shelfd, leaving `shelf_hits_total` flat.
  `config/clickhouse/00-shelf-example.xml` disables the metadata
  cache so the demo actually exercises Shelf on warm reads.
- **PyIceberg — naming gotcha on path-style.** PyIceberg uses
  `s3.force-virtual-addressing` (note: the *inverse* of Java
  Iceberg's `s3.path-style-access`). PyIceberg *silently ignores*
  `s3.path-style-access` — verified against
  [`pyiceberg/io/pyarrow.py:_initialize_s3_fs`](https://github.com/apache/iceberg-python/blob/main/pyiceberg/io/pyarrow.py).
  The default is already path-style, so the misnamed property
  appears to work; the example sets the correct one.
- **StarRocks — 5 GiB FE meta-dir check.** StarRocks ≥ 3.0 refuses
  to start when the meta-dir filesystem has less than 5 GiB free
  ([StarRocks/starrocks#34813](https://github.com/StarRocks/starrocks/pull/34813)),
  and the `allin1-ubuntu:3.2.16` image is itself ~5 GiB compressed.
  On laptops or shared CI runners with sister-stack volumes parked,
  FE will fail to boot before the example gets to a query —
  `examples/starrocks/VALIDATION_NOTES.md` documents this. The
  CI workflow runs the
  [`jlumbroso/free-disk-space`](https://github.com/jlumbroso/free-disk-space)
  step before each engine to keep this from biting in shared CI.

## Adding a new engine

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the contract every
example follows: required artifacts, the `cold→warm` print
discipline, the shelfd `/metrics` deltas to scrape, and the
`docker compose down -v` tear-down rule the matrix CI relies on.

## CI

Every PR that touches `examples/**`, `shelfd/**`, `charts/shelf/**`,
or [`.github/workflows/multi-engine.yml`](../.github/workflows/multi-engine.yml)
runs all seven examples in parallel matrix slots
(`fail-fast: false`). Manual runs are available via the
**workflow_dispatch** button on the workflow page.
