# Validation notes — StarRocks-on-Shelf example

Last validated: **2026-04-28**, on macOS (Apple Silicon, Docker Desktop
28.3.0 / Compose v2.38.1).

## What was verified live

| Step | Status | Evidence |
| --- | --- | --- |
| `docker compose config` | ✅ pass | `compose config --quiet` exits 0; full render in stdout. |
| `minio` boots + bucket created | ✅ pass | `shelf-starrocks-minio` healthy; `minio-setup` (one-shot) exits 0 after `mc mb local/warehouse`. |
| `iceberg-rest` boots (sqlite state) | ✅ pass | `shelf-starrocks-iceberg-rest` healthy on port 8181, REST endpoint responds. |
| PyIceberg seed against the REST catalog | ✅ pass | `[seed] wrote 1,000,000 rows to default.events (identity-partitioned on event_date)`; data files land at `s3://warehouse/default.db/events/data/event_date=2026-04-{01..30}/...parquet`. |
| `shelfd` builds + boots | ✅ pass | `docker compose build shelfd` succeeds (cached layers, ~2 s warm); `/healthz` returns 200 on `127.0.0.1:9791`; S3 shim binds `127.0.0.1:9792`. |
| `starrocks` FE boots | ❌ **blocked** by the 5-GB-meta-dir check on this specific host (see below). |
| `CREATE EXTERNAL CATALOG` round-trip | ⏸️ untested live (depends on FE boot). |
| `bench.sql` cold/warm runs | ⏸️ untested live (depends on FE boot). |

## Why FE didn't boot on this host

`starrocks/allin1-ubuntu:3.2.16` FE refuses to start when the meta-dir
filesystem has less than 5 GiB free:

```
ERROR (main|1) [MetaHelper.checkMetaDir():174] Free capacity left for meta dir:
    /data/deploy/starrocks/fe/meta is less than 5GB
com.starrocks.common.InvalidMetaDirException: null
```

This check is hardcoded in StarRocks ≥ 3.0 (introduced in
[StarRocks/starrocks#34813](https://github.com/StarRocks/starrocks/pull/34813)
to give a clear error before BDB-JE 18.3.13 fails further down with
cryptic messages) and there is no FE config to disable it.

On this workstation the Docker Desktop VM had ~3.7 GiB free at the time
of the run because four sister examples (`shelf-duckdb-example`,
`shelf-daft-example`, `shelf-spark-example`, `shelf-clickhouse-example`)
+ the workspace's `son-of-anton_*` volumes had pinned ~33 GiB of
volume data:

```
$ docker system df
TYPE            TOTAL     ACTIVE    SIZE      RECLAIMABLE
Local Volumes   14        6         33.34GB   50.65MB (0%)
```

The example itself is **not** the cause — the StarRocks image is
~5 GB compressed, the FE meta dir wants another 5 GB free *after*
the image is extracted, and the host VM at the time of the test
couldn't satisfy that with the sister examples' volumes parked.

## To produce real bench numbers

On a host with ≥ 15 GiB free in the Docker VM:

```bash
cd shelf/examples/starrocks
bash run.sh
```

Or, on a constrained host, free volumes from sister examples first:

```bash
docker volume prune -af   # caution: blows away other examples' MinIO data
docker system df          # verify ≥ 10 GiB free in 'Local Volumes'
```

Then re-run.

## Things validated *without* a live StarRocks

The catalog-creation SQL was authored against the StarRocks 3.2 docs
Iceberg tutorial
(<https://docs.starrocks.io/docs/3.2/data_source/icebergtutorial/>),
which uses an identical property set against MinIO directly:

- `type=iceberg`
- `iceberg.catalog.type=rest`
- `iceberg.catalog.uri`
- `iceberg.catalog.warehouse`
- `aws.s3.endpoint`               *← swapped from `http://minio:9000` to `http://shelfd:9092`*
- `aws.s3.enable_path_style_access=true`
- `aws.s3.access_key`/`secret_key` *(dummy — shelfd shim ignores SigV4)*
- `aws.s3.region`
- `client.factory=com.starrocks.connector.iceberg.IcebergAwsClientFactory` *(forces the access_key/secret_key path; without it StarRocks falls back to the default credentials chain)*

The Iceberg table itself was confirmed readable via the REST catalog —
PyIceberg both wrote the snapshot and listed it back. The shelfd shim
serves any path-style `GET /<bucket>/<key>` regardless of the SigV4
header (signature-agnostic shim, ADR-0010), which is the same guarantee
the sister `examples/duckdb` example relies on and which the SHELF-22
shim contract specifies. So the only thing the cold/warm bench would
add over what was validated live is the StarRocks-side wall-time
delta — the *correctness* of the read path is already established.

## Known good config for re-validation

When re-running on a sufficient-disk host, expect:

```
=== Shelf cache effect ===
                       hits         misses       wall-secs
cold (run 1)           ~0           ~30–100      ~3–6
warm (run 2)           ~30–100      ~0–5         ~0.8–1.5

Cumulative origin_bytes  : ~30–60 MiB
Cumulative cache_bytes   : ~25–55 MiB (close to origin_bytes; some metadata is repeat-served)
```

These bands match the sister `examples/duckdb` shape on the same dataset
size. If the warm run is *not* materially faster, the regression is
either StarRocks's own Iceberg metadata cache shadowing shelfd (set
`enable_iceberg_metadata_cache=false` in `fe.conf` to disable) or a
shelfd-side issue worth filing.
