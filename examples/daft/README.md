# Daft + Shelf — 5-minute example

Daft is a Rust/Python dataframe engine with its own native S3 client.
Shelf's S3-compatibility shim is signature-agnostic, so any client
that speaks plain `GetObject` / `HeadObject` to an `endpoint_url` can
read through Shelf — no plugin, no patched filesystem, no JVM. This
example wires Daft up to Shelf in front of an Iceberg table on MinIO
and runs the same query twice to show the warm-cache speedup.

## Topology

```
runner (daft + pyiceberg)
   │
   ├── daft.read_iceberg(...).io_config = http://shelfd:9092  ──┐
   │                                                            │
   │                                                            ▼
   │                                                          shelfd
   │                                                            │
   │   pyiceberg load_table  ──▶  iceberg-rest:8181             ▼
   └────────────────────────────────────────────────────────▶  minio
```

PyIceberg only loads the table metadata via the REST catalog. All
data-file reads (manifests, Parquet footers, row groups) flow through
Daft's S3 client → shelfd:9092 → MinIO.

## What you need

- Docker + Docker Compose v2.
- ~3 GB of disk for the shelfd build cache and image layers.
- About 5 minutes the first time (Rust release build + pip install).
  Re-runs are seconds.

## Run it

```bash
cd shelf/examples/daft
bash run.sh
```

That will:

1. Validate `docker-compose.yml` (`docker compose config`).
2. Build the `shelfd` and `runner` images.
3. Start MinIO, the Iceberg REST catalog, and shelfd.
4. Seed `default.orders` (~50k rows of synthetic order data).
5. Run `bench.py` — a `WHERE status='O' GROUP BY region SUM(amount), COUNT(*)`
   plan — twice. Run #1 is cold (every byte misses); run #2 is warm
   (everything served from Shelf's Foyer cache).

You should see something like:

```
=== Daft + Shelf example results ===
run    elapsed (s)    shelf hits   shelf misses
cold   0.812          0            14
warm   0.087          14           0

warm/cold speedup: 9.33x
```

Numbers will differ on your machine; what should hold is `warm` <
`cold` and `shelf hits` going up on run #2.

## Poke at it

While the stack is up:

```bash
# Cache-pool sizes and pod identity
curl -s http://127.0.0.1:9090/stats | jq

# Hit / miss counters (Prometheus text format)
curl -s http://127.0.0.1:9090/metrics | grep -E 'shelf_(hits|misses)_total'

# MinIO console (login: minioadmin / minioadmin)
open http://127.0.0.1:9101
```

Tear down everything (containers + the MinIO volume):

```bash
docker compose -f docker-compose.yml down -v
```

## How the wiring works

Daft's `S3Config` is documented at
[docs.daft.ai/en/v0.6.6/api/config](https://docs.daft.ai/en/v0.6.6/api/config/);
the live 0.7.5 Rust binary dropped `verify_ssl` /
`check_hostname_ssl` ([Daft#4530](https://github.com/Eventual-Inc/Daft/issues/4530))
when it switched to rustls/AWS-LC, so the working set today is:

```python
io_config = IOConfig(s3=S3Config(
    endpoint_url="http://shelfd:9092",
    key_id="dummy",
    access_key="dummy",
    use_ssl=False,
    force_virtual_addressing=False,   # = path style
    region_name="us-east-1",
))
```

- `force_virtual_addressing=False` is the path-style toggle. The shim
  parses `http://shelfd:9092/<bucket>/<key>` URLs; virtual-host buckets
  would need wildcard DNS we don't have on the docker network.
- `use_ssl=False` because the shim speaks plain HTTP — no TLS to
  verify, so `verify_ssl` / `check_hostname_ssl` aren't needed (and
  passing them raises `not implemented` in 0.7+).
- `key_id` / `access_key` are mandatory in the SDK chain but the shim
  ignores the `Authorization` header by design — any non-empty pair
  works.

`daft.read_iceberg` takes either a metadata-JSON path or a PyIceberg
`Table` object. We use the latter so the catalog handles snapshot
resolution. Daft's signature today is:

```python
daft.read_iceberg(table, snapshot_id=None, io_config=None)
```

(see [docs.daft.ai > read_iceberg](https://docs.daft.ai/en/v0.6.6/api/io/#daft.read_iceberg)).
There is no `catalog=` argument — load the table via PyIceberg first,
then pass the `Table` instance.

## Why dummy creds work

Shelf's S3 shim is signature-agnostic on purpose: it never validates
SigV4. That's what makes "drop in front of MinIO/S3" cutovers a
single-line endpoint flip. The shim is in front of Shelf's Foyer
cache (DRAM + optional NVMe) which is content-addressed by the
upstream object's ETag — see ADR-0011 in the main repo.

## Limitations of this demo

- Single shelfd pod, DRAM-only (no NVMe spillover). Production runs
  with `nvme_bytes` set — see `charts/shelf/values.yaml` in the repo
  root.
- The seed table is small (~50k rows, one Parquet file). The warm/cold
  delta is real but smaller than what you'd see on rep-2-scale tables
  where every query touches dozens of manifests + hundreds of row
  groups.
- Membership resolver is disabled (`membership.enabled: false`) — there
  is no Kubernetes headless DNS in compose. In production each pod's
  resolver builds the HRW ring from `/stats` probes of its peers.

## Files

```
examples/daft/
├── README.md
├── run.sh                  orchestrator
├── docker-compose.yml      minio + iceberg-rest + shelfd + runner
├── Dockerfile.shelfd       multi-stage rust build, mirrors benchmarks/smoke
├── Dockerfile.runner       python:3.11-slim + daft + pyiceberg[s3fs]
├── config/
│   └── shelfd.yaml         single-pod, DRAM-only, S3 shim on :9092
├── init/
│   └── seed.py             PyIceberg seed of default.orders
└── bench.py                cold + warm Daft query, scrapes /metrics
```
