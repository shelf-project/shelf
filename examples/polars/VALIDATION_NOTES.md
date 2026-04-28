# Validation notes — `examples/polars`

Run on 2026-04-28 against this branch (`shelf-23-peer-fetch`) on
Docker Desktop, arm64 macOS.

## Compose syntax

```
$ docker compose config --quiet && echo OK
OK
```

## End-to-end run

Built the runner image (`python:3.11-slim` + polars 1.40.1 +
pyiceberg 0.9.1 + boto3 1.42.91) and reused a sibling agent's
shelfd image (re-tagged as `shelf-polars-example/shelfd:local`,
content identical to a fresh `docker build -f shelfd/Dockerfile`).

```
$ bash run.sh
[run] starting MinIO + shelfd...
[run] waiting for shelfd /healthz...
[run] seeding Iceberg table demo.events...
[seed] warehouse=s3://warehouse/ endpoint=http://minio:9000
[seed] created namespace demo
[seed] generating 200000 rows...
[seed] schema: user_id: int64
event_type: string
country: string
amount: double
ts: timestamp[us]
[seed] metadata: s3://warehouse/demo.db/events/metadata/00001-76ea53b7-...metadata.json
[seed] wrote /shared/metadata_path.txt

[run] running Polars cold→warm benchmark...
[bench] table metadata: s3://warehouse/demo.db/events/metadata/00001-...metadata.json
[bench] reading via shelfd shim: http://shelfd:9092

[bench] cold:     151.7 ms   (40 groups)
[bench] warm:      31.1 ms   (40 groups)

shelfd cold→warm speedup: 4.88x
summary: cold: 0.15s | warm: 31ms
```

## Shelf telemetry confirms the cache is doing work

```
$ curl -s http://127.0.0.1:29090/metrics | grep -E "shelf_(hits|misses)_total"
shelf_hits_total{pool="metadata"} 2
shelf_hits_total{pool="rowgroup"} 2
shelf_misses_total{pool="metadata"} 3
shelf_misses_total{pool="rowgroup"} 2

$ curl -s http://127.0.0.1:29090/stats
{
  "pod_id": "shelf-polars-0",
  "capacity_bytes": 536870912,
  "used_bytes": 1282607,
  "metadata_pool": {
    "capacity_bytes": 268435456,
    "used_bytes": 7977,
    "disk_used_bytes": 0,
    "disk_capacity_bytes": 0
  },
  "rowgroup_pool": {
    "capacity_bytes": 268435456,
    "used_bytes": 1274630,
    "disk_used_bytes": 1282048,
    "disk_capacity_bytes": 536870912
  },
  "pinned_bytes": 0,
  "pinned_count": 0,
  "draining": false
}
```

Cold pass: 3 metadata + 2 rowgroup misses (the only origin traffic).
Warm pass: 2 metadata + 2 rowgroup hits, zero new misses, ~1.28 MB
held in the rowgroup pool (DRAM + spilled to the NVMe path).

## Notes / caveats

- 200 k rows × 5 columns is small enough to fit in a single Parquet
  row group, so the warm latency is dominated by Polars' Python
  startup + Arrow conversion, not S3 I/O. Bigger seeds (e.g.
  `SEED_ROWS=2000000`) widen the cold/warm gap further.
- The `Falling back to pure Python Avro decoder` warning from
  PyIceberg is benign — it only affects manifest decode speed and
  doesn't change the cache path.
- Numbers will vary by machine. The signal is the cold→warm
  ratio + the metric counters above, not the absolute milliseconds.
- Default ports `9000`, `19000` were already taken by sibling
  example stacks during validation; this example pins `29000`,
  `29001`, `29090`, `29092` to avoid collision. Adjust in
  `docker-compose.yml` if those clash too.
