# In-cluster benchmark fixture

> The "clean method" path: stand up an ephemeral Trino + Shelf in a
> fresh `trino-bench` namespace on any Kubernetes cluster, run the
> bench, `helm uninstall` cleanly. Zero new EKS, zero Terraform,
> zero touch to any production namespace.

This is the v1 reproducible-on-your-cluster path that produces the
numbers in [`benchmarks/RESULTS.md`](../RESULTS.md). The standalone
EKS path (`benchmarks/env/`) stays in the repo for users who want a
fully-isolated cluster, but it is **not** the documented v1
publication path — the in-cluster path is.

## Layout

```
benchmarks/in-cluster/
├── README.md                    — this file
├── up.sh                        — install everything (idempotent)
├── down.sh                      — tear down + optional archive
└── manifests/
    ├── namespace.yaml           — trino-bench namespace
    ├── shelf-bench-values.yaml  — Helm values for charts/shelf
    ├── trino-bench-values.yaml  — Helm values for upstream trino/trino
    └── catalogs/
        ├── cdp.properties       — raw S3 baseline
        └── cdp_shelf.properties — Shelf-fronted (s3.endpoint=shelf-bench:9092)
```

## What gets created

- **Namespace** `trino-bench` (labelled `shelf.io/scope: ephemeral` so a
  cluster janitor can identify it).
- **`shelf-bench` StatefulSet** — 3 fresh shelfd pods, image
  `ghcr.io/shelf-project/shelfd:1.0.0`, 40 GiB pod-memory limit, 240 GiB
  NVMe per pod via `ebs-gp3-wffc` PVCs (override
  `storage.storageClassName` for local-NVMe nodes).
- **`trino-bench`** — 1 coordinator + N workers (default 4), Trino 480,
  two catalogs side-by-side:
    - `cdp` — raw S3 endpoint (baseline).
    - `cdp_shelf` — `s3.endpoint=http://shelf-bench.trino-bench.svc.cluster.local:9092`,
      `iceberg.metadata-cache.enabled=false` (forces metadata reads
      through the shim — required to surface metadata-pool hit ratios).

The two catalogs share the same HMS, same S3 bucket, same Iceberg
fixture. The only difference is the `s3.endpoint`.

## Required preconditions

The bench fixture is vendor-neutral, but you have to point it at your
own data plane:

| Variable                | Purpose                                             |
| ----------------------- | --------------------------------------------------- |
| `BENCH_BUCKET`          | S3 bucket holding the TPC-DS Iceberg fixture (`s3://…`). |
| `BENCH_REGION`          | AWS region of that bucket.                          |
| `HMS_THRIFT_URI`        | Hive Metastore endpoint (`thrift://host:9083`).     |
| `SHELF_IRSA_ROLE_ARN`   | IRSA role with `s3:GetObject` on the bench bucket. |

You also need a kube-prometheus-stack with a `release: kube-prometheus-stack`
ServiceMonitor selector, or override `serviceMonitor.additionalLabels` to
match your cluster.

## Quickstart

```bash
# 1. Set up your environment.
export BENCH_BUCKET=s3://my-tpcds-bench
export BENCH_REGION=us-east-1
export HMS_THRIFT_URI=thrift://my-metastore.svc.cluster.local:9083
export SHELF_IRSA_ROLE_ARN=arn:aws:iam::123456789012:role/shelf-bench-s3

# 2. Generate the Iceberg fixture once (~2 hr for SF100, ~half-day for SF1000).
./benchmarks/tpcds/generator/generate_sf1000.sh   # or smoke.sh for SF1

# 3. Stand up the fixture.
./benchmarks/in-cluster/up.sh

# 4. Run the bench (in any order; cdp_shelf side gets warm intentionally).
python3 benchmarks/tpcds/runner/run.py    --engine shelf   --sf 100 --out results/$(date -u +%F)/shelf/tpcds.csv
python3 benchmarks/tpcds/runner/run.py    --engine raw-s3  --sf 100 --out results/$(date -u +%F)/raw-s3/tpcds.csv
./benchmarks/cold-start/run.sh            --backend=shelf  --apply
./benchmarks/cold-start/run.sh            --backend=raw-s3 --apply
./benchmarks/replay/run.sh                --backend=shelf  --days=1 --speed=2x --apply
./benchmarks/replay/run.sh                --backend=raw-s3 --days=1 --speed=2x --apply

# 5. Cost model.
python3 benchmarks/tpcds/cost/model.py --run-dir results/$(date -u +%F)

# 6. Optional — archive the results to S3 before teardown.
export SHELF_BENCH_RESULTS_BUCKET=my-shelf-bench-results
ARCHIVE_RESULTS=1 ./benchmarks/in-cluster/down.sh

# 7. Or just tear down without archiving.
./benchmarks/in-cluster/down.sh
```

## Cost-cap and capacity sketch

A full run on a small bench cluster (3 shelfd pods + 1 coord + 4 workers)
holds the following resources for the duration of the run:

| Component        | Pods | CPU req / pod | Mem req / pod | Storage / pod |
| ---------------- | ---- | ------------- | ------------- | ------------- |
| `shelf-bench`    | 3    | 4             | 32 GiB        | 240 GiB EBS   |
| `trino-bench` coord | 1 | 4             | 16 GiB        | —             |
| `trino-bench` worker | 4 | 4            | 16 GiB        | —             |

On `m5a.4xlarge` (16 vCPU / 64 GiB) you fit ~2 worker + 1 shelfd per
node → 3 shelfd nodes + 3 worker nodes + 1 coord node ≈ 7 nodes. At
`ap-south-1` on-demand list (~$0.40/hr/node), the fixture costs
~$3/hr. Full SF100 + cold-start + 1-day replay completes in ~24 h
wall-clock, so a full bench cycle ≈ $70.

## Honest caveats

- **The bench Shelf is fresh.** It does NOT inherit the warm working
  set of any production `shelf-pool`. p95 numbers in the first 30 min
  reflect cold-cache state; that's the point. Reproducing operator
  evidence ("rep-2 cutover saved 95 % of `ICEBERG_INVALID_METADATA`")
  is out of scope here — see
  [`docs/rollout-v1/cutover-rep2.md`](../../docs/rollout-v1/cutover-rep2.md).
- **Vendor numbers are NOT measured here.** Alluxio / Warp Speed /
  Firebolt comparisons use vendor-published TPC-DS / TPC-H runs. See
  [`docs/VENDOR-COMPARE.md`](../../docs/VENDOR-COMPARE.md) for the
  citation matrix.
- **Network latency dominates SF1.** SF1 (~1 GB) is too small to surface
  cache-effectiveness signal — query setup and JVM warm-up dominate.
  SF100 is the smallest scale that produces a meaningful Shelf vs raw-S3
  delta on Iceberg reads. SF1000 is the OSS launch headline number.
- **Down.sh is the operative cleanup path.** No part of the fixture is
  intended to outlive the run. If `down.sh` exits non-zero, fall back to
  `kubectl delete ns trino-bench --force --grace-period=0` and file an
  issue.
