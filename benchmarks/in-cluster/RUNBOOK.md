# In-cluster benchmark — operator runbook

> Step-by-step procedure for running a full v1 Shelf benchmark
> (TPC-DS + cold-start + 1-day replay) against the in-cluster fixture
> from [`README.md`](README.md). Wall-clock budget: ~24 h.
>
> Each step is idempotent. If a step fails, fix it and re-run; the
> harness writes results to per-step paths so you don't lose work
> earlier in the sequence.

## Pre-flight (T-30 min)

| Check | Command | Expected |
| ----- | ------- | -------- |
| Bench S3 bucket exists | `aws s3 ls s3://${BENCH_BUCKET}` | no error |
| IRSA role mapped | `aws iam get-role --role-name shelf-bench-s3` | role exists |
| HMS reachable | `kubectl run hms-probe --rm -it --image=alpine -- sh -c "nc -zv ${HMS_HOST} 9083"` | open |
| Cluster context | `kubectl config current-context` | matches the cluster you intend to run on |
| `shelf-bench` runner installed | `which trino` | python `trino` package |

```bash
export BENCH_BUCKET=s3://my-tpcds-bench
export BENCH_REGION=us-east-1
export HMS_THRIFT_URI=thrift://my-metastore.svc.cluster.local:9083
export SHELF_IRSA_ROLE_ARN=arn:aws:iam::123456789012:role/shelf-bench-s3
export SHELF_BENCH_RUN_DATE=$(date -u +%F)
export SHELF_BENCH_RESULTS_DIR=benchmarks/results/${SHELF_BENCH_RUN_DATE}
```

## Step 1 — TPC-DS Iceberg fixture (T+0, ~2 h for SF100, ~half-day for SF1000)

```bash
# Generate the fixture into BENCH_BUCKET. SF100 = ~100 GB Iceberg
# tables; SF1000 = ~1 TB. v1 default is SF100; bump to SF1000 once
# the bench cluster's Karpenter pool has the headroom.
export TRINO_URL=https://example-trino-cluster.${YOUR_DOMAIN}
./benchmarks/tpcds/generator/generate_sf1000.sh   # or smoke.sh for SF1
```

The generator writes the 24 TPC-DS tables (`store_sales`, `web_sales`,
`catalog_sales`, etc.) under
`s3://${BENCH_BUCKET}/tpcds_sf100/<table>/data/`. Once done, the fixture
is reusable for every subsequent run; you don't regenerate per
run.

## Step 2 — Stand up the bench fixture (T+2 h, ~10 min)

```bash
./benchmarks/in-cluster/up.sh
```

Verify `kubectl -n trino-bench get pods -o wide` shows:
- `shelf-bench-{0,1,2}` Running 1/1
- `trino-bench-coordinator-…` Running 1/1
- `trino-bench-worker-{0..3}` Running 1/1

Port-forward the coordinator for harness queries:
```bash
kubectl -n trino-bench port-forward svc/trino-bench 18080:8080 &
kubectl -n trino-bench port-forward svc/shelf-bench 19090:9090 &
```

## Step 3 — TPC-DS run (T+2.5 h, ~1 h per backend)

Two passes: `cdp_shelf` (Shelf-fronted) and `cdp` (raw S3 baseline).
The catalog determines which path the bench Trino takes.

```bash
mkdir -p ${SHELF_BENCH_RESULTS_DIR}/{shelf,raw-s3}

python3 benchmarks/tpcds/runner/run.py \
  --engine shelf \
  --sf 100 \
  --out ${SHELF_BENCH_RESULTS_DIR}/shelf/tpcds.csv

python3 benchmarks/tpcds/runner/run.py \
  --engine raw-s3 \
  --sf 100 \
  --out ${SHELF_BENCH_RESULTS_DIR}/raw-s3/tpcds.csv
```

The runner consumes `engines.yaml` for connection details. Override
its `shelf` entry's URL to point at the bench coordinator
(`http://localhost:18080`) for this run; commit the override to
`engines.bench.yaml` so the next operator can re-use it.

## Step 4 — Cold-start (T+4.5 h, ~30 min per backend)

```bash
./benchmarks/cold-start/run.sh --backend=shelf  --apply
./benchmarks/cold-start/run.sh --backend=raw-s3 --apply
```

The driver scales `trino-bench-worker` from 2 → 20, fires the 20
queries in `cold-start/queries/dashboard-20.sql`, records TTFQ per
(query, cycle), then scales back. Cycles=3 by default.

## Step 5 — 1-day replay (T+6 h, ~12 h per backend at 2×)

```bash
# Materialise a 1-day rep-2 trace into ${SHELF_BENCH_RUN_DATE}/replay-fixture/
./benchmarks/replay/prep.sh --replica=rep-2 --days=1 --apply

# Then issue the trace against the bench Trino, sampling shelfd
# /metrics every 10 s.
python3 benchmarks/replay/run.py \
  --trace ${SHELF_BENCH_RESULTS_DIR}/replay-fixture/trace.jsonl \
  --backend shelf \
  --trino-url http://localhost:18080 \
  --trino-user bench-runner \
  --catalog cdp_shelf \
  --speed 2x \
  --shelfd-metrics-url http://localhost:19090/metrics \
  --out ${SHELF_BENCH_RESULTS_DIR}/shelf/replay-$(./benchmarks/tools/ulid.sh).json

python3 benchmarks/replay/run.py \
  --trace ${SHELF_BENCH_RESULTS_DIR}/replay-fixture/trace.jsonl \
  --backend raw-s3 \
  --trino-url http://localhost:18080 \
  --trino-user bench-runner \
  --catalog cdp \
  --speed 2x \
  --out ${SHELF_BENCH_RESULTS_DIR}/raw-s3/replay-$(./benchmarks/tools/ulid.sh).json
```

## Step 6 — Cost model (T+18 h, ~5 min)

```bash
python3 benchmarks/tpcds/cost/model.py --run-dir ${SHELF_BENCH_RESULTS_DIR}
```

Reads each backend's tpcds.csv, joins against `cost/hardware.yaml`,
emits `cost-summary.csv` with per-query $/query.

## Step 7 — Gate evaluation (T+18.5 h, ~1 min)

The v0.5 ADR-0010 gate is comparative: Shelf vs Alluxio. With v1's
scope-down to `shelf | raw-s3`, the gate evaluator can only score
`latency_ns_p95_vs_alluxio` against vendor-cited Alluxio numbers from
[`docs/VENDOR-COMPARE.md`](../../docs/VENDOR-COMPARE.md). The other 4
gate metrics evaluate cleanly without Alluxio.

```bash
python3 benchmarks/tools/gate.py \
  --shelf    ${SHELF_BENCH_RESULTS_DIR}/shelf/replay-*.json \
  --baseline ${SHELF_BENCH_RESULTS_DIR}/raw-s3/replay-*.json \
  --pages-shelf-attributed 0 \
  --oncall-surface-shelf 1.0 \
  --oncall-surface-baseline 1.0 \
  --emit-row >> benchmarks/RESULTS.md
```

NOTE: `--baseline=raw-s3-replay.json` is a *substitute*, not a true
Alluxio baseline. The gate evaluator will produce a row but the
`latency_ns_p95_vs_alluxio` cell is computed against raw S3 rather
than Alluxio. Mark it as such in the RESULTS.md row commentary.

## Step 8 — Aggregate to RESULTS.md + open PR (T+18.5 h)

If the nightly workflow (`bench.yml`) is the runner, this happens
automatically via the `Open results PR` job. For a manual run:

```bash
git checkout -b results/${SHELF_BENCH_RUN_DATE}
git add benchmarks/RESULTS.md ${SHELF_BENCH_RESULTS_DIR}/
git commit -m "results(${SHELF_BENCH_RUN_DATE}): v1 nightly aggregate"
git push -u origin HEAD
gh pr create --base main --title "results: ${SHELF_BENCH_RUN_DATE}" \
  --body "Nightly v1 bench. Backends: shelf, raw-s3."
```

## Step 9 — Archive + tear down (T+19 h, ~5 min)

```bash
export SHELF_BENCH_RESULTS_BUCKET=my-shelf-bench-archive
ARCHIVE_RESULTS=1 ./benchmarks/in-cluster/down.sh
```

Verifies the archive completed, then `helm uninstall`s both releases
and deletes the namespace.

## Failure-mode quick reference

| Symptom | Likely cause | Fix |
| ------- | ------------ | --- |
| `up.sh` errors `OOMKilled` on shelf-bench pods | Pod limit ≤ allocatable on c-family node | Drop c-family from your NodePool (AGENTS.md May 1 OOM cascade RCA); keep `m`/`r` 4xlarge only. Pod limit 40 GiB. |
| Trino query 503 on `cdp_shelf` | `shelf-bench-pool` Service not ready, or NetworkPolicy too tight | `kubectl -n trino-bench logs shelf-bench-0`; verify `shelf-bench` Service has endpoints; check the chart's `networkPolicy.extraIngressFrom` |
| `iceberg.metadata-cache` not flipped | `cdp_shelf.properties` wasn't loaded | `kubectl -n trino-bench rollout restart deploy/trino-bench-coordinator` (catalog props are parsed at coord start, NOT hot-reloaded — AGENTS.md May 1 entry) |
| Replay run hits `ICEBERG_INVALID_METADATA` | Pre-preview-9 PUT corruption tail (workspace memory) | Use a freshly-prep'd trace; the bench fixture is read-only and shouldn't see write corruption |
| `shelf_disk_bytes_used == capacity` | NORMAL on a warm hybrid pool — Foyer reports `min(written, configured_cap)` | Surface in dashboard explicitly; not an alarm |
| `latency_ns_p95_vs_alluxio` shows 1.0 vs raw S3 | v1 has no Alluxio in matrix (see Step 7); ratio is vs raw S3, not Alluxio | Document explicitly in the RESULTS.md commentary; the v0.5 gate threshold (≤ 1.20) is calibrated for Alluxio specifically |
