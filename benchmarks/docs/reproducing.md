# Reproducing a Shelf benchmark from zero

_Target: 90 minutes, clean laptop, clean AWS account, to first number._

This is the quality bar in `agents/7-benchmarker.md`. If a step takes
more than this, the harness has a bug — file it against SHELF-26.

---

## Prerequisites (pre-flight, not counted in 90 min)

On your laptop:

- `aws` CLI v2, `kubectl`, `helm` v3.14+, `terraform` ≥ 1.7, `jq`,
  `python` ≥ 3.11, `docker`, GNU `make`.
- An AWS account with permissions to create an EKS cluster, EC2 node
  groups, a VPC, an S3 bucket, and an IAM role for IRSA.
- A profile in `~/.aws/config` named `shelf-bench` (or pass
  `AWS_PROFILE=…` to every step).
- ≥ 20 GiB free disk for docker images + Iceberg fixtures.

Verify:

```bash
aws sts get-caller-identity --profile shelf-bench
kubectl version --client=true
helm version --short
terraform version
```

---

## Step 0 — Clone and bootstrap locally (5 min)

```bash
git clone https://github.com/<org>/shelf && cd shelf/benchmarks
export AWS_PROFILE=shelf-bench
export AWS_REGION=ap-south-1            # or wherever you run
```

---

## Step 1 — Spin up the cluster (15 min)

```bash
make env-up
```

Under the hood this runs `terraform -chdir=env apply -auto-approve`
with defaults tuned for a `shelf-bench-$USER` cluster:

- 1× EKS control plane.
- 3× `m6i.2xlarge` Trino workers (on-demand).
- 3× `i4i.2xlarge` Shelf nodes (NVMe-backed, on-demand — **never spot**
  per blueprint §12 Phase 5).
- 1× `m6i.large` benchmark driver.

Override any of these in `env/variables.tf` or via `TF_VAR_*` env vars.

Verify:

```bash
kubectl get nodes -L role
# expect: 7 nodes, roles trino / shelf / driver
```

---

## Step 2 — Install the software stack (20 min)

```bash
./bootstrap.sh
```

This is idempotent and resumable. It installs, in order:

1. MinIO (or S3 bucket) with 1 TB TPC-DS Iceberg fixture.
2. Trino via Helm.
3. Shelf via Helm (`configs/shelf/shelf-values.yaml`).
4. Alluxio OSS 2.9 baseline (`configs/alluxio-2-9/`).
5. fs.cache baseline (Trino sidecar configured via
   `configs/fs-cache/`).
6. The benchmark driver pod with `tpcds-kit`, `k6`, and the replay
   harness binaries.

If any step fails, rerun — bootstrap is idempotent by design.

---

## Step 3 — Run the smoke benchmark (5 min)

```bash
./tpcds/run.sh --profile=smoke --backend=shelf
```

The smoke profile runs 3 TPC-DS queries (Q3, Q19, Q42) against the
10-GB scale fixture, writes a result JSON, validates it against
`tpcds/schema.json`, and exits. If this passes, the cluster is healthy.

---

## Step 4 — Run the authoritative replay benchmark (30 min)

```bash
./replay/run.sh --backend=shelf --days=1
```

One day of rep-2 `cdp.trino_logs.trino_queries` is pulled from the
fixture, replayed at 2× real-time against the warm Shelf pool, and the
resulting hit rate + latency quantiles written to
`results/$(date +%F)/shelf/replay-<run_id>.json`.

Compare against Alluxio:

```bash
./replay/run.sh --backend=alluxio-2-9 --days=1
```

Then:

```bash
python3 -m tools.compare results/$(date +%F)/shelf/replay-*.json \
                         results/$(date +%F)/alluxio-2-9/replay-*.json
```

---

## Step 5 — Tear down (10 min)

```bash
./cleanup.sh
make env-down
```

Verify with `aws eks list-clusters` and `aws ec2 describe-instances`
that nothing `shelf-bench-*` remains.

---

## 90-minute budget

| Step                               | Budget |
| ---------------------------------- | ------ |
| 1. `make env-up`                   | 15 min |
| 2. `bootstrap.sh`                  | 20 min |
| 3. smoke TPC-DS                    | 5 min  |
| 4. 1-day replay × 2 backends       | 35 min |
| 5. `cleanup.sh` + `make env-down`  | 10 min |
| **Budget**                         | **85 min** |

5 minute buffer for network jitter, IAM propagation, etc.

---

## Known gotchas

- IAM role creation → IRSA trust policy can take 30-60 s to propagate.
  `bootstrap.sh` retries with backoff.
- NVMe PVs on `i4i.*` require the AWS EBS CSI driver — provisioned by
  Terraform automatically.
- If your AWS quota for on-demand `i4i.2xlarge` is 0 in the chosen
  region, `env-up` fails fast with a clear error. File a quota
  increase before running.

## TODO_SHELF-26 / TODO_SHELF-28

- `replay/` sample fixture (1-day rep-2 slice) must be checked into a
  public S3 prefix — until then step 4 requires internal access.
- `spot-churn/run.sh` needs the chaos driver from SHELF-28 to pass
  Step 3 smoke in < 5 min. Until then smoke skips it.
