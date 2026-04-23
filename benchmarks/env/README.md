# `env/` — Terraform for the bench cluster

EKS cluster + three node groups (Trino workers, Shelf nodes, driver).
Every instance type, count, and AMI family is a variable in
`variables.tf` with an explicit default.

**Status: scaffolding.** The actual module versions are pinned via `~>`
but there is no first-green apply yet — see `TODO_SHELF-27` in
`main.tf`.

## Files

| File           | Contents                                                   |
| -------------- | ---------------------------------------------------------- |
| `versions.tf`  | Pinned Terraform + provider versions.                       |
| `variables.tf` | Every parameter. Instance type, count, region, tags, etc.   |
| `main.tf`      | VPC + EKS + node groups + results bucket.                   |
| `outputs.tf`   | `cluster_shape` structured output used by every run record. |

## Quick start

```bash
export AWS_PROFILE=shelf-bench
export AWS_REGION=ap-south-1

terraform -chdir=env init
terraform -chdir=env validate
terraform -chdir=env plan -out=plan.tfplan
terraform -chdir=env apply plan.tfplan
```

Tear down:

```bash
terraform -chdir=env destroy -auto-approve
```

Or use the harness-level targets:

```bash
make env-up    # wraps init + apply
make env-down  # wraps destroy
```

## What this cluster is not

- **Not prod-grade.** No ACLs, no private endpoint, no audit log sink.
  Purely for reproducible benchmark runs.
- **Not multi-region.** Single-region by design; cross-AZ only.
- **Not cost-optimised.** On-demand for Shelf nodes (never spot, per
  plan §3 Phase 5). Spot on Trino workers is an explicit `spot-churn`
  test variable, not a default.

## Cost sketch (ap-south-1, 2026 list price)

| Resource              | Hourly | Notes                               |
| --------------------- | ------ | ----------------------------------- |
| EKS control plane     | $0.10  | Flat.                                |
| 3× `m6i.2xlarge`      | ~$1.26 | Trino workers.                       |
| 3× `i4i.2xlarge`      | ~$1.80 | Shelf nodes, NVMe included.          |
| 1× `m6i.large`        | ~$0.14 | Driver.                              |
| NAT gateway + data    | ~$0.20 | Single NAT; tune with `--single-nat`.|
| **Total**             | ~$3.50/hr | A full nightly run ≈ $14.         |

TPC-DS @ 1 TB adds ~$5 of S3 GET cost per full matrix sweep.

## TODO_SHELF-27

- First-green apply.
- State migration to S3 backend (`versions.tf`).
- IRSA role wiring for the driver pod to read the results bucket.
- ArgoCD bootstrap of the Grafana stack on the driver node group.
