# Shelf benchmark harness

_Authoritative spec lives in `BLUEPRINT.md` §10 and `shelf/agents/out/03-plan.md` §6.
This directory is the runnable contract — no prose claim in the launch blog
may leave this repo unless there is a `run.sh` here that produces the
number, end to end, from a clean AWS account._

Status: **SCAFFOLDING** (v0.0). Every runner in this tree exits 0 on
`--dry-run`, echoes what it *would* do, and writes no real results.
Reality of runs is out of scope until Phase 1 (SHELF-26) lands.

---

## Four benchmarks

| Dir            | Name                    | Owns which SLO in plan §6                   |
| -------------- | ----------------------- | ------------------------------------------- |
| `tpcds/`       | TPC-DS @ 1 TB           | Published launch numbers (§10.1 blueprint). |
| `cold-start/`  | 2 → 20 worker scale-up  | Phase 2 gate (§6.5 plan): TTFQ p95 ≤ 3 s.    |
| `spot-churn/`  | Pod-kill chaos          | Phase 3 gate (§6.6 plan): hit rate ≥ 65 %.   |
| `replay/`      | 7-day rep-2 replay      | **v0.5 kill-switch** (ADR-0010).            |

The `replay` benchmark is the authoritative one for our shop; TPC-DS
exists because external reviewers expect it.

---

## Backends compared

Every benchmark is a matrix over these backends. Config for each lives
under `configs/<backend>/`.

| Backend              | Short name       | Purpose                                      |
| -------------------- | ---------------- | -------------------------------------------- |
| Raw S3               | `raw-s3`         | Lower bound. No cache.                        |
| Trino `fs.cache`     | `fs-cache`       | Status quo on rep-0.                          |
| Alluxio OSS 2.9.5    | `alluxio-2-9`    | **The number we must beat** (E12 baseline).   |
| Alluxio 3.x DORA     | `alluxio-3-dora` | OSS peer.                                     |
| Shelf                | `shelf`          | This repo.                                    |

---

## Reproducibility contract

A published number is only a number if it carries all of:

1. `run_id` — ULID.
2. `commit_sha` — of this repo, the Shelf pod image, and the Trino image.
3. `cluster_shape` — from `terraform show -json` snapshot.
4. `config` — hash of the `configs/<backend>/` tree used.
5. Raw latency data in `results/<date>/<backend>/<run-id>.json`,
   validating against the per-benchmark `schema.json`.
6. A `reproduce:` command block in `RESULTS.md` that a reviewer with
   AWS creds can paste and get the same number (±noise) in ≤ 90 min.

If any one is missing, the result is **invalid** and may not be cited.

---

## How to run any benchmark (one command)

```bash
# Tear up the cluster (~15 min on EKS)
make env-up

# Install Trino, Shelf, Alluxio baselines, MinIO fixture, load-gen
./bootstrap.sh

# Run any benchmark against any backend
./tpcds/run.sh       --backend=shelf      --scale=1tb
./cold-start/run.sh  --backend=alluxio-3-dora
./spot-churn/run.sh  --backend=shelf
./replay/run.sh      --backend=shelf      --days=7

# Tear it all down
./cleanup.sh
make env-down
```

All `run.sh` scripts accept `--dry-run` (no cluster side effects) and
`--results-dir=<path>` (default `results/$(date +%F)/<backend>/`).

Smoke mode (the CI regression gate) is triggered by
`--profile=smoke`: a fixture-only subset finishing in ≤ 10 min.

---

## Directory layout

```
benchmarks/
├── README.md              ← this file
├── RESULTS.md             ← aggregate summary, one row per (tag, backend, bench)
├── bootstrap.sh
├── cleanup.sh
├── env/                   ← Terraform (EKS + node groups)
├── configs/
│   ├── raw-s3/
│   ├── fs-cache/
│   ├── alluxio-2-9/
│   ├── alluxio-3-dora/
│   └── shelf/
├── tpcds/
├── cold-start/
├── spot-churn/
├── replay/
├── results/
│   └── README.md          ← publishing + naming convention
└── docs/
    └── reproducing.md     ← step-by-step, ≤ 90 min to first number
```

---

## Quality bar (from `agents/7-benchmarker.md`)

- Every published number carries: exact cluster shape, software
  versions, input data hash, run-ID, timestamp.
- No "representative" screenshots without raw data linked.
- Reviewer with AWS creds reproduces in ≤ 90 min, zero to first number.
- Harness survives spot interruption on the benchmark cluster itself
  (resumable, or marks runs as invalid — never silent corruption).
- Results published as p50 / p95 / p99 / **p99.9**, never "mean".

---

## TODOs tracked as tickets

Every scaffold gap here maps to a ticket in the plan. Look for
`TODO_SHELF-NN` markers in the scripts.

| Ticket      | Gap                                                   |
| ----------- | ----------------------------------------------------- |
| SHELF-26    | `replay/` materialises real manifests + byte ranges.   |
| SHELF-27    | Grafana dashboard JSON for live view.                  |
| SHELF-28    | Chaos drill driver for `spot-churn/`.                  |
| SHELF-12    | Docker-compose smoke fixture reused in CI gate.        |

See `agents/out/03-plan.md` §4 for ticket bodies.
