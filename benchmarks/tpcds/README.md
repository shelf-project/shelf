# TPC-DS SF1000 Iceberg benchmark — Track F

This directory is the **scoreboard** for the mission laid out in
[`shelf_trino_perf_research.plan.md`](../../docs/plans/): "beat
Warp Speed, Alluxio, and Firebolt on TPC-DS SF1000 Iceberg, at the
same hardware budget, on both p50 wall-clock and `$/query`."

Nothing in this directory is allowed to make a performance claim
that cannot be reproduced by a third party running the scripts here
on their own AWS account.

## Layout

```
benchmarks/tpcds/
├── README.md              — this file
├── generator/             — F1: Iceberg SF1000 data generator
│   ├── generate_sf1000.sh — 24-table CTAS from Trino's built-in
│   │                         `tpcds` connector
│   └── smoke.sh           — SF1 smoke (fast sanity check)
├── queries/               — F2: the 99 canonical TPC-DS queries,
│                            pulled verbatim from trinodb/trino
├── runner/                — F2: cross-engine runner
│   ├── run.py             — invokes Trino / Trino+Shelf / Trino+Alluxio
│   │                         / Starburst+WarpSpeed / Firebolt via JDBC
│   │                         with cold/warm1/warm2 repeats
│   ├── engines.yaml       — connection + auth for each engine config
│   └── queries.yaml       — per-query timeout overrides
├── cost/                  — F3: cost model
│   ├── model.py           — reads runner CSVs, joins against
│   │                         `hardware.yaml` + `licenses.yaml`,
│   │                         emits per-query `$/query`
│   ├── hardware.yaml      — on-demand hourly prices per instance
│   ├── licenses.yaml      — Starburst license, Firebolt FBU rate
└── results/               — F4: per-run output
    └── <YYYY-MM-DD>/      — one directory per full run
        ├── raw.csv
        ├── summary.md
        └── signed-off-by.txt (two engineer signatures)
```

## Hardware budget (identical across engines)

- 4 × `m6a.12xlarge` (48 vCPU, 192 GiB each → 192 vCPU, 768 GiB total).
- EBS: 2 TiB gp3 per node for cache/scratch.
- Shelf and Alluxio run as additional pods on the same nodes; they
  do **not** get a bonus cluster.
- Firebolt is sized to match the same dollar/hour on on-demand
  list; ingestion time counts against the first-query cost.

See [`cost/hardware.yaml`](cost/hardware.yaml) for exact SKUs.

## How to reproduce

1. **Generate data (F1)** — runs once per Scale Factor, then the
   data stays in S3 for every subsequent run.

   ```bash
   export TRINO_URL=https://trino.alluxio.svc.cluster.local
   ./generator/generate_sf1000.sh
   ```

2. **Run the 99 queries (F2)** for each engine configuration.

   ```bash
   cd runner/
   python run.py --engine shelf --sf 1000 --out ../results/$(date +%F)/shelf.csv
   python run.py --engine alluxio --sf 1000 --out ../results/$(date +%F)/alluxio.csv
   python run.py --engine warpspeed --sf 1000 --out ../results/$(date +%F)/warpspeed.csv
   python run.py --engine firebolt --sf 1000 --out ../results/$(date +%F)/firebolt.csv
   ```

3. **Compute `$/query` (F3)**.

   ```bash
   cd cost/
   python model.py --run-dir ../results/$(date +%F)
   ```

4. **Publish (Exit criteria)** — see
   [`../../docs/exit-criteria.md`](../../docs/exit-criteria.md) for
   the two-engineer sign-off protocol.

## Regression gate (F4)

Every tag triggers `shelf-only-sf100`
([`.github/workflows/tpcds-regression.yml`](../../.github/workflows/tpcds-regression.yml)).
The gate fails if p50 regresses > 10 % against the baseline on
disk at `benchmarks/tpcds/baseline/shelf-sf100.csv`.

Full SF1000 nightly runs use the self-hosted runner
`tpcds-sf1000-runner` and land in `results/<date>/`.

## Honest residuals

Per [`BLUEPRINT.md`](../../BLUEPRINT.md) §7.4.4 and §7.5.4 we expect
to lose on two classes of queries:

- **Multi-predicate AND on unsorted, unbloomed columns** — Warp
  Speed's proprietary bitmap index wins. ~5 % of TPC-DS queries.
- **Novel ad-hoc aggregations on cold-MV state** — Firebolt's
  speculative cubes win. Even fewer queries.

Both classes are explicitly identified in `summary.md` for every
run. Hiding them is not an option.
