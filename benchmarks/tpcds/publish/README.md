# TPC-DS SF1000 publication gate

Final milestone of the mission. This directory owns the procedure
for publishing the benchmark results to the outside world; the
scaffold is in place today so that when the F-track harness
crosses the publication bar the handle is a known quantity instead
of a rush-order under launch pressure.

## Publication gate

From the plan:

> Publish — full TPC-DS SF1000 results for all four engines,
> CSVs signed by two engineers, third-party replication
> instructions. Gated on shelf winning p50 on ≥ 80/99 queries AND
> $/query on ≥ 95/99 queries. Honest residuals per §7.4.4 / §7.5
> called out explicitly.

Until every one of the following is true, nothing in this
directory publishes:

- F2 harness has run all 99 queries on all four engine
configurations (shelf+Trino, Trino+Alluxio, Starburst+Warp
Speed, managed Firebolt) at identical 192 vCPU / 768 GiB.
- Each engine has at least 10 cold, 10 warm₁, 10 warm₂ runs
per query. Median is the publishable number; `p5`-`p95` is
in the dataset.
- Shelf beats all three engines on **p50 wall-clock** for ≥
80/99 queries.
- Shelf beats all three engines on `**$/query`** for ≥ 95/99
queries. The cost model is F3's `model.py`; inputs (rates,
licenses) are frozen in the commit that produces the
publishable result.
- Every losing query has an entry in `residuals.md`
explaining why shelf loses, mapped back to a BLUEPRINT §7
residual or a fresh one documented in the same PR.
- Two engineers have signed `results-sf1000.csv`; signatures
live in `results-sf1000.csv.sig` alongside the CSV so the
provenance chain is reproducible.
- The third-party replication guide below is walked through
by an engineer who has **not** touched shelf before. Their
resulting CSV is within ±10 % of our published numbers.

## Directory layout when populated

```
publish/
├── README.md                ← you are here
├── results-sf1000.csv       ← the numbers we stand behind
├── results-sf1000.csv.sig   ← two engineer signatures (GPG)
├── residuals.md             ← every loss, with a BLUEPRINT pointer
├── replication.md           ← step-by-step third-party guide
├── hardware.md              ← instance types, AZ, pricing lookup
└── engines/                 ← one directory per engine config
    ├── shelf-trino/
    │   ├── config.md        ← every knob that moved from defaults
    │   ├── catalogs/        ← verbatim `.properties` files
    │   └── deploy.yaml      ← Kubernetes manifests used
    ├── trino-alluxio/
    ├── starburst-warpspeed/
    └── firebolt/
```

## Third-party replication (scaffold)

The full text will live in `replication.md` once the results
exist. The structure is fixed so the docs don't drift:

1. **Data prep.** `benchmarks/tpcds/generator/generate_sf1000.sh
  s3://your-bucket/sf1000/`. SHA-256 manifest is published so
   third parties can confirm bit-identical input.
2. **Cluster provisioning.** Terraform under `infra/sf1000/`
  brings up a 16 × `r6id.12xlarge` EKS cluster (192 vCPU / 768
   GiB shared across Trino workers + 1 coord). Same plan for
   Alluxio + Starburst variants; managed Firebolt uses the sizing
   Firebolt supports publicly, with notes in `hardware.md`.
3. **Engine install.** Four documented paths, one per engine
  variant. Each points to the exact container image + config
   commit used for the published run.
4. **Benchmark execution.** `benchmarks/tpcds/runner/run.py
  --engine  --queries 1-99 --repeats 10 --phase all `produces an engine-scoped CSV matching`runner/schema.md`.
5. **Cost join.** `benchmarks/tpcds/cost/model.py --input
  .csv`emits a`$/query` column; compare to our
   published numbers.

The guide must be so concrete that running it on an AWS account
you freshly provisioned produces the claim we publish. The F4
regression gate is the first-party smoke test that this guide
doesn't bit-rot — every tagged release has to still reproduce the
SF100 baseline before it's allowed to promote.

## Disclosure policy

- **Engine attribution.** Numbers for Starburst + Firebolt come
from the managed offerings at their documented list prices as
of the publication commit. We publish the commit SHA that
froze those inputs; we do not backdate.
- **License math.** Starburst Enterprise unit price is negotiated
per-customer. We publish the **list price** per
`[docs.starburst.io/pricing](https://docs.starburst.io/pricing)`
(or equivalent) and call out that enterprise discounts can
close the gap.
- **Residuals.** Every query shelf loses gets its own paragraph
in `residuals.md` — no silent dropping. If the residual is
fixable, it becomes a ticket in the next tier. If it's a
fundamental architecture trade-off, we cite the BLUEPRINT §7
paragraph that anticipated it.
- **Updates.** Results are re-run whenever a new Trino version
lands, quarterly at minimum. Diffs between runs are published
with the same signatures.

## What you do when you think we're ready

1. Run the full 99-query harness on every engine config at
  SF1000 with the latest `main` shelf build. CSVs land in
   `runner/out/<timestamp>/`.
2. Execute `cost/model.py` to produce the `$/query` column.
3. Evaluate the gate above. Either:
  - Not ready — file issues for the queries that lost, close
   this PR without publishing. OK.
  - Ready — populate every file in this directory, collect the
  two GPG signatures, run the third-party rehearsal, and only
  then open the publication PR.
4. Link the PR from `shelf_trino_perf_research.plan.md`'s exit
  criteria so the mission artefact points at the publication
   artefact.

