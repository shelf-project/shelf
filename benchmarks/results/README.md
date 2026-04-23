# Results directory

_Raw benchmark output lives here. One file per run. Publishing,
versioning, and discovery rules below._

Status: **empty at launch**.

---

## Naming convention

```
results/<YYYY-MM-DD>/<backend>/<benchmark>-<run_id>.json
```

- `<YYYY-MM-DD>` is the UTC date the run started.
- `<backend>` ∈ {`raw-s3`, `fs-cache`, `alluxio-2-9`, `alluxio-3-dora`, `shelf`}.
- `<benchmark>` ∈ {`tpcds`, `cold-start`, `spot-churn`, `replay`}.
- `<run_id>` is a Crockford ULID. Every `run.sh` invocation creates one
  and echoes it to stdout before first work.

Examples (illustrative; none of these files exist yet):

```
results/2026-06-01/shelf/tpcds-01HF8K9X2A9WV3NK9B8H3G0VZP.json
results/2026-06-01/alluxio-2-9/replay-01HF8K9XAB8H8QZC9NB8PR2KX7.json
```

Each `.json` validates against the benchmark's `schema.json`. A run
that cannot be validated is **discarded** by the publisher job.

---

## Publishing

Results are pushed to an S3 bucket — one object per file, plus an
aggregate Parquet per month. Bucket name is parameterised in
`env/variables.tf` (`results_bucket`). The CI job that publishes:

1. Runs `jsonschema` against every new `.json` in the current run.
2. Uploads to `s3://$results_bucket/<path>` with immutable retention
   (14 days S3 Glacier, then deep archive).
3. Appends a row to `../RESULTS.md` via a PR (machine-authored).
4. Emits `s3://$results_bucket/latest.json` pointing at the newest
   run per `(backend, benchmark)`.

Nothing else writes to this tree. Human-authored files are rejected by
the `paths-filter` rule in `.github/workflows/bench.yml`.

---

## Linking to raw data

When `RESULTS.md` cites a number, the `raw` column is a link of the
form:

```
[raw](s3://shelf-bench-results/2026-06-01/shelf/tpcds-01HF...json)
```

For public-launch readers who cannot authenticate to the S3 bucket, a
nightly mirror job copies the last 30 days to a public-read prefix —
see `RESULTS.md` `Changelog` for the public URL.

---

## Retention

- Raw JSON: 90 days hot, 1 year warm, forever cold.
- Aggregated Parquet: forever.
- Grafana panel screenshots (if any): attached to the same S3 prefix,
  not to `RESULTS.md` (per quality bar: "no screenshots without raw").

---

## TODO_SHELF-26

`replay/run.sh` must emit a record that links back to:

- the `trino_queries` snapshot ID it replayed (so the input is
  reproducible),
- the shelfd commit SHA that served the run,
- the Trino image digest.

See `replay/SPEC.md` §Reproducibility Command.
