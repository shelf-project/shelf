# Charts — provenance and how to regenerate

These PNGs are referenced from [`shelf/EXEC_BRIEF.md`](../../../../EXEC_BRIEF.md). Each one is generated **deterministically** from data captured today on the dev Trino cluster (`trino` namespace) or from the completed-todo list of the session. Nothing here is fabricated; nothing here is a Shelf-vs-vendor wall-clock comparison (we do not have those numbers yet).

## Source for each chart

| Chart | File | Source data |
|-------|------|-------------|
| 1 | `01-cold-warm-wallclock.png`     | [`../tpcds-sf1.csv`](../tpcds-sf1.csv) — 8 queries × 3 phases, real Trino timing |
| 2 | `02-planning-cold-vs-warm.png`   | same CSV, `planning_ms` column |
| 3 | `03-iceberg-cold-warm.png`       | [`../run.log`](../run.log) — `cdp.lms.silver_companies` cold→warm3 |
| 4 | `04-status-scorecard.png`        | completed-todo list of the session (counts are auditable in the chat transcript and in `shelf/` source tree) |
| 5 | `05-vendor-headlines.png`        | vendor marketing pages — URL printed inside each bar |
| 6 | `06-time-breakdown.png`          | same CSV as chart 1, decomposed into planning / CPU / other |

## Regenerate

```bash
python3 -m venv /tmp/charts_venv
/tmp/charts_venv/bin/pip install matplotlib
/tmp/charts_venv/bin/python build_charts.py
```

Output is written back into this directory. The script is the *only* thing that produces the PNGs — it reads the CSV / `run.log` and never invents a number.

## Why no Shelf-vs-vendor wall-clock chart

Three reasons, in priority order:

1. **shelfd has not yet been deployed in the dev or prod clusters.** Today's smoke test ran *raw Trino* against `cdp` (S3 direct) and `cdp_shelf` (Alluxio) only.
2. **The F2 cross-engine cluster (192 vCPU / 768 GiB) does not exist yet.** Without identical hardware you cannot honestly stack one bar next to another.
3. **No Starburst Galaxy or Firebolt account is provisioned.** Vendor-side numbers from their own marketing pages are *their* hardware, *their* workload — chart 5 reproduces them faithfully and refuses to combine them.

When (1)–(3) land, this directory will gain `07-shelf-vs-vendors-sf1000.png`.
