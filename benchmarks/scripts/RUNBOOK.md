# Production-trace replay harness — operator runbook

> Wraps the SHELF-35 replay tooling (`tools/gen_replay_list.py` +
> `tools/replay_pinlist.py`) with end-to-end orchestration, sidecar
> metric scrapes, and schema-valid output records that satisfy
> `benchmarks/replay/schema.json`. Designed for V1 of the rc8 roadmap
> as the gate before V2 (12-hour bench execution) and V3 (verdict
> ADR-0039).

> **Scope deviation note.** The original V1 spec referenced
> `benchmarks/in-cluster/RUNBOOK.md`; that file is part of the V2 dispatch
> (in-cluster bench fixture) and does not yet exist on `main`. This
> RUNBOOK is therefore self-contained under `benchmarks/scripts/` and
> documents the harness alone. When V2 lands, fold the
> "Production-Trace Replay" section here into
> `benchmarks/in-cluster/RUNBOOK.md` between the existing TPC-H and
> archive sections.

---

## What this harness does

For one operator invocation, the harness:

1. Generates a per-replica pin-list from `your_query_log_table` over the
   last `--window-days`, ranking Iceberg tables by
   `physicalInputBytes × queries`. Reuses `tools/gen_replay_list.py`
   verbatim — no SQL re-implementation, no boto3 dep added.
2. Runs a **cold-pass replay** (`vendor` records) against the shelf
   shim and the raw S3 endpoint. This is the "first scan after fresh
   cache" measurement.
3. Runs a **warm-pass replay** (`repeat` records) against both
   endpoints to surface the cache-warmth lift over the same trace.
4. Captures `/metrics` and `/stats` snapshots from every shelf-bench
   pod **before** and **after** the run via a sidecar curl pod —
   sidesteps the distroless-`kubectl exec wget` failure documented in
   [`benchmarks/results/2026-05-01/SUMMARY.md`](../results/2026-05-01/SUMMARY.md)
   §"Metric scrape gap".
5. Emits **four schema-valid JSON records** (shelf-vendor,
   shelf-repeat, raw-vendor, raw-repeat) under
   `<output-dir>/<backend>/replay-<phase>-<run_id>.json`, all valid
   against `benchmarks/replay/schema.json`.
6. Writes a side-by-side `summary.txt` mirroring the comparison shape
   from `benchmarks/results/2026-05-01/SUMMARY.md` so the post-run
   summary can be pasted into a release-cycle MR or status update
   without manual reformatting.

## Prerequisites

| Requirement | Why |
| --- | --- |
| Bench cluster up with a `shelf-bench-pool` StatefulSet | The shim endpoint the harness GETs against. |
| Trino coord reachable from the harness host | `gen_replay_list.py` needs `your_query_log_table` access via Trino REST. |
| `~/.cursor/mcp.json` carrying `mcp-trino` env vars (`TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_PASSWORD`) | `gen_replay_list.py` reads creds from there. Override path with `--mcp-json`. |
| Two bench-side Iceberg catalogs sharing one HMS | `--catalog-shelf` (`s3.endpoint=<shelf shim>`) for the cache path, `--catalog-raw` for the S3 baseline. |
| Permission to `kubectl exec` ephemeral pods in the shelf namespace | Sidecar `/metrics` scrape (skip with `--skip-scrape` if not available). |
| `python3` ≥ 3.9, stdlib only (no boto3 needed for the harness — the underlying `replay_pinlist.py` is pure stdlib). Optional `jsonschema` enables post-write schema validation. |

## One-line invocation

```bash
./benchmarks/scripts/run_prod_replay.sh \
  --window-days       7 \
  --output-dir        benchmarks/results/$(date -u +%F)/prodreplay \
  --shelf-endpoint    http://shelf-bench-pool.<your-namespace>.svc.cluster.local:9092 \
  --raw-endpoint      https://s3.<your-region>.amazonaws.com \
  --trino-host        trino-bench-coordinator.<your-namespace>.svc.cluster.local:8080 \
  --catalog-shelf     bench_iceberg_shelf \
  --catalog-raw       bench_iceberg \
  --replica           rep-2 \
  --top-n             200 \
  --prewarm-secs      1800 \
  --measurement-secs  43200    # 12 h. Default 7200 s = 2 h.
```

All endpoint values, namespace names, IAM role ARNs, and S3 bucket
names are **operator-supplied**; the harness ships zero site-specific
defaults.

## Expected runtime

| Phase | Wall-clock |
| --- | --- |
| Pin-list generation | 30–60 s (one Trino SELECT + N×2 system-table queries) |
| Pre-warm metric scrape | ~10 s (one curl pod, one round per shelf pod) |
| Cold-pass replay (vendor records) | `--prewarm-secs` (default 30 min) |
| Warm-pass replay (repeat records) | `--measurement-secs` (default 2 h; override to 12 h via `43200`) |
| Post-warm metric scrape | ~10 s |
| Record assembly + summary.txt | < 1 s |

A typical V1 run targeting the 12-hour soak window runs ~13 hours
end-to-end. Run inside `tmux` / `screen`; the harness writes records
to disk after each phase so a `SIGINT` between phases does not lose
prior work.

## Exit gates

A run is considered **green** when **all five** are true:

1. `<output-dir>/pinlist.json` is a non-empty JSON array and at least
   one entry per `--top-n` table is present.
2. `<output-dir>/{shelf,raw-s3}/replay-{vendor,repeat}-<run_id>.json`
   exist (4 files), each valid against
   `benchmarks/replay/schema.json` (run with `pip install jsonschema`
   for inline validation, or check via the bench `gate.py`).
3. `<output-dir>/summary.txt` exists and has both phase tables filled.
4. Both `<output-dir>/shelf-metrics/shelf-bench-N-metrics-pre.txt`
   AND `…-post.txt` exist for every pod (skipped only when
   `--skip-scrape` was passed).
5. The `shelf hit rate` row in the warm-pass summary is **≥ 80 %**.
   Below that, the pin-list almost certainly missed the working set —
   re-run `tools/gen_replay_list.py` with a wider `--top-tables`, or
   inspect `<output-dir>/_summary-shelf-vendor.txt` for the per-pool
   miss breakdown and tune accordingly.

## Sidecar curl pod — the one nontrivial bit

`shelfd` ships in `gcr.io/distroless/cc-debian12:nonroot`. There is no
shell, no `wget`, no `curl`. `kubectl exec shelf-X -- wget /metrics`
fails silently and returns a zero-byte file (caught on the
2026-05-01 cluster bench run; see SUMMARY.md §"Metric scrape gap").

`scrape_shelf_metrics.sh` works around this by:

1. Creating an ephemeral `curlimages/curl:8.10.1` pod
   (`restartPolicy=Never`) in the operator-specified namespace.
2. Waiting up to 60 s for the pod to become Ready.
3. For each `<pod-prefix>-N` (N = 0..pod-count-1), `kubectl exec`-ing
   curl against
   `http://<pod-prefix>-N.<service>.<ns>.svc.cluster.local:<metrics-port>/metrics`
   (and `/stats`) and writing the response to
   `<output-dir>/<pod-prefix>-N-{metrics,stats}-<phase>.{txt,json}`.
4. Tearing down the curl pod on every exit path including SIGINT
   (via `trap cleanup EXIT INT TERM`).

The orchestrator chose `kubectl` over the raw Kubernetes API client
because `kubectl` is the universal operator artefact — every operator
who can run the bench already has `kubectl` configured for the target
cluster, and re-implementing the watch / `exec` / port-forward
machinery against the API surface would add ~600 LOC for zero
operational benefit. If a future runner-image variant strips
`kubectl`, swapping in a stdlib HTTP client is a localised change in
`scrape_shelf_metrics.sh` only.

## Where to look when things go wrong

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `gen_replay_list.py` returns 0 rows | `your_query_log_table` is not the deployed name; mcp.json `TRINO_HOST` is stale; `--replica rep-N` doesn't match `environment` strings in the log table | Pass `--logs-table <fully-qualified>` if your event-listener writes elsewhere; verify with a manual `SELECT count(*) FROM <logs-table> WHERE environment='replica2' AND query_date >= current_date - interval '7' day` |
| `replay_pinlist.py` reports 100 % `error_other` | Shelf shim DNS not resolving from the harness host, OR firewall between operator laptop and `<service>.<ns>.svc` (k8s ClusterIP only resolvable from inside the cluster) | Run the wrapper from inside the cluster (`kubectl run -it --image python:3.12 …`), or set up a `kubectl port-forward svc/<service> 19092:9092` and pass `--shelf-endpoint http://localhost:19092` |
| Sidecar curl pod CrashLoopBackOff | Image pull blocked by network policy / private registry | Pre-pull the curl image into your private registry and pass `--curl-image <yours>` to `scrape_shelf_metrics.sh` (the wrapper hands it through) |
| Hit rate < 80 % on warm pass | Pin-list missed the working set (small tables, recent snapshot churn) | Re-run with `--top-n 500` or pass `--source grafana-mysql` as a sanity check on the per-replica query volume |
| `summary.txt` shows `shelf hit rate 0.0%` even on warm pass | Heuristic time-based classification mis-classifying everything as miss because the network RTT is high (e.g. operator running over VPN) | Run inside the cluster, or accept the classification skew and read `<output-dir>/_summary-shelf-repeat.txt` for the absolute outcome counts |

## Smoke test

```bash
./benchmarks/scripts/test_prod_replay.sh
```

Runs `bash -n`, `python3 -m py_compile`, `--dry-run` end-to-end with a
synthetic pin-list, and asserts CLI-arg validation. No live cluster
required; PASS/FAIL summary printed to stdout.
