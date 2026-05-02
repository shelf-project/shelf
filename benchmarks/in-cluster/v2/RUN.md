# V2 — 12-hour production-trace replay bench

> Operator runbook for the V2 dispatch in
> [`shelf_rc.8_roadmap_*.plan.md`](../../..). One command, with all
> setup pre-staged (pin-list, cluster manifests, harness wiring). V2
> requires a live cluster and a 12-hour quiet window — the script
> stages everything else for you.

## What V2 measures

- **12-hour cold→warm trace**: replay last-7-day production query mix
  through a fresh `shelf-bench-pool` (3-pod StatefulSet, no shared cache
  with prod `shelf-pool`).
- **A/B against direct S3**: same trace runs against a parallel
  `bench_iceberg` catalog wired to direct S3, so latency / hit-rate
  deltas attribute cleanly to Shelf vs raw S3 (per the apples-to-apples
  pattern from
  [`benchmarks/results/2026-05-01/SUMMARY.md`](../../results/2026-05-01/SUMMARY.md)).
- **Hit-rate fidelity**: sidecar curl-pod scrape of every shelf-bench
  pod's `/metrics` + `/stats` before and after the run, so the cold→warm
  delta is a real measurement, not an inference (sidesteps the
  distroless-`kubectl exec wget` failure documented in the same SUMMARY).

The output is a `summary.txt` + four schema-valid records (shelf-vendor,
shelf-repeat, raw-vendor, raw-repeat) that V3 (verdict + ADR-0039) reads
to write the final go/no-go writeup.

## Prerequisites

| Requirement | Why |
| --- | --- |
| `kubectl` context = the bench cluster | `run.sh` `kubectl apply`s the manifests |
| Permission to `kubectl apply` namespaces, SAs, RBAC, ConfigMaps, Services, StatefulSets in `${BENCH_NAMESPACE}` | Render targets |
| AWS credentials with `s3:GetObject` + `s3:HeadObject` on `${ORIGIN_BUCKET}` | shelf-bench IRSA assumes a role that has these |
| The bench Trino is reachable at `${TRINO_HOST}` and has the V2 catalog ConfigMap mounted under `/etc/trino/catalog/` | The harness's two `--catalog-{shelf,raw}` paths require both catalogs to be loadable; see "Wiring the bench Trino" below |
| `envsubst` (gettext) installed | All manifests use `${PLACEHOLDER}` tokens |
| `python3` ≥ 3.9 | Pin-list smoke test + harness orchestration |
| (optional) `yq` ≥ 4 | Manifest validation; falls back to `python3 -c yaml.safe_load_all` if missing |

## One-command run

```bash
cd benchmarks/in-cluster/v2

# OPERATOR-PRIVATE — do NOT commit any of these to git.
export AWS_ACCOUNT_ID="123456789012"                                # your account
export IRSA_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/shelf-bench-s3"
export PROD_HMS_URI="thrift://hive-metastore-host:9083"             # per SUMMARY's prod-HMS pivot
export ORIGIN_BUCKET="my-bench-bucket"                              # NO s3:// prefix
export ORIGIN_REGION="us-east-1"
export TRINO_HOST="trino-bench-coordinator.trino-bench.svc.cluster.local:8080"

# Optional overrides (defaults in run.sh)
export BENCH_NAMESPACE="trino-bench"   # set to your shelf ns if reusing one
export SHELF_IMAGE_TAG="1.0.1"
export SHELF_BENCH_NVME_GIB="60"

./run.sh
```

That single invocation:

1. Renders all 7 manifests via `envsubst` into `${RENDERED_DIR}` (default
   `/tmp/v2-rendered/`).
2. Validates the rendered YAML (yq if present, python yaml otherwise).
3. `kubectl apply`s the manifests in dependency order: namespace, IRSA
   SA, RBAC, catalog ConfigMap, services, StatefulSet.
4. Waits up to 10 min for `sts/shelf-bench` rollout to complete (3 pods
   Ready). Each pod has up to 5 min Foyer NVMe-init grace via the
   `startupProbe`.
5. Hands off to
   [`benchmarks/scripts/run_prod_replay.sh`](../../scripts/run_prod_replay.sh)
   with a 12-hour `--measurement-secs` (43200), 30-min `--prewarm-secs`,
   the pre-staged `pin-list.json` as `--pinlist-override`, and the bench
   catalog FQDNs.
6. On exit (success OR failure), tears down via `99-cleanup.yaml` —
   leaves the namespace alone (you may have other workloads there) but
   removes every V2-specific resource.

Total wall clock: ~13 hours (30 min cold pre-warm + 12 hr warm
measurement + ~10 min cluster bring-up + ~1 min teardown).

## Abort & cleanup mid-run

The harness writes records to disk after each phase. A `Ctrl+C` runs
the cleanup trap (deletes the manifests, leaves results on disk).

For a force teardown without `Ctrl+C`:

```bash
envsubst < cluster-manifests/99-cleanup.yaml | kubectl delete --ignore-not-found -f -
```

The PVCs that back `nvme` are reclaimed per the storage-class default
(usually `Delete` for `ebs-gp3-wffc`); no manual disk cleanup needed.

## Wiring the bench Trino

V2 ships only the **catalog ConfigMap** at
`cluster-manifests/04-bench-trino-catalogs.yaml`. The bench Trino is
operator-supplied — you point it at this ConfigMap one of two ways:

1. **Helm-managed bench Trino** (using the upstream `trino/trino`
   chart): in your values file, add the `bench_iceberg` and
   `bench_iceberg_shelf` catalogs as `additionalConfigFiles` referencing
   the keys in this ConfigMap, e.g.:
   ```yaml
   coordinator:
     additionalConfigFiles:
       /etc/trino/catalog/bench_iceberg.properties:
         configMapKeyRef:
           name: bench-trino-catalogs
           key: bench_iceberg.properties
       /etc/trino/catalog/bench_iceberg_shelf.properties:
         configMapKeyRef:
           name: bench-trino-catalogs
           key: bench_iceberg_shelf.properties
   ```
2. **Direct kubectl coord pod patch**: mount the ConfigMap as a volume
   under `/etc/trino/catalog/` in the coord pod spec.

After either, **restart the coordinator** — Trino parses catalog
.properties files at coord startup, NOT hot-reload.

## Pin-list

`pin-list.json` was pre-generated against `cdp.trino_logs.trino_queries`
(last 7 days, top-100 tables ranked by `physical_input_bytes × queries`,
filtered to `environment='replica0'` since rep-0 carries the heaviest
load and yields the richest table coverage for a fresh shelf cache).
See `pin-list.summary.txt` for the per-table breakdown.

If the pre-staged pin-list is stale (e.g. days-old, or the workload mix
has shifted), re-generate via:

```bash
python3 ../../tools/gen_replay_list.py \
  --replica rep-0 \
  --catalog cdp \
  --days 7 \
  --top 5000 \
  --top-tables 100 \
  --logs-table cdp.trino_logs.trino_queries \
  --out ./pin-list.json
```

The script reads Trino REST creds from `~/.cursor/mcp.json`'s `mcp-trino`
block (per `gen_replay_list.py`'s docstring); no creds on argv.

## Expected output

```
benchmarks/results/<date>/prodreplay/
├── pinlist.json                                # symlink/copy of v2/pin-list.json
├── shelf/
│   ├── replay-vendor-<ULID>.json               # cold-pass shelf record
│   └── replay-repeat-<ULID>.json               # warm-pass shelf record
├── raw-s3/
│   ├── replay-vendor-<ULID>.json               # cold-pass raw-S3 record
│   └── replay-repeat-<ULID>.json               # warm-pass raw-S3 record
├── shelf-metrics/
│   ├── shelf-bench-{0,1,2}-metrics-pre.txt     # Prom text-format /metrics
│   ├── shelf-bench-{0,1,2}-metrics-post.txt
│   ├── shelf-bench-{0,1,2}-stats-pre.json
│   └── shelf-bench-{0,1,2}-stats-post.json
└── summary.txt                                 # V3 reads this for VERDICT.md
```

All four `replay-*.json` files are valid against
`benchmarks/replay/schema.json` (run with `pip install jsonschema` for
inline validation).

## Exit gates V3 evaluates

| Gate | Threshold | Source |
| --- | --- | --- |
| Pin-list non-empty | `n_entries ≥ 1` | `pinlist.json` |
| All 4 records exist + schema-valid | `4/4` | `<output-dir>/{shelf,raw-s3}/*.json` |
| Sidecar scrape complete | 6 metrics files + 6 stats files | `<output-dir>/shelf-metrics/` |
| Hit ratio (warm pass) | ≥ 80 % | `summary.txt` |
| Origin bytes saved (warm pass) | ≥ 50 % vs cold | `summary.txt` |
| p95 wall (shelf vs raw, warm) | shelf ≤ 1.20 × raw OR cite Foyer-cold caveat | `summary.txt` |

## Failure-mode quick reference

| Symptom | Likely cause | Fix |
| ------- | ------------ | --- |
| `run.sh` exits 2 with "X must be exported" | One of the 6 required env vars is unset | Re-read the "One-command run" section above |
| `kubectl apply` rejects the manifest with "namespaces 'X' not found" | First-time install on a fresh cluster — `00-namespace.yaml` is applied first by the script, but if you run apply manually, do it in order | Use `run.sh` (which applies in the right order) |
| `shelf-bench-0` `OOMKilled` (exit 137) | Pod limit ≤ allocatable on c-family node | Drop c6a from your Karpenter NodePool; keep `m5a/m6a/m7a/r-family` 4xlarge only. Pod limit is 40 GiB; m-family 4xlarge has ~57 GiB allocatable, c6a only ~27 GiB (compute-optimised, 8 GiB RAM/vCPU). |
| `sts/shelf-bench` rollout times out | PVC binding stuck (StorageClass missing / wrong AZ) | `kubectl -n ${BENCH_NAMESPACE} get pvc` → if Pending, your default StorageClass isn't `ebs-gp3-wffc`. Override `02-shelf-bench-statefulset.yaml`'s `storageClassName` to whatever your cluster uses. |
| Harness reports `error_other` 100 % | Shelf shim DNS not resolving (operator running outside cluster) OR NetworkPolicy too tight | Run inside the cluster (the SA + RBAC in `05-curl-pod-rbac.yaml` lets you `kubectl run -it --image python:3.12 …`); or `kubectl port-forward svc/shelf-bench-pool 19092:9092` and re-invoke the harness with `--shelf-endpoint http://localhost:19092` |
| `iceberg.metadata-cache` not flipped on `bench_iceberg_shelf` | The catalog ConfigMap was mounted but coord wasn't restarted | `kubectl rollout restart deploy/<bench-trino-coord>` (catalog props are parsed at coord start, NOT hot-reload) |
| `summary.txt` shows `shelf hit rate 0.0%` even on warm pass | Heuristic time-classification mis-classifying everything as miss because RTT is high (operator ran over VPN) | Run inside the cluster, or read `<output-dir>/_summary-shelf-repeat.txt` for absolute outcome counts |

## See also

- [`benchmarks/scripts/RUNBOOK.md`](../../scripts/RUNBOOK.md) — V1
  prod-replay harness operator playbook (the harness V2 hands off to)
- [`benchmarks/results/2026-05-01/SUMMARY.md`](../../results/2026-05-01/SUMMARY.md) —
  the cluster bench from May 1 that surfaced the prod-HMS pivot, the
  capacity-engineering rule (40 GiB pod limit on m-family, NOT c6a),
  and the metric-scrape gap that V2's sidecar curl pod sidesteps
