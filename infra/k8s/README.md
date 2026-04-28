# infra/k8s

Cluster-side glue (cronjobs, prewarm jobs, advisor schedules) is deployment-
specific and intentionally kept out of the OSS repository. The Helm chart in
[`charts/shelf/`](../../charts/shelf/) is the reusable, distributable
contract; everything else (which bucket holds the pin list, which IRSA role
your shelfd service account assumes, which Trino namespace is allowed to
ingress, where the metastore lives) is environment-specific glue that
belongs in your own deployments repository.

## What goes in your own deployments repo

Per-cluster manifests for:

- **`hms-poller`** — Hive metastore notification → pin-list refresher.
- **`shelf-advisor`** — nightly query-log analyser that emits MV
  candidates.
- **`mv-pin-watcher`** — adds dbt-emitted MVs to the pin list once they
  start showing up in Iceberg.
- **`prewarm`** — one-shot pre-cutover prewarm against a per-replica
  trace.
- **`planner-warmup`** — keeps Trino's coordinator metadata cache warm.

The Python entry points for each of those tools live in
[`tools/`](../../tools/) at the repository root and are designed to be
runnable as plain CLIs. To deploy them on Kubernetes, copy the Python
scripts into your own image (or build a thin wrapper image) and define
your own `CronJob`/`Job` manifests with the bucket names, IRSA role ARN,
namespace, and image registry that match your cluster.

A worked example wiring `tools/gen_pin_list.py` into a CronJob is
documented under
[`docs/runbook.md`](../../docs/runbook.md#nightly-pin-list-refresh).
