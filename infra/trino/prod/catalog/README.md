# B1a — Per-coordinator shelf pinning (template)

Each Trino coordinator pod loads a **different** `cdp_shelf` catalog
file. Each file pins `s3.endpoint` to one specific shelfd pod via
Kubernetes headless-service DNS, deterministically preventing
kube-proxy ClusterIP round-robin from spreading one coordinator's
traffic across all shelfd pods.

> These files are an **OSS template**. The per-replica `s3.endpoint`
> binding is delivered via the `SHELF_S3_ENDPOINT` env var, populated
> from a private deployment repo (Helm values / ArgoCD app values /
> kustomize overlay). No operator-specific identifiers (HMS hostname,
> S3 bucket, IRSA role ARN, internal DNS suffix) belong in this repo.

## Mapping (suggested — adjust to operator's pod count)

| Trino coord pod ordinal | Catalog file | Shelfd target (env var `SHELF_S3_ENDPOINT`) |
| --- | --- | --- |
| 0 | `cdp_shelf-rep0.properties` | `http://shelf-0.shelf.<shelf-ns>.svc.cluster.local:9092` |
| 1 | `cdp_shelf-rep1.properties` | `http://shelf-1.shelf.<shelf-ns>.svc.cluster.local:9092` |
| 2 | `cdp_shelf-rep2.properties` | `http://shelf-2.shelf.<shelf-ns>.svc.cluster.local:9092` |
| 3 | `cdp_shelf-rep3.properties` | `http://shelf-(3 mod shelfd_pod_count).shelf.<shelf-ns>.svc.cluster.local:9092` |

Operators with a different number of replicas or shelfd pods adjust
the mapping in their private values file. The file *body* in this
repo is identical across replicas — the only thing that varies per
replica is the env var `SHELF_S3_ENDPOINT`.

## Why this matters

- **Without B1a**: `s3.endpoint=http://shelf.<shelf-ns>.svc.cluster.local:9092`
  resolves to all N shelfd pod IPs. Every split's range GET
  round-robins. The cache hit rate for any one coordinator's working
  set is `1/N` of what it could be — each shelfd pod holds only `1/N`
  of the hot bytes.

- **With B1a**: each coord's working set is concentrated on one shelfd
  pod. Steady-state hit rate is much closer to `1` (subject to
  mutual-exclusion of working sets across coordinators; tenants that
  share a working set still benefit from cross-coord co-location).

## Temporary workaround

This is **not** the correct end-state. SHELF-29 (client-side blob-cache
plugin in Trino) gives us proper HRW (Rendezvous hashing) over the
membership list, so any worker talks to the owner of any key. Track it
via [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184).

Until then B1a is the practical approximation. Once SHELF-29 lands,
drop these per-coord files and go back to a single
`cdp_shelf.properties` that points at the headless service — the
plugin does the routing.

## Apply (one replica at a time)

The actual rollout is two MRs:

1. **This repo (OSS).** Land the catalog template + runbook + verify
   script. Operators consume this as the source of truth for the
   *shape* of the catalog, the verify script, and the runbook.
2. **Your private deployment repo.** Add `cdp_shelf.properties` to the
   targeted replica's `catalogs:` block (body verbatim from the OSS
   template) and bind the env vars (`HIVE_METASTORE_URI`,
   `SHELF_S3_ENDPOINT`, `AWS_REGION`,
   `ICEBERG_PARTITION_FILTER_SCHEMAS`) from a Secret/ConfigMap. Your
   reconciler (ArgoCD / Flux) picks it up and rolls the coordinator +
   workers.

**Do not `kubectl patch` or `kubectl edit` the catalog ConfigMap
directly** if it is reconciled by ArgoCD / Flux — the reconciler will
revert the change on its next sync, and you will be left with a
partially-deployed coordinator that disagrees with its workers.

The earlier proposal to bind per-pod ConfigMaps via an init-container
patch (`statefulset-per-pod-catalog.patch.yaml`) was written for
deployments that consolidate replicas into a single 4-ordinal
StatefulSet. It does not match the more common multi-Helm-release
layout and is retained as a *future* design note.

## Secrets / auth

**Do not set `s3.aws-access-key` / `s3.aws-secret-key` in these files
or in the env vars they reference.** Each Trino coordinator/worker pod
should mount AWS credentials via IRSA (
`AWS_WEB_IDENTITY_TOKEN_FILE`); Trino's `fs.native-s3` client picks up
IRSA via the DefaultCredentialsProvider chain. shelfd's S3 shim
ignores the inbound `Authorization` header anyway (see
`shelfd/src/s3_shim.rs::handle_get_object`) and re-signs upstream with
its own IRSA.

Earlier revisions of these files included `${ENV:SHELF_S3_ACCESS_KEY}`
/ `${ENV:SHELF_S3_SECRET_KEY}` placeholders. They were a leftover from
when shelfd validated client signatures; that path was dropped in
SHELF-22. The placeholders have now been removed; static credentials
should never be needed for `cdp_shelf`.
