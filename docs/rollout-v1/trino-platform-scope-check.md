# trino-platform scope check — blast radius of the `iceberg.properties` endpoint flip

**Status**: pending — blocks rep-2 cutover at T-24h.
**Owner**: trino-platform.
**Requested by**: shelf-core (rollout-v1 pre-req).
**Expected turnaround**: 1 business day.

## Why we need this

The rollout flips each replica's `iceberg.properties` so that the
Iceberg catalog's native-S3 client talks to shelfd instead of S3
directly:

```diff
- s3.endpoint=https://s3.ap-south-1.amazonaws.com
+ s3.endpoint=http://shelfd.shelf.svc.cluster.local:9092
```

(And analogous `s3.path-style-access=true` so the shim's `/bucket/key`
addressing works; shelfd doesn't do virtual-hosted-style.)

This change is scoped to **one catalog file per replica**.
What we need trino-platform to confirm is:

1. **No other catalog** in that replica's `etc/catalog/` directory
   contains a `s3.endpoint` that already points at the same S3
   region/bucket as the Iceberg catalog and would also be picked
   up if someone misread the PR and applied it to the wrong
   file.
2. **No catalog-coordinator sharing** of the S3 client at the
   JVM layer that would unexpectedly route a non-Iceberg catalog
   through shelfd just because Iceberg's config changed.
3. **No non-Iceberg table** (Hive, Delta, Hudi) lives in the
   same bucket as Iceberg and has an overlapping-key collision
   with the shim's addressing.

## What we're asking for

### 1. Catalog inventory per replica

For each of rep-0, rep-1, rep-2, rep-3, a listing of
`etc/catalog/*.properties` on the coordinator pod:

```bash
# trino-platform can run this on each replica's coordinator
kubectl -n trino-db exec trino-rep-${R}-coordinator-0 -- \
  ls /etc/trino/catalog/
```

For each catalog, we need to know:

- `connector.name` (the catalog type)
- `s3.endpoint` (if present) — this is the field the rollout PR touches
- `hive.metastore.uri` / `iceberg.rest-catalog.uri` / analogous —
  confirms where the metadata lives
- The warehouse bucket the catalog reads from

Compact form: a Markdown table per replica, one row per catalog file.

### 2. S3-client sharing confirmation

Trino 480's Iceberg connector uses the
`fs.native-s3.enabled=true` native-S3 client (see
[smoke `iceberg.properties`](../../benchmarks/smoke/config/trino/etc/catalog/iceberg.properties)
line 23 — we mirror your production pattern). The concern: does
the native S3 filesystem factory **share** its configuration
across catalogs, or is each catalog's `s3.*` config isolated?

Our reading of `trino/lib/trino-filesystem-s3/` is that each
catalog gets its own
`S3FileSystemFactory` instance with its own `S3Client` — the
config is per-catalog. We need trino-platform to confirm this for
our specific Trino 480 deployment; in particular, confirm there
is **no shared S3 client cache** keyed on region or endpoint that
would cause a config read for one catalog to leak into another.

### 3. Bucket / key-namespace collision check

The shim at `http://shelfd:9092` accepts paths of the form
`/{bucket}/{key}`. It is bucket-agnostic — it caches whatever
key the client asks for. If a non-Iceberg catalog is already
pointed at the same bucket (e.g. a Hive catalog reading
`s3://penpencil-cdp-prod/hive-warehouse/...`), its traffic does
not touch shelfd **unless the catalog's own `s3.endpoint` is
also flipped**. Our PR only touches the Iceberg file, so the
Hive catalog continues to read via its configured endpoint
(direct S3 or whatever).

We need trino-platform to confirm:

- The list of catalogs in `trino-db` that read from
  `penpencil-cdp-prod` (the Iceberg warehouse bucket)
  regardless of catalog type.
- For each, confirm its `s3.endpoint` is not
  `http://shelfd:9092` currently (we're not double-writing) and
  will not be after our PR.

## Rollout PR template (what trino-platform will review)

We'll ship one PR per replica to the `trino-db` manifest repo.
Indicative diff for rep-2:

```diff
--- a/trino/rep-2/etc/catalog/iceberg.properties
+++ b/trino/rep-2/etc/catalog/iceberg.properties
@@ -1,8 +1,12 @@
 connector.name=iceberg
 iceberg.catalog.type=rest
 iceberg.rest-catalog.uri=http://iceberg-rest.trino-db:8181
 iceberg.rest-catalog.warehouse=s3://penpencil-cdp-prod/iceberg-warehouse/
 iceberg.file-format=PARQUET

+fs.native-s3.enabled=true
-s3.endpoint=https://s3.ap-south-1.amazonaws.com
+s3.endpoint=http://shelfd.shelf.svc.cluster.local:9092
 s3.path-style-access=true
 s3.region=ap-south-1
+# SHELF-27a: tag requests with the originating Trino replica.
+s3.headers=x-shelf-client-replica:rep-2
```

(Exact before-state will be whatever production currently has —
trino-platform to inform us if there's surface we're not
accounting for like `s3.max-connections`, `s3.signer-type`, or
IRSA role annotations that need to survive the rollout.)

No change to the **REST catalog** endpoint — that's
`iceberg.rest-catalog.uri`, a separate config surface, and it
continues to talk to the REST catalog directly for metadata
mutations. Only the **filesystem-layer** reads (Parquet byte-
ranges, manifest files) route through shelfd.

## Known non-blockers (listed to save a round-trip)

- **IAM / IRSA**: shelfd uses its own IRSA role for S3 access.
  Trino's IRSA role continues to exist on the pod but isn't used
  for Iceberg reads post-rollout. Trino's role still covers the
  REST catalog mutations.
- **Access logs**: S3 access logs for the Iceberg warehouse bucket
  will drop to near-zero once all four replicas cut over; Shelf's
  access logs replace them. If there's a SOC-2 or audit requirement
  that specifically requires S3-server-side access logs, flag it.
- **VPC endpoint cost**: shelfd continues to read from S3 via the
  existing VPC endpoint in the same region; total S3 GET request
  count drops (that's the point). No ENI / TGW change.

## Delivery

Post the catalog inventory (the tables under #1) and confirmations
for #2 and #3 in the shelf-core Slack channel. The expected
"green" reply is all three numbered items answered with no
surprises.

## Escalation

If the inventory reveals something unexpected — a Hive catalog
pointed at the Iceberg warehouse, say, or a shared S3 client
cache — ping shelf-core and we re-scope the PR template before
any rep-2 work. We'd rather delay 24 h than cut over with a
miscounted blast radius.
