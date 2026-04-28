# Architecture

This page is the **user-facing summary** of Shelf's architecture. The
canonical, exhaustive design document is
[`BLUEPRINT.md`](../BLUEPRINT.md) at the repo root, with later
overrides captured as [ADRs](../agents/out/adr/). When this page and
the blueprint disagree, the blueprint wins; file an issue so we can
fix this page.

## 30-second mental model

```
┌──────────────┐         ┌──────────────────────┐         ┌──────────┐
│   Trino 480  │ S3 API  │       shelfd         │ S3 API  │    S3    │
│ Iceberg cat. │────────▶│  (Rust, one process  │────────▶│ (MinIO,  │
│ s3.endpoint= │         │   per cache node)    │         │   AWS)   │
│  shelfd:9092 │◀────────│                      │◀────────│          │
└──────────────┘         │  ┌──────────────┐    │         └──────────┘
        ▲                │  │ Foyer hybrid │    │
        │ event listener │  │  cache       │    │
        │ (prefetch)     │  │  (RAM+NVMe)  │    │
        └────────────────┤  └──────────────┘    │
                         │  Prometheus /metrics │
                         └──────────────────────┘
```

**Three properties** that distinguish Shelf from "just another S3
proxy":

1. **Row-group granular.** Cache keys are
   `sha256(etag || offset || length || ordinal)` — the Parquet footer
   (64 KB) and each row group (~4 MB) are their own cache entries.
   See [ADR 0011](../agents/out/adr/0011-shelf04-key-is-sha256-etag-offset-length-ordinal.md).
2. **Plan-aware prefetch.** A Trino coordinator plugin warms
   file-level and footer bytes during split assignment. Row-group
   prefetch is plugin-observation-driven; we deliberately don't
   depend on `SplitCompletedEvent` (upstream removed it in PR
   #26436 — see [ADR 0005](../agents/out/adr/0005-drop-splitcompleted-event-path.md)).
3. **Consensus-free multi-node.** Membership = the K8s headless
   service. Pin list + tenant quotas = versioned S3 ConfigMap.
   No Raft, no etcd. See [ADR 0001](../agents/out/adr/0001-no-embedded-raft.md)
   and [ADR 0002](../agents/out/adr/0002-hrw-hashing-over-vnode-ring.md).

## Read path

Today (v0.x — Phase 1 of ADR 0012):

```
Trino native S3 client
  ├── HEAD s3://warehouse/default/t/metadata/123.json
  │    → routed to http://shelfd:9092 (s3.endpoint)
  │    → shelfd hashes etag/offset/length → Foyer lookup
  │    → miss → fetch from real S3 → populate Foyer → respond
  └── GET  s3://warehouse/default/t/data/0.parquet  Range: bytes=100-200
       → same flow; row-group byte range is the cache key
```

There is **no forked Trino, no JAR install, no JVM property**
required. The integration point is a single catalog property:
`s3.endpoint=http://shelfd:9092`. See
[ADR 0012](../agents/out/adr/0012-trino-read-path-endpoint-swap-then-blob-cache-spi.md).

## Cache internals

- **Engine**: [Foyer](https://github.com/foyer-rs/foyer) 0.19 with the
  built-in S3-FIFO admission policy
  ([ADR 0009](../agents/out/adr/0009-foyer-s3-fifo-over-gl-cache-custom.md)).
- **Two pools** by default — `metadata` (in-memory only, small)
  and `rowgroup` (RAM + NVMe-backed, large). See
  [ADR 0008](../agents/out/adr/0008-two-pools-in-v1.md) and
  `shelfd/docs/design-notes/SHELF-18-nvme-hybrid-pool.md`.
- **Metrics**: `shelf_hits_total`, `shelf_misses_total`,
  `shelf_disk_hits_total`, `shelf_disk_misses_total`,
  `shelf_upstream_bytes_total`. Sorted by `pool` label
  (`metadata` / `rowgroup`). See
  `shelfd/docs/design-notes/SHELF-08-observability.md`.

## Write path

There isn't one. Shelf is a **read cache, not a write-through cache**.
Iceberg writes go directly to S3/MinIO from Trino or whichever writer
produced the Parquet files; Shelf only observes reads. This keeps the
durability story simple: at worst, a Shelf outage causes reads to
fall through to S3 (slower, not wrong); writes are never affected.

## Out-of-process / plugin layout

```
clients/trino/    — the Trino plugin (event listener today;
                    blob-cache-manager when #29184 lands)
shelfd/           — the Rust service
shelfctl/         — operator CLI (pin list, smoke checks)
charts/           — Helm chart for k8s deploy
benchmarks/       — smoke, replay, and trino-logs harnesses
SECURITY/         — threat model, IAM, supply chain
```

## What's not here yet

Everything the BLUEPRINT calls v1+ is deferred until the v0.5 gate
([ADR 0010](../agents/out/adr/0010-v05-gate-beat-alluxio-on-rep2.md)):
learned admission, Arrow Flight, side-built blooms (§7.4.2),
z-order awareness (§7.4.3), MV-aware caching. ADRs explain the
sequencing.

## Further reading

- [BLUEPRINT.md](../BLUEPRINT.md) — full canonical design
- [COMPARISON.md](../COMPARISON.md) — Alluxio / JuiceFS / Warp Speed
- [ADR index](../agents/out/adr/) — all architectural decisions to date
- [docs/runbook.md](./runbook.md) / [docs/oncall.md](./oncall.md) —
  operational surface
