# B1 — row-group zstd compression

> **Status**: shipped in `1.0.0-rc.3` (under `[Unreleased]` until tagged).
> **Owner**: shelfd
> **Related**: ADR-0008 (two-pool architecture), ADR-0009 (rowgroup hybrid pool), `shelfd/docs/design-notes/SHELF-E2-zstd-metadata.md` (metadata-pool sibling).

## TL;DR

`cache.pools.rowgroup.compression.enabled = true` zstd-encodes row-group payloads before they reach Foyer. Real Iceberg / Parquet rowgroup data compresses ~ 1.4–2.5× under zstd-3 because the columnar payloads are already dictionary-encoded but the per-page headers + repeated string values still win meaningfully. The same NVMe budget therefore holds 1.4–2.5× more keys ⇒ **+5–10 pp hit ratio** at constant pod count, **or** equivalent S3-spend savings at constant hit ratio (the StatefulSet shrinks by one pod, ~$200/mo per replica on the alluxio NodePool).

The pipeline is opt-in, runtime-toggled, and observable. **Flipping the knob on a populated NVMe ring without wiping it is unsafe**; the boot path enforces this with a `.shelf-compression.json` marker file that aborts loud-and-early on a mismatched config.

## Why a separate pipeline (not the metadata pool's `zstd_metadata` feature)

The legacy `zstd_metadata` Cargo feature was scoped to the metadata pool (~5× ratio on JSON manifests) and gated by build flag because the metadata pool is DRAM-only — flipping it costs at most one pod restart. The rowgroup pool is hybrid: NVMe persistence means a config flip must be reconcilable on disk, which a build flag cannot enforce. B1 therefore introduces a runtime config knob and reuses the existing `compression::encode` / `decode` pure functions through a new pool-agnostic [`CompressionPipeline`](../../src/compression.rs) helper. The same helper can be wired into the metadata pool in a follow-up to retire the build flag entirely.

## Data flow

```
Origin GET (S3) → bytes → admission gate → encode_for_store()
                                          │
                                          │   header byte +
                                          │   zstd-3 frame
                                          ▼
                                    Foyer DRAM tier
                                          │   (S3-FIFO / LRU promotion)
                                          ▼
                                    Foyer NVMe tier
                                          │
                                          ▼
                                  cache.get() → bytes
                                          │
                                          ▼
                                  decode_from_store() → original payload
```

Encode and decode each touch one Prometheus histogram observation + a counter; on a 32 MiB rowgroup with zstd-3 decode at ~500 MB/s/core, that's ~64 ms of CPU and three counter increments. Real workloads see far less (median rowgroup is < 8 MiB).

## Frame format

Every stored byte stream starts with a single version byte:

| Byte 0 | Meaning                          | Body                            |
|--------|----------------------------------|---------------------------------|
| `0x00` | uncompressed (skipped or legacy) | raw payload, unchanged          |
| `0x5A` | zstd frame                       | standard zstd frame, no wrapper |
| other  | corrupt — boot must have aborted | undefined                       |

`0x00` covers two cases: payloads below `min_size_bytes` (default 256 B), and payloads where zstd inflated rather than shrunk the byte count. Both are returned to the cache verbatim — we never inflate.

## On-disk safety: the marker file

Foyer's `DirectFsDevice` lays region files immediately under `nvme_dir`. There is no Foyer-level "format" indicator we can piggyback on; mixing post-flip header-tagged frames with pre-flip raw Parquet bytes is undecidable from byte 0 alone (real Parquet content can land on `0x00` or `0x5A` arbitrarily). We therefore write a `<nvme_dir>/.shelf-compression.json` marker:

```json
{
  "version": 1,
  "descriptor": "zstd@3",
  "min_size_bytes": 256
}
```

`FoyerStore::ensure_compression_marker` runs **before** any Foyer pool is constructed and enforces:

| `nvme_dir`     | marker | config compression | result                    |
|----------------|--------|--------------------|---------------------------|
| empty          | —      | on                 | write marker, proceed     |
| empty          | —      | off                | proceed                   |
| has payload    | matches| on                 | proceed                   |
| has payload    | mismatches `descriptor` | on  | **abort** with "wipe NVMe to switch compression mode" |
| has payload    | present| off                | **abort** ("ring was written with `<descriptor>`") |
| has payload    | missing| on                 | **abort** ("pre-existing uncompressed data") |
| has payload    | missing| off                | proceed (legacy state)    |

The error message names the directory + the offending descriptors so the operator can act without log archaeology.

## Switch playbook

1. `kubectl scale statefulset shelf -n alluxio --replicas=0`
2. `kubectl exec -it <maintenance pod> -- rm -rf /var/cache/shelf/*` against each PVC
3. Toggle `cache.pools.rowgroup.compression.enabled` in `infra/penpencil/charts/shelf/values-prod.yaml`
4. `helm upgrade shelf charts/shelf -f infra/penpencil/charts/shelf/values-prod.yaml -n alluxio`
5. `kubectl scale statefulset shelf -n alluxio --replicas=4`

The cluster cold-starts compressed; in-flight queries fail-over to direct S3 per ADR-0010 §Fail-open.

## Observability

The four series in `metrics.md` (B1 rows) feed three live panels in the `shelf-overview` dashboard:

- **Compression ratio (live)** — `1 - rate(shelf_compress_bytes_out_total[5m]) / rate(shelf_compress_bytes_in_total[5m])`
- **Encode/decode p99** — `histogram_quantile(0.99, sum by (pool, op, le)(rate(shelf_compress_seconds_bucket[5m])))`
- **Outcome split** — stacked area on `sum by (outcome)(rate(shelf_compress_outcomes_total[5m]))`

Sustained `decompress_error` rate > 0 is a signal that someone bypassed the marker check (e.g. mounted a foreign PVC); on-call should respond by scaling the pod to 0 and inspecting `nvme_dir` directly.

## Performance budget

zstd-3 decode is ~500 MB/s/core (single-thread). At 60 MB/s/pod sustained read throughput on rep-1's heaviest hour, that's < 0.12 cores of decode CPU per pod. Encode is ~150 MB/s/core; at the same 60 MB/s/pod, ~0.4 cores of encode CPU per pod under sustained ingress. The 32 MiB worst-case rowgroup adds < 80 ms encode latency on the miss path, dwarfed by S3 GET p50 (~ 25 ms) + p95 (~ 200 ms). The encode happens *after* the bytes have already been served back to Trino (it lives on the admission seam), so it never blocks user-visible latency.

## Out of scope (deferred)

- **Metadata-pool wiring** — keeps the legacy `zstd_metadata` feature gate. A follow-up will swap the wiring to `CompressionPipeline` and retire the gate.
- **lz4** as an alternative algo — `compression.algo` is hard-coded to zstd in v1; the config struct has room for it but no plumbing.
- **Per-key compression hints** (e.g. metadata.json always compresses well, raw Parquet may not). The `skipped_incompressible` outcome already short-circuits the bad cases, so the gain from a hint table is < 1 % of bytes.
