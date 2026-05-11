# MR template — rep-0 / rep-3 catalog tuning (mirror of rep-1 MR !17967)

**Status:** DRAFT — pre-staged by T10 of `analyst_report_validation_rc9_plan_de82494e.plan.md`. Do NOT merge until the cutover decision is taken (rep-0 today is on direct S3; rep-3 is the explicit rollback escape hatch — do NOT cut over rep-3 until rep-0 has 30 clean days first per workspace lifeboat rule).

**Scope.** Mirror the 5 cost-neutral Trino 480 catalog properties that landed on rep-1 via deployments-repo MR `!17967` (May 1) onto:
- `values-files/data-platform-cluster/trino-replica-0-values.yaml` — the `cdp` catalog block ONLY (NOT `cdp_curated`)
- `values-files/data-platform-cluster/trino-replica-3-values.yaml` — same scope

**This MR does NOT change `s3.endpoint`** — that's the cutover line and is a separate decision. This MR is structurally cost-neutral (per the standing rep-N tuning invariant: cost-neutral-or-cost-down is the gate; performance must be fast).

## Properties to add

```properties
# rep-1 MR !17967 mirror — all 5 validated against Trino 480 source
# (io.trino.plugin.iceberg.IcebergConfig, io.trino.parquet.ParquetReaderConfig,
#  io.trino.filesystem.s3.S3FileSystemConfig)

iceberg.metadata-cache.enabled=false
iceberg.dynamic-filtering.wait-timeout=3s
s3.max-connections=256
parquet.small-file-threshold=8MB
s3.tcp-keep-alive=true
```

## Per-property cost / latency justification (required in MR body)

| Property | Cost delta | Latency delta | Why |
|---|---|---|---|
| `iceberg.metadata-cache.enabled=false` | ↓ small | ↓ large (when shelf is fronting) | Forces every Iceberg metadata read past Trino's JVM-local `MemoryFileSystemCache` and into shelfd's metadata pool, where it is hot. On rep-1 this drove metadata-pool req-rate **7.95 → 250 ops/s** (31× more reads bypass the JVM cache). With shelf NOT yet fronting (rep-0 today), this is **cost-neutral** because the metadata reads still go to S3 either way — but it's pre-staged for the day shelf is enabled. |
| `iceberg.dynamic-filtering.wait-timeout=3s` | ↓ via reduced wasted scans | ↓ tail | Caps the planner's wait for build-side dynamic filters at 3s instead of the default 20s — shaves p99 wall on JOIN-heavy queries (50.6% of rep-1 traffic) where the build side is slow. `IcebergDynamicFilteringConfig.dynamicFilteringWaitTimeout`. |
| `s3.max-connections=256` | neutral | ↓ tail | Raises the native-S3 client connection pool from default 50 → 256, eliminating "Timeout waiting for connection from pool" tails under burst load. `S3FileSystemConfig.maxConnections`. |
| `parquet.small-file-threshold=8MB` | ↓ slightly | ↓ p50 | Trino merges range reads to small Parquet files into one GET when the file is below the threshold, cutting per-row-group HTTP overhead. `ParquetReaderConfig.smallFileThreshold`. |
| `s3.tcp-keep-alive=true` | neutral | ↓ tail | Keeps idle S3 connections warm across the shelf-shim proxy hops; otherwise a 60s-idle close incurs a TLS round-trip on the next read. `S3FileSystemConfig.tcpKeepAlive`. |

**Aggregate: cost-neutral-or-down on every property; latency strictly down or neutral.** Passes the user's standing rep-N tuning gate.

## Rollback signal table (required in MR body per workspace cutover-MR rule)

| Trigger | Threshold | Action |
|---|---|---|
| `ICEBERG_INVALID_METADATA` rate | > 0.5% / 5 min | Revert MR |
| `ICEBERG_CANNOT_OPEN_SPLIT` rate | > 0.5% / 5 min | Revert MR |
| p99 wall regression | > 50% sustained 10 min | Revert MR |
| Hit ratio (cluster aggregate, mimir-data `shelf_hits_total / total`) | < 30% sustained 60 min after flip | Revert MR (only applicable if shelf is also fronted on this MR) |
| Any pod RSS > 24 GiB sustained 5 min OR any OOMKill | (instant) | Revert MR + capacity-engineering follow-up (workspace OOM cascade RCA May 1) |
| `shelf_lodc_drops_total{reason="submit_queue_overflow"}` | > 2× baseline sustained 5 min | Revert MR + tune RowGroupDiskCacheConfig |

## Pre-merge checklist

- [ ] Properties validated against Trino 480 `Config` classes via `unzip -p trino-server.jar | javap` (workspace validation discipline — do NOT trust blog-post property names; `parquet.optimized-reader.enabled` was REMOVED in 424 and will silently no-op)
- [ ] Audit covers `config.properties`, `jvm.config`, `event-listener.properties`, KEDA scaler section — NOT just the catalog block (workspace audit-scope rule)
- [ ] Per-property cost-neutrality acked by user (the standing invariant)
- [ ] Coordinator rolling-restart wall-clock noted: ~2 min deployments-repo reconcile + ~2 min `kubectl rollout restart deploy/trino-replica-N-coordinator` Ready-wait. **Manual restart is REDUNDANT** — workspace memory: deployments-repo helm-reconcile auto-rolls the coordinator on existing-key catalog change (verified May 1 on MR !17967).
- [ ] Verify post-restart: `kubectl -n trino-db exec <coord-pod> -- cat /etc/trino/catalog/cdp.properties | grep -E '(metadata-cache|tcp-keep-alive|max-connections)'`

## Operator notes

- **Auto-rollback authority**: per the rep-1 May 1 reverification rule, watcher posture on rep-3 is **conservative — halt and surface, do NOT auto-merge revert MR** unless the user explicitly re-authorizes for the window. Default for rep-0 is also conservative until the user re-authorizes (rep-0's prior auto-rollback authority was per-cutover, not standing).
- **Soak window**: 90 min minimum, with the 7 rollback triggers above armed. Recompute the rep-0 / rep-3 7-day workload shape from `cdp.trino_logs.trino_queries` first — rep-0/rep-3 carry materially different shapes from rep-1 and the cost/latency expectations vary accordingly.
