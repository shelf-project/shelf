# SHELF-45: Compaction-aware re-warm reactor

**Status:** Draft
**Tier:** S
**Estimated effort:** M
**Depends on:** none
**Blocks:** none

## Problem (OSS-cited)

Every Iceberg-on-Trino shop with nightly `EXECUTE optimize` / `expire_snapshots` eats a 100 % miss morning: the cache was warmed for *yesterday's* files; today's `Snapshot.summary["operation"]="replace"` rewrites every path with a new ETag. Content-addressed keys (per ADR-0011) correctly invalidate but leave the cache cold. Apache Iceberg [Maintenance docs](https://iceberg.apache.org/docs/latest/maintenance/) describe the lifecycle; [Alex Merced's metadata-bloat post (Jul 2025)](https://iceberglakehouse.com/posts/iceberg-metadata-bloat-cleanup/) shows operational impact. Mountpoint-S3 issue [awslabs/mountpoint-s3 #631](https://github.com/awslabs/mountpoint-s3/issues/631) ("scripted prefetch") is still open. **Alluxio EE only invalidates after compaction; no OSS cache re-warms.**

## Goal

When a watched Iceberg table commits a snapshot whose `summary["operation"]` is `replace` (i.e. compaction / rewrite_data_files), `shelfd` automatically re-warms the matching new files for any old file that was hot in the cache, before the next morning's queries hit them cold.

## Approach

New module `shelfd/src/compaction_watcher.rs` (or extend the existing `shelfd/src/freshness.rs` if the snapshot watcher already lives there). Reactor loop:

1. Subscribe to snapshot transitions for tables in the pin list (and tables tagged "hot" by SHELF-38). Source: poll `metadata.json` via `shelfd`'s S3 origin client every 30 s (default), or subscribe to a SQS / SNS feed if configured.
2. On a new snapshot whose `summary["operation"] == "replace"`, fetch the manifest list and diff `removed_data_files` vs `added_data_files`.
3. Cross-reference `removed_data_files` against the live cache (`shelfd/src/store.rs::contains(key)`).
4. For each match: enqueue `Prefetch(new_file, FOOTER+PAGE_INDEX)` against the existing prefetch worker. Optionally re-warm row groups whose `lower_bounds`/`upper_bounds` cover the same predicate as the most recent N hits against the old file (read recent-hit predicate from the SHELF-37 log table or an in-process LRU of "what predicates hit this file last 24 h").
5. Cap per-snapshot re-warm bytes (default 5 GiB) and per-prefix concurrency (default 32 inflight S3 GETs) to avoid thundering-herd.

Schema of the re-warm record (Prom counters + audit log):
- `shelf_compaction_rewarm_files_queued_total{table}`
- `shelf_compaction_rewarm_bytes_total{table}`
- `shelf_compaction_rewarm_skipped_cap_total{reason}`

Eviction policy: re-warmed entries are admitted with the same admission policy as a normal miss (size threshold + pin list bypass per SHELF-25). The reactor never bypasses admission — it only schedules fetches. Pinned tables get re-warmed first.

Operational levers in `shelfd/src/config.rs`:
- `compaction.enabled` (default true)
- `compaction.poll_interval_seconds` (default 30)
- `compaction.max_bytes_per_snapshot` (default 5 GiB)
- `compaction.tables` — explicit allowlist; default = pin-list-derived hot set.

## Acceptance criteria

- [ ] When a watched table commits a `replace` snapshot, the reactor diffs and enqueues re-warm prefetches within 60 s of the commit (poll-interval + processing).
- [ ] Re-warm respects the per-snapshot byte cap (default 5 GiB) and emits a counter when the cap fires.
- [ ] On a synthetic compaction (1 GiB old hot file → 1 GiB new file, identical predicate distribution), morning hit-ratio for that table is ≥ 90 % of the pre-compaction baseline (vs ~5 % without the reactor).
- [ ] Reactor adds < 0.1 req/s of HMS / Trino-system-table load when the watcher uses the metadata.json poll path (not HMS).
- [ ] Disabled-mode (`compaction.enabled=false`) is a no-op with zero metrics churn.
- [ ] Unit tests ≥ 12 cases covering snapshot-diff, predicate match, byte-cap, prefix-rate-limit interaction, no-op disabled mode.
- [ ] Quantitative gate: re-warm latency from snapshot commit → first new-file footer admitted ≤ 90 s p95.

## Out of scope

- `expire_snapshots` cleanup (the cache invalidates by content-key naturally; nothing to re-warm).
- Cross-table re-warm (MV-base interplay) — that's SHELF-47.
- Changing admission policy for re-warmed entries.
- Auto-detecting "hot tables" from scratch — uses the existing pin list and SHELF-38's outputs.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Thundering-herd against S3 on a large compaction | Per-prefix rate limiter on the fallback path (BLUEPRINT §9.4); per-snapshot byte cap; max 32 inflight GETs. |
| Re-warming files that won't be queried (waste) | Only re-warm files whose old counterparts were *hot* (had ≥ 1 hit in the last 24 h); cap by pin-list membership. |
| Snapshot-watcher polling adds HMS load | Poll `metadata.json` directly via S3, not HMS / Trino system tables. SQS / SNS path is optional. |
| Predicate parsing fails on unusual SQL | Best-effort; fall back to "re-warm only the footer + page index" without row-group prediction. |

## Test plan

- Unit tests: snapshot-diff (`replace` vs `append` vs `overwrite`), predicate match (range / equality / null), byte-cap enforcement, prefix-rate-limit interaction, disabled-mode no-op.
- Integration tests: `shelfd/tests/it_compaction_rewarm.rs` boots a fixture S3 with two snapshot states (pre/post compaction), drives the reactor, asserts the new-file footer is admitted within 60 s.
- (If applicable) docker compose smoke: SHELF-12 + `make compaction-smoke` runs `OPTIMIZE` against a seeded Iceberg table and asserts `shelf_compaction_rewarm_files_queued_total > 0`.

## Open questions

- Should the reactor also handle `overwrite` (DELETE / MERGE) snapshots, not just `replace`? Recommend yes for `overwrite` with deletes only (predicate-bounded); skip pure inserts.
- Default poll interval 30 s vs 60 s — depends on the operational SLA for re-warm. Default 30 s and let SHELF-38 retune.
- Is the diff scoped per-partition or per-data-file? Per-data-file (the manifest entries are file-level).
