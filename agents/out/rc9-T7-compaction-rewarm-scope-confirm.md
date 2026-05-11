---
ticket: rc9-T7
date: 2026-05-04 IST
status: closed — analyst's compaction-rewarm proposal IS shipped via SHELF-45 (PR #69) + A3 (PR #101)
---

# T7 — A3 / SHELF-45 compaction-rewarm scope confirmation

## TL;DR

The analyst's "Compaction-Aware Re-Warm — eliminates the Tuesday morning problem" recommendation (Plan v2 P10, Plan v2 T7 framing) is **already shipped on `origin/main`** as the combination of two merged PRs:

- **SHELF-45 / PR #69** (commit `29278e8`, Apr 30 2026): `shelfd/src/compaction_rewarm.rs` — long-running Tokio reactor that consumes `IcebergSnapshotEvent`s, classifies compaction-class transitions (`ALTER TABLE … EXECUTE optimize`, `expire_snapshots`, `remove_orphan_files`), and proactively re-warms new file paths into the rowgroup pool through `FoyerStore::get_or_fetch` (single-flight) BEFORE the cold-miss thundering herd hits S3. Rate-limited (default 50 MiB/s/pod), concurrency-capped (default 4 in-flight). Default-OFF (`cache.rewarm.enabled: false`) on the OSS chart.

- **A3 / PR #101** (commit `b80e459`, May 1 2026): metadata.json polling producer for the SHELF-45 reactor, **bypassing the JDK-25-blocked SHELF-37 listener**. Polls each watched table's `metadata.json` at a configurable interval (default 30s), detects `summary["operation"] = "replace"` (compaction snapshots), forwards to the reactor through the public `SnapshotPublisher` surface. Per-snapshot byte cap (`max_bytes_per_snapshot`, default 5 GiB) prevents runaway prefetch on large `expire_snapshots` rewrites.

Plan v2's "T7 — A3 is confirmed NOT shipped" claim was wrong. The original plan v1 framing ("PARTIALLY SHIPPED as A3") was correct.

## Diff of analyst proposal vs. what shipped

Analyst proposal (verbatim from the v2 brief):
> A compaction-watcher subscribes to Iceberg snapshot transitions, diffs `removed_data_files` vs `added_data_files`, and pre-warms matched entries.

Shipped behavior:
| Analyst step | A3 + SHELF-45 implementation |
|---|---|
| Subscribe to Iceberg snapshot transitions | A3 polls `metadata.json` per watched table on 30s default; SHELF-37 listener will push events when JDK 25 unblocks |
| Detect compaction (`operation=replace`) | A3 producer filters on `summary["operation"]`; SHELF-45 reactor double-checks via `is_compaction_event` predicate (also catches `expire_snapshots`, `remove_orphan_files`) |
| Diff `removed_data_files` vs `added_data_files` | SHELF-45 re-warms ALL `added_data_files`. The diff against `removed_data_files` is implicit: ADR-0011 content-addressed keys mean removed files become unreachable orphans automatically (Foyer evicts on capacity), so there's nothing to "diff out" — the operation is just "warm what's new" |
| Pre-warm matched entries | SHELF-45 fetches `added_data_files` footers + (configurable) row-group prefixes through `FoyerStore::get_or_fetch`, single-flight-coalesced with any concurrent client read |

The shipped "warm what's new" approach is **slightly broader than the analyst's "diff" approach** — it pre-warms every newly-added file rather than skipping ones whose data already maps to existing-cached entries. In practice this is identical or better because:
- Compaction always rewrites file boundaries → new ETag → new content-addressed key → there's no "match" to skip.
- The single-flight coalescing means a real query landing on the same file mid-rewarm pays the cost ONCE, not twice.

## Outstanding dependency

SHELF-37 (PR #66, Iceberg event-listener jar) remains parked on JDK 25 (workspace memory). When it lands, it becomes the lower-latency producer alongside A3's polling producer. Both producers can coexist: SHELF-45's reactor reads from a single bounded mpsc and doesn't care which producer wrote.

## Operator action

- Both modules are **default-OFF** in the OSS chart (`cache.rewarm.enabled: false`) and in the penpencil overlay (commented with the workspace's "flip after Tier-1 substrate is green for 7 days" hint).
- After Track-1 measurement substrate (SHELF-37 listener / SHELF-40 dollars-saved / SHELF-42 A/B tag) lands and soaks 7 days, flip rewarm on per-replica with the existing `cache.rewarm.tables: [...]` list populated from `gen_pin_list.py --top-N`.
- No new ticket required. T7 closed.

## Anchor data files

- `shelfd/src/compaction_rewarm.rs` (1186 lines) — full SHELF-45 reactor
- PR #69 (`29278e8`) — initial SHELF-45 land
- PR #101 (`b80e459`) — A3 polling producer
- Workspace memory: "A3 metadata-poll cold-morning rewarm landed in v1.0.0"
