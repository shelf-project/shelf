# SHELF-43: `shelfctl explain query <id>` CLI

**Status:** Draft
**Tier:** B
**Estimated effort:** M
**Depends on:** SHELF-37, SHELF-23
**Blocks:** none

## Problem (OSS-cited)

The original idea was `EXPLAIN (TYPE SHELF)`. Trino's EXPLAIN grammar hardcodes `LOGICAL` / `DISTRIBUTED` / `VALIDATE` / `IO`; `io.trino.spi.Plugin` has no extension hook for new EXPLAIN types ([io.trino.spi.Plugin docs](https://trino.io/docs/current/develop/spi-overview.html)). A CLI post-hoc reader of `QueryStatistics.getOperatorSummaries()` (per-query `bytesReadFromCache` / `bytesReadExternally` from [trinodb/trino #26342](https://github.com/trinodb/trino/issues/26342)) and `QueryStatistics.getJsonPlan()` recovers the same UX without an upstream change.

## Goal

`shelfctl explain query <query-id>` prints a per-query breakdown — file list with cache state per file (`HIT_DRAM | HIT_NVME | MISS | PREFETCH_QUEUED`), MB served, $-saved — equivalent to `EXPLAIN (TYPE SHELF)` but driven from the post-hoc log table.

## Approach

New subcommand `shelfctl/src/cmd_explain.rs`. Flow:

1. Look up the query in the SHELF-37 log table by `query_id`. Pull `plan` (JSON), `operator_summaries`, `physical_input_bytes`, `wall_ms`, `bytes_read_from_cache`, `bytes_read_externally`.
2. Parse `plan` JSON to recover the file list per scan operator. (Use the same `getJsonPlan()` shape Trino emits — fall back to operator summaries if `plan` is null.)
3. For each file, query `shelfd`'s `/admin/lookup?key=<contentkey>` (a small new admin endpoint added in `shelfd/src/http.rs` under the SHELF-23 admin surface) to get the current cache state per `(pool, key)`. Cache state encoding:
   - `HIT_DRAM` — present in DRAM Foyer cache.
   - `HIT_NVME` — present in NVMe Foyer disk tier.
   - `MISS` — not present.
   - `PREFETCH_QUEUED` — pending in the prefetch queue.
4. Render a table: `file, rg_count, bytes, cache_state, dollars_saved`.

The `dollars_saved` column reuses the SHELF-40 shared crate. Output formats: human, JSON, markdown.

Implementation cross-references:
- `shelfd/src/http.rs` — new `/admin/lookup` route returning `{key, pool, state}`.
- `shelfd/src/store.rs` — expose a `lookup(key, pool) -> CacheState` API.
- `shelfctl/src/cmd_explain.rs` — orchestration + rendering.
- `clients/trino/event-listener-iceberg/docs/explain.md` — usage doc.

Performance target: a single `explain query` for a 50-file query returns within 2 s p95.

## Acceptance criteria

- [ ] `shelfctl explain query <id>` for a known query in the log table returns within 2 s p95.
- [ ] Cache state classification is correct on ≥ 95 % of files in a synthetic fixture (DRAM-only, NVMe-only, mixed, evicted, prefetch-queued).
- [ ] Total `dollars_saved` reported by `explain query` matches the per-query slice of SHELF-40's counter to within 0.5 %.
- [ ] Unknown `query_id` returns a clear error and exit code 2.
- [ ] If the log row's `plan` is null, the command falls back to operator summaries and prints a `--degraded` banner.
- [ ] Unit tests ≥ 12 cases covering plan parsing, cache-state lookup, fallback path.
- [ ] Integration test: seeded log table + faked shelfd admin → asserts byte-identical golden output.

## Out of scope

- Filing the upstream TIP for engine-pluggable EXPLAIN — covered as item #1 in `agents/out/03-plan.md` §8 and not duplicated here.
- Live (in-flight) explain — the CLI is post-hoc only.
- Cross-cluster aggregation.
- Re-running the query.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| `plan` JSON shape changes between Trino versions | Parser tolerates missing fields; fallback to operator summaries; tested against Trino 473 / 476 / 480 fixtures. |
| `shelfd` admin lookup adds load during incidents | Admin endpoint is rate-limited (default 100 req/s); CLI batches lookups by 50 keys per call. |
| Long file lists (1000+ files) blow up output | Default truncates to top-50 by `bytes`; `--all` flag for full output. |

## Test plan

- Unit tests: plan parsing (full + partial + null), cache-state classification, dollar attribution per file.
- Integration tests: `shelfctl/tests/it_explain.rs` against a faked admin server; asserts byte-identical golden output.
- (If applicable) docker compose smoke: SHELF-12 + listener; run a query, capture its id, run `shelfctl explain query <id>`, assert exit 0 and rendered output non-empty.

## Open questions

- Cache-state lookup races against eviction (the file may be present at lookup but evicted by the time the operator reads the report). Recommend including an `as_of` timestamp on the report.
- Should the CLI also surface "PREFETCH_DROPPED" if the row group was queued but evicted before being served? Useful for SHELF-39 cross-reference. Recommend yes.
- Should `--all` mode page through more than 1 K files? Probably a follow-up; v1 truncates with a warning.
