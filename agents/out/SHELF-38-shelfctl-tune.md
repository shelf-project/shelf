# SHELF-38: `shelfctl tune` — ROI report + values.yaml patch

**Status:** Draft
**Tier:** S
**Estimated effort:** M
**Depends on:** SHELF-37, SHELF-23
**Blocks:** none

## Problem (OSS-cited)

New Shelf operators don't know if the cache is helping them, what to pin, or how to size pools. The #1 reason teams stay on direct S3 even after install is the absence of a one-command "is this worth it?" answer. Alluxio, JuiceFS, and Mountpoint-S3 ([awslabs/mountpoint-s3](https://github.com/awslabs/mountpoint-s3)) ship hit-rate panels but no per-Iceberg-table ROI report and no recommended config patch. Trino's stable `QueryCompletedEvent` SPI ([trinodb/trino #26342](https://github.com/trinodb/trino/issues/26342)) carries the per-operator `bytesReadFromCache` / `bytesReadExternally` fields that make this report computable.

## Goal

`shelfctl tune --window 7d` prints a one-page ROI report — $-saved, hit-ratio per table, top-10 pin candidates, recommended pool sizing — plus a unified-diff `values.yaml` patch the operator can review and apply.

## Approach

New subcommand under `shelfctl/src/cmd_tune.rs`, registered in `shelfctl/src/main.rs` next to `cmd_stats`, `cmd_pin`, `cmd_evict`, `cmd_ring`, `cmd_reload`. Inputs: (a) the SHELF-37 Iceberg log table — read via Trino over JDBC using a connection string from `~/.shelfctl/config.toml` or `--trino-url`; (b) the live `/stats` JSON from `shelfd` over the SHELF-23 admin surface for current `metadata_pool` / `rowgroup_pool` capacity + used bytes + pinned bytes. Window flag accepts `1h | 24h | 7d | 30d` (default `7d`).

Compute pipeline (all SQL, no Python in `shelfctl`):
1. `bytes_saved_per_table = Σ bytes_read_from_cache GROUP BY table` over the window.
2. `hit_ratio_per_table = bytes_read_from_cache / (bytes_read_from_cache + bytes_read_externally)`.
3. Top-10 pin candidates: tables with `(scanned_bytes × wall_ms × frequency)` highest *and* `hit_ratio < 0.7` *and* `total_bytes < 0.3 × pool.rowgroup.capacity_bytes`.
4. Pool sizing: `recommended_metadata_dram = max(current_used × 1.3, p95_metadata_used)`, `recommended_rowgroup_nvme = max(current_used × 1.2, p95_rowgroup_used + working_set_estimate)`.

Output rendered through a small `comfy-table` summary + a unified diff against the user's current `charts/shelf/values.yaml` (path via `--values-file`, default auto-detected). Diff format compatible with `kubectl diff` / `helm diff upgrade`.

## Acceptance criteria

- [ ] `shelfctl tune --window 7d --trino-url <url> --values-file charts/shelf/values.yaml` returns within 30 s p95 on a 7-day window of 1 M queries.
- [ ] Report shows per-table $-saved column populated from the same SHELF-40 formula (no double-counting; calls into the shared `dollars_saved` crate).
- [ ] Top-10 pin candidates are stable across two consecutive runs in the same window (deterministic ordering).
- [ ] Generated `values.yaml` patch applies cleanly via `git apply --check` against an unmodified upstream `values.yaml`.
- [ ] Unit tests cover the SQL builder, the pool-sizing formula, and the diff renderer (≥ 15 cases).
- [ ] Integration test runs against a seeded SHELF-37 log table and asserts the report output is byte-identical to a committed golden fixture.

## Out of scope

- Auto-applying the patch (operator runs `kubectl apply`/`helm upgrade` themselves).
- Cross-cluster aggregation (single Trino URL only in v1).
- LightGBM-style learned recommendations (Phase 4 if it ships).
- `regret` analysis (SHELF-39).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Slow Trino query against a large log table | Push down the window predicate on `end_time`; cap rows; document that the SHELF-37 table should be partitioned by `day(end_time)`. |
| Recommendation drift between runs | Deterministic ordering (stable sort on `(score DESC, table_name)`) + a pinned random seed for any tie-break. |
| Operator runs `tune` against a 1-hour window and overfits | `tune` refuses to emit a patch for windows < 24 h with a `--force` escape hatch. |

## Test plan

- Unit tests: SQL builder, formula computation, top-N selection, values.yaml diff renderer, JSON output mode (`--format json`).
- Integration tests: seeded SHELF-37 table fixture + golden report fixture under `shelfctl/tests/fixtures/tune/`; asserts byte-identical output.
- (If applicable) docker compose smoke: extends the SHELF-12 harness with the SHELF-37 listener and asserts `make tune-smoke` returns rc=0.

## Open questions

- Should `tune` also recommend `shelf.admission.size_threshold_mib` adjustments, or stay pool-sizing-only? Recommend: pool-sizing only in v1; admission tuning is separate.
- Format default: human-readable table or JSON-first? Default human; `--format json` for CI.
- Where does the JDBC driver live — bundled in `shelfctl` (~30 MiB) or shelled out via `trino-cli`? Recommend: prefer `trino-rs` if available; fall back to a small native HTTP client for the few endpoints used.
