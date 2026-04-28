# SHELF-39: `shelfctl regret` — anti-bragging report

**Status:** Draft
**Tier:** S
**Estimated effort:** M
**Depends on:** SHELF-37, SHELF-23
**Blocks:** none

## Problem (OSS-cited)

No OSS infra project today proactively surfaces *where it made things worse* — users discover regressions via incidents. Alluxio, JuiceFS, Mountpoint-S3, and Trino's `fs.cache` all report aggregate hit rates but none expose per-table negative-impact analysis. Trino's `QueryCompletedEvent` SPI ([trinodb/trino #26342](https://github.com/trinodb/trino/issues/26342)) gives us per-query wall time and per-operator I/O bytes; with the SHELF-42 `shelf_arm` tag (or simply A vs B catalog comparison) the data needed to compute "p95 worse via Shelf vs direct S3" is on the table.

## Goal

`shelfctl regret --window 24h` lists tables and queries where p95 latency was worse through Shelf than through direct S3, with the diagnosed reason (rate-limited / evicted / origin-pool-exhausted / NVMe-pressure).

## Approach

Subcommand `shelfctl/src/cmd_regret.rs` alongside `cmd_tune` from SHELF-38. Reads the SHELF-37 log table over JDBC plus shelfd's `/metrics` Prometheus scrape (or a Prom URL if Grafana is wired) for `shelf_origin_rate_limited_total`, `shelf_evictions_total`, `shelf_origin_pool_saturation_seconds_total`, `shelf_disk_used_bytes / shelf_disk_capacity_bytes`. Compute pipeline:

1. Group queries by `(table, shelf_arm)` over the window; compute p95 `wall_ms`.
2. Flag pairs where `p95_wall_ms[arm=A] > 1.10 × p95_wall_ms[arm=B]` AND the `arm=A` cohort has `bytes_read_from_cache > 0.2 × physical_input_bytes` (i.e. Shelf was actually engaged).
3. For each flagged table, attribute root cause: cross-correlate the `wall_ms` regression window against the metric series — pick the dominant signal among `rate_limited` / `evicted` / `pool_saturated` / `nvme_pressure`.
4. Emit a sorted report; top-N tables with regression × frequency.

Default window 24 h, configurable. Default p95 ratio threshold 1.10, configurable via `--regression-threshold`. Output formats: human table (`comfy-table`), `--format json` for CI, `--format markdown` for incident-doc paste. Subsystems live in a new `shelfctl/src/analytics/` module so SHELF-38 and SHELF-39 share the SQL backbone.

If SHELF-42 has not landed (no `shelf_arm` column), `regret` falls back to comparing per-table p95 across consecutive 24-h windows ("did p95 get worse after this morning's deploy?") and prints a banner explaining the limitation.

## Acceptance criteria

- [ ] `shelfctl regret --window 24h` returns within 20 s p95 on a 24-h window of 100 K queries.
- [ ] When fed a synthetic dataset where 1 table is 50 % slower through Shelf, regret flags that table at the top of the output.
- [ ] When fed a clean dataset (no regressions), regret prints "no regrets in window" and exits 0.
- [ ] Root-cause attribution is correct on ≥ 90 % of seeded scenarios across the four root causes (`rate_limited`, `evicted`, `pool_saturated`, `nvme_pressure`).
- [ ] Without SHELF-42, regret falls back to consecutive-window comparison and prints the limitation banner.
- [ ] Unit tests ≥ 12 cases covering the analytics core.

## Out of scope

- Auto-mitigation (raising thresholds, unpinning).
- Cross-cluster regret (single Trino URL).
- Long-window historical drift (> 30 d).
- "Why did this individual query slow down?" — that is SHELF-43 (`explain query`).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| False positive: BI dashboard ran a one-off heavy query | Require ≥ N queries per arm per table (default N=20) for a flag. |
| Operator reads regret report and panics | Format includes a "this is informational, not page-worthy" header; severity tag per row. |
| Prometheus retention < window | Detect missing metric series and downgrade root-cause to `unknown` rather than misattribute. |

## Test plan

- Unit tests: regression detection, root-cause attribution, fallback mode without `shelf_arm`, JSON/markdown renderers.
- Integration tests: seeded log table + synthetic Prom counters under `shelfctl/tests/fixtures/regret/`; asserts byte-identical output for both regression-present and clean inputs.
- (If applicable) docker compose smoke: extends SHELF-12 with the listener, runs a synthetic regression workload, asserts `shelfctl regret` flags the offending table.

## Open questions

- Default regression threshold 1.10× — too tight or too loose? Default chosen to match SLO `p95 ≤ 120 % of Alluxio baseline` from §6.4 of `agents/out/03-plan.md`; revisit after first 30 days of OSS feedback.
- Should regret produce a Slack/PagerDuty-pingable artefact or stay CLI-only? Stay CLI-only in v1; auto-publish is post-v1.
- Should attribution include "user error: pin list contains a 50 GiB table"? Recommend: yes, as a fifth root-cause class `pin_misuse`.
