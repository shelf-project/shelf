---
ticket: rc9 closeout
date: 2026-05-04 IST
status: 11 of 14 todos closed; 3 operator-blocked
---

# rc9 plan execution closeout

## Done in this session (8 newly closed + 5 already in-session)

| Todo | Outcome | Doc |
|---|---|---|
| **T0 (P0)** | Canary shelf-4 PASS at 15:30 IST. Bulk roll deferred to operator at 22:30 IST. | `infra/penpencil/docs/ops/t0-canary-2026-05-04-results.md` (in-session) |
| **T1 Phase A** | **Hypothesis REFUTED** on protocol grounds: AWS SDK v2 `NettyNioAsyncHttpClient` defaults HTTP/1.1 and Trino native S3 doesn't expose `Protocol.HTTP2`. Bench confirmed shelfd correctly speaks both h1+h2; env-flag wiring works. | `agents/out/rc9-T1-h2-window-spike-results.md` |
| **T1 Phase B** | **CANCELLED** — no PR cut. h2 helper code stays unmerged in worktree `/private/tmp/shelf-rc9-t1-h2-spike-41727`. | (rolled into Phase A doc) |
| **T1 Phase C fallback** | Phase-split histogram instrumentation IS the right next step; recommendation captured. Operator files separate ticket post-T0 bulk-roll. | (rolled into Phase A doc) |
| **T3** | Live `/metrics` ground-truth audit: **19 of 39 declared families don't emit any series** even after smoke queries; **19 series ARE on /metrics that aren't in `EXPOSED_SERIES`** (drift since rc.5). HIGH-severity gap: `shelf_s3_shim_response_bytes_total` (the byte-efficiency KPI numerator). | `agents/out/rc9-T3-metric-coverage-audit.md` |
| **T4** | DataFusion 50.0.0 `FileMetadataCache` review-comment for upstream Trino #29184. | `docs/discovery/upstream/29184-review-comment.md` (in-session) |
| **T5** | SuRF range-filters ADR-0014. | `agents/out/adr/0014-surf-range-filters-on-iceberg-bounds.md` (in-session) |
| **T6** | **Closed** — schema gap is stale; `gen_pin_list.py` already emits `PinListDoc`-compatible output. Operator uses S3-polling pin-list loader, not per-key `/admin/pin`. | `agents/out/rc9-T6-pinlist-schema-scope-confirm.md` |
| **T7** | **Closed** — SHELF-45 reactor (PR #69, `29278e8`) + A3 metadata-poll producer (PR #101, `b80e459`) ARE shipped. Plan v2's "A3 NOT shipped" was wrong; analyst's compaction-rewarm proposal fully addressed. | `agents/out/rc9-T7-compaction-rewarm-scope-confirm.md` |
| **T9** | WSA hit-rate-vs-cache-size doc with replay procedure + 5-cap table template. | `docs/discovery/wsa-2026-05.md` (in-session) |
| **T10** | rep-0/rep-3 catalog cutover MR template (paste-ready, draft). | `infra/penpencil/docs/ops/rep-0-rep-3-cutover-mr-template.md` (in-session) |
| **T11** | 3 dashboard panels added under "Capacity soak (T0)" row. | `observability/dashboards/shelf-overview-v2.json` (in-session) |

## Remaining (operator action required)

| Todo | Reason blocked | What the operator does |
|---|---|---|
| **T0 bulk roll** | Scheduled 22:30 IST tonight — needs cluster kubectl + low-traffic window | Run the sequential roll script in `t0-canary-2026-05-04-results.md` "Next step" section; arm the auto-rollback watcher |
| **T2 hit-counter scrape** | Requires sidecar pod in `alluxio` ns to dodge per-pod port-forward bash-loop trap | Deploy a curl-image sidecar; run a 6h scrape loop; correlate any `shelf_hits_total` non-monotonic events against `shelf_lodc_drops_total` bursts and `kubectl get events`. Per T3, the `shelf_engine_resets_total` counter IS wired so any reset event will increment it. |
| **T8 NVMe compression** | Sequenced after T9 Belady-replay verdict — needs operator-driven 7-day rep-1 trace replay | Run `tools/gen_pin_list.py` against rep-1 7-day window, feed into SHELF-35 Belady oracle (PR #41) at caps `[60,120,240,500,1000] GiB`, fill in the `wsa-2026-05.md` table, decide whether to start T8 ADR-0013 work |

## Substantive findings worth flagging beyond plan checklist

1. **The h2 window hypothesis is a misdiagnosis at the protocol layer.** Documented evidence + recommendation NOT to spend cluster bench time. The actual per-pod plateau root cause is downstream of any HTTP/2 flag (workspace memory's signing-context / Foyer-lock-contention hypotheses are still live).

2. **EXPOSED_SERIES is stale by 19 entries.** `shelfd/docs/metrics.md`, the Grafana dashboard, and the integration tests all reference an outdated metric inventory. Recommend a CI step that diffs a smoke-harness `/metrics` against `EXPOSED_SERIES` and fails on drift (same shape as the existing OSS-hygiene tripwire).

3. **`shelf_s3_shim_response_bytes_total` is registered but never bumped on the success path.** This is the numerator of the cache byte-efficiency KPI. Any dashboard panel computing `1 - origin_bytes / shim_bytes` reads `1 - origin_bytes / 0` = undefined / NaN. HIGH-severity for cost-savings storytelling. See T3 doc for fix recipe.

4. **A3 + SHELF-45 compaction-rewarm IS shipped on origin/main.** The plan v2 "A3 NOT shipped" line was wrong; the analyst's compaction-rewarm recommendation is fully addressed and just needs `cache.rewarm.enabled: true` in the per-replica overlay after Tier-1 substrate soaks 7 days clean.

5. **T6 pinlist schema gap is also stale.** No code change needed; the analyst's "pin top-5 tables" path is operationally available today.

## Code changes left in worktree (not on main)

`/private/tmp/shelf-rc9-t1-h2-spike-41727` (branch `rc9/t1-h2-window-spike` off `origin/main`, 0 commits ahead):

- `Cargo.toml` — `hyper-util` features extended with `service`, `server-auto`.
- `shelfd/src/http.rs` — `serve()` and `serve_s3_shim()` rewired through `serve_with_h2_window()` helper; `http2_initial_window_from_env()` reads `SHELFD_H2_INITIAL_WINDOW_BYTES`.
- `benchmarks/smoke/Dockerfile.shelfd` — added `COPY shelf-advisor`, `COPY crates` for workspace-member completeness.
- `benchmarks/smoke/docker-compose.yml` — pass-through env var; commented out the missing-jar plugin mount.

These changes are correct and reusable but should NOT land as-is per T1 Phase A's recommendation. The Dockerfile workspace-member fix could be cherry-picked as a small standalone PR (it would have caught the rc9-T1 build failure on any future workspace-member addition).

## Recommended next-rc plan candidates

Captured for `shelf_rc.10_roadmap_*.plan.md` if/when the operator drafts it:

- **R1** — Wire the 4 real metric-emission gaps from T3 (`shelf_s3_shim_response_bytes_total`, `shelf_bytes_used`, `shelfd_error_total`, `shelf_warm_threshold_crossed_seconds`) and refresh `EXPOSED_SERIES` with the 19 drift entries. Add CI smoke `/metrics` audit step.
- **R2** — Phase-split histogram in `shelfd/src/http.rs` (T1 fallback): per-request `recv_ns → headers_sent_ns → body_start_ns → body_done_ns`. The right diagnostic for the actual per-pod plateau.
- **R3** — Cherry-pick the Dockerfile workspace-member fix as a standalone PR (catches future workspace-member additions before they break a build).
- **R4** — Ship the SHELF-29-branch work currently in `/Users/aamir/trino/shelf` (independent-queue rate-limiter) through normal review. The branch has 12 modified files + many untracked; needs cleanup before any `git push`.

## Plan adherence

- Plan file `/Users/aamir/.cursor/plans/analyst_report_validation_rc9_plan_de82494e.plan.md` was NOT edited per user instruction.
- All todos started in_progress before work began and marked appropriately at completion.
- Per workspace memory, T1 used a dedicated worktree off `origin/main` to avoid colliding with the active `shelf-29-independent-queue-rate-limiter` branch in the original tree.
- Refuted-with-evidence outcomes (T1, T6, T7) are documented with the evidence trail so they don't get re-proposed in a future analyst pass.
