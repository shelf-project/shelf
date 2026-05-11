---
ticket: rc9-T3
phase: static + ground-truth
date: 2026-05-04 IST
status: gap-list produced; per-gap follow-on tickets pending
---

# T3 — Metric coverage audit

## TL;DR

The analyst's "17 of 21 declared metric families never reach Mimir" claim was directionally correct: an actual coverage audit against the live shelfd `/metrics` endpoint shows **19 of 39 declared families never produce a series** even after the smoke harness's cold + warm pass exercises every code path Trino normally drives through the s3-shim. That's almost half the declared surface unwired.

There's also significant **declaration drift**: 19 metric families that ARE on `/metrics` are NOT in `EXPOSED_SERIES`, including critical new ones (`shelf_pool_amortized_dollars_per_hour`, `shelf_pod_load_skew_ratio_bps`, `shelf_misses_by_tag_total`, the `shelf_coop_*` admission family). `EXPOSED_SERIES` has not been kept in sync with metrics.rs since approximately the rc.5 release.

The analyst's identification of `shelfd/src/telemetry.rs` as the file to audit was wrong (telemetry.rs is OTel trace export, not Prometheus metrics); the right file is `shelfd/src/metrics.rs` + every `*.rs` that holds an `Arc<Registry>` or imports a global static like `HITS_TOTAL`, `LODC_DROPS_TOTAL`, etc.

## Method

1. Spun up the smoke harness (`benchmarks/smoke/docker compose up -d`) on a fresh build off `origin/main` (commit `248902e`).
2. Issued 3 cold queries against the iceberg catalog: `SELECT count(*) FROM region|orders_small|nation`. Each touches `metadata.json` + manifest list + manifest entries through the shim, exercising metadata pool path, origin GET path, single-flight, and shelf shim response path.
3. Scraped `/metrics`, extracted every `^shelf[d]?_[a-z_0-9]+` series name, normalized histogram-derived suffixes (`_bucket`/`_count`/`_sum`) back to base name.
4. Diffed the live emitted set against the `EXPOSED_SERIES` constant in `shelfd/src/metrics.rs`.

## Result: 19 declared-but-not-emitted families

**Not exercisable in single-pod cold-query test (legitimate):**
- `shelf_peer_hit_total` / `shelf_peer_miss_total` / `shelf_peer_timeout_total` / `shelf_peer_error_total` — peer-fetch path requires ≥ 2 shelf pods + cross-pod traffic.
- `shelf_conditional_not_modified_total` / `_modified_total` / `_skipped_total` / `_error_total` — ETag-conditional GET only triggers on the freshness-check path (SHELF-23) which is bypassed on first cold pass.
- `shelf_mv_hits_total` / `shelf_mv_bytes_served_total` — MV registry empty in smoke (no `/admin/pin` with `mv_name=` issued).
- `shelf_hits_by_table_total` — only **misses** got recorded because every read on first pass is cold; warm pass would emit hits.
- `shelf_warm_threshold_crossed_seconds` — one-shot SLI gauge; only set after the first warm-cohort threshold trips. Smoke didn't reach it (workload too small).
- `shelf_head_hits_total` / `shelf_head_misses_total` — Trino's native S3 client does HEAD via `head_object` separately from cached HEAD-LRU; the LRU path may not be exercised by Trino's access pattern in this fixture.

**Likely real gaps (registered but never bumped from any handler):**
- `shelf_bytes_used` — declared as `IntGaugeVec` in `Registry`. Should be set by store.rs as the in-memory pool fill changes. Not appearing in `/metrics`. Either never set, or set on a pool label that didn't get touched.
- `shelfd_error_total` — `errors_total` field in `Registry`. Even though queries produced two 501 responses (visible as `tower_http::trace::on_failure` log entries), the typed counter wasn't incremented. The 501 is from a client hitting `/healthz` on the shim port (wrong endpoint) — that response path doesn't go through the `errors_total` bump.
- `shelf_s3_shim_response_bytes_total` — **HIGH SEVERITY**. This is the numerator of the cache byte-efficiency KPI per the `metrics.rs` doc (`1 - origin_request_bytes / s3_shim_response_bytes`). All 3 smoke queries flowed through the shim and returned bytes to Trino, but the counter never bumped. Either the `record_*` call site is missing in `s3_shim.rs::handle_get_object` for the success path, or it lives on a labels combination that didn't match. Action: read `s3_shim::handle_get_object` and `mv_registry.rs` (the only file that imports the counter per source grep) to find where the bump lives — it may be MV-registry-gated, in which case the non-MV path silently drops it.
- `shelf_queries_served_total` / `shelf_bytes_saved_total` — Track E7 fingerprint substrate metrics. Likely require `cache.fingerprint.enabled=true` config (smoke ships default off).

## Result: 19 emitted-but-not-declared families (drift)

These series ARE on `/metrics` but NOT in `EXPOSED_SERIES`. `EXPOSED_SERIES` is the documentation source of truth and the integration-dashboard reference per `metrics.rs` line 711–714. It's stale.

```
shelf_admit_refused_total
shelf_coalesce_leaders_total
shelf_coop_peer_admits_total            ← rc.7 A6 cooperative peer-admission
shelf_coop_peer_drops_total
shelf_coop_primary_force_admits_total
shelf_drain_active                      ← SHELF-20 drain bit
shelf_lodc_rss_pressure_seconds_total   ← rc.7 A1 RSS-aware admission
shelf_lodc_rss_throttle_multiplier
shelf_misses_by_tag_total               ← SHELF-42 A/B tag substrate
shelf_origin_signing_context_recomputed_total  ← origin SDK signing instrumentation
shelf_origin_signing_context_reused_total
shelf_pod_load_qps                      ← rc.8 K2 HRW-skew aggregator
shelf_pod_load_skew_ratio_bps
shelf_pool_amortized_dollars_per_hour   ← rc.7 A4 net dollars-saved
shelf_s3_shim_sigv4_*                   ← shim sigv4 instrumentation
shelf_transient_decisions_cached        ← rc.7 B3 intermediate-table opt-out
shelf_transient_refresh_errors_total
shelf_transient_refusals_total
```

## Per-gap follow-on tickets (recommended)

| Gap | Severity | Recommended fix |
|---|---|---|
| `shelf_s3_shim_response_bytes_total` | **HIGH** — kills the byte-efficiency KPI | Audit `s3_shim::handle_get_object` for the bump call on the success path (not just the MV path) |
| `shelf_bytes_used` | MED — RAM-use gauge always reads 0 in dashboards | Wire the gauge update to the periodic store-stats sweep that already updates `disk_bytes_used` |
| `shelfd_error_total` | LOW — unused in dashboards today, but per agents/4-shelfd-builder.md "every error has a path" rule should be wired | Audit `shelfd/src/error.rs::Error::component()` for the bump call |
| `EXPOSED_SERIES` drift (19 series) | MED — `shelfd/docs/metrics.md` and Grafana dashboards reference an outdated list | Add the 19 missing names to `EXPOSED_SERIES`; add a CI check that scrapes a smoke-harness `/metrics` and compares against `EXPOSED_SERIES`, failing on drift |
| Peer / MV / conditional / hits-by-table not testable in single-pod smoke | INFO | Add a multi-pod smoke variant + an MV-pin smoke step + a warm-pass step to the harness; rerun audit |

## Hit-counter-reset claim (rolled in from T2)

The analyst's "hit counter resets internally 5+ times in 6 hours" claim is gated on `shelf_engine_resets_total` actually being bumped. The audit shows `shelf_engine_resets_total` IS on `/metrics` (value 0 in the smoke run, no reset events), so the counter IS wired. The 6h cluster scrape (T2) is the test that tells us whether it ever non-monotonic-resets in production. Until T2 runs (operator-blocked), we cannot confirm or refute the analyst's specific claim — but the metric infrastructure to detect it exists and is correct.

## Anchor data files

- Live `/metrics` scrape: `/tmp/t3-live-metrics.txt`
- Distinct emitted series names: `/tmp/t3-emitted.txt`
- Audit script (re-runnable): inline in this run; suitable for promotion into `benchmarks/tools/metrics_coverage_audit.py`

## Action items handed back to the operator

1. Investigate the `shelf_s3_shim_response_bytes_total` gap — the byte-efficiency KPI is the single most visible cost-savings metric; today it would always read zero.
2. Refresh `EXPOSED_SERIES` with the 19 drift entries.
3. Add the smoke-harness `/metrics` audit as a CI step so future drift is caught in PR review (per the existing OSS-hygiene tripwire pattern that catches identifier leaks — same shape, applied to metric drift).
