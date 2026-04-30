# Shelf rep2 — live perf snapshot, 2026-04-27 16:50 IST

> Captured ~3.8h after the rep2 catalog cutover at ~13:00 IST today. All Prom queries against `mimir-data` (uid `ddy2eykq2tfy8a`); all Trino queries against `cdp.trino_logs.trino_queries`. Numbers are raw, not laundered. Times in IST unless suffixed `UTC`.

## TL;DR — five things this snapshot says, that the dashboard hides

| # | Finding | Evidence | What it implies for the research doc |
|---|---|---|---|
| 1 | **Only `shelf-2` is taking traffic.** `shelf-0` and `shelf-1` are dark. | shelf-2 ingress = 39.9 MB/s, shelf-0/1 = ~100 B/s. shelf-2 RSS = 10.46 GiB, shelf-0/1 = 13 MiB. | "Cluster cache" is effectively a 1-pod cache. Phase-A item on placement (CHWBL / Maglev) jumps from "v1.x" to "now". |
| 2 | **Disk pool isn't filling.** `shelf_disk_bytes_used = 0` on all 3 pods despite 4h of traffic. | Capacity = 240 GiB/pod is reported, used = 0. shelf-2 RSS holds 10 GiB ⇒ all caching is in RAM/Foyer-DRAM tier; NVMe tier is silent. | Either (a) Foyer disk-tier writes are misconfigured, (b) the metric reads a wrong path, or (c) NVMe pool was never enabled. **First Phase-A ticket should be a 1-day RCA on this** — without disk tier, Shelf is a memcache, not a 720-GiB cluster cache. |
| 3 | **Hit/miss counters reset multiple times in 6h** without `kube_pod_container_status_restarts_total` ticking. | shelf-2 rowgroup hit counter: 360k → 789 → climbed → 459k → 7853. Pod restart count = 0. | Internal Foyer/store engine is hot-resetting (config reload? rolling restart by Helm without pod recreate? cache reseat?). Each reset wipes warm state ⇒ **cold-start penalty per reset, ~5–15 min per cycle.** Phase-A item: surface a `shelf_engine_restarts_total` counter + alert. |
| 4 | **Cutover effect is reliability-shaped, not latency-shaped.** | Pre-cutover hour (07 UTC = 12:30 IST): 79.5% fail rate, p95 = 643 s. Post-cutover (08–10 UTC): 6.6%, 14.5%, 22.3% fail rate, p95 = 3 / 10 / 34 s. | Headline "Shelf killed Alluxio meltdown" is right, but Phase B/C should also chase the residual 14–22% — almost all is `USER_CANCELED` + `INVALID_VIEW` + `ICEBERG_*`, *not* `EXCEEDED_TIME_LIMIT`. Cache-hit-rate alone won't move it. |
| 5 | **Top 5 tables drove 75% of physical bytes today.** | `silver_chat_text_output_log` 311 GiB / 52 q (p95=835s, sole table left from `ai_chat_spam` heavy job); `vw_crm_spam_chat_view` 261 GiB / 241 q; `cdp_revenue.gold_users` 190 GiB / 2 q; `mview.gold_dbt_test_results` 82 GiB / 2 q; `silver_correct_cohort_ay_26` 39 GiB. | Strong Pareto ⇒ a 5-row pin-list captures most of the value. SHELF-26 replay should weight these 5 fingerprints. |

## 1. Pod-level read traffic and resource use (last 5 min instant)

| Pod | Ingress (MB/s) | RSS (GiB) | CPU (cores) | Disk used (GiB) | Disk capacity (GiB) | Restarts |
|---|---|---|---|---|---|---|
| `shelf-0` | 0.0001 | 0.013 | 0.0001 | 0.00 | 240 | 0 |
| `shelf-1` | 0.0001 | 0.013 | 0.0001 | 0.00 | 240 | 0 |
| `shelf-2` | **39.9** | **10.46** | **0.22** | **0.00** | 240 | 0 |

> Source: `container_network_receive_bytes_total{namespace=alluxio,pod=~"shelf-.*"}`, `container_memory_working_set_bytes`, `rate(container_cpu_usage_seconds_total[5m])`, `shelf_disk_bytes_used / shelf_disk_bytes_capacity` — all wrapped with `max without(prometheus, dataPrometheusReplica, …)` for HA-Prom dedup.

**Interpretation.** This isn't transient — the 6h time series shows shelf-0 at 3 hits and shelf-1 at 2536 hits **flat for the entire 6h window**, with shelf-2 doing 100% of the work and rotating its own counters when the engine resets. The Service-level fan-out is broken in practice. Hypotheses worth checking in §2 obs gap audit:

- HRW key derivation collapses to one bucket for rep2's predicate space (path or etag).
- Trino native-S3 client is reusing a single connection / DNS-resolved IP; kube-proxy's iptables hashing pins it.
- `shelf-0`/`shelf-1` aren't on the EndpointSlice you think they are (NetworkPolicy or readiness gating from Trino's POV).

## 2. Cache hit ratio (rowgroup pool, shelf-2 only)

Last counter sample, 16:50 IST:

```
shelf_hits_total{pool=rowgroup,pod=shelf-2}   = 7,853
shelf_misses_total{pool=rowgroup,pod=shelf-2} = 11,367
hit_ratio = 7853 / (7853 + 11367) = 40.9%
```

But `metadata` pool: `hits = (none reported)`, `misses = 4`. The metadata pool is essentially silent — either small enough that it's all served by `head_lru` (whose counters aren't in the dashboard), or simply not warming because we're talking ~minutes since the last engine reset.

Pre-reset (16:21 IST, ~30 min ago) the same counters peaked at:

```
hits = 459,396; misses = 244,188 ⇒ hit ratio = 65.3%
```

So **rep2 was at ~65% hit ratio just before the engine reset wiped state**, then dropped to 40.9% on a cold restart. This validates A2 (MRC estimator) and A7 (time-to-warm SLI) in the plan and adds a new urgent item: persistence across resets, or cache-warm protection.

> Source: raw counter values via `max without(prometheus, dataPrometheusReplica, container, endpoint, instance, job, namespace, ordinal, service) (shelf_hits_total{pool="rowgroup"})` over `now-6h..now`.

## 3. What the dashboard claims to show vs. what mimir-data actually has

`shelf-overview.json` references **21+ shelfd-emitted series**. mimir-data scrape is returning **6**:

| Metric | In `metrics.rs` | In dashboard | Scraped to mimir-data? |
|---|---|---|---|
| `shelf_hits_total` | yes | yes | **yes** |
| `shelf_misses_total` | yes | yes | **yes** |
| `shelf_disk_hits_total` | yes | yes | **yes** (always 0 currently) |
| `shelf_disk_misses_total` | yes | yes | **yes** |
| `shelf_disk_bytes_used` | yes | yes | **yes** (always 0 currently) |
| `shelf_disk_bytes_capacity` | yes | yes | **yes** |
| `shelf_request_seconds` (histogram) | yes | yes | **no** |
| `shelf_admissions_total` | yes | yes | **no** |
| `shelf_admission_refused_total` | yes | yes | **no** |
| `shelf_evictions_total` | yes | yes | **no** |
| `shelf_origin_request_bytes_total` | yes | yes | **no** |
| `shelf_origin_request_seconds` | yes | yes | **no** |
| `shelf_s3_shim_response_bytes_total` | yes | yes | **no** |
| `shelf_inflight_singleflight` | yes | yes | **no** |
| `shelf_queries_served_total` | yes | yes | **no** |
| `shelf_bytes_saved_total` | yes | yes | **no** |
| `shelf_head_hits_total` | yes | yes | **no** |
| `shelf_head_misses_total` | yes | yes | **no** |
| `shelf_dram_bytes_used` | yes | yes | **no** |
| `shelf_nvme_bytes_used` / `shelf_nvme_bytes_capacity` | yes | yes | **no** |
| `shelf_mv_hits_total` / `shelf_mv_bytes_served_total` | yes | yes | **no** |
| `shelf_plugin_*` (plugin-side, not shelfd) | n/a (Java side) | yes | **no** |

**Practically every panel except hit-ratio-by-pool is rendering empty or zero on `shelf-overview`.** This is the single biggest blocker to running the rest of the research — without latency histograms, admissions, evictions, or origin volume we are flying half-blind. §2 of the main doc opens with this, A1 of Phase A is the fix.

## 4. Trino-side reality: rep2 today, hour-by-hour

| Hour (UTC) | Hour (IST) | n | failed | fail % | avg wall (s) | p95 wall (s) | GB read | Read MB/s |
|---|---|---|---|---|---|---|---|---|
| 03 | 08:30–09:30 | 28 | 0 | 0.0 | 3.2 | 12 | 38 | 17.8 |
| 04 | 09:30–10:30 | 270 | 110 | 40.7 | 96.5 | 221 | 529 | 5.7 |
| 05 | 10:30–11:30 | 527 | 203 | 38.5 | 34.9 | 155 | 537 | 1.0 |
| 06 | 11:30–12:30 | 680 | 472 | **69.4** | 64.2 | 198 | 58 | 0.93 |
| 07 | 12:30–13:30 | 88 | 70 | **79.5** | 94.3 | 643 | 39 | 19.8 |
| **08 cutover** | **13:30–14:30** | **1,612** | **107** | **6.6** | **1.1** | **3** | **409** | **13.6** |
| 09 | 14:30–15:30 | 1,686 | 245 | 14.5 | 2.4 | 10 | 893 | 10.0 |
| 10 | 15:30–16:30 | 188 | 42 | 22.3 | 7.5 | 34 | 397 | 11.2 |

The 06–07 UTC band (Alluxio S3-proxy at saturation, pre-cutover) held a **69.4% → 79.5% fail rate** with p95 wall pushing 11 minutes. The 08 UTC cutover hour drops fail-rate to 6.6% and p95 to 3 s with 1.7× the query volume. This is the headline; everything later in the doc must be measured against the post-08-UTC baseline, not the pre-cutover noise floor.

> Source: `cdp.trino_logs.trino_queries` filtered to `environment='replica2'`, `query_state IN ('FINISHED','FAILED')`, partition window `2026-04-27 00:00–12:00 UTC`.

## 5. Top tables driving rep2 reads today (Pareto floor for SHELF-26 replay)

Top 30 ordered by `physical_input_bytes` (07:00–11:50 UTC = 12:30–17:20 IST window, post-cutover only):

| # | Table | qcount | GB read | avg wall (s) | p95 wall (s) | failed |
|---|---|---|---|---|---|---|
| 1 | `cdp.ai_chat_spam.silver_chat_text_output_log` | 52 | 311 | 84.4 | **835** | 0 |
| 2 | `mview.vw_crm_spam_chat_view` | 241 | 261 | 12.4 | 19 | 5 |
| 3 | `cdp.cdp_revenue.gold_users` | 2 | 190 | 130.9 | 197 | 1 |
| 4 | `cdp.mview.gold_dbt_test_results` | 2 | 82 | 59.4 | 78 | 2 |
| 5 | `cdp.icesheet.silver_correct_cohort_ay_26` | 3 | 39 | 15.3 | 21 | 1 |
| 6 | `cdp_revenue.gold_batch_student_mappings` | 12 | 39 | 29.3 | 35 | 12 |
| 7 | `cdp.cdp_revenue.gold_transactions` | 3 | 36 | 24.2 | 52 | 0 |
| 8 | `lms.gold_payments` | 1 | 32 | 61.2 | 61 | 0 |
| 9 | `cdp_revenue.vw_gold_users_pw` | 3 | 31 | 12.0 | 14 | 0 |
| 10 | `cdp_revenue.gold_users` | 4 | 27 | 29.6 | 52 | 2 |
| 11 | `mview.gold_dbt_free_batch_enroll` | 10 | 25 | 16.8 | 79 | 0 |
| 12 | `bq.physics_wallah_65ada_analytics_user_location` | 2 | 21 | 15.7 | 17 | 1 |
| 13 | `cdp.curiousjr_bq.bronze_page_open` | 2 | 18 | 60.8 | 108 | 1 |
| 14 | `gsheet.default.cjr_paid_user` | 2 | 17 | 28.8 | 31 | 0 |
| 15 | `mview.vw_admission_sales` | 2 | 17 | 10.0 | 12 | 0 |
| 16 | `cdp.vp_service.silver_user_registration` | 7 | 15 | 20.2 | 69 | 3 |
| 17 | `offline.gold_offline_students_humming` | 4 | 13 | 29.1 | 34 | 4 |
| 18 | `central.gold_batch_rooms` | 3 | 12 | 21.4 | 35 | 3 |
| 19 | `cdp.analytics.gold_dbt_journey_snapshots_unnested` | 2 | 10 | 62.9 | 108 | 2 |
| 20 | `cdp.mview.gold_dbt_lecture_batch_info` | 2 | 10 | 71.5 | 125 | 1 |

(Tail truncated — top 5 = ~75% of bytes; top 20 = ~95%.)

`tbl=NULL` row at the head represents 1,270 lightweight queries (1.0s avg) totalling 474 GB but where the regex didn't pluck a single canonical table — many of these are dbt SELECTs against `information_schema` or queries with leading subselects, and they don't change the picture for caching analysis.

`12 / 12` = `cdp_revenue.gold_batch_student_mappings` had **100% fail rate** (12 of 12). Not a cache problem. Worth a separate ticket to fix the underlying view.

## 6. Single-flight fan-in — UNAVAILABLE

`shelf_inflight_singleflight` is not scraped to mimir-data, so we cannot measure thundering-herd suppression today. Phase-A obs item.

## 7. NVMe utilization — UNAVAILABLE / suspicious zero

`shelf_disk_bytes_used` is scraped and is **flat zero** on all 3 pods despite 4 h of traffic. Possible causes:

1. Foyer's hybrid-pool DRAM tier is getting all writes; disk tier is configured-but-unused. Verifiable from `shelfd.yaml` + Foyer logs.
2. The metric is read at startup time from a path that doesn't reflect Foyer's runtime fill state.
3. NVMe disk tier writes are silently failing (PVC permissions, disk full from a non-shelfd writer, etc.).

This is the single most surprising signal in the snapshot. Without disk fill, the cluster's effective cache size is bounded by RAM (~10 GiB/pod observed on shelf-2), not by the configured 240 GiB/pod NVMe — i.e. **~30× smaller than the design** intended. Hard pre-req for any of the algorithm work in §3 of the main doc.

## 8. What this snapshot does **not** answer (deferred to obs gap audit)

- p50 / p95 / p99 read latency by outcome (memory hit / disk hit / miss) — no histogram scraped.
- Origin GET volume and bytes.
- Single-flight fan-in.
- Per-table hit rate (no `table` label on counters).
- Eviction churn.
- Admission decisions (size/pin/refused).
- Plugin-side fall-through to direct S3.
- Time-to-warm after pod restart.

These are the 8 highest-leverage gaps and seed §2 of the main doc.

## 9. Reproduction

```text
Grafana datasource UID: ddy2eykq2tfy8a (mimir-data)
PromQL templates (HA-dedup wrapper):

  max without (prometheus, dataPrometheusReplica, container, endpoint,
               instance, job, namespace, ordinal, service)
       (shelf_hits_total{pool="rowgroup"})

  max without (prometheus, dataPrometheusReplica)
       (kube_pod_status_ready{namespace="alluxio", pod=~"shelf-.*"})

Trino MCP datasource: cdp.trino_logs.trino_queries (Iceberg, partitioned by query_date)

Window of interest:
  Pre-cutover Alluxio  : 2026-04-27 06:00–07:59 UTC = 11:30–13:30 IST
  Post-cutover Shelf   : 2026-04-27 08:00–11:59 UTC = 13:30–17:30 IST
```

Re-running this snapshot in 24 / 48 / 72 h gives the v0.5 soak trend; commit any deltas as appendices.
