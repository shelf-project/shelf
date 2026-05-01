# Shelf SLOs

This is the operator-owned, PR-gated source of truth for Shelf
service-level objectives. It transcribes plan §6 into PromQL queries
against placeholder metric names that `shelfd` will expose (tracked in
`shelfd/docs/metrics.md` — TBD).

Every row below has:

- **Primary metric** — what we commit to.
- **Guardrails** — adjacent metrics that must not degrade.
- **Target** — what "good" looks like.
- **Rollback threshold** — what forces us to pull the lever.
- **Dashboard** — where on-call checks the live value.
- **PromQL** — paste into Grafana Explore.

Placeholder metric names are the `shelf_*` family in
`observability/dashboards/shelf-overview.json`. When `shelfd/docs/metrics.md`
lands, names that diverge will be tracked under "Metric delta" at the
bottom of this doc.

---

## 6.1 — Phase −1 (Stabilisation — existing Trino `fs.cache`)

| Facet                   | Value |
|-------------------------|-------|
| Primary metric          | Trino `fs.cache` cumulative hit rate (scan-operator `cacheHitPct`) |
| Guardrails              | `QueryFailedEvent` rate ≤ 1.2× baseline; `<your_critical_dag>` ok-rate ≥ 99.9% |
| Target                  | ≥ 45% (5-day rolling) |
| Rollback threshold      | < 20% for 24h → revert hostPath migration on affected replica |
| Dashboard               | `trino-stability-overview` (existing) |

PromQL (from Trino operator summaries, re-exported by the existing
exporter):

```promql
sum(rate(trino_operator_scan_cache_hits_total[1d]))
  / (sum(rate(trino_operator_scan_cache_hits_total[1d])) +
     sum(rate(trino_operator_scan_cache_misses_total[1d])))
```

---

## 6.2 — Phase 0 (v0.1 PoC — plugin overhead)

| Facet                   | Value |
|-------------------------|-------|
| Primary metric          | `p99(plugin-enabled read) / p99(plugin-disabled read)` on shadow traffic |
| Guardrails              | `shelf_pod_cpu` < 50% limit; zero Shelf-attributed query failures |
| Target                  | ≤ 1.05× |
| Rollback threshold      | ≥ 1.15× for 1 h → disable plugin (`fs.shelf.enabled=false`) |
| Dashboard               | `shelf-overview` (new) |

PromQL (both series come from the plugin exporter that will land in
SHELF-10; names placeholder):

```promql
histogram_quantile(0.99, sum by (le) (
  rate(shelf_plugin_read_seconds_bucket{enabled="true"}[15m])
))
/
histogram_quantile(0.99, sum by (le) (
  rate(shelf_plugin_read_seconds_bucket{enabled="false"}[15m])
))
```

---

## 6.3 — Phase 0R (Redis-Gateway result cache)

| Facet                   | Value |
|-------------------------|-------|
| Primary metric          | BI-user result-cache hit rate (Gateway plugin) |
| Guardrails              | Gateway p95 query latency ≤ baseline; HMS load from SnapshotWatcher stable |
| Target                  | ≥ 60% after 5 days |
| Rollback threshold      | < 20% for 24h OR HMS p95 > 2× baseline → disable plugin |
| Dashboard               | `trino-gateway` (existing) |

PromQL (from the Gateway plugin exporter):

```promql
sum(rate(trino_gateway_result_cache_hits_total[1h]))
  / (sum(rate(trino_gateway_result_cache_hits_total[1h])) +
     sum(rate(trino_gateway_result_cache_misses_total[1h])))
```

---

## 6.4 — Phase 1 (v0.5 gate on rep-2) — **kill-switch** (ADR-0010)

Five concurrent primary metrics. Missing ANY one for 7 consecutive
days kills the project.

### 6.4.1 Cumulative hit rate

| Facet                   | Value |
|-------------------------|-------|
| Primary metric          | `shelf_hits_total / (shelf_hits_total + shelf_misses_total)`, 7-day window |
| Guardrails              | Per-pool hit rate: `pool.metadata` ≥ 95%, `pool.rowgroup` ≥ 65% |
| Target                  | ≥ 71% (Alluxio baseline from E12) |
| Rollback threshold      | < 60% for any 24-h window → flip `fs.shelf.enabled=false` |
| Dashboard               | `shelf-overview` row 1 big-number |
| Alert                   | `ShelfHitRateTooLow` (fires at < 60% for 10m) |

```promql
sum(rate(shelf_hits_total[7d]))
  / (sum(rate(shelf_hits_total[7d])) + sum(rate(shelf_misses_total[7d])))
```

### 6.4.2 `<your_critical_dag>` ok-rate

| Facet                   | Value |
|-------------------------|-------|
| Primary metric          | `<your_critical_dag>` DAG task ok-rate (Airflow) |
| Guardrails              | No increase in dbt failure reasons attributed to Shelf |
| Target                  | ≥ 99.9% over rolling 7 days |
| Rollback threshold      | < 99.5% for 24h → flip `fs.shelf.enabled=false` |
| Dashboard               | `airflow-dbt` (existing) + cross-linked from `shelf-overview` |

```promql
sum(rate(airflow_task_instance_success_total{dag_id="<your_critical_dag>"}[7d]))
  / sum(rate(airflow_task_instance_end_total{dag_id="<your_critical_dag>"}[7d]))
```

### 6.4.3 Rep-2 p95 query latency

| Facet                   | Value |
|-------------------------|-------|
| Primary metric          | `histogram_quantile(0.95, rate(trino_query_duration_seconds_bucket[5m]))` |
| Guardrails              | p50 not drifting > 10% vs baseline |
| Target                  | ≤ 120% of Alluxio baseline (E12-derived) |
| Rollback threshold      | > 140% for 1h → flip `fs.shelf.enabled=false` |
| Dashboard               | `shelf-v05-gate` (new) |

```promql
histogram_quantile(0.95, sum by (le) (
  rate(trino_query_duration_seconds_bucket{cluster="rep-2"}[5m])
))
```

### 6.4.4 Shelf-attributed pages

| Facet                   | Value |
|-------------------------|-------|
| Primary metric          | Count of PagerDuty incidents with `service=shelf` label |
| Guardrails              | None — this is a binary metric: zero or not zero |
| Target                  | 0 over rolling 7 days |
| Rollback threshold      | ≥ 1 → incident review; gate does not auto-reset |
| Dashboard               | `shelf-v05-gate` (new) |

```promql
sum(increase(pagerduty_incidents_total{service="shelf"}[7d]))
```

### 6.4.5 Oncall surface

| Facet                   | Value |
|-------------------------|-------|
| Primary metric          | Pages + runbook lookups + Slack incidents tagged `#shelf-oncall` per week |
| Guardrails              | None |
| Target                  | ≤ 50% of Alluxio's 7-day rolling count (E12 baseline) |
| Rollback threshold      | > Alluxio baseline for 14 days → revisit scope (not a rollback but a scope question) |
| Dashboard               | `shelf-v05-gate` (new) |

```promql
(
  sum(increase(pagerduty_incidents_total{service="shelf"}[7d]))
  +
  sum(increase(runbook_page_views_total{service="shelf"}[7d]))
  +
  sum(increase(slack_incident_channel_messages_total{channel="shelf-oncall"}[7d]))
)
```

---

## 6.5 — Phase 2 (plan-aware prefetch — TTFQ)

| Facet                   | Value |
|-------------------------|-------|
| Primary metric          | p95 TTFQ (time-to-first-query) after 10× worker scale-up |
| Guardrails              | Prefetch listener blocking coordinator < 10ms median; prefetch queue depth ≤ 1024 |
| Target                  | ≤ 3 s p95 |
| Rollback threshold      | Listener blocking p95 > 50ms OR queue at capacity for > 5m → `shelf.prefetch.enabled=false` |
| Dashboard               | `shelf-prefetch` (new, Phase 2) |

```promql
histogram_quantile(0.95, sum by (le) (
  rate(shelf_bench_ttfq_seconds_bucket[30m])
))
```

---

## 6.6 — Phase 3 / 4 / 5 operational metrics

### NVMe admit bytes cut (Phase 4)

| Facet                   | Value |
|-------------------------|-------|
| Primary metric          | `increase(shelf_nvme_bytes_admitted_total[7d])` post-pin-list vs baseline |
| Target                  | ≥ 40% reduction vs v0.5 |
| Rollback threshold      | Hit rate regression > 3 pp → revert pin list (`evict-poisoned-key.md` + prior S3 version) |
| Dashboard               | `shelf-overview`, panel "Admission vs fall-through" |

```promql
1 - (
  sum(increase(shelf_nvme_bytes_admitted_total{phase="4"}[7d]))
  /
  sum(increase(shelf_nvme_bytes_admitted_total{phase="0.5"}[7d]))
)
```

### Rep-2 Alluxio retirement (Phase 5)

| Facet                   | Value |
|-------------------------|-------|
| Primary metric          | `alluxio-worker` replicas on rep-2 |
| Target                  | 0 for 7 consecutive days |
| Rollback threshold      | > 0 unexpectedly → eng-lead review |
| Dashboard               | `trino-rep2-overview` (existing) |

```promql
max(kube_statefulset_status_replicas{namespace="alluxio", statefulset=~"alluxio-worker.*"})
```

---

## 6.7 — Phase 6 (full rollout)

Same metrics as 6.4.1–6.4.5, evaluated per replica. Gate: pass on each
replica individually for 7 consecutive days.

PromQL pattern (example for hit rate):

```promql
sum by (cluster) (rate(shelf_hits_total[7d]))
  / (sum by (cluster) (rate(shelf_hits_total[7d]))
     + sum by (cluster) (rate(shelf_misses_total[7d])))
```

---

## 6.8 — Phase 7 (OSS launch)

Operator-facing SLO is: **no regression in production Shelf SLOs
during launch week**. See plan §6.8. No new PromQL; gate is the
conjunction of 6.4.1–6.4.5 evaluated during the launch window.

---

## Metric delta vs `shelfd/docs/metrics.md`

To be filled in when `shelfd/docs/metrics.md` lands. Any metric name
change here requires a PR against this file AND the dashboard + alert
YAMLs.

Metrics referenced from this doc (placeholder contract with `shelfd`):

- `shelf_hits_total{pod, pool}`
- `shelf_misses_total{pod, pool, reason}`
- `shelf_read_latency_seconds_bucket{transport="http2", pod}`
- `shelf_plugin_fallthrough_total`
- `shelf_plugin_requests_total`
- `shelf_plugin_read_seconds_bucket{enabled}`
- `shelf_dram_bytes_used{pod, pool}`
- `shelf_nvme_bytes_used{pod}`
- `shelf_nvme_bytes_capacity{pod}`
- `shelf_nvme_bytes_admitted_total{pod, phase}`
- `shelf_admissions_total{pod, decision}`
- `shelf_admission_refused_total{pod}`
- `shelf_admission_model_enabled`
- `shelf_admission_model_promoted_timestamp_seconds`
- `shelf_pin_list_promoted_timestamp_seconds`
- `shelf_circuit_breaker_state{pod, target_pod}`
- `shelf_corruption_detected_total{pod}`
- `shelf_tenant_bytes_used{tenant}`
- `shelf_tenant_bytes_quota{tenant}`
- `shelf_evictions_total{tenant, reason}`
- `shelf_bench_ttfq_seconds_bucket`

Non-shelfd metrics expected in the cluster:
`kube_pod_container_status_restarts_total`,
`kube_statefulset_status_replicas*`,
`trino_operator_scan_cache_hits_total`,
`trino_query_duration_seconds_bucket`,
`trino_gateway_result_cache_*`,
`airflow_task_instance_*`, `pagerduty_incidents_total`.
