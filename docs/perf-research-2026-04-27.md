# Shelf perf — research roadmap, 2026-04-27

> Live state, observability gaps, paper-cited algorithm survey, phased SHELF-NN ticket roadmap, and a 4-item shipping cap per phase. Companion files: `[perf-research-2026-04-27/snapshot.md](./perf-research-2026-04-27/snapshot.md)` (Phase-0 hard numbers), `[perf-research-2026-04-27/refs.bib](./perf-research-2026-04-27/refs.bib)` (BibTeX).

## 1. Live state — rep2, 4 hours after cutover

Full numbers in `[snapshot.md](./perf-research-2026-04-27/snapshot.md)`. Headline:

- **Reliability win is huge, latency win is moderate.** Pre-cutover hour: 79.5 % fail rate, p95 wall = 643 s. Post-cutover hour: 6.6 % fail rate, p95 = 3 s. The 14–22 % residual fail rate in the next two hours is `USER_CANCELED` + `INVALID_VIEW` + `ICEBERG_`* — not a cache issue.
- **Cluster cache is effectively single-pod.** `shelf-2` is doing 39.9 MB/s ingress and 100 % of recorded hit/miss traffic. `shelf-0` and `shelf-1` are at 100 B/s baseline, 13 MiB RSS, 0 hits in the last 6 h. Whatever is routing reads is collapsing onto one pod.
- **NVMe disk pool is empty.** `shelf_disk_bytes_used = 0` on all three pods despite 4 h of traffic, while `shelf-2` RSS is 10.46 GiB. All caching today is in DRAM/Foyer-memory tier; the configured 240 GiB/pod NVMe is unused. Effective cache size is **~30× smaller** than design.
- **The hit-counter resets internally without the pod restarting.** kube_pod_container_status_restarts_total = 0; the rowgroup hit counter rotated 360 k → 789 → climbed → 459 k → 7 853 inside the same 6 h window. Some inner Foyer engine reset is wiping warm state. Just before the last reset we were at 65.3 % rowgroup hit ratio; we're back at 40.9 % cold.
- **Strong Pareto on workload.** Top 5 tables (`silver_chat_text_output_log`, `vw_crm_spam_chat_view`, `cdp_revenue.gold_users`, `gold_dbt_test_results`, `silver_correct_cohort_ay_26`) drove **75 %** of physical bytes today. A 5-row pin-list captures most of the value, before any algorithmic work.

Three of these — pod skew, empty NVMe, internal counter reset — are bugs hiding behind the dashboard, not directions for new research. Phase A starts with surfacing them.

---

## 2. Observability gap audit

Seventeen of the 21 metric families declared in `[shelfd/src/metrics.rs](../shelfd/src/metrics.rs)` are not visible in `mimir-data` today. The dashboard (`[charts/shelf/grafana/dashboards/shelf-read-path.json](../charts/shelf/grafana/dashboards/shelf-read-path.json)` + the org's `shelf-overview` Grafana dashboard) renders many panels off series that never arrive — the result is a green dashboard masking the structural issues in §1.

This is the first thing to fix. Without latency-by-outcome, admissions, evictions, origin volume, and per-table labels, every algorithm comparison in §3 is unfalsifiable. The list below ranks **12 missing or under-leveraged signals** by leverage; the implementation cost for almost all of them is "increment an existing counter from a hot path that already runs". The raw cost is therefore wiring + scrape config + dashboard panels, not Rust changes.

> Convention: **G-N — name** *(metric proposal)* — what it answers — concrete fix.

### G-1 — Latency by outcome  *(`shelf_request_seconds{path,outcome}`, already declared)*

What it answers: is a hit_memory really 100 µs, a hit_disk really 2 ms, a miss really 30 ms? The single-line `p95(rate(...))` panel today mixes all three and its number is meaningless.

Why it's missing in prod: declared and registered, but the histogram has zero observed children in the production binary — either the hot path isn't calling `.observe()` or the 0.1.0-preview image was cut before that code landed. Verify with `kubectl exec shelf-2 -c shelfd -- curl -s localhost:9090/metrics | grep ^shelf_request_seconds`.

Fix: 1-day audit of `shelfd/src/http.rs` + `s3_shim.rs` to confirm the histogram is observed on every request path. Add a unit test asserting at least one observed bucket per `outcome` value after a successful end-to-end test. Then add three panels to `shelf-overview`: p50, p95, p99 each split by outcome.

### G-2 — NVMe / DRAM tier split  *(`shelf_dram_bytes_used`, `shelf_nvme_bytes_used`, `shelf_nvme_bytes_capacity`)*

What it answers: are we actually using the 240 GiB/pod NVMe? The snapshot shows `shelf_disk_bytes_used=0`. Either Foyer's hybrid pool isn't writing to disk or the gauge is reading the wrong path.

Fix:

1. Confirm in `[shelfd/src/store.rs](../shelfd/src/store.rs)` where `disk_bytes_used` is sampled — likely a Foyer API call that returns NVMe tier bytes. If it's a wrapper around a working method, this is a config issue (the hybrid pool isn't enabled in `values-prod.yaml`). If it's a stub that always returns 0, this is a code issue.
2. Add `shelf_dram_bytes_used` as a separate gauge to break out the in-memory tier so the operator can see "10 GiB RAM, 0 GiB NVMe" at a glance.
3. Alert when `shelf_disk_bytes_used / shelf_disk_bytes_capacity < 0.01` after 30 min of traffic. This single alert would have caught the issue on day 1.

### G-3 — Pod-level traffic skew  *(`shelf_s3_shim_response_bytes_total`, already declared)*

What it answers: is HRW / CHWBL / kube-proxy fan-out working? The snapshot shows shelf-2 at 39.9 MB/s and the others at 100 B/s. This is in-band data, not external observation.

Fix: declared but not scraped. Verify children are being incremented in `s3_shim.rs`. The dashboard already has a panel referencing this; just needs the scrape to actually surface it.

Companion: `kube-proxy` mode (iptables vs ipvs) determines whether ClientIP affinity collapses to one pod under steady DNS. A single-line panel `count by (pod) (rate(shelf_s3_shim_response_bytes_total[5m]) > 0)` instantly tells you "1 of 3 pods receiving traffic".

### G-4 — Per-table hit rate  *(extend `shelf_hits_total` / `shelf_misses_total` with `table` label)*

What it answers: which tables are warm (95 % hits) and which are cold (5 % hits)? The current global hit ratio is a single number that hides per-table behaviour. The Pareto in §1 says 5 tables drive 75 % of bytes; we can't currently say "is `silver_chat_text_output_log` warm yet?".

Cardinality is bounded by ≤ 500 tables in the `cdp` catalog plus the few cross-catalog reads, well within Prometheus practice. Add the label, plumb the table name from the Trino-issued S3 path through `s3_shim.rs`. Estimated 2-day work.

### G-5 — Per-fingerprint hit rate  *(`shelf_queries_served_total{fingerprint,tenant}`, declared)*

What it answers: dashboard X is 95 % warm, dashboard Y is 5 % cold. Lets product-side teams know which queries to expect to be fast. Already declared; needs the plugin-side fingerprint header populated and the counter incremented from the shim.

Bounded by the existing 200-fingerprint cap in metrics.rs.

### G-6 — Eviction churn  *(`shelf_evictions_total{pool, reason}`, declared)*

What it answers: are we evicting useful data? Without this, MRC analysis (Mattson stack distance, see §3 admission/eviction) is impossible. Reason should be one of `capacity`, `ttl`, `admin`, `unpin`, `reload`. Plumb from Foyer's eviction callback in `store.rs`.

### G-7 — Admission decisions  *(`shelf_admissions_total{pool, decision}`, declared)*

What it answers: how often is the size-threshold gate rejecting? If it's >50 %, the gate is too tight. If it's <5 %, every byte that arrives is admitted, and the only thing keeping the cache from getting polluted is eviction.

Hot-path increment in `[shelfd/src/admission.rs](../shelfd/src/admission.rs)` — counter is declared, just needs increments. Estimated 0.5 day.

### G-8 — Origin volume / cache-byte-efficiency KPI  *(`shelf_origin_request_bytes_total`, declared)*

What it answers: how many bytes did we save vs the S3 origin? The headline business metric. Computed as `1 - (origin_bytes / s3_shim_response_bytes)`; also feeds the cost dashboard.

Declared but not visible. Same wiring fix as G-1.

### G-9 — Single-flight fan-in  *(`shelf_inflight_singleflight{pool}`, declared as gauge)*

What it answers: thundering-herd suppression. A spike of N concurrent split-source workers requesting the same row group should collapse into 1 origin call; the gauge should show the fan-in factor.

Declared, gauge — needs the `get_or_fetch` path in `store.rs` to bump on entry and decrement on exit.

### G-10 — Engine-reset counter  *(new `shelf_engine_resets_total`)*

What it answers: the snapshot showed shelf-2's hit counter rotating 5+ times in 6 h while pod restart count = 0. Something inside the process is hot-resetting Foyer state. We need to count it, surface it, and root-cause it.

New metric, 5-line addition to wherever `FoyerStore::reset` (or equivalent) is called. Then alert on `increase(shelf_engine_resets_total[1h]) > 0` so it's seen the moment it happens.

### G-11 — Time-to-warm SLI  *(`shelf_time_to_warm_seconds` histogram)*

What it answers: after a pod start (or engine reset, see G-10), how long until hit ratio crosses 50 %? Critical for evaluating spot-churn tolerance and pod restart cost. With Karpenter spot rotation, this directly bounds availability.

Implementation: at process start, log `t0`. Sample hit ratio every minute. When it crosses thresholds 25 / 50 / 75 / 90 %, emit a histogram observation `t_now - t0` keyed on the threshold. Or use a simpler `shelf_warm_threshold_crossed_seconds{threshold}` counter.

### G-12 — Plugin-side fall-through rate  *(`shelf_plugin_fallthrough_total`, dashboard refers to this)*

What it answers: how often did the Trino S3 client circuit-break or 5xx away from shelfd and read directly from S3? Today rep2's catalog points `s3.endpoint` at shelfd, and Trino's native S3 client has no fallback — so this counter is currently always zero by design (`SHELF-22` shim, ADR-0012). But once SHELF-29 / blob-cache SPI / circuit breaker lands, this becomes the most important counter for blast-radius.

Belongs on the Java client side (`clients/trino/src/main/java/io/shelf/filesystem/`), not shelfd. Tracked separately because the metric source is different.

### Summary table — observability gap leverage


| ID   | Signal                          | Status                     | Effort | Leverage on §3 work                               |
| ---- | ------------------------------- | -------------------------- | ------ | ------------------------------------------------- |
| G-1  | Latency by outcome              | declared, no children      | 1 d    | High — every algorithm comparison needs this      |
| G-2  | NVMe vs DRAM split + alert      | gauge wrong path           | 1 d    | High — without disk fill, nothing else matters    |
| G-3  | Pod-level shim bytes            | declared, not scraped      | 0.5 d  | High — single-pod issue is invisible today        |
| G-4  | Per-table hit rate              | new label                  | 2 d    | High — needed for pin-list, MRC, admission        |
| G-5  | Per-fingerprint hit rate        | declared, plugin-side wire | 1 d    | Medium — feeds cost dashboard                     |
| G-6  | Evictions by reason             | declared, not incremented  | 1 d    | High — needed for any eviction comparison         |
| G-7  | Admission decisions             | declared, not incremented  | 0.5 d  | High — needed for any admission comparison        |
| G-8  | Origin volume / byte-efficiency | declared, not scraped      | 0.5 d  | High — primary KPI                                |
| G-9  | Single-flight fan-in            | declared, gauge            | 0.5 d  | Medium — useful for thundering-herd debugging     |
| G-10 | Engine-reset counter            | new metric                 | 0.25 d | Critical — explains observed counter resets       |
| G-11 | Time-to-warm SLI                | new histogram              | 1 d    | Medium — needed for spot-churn evaluation         |
| G-12 | Plugin fall-through             | new metric, Java side      | 1 d    | Medium — only matters once circuit-breaker exists |


Total cost: **~10 engineer-days** to close every gap. Approximate split: 5 d wiring already-declared metrics into hot paths (G-1, G-6, G-7, G-8, G-9), 3 d new metrics (G-10, G-11, G-12 on Java), 2 d new labels (G-4, G-5). One observability sprint, not a quarter.

---

## 3. Algorithm survey — 6 axes

For each axis we score candidates on: paper / origin, expected impact band on Shelf's workload, engineering cost, and **Shelf-fit** — does the candidate slot into the Foyer + S3-shim architecture without re-platforming?

Workload assumptions used throughout (from §1 and the SHELF-26 replay design):

- 50 % of bytes come from < 30 distinct (table, file) pairs (heavy Pareto).
- Object size distribution is bimodal: ≤ 1 MiB metadata + footers (Iceberg manifests, Parquet footer, page-index), and 0.5–8 MiB row groups.
- Single-flight fan-in is high during DAG / dbt runs (10–100 concurrent splits hit the same row group).
- Reads dominate ~95:5 over writes (writes only happen via the metadata-pool path for `.alluxio_s3_api_metadata` writes, irrelevant for shelfd).
- Long-tailed cold tail: ~10 % of bytes come from one-off interactive queries that won't repeat in any window.
- Pod set rotates on Karpenter spot churn every few hours, so cold-start matters.

### 3.1 Admission

Today: size-threshold + pin-list (ADR-0003). Anything below 8 MiB is admitted; pin-list tags MV files for guaranteed admission. There is **no frequency-aware filter**, so a stream of one-shot 4 MiB row groups can pollute the working set.


| Candidate                                           | Origin                                                                                      | Impact band                                                                                                               | Eng cost                                                                                       | Shelf-fit                                                                                                                                     |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| **W-TinyLFU + Doorkeeper**                          | Einziger & Friedman, USENIX ATC 2017 [@einziger2017tinylfu]                                 | +5 to +15 pp hit rate vs LRU-class baselines on Zipfian; well-validated in Caffeine & Cachelib                            | 1 wk: a `tinylfu` Rust crate exists; slots in front of Foyer admission as a probabilistic gate | High — pure pre-filter, doesn't touch Foyer's eviction. The Doorkeeper bloom suppresses the singleton problem we know we have.                |
| **Size-aware admission (existing) + LightGBM gate** | Project doc [SHELF-TIER5](../shelfd/docs/design-notes/SHELF-TIER5-lightgbm-escape-hatch.md) | Conditional: only worth shipping if SHELF-26 replay shows ≥ 5 pp lift over W-TinyLFU baseline                             | 3 wk: train + serve + eval                                                                     | Medium — feature engineering and training pipeline are non-trivial. Per AGENTS.md the project rejected ONNX; LightGBM is the sanctioned path. |
| **AdaptSize**                                       | Berger et al., NSDI 2017 [@berger2017adaptsize]                                             | +5–10 pp on long-tailed CDN workloads; original goal is similar to Shelf's bimodal sizes                                  | 1.5 wk: per-pool integration with online tuning of size threshold                              | High — generalises today's static threshold. Easy to deploy with a feature flag and AB.                                                       |
| **Cachelib's hybrid admission gating**              | Berg et al., OSDI 2020 [@berg2020cachelib]                                                  | Engineering reference rather than a swap target — informs the wiring for retry budgets, hot/cold tiers, and single-flight | n/a (design reference)                                                                         | High                                                                                                                                          |


**Recommendation.** Run W-TinyLFU first (lowest risk, paper-cited, well-engineered library available). Hold the LightGBM gate (B1-LGBM) until SHELF-26 produces a replay that shows W-TinyLFU under-performs by ≥ 5 pp on rep2's actual mix.

### 3.2 Eviction

Today: Foyer 0.x runs S3-FIFO with default ghost-queue ratios. We have no telemetry on eviction (G-6) so the bake-off below has to ride on offline replay until G-6 lands.


| Candidate                     | Origin                                              | Impact band                                                                               | Eng cost                                                              | Shelf-fit                                                               |
| ----------------------------- | --------------------------------------------------- | ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| **S3-FIFO (current default)** | Yang et al., SOSP 2023 [@yang2023s3fifo]            | Baseline; paper claims ≤ LRU efficiency at fraction of CPU. Beats LRU on most CDN traces. | 1 d: tune `small_fifo_ratio` and `ghost_size` against SHELF-26 replay | High — already in Foyer                                                 |
| **SIEVE**                     | Zhang et al., NSDI 2024 [@zhang2024sieve]           | -1 to +5 pp hit rate vs LRU at half the metadata overhead; higher gains on web/CDN traces | 1 wk if Foyer ships a SIEVE plugin; ~2 wk if we have to write one     | Medium — Foyer's plugin surface for evictioners is small but extensible |
| **LeCaR**                     | Vietri et al., HotStorage 2018 [@vietri2018lecar]   | Hybrid LRU + LFU with regret-minimising weights. +2 to +8 pp on mixed workloads.          | 1.5 wk                                                                | Medium — adds a per-key weight vector; memory cost ~16 B/key            |
| **CACHEUS**                   | Rodriguez et al., FAST 2021 [@rodriguez2021cacheus] | Successor to LeCaR with lower CPU and better tail behaviour. +3 to +7 pp consistently.    | 2 wk                                                                  | Medium                                                                  |
| **ARC**                       | Megiddo & Modha, FAST 2003 [@megiddo2003arc]        | Classical adaptive baseline; +2 to +5 pp vs LRU. Simple but Foyer-replacement-grade.      | 1 wk                                                                  | High — well-known reference implementations                             |


**Recommendation.** First land G-6 (eviction telemetry) so the bake-off is measurable on live traffic. Then run all five against SHELF-26 with the rep2-2026-04-27 trace. Keep S3-FIFO as default unless SIEVE or CACHEUS delivers ≥ 3 pp at equal or better p99.

### 3.3 Prefetch

Today: B3+E3 ships range coalescing with a fixed 15 ms window and a fixed 128 KiB footer read-ahead. There's a `[SHELF-I3-rl-prefetch.md](../shelfd/docs/design-notes/SHELF-I3-rl-prefetch.md)` design note with a PPO-style RL policy planned. **The design note flags risk: live RL policies need extensive offline training, online safety checks, and SHELF-26 replay validation before production.**


| Candidate                                              | Origin                                                                                                      | Impact band                                                            | Eng cost                                                                       | Shelf-fit                                                                              |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| **Adaptive footer read-ahead window (PID controller)** | Generic adaptive prefetch — see Kraska et al., SIGMOD 2018 for the framework [@kraska2018learned]           | +2 to +5 pp hit rate on metadata pool; cheap origin GET reduction      | 3 d: ~30 LOC PID loop in `[shelfd/src/coalesce.rs](../shelfd/src/coalesce.rs)` | High — knob already exists, just needs a feedback loop on `hit-on-readahead-bytes`     |
| **Adaptive read-ahead per file**                       | Schroeder & Harchol-Balter, USENIX ATC 2003 [@schroeder2003web] (timeless work on prefetch-quality scoring) | +3 to +8 pp on bimodal workloads                                       | 1 wk                                                                           | High                                                                                   |
| **LRB — Learning Relaxed Belady**                      | Song et al., NSDI 2020 [@song2020lrb]                                                                       | +5 to +12 pp hit rate vs LRU-class baselines; productionised at Akamai | 3–4 wk: feature pipeline, online inference, AB harness                         | Medium — recommend running LRB before PPO/RL. Lower risk, paper-cited deployment story |
| **PPO-style RL prefetch (SHELF-I3)**                   | Doc [SHELF-I3](../shelfd/docs/design-notes/SHELF-I3-rl-prefetch.md)                                         | +5 to +15 pp upper bound; high variance                                | 6+ wk research spike                                                           | Low — see SHELF-I3 risk section. Defer until LRB tops out                              |


**Recommendation.** PID-controller adaptive read-ahead lands as a Phase-A quick win. LRB is the right Phase-C spike; PPO is deferred until LRB demonstrably plateaus.

### 3.4 Indexing / row-group skip

Today: no metadata-side filter; every range read goes to disk/origin even if the predicate would have rejected the row group. SHELF-G2 (side blooms) and SHELF-G3 (sort-order awareness) are designed but not built.


| Candidate                          | Origin                                                                                                                      | Impact band                                                                                                  | Eng cost | Shelf-fit                                                                                                   |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | -------- | ----------------------------------------------------------------------------------------------------------- |
| **Side blooms over min/max stats** | `[SHELF-G2](../shelfd/docs/design-notes/SHELF-G2-side-blooms.md)`                                                           | +5 to +25 pp byte-skip on equality-predicate workloads                                                       | 2 wk     | High                                                                                                        |
| **Learned blooms**                 | Mitzenmacher 2018 (Sandwiched Learned Bloom Filters) [@mitzenmacher2018sandwiched]; Kraska et al. 2018 [@kraska2018learned] | Halves bloom memory at equal FPR; effectively doubles the filter coverage we can afford in the metadata pool | 3 wk     | Medium — model training loop                                                                                |
| **SuRF — Succinct Range Filters**  | Zhang et al., SIGMOD 2018 [@zhang2018surf]                                                                                  | Range pushdown without scanning manifests; 5–15× speedup on range-predicate scans                            | 2.5 wk   | High — Iceberg manifests already carry `lower_bounds`/`upper_bounds`; SuRF is a one-shot build per manifest |
| **Iceberg page-index**             | Iceberg 1.6 spec                                                                                                            | Native row-group skip via Iceberg metadata; complementary to blooms                                          | 1.5 wk   | High                                                                                                        |


**Recommendation.** SuRF is the highest-leverage single addition. Side blooms work for equality; SuRF gives us range. Combine, don't choose.

### 3.5 Coalescing

Today: fixed 15 ms window in `[shelfd/src/coalesce.rs](../shelfd/src/coalesce.rs)` (see `[SHELF-B3E3-range-coalescing.md](../shelfd/docs/design-notes/SHELF-B3E3-range-coalescing.md)`). Window is short-circuited by some workloads and over-shoots others.


| Candidate                                         | Origin                              | Impact band                                            | Eng cost | Shelf-fit                                         |
| ------------------------------------------------- | ----------------------------------- | ------------------------------------------------------ | -------- | ------------------------------------------------- |
| **Static tuned window**                           | n/a                                 | +0 to +3 pp                                            | 1 d      | High — sweep `DEFAULT_WINDOW_MS` against SHELF-26 |
| **Per-table window via online learning (LinUCB)** | Li et al., WWW 2010 [@li2010linucb] | +3 to +7 pp; bigger gains on mixed-fingerprint traffic | 2.5 wk   | Medium — needs telemetry G-4                      |
| **Adaptive read-ahead inside coalesce**           | Same as 3.3                         | n/a (overlap)                                          | n/a      | n/a                                               |


**Recommendation.** Quick static tune as Phase-A; LinUCB only after G-4 lands and we know per-table win sizes are real.

### 3.6 Placement / sharding

Today: HRW (Highest Random Weight) keys reads to one of N shelfd pods. The snapshot proves HRW is collapsing onto shelf-2 in practice. Either the keys aren't well-distributed or there's a downstream pin (Service VIP / kube-proxy / DNS).


| Candidate                                         | Origin                                           | Impact band                                                                        | Eng cost | Shelf-fit                                                                          |
| ------------------------------------------------- | ------------------------------------------------ | ---------------------------------------------------------------------------------- | -------- | ---------------------------------------------------------------------------------- |
| **CHWBL — Consistent Hashing with Bounded Loads** | Mirrokni et al., SODA 2018 [@mirrokni2018chwbl]  | Caps any pod at `(1+ε)·avg`; would force shelf-2's overflow onto shelf-0 / shelf-1 | 1 wk     | High — but requires a coordinated key router. Sits in the Java client (or a proxy) |
| **Maglev**                                        | Eisenbud et al., NSDI 2016 [@eisenbud2016maglev] | Stable assignment under pod set changes; same load uniformity as CHWBL             | 1.5 wk   | Medium — needs a control-plane sync of the lookup table                            |
| **HRW (current)**                                 | Thaler & Ravishankar 1996 [@thaler1996hrw]       | Baseline                                                                           | n/a      | n/a                                                                                |
| **Plain consistent hashing**                      | Karger et al., STOC 1997                         | Worse load distribution than HRW; not a step forward                               | n/a      | n/a                                                                                |


**Recommendation.** CHWBL behind G-3 telemetry. But before any algorithm change, **debug why HRW collapses onto one pod today**: is it the Trino S3 client connection-reuse, kube-proxy iptables hash quirk, or actually a HRW key collision? CHWBL solves the wrong problem if the issue is connection reuse.

### Cross-cutting techniques worth flagging

- **Themis-style cache partitioning** [@mahgoub2024themis] — when we add multi-tenant SLOs, partition the cluster cache by tenant share so `commonuser` floods can't starve `mbuser_admin`. Per-tenant working-set caps + share-based admission. Phase B candidate.
- **Subplan caching** — Snowflake's elastic cache paper [@vuppalapati2024cache] shows subplan-granular caching is >10× cheaper than rowgroup caching when CTEs repeat. Validate the hypothesis on rep2 by counting repeated query-fingerprints first; if it's <5 % of queries, drop. SHELF-I1 is the existing design note.
- **Cliffhanger-style compression** [@cidon2017cliffhanger] + path-prefix dictionary on the metadata pool — halves metadata pool memory at the cost of CPU. Phase C.
- **Cachelib peer-failover warmup** [@berg2020cachelib] — when a pod rotates, neighbours stream their hottest 1 % via a sidecar. Ties into G-11 (time-to-warm SLI). `[SHELF-E6](../shelfd/docs/design-notes/SHELF-E6-peer-failover.md)` is the existing design.

---

## 4. Phased roadmap — SHELF-NN tickets

Each item has an indicative SHELF-NN suggestion (the actual ID is assigned when the ticket lands in `[agents/out/03-plan.md](../agents/out/03-plan.md)`) and a hard go/no-go gate. The 4-item cap per phase is enforced in §5.

### Phase A — quick wins (1–2 weeks)

Theme: **close the observability blindfold and do the bug-fixes the snapshot revealed.** Don't disturb v0.5 soak with algorithm swaps yet.

- **A1 — SHELF-G1 — Latency-by-outcome wiring + dashboard split.** Scope: G-1 (audit + fix histogram increments) and add p50/p95/p99 panels. Effort: 1 d. Gate: `shelf_request_seconds` returns ≥ 1 observed bucket per outcome on shelf-2 within 5 min of deploy.
- **A2 — SHELF-G2 — NVMe RCA + DRAM/NVMe split gauge + alert.** Scope: G-2. Effort: 1 d. Gate: after deploy, `shelf_disk_bytes_used > 1 GiB` on shelf-2 within 30 min of post-cutover traffic; alert fires within 30 min if not.
- **A3 — SHELF-G3 — Pod-traffic-skew RCA.** Scope: G-3 telemetry on, then root-cause whether it's HRW key collapse, Trino client connection reuse, or kube-proxy. Effort: 1 d obs + up to 3 d RCA. Gate: shelf-0 / shelf-1 each show ≥ 5 MB/s ingress on a representative workload.
- **A4 — SHELF-26-tune — S3-FIFO ghost-queue sweep on the replay harness.** Scope: run `[SHELF-26 replay](../shelfd/docs/design-notes/SHELF-26-replay-harness.md)` with the rep2-2026-04-27 trace and sweep `small_fifo_ratio ∈ {0.05, 0.10, 0.15}` and `ghost_size ∈ {0.5×, 1.0×, 2.0× capacity}`. Ship best-perf default. Effort: 2 d. Gate: ≥ 1 pp hit-rate lift over default in replay; if zero, ship as a no-change.
- **A5 — SHELF-G6E7 — Eviction + admission counters incremented.** Scope: G-6 + G-7. Effort: 1 d. Gate: `shelf_evictions_total` and `shelf_admissions_total` show non-zero rates on shelf-2 after 1 h.
- **A6 — SHELF-G10 — Engine-reset counter + alert.** Scope: G-10. Effort: 0.5 d. Gate: counter increments on the next observed reset; reset cause logged.
- **A7 — SHELF-PIN — Phase-1 pin-list = top 5 tables from §1 Pareto.** Scope: pre-warm `silver_chat_text_output_log`, `vw_crm_spam_chat_view`, `gold_users`, `gold_dbt_test_results`, `silver_correct_cohort_ay_26` via the pinlist mechanism. Effort: 0.5 d. Gate: per-table hit rate (G-4) ≥ 90 % on each within 4 h of deploy.

Phase A total notional effort: ~9 d. Realistic completion in 2 weeks with one engineer at 60 % focus.

### Phase B — algorithm swaps (next quarter)

Theme: **with telemetry on, run paper-cited swaps and AB them.** Each item ships behind a feature flag. Each is single-replica AB on rep2 first, expanded only after a 7-day clean window.

- **B1 — SHELF-ADM-WTLFU — W-TinyLFU + Doorkeeper admission.** Effort: 1 wk. Gate: ≥ 5 pp hit-rate lift vs size-threshold baseline on SHELF-26 replay. If conditional: trigger SHELF-TIER5 LightGBM.
- **B2 — SHELF-EVICT-BAKE — Eviction bake-off (S3-FIFO / SIEVE / LeCaR / CACHEUS / ARC).** Effort: 2 wk in offline replay; 1 wk live AB on the chosen winner. Gate: ≥ 3 pp hit-rate at equal or better p99.
- **B3 — SHELF-IDX-SURF — SuRF over Iceberg lower/upper bounds.** Effort: 2.5 wk. Gate: ≥ 5× speedup on a known range-predicate workload (`silver_correct_cohort_ay_26` joins on date ranges, ideal target).
- **B4 — SHELF-IDX-LBLOOM — Side blooms (G2 design) + learned bloom feasibility.** Effort: 2 wk for hand-built blooms; learned-bloom is a 3-wk follow-up only if memory pressure justifies. Gate: ≥ 10 pp byte-skip on equality predicates.
- **B5 — SHELF-COAL-LINUCB — Per-table LinUCB coalescing window.** Effort: 2.5 wk. Requires G-4 (per-table label). Gate: ≥ 3 pp hit-rate on per-table replay vs static window.
- **B6 — SHELF-PLACE-CHWBL — CHWBL placement (only if A3 RCA confirms HRW is the bottleneck).** Effort: 1 wk. Gate: load standard deviation across pods ≤ 0.2 of mean over 24 h.
- **B7 — SHELF-MV-EXTEND — Extend pin-list with MV advisor recommendations.** Effort: 2 wk. Gate: bytes-saved/query (G-5) ≥ 30 % above baseline on the top-10 fingerprints.

Phase B total notional effort: ~12 wk. Realistic 1 quarter for 1 engineer.

### Phase C — research spikes (v1.x, after Phase B settles)

Theme: **2-week spikes with explicit go/no-go criteria, not commitments.** Each ends in a recommendation memo, not necessarily a ship.

- **C1 — SHELF-I3-LRB — LRB before PPO.** 2-wk spike: feature pipeline + offline replay against rep2 trace. Gate: ≥ 8 pp hit-rate lift vs A4-tuned S3-FIFO. If yes, plan production landing for v1.1; if no, archive the design and revisit RL-prefetch (SHELF-I3) in v1.2.
- **C2 — SHELF-I2-FLIGHT — Arrow Flight crossover.** 2-wk spike: implement Flight server in shelfd, plot the actual size crossover on the SHELF-26 replay. Gate: < 1 MB objects must show ≥ 20 % latency lift over HTTP for Flight to be worth shipping; otherwise keep HTTP-only.
- **C3 — SHELF-G3-SORT — Sort-order awareness.** Effort: 2 wk. Gate: ≥ 10 % byte-skip on `vw_crm_spam_chat_view` (heavily sort-keyed table from §1 top-5).
- **C4 — SHELF-COMP-ZSTD — zstd metadata pool + path-prefix dictionary.** Effort: 1.5 wk. Gate: ≥ 30 % memory reduction on metadata pool at ≤ 5 % CPU overhead. Aligns with shipped E2.
- **C5 — SHELF-E6-PEER — Peer-failover warmup.** Effort: 3 wk. Gate: G-11 time-to-50 %-warm < 5 min after pod restart.
- **C6 — SHELF-MT-THEMIS — Multi-tenant Themis-style partitioning.** Effort: 4 wk. Gate: under simulated `commonuser` flood, `mbuser_admin` p95 ≤ 1.5× clean-state baseline.

Phase C total notional effort: ~14 wk if all six ship. Realistic: pick 1–2 spikes that survive Phase A/B and run them in v1.x cycles.

---

## 5. Decision matrix and 4-item shipping cap per phase

### 5.1 All candidates ranked (effort_weeks vs expected lift)

Lift is expressed as **expected hit-rate uplift in pp** (percentage points) on rep2's actual workload, conservatively bounded. p99 lift is qualitative (↓ small, ↓↓ medium, ↓↓↓ large) because we don't yet have G-1 latency telemetry to quantify it.


| Phase | Item                          | Effort (wk) | Hit-rate lift (pp)                                 | p99 lift             | Risk   | Notes                             |
| ----- | ----------------------------- | ----------- | -------------------------------------------------- | -------------------- | ------ | --------------------------------- |
| A     | A1 latency-by-outcome         | 0.2         | 0                                                  | n/a (telemetry only) | low    | unblocks every other item         |
| A     | A2 NVMe RCA                   | 0.2         | **+15 to +30**                                     | ↓↓↓                  | low    | makes the cache actually a cache  |
| A     | A3 pod-skew RCA               | 0.6         | **+5 to +15**                                      | ↓↓                   | low    | 3× usable cache if shelf-0/1 join |
| A     | A4 S3-FIFO sweep              | 0.4         | +1 to +3                                           | ↓ small              | low    | quick tune                        |
| A     | A5 evict+admit counters       | 0.2         | 0                                                  | n/a                  | low    | unblocks B1, B2                   |
| A     | A6 engine-reset counter       | 0.1         | 0                                                  | n/a                  | low    | unblocks RCA                      |
| A     | A7 pin-list top-5             | 0.1         | **+10 to +20**                                     | ↓↓                   | low    | exploits §1 Pareto                |
| B     | B1 W-TinyLFU + Doorkeeper     | 1.0         | +5 to +15                                          | ↓↓                   | medium | proven in Caffeine, Cachelib      |
| B     | B2 Eviction bake-off (winner) | 3.0         | +3 to +7                                           | ↓                    | medium | depends on A5                     |
| B     | B3 SuRF range filters         | 2.5         | +5 to +25 (byte-skip)                              | ↓↓↓                  | medium | huge on range-predicate jobs      |
| B     | B4 Side blooms                | 2.0         | +5 to +25 (byte-skip)                              | ↓↓                   | medium | complementary to B3               |
| B     | B5 LinUCB coalescing          | 2.5         | +3 to +7                                           | ↓                    | medium | needs G-4                         |
| B     | B6 CHWBL placement            | 1.0         | conditional on A3                                  | ↓                    | medium | only if HRW root cause            |
| B     | B7 MV-advisor pin extension   | 2.0         | +10 to +20                                         | ↓↓                   | low    | builds on A7                      |
| C     | C1 LRB prefetch               | 2.0 spike   | +5 to +12                                          | ↓↓                   | high   | risk: feature engineering         |
| C     | C2 Arrow Flight crossover     | 2.0 spike   | +0 to +5                                           | ↓ small              | medium | might not justify itself          |
| C     | C3 Sort-order awareness       | 2.0         | +5 to +15 (per-table)                              | ↓↓                   | medium | scoped to sort-keyed tables       |
| C     | C4 zstd metadata + dict       | 1.5         | 0 directly; +5 to +10 effective via larger filters | ↓                    | low    | shipping E2 + dictionary          |
| C     | C5 Peer-failover warmup       | 3.0         | n/a (warmup); ↓↓ during rotations                  | ↓↓                   | high   | tied to G-11                      |
| C     | C6 Themis multi-tenancy       | 4.0         | n/a (fairness, not throughput)                     | ↓ for victim tenant  | high   | only when SLOs added              |


### 5.2 Top-4 picks per phase — the shipping cap

**Phase A (ship in 2 weeks):**

1. **A2 — NVMe RCA + DRAM/NVMe split + alert.** Highest single-item leverage. Without disk fill, every algorithm change is wasted.
2. **A3 — Pod-skew RCA + telemetry.** Second highest. Turns a 1-pod cache into a 3-pod cache.
3. **A1 — Latency-by-outcome wiring.** Unblocks all of Phase B. Cheap.
4. **A7 — Pin-list top-5.** Cheap insurance regardless of algorithm work; immediate user-visible impact for the heavy workloads.

> Deliberately holds A4, A5, A6 to a follow-up cycle. A4 (FIFO sweep) is wasted while A2 is broken; A5 / A6 can be done in 0.5 d each in week 2 if there's slack.

**Phase B (ship in Q):**

1. **B1 — W-TinyLFU + Doorkeeper.** Best risk/reward for hit-rate. Library exists, paper is mature.
2. **B3 — SuRF range filters.** Highest byte-skip ceiling on the workloads we have. Iceberg already carries the bounds.
3. **B7 — MV-advisor pin extension.** Builds directly on A7; the bytes-saved metric (G-5) will already be in place.
4. **B6 — CHWBL placement** (conditional). Only if A3 RCA confirms HRW is the bottleneck. If A3 finds the issue elsewhere (Trino client pinning, kube-proxy), drop B6 and promote **B4 (side blooms)** as the 4th item.

> Holds B2 and B5 to next quarter. The eviction bake-off is exciting but the snapshot suggests admission and skip-filtering have higher leverage today; LinUCB is sophisticated for marginal gain on top of B1.

**Phase C (research, v1.x):**

1. **C1 — LRB prefetch.** The right "next" prefetch step. Defers PPO/RL.
2. **C3 — Sort-order awareness.** Targeted at the largest table in the §1 top-5.
3. **C5 — Peer-failover warmup.** Ties to the engine-reset finding (G-10) and Karpenter spot churn.
4. **C4 — zstd metadata + dictionary.** Cheap, extends the runway of B3/B4 by halving filter memory cost.

> Holds C2 (Arrow Flight) and C6 (Themis) until there's a multi-tenant SLO requirement or a measured workload that justifies Flight's complexity.

### 5.3 What this matrix says, in one paragraph

The single biggest lever is **fixing the cache, not changing the algorithm**. A2 (NVMe enabled), A3 (3 pods used), and A7 (pin-list) are expected to deliver +30 to +65 pp combined hit-rate improvement against an algorithmically-untouched Foyer. Only after that do paper-cited admission (W-TinyLFU) and skip-filtering (SuRF) bring meaningful additional gain. Phase C is a research deck, not a commitment — keep one or two spikes alive at any time, with hard go/no-go gates before any of them earn ship effort.

---

## Acknowledgements

This document was written during the rep2 active-cutover window on 2026-04-27 with `[shelf-overview](https://platform-grafana.example.com/d/shelf-overview)` open and `cdp.trino_logs.trino_queries` queried in real time. Snapshot reproducible per the steps in `[snapshot.md §9](./perf-research-2026-04-27/snapshot.md#9-reproduction)`.

## Appendix — citations

See `[refs.bib](./perf-research-2026-04-27/refs.bib)`.