# Shelf capacity planning

Sizing formulas for `shelfd` in terms of the two-pool layout
(ADR-0008), with worked examples for the rep-2 v0.5 gate (ADR-0010)
target and the v1 cluster rollout (all four `example` Trino
replicas) that the compressed-canary rollout plan targets.

This doc is an **estimate**. The E3 (h2 throughput), E5
(row-group/file reduction), E7 (ring churn), and E12 (Alluxio
baseline) experiments produce the numbers that turn this from an
estimate into a commitment. Until then, every figure below has a
citation or an explicit "placeholder — refine with X".

---

## 1. Inputs


| Symbol   | Name                                       | Phase 0 source                       |
| -------- | ------------------------------------------ | ------------------------------------ |
| `W`      | Working-set size (7-day unique byte range) | E5 harness on `trino_logs`           |
| `H_rate` | Target cumulative hit rate                 | plan §6.4.1 = 0.71 (Alluxio)         |
| `R_rg`   | Row-group / file byte-reduction ratio      | E5 — placeholder 0.35 (65% cut)      |
| `N`      | Pod count                                  | config — default 3 (v0.5) → 5 (prod) |
| `H_meta` | DRAM required for metadata pool            | constant 5 GiB (ADR-0008)            |
| `H_dram` | DRAM required per pod for rowgroup pool    | `W × R_rg × (1 - H_NVMe) / N`        |
| `H_NVMe` | NVMe per pod (rowgroup pool)               | `W × R_rg × headroom / N`            |
| `egress` | Steady-state S3 GB/month after warmup      | `(1 - H_rate) × monthly_reads`       |


## 2. Sizing formulas

### 2.1 Metadata pool (DRAM-only, FrozenHot)

**Absolute**, not percentage. Holds `metadata.json`, manifest lists,
manifests, Parquet footers, page indexes for the hot tables.

```
H_meta = 5 GiB  (fixed; ADR-0008)
```

Rule of thumb: if the hot-table count × avg-metadata-size-per-table
exceeds 3 GiB, alert-review this number. Metadata should fit
comfortably in 5 GiB for a catalog on the order of 10² hot tables.

### 2.2 Rowgroup pool (hybrid DRAM + NVMe, S3-FIFO)

Working-set estimate `W` comes from E5's `trino_logs` replay. Plug-in:

```
row_group_bytes = W × R_rg              # bytes actually read given predicates
per_pod_hot     = row_group_bytes / N   # HRW distributes evenly in expectation
```

Split per pod across tiers. NVMe is the persistent tier; DRAM is the
hot-front:

```
H_dram  = per_pod_hot × hot_fraction_dram       # hot_fraction_dram ≈ 0.10
H_NVMe  = per_pod_hot × (1 + headroom_frac)     # headroom_frac ≥ 0.30
```

Headroom of 30% is the Phase-5 deliverable ("NVMe headroom ≥ 30%" per
plan §3 Phase 5) — without it, Foyer admission oscillates between
refuse and admit as eviction lags.

### 2.3 CPU + memory

**Memory.** Pod memory request = `H_meta + H_dram + overhead`. Overhead
= 8 GiB for tokio buffers, Axum HTTP/2, aws-sdk-s3 hyper pool, OTel
exporter, and tracing buffers.

```
mem_req = 5 GiB + H_dram + 8 GiB
```

**CPU.** Placeholder: 8 cores (BLUEPRINT §9.1). To be refined with E3
(HTTP/2 per-stream throughput on EKS) — on first-principles, one core
sustains ~1 GB/s of h2 range-GETs with the SDK pooled client, so
`N_cpu = ceil(peak_GBps / 0.8)`. Until E3: 8 cores request, 16 limit.

### 2.4 S3 egress (steady state)

```
egress_steady_GBpm = monthly_reads_GBpm × (1 - H_rate)
```

Worst case (full cache wipe):

```
egress_wipe_GBpm = monthly_reads_GBpm        # one-month warmup at 0% hit rate
```

---

## 3. Worked example: rep-2 v0.5 target

From E12 (Alluxio baseline on rep-2, already captured) and a first-pass
`trino_logs` inspection:


| Input               | Value   | Source                                         |
| ------------------- | ------- | ---------------------------------------------- |
| `W` (working set)   | 1.2 TiB | E5 placeholder — rep-2 7-day unique byte range |
| `R_rg`              | 0.35    | E5 placeholder (65% file→row-group reduction)  |
| `N`                 | 3       | plan §3 Phase 1 deliverable 7                  |
| `hot_fraction_dram` | 0.10    | BLUEPRINT §9 Foyer heuristic                   |
| `headroom_frac`     | 0.30    | plan §3 Phase 5                                |
| `H_rate`            | 0.71    | plan §6.4.1                                    |
| `monthly_reads`     | 900 TiB | sre-1 pulled from Trino `QueryCompletedEvent`  |


Compute:

```
row_group_bytes = 1.2 TiB × 0.35            = 430 GiB
per_pod_hot     = 430 GiB / 3               = 143 GiB
H_dram          = 143 GiB × 0.10            = 14 GiB   (per pod)
H_NVMe          = 143 GiB × 1.30            = 186 GiB  (per pod)
```

The stock chart default today is **60 GiB** NVMe per pod (`values.yaml`,
ADR-0042 / K1 cost-down). The numeric example above is **model math**;
if `H_NVMe` from replay exceeds ~60 GiB, merge
`charts/shelf/examples/values-latency-priority.yaml` (**500 GiB** +
matched `nvmeSizeBytes`) or pick another overlay size — re-validate
node allocatable + pod memory before raising DRAM/NVMe together.

```
mem_req         = 5 + 14 + 8                = 27 GiB   (per pod)
```

Chart default is `resources.requests.memory = 48 GiB` to absorb the
aws-sdk-s3 hyper pool (256 connections × buffer memory) under peak
without OOMing. When E3 lands, trim toward 32 GiB.

```
egress_steady   = 900 TiB × (1 - 0.71)      = 261 TiB/month
egress_wipe     = 900 TiB                   = 900 TiB/month (one-off)
```

At `$0.09/GB` S3 data-transfer (OUT → cross-AZ / internet) and
`$0/GB` for same-region same-AZ, **bulk reads from S3 to Shelf are
free** (assuming VPC endpoint + in-region bucket). The ≥ $0 cost is
GET request-count (`$0.0004 / 1000 GET`); at a 256 KiB median range
`261 TiB / 256 KiB ≈ 1.1 B GET ≈ $440/month`. Document this — it's
larger than it intuits.

---

## 4. Worked example: v1 cluster target (4-replica rollout)

When the compressed-canary rollout promotes Shelf from rep-2 canary
to all four `example` Trino replicas (rep-0, rep-1, rep-2, rep-3),
the inputs to §2 scale ~4× on the read-volume axis. `values-prod.yaml`
already pins `replicaCount: 5` for this target; this section documents
the underlying math and where the assumptions are load-bearing.

> **Caveat — this is an extrapolation, not a measurement.** The rep-2
> `monthly_reads` figure (900 TiB/month) is from an sre-1 pull of
> `QueryCompletedEvent` aggregate bytes. Per-replica figures for
> rep-0 / rep-1 / rep-3 are captured as the `sre1-workload-confirm`
> rollout pre-req in `docs/rollout-v1.md` — do not ship the rollout
> until those numbers replace the 4× assumption below.


| Input               | Value       | Source                                                          |
| ------------------- | ----------- | --------------------------------------------------------------- |
| `W` (working set)   | **4.8 TiB** | 4× rep-2 E5 placeholder — confirm per-replica with sre-1        |
| `R_rg`              | 0.35        | same as §3; E5 gives one number for now                         |
| `N`                 | **5**       | `values-prod.yaml` → `replicaCount: 5` (plan §3 Phase 5)        |
| `hot_fraction_dram` | 0.10        | BLUEPRINT §9 Foyer heuristic                                    |
| `headroom_frac`     | 0.30        | plan §3 Phase 5                                                 |
| `H_rate`            | 0.71        | plan §6.4.1 (Alluxio baseline; Shelf's own steady-state TBD)    |
| `monthly_reads`     | **3.6 PiB** | 4× rep-2; **refine with sre-1 per-replica pull before cutover** |


Compute (plug into §2 formulas unchanged):

```
row_group_bytes = 4.8 TiB × 0.35            = 1.68 TiB
per_pod_hot     = 1.68 TiB / 5              = 345 GiB
H_dram          = 345 GiB × 0.10            = 35 GiB   (per pod)
H_NVMe          = 345 GiB × 1.30            = 448 GiB  (per pod)
```

Chart defaults (`values-prod.yaml`):

- `storage.size = 500 GiB` per pod → **~11 %** NVMe headroom vs. the
  448 GiB target. That is tighter than the v0.5 rep-2 case (63 %
  headroom) and sits below the 30 % `headroom_frac` the formula
  assumed. **Action**: either bump `storage.size` to 640 GiB (keeps
  30 % headroom) *or* accept tighter headroom and commit to daily
  NVMe-utilisation monitoring for the first 30 days. Prefer the
  former; the StorageClass is `local-nvme` and the larger ask is
  almost never node-bound at the Karpenter pool.

```
mem_req         = 5 + 35 + 8                = 48 GiB   (per pod)
```

This matches `resources.requests.memory = 48Gi` in the chart default
exactly — no bump needed.

```
egress_steady   = 3.6 PiB × (1 - 0.71)      ≈ 1.04 PiB/month
egress_wipe     = 3.6 PiB                   = 3.6 PiB/month (one-off)
```

GET request-count cost at 256 KiB median range:
`1.04 PiB / 256 KiB ≈ 4.5 B GET/month ≈ $1 800/month`. Roughly 4× the
rep-2 figure — proportional, but worth flagging at finance-review
level because the 1.1 B → 4.5 B jump crosses an attention threshold
for most request-volume dashboards.

### 4.1 Per-replica cold-start burst

At rollout time, each replica's first 2-4 h post-cutover serves
~100 % miss rate until shelfd's NVMe is warm. Worst case (no pre-warm):

```
cold_burst_bytes = per_replica_W × R_rg    ≈ 1.2 TiB × 0.35 = 420 GiB / replica
cold_burst_get   = 420 GiB / 256 KiB       ≈ 1.7 M GET in 2-4 h
```

This is small compared to steady-state traffic, but concentrated in a
short window. It will trip `ShelfReadPathP99Degraded` as a false
positive unless the pre-warm script (`make prewarm`) runs first;
target a 60 % hit-ratio floor at T+0 to stay below the alert.

### 4.2 Why `replicaCount=5` and not `=4` (one per Trino replica)

HRW routing distributes keys evenly in expectation, not
per-Trino-replica. Keeping the pod count decoupled from the Trino
replica count lets us:

- Scale shelfd independently when the working set grows past the `W`
  here (§5 scale triggers).
- Survive a single-pod loss without coupling to a single Trino
  replica being "in charge" of a pod.
- Match anti-affinity to AZs rather than to Trino replicas —
  `values-prod.yaml` requires hostname anti-affinity and prefers zone
  anti-affinity; with 5 pods and typical 3-AZ spreads that lands
  ≥ 2 per zone.

---

## 5. Scale triggers

### 5.1 Horizontal scale-up

Trigger if any of the following holds for > 24 h:

- `max(nvme_bytes_used / nvme_bytes_capacity) > 0.80` on majority of
  pods.
- `p95(shelf_read_latency_seconds{pool="rowgroup"}) > 50 ms` and the
  origin S3 is healthy (i.e. the latency is NOT an S3 storm).
- Working set estimate grows > 1.5× the `W` used in the latest sizing.

Action: `scale-up.md`. Expected rebalance cost: `1/(N+1)` of keys
re-fetch from S3. Usually negligible to users.

### 5.2 Vertical scale-up

Trigger only if horizontal scale-up is not possible (node-pool
constraint):

- DRAM pool saturating at `H_dram ≈ H_dram_limit` for > 7 days;
  dashboards evict under ad-hoc scan pressure.
- CPU limit hit (throttling) sustained > 50% of nodes.

Action: bump `resources.requests` + `resources.limits` via Helm
overlay. This requires a pod-restart-per-pod — use the PDB.

### 5.3 Horizontal scale-down

Trigger if:

- `max(nvme_bytes_used / nvme_bytes_capacity) < 0.50` on all pods for
  14 days AND
- Hit rate holds at ≥ 71% in that window.

Action: `scale-down.md`.

### 5.4 Latency vs net TCO — growing the NVMe tier (e.g. 500Gi overlay)

Use when the rowgroup pool is **capacity-bound** (NVMe utilisation near
the configured cap for sustained windows) *and* either (a) byte hit
ratio is depressed versus historical baseline, or (b) S3 **GET**
variable spend is high — larger NVMe often trades **higher EBS
byte-months** for **lower request-variable S3** and better read tail
latency. Reconcile with **GET-isolated** cost lines, not aggregate S3
storage charges.

| Signal | Interpretation | Typical lever |
| ------ | -------------- | ------------- |
| `shelf_disk_bytes_used` ≈ cap, hit ratio low, LODC drops climbing | Working set > NVMe budget or admit faster than disk drains | Merge `charts/shelf/examples/values-latency-priority.yaml` (500Gi) **or** tighten size-threshold / transient opt-out / SHELF-29 limiter **or** scale out pods |
| Need in-place PVC growth | StorageClass must allow expansion | `kubectl get storageclass <name> -o jsonpath='{.allowVolumeExpansion}'` → must be `true` |
| OOM / RSS near node allocatable | Memory pressure, not “disk full” | Follow `values.yaml` RSS budget comments before adding NVMe |

**StorageClass check:** `kubectl get sc -o custom-columns=NAME:.metadata.name,EXPAND:.allowVolumeExpansion` — only classes with
`EXPAND=true` support `kubectl patch pvc … spec.resources.requests.storage`
for online growth.

---

## 6. Pin list budget

Pinned bytes per pod should stay under **20% of NVMe capacity** in the
steady state (compute 20% from your configured `storage.size` /
`nvmeSizeBytes`).

If the trainer's next-cycle diff would exceed the budget, the diff is
held back and an issue opened against the data-eng owner of the table
that tipped it over.

---

## 7. Known gaps

- `R_rg` is a single number here. In reality it varies by query
  cohort (dashboard vs ad-hoc vs dbt). Phase-4 splits this and tunes
  admission per cohort.
- `H_rate` in the steady state is 0.71 from Alluxio; Shelf's own
  steady state may differ. The v0.5 gate window gives the first real
  number; update this doc immediately after gate evaluation.
- Multi-region egress costs are out of scope for v1 (single-region
  deployment). `regional-outage.md` covers the operational story.
- The §4 extrapolation assumes per-replica workloads are roughly equal
  in volume. They almost certainly are not — rep-2 is already the
  canary target because it has the cleanest workload signal.
  Per-replica `monthly_reads` from sre-1 replaces the 4× multiplier
  before rollout.
