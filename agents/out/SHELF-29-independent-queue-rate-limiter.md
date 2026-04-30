# SHELF-29 — Independent-queue admission rate-limiter

**Status:** designed → implementing → shipping as `1.0.0-rc.4`
**Owner:** shelfd
**Supersedes (operationally):** SHELF-21e level-based watermark for the OOM-under-burst class
**Companion to:** SHELF-21e (kept as second-line hard cap), SHELF-21f (`origin.max_inflight=128`)

## Problem we are solving

Two pods OOM-killed today (2026-04-29) on `1.0.0-rc.3`:

- `shelf-0` 11:32 UTC (17:02 IST), exit 137
- `shelf-2` 12:05 UTC (17:35 IST), exit 137

The 90-min smoke that closed at ~16:00 IST was clean (HRW rebalance achieved,
0 restarts). The OOMs landed in a 33-min window starting ~1h after the smoke
window closed, and **all 48 shelf-class Trino errors landed inside that
75-min window** — i.e. memory-pressure-correlated, not a steady-state failure.

The chronic ingress envelope on rep-1 + rep-2 sees **bursts at ~700 admit/s** of
~4 MiB rowgroups (2.8 GB/s ingress), against an EBS gp3 NVMe sustained drain
of ~250 MB/s. The existing `LodcBackpressure` (SHELF-21e) is a **level gate**:
it drops when `admitted - committed ≥ 80% × submit_queue_threshold` (= 800 MiB
with the prod 1 GiB threshold). That gate works on inflight bytes but does not
bound the **rate** of new admits feeding Foyer's submit queue. Under a burst,
admits can spike past the watermark for a few hundred ms before the gate trips
— and during those hundreds of ms, the in-flight body buffers (256 inflight
GETs × 32 MiB worst case, capped to 128 by SHELF-21f), the DRAM cache (14 GiB
configured), and the Foyer submit queue all inflate together. Worst-case RSS
crosses the kubelet 27.3 GiB ceiling on the alluxio NodePool's
`m6a/m5a/m7a/c6a 4xlarge` instances and the kernel kills the pod.

## Why the existing knob is not enough

- `LodcBackpressure::should_admit` is **level-based**: it reacts to in-flight
  bytes, not to the rate of new admits.
- Foyer 0.12 `RateLimitPicker` is **disqualified by workspace policy** (chaos
  window 2026-04-28): it shares Foyer's submit queue with the read path, so
  under sustained back-pressure `hit_disk` p99 pegs at the histogram-max
  bucket while writes are throttled. The hard rule from
  `/Users/aamir/trino/AGENTS.md`:

  > Any future admission rate-limiter must use a queue independent of reads.

- We can't tune our way out of this with `submit_queue_size_threshold_bytes`
  alone — lowering the threshold cuts Foyer's drain budget too and starves
  legitimate writes.

## Design — `LodcAdmission`, an independent-queue token bucket

A new admission gate sits **at the same admission seam** as `LodcBackpressure`
(inside `FoyerStore::get_or_fetch`, between the policy decision and
`cache.insert(...)`), but with three properties the existing gate lacks:

1. **Independent queue** — the limiter holds a token-bucket counter (not a
   real queue of bytes). Token state is `AtomicU64` (last-refill timestamp +
   available-tokens). Reads do not touch this state at all.
2. **Bounded by rate**, not just by level — refill rate is
   `target_bytes_per_sec`; bucket capacity is `max_burst_bytes`.
3. **Drop-on-empty** — `try_admit(bytes)` is a synchronous CAS. If tokens
   available ≥ `bytes`, it succeeds and the admit proceeds to `cache.insert`.
   If not, it drops immediately and increments
   `shelf_lodc_drops_total{reason="rate_limit"}`. **No await, no
   tokio::Semaphore, no channel send** — the limiter never sleeps the read
   path.

### Why a token bucket and not an `mpsc::Sender::try_send`

Both shapes drop on full and bound RSS. The token bucket wins on three counts:

- **No cross-task RSS** — an `mpsc<Bytes>` queue would hold up to
  `max_queue_depth × avg_bytes` of cached body in shelfd memory while the
  drain task fed Foyer; that's RSS we don't want to take. The token bucket
  gates `cache.insert` directly with no intermediate buffering.
- **Trivially testable** — the limiter is a pure function of (now,
  state, request bytes); no tokio runtime needed.
- **Hot path is two atomic loads + one CAS** — same cost as the existing
  `should_admit`.

### Bounds (config knobs)

| Config key | Default | What it does |
|---|---|---|
| `lodc.admission.enabled` | `true` | Master switch. Off-switch via env var `SHELFD_LODC_ADMISSION=off` for emergency rollback without a redeploy. |
| `lodc.admission.target_bytes_per_sec` | `200 MiB/s` (≈ EBS gp3 sustained, headroom for both flushers and parallel reads) | Token refill rate. |
| `lodc.admission.max_burst_bytes` | `256 MiB` | Bucket capacity. Allows a 1.3 s burst at refill rate before any drop. Sized to hold ≈ 64 × 4 MiB rowgroups — the largest legitimate burst we observed in rc.2/rc.3 traces. |
| `lodc.admission.max_inflight_admissions` | `1024` | Optional secondary safety: a coarse counter of in-flight admits. Kept primarily for forward compatibility; default is high enough to be a no-op next to the byte budget. |

The defaults target the observed envelope: `200 MiB/s × 4 MiB/entry =
50 entries/s` sustained — the user spec says *"let through ~200/s sustained,
drop the rest"*. Reconciling: 200/s × ~1 MiB avg = ~200 MiB/s. We size in
**bytes** because the per-entry size varies 100×; sizing in entries would
either choke 32 MiB rowgroups or let through unbounded volumes of 8 KiB
manifests. Bytes is the right unit.

### Metric labels

We extend the existing `shelf_lodc_drops_total` counter with a `reason`
label rather than introducing a parallel counter, because dashboards and
alerts already filter by pool — the new label is additive and non-breaking
for queries that ignore it. Reasons:

- `"submit_queue_overflow"` — the existing SHELF-21e level-gate path
  (rename of the unlabeled v1 series; the metric carrying no `reason` label
  yesterday is upgraded to carry `reason="submit_queue_overflow"` from
  rc.4 onwards).
- `"rate_limit"` — the new SHELF-29 token-bucket path.

`EXPOSED_SERIES` keeps the existing `shelf_lodc_drops_total` entry; the
chart Prometheus query already groups by `pool` so adding `reason` is
a no-op until dashboards opt in.

Two new gauges, low-cardinality:

- `shelf_lodc_admit_tokens_available` — current token bucket fill (bytes)
- `shelf_lodc_admit_burst_capacity` — configured `max_burst_bytes` (constant
  per pod, but emitting it lets the dashboard label "fill % of capacity"
  without a hard-coded denominator)

### Invariants (verified in unit tests)

1. **Read path never blocks on the limiter** — `try_admit` is `fn` (not
   `async fn`), uses only `AtomicU64::load` / `compare_exchange_weak`, and
   has a unit test that calls it ten thousand times back-to-back inside a
   single thread to assert wall-clock < 50 ms.
2. **Sustained 1000/s caps at target rate ± 10%** — a burst-then-steady
   load test runs 5 s at 1000 admits/s × 4 KiB and asserts admitted bytes
   ≤ `target_bytes_per_sec × 5.5 s` (10% headroom for first-burst
   credit).
3. **Bursts up to `max_burst_bytes` admit fully** — start with a full
   bucket, fire `max_burst_bytes / entry_size` admits in a tight loop,
   assert every one is admitted.
4. **Healthy capacity is a no-op** — when the bucket is full and stays
   full (calls spaced > 1 token-refill apart), every call returns admit;
   the counter stays at zero.

Plus edge cases: zero rate (`target_bytes_per_sec = 0` → every call
drops, used as kill-switch path), `entry_bytes > max_burst_bytes` (always
drop — the entry never fits the bucket; counted as `rate_limit`).

### Wire-in points

- `shelfd/src/admission_limiter.rs` (new module) — `LodcAdmission` struct,
  pure-atomics implementation, unit tests.
- `shelfd/src/store.rs` — `FoyerStore` grows `rowgroup_lodc_admit:
  Option<LodcAdmission>`. Open path constructs from
  `RowGroupDiskCacheConfig`. `get_or_fetch` admission seam consults it
  *after* the existing policy decision and *after* the SHELF-21e level
  gate. Order matters: cheapest gate first (size), then level (cheap
  atomics), then rate (cheap atomics + clock). All three must say admit
  for the insert to proceed.
- `shelfd/src/config.rs` — `LodcAdmissionConfig` struct on
  `RowGroupDiskCacheConfig`. All fields `#[serde(default)]` so existing
  values.yaml keeps parsing.
- `shelfd/src/metrics.rs` — `LODC_DROPS_TOTAL` gains a `reason` label
  (registered with two children pre-touched in `FoyerStore::open`).
- Env var `SHELFD_LODC_ADMISSION=off` short-circuits to `try_admit →
  always-true` for emergency rollback without a redeploy. Implemented at
  config load (`apply_env_overrides`).

## Roll-out

- `1.0.0-rc.4`: defaults on, watermark soft (target rate well above
  observed steady-state). The drop counter is the canary.
- Single-pod canary on `shelf-3` first via STS `partition`. Then 2, 1, 0.
- Auto-rollback to `rc.3` on any OOMKill, `hit_disk` p99 > 5 s sustained
  > 5 min, or rep-1 P50 +50% vs rc.3 baseline > 10 min.

## Out of scope for rc.4

- LightGBM-driven admission (SHELF-26): blocked on replay data.
- Per-table rate limits: the token bucket is global per pod; per-table
  pinning is the existing alternative.
- Adaptive refill (auto-tune from observed NVMe drain): rc.4 is static
  defaults, retune ticket = SHELF-29b.
