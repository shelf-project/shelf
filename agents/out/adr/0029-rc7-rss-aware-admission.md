# ADR-0029 — RSS-aware admission multiplier (rc.7 / A1)

| Field    | Value                                                |
| -------- | ---------------------------------------------------- |
| Status   | Accepted                                             |
| Date     | 2026-05-01                                           |
| Ticket   | A1 (rc.7 roadmap)                                    |
| Vehicle  | `feat(rc7): A1 RSS-aware admission multiplier`       |
| Supersedes | none                                              |
| Related  | ADR-0009 (eviction), SHELF-21e (level back-pressure), SHELF-29 (byte-rate limiter), ADR-0024 (RSS watermark alerts) |

## Context

`shelfd` lives or dies on its admission gates. As of v1.0.0 there are
two gates in front of `cache.insert(...)` on the rowgroup hybrid pool:

1. **SHELF-21e** — *level-based* back-pressure. Drops admits when the
   in-flight byte budget exceeds 80 % of Foyer's
   `submit_queue_size_threshold`. Catches sustained drain-saturation.
2. **SHELF-29** — *rate-based* token bucket on top of SHELF-21e.
   Bounds the *rate* of admissions feeding Foyer's submit queue
   independently of the in-flight level. Catches the "spike of 64 ×
   4 MiB rowgroups all admitted in 200 ms" failure mode.

Neither gate watches **process RSS**. Both gates assume the byte
budget is a sufficient proxy for memory pressure. That assumption
broke on the night of 2026-04-30 → 2026-05-01:

> rep-0 stayed on shelf overnight per user directive ("low traffic
> at night time"). The cascade hit at the 05:46–08:01 IST pre-peak
> window: shelf-1/3/4 OOMKilled; shelf-0/2/5 pod-recreated with
> prior-container RSS 26.0–26.9 GiB at termination. Only shelf-5
> stable. **Every pod hit the same RSS-cap-at-allocatable wall.**
> *(workspace memory: "May 1 morning OOM cascade RCA")*

Root cause: the `alluxio` Karpenter NodePool listed `c6a` in its
`instance-family` array. `c6a` is compute-optimised (8 GiB RAM /
vCPU), so a `c6a.4xlarge` advertises ~27.3 GiB allocatable after
kubelet/system-reserved. The shelf pod limit was 32 GiB. When
RSS climbed past ~27 GiB — driven by inflight S3 buffers, Foyer
DRAM growth, and LODC submit-queue spill all expanding at once —
the kernel killed the pod (exit 137) long before the SHELF-29
byte-rate budget would have throttled.

The structural fixes in flight (drop `c6a` from the NodePool +
re-introduce a 40 GiB pod limit on m-family nodes) close the
**capacity** half of the bug. They do not close the **feedback**
half: shelfd has no signal that RSS is climbing, and no lever to
slow itself down before the kubelet kills it.

## Decision

Add an **RSS-aware admission multiplier** layered on top of the
SHELF-29 byte-rate limiter. The multiplier is computed from a
periodic poll of `/proc/self/status` (`VmRSS:`) and is multiplied
into every admit decision in `LodcAdmission::try_admit`.

### Curve

Let `pressure = current_rss / rss_target_bytes`.

```text
pressure < low_watermark   ⇒ multiplier = 1.0   (no throttle)
pressure >= high_watermark ⇒ multiplier = 0.0   (full pause)
otherwise                  ⇒ multiplier linearly interpolated
                              from 1.0 at low_watermark
                              to   0.0 at high_watermark
```

Defaults:

| Knob                       | Default                | Notes                                                      |
| -------------------------- | ---------------------- | ---------------------------------------------------------- |
| `enabled`                  | `true`                 | This is the operational fix from the May 1 incident.       |
| `rss_target_bytes`         | `40 GiB`               | Matches the rc.7 pod memory limit on m5a.4xlarge.          |
| `rss_poll_interval_secs`   | `5`                    | < 1 µs/poll on Linux; "fresh enough to react before OOM".  |
| `low_watermark`            | `0.7`                  | 28 GiB on a 40 GiB target.                                 |
| `high_watermark`           | `0.9`                  | 36 GiB on a 40 GiB target. 4 GiB OOM headroom.             |

### Mechanism

- `RssThrottle` holds a single `AtomicU64` for the latest RSS,
  updated by a tokio interval task that calls
  `std::fs::read_to_string("/proc/self/status")` once per
  `rss_poll_interval_secs`.
- `RssThrottle::multiplier_bps()` is a pure function: integer
  arithmetic in basis points (`0..=10_000`), no floating-point on
  the hot path.
- `LodcAdmission::try_admit` consults the multiplier:
  - `mult == FULL` (10000) ⇒ no throttle (skip this gate)
  - `mult == NONE` (0) ⇒ drop unconditionally
  - `0 < mult < FULL` ⇒ drop probabilistically with probability
    `1 - mult/FULL`
- The probabilistic path uses a splitmix64-mixed PRNG seeded at
  construction. Tearing on the rng-state atomic is harmless — we
  only need a uniform u32 per call.
- Drops are filed under the existing
  `shelf_lodc_drops_total{reason="rate_limit"}` label so existing
  dashboards keep working without a relabel.

### Fail-open

`read_proc_rss` returns `None` on non-Linux hosts, sandboxed
container `procfs` masks, or any I/O failure. In that case
`current_rss` stays at the `RSS_UNKNOWN` sentinel and
`multiplier_bps()` returns `MULT_BPS_FULL`. The throttle silently
degrades to a no-op rather than spuriously paging admits —
**workspace policy: no throttle is preferable to a throttle of
unknown provenance.**

### Observability

- `shelf_lodc_rss_throttle_multiplier{pool}` (gauge, basis points)
  — current multiplier; sampled every poll. Divide by 10_000 to
  render as a fraction.
- `shelf_lodc_rss_pressure_seconds_total{pool}` (counter,
  seconds) — cumulative seconds the multiplier was below `1.0`.
  `rate(...[1m])` ≈ fraction of wall-clock time under pressure.
- Existing `shelf_lodc_drops_total{pool, reason="rate_limit"}`
  bumps on every probabilistic / forced drop the throttle causes
  (no new label needed).

### Configuration surface

```yaml
cache:
  pools:
    rowgroup:
      diskCache:
        admission:
          enabled: true                # SHELF-29 byte-rate gate
          targetBytesPerSec: 209715200 # 200 MiB/s
          maxBurstBytes: 268435456     # 256 MiB
          rssThrottle:                 # **A1**
            enabled: true              # default-on; flip false to disable
            rssTargetBytes: 42949672960  # 40 GiB
            rssPollIntervalSecs: 5
            lowWatermark: 0.7
            highWatermark: 0.9
```

## Consequences

### Positive

- **Closes the OOM-cascade class** observed 2026-04-30 / 2026-05-01.
  When pod RSS climbs past 28 GiB on a 40 GiB target, admits
  start dropping; by 36 GiB they are paused entirely. Inflight
  buffers absorb under the 4 GiB headroom and the kernel does
  not OOMKill.
- **Composable with existing levers.** SHELF-21e and SHELF-29
  remain unchanged; the new multiplier is a third gate ANDed
  with the previous two. Disabling A1 (`enabled: false`)
  reverts behaviour to v1.0.0 exactly.
- **Cardinality-neutral.** No new label values; the only new
  metric series are two per pool (multiplier gauge + pressure
  counter). Cardinality cap = number of pools (today: 1, the
  rowgroup pool).
- **Fail-open by design.** Non-Linux dev laptops, sandboxed
  containers, and procfs failures all degrade to a no-op.

### Negative / Trade-offs

- **Cache fill rate slows under sustained pressure.** When RSS is
  near the high watermark, we are deliberately rejecting admits
  that would otherwise grow the cache. Hit ratio may dip
  temporarily during a warm-up phase that approaches the target.
  Mitigation: operators size `rss_target_bytes` to the actual pod
  limit so the throttle only engages near genuine OOM territory.
- **Throttle does not cause RSS to fall.** The multiplier reduces
  the *rate of new admits*, but already-admitted bytes still
  flow through Foyer's submit queue. If a leak is unrelated to
  admission (e.g. an HTTP body buffer that never drops), the
  throttle will be permanently engaged without ever recovering.
  This is intentional: the throttle is an *admit-rate* knob,
  not a memory reclaimer. The
  `shelf_lodc_rss_pressure_seconds_total` counter is the
  paging signal for "the throttle is up but RSS isn't falling —
  go investigate".

### Out of scope

- **No process-level RSS reclamation.** `madvise(MADV_DONTNEED)`,
  forcing Foyer eviction passes, or shrinking DRAM pool caps at
  runtime are out of scope. The throttle is admit-rate only.
- **No PSI / cgroup memory.pressure integration.** Linux PSI
  would give us `some` / `full` pressure signals at finer
  granularity than VmRSS, but adds a Linux-specific dependency
  and a new failure mode (PSI not enabled in some kernels).
  Defer to a future ADR.
- **No automatic tuning of the watermarks.** The 0.7 / 0.9 numbers
  are sized for the rc.7 m5a.4xlarge / 40 GiB-pod-limit baseline.
  Operators on different node families re-size the limit AND the
  watermark in lock-step in their values overlay.

## Rollback

| Trigger | Action |
| --- | --- |
| `shelf_lodc_rss_throttle_multiplier` < 5_000 sustained > 30 min on any pod | Investigate; may indicate genuine RSS leak unrelated to admission rate. |
| Foyer hit ratio drops > 10 pp post-deploy with no other changes | Revert via `cache.pools.rowgroup.diskCache.admission.rssThrottle.enabled=false` config + rolling restart. |
| Any pod OOMKill > 0 post-deploy | RSS target too high OR throttle insufficient; lower `rssTargetBytes` to 36 GiB. |

Toggle path (no redeploy): set the values overlay key
`cache.pools.rowgroup.diskCache.admission.rssThrottle.enabled: false`,
re-render the ConfigMap, then `kubectl rollout restart sts/shelf`.
The byte-rate limiter (SHELF-29) and the level gate (SHELF-21e)
remain in force — only the RSS feedback path is silenced.

## References

- Workspace memory: "May 1 morning OOM cascade RCA + capacity-engineering corrections" (2026-05-01).
- Workspace memory: "Apr 30 EOD rep-0 cutover Phase D rollback" (durable observation that c6a allocatable is 27.3 GiB).
- ADR-0009 — eviction policy + cache miss next time as the fallback for any disk-side failure mode.
- SHELF-21e — `shelfd/src/lodc_backpressure.rs` (level gate).
- SHELF-29 — `shelfd/src/admission_limiter.rs` (byte-rate gate).
- `shelfd/docs/runbooks/2026-04-shelf-1-oom.md`.
