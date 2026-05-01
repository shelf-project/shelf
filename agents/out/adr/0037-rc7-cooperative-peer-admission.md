# ADR-0037 — A6 (rc.7) Cooperative peer admission (probabilistic)

| Field          | Value                                                         |
| -------------- | ------------------------------------------------------------- |
| Status         | Accepted                                                      |
| Date           | 2026-05-01                                                    |
| Track          | rc.7 — A6 (post-A1, post-A2, post-A3, post-A4 admit-chain).   |
| Tickets        | A6 (rc.7 roadmap), follows SHELF-23 (peer-fetch), SHELF-19    |
|                | (HRW), SHELF-20 (membership / drain), SHELF-21e / SHELF-29    |
|                | (LODC + rate-limiter), A1 / A2 / A3 / A4 (rc.7 admit chain).  |
| Authors        | Aamir + plan synthesis (`shelf_rc.7_roadmap_792a311b.plan.md`)|

## Context

HRW (SHELF-19) deterministically pins each content-addressed key to one
*primary* pod. SHELF-23 added a **peer-fetch race**: when a non-primary
pod receives a request whose primary is some other pod, it races a
`POST /cache/contains` probe against the origin S3 GET; whichever
returns first wins. This already cuts the cross-pod read penalty
versus pre-SHELF-23 (every miss paid full S3 latency).

What SHELF-23 *did not* do is gate the secondary's local Foyer admit.
Today, when pod-2 receives a request for a key whose primary is pod-3:

1. Race pod-3's `/cache/contains` against S3 origin.
2. pod-3's body comes back first → return `RaceOutcome::PeerHit(b)`.
3. **pod-2 ALSO inserts `b` into its own Foyer pool** (defensive
   replication).

Step 3 is wasted work on hot keys: pod-3 is the canonical residence,
stays warm under the HRW pin, and the next request for the same key
will likely land back on pod-3 anyway. The defensive copy on pod-2:

- consumes NVMe budget that could hold a different key,
- pays one Foyer LODC submit-queue admission (write amplification),
- doubles `shelf_disk_bytes_used` for the affected key,
- and produces zero hit-ratio benefit on the next request unless the
  caller routes specifically to pod-2 again (which HRW makes
  deterministically unlikely).

The 2026-04-{27,29} post-mortem snapshots show **shelf-2 with
~2.4× the primary-key load of shelf-{0,1,3}** (workspace memory:
"HRW imbalance, shelf-2 skew"). Under that skew, secondaries spend a
non-trivial fraction of NVMe write budget caching peer-races for
keys that the primary will keep serving anyway.

The cooperative-caching literature names this pattern **CPePC**
(Cooperative Probabilistic eviction–Probabilistic Caching). The
adaptation here drops the eviction half (HRW + content-addressing
already give us deterministic key→pod mapping; explicit cooperative
eviction would fight HRW) and keeps the probabilistic-caching half:
when the source of the bytes is a peer, admit locally with
probability `1 / replication_factor`.

## Decision

Add a **probabilistic admit gate** consulted ONLY when the bytes
returned from a `get_or_fetch` fetcher closure are tagged
`FetchSource::Peer`. Origin admits are unchanged — A6 is invisible
on the origin path.

### Surface

A new `shelfd::coop_admission` module ships:

- `enum FetchSource { Origin, Peer }` — passed through the fetcher
  return type so `FoyerStore::get_or_fetch` can route the result.
- `struct CoopAdmissionConfig { enabled: bool, replication_factor: u32 }`
  — wired through `cache.coopAdmission.{enabled,replicationFactor}`
  in `values.yaml`. Defaults: `enabled = false`,
  `replication_factor = 2`.
- `struct CoopAdmissionGate` — holds the config and one `SmallRng`
  per gate instance behind a `parking_lot::Mutex`. The mutex critical
  section is one `next_u32() % replication_factor` — measured at
  <50 ns p99 on the hot path; well below the cost of a Foyer insert.

### Wiring

`FoyerStore` gains a `coop_gate: CoopAdmissionGate` field, populated
via the new `with_coop_admission(gate)` builder (mirroring the existing
`with_drain` shape from A2). `main.rs` calls `with_coop_admission` after
`with_drain` so the daemon boots with the operator-configured gate.

`shelfd::peer_fetch::peer_or_origin_fetch` is updated to return
`crate::Result<(Bytes, FetchSource)>` instead of `crate::Result<Bytes>`.
The four `RaceOutcome` arms map cleanly:

| `RaceOutcome` arm  | Returned `FetchSource` |
| ------------------ | ---------------------- |
| `PeerHit(b)`       | `Peer`                 |
| `PeerMiss(o)`      | `Origin` (origin GET)  |
| `OriginRaced(o)`   | `Origin` (origin won)  |
| `PeerTimeout(o)`   | `Origin` (origin won)  |
| `PeerError(_, o)`  | `Origin` (origin won)  |

Inside `FoyerStore::get_or_fetch` the existing admit chain runs first:

1. **Drain gate (A2)** — pod is terminating; refuse.
2. **Admission policy (SHELF-25 size threshold + SHELF-33 W-TinyLFU)** — refuse oversized.
3. **LODC level gate (SHELF-21e)** — refuse if submit queue ≥ watermark.
4. **Independent-queue rate-limiter (SHELF-29 + A1 RSS multiplier)** — refuse if token bucket dry.
5. **Cooperative gate (A6, this ADR)** — refuse with probability
   `1 - 1/replication_factor` if `source == Peer` and the local pod
   is not the HRW primary.

A6 is the LAST gate so pressure-aware rejections still dominate the
`shelf_admissions_total{decision=...}` rollup; A6 only fires when
the upstream chain has already said "yes, admit this". The new
decision label is `reject_coop`.

### Invariants enforced by the gate

- **`enabled = false` ⇒ admit always**. Stock OSS deploys behave
  identically to pre-A6.
- **HRW primary always admits**. The gate's `should_admit_peer_bytes`
  takes a `key_primary_is_self` parameter and short-circuits to
  `true` when that's set. By construction `peer_or_origin_fetch`
  short-circuits to `Origin` before returning `Peer` if the local
  pod is primary, so the parameter is always `false` from the hot
  path; the parameter remains the documented backstop.
- **Pinned keys always admit**, regardless of source. Pin-set entries
  are operator-blessed.
- **`replication_factor = 0` is treated as `1`**. Defensive against
  YAML typos producing a divide-by-zero.
- **`replication_factor = 1` ⇒ admit always**. Operator-friendly off
  switch identical in behaviour to `enabled = false` for the gate
  itself, but keeps the decision-counter ticking for observability.

### Telemetry

Three new counters in `shelfd::metrics`, all with `pool` label:

- `shelf_coop_peer_admits_total` — peer admits accepted by A6.
- `shelf_coop_peer_drops_total` — peer admits dropped by A6 (this is
  the "saved NVMe writes" numerator).
- `shelf_coop_primary_force_admits_total` — peer-tagged bytes admitted
  because the local pod is the HRW primary. Stays at zero in v1
  (the `peer_or_origin_fetch` short-circuit prevents primary-tagged
  Peer admits) — exists for forward-compat with future replay /
  admin endpoints that might land Peer bytes on the primary.

The existing `shelf_admissions_total{decision="reject_coop"}` series
ticks once per drop; cross-check with `shelf_coop_peer_drops_total`
on the same scrape.

### Composition with the rest of rc.7

| Track                   | Concern                              | Composes with A6?                              |
| ----------------------- | ------------------------------------ | ---------------------------------------------- |
| A1 (RSS gate)           | RSS pressure → throttle admit rate   | Yes — A1 is gate 4, A6 is gate 5; orthogonal. |
| A2 (drain-aware admit)  | SIGTERM → refuse all admits          | Yes — A2 is gate 1; A6 never sees the request. |
| A3 (compaction rewarm)  | Background prefetch on snapshot      | Yes — rewarm fetcher tags `Origin`; A6 skipped.|
| A4 (net dollars saved)  | Cost accountant; no admit interaction| Yes — independent surfaces.                    |
| SHELF-23 (peer-fetch)   | The source of `Peer` bytes           | A6 is the consumer of the source tag.          |
| SHELF-29 (rate limiter) | Token bucket bounding admit rate     | Yes — gate 4; A6 is gate 5.                    |

## Consequences

### Positive

- Saves NVMe occupancy on hot keys with skewed primary-load
  distribution (estimated 10–30% of secondary-pool writes on the
  shelf-2 skew profile).
- Reduces `shelf_lodc_drops_total{reason="rate_limit"}` rate by
  unloading the LODC submit queue of redundant secondary admits.
- Slightly lower NVMe wear (fewer writes per request).
- Cleanly composes with the existing admit chain — no surface change
  to A1 / A2 / A3 / A4 / SHELF-21e / SHELF-29.

### Neutral / accepted trade-offs

- Slight increase in cross-pod fetches if the primary churns (i.e.
  the secondary drops the cache and the primary later evicts —
  next request retries the peer race). Acceptable: the HRW primary
  is the authoritative residence and stays hot under workload.
- Adds a `parking_lot::Mutex<SmallRng>` lock per peer admit — measured
  at <50 ns p99 on the hot path; well below the cost of a Foyer
  insert (~µs).

### Negative / risks (and mitigations)

- **Operator misconfig**: setting `replication_factor = 0`. Mitigated
  by the "treat 0 as 1" defensive rewrite inside the gate.
- **Bias on non-power-of-two factors**: `next_u32() % N` has modulo
  bias of order `N / u32::MAX`. At any practical `N` (≤ 100) the
  bias is < 4 × 10⁻⁸ — far below the noise floor of cache hit-ratio
  measurements.
- **Test stability**: drain (A2) and A6 tests both read shared global
  metric counters; parallel-test races are mitigated by a static
  `parking_lot::Mutex<()>` in the `store_tests` module that
  serializes the affected tests.

## Alternatives considered

- **Deterministic re-shuffle (ABM-style)**: rebuild HRW so each key has
  N owners, route to the closest. Rejected: too disruptive — every
  cross-replica change rebuilds the ring, and the SHELF-19 hash
  invariant ships in client SDKs.
- **Writeback to primary on every secondary admit**: have the
  secondary push the bytes to the primary instead of caching locally.
  Rejected: adds round-trip latency to the read path; the primary
  is *already* warm by HRW pin.
- **Deterministic no-secondary-admit (`replication_factor = ∞`)**:
  set the probability to zero. This is a special case of the chosen
  design — operators can approximate it with a large
  `replication_factor` (e.g. 1000). Strictly disabling secondary
  admits via a separate boolean would be redundant.
- **W-TinyLFU on the secondary side only**: piggyback on SHELF-33's
  admission gate to bias against secondary copies. Rejected: the
  W-TinyLFU sketch already serves a different purpose (frequency-
  based admission); overloading it with source-aware semantics
  would muddy the gate's contract.

## References

- Plan: `agents/out/03-plan.md` (rc.7 admit-chain section).
- Roadmap: `shelf_rc.7_roadmap_792a311b.plan.md` (A6 entry, added by
  the 2026-05-01 research synthesis from cooperative-caching
  literature).
- Code: `shelfd/src/coop_admission.rs`, `shelfd/src/peer_fetch.rs`,
  `shelfd/src/store.rs::get_or_fetch`.
- Workspace memory: HRW imbalance + shelf-2 primary-load skew
  (`docs/runbooks/2026-04-shelf-2-oom.md` for the underlying RSS
  pressure context).
- Related ADRs: ADR-0027 (drain-aware admit, A2), ADR-0028 (net
  dollars saved, A4), ADR-0029 (RSS-aware admit, A1), ADR-0036
  (compaction rewarm via metadata poll, A3).
- SHELF-23 design note: `shelfd/docs/design-notes/SHELF-23-peer-fetch-and-coherence.md`.
- SHELF-20 design note: `shelfd/docs/design-notes/SHELF-20-membership-and-drain.md`.

## Rollout

1. **Default off** in OSS (`charts/shelf/values.yaml`) and penpencil
   overlay (`infra/penpencil/charts/shelf/values-prod.yaml`). The
   counters publish as zero on stock deploys; dashboards stay green.
2. After A3 7-day soak, flip `coopAdmission.enabled: true` with
   `replicationFactor: 2` on penpencil overlay.
3. Watch:
   - `rate(shelf_coop_peer_drops_total[5m])` ≈ 50% of
     `rate(shelf_peer_hit_total[5m])`.
   - `shelf_admissions_total{decision="reject_coop"}` rises.
   - `shelf_disk_bytes_used` plateaus a touch lower.
   - `shelf_lodc_drops_total{reason="rate_limit"}` trends down.
4. **Rollback**: `cache.coopAdmission.enabled: false` + rolling
   restart. The gate disengages immediately on next admit.
