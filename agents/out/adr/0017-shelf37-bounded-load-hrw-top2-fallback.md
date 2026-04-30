# ADR 0017: SHELF-37 Bounded-load HRW (top-2 HRW with load-aware fallback)

- Ticket: SHELF-37 — HRW with bounded-load overflow (LRH or top-2 HRW with load-aware fallback)
- Status: **Proposed (gated)** — ships only if 7-day post-SHELF-23 soak shows imbalance re-emerging (per-pod p99 read latency divergence > 30 % across the 4 pods, OR per-pod NVMe-bytes divergence > 2× across the pool).
- Author: AI agent on behalf of orchestrator
- Date: 2026-04-30 (UTC+5:30)
- Supersedes: extends ADR-0002 (HRW hashing over vnode-ring) — does not replace HRW, layers a bounded-load fallback on top.
- Superseded-by: none

## Context

`shelfd/src/router.rs::hrw_score` implements plain HRW over the headless
service membership; pod selection is `argmax_i hrw_score(key, member_i)`.
HRW gives uniform expected load *across keys*, but does NOT give uniform
load when the key-popularity distribution itself is skewed (workspace
memory: "HRW does not have ring-skew … key-popularity skew is the real
cause of `shelf-2` concentration"). Pre-SHELF-23, a single high-traffic
key family (Metabase queries on rep-2) hashed all reads onto `shelf-2`
while peers stayed near-idle.

[SHELF-23 peer-fetch race](https://github.com/shelf-project/shelf/pull/38)
shipped to prod 2026-04-29 ~16:28 IST and the 90-min smoke watch on
rep-1 showed shelf-2/shelf-3 hit-ratio go 1 % → 49 % with all 4 pods
within 0.8 pp of each other — i.e. the imbalance has been **measurably
mitigated** by the peer race because any pod can fetch any key now.

Plan §"P2-conditional" (lines 260–271) explicitly demoted SHELF-37 from
P2-default to P2-conditional for this reason: re-evaluate only if 7-day
post-SHELF-23 soak shows the imbalance *re-emerging*. The lever is real
research-grounded headroom — [Local Rendezvous Hashing (LRH), arXiv 2512.23434](https://www.arxiv.org/pdf/2512.23434)
restricts HRW selection to a contiguous C-wide window with bounded-load
enforcement; Google's [CH-BL (Mirrokni et al., SODA 2018)](https://dl.acm.org/doi/10.1145/3184400)
is the foundational bounded-load consistent-hashing paper — but it is
only worth shipping if the symptom returns under sustained traffic.

## Decision

When the gate fires, ship **top-2 HRW with load-aware fallback** in
`shelfd/src/router.rs` + `shelfd/src/peer.rs`:

- Primary owner = `argmax_i hrw_score(key, member_i)` (unchanged).
- Secondary owner = `arg2max_i hrw_score(key, member_i)` (already
  computed during the existing HRW scoring pass — zero additional
  scoring CPU).
- If primary's CH-BL-style load gauge (`shelf_lodc_inflight_bytes{pool}`
  + `shelf_admission_limiter_concurrency_window` if SHELF-31 is live,
  else just inflight bytes) exceeds `c · avg(load across all pods)`
  for `c ∈ [1.25, 1.5]` (configurable, start at 1.25), peer fetch
  routes to secondary. Otherwise route to primary as today.

Top-2 is preferred over full LRH because the secondary candidate is
already a free side-effect of the current HRW scoring pass — the diff
to `peer.rs` is materially smaller than implementing a contiguous
C-wide window, and the soak window has already shown the peer race
fully closes the gap on its own. LRH is the next-step fallback if
top-2 + load gauge oscillates.

## Why this and not the alternatives

| Option | Pro | Con | Why this / not |
|---|---|---|---|
| **Top-2 HRW + CH-BL load gauge (chosen)** | Smallest diff to `peer.rs`; secondary is already computed by HRW scoring; CH-BL bound `c·avg` is a documented invariant; one config knob (`c`) | Two-pod load-balancing only; degenerate case if both top-2 candidates are saturated | Chosen for SHELF-37 because the SHELF-23 baseline already balances 4 pods to within 0.8 pp; the lever exists for the long-tail single-key burst. |
| Full Local Rendezvous Hashing (LRH) | Provably bounded-load over a contiguous C-wide window; arXiv 2512.23434 publishes load-bound proofs | Larger diff to `router.rs` (window state per node); needs replay tuning of `C` | Acceptable but bigger surgery than is justified given SHELF-23's measured mitigation. |
| Google CH-BL (ring-CH bounded-load) | Foundational paper (Mirrokni 2018); production-proven at YouTube | Shelf is HRW, not ring-CH; would require a topology rewrite | Rejected — wrong substrate; load-bound idea is borrowed without the data structure. |
| Maglev (Google) | High-quality lookup table; consistent under add/remove | Designed for L4 LB at thousands of backends — overkill for a 4-pod cluster | Rejected. |
| Do nothing (rely on SHELF-23 peer race) | Zero risk; current measured state is balanced | Single-pod RSS spike on key bursts will re-occur; SHELF-23 mitigates symptom but not cause | This is the status quo while the gate is open; SHELF-37 ships only on imbalance recurrence. |

## Gate to ship

Sustained (≥ 24 h within a 7-day window) breach of either signal,
measured per pod via `mimir-data` UID `ddy2eykq2tfy8a`:

- **Per-pod p99 read latency divergence > 30 %** across the 4 pods —
  `(max - min) / avg` of `histogram_quantile(0.99, rate(shelf_request_seconds_bucket[5m]))`
  per `pod` label > 0.30 sustained > 24 h.
- **Per-pod NVMe-bytes divergence > 2×** across the pool —
  `max(shelf_disk_bytes_used{pool="rowgroup"})` /
  `min(shelf_disk_bytes_used{pool="rowgroup"})` > 2.0 sustained
  > 24 h.

If neither signal fires within 7 days post-SHELF-23, freeze the lever
and document as "SHELF-23 fully mitigates" in the handoff.

## Implementation outline

Files modified (target ≤ 200 LOC + tests):

- `shelfd/src/router.rs` — extend `hrw_score`'s caller surface with
  `pub fn top2_owners(key: &[u8], members: &[Member]) -> (Member,
  Option<Member>)` returning the two highest-scoring pods. Update
  the existing golden-vector fixture
  (`shelfd/tests/fixtures/hrw_golden_vectors.txt`) with a `secondary`
  column so Java + Rust agree byte-for-byte (cross-language agreement
  per ADR-0011 precedent).
- `shelfd/src/peer.rs` — extend `race_peer_or_origin` to consult a
  `LoadGauge` trait at the primary candidate; on overload route to
  the secondary returned by `top2_owners`. Add metric
  `shelf_router_secondary_used_total{reason="primary_overload"}`.
- `shelfd/src/config.rs` — add `peer.bounded_load_factor: f64`
  (default 1.25) and `peer.bounded_load_enabled: bool` (default
  `false` — opt-in until the gate fires).
- `shelfd/src/metrics.rs` — add the secondary-used counter +
  `shelf_router_load_factor` gauge for ops visibility.
- No changes to admission, store, shim, or origin paths.

Rollback signals (verbatim from plan lines 268–271):

| Trigger | Action |
|---|---|
| Any per-pod RSS > 24 GiB sustained > 5 min | revert HRW selection to plain HRW |
| Any OOMKill on any shelf pod | revert |
| `shelf_peer_timeout_total` rate doubles at constant traffic | revert |

Revert is a single config flip `peer.bounded_load_enabled: false`; no
image rebuild required.

## Validation discipline

- **SHELF-35 replay**: required to confirm the lift before shipping —
  freeze tsv at `agents/out/SHELF-35-replay-top2hrw-vs-hrw-<date>.tsv`
  showing top-2 + bounded load reduces the worst-pod / best-pod
  inflight-byte ratio by ≥ 30 % on the same trace.
- **24–48 h canary on rep-1**: hit-ratio ≥ 80 % after 12 h warm, p99
  read ≤ 100 ms, 5xx ≤ 1 %.
- **Hourly byte-identity diff harness** on 5 canonical Iceberg queries
  vs `cdp_direct` for the first 24 h (routing changes do not affect
  bytes returned; diff = bug).
- **Integration-test gate**: `SHELF_INTEGRATION=1 cargo test -p shelfd
  --tests router peer` with new top-2 routing tests; existing HRW
  golden-vector test must stay green.
- **Cross-language fixture sync**: regenerate
  `shelfd/tests/fixtures/hrw_golden_vectors.txt` and re-run the Java
  side's `io.shelf.client.HrwTest` (ADR-0002 / ADR-0011 cross-machine
  agreement discipline).

## Citations

- [Local Rendezvous Hashing, arXiv 2512.23434](https://www.arxiv.org/pdf/2512.23434) — bounded-load HRW with contiguous C-wide window.
- [CH-BL: Mirrokni, Thorup, Zadimoghaddam, "Consistent Hashing with Bounded Loads" (SODA 2018)](https://dl.acm.org/doi/10.1145/3184400) — foundational bounded-load proof; the `c·avg` invariant SHELF-37 borrows.
- Plan: `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md` lines 260–271 (P2-conditional lever 11), §Verification corrections lines 475–476 ("HRW does not have ring-skew … key-popularity skew is the real cause of `shelf-2` concentration").
- Workspace memory: "post-SHELF-23 imbalance is materially mitigated: 4-pod 90-min smoke watch on rep-1 showed shelf-2/shelf-3 hit ratio go 1 % → 49 % with all 4 pods within 0.8 pp" — quantitative basis for the gate.
- Existing ADR-0002 (HRW hashing over vnode-ring) — top-2 layers on top, does not replace.

## Risk register

1. **SHELF-23 may already be sufficient.** The 90-min smoke watch
   shows 0.8 pp pod-divergence — well inside the gate's 30 % p99 and
   2× NVMe-bytes thresholds. There is a real chance the gate never
   fires in the 7-day window, in which case SHELF-37 stays frozen.
   Mitigation: this is the *intended* outcome; the gate is exactly
   what the workspace-memory rule against over-engineering requires.
2. **Replay (SHELF-35) must confirm the lift before shipping.** A
   replay number that shows < 30 % reduction in worst/best-pod
   inflight-byte ratio means top-2 + bounded load is no better than
   plain HRW + peer race on our trace — do NOT ship; document as
   "headroom insufficient" in the handoff.
3. **Two-pod load balancing is not three-pod.** If both top-2 pods
   are saturated simultaneously (degenerate single-key burst), the
   lever is no better than plain HRW + peer race. Mitigation: full
   LRH is documented in the alternatives table as the next step;
   the existing `peer_or_origin_fetch` already falls through to
   origin on peer error so the worst case stays bounded.
