# SHELF-23 — Peer-fetch + ETag-conditional GET

Status: in-progress (Agent C, branch `shelf-23-peer-fetch`)
Plan: see `agents/out/03-plan.md` (Stage 1b — zero-downtime + capacity
rollout plan). Audit answer 2 ("in-scope") confirms that write-side
cross-pod coherence (ETag-conditional GET) is in scope for this PR.

## Problem statement

Today, with per-rep ordinal pinning (`trino-replica-N` → `shelf-N`), every
key for a given Trino coordinator hits exactly one shelfd pod. HRW
ownership is structurally aligned with the pin: the request lands on the
HRW primary by construction, and the local-vs-peer question does not
arise.

After SHELF-22 introduces the cluster-svc routing (one Service spreading
traffic across all `shelf-{0,1,2,…}` pods), that alignment breaks:

- A read for key `K` whose HRW primary is `shelf-1` may land on `shelf-2`
  on its first request. `shelf-2` will issue a fresh S3 GET, double the
  origin egress bill, and store `K` locally — meaning two pods now hold
  the same content-addressed slot. Cluster-wide cache footprint balloons,
  and the per-key hit ratio falls until both copies warm.
- The same indirection applies to **writes**. A `PUT K'` that lands on
  `shelf-0` invalidates `shelf-0`'s HEAD-LRU only. The next `GET K'` may
  land on `shelf-2`, which still holds its own positive HEAD-LRU entry
  and the underlying ETag-keyed Foyer slot from before the write — so
  Trino reads the stale bytes. Iceberg's path-immutable metadata.json
  hides the worst-case here (each write produces a new path), but mutable
  formats (e.g. Iceberg manifests touched by `replace_table`) and
  S3-versioned blobs are vulnerable.

The hot-path peer-fetch primitives (`probe_peer_contains`,
`peer_is_better`) and the HRW ring (`router::Router::owner`) already
exist, but are **not** invoked from `s3_shim::handle_get_object` or
`store::get_or_fetch`. SHELF-23 wires them in.

## Design decisions

### D1 — Peer-fetch on local miss, owner-driven

On a local cache miss inside `s3_shim::handle_get_object` we:

1. Compute `router.owner(content_addressed_key)` → `Member`.
2. If `member.id == self.pod_id`, fetch from origin (current behaviour;
   no extra hop).
3. Else, race a peer fetch against the origin fetch. The peer is
   probed via SHELF-D7 `POST /cache/contains`, then the body is pulled
   directly via the peer's data plane on hit. The origin fetch keeps
   running and wins if the peer is slow or returns miss/unavailable.

The race is **bounded** at 10 ms for the probe (per `peer.rs:42-49`,
matching SHELF-08 same-AZ p99 jitter data). The peer fetch itself
inherits the outer request deadline so a pinned-but-overloaded peer
cannot stall the request indefinitely — it always falls through to the
origin path in time for the existing `get_or_fetch` deadline.

### D2 — `race_peer_or_origin` API gap

The plan describes `peer.rs::race_peer_or_origin` as already existing.
It does not — only the probe + decision primitives (`probe_peer_contains`,
`peer_is_better`) are present. SHELF-23 ships the missing function:

```rust
pub async fn race_peer_or_origin<F, O>(
    http: &reqwest::Client,
    peer_base_url: &str,
    pool: &str,
    key: &str,
    origin_fut: F,
    probe_deadline: Duration,
) -> RaceResult
where
    F: Future<Output = O> + Send,
    O: Send,
```

The function returns a `RaceResult` enum with arms for `PeerHit(Bytes)`,
`PeerMiss(O)`, `PeerTimeout(O)`, `PeerError(O)`. Each maps cleanly to a
distinct Prometheus counter (see "Counters" below). The origin future
is **not** dropped — it is allowed to complete in the background only
when the peer wins, so the caller is never billed for a torn S3
connection. (The current pre-shim path observes this via
`tokio::join!`-like semantics; SHELF-23's race uses `tokio::select!` so
the loser is cancelled cleanly.)

### D3 — Cross-pod write coherence: ETag-conditional GET

On every read with a **local positive cache hit**, send a conditional
GET to origin with `If-None-Match: <cached-ETag>`:

- `304 Not Modified` → serve the cached body (no body transfer; one
  small RTT to S3, ~5 ms p50).
- `200 OK` with a new ETag → invalidate local + serve the fresh body
  + repopulate the cache.

This makes the cache **pull-revalidating** rather than
write-broadcasting. Three reasons it's preferred over peer-broadcast
invalidation:

1. **Self-healing on partition.** A peer pod that misses an
   invalidation broadcast (NetworkPolicy hiccup, OOM kill mid-broadcast)
   keeps serving stale forever in the broadcast model. The
   conditional-GET model heals on the next read.
2. **No new wire protocol.** No `/admin/invalidate` peer endpoint, no
   message ordering guarantees, no idempotency tokens.
3. **Bounds blast radius to origin RTT.** The worst-case extra cost is
   one HEAD-equivalent RTT per read, which is what we're already paying
   on a cold HEAD-LRU miss.

The freshness-window optimisation (D4) keeps the steady-state cost near
zero.

### D4 — Freshness window

Per-key counter: number of consecutive `304`s observed. Once it reaches
`FRESHNESS_WINDOW_THRESHOLD` (default 10), skip the conditional GET for
up to `FRESHNESS_WINDOW_TTL` (default 5 s). Reset on any `200`.

Rationale: in the steady state (no concurrent writer), a hot manifest
file gets revalidated on every read, paying ~5 ms × queries-per-second.
At 100 qps this is 500 ms/s of S3 RTT — a real cost. The freshness
window cuts this to one revalidation per 5 s under sustained read
traffic. The 5 s upper bound is the bound on staleness during a
cross-pod write race, which is acceptable for Iceberg metadata that
already has commit-conflict retries downstream.

The window is local to each shelf pod's process; it does not need to be
synchronised across peers.

### D5 — Symmetric coverage in `store::get_or_fetch`

`store::get_or_fetch` is the **metadata-prefetch path** entry point —
warm-path Java plugin reads, the pin-list replay loader, and any future
direct-fetch caller flow through it. SHELF-23 adds the same primary-vs-
self decision there, behind the same `peer.rs` function. Without this,
the s3-shim path would peer-fetch but the pin-list replay would
double-warm, defeating Phase-3 pre-warm windows.

### D6 — Failure modes

| Failure | Behaviour |
|---|---|
| Peer probe times out (>10 ms) | Fall through to origin fetch (already running) |
| Peer probe says `Hit` but body fetch times out | Cancel peer, fall through to origin |
| Peer returns 5xx / non-200 on data plane | Cancel peer, fall through to origin |
| Conditional-GET origin returns 5xx | Treat as non-fresh, fall through to full fetch (cache-busting safe) |
| Conditional-GET origin returns 412 (precondition failed, rare) | Treat as `200` semantics; refetch + repopulate |

In all cases the read completes successfully as long as origin is
reachable. Shelf adds *opportunistic* peer optimisation; it never
becomes a single point of failure.

## Counters / observability

```
shelf_peer_hit_total{pool}           # peer probe + body served from peer
shelf_peer_miss_total{pool}          # peer probe ⇒ Miss; fell through to origin
shelf_peer_timeout_total{pool}       # peer probe deadline elapsed
shelf_peer_error_total{pool,kind}    # network / 5xx / decode

shelf_revalidate_total{pool,result}  # result ∈ {fresh_304, refresh_200, error, skipped_window}
shelf_freshness_window_active{pool}  # gauge: number of keys currently inside the freshness window
```

`peer_hit_total / (peer_hit_total + peer_miss_total + peer_timeout_total + peer_error_total)` is the
"peer payoff ratio" — the fraction of cross-pod misses that benefited
from peer fetch instead of double-fetching origin.

`refresh_200 / (fresh_304 + refresh_200)` is the cross-pod write rate;
expected to be tiny in steady state, spikes during dbt batch windows.

## Migration path

1. **preview-9**: peer-fetch + ETag-conditional GET behind chart values
   `peerFetch.enabled` and `revalidate.enabled`, both **default true**
   on `values.yaml`. The cluster-svc cutover (Stage 5) requires this; no
   separate Stage 0/1/2 enables it.
2. **preview-10** (only if preview-9 reveals tuning hot spots): adjust
   `FRESHNESS_WINDOW_TTL` / `FRESHNESS_WINDOW_THRESHOLD` defaults, or
   gate revalidation by pool (e.g. metadata-only).

## Open questions for Conductor A

- **Q1 — chart-side toggle.** Is `peerFetch.enabled=true` acceptable as a
  pure code-path on preview-9 with no chart values key? My plan: ship it
  as a code-path with a pod-env override (`SHELFD_PEER_FETCH_ENABLED`)
  for the initial roll, add chart values in preview-10 once we have
  steady-state metrics.
- **Q2 — ETag normalisation.** S3 multipart ETags include `-N` and quotes
  (`"abc-1"`); the existing HEAD-LRU stores them quoted. Should the
  `If-None-Match` header pass the value verbatim (with quotes) or
  unquoted? AWS docs say the request should mirror the server-emitted
  form, so I will pass it verbatim and add a unit test that pins the
  shape. Confirm acceptable.
- **Q3 — kind cluster ownership.** The integration test (deliverable
  #4) needs a 3-pod kind cluster. Is `shelf/benchmarks/smoke/`
  acceptable as the home for the harness, or should it land in
  `shelfd/tests/it_peer_fetch.rs` only with a `docker compose`
  alternative? The spec says the latter, but kind is more representative
  of prod. My plan: ship `shelfd/tests/it_peer_fetch.rs` as a Cargo
  integration test using axum-based mock peers, defer the real kind
  harness to a follow-up if needed.
- **Q4 — RCA report.** Plan refers to
  `docs/rollout-v1/rca-stage0bc.md` (Agent B's
  output) for the MR description. Will reference as
  "see rca-stage0bc.md when published" until B publishes.
