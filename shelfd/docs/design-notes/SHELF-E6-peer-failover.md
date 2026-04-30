# SHELF-E6 — Peer /cache/contains probe race

**Status:** Phase 1 primitives shipped; Phase 2 wiring blocked on
`membership::Resolver` (SHELF-20).

## Motivation

On a local miss, `shelfd` today has exactly one fallback: go back to
S3. That's correct but expensive — S3 GET is 30–80 ms in-region,
metered, and competes with every other tenant on the NAT gateway.

Meanwhile, the adjacent pod in the same StatefulSet often already
holds the missing bytes. The HRW ring (SHELF-19) assigns a single
*owner* per key, but a non-owner frequently caches a key too (hot
tables land on every replica via the `pin_list.json` fast path, and
full-table scans fan reads across the whole ring). That pod can
serve the range in O(50 µs) over ClusterIP — roughly 1000× faster
than S3 and at zero egress cost.

SHELF-D7 exposes the wire primitive (batch residency bitmap). This
note wires the client half.

## Shipped (Phase 1)

- `shelfd/src/peer.rs` — `probe_peer_contains(...)` POSTs to
`/cache/contains` on a given peer, parses the JSON bitmap, and
maps the response into a typed `ProbeResult { outcome, bitmap, hits }`. Every failure mode (timeout, non-2xx, malformed JSON,
base64 decode error) collapses to `ProbeOutcome::Unavailable`.
- `peer_is_better(probe, single_key)` — the decision primitive. Kept
as a free function so unit tests can drive it with canned
`ProbeResult`s and Phase 2 wiring can plug it into either the
`s3_shim::handle_get_object` fast path or the
`store::get_or_fetch` slow path without dragging HTTP into the
store module.
- Five unit tests covering Hit, Miss, Unavailable, single-key
Partial (defensive miss), batch Partial (picks peer when any hit).

## Phase 2 wiring (blocked on SHELF-20)

Once `membership::Resolver::spawn` returns a real `Router` with the
peer list, the integration is:

```rust
// in s3_shim::handle_get_object, right before get_or_fetch:
let owner = state.router.owner(&key_obj);
let is_local = owner.id == state.pod_id;
let peer_future = if is_local {
    None
} else {
    Some(peer::probe_peer_contains(
        &state.http_client,
        &owner.endpoint,
        pool_label,
        &[key_obj.to_hex()],
        Duration::from_millis(10), // probe budget
    ))
};

let origin_future = state.store.get_or_fetch(pool, key_obj, admission, fetcher);

if let Some(pf) = peer_future {
    tokio::select! {
        biased;
        probe = pf => {
            if peer::peer_is_better(&probe, true) {
                // Issue a GET to the peer's /cache/:pool/:key/:range
                // and return the first byte.
                return fetch_from_peer(&state, &owner, ...).await;
            }
            // peer said miss/unavailable — fall through to origin.
            origin_future.await
        }
        bytes = origin_future => bytes,
    }
} else {
    origin_future.await
}
```

Note the `biased` branch. We *want* the probe to win ties because
it's the cheap option; `tokio::select!` is fair by default.

### Race budget

- Probe: 10 ms hard timeout (set in `probe_peer_contains`).
- Peer GET: inherits the outer request deadline, but we will set
an explicit `Duration::from_secs(2)` on the peer-fetch client so
a wedged peer cannot hold the request open past the S3 SLA.
- S3 fallback: unchanged.

### Metrics (SHELF-08)

Phase 2 will add:

- `shelf_peer_probes_total{outcome="hit|miss|unavailable"}`
- `shelf_peer_fetches_total{outcome="ok|error"}`
- `shelf_peer_fetch_seconds` histogram (same bucket layout as
`shelf_origin_request_seconds` so dashboards can overlay).

## Tests

Phase 1:

- `peer::tests::peer_is_better_picks_peer_on_hit`
- `peer::tests::peer_is_better_stays_on_origin_on_miss`
- `peer::tests::peer_is_better_stays_on_origin_when_unavailable`
- `peer::tests::single_key_partial_is_defensive_miss`
- `peer::tests::batch_partial_picks_peer_when_any_hit`

Phase 2 will add an integration test that stands up a pair of
`shelfd` Axum servers in-process, seeds pod A with a key, and asserts
that pod B's miss path prefers pod A over a deliberately-broken S3
endpoint.

## Risks and rollbacks

- **Feedback loop:** if the owner pod is unhealthy but keeps
returning `Hit`, we'll keep racing and failing. Mitigation: the
peer client will circuit-break (3 consecutive failures → 30 s
cooldown per `(peer_id, pool)`).
- **Fan-out amplification:** a batch probe for 10k keys against
every replica would be O(N*R). We cap batch size at 65_536 in the
server (SHELF-D7) and Phase 2 will only probe the HRW *owner* for
each key, not the whole ring.
- **Rollback:** the probe is advisory. Flip the feature off and
every call falls back to the original `get_or_fetch` path.

