# SHELF-20 — Membership resolver and lameduck drain

**Status:** Implemented in `shelfd/src/membership.rs`. Wiring into
`main.rs` (SIGTERM handler + `ServerState::drain_signal`) lands in a
follow-up.

## Problem

`Router` (SHELF-19) computes HRW ownership over a `Vec<Member>`, but
the codebase had no producer for that vector. `membership::Resolver::spawn`
was a `todo!()`, so:

- Each Trino replica was 1:1-pinned to its co-numbered Shelf pod by
  putting a per-pod hostname into `cdp.properties.s3.endpoint`. That
  works for the current 3-pod, 3-replica deployment, but it gives
  every pod a private cache: a key fetched on `shelf-1` is invisible
  to `shelf-2`, even though they sit in the same `StatefulSet`.
- Peer probing (SHELF-E6 / `peer.rs`) shipped its primitives but had
  nothing to call them with — there was no peer list.
- `kubectl drain` of a Shelf node was unsafe: in-flight reads
  destined for the node would route to S3 instead of the surviving
  pods, magnifying tail latency at the worst possible moment.

This note describes how the resolver fills that gap and how the
lameduck drain protocol keeps the ring view honest during
StatefulSet rolls.

## Decision

Use the `/stats` JSON endpoint that every pod already exposes as the
source of truth for membership. Resolve the headless service via
`tokio::net::lookup_host`, probe each returned IP for `/stats` in
parallel, and feed the resulting members into `Router::update`. No
gossip, no Raft, no embedded service registry — ADR-0001 stands.

```text
       ┌───────────────────────────────────────────────────────────┐
       │ Per-pod resolver                                          │
       │                                                           │
       │   tick = interval(cfg.dns_refresh)                        │
       │   loop {                                                  │
       │     ips    <- lookup_host(headless:stats_port)            │
       │     stats  <- join_all(GET /stats with per-peer timeout)  │
       │     filter !draining, dedupe pod_id, sort id              │
       │     router.update(members)                                │
       │   }                                                       │
       └───────────────────────────────────────────────────────────┘
```

A single slow peer cannot stall the round: each `/stats` probe is
independently bounded by `cfg.stats_timeout`. A peer that does not
answer within that window is simply absent from the round's ring,
without affecting the others.

There is **no failure budget**. A peer that misses one round and
succeeds the next is restored to the ring on the next round. HRW
keys re-route deterministically: at most ~1/N of keys move per
membership change, which is well within the read-through fallback's
absorption envelope (peer probe → origin race in
`peer::race_peer_or_origin`).

## Drain protocol

`DrainSignal` is a one-bit `Arc<AtomicBool>` shared between the
process's `/stats` handler and any code that wants to flip the bit.
The bit is **local** — it represents *this* pod's drain state, not
any peer's. Peers learn the state via `/stats.draining` on the next
refresh.

```text
                   ┌─────── this pod ────────┐
   SIGTERM ─►  drain_signal.begin()
                          │
                          ▼
                   /stats.draining = true
                          │
                          ▼ (≤ cfg.dns_refresh later)
   peer resolver ─►  filter out this pod_id
                          │
                          ▼
   peer router    ─►  HRW reroutes keys to surviving pods
                          │
                          ▼ (cfg.drain_grace later)
   wait_drained()  returns
                          │
                          ▼
   shutdown.cancel() — server exits
```

`wait_drained` races `cfg.drain_grace` against a caller-supplied
shutdown token, so a hard kill (`kill -9` upstream) can skip the
grace window. The default of 15 s is `3 × dns_refresh`, which gives
every peer at least one full refresh round to observe the drain
flag and pick new owners.

## Why a probe trait, not a free function

The loop's I/O surface is two operations: DNS resolve + `/stats`
HTTP. Production wires those to `tokio::net::lookup_host` and
`reqwest`. But we don't want unit tests to hit either:

- `lookup_host` resolves through the OS resolver. A test cannot
  redirect that without elevated privileges.
- `reqwest::Client` carries TLS, connection pooling, and HTTP/2
  state — far heavier than a unit test needs, and a chronic source
  of flakes when CI runners share a port range.

`HostResolver` and `StatsProbe` are 1-method traits that the
`Resolver::spawn_with` constructor accepts. The default
`spawn` builds the production stack; tests instantiate
`ScriptedResolver` and `StaticProbe` (see `mod tests`).

This is also how the future Phase-2 SRV-aware resolver lands: a
new `SrvResolver` impl, no churn in the loop.

## Capacity weighting

`weight_for_capacity(capacity_bytes, unit_bytes)` divides a peer's
reported `capacity_bytes` by `unit_bytes` (default 1 GiB) and clamps
the result to `[1, u32::MAX]`.

- Bottom clamp: a freshly-started pod with `capacity_bytes == 0`
  still gets weight `1`, so the ring is non-empty during a roll.
- Top clamp: a misconfigured "100 PiB" peer can't overflow the
  HRW score arithmetic.

Linear scaling is consistent with ADR-0002: HRW score divides the
weight by `-ln(x)`, so a peer with 5× the capacity carries
roughly 5× the keys. Non-linear curves (e.g. log-scale to
de-emphasize very large peers) are not justified by current
deployment shape (every pod has the same DRAM budget) and would
make the ring decision harder to reason about.

## What ships in this commit

- `shelfd/src/membership.rs`:
  - `ResolverConfig` with sensible defaults (`for_self`).
  - `DrainSignal` (cheap, clonable, idempotent).
  - `Resolver` with `spawn`, `spawn_with`, `begin_drain`,
    `is_draining`, `wait_drained`, `join`.
  - `HostResolver` + `StatsProbe` traits with a production
    implementation each.
  - Pure helpers `build_members`, `weight_for_capacity`.
  - 13 unit tests covering the pure helpers, drain signal,
    happy path, draining-peer filtering, all-draining preserves
    previous ring view, empty-DNS soft failure, drain grace
    elapse, drain race against shutdown.
- `shelfd/src/control.rs`:
  - `Stats.draining: bool` (`#[serde(default)]`) so the wire
    payload survives a wave of mixed-version clients.
- `shelfd/src/http.rs`:
  - `/stats` handler now emits `draining: false`.
  - `stats_payload_has_contract_keys` test asserts the new key
    is part of the wire contract Agent 5 consumes.

## Follow-ups (not in this commit)

1. **`main.rs` wiring (SHELF-20a).** Build `DrainSignal` and a
   `Router` in `main`, hand both clones to `ServerState` and to
   `Resolver::spawn`. On `SIGTERM`, call `resolver.begin_drain()`
   and `resolver.wait_drained(&shutdown).await` before
   cancelling the shutdown token.
2. **`/admin/ring` populates from `Router::view()`** instead of
   the placeholder single-row response in
   `http::handlers::admin_ring`.
3. **Plugin-side rebalance (SHELF-20b).** Trino's `s3.endpoint`
   stops being a single hostname; the plugin polls `/stats` on
   every Shelf pod and routes by HRW. Until then the deployment
   stays in 1:1 pinned mode (rep-N → shelf-N).
4. **Phase 3 hardening:**
   - SRV-record resolver for AZ awareness.
   - Chaos-drill test (`benchmarks/chaos/`) that randomly kills
     a pod mid-traffic and asserts client-side error rate
     stays under 0.1% during the rollout.
   - `shelf_membership_refreshes_total{outcome}` metric so
     ops can spot a stuck resolver.

## Risks / non-decisions

- **No quorum.** A single resolver instance is the source of
  truth for that pod's ring. There is no consensus across pods,
  by design (ADR-0001). Two pods seeing slightly different ring
  views during a refresh cycle is fine: HRW agrees on owners
  given the same input set, and the `/cache/contains` peer probe
  keeps the data plane correct even if a request lands on a
  non-owner.
- **No anti-affinity awareness.** A peer in a different AZ has
  the same weight as one in the same AZ, even though the
  cross-AZ probe is two orders of magnitude slower. The peer
  probe budget (10 ms) absorbs the difference today, but Phase 3
  will revisit if cross-AZ traffic becomes a measurable
  contributor to p99.
- **`reqwest::Client` per resolver.** One client per pod, not
  one per probe. Connection pooling means probes during the same
  round share TCP/TLS state, so a 3-pod ring costs ~6 sockets
  not 3 × N.
