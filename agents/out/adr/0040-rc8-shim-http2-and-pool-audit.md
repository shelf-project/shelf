# ADR-0040 — S2 (rc.8) Shim HTTP/2 + connection-pool audit

| Field   | Value                                                                  |
| ------- | ---------------------------------------------------------------------- |
| Status  | Accepted                                                               |
| Date    | 2026-05-02                                                             |
| Track   | rc.8 — S2 (shim per-request latency lever, pairs with S1 + S3).        |
| Tickets | S2 (rc.8 roadmap). Pairs with S1 (shim CPU/wakeup profile) and S3      |
|         | (SigV4 verification cost), all targeting the 5–15 ms per-request shim  |
|         | overhead surfaced by the S1 bench finding on a low-RTT origin.         |
| Authors | Aamir + plan synthesis (`shelf_rc.8_roadmap_beb7f350.plan.md`)         |

## Context

The S1 benchmark on a low-RTT origin (same-region MinIO; bench fixture
under `benchmarks/smoke/`) showed shelfd's S3 shim adding **5–15 ms of
per-request latency** that does not trace to either Foyer DRAM/NVMe
hits or origin GET wall time. The two structural suspects on the
read path between Trino's native S3 client and the S3 backend are:

1. **Server-side connection cost** — Trino opens a fresh HTTP/1.1
   connection to the shim, pays the TCP handshake, sends one request,
   waits for the response, and either reuses or tears down the
   connection. Under the rc.7 default, every parallel split-source
   open in Trino fans out to a separate connection because HTTP/1.1
   serialises requests within a connection.
2. **Client-side connection cost on the shim → origin hop** — the
   AWS SDK reuses connections internally, but our peer-fetch path
   (SHELF-23) and our membership stats poller use `reqwest::Client`
   instances built with default settings (no explicit pool size, no
   HTTP/2 keepalive). On a churning peer set or a slow membership
   refresh, the absence of explicit keepalive can cause idle
   connection eviction by the OS / kernel before the next probe,
   forcing fresh handshakes.

The S1 PR profiles the wakeup path; S3 looks at SigV4 verification
cost; this ADR covers the audit + minimal fix on the **HTTP plumbing**
itself.

## Decision

### Server side — HTTP/2 (h2c) is **already supported**, no code change

`shelfd/src/http.rs` binds two listeners: the data plane on `:9090`
(via `serve()`) and the S3-compat shim on `:9092` (via
`serve_s3_shim()`). Both call `axum::serve(listener, app)`.

Reading the axum 0.8 source confirms that `axum::serve` wraps
`hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())`
and invokes `serve_connection_with_upgrades(io, hyper_service)`. The
`auto::Builder` peeks the first bytes of the connection and routes
to either the HTTP/1.1 codec or the HTTP/2 codec based on whether
the client sends the HTTP/2 connection preface
(`PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n`). With the workspace
`axum = { version = "0.8", features = ["http2", "macros"] }` feature
on, the HTTP/2 codec is compiled in, so:

- A Trino native S3 client that opens a fresh HTTP/1.1 connection
  keeps working unchanged (no protocol break).
- A client speaking HTTP/2 prior knowledge (h2c) — including curl
  with `--http2-prior-knowledge`, `aws-smithy-runtime` with the
  HTTP/2 ALPN negotiated against a non-TLS endpoint via prior
  knowledge, or any future Trino release that adopts HTTP/2 — gets
  multiplexed over a single connection with HPACK header
  compression.

This means shelfd serves h2c **today** and the S2 audit's
server-side conclusion is "no source change required, but document
the fact so a future audit doesn't re-conclude that we're forcing
HTTP/1.1." The fix in this PR is comments at the two call sites and
this ADR.

### Client side — standardise the `reqwest::Client` config

Three production `reqwest::Client::builder()` sites exist:

| Site                                 | Purpose                              | Pre-S2 settings                              |
| ------------------------------------ | ------------------------------------ | -------------------------------------------- |
| `main.rs:289` (`peer_http`)          | Peer-fetch (SHELF-23) + cap-ready    | `pool_max_idle_per_host=4`, `timeout=…`      |
| `http.rs:407` (`default_peer_http`)  | Test-time fallback for `ServerState` | `pool_max_idle_per_host=2`, `timeout=2s`     |
| `membership.rs:293` (`Resolver`)     | Membership stats poller              | `pool_max_idle_per_host=2`, `timeout=…`      |

(The `peer.rs:570` builder lives inside `#[cfg(test)] mod tests` and
is intentionally minimal — out of scope for the production audit.)

Each is now built with the canonical S2 knob set:

```rust
reqwest::Client::builder()
    .pool_max_idle_per_host(N)                            // size unchanged per site
    .pool_idle_timeout(Duration::from_secs(90))           // matches S3 server-side keep-alive
    .http2_keep_alive_interval(Duration::from_secs(30))   // PING every 30s on idle h2 streams
    .http2_keep_alive_timeout(Duration::from_secs(60))    // close after 60s of unanswered PING
    .http2_keep_alive_while_idle(true)                    // keep PINGing even with no active streams
    .tcp_nodelay(true)                                    // already the reqwest default; explicit
    .timeout(<existing per-site value>)
    .build()?;
```

`pool_max_idle_per_host` was bumped from 4 → 8 only on the
`main.rs` `peer_http` site. The other two sites stay at their
existing values (2). Rationale: peer count in a typical 3–6 pod
shelf-pool is small, so the per-peer idle cache only needs to cover
the warm path, not be sized "for high throughput". The HTTP/2
keepalive knobs are no-ops over HTTP/1.1 — peers may stay on h1 if
they don't advertise h2c — but they cost nothing if HTTP/2 isn't
negotiated.

The `S3Origin` AWS SDK client (`origin.rs:332`) is also reviewed:
the SDK uses `aws-smithy-runtime`'s default Hyper-rustls connector
with its own pool and ALPN-negotiated HTTP/2 to S3. The SDK's HTTP
client is overridable via `aws-smithy-runtime` directly, but
attaching a custom client would either require pulling
`aws-smithy-runtime` in as a direct dep (a major refactor) or
working through the SDK's `HttpClient` trait. ADR-0040 leaves the
SDK HTTP-client override **out of scope for rc.8**; it is captured
as a follow-up if rc.8 profiling shows the SDK pool, not the shim,
is the bottleneck on the origin GET path.

### Singleton property

A new unit test in `shelfd/src/http.rs` asserts that
`ServerState::with_peer_fetch(...)` stores the supplied
`reqwest::Client` exactly once and that subsequent reads from the
state observe the same `Client` (same internal `Arc`). This guards
against a future refactor that quietly creates a fresh `Client` per
request, which would defeat both the pool reuse and the HTTP/2
multiplexing benefits.

## Consequences

### Server side

- **Backward compatible.** HTTP/1.1 clients (Trino native S3 today)
  continue to work without negotiation. h2c clients now multiplex
  over a single connection.
- **Zero new dependencies.** `axum 0.8` + `hyper-util 0.1` (with
  `http2` feature) was already in the workspace.
- **Observable effect.** Per-connection HTTP/2 multiplexing reduces
  the number of concurrent TCP connections on heavy parallel-split
  workloads (Trino's per-worker fan-out) — though only when a
  matching client exists. Today's Trino native S3 client opens a
  fresh HTTP/1.1 conn per request, so the immediate impact is
  bounded; we light the path for a future Trino-side change.

### Client side

- **Improved connection reuse.** `pool_idle_timeout(90s)` outlives
  most natural pauses between successive peer fetches and stats
  polls, so the next request lands on a warm connection instead of
  paying a fresh TCP handshake.
- **HTTP/2 keepalive prevents NAT/idle-eviction.** Some k8s CNIs
  reap idle conntrack entries after 60–120 s; the 30 s PING
  cadence keeps shared-port streams alive and fails fast (via the
  60 s answer timeout) when a peer pod has actually died.
- **`tcp_nodelay(true)` is reqwest's default**; setting it
  explicitly is documentation, not behaviour change.

### Risk

- **HTTP/2 head-of-line (HoL) blocking on slow streams.** A single
  slow response within an HTTP/2 connection can stall other
  multiplexed streams behind it. Mitigation: the existing
  per-request `timeout(...)` on each `reqwest::Client` site already
  bounds the per-stream wait; the `http2_keep_alive_timeout(60s)`
  closes a stuck connection within a bounded window so the next
  request opens a fresh one. If we observe HoL stalls in the wild,
  the next move is `http2_max_concurrent_streams` tuning, not a
  protocol revert.
- **Pool-size bump is small (4 → 8 on `main.rs`).** Worst-case fd
  growth is `(num_peers × 8) + (membership × 2) + (default × 2)` —
  on a 6-pod shelf-pool that's ≤ 60 idle fds per process, well
  inside any reasonable ulimit.

### What this PR explicitly does **not** do

- **No major-version dep bumps.** `axum`, `hyper`, `hyper-util`,
  `reqwest`, `aws-sdk-s3`, `aws-smithy-runtime` all stay at their
  workspace-pinned major/minor lines. Major bumps go through the
  F-track (Foyer/dependency lane) PR pattern.
- **No SDK HTTP-client override.** Captured above; deferred.
- **No TLS / HTTPS server path.** shelfd in-cluster runs plaintext
  behind a Service; HTTPS termination remains an operator choice
  outside shelfd.
- **No removal of HTTP/1.1 support.** The `auto::Builder` keeps
  HTTP/1.1 as a first-class codec.

## Alternatives considered

1. **Switch to HTTP/3 / QUIC.** Rejected: no Trino-side support, the
   `quinn`/`reqwest` integration is still pre-1.0, and shelfd runs
   in-cluster where the QUIC over UDP path doesn't materially help
   over a low-RTT TCP keepalive. Re-evaluate when Trino lands a
   QUIC client and `quinn` ships a 1.0.
2. **Drop axum, use `hyper` directly.** Rejected: would strip the
   `axum::Router` / `with_state` / `Layer` ergonomics across both
   the data plane and the shim, and the marginal perf win on the
   axum → hyper bridge is ~µs scale next to the ms-scale handshake
   cost we're fixing.
3. **Force HTTP/2 only on the server (`http2_only(true)`).**
   Rejected: would break Trino's native S3 client which speaks
   HTTP/1.1 today, and the auto-negotiation path costs nothing
   when the client isn't h2c.
4. **Per-request `reqwest::Client`.** Rejected: defeats the entire
   pool. The new `http_client_singleton` test exists specifically
   to prevent this regression.

## References

- S1 PR (rc.8 shim profile lever) — the source of the 5–15 ms
  per-request latency finding.
- `benchmarks/smoke/COMPREHENSIVE-RESULTS.md` §1 — bench evidence
  for the per-request shim overhead on a low-RTT origin.
- ADR-0011 — content-addressed cache keys; orthogonal to this ADR
  but referenced because the singleton-client property matters for
  the cache-key lookup hot path.
- axum 0.8 source at `axum-0.8.9/src/serve/mod.rs:391` — the
  `auto::Builder::new(TokioExecutor::new())` + `serve_connection_with_upgrades`
  call that confirms server-side h2c support.
- `hyper_util::server::conn::auto::Builder` — the codec auto-detect
  used by `axum::serve`.
- `reqwest::ClientBuilder::http2_keep_alive_interval` and
  siblings — the client-side knobs standardised in this PR.
