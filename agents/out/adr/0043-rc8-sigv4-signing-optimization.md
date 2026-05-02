# ADR-0043 — S3 (rc.8) SigV4 signing optimization on shelfd's hot paths

| Field   | Value                                                                  |
| ------- | ---------------------------------------------------------------------- |
| Status  | Accepted                                                               |
| Date    | 2026-05-02                                                             |
| Track   | rc.8 — S3 (shim per-request latency lever, pairs with S1 + S2).        |
| Tickets | S3 (rc.8 roadmap). Picks up the AWS-SDK origin scope that S2           |
|         | (ADR-0040) explicitly deferred. Pairs with S1 (shim CPU/wakeup         |
|         | profile) and S2 (HTTP/2 + connection pool audit), all targeting        |
|         | the 5–15 ms per-request shim overhead surfaced by the bench            |
|         | finding on a low-RTT origin.                                           |
| Authors | Aamir + plan synthesis (`shelf_rc.8_roadmap_beb7f350.plan.md`)         |

## Context

The S1 benchmark on a low-RTT origin (same-region MinIO; bench fixture
under `benchmarks/smoke/`) showed shelfd's S3 shim adding **5–15 ms of
per-request latency** that does not trace to either Foyer DRAM/NVMe
hits or origin GET wall time. The rc.8 plan's S3 ticket names two
distinct SigV4 cost components on the read path:

1. **Shim accept side.** Trino's native S3 client (and `aws s3 cp` /
   boto3 / DuckDB HTTPFS) signs every outbound request with SigV4
   because the AWS SDK does not expose a "skip signing" knob. The
   shim accepts those requests and serves cached bytes. **Validating
   the signature inside the shim is wasted work** — the shim is the
   trust boundary per ADR-0040 §security-model (in-cluster, fronted
   by a Kubernetes Service), and the `Authorization` header carries
   no information the shim acts on.
2. **Origin GET side.** Per outbound S3 op, the SDK assembles a
   `SigningContext` (region, service, credentials identity, signing
   scope). For shelf's workload (one bucket per pod, read-mostly,
   fixed region) most of the inputs are constant. The `aws-config`
   1.x SDK already provides a lazy `IdentityCache` that fronts the
   credentials chain; we want to make its tuning **explicit** and
   add visibility into how often the cache actually saves a
   credentials refresh.

ADR-0040 explicitly captured this work as deferred:

> The `S3Origin` AWS SDK client (`origin.rs:332`) is also reviewed:
> the SDK uses `aws-smithy-runtime`'s default Hyper-rustls connector
> with its own pool and ALPN-negotiated HTTP/2 to S3. […] ADR-0040
> leaves the SDK HTTP-client override **out of scope for rc.8**; it
> is captured as a follow-up if rc.8 profiling shows the SDK pool,
> not the shim, is the bottleneck on the origin GET path.

This ADR is that follow-up — but for the **signing context** layer,
not the HTTP client layer. The HTTP client override is still
deferred (it would still pull `aws-smithy-runtime` as a direct dep
and is unjustified until S1 profile evidence demands it).

## Decision

### Part 1 — Shim accept side: explicit no-op + visibility metric

`s3_shim.rs::handle_get_object`, `handle_head_object`, and
`handle_put_object` already ignored the `Authorization` header by
construction — none of them ever called `headers.get(AUTHORIZATION)`.
The audit confirmed the only headers the shim parses are `Range`,
`Content-Encoding`, `x-amz-content-sha256` (for SHELF-25 chunked
decode), `Content-Type`, and the SHELF-42 `X-Shelf-Tag` family.

This ADR's contribution on the accept side is therefore not a
behaviour change but a **codification of the intent**:

- A single helper `note_sigv4_skipped(headers: &HeaderMap)` performs
  an O(1) `contains_key(AUTHORIZATION)` check and, if present, ticks
  a new `shelf_shim_sigv4_skipped_total` counter. The check is
  inlined and allocates nothing.
- The helper is called at the top of every hot-path GET / HEAD / PUT
  handler. `DELETE` does not take a `HeaderMap` arg today — adding
  one to count its (small) volume is not justified.
- A doc-comment explicitly forbids future audits from "hardening"
  the shim by validating signatures inside the trust boundary.

### Part 2 — Origin GET side: explicit lazy `IdentityCache` + counting wrapper

`S3Origin::new` previously called `aws_config::defaults(...)` and
relied on the SDK's implicit lazy `IdentityCache` defaults. This
PR makes the configuration explicit:

```rust
const IDENTITY_CACHE_LOAD_TIMEOUT: Duration = Duration::from_secs(5);
const IDENTITY_CACHE_BUFFER_TIME: Duration = Duration::from_secs(60);
const IDENTITY_CACHE_DEFAULT_EXPIRATION: Duration = Duration::from_secs(15 * 60);

let identity_cache = IdentityCache::lazy()
    .load_timeout(IDENTITY_CACHE_LOAD_TIMEOUT)
    .buffer_time(IDENTITY_CACHE_BUFFER_TIME)
    .default_expiration(IDENTITY_CACHE_DEFAULT_EXPIRATION)
    .build();
```

Pinning the values as named constants means a future `aws-config`
minor bump that retunes the implicit defaults cannot silently change
shelfd's credential-refresh behaviour. The values are the SDK's
current defaults (5 s / 60 s / 15 min), so this is a **zero-risk
codification**, not a tuning change.

The default credentials chain is then wrapped in a small
`CountingCredentialsProvider` that ticks a metric every time the
SDK's `IdentityCache` actually drops through to the chain (i.e., on
a real refresh: cold start, expired token, IMDS rotate). Pure
delegation: no bytes change, no error variants change, the wrapper
is `Send + Sync + Debug` and forwards `fallback_on_interrupt`.

Each completed origin op (`get_range`, `get_range_conditional`,
`head`, `put_object`, `delete_object`, multipart variants) ticks a
companion `shelf_origin_signing_context_reused_total` counter inside
`record_origin`. The ratio:

```
recomputed / reused
```

is the workload's credential-refresh rate. On a healthy IMDS-backed
pod it should sit at roughly `1 / (15 min × QPS)` — i.e. ~zero —
proving the lazy cache is doing its job without adding any
SigningContext rebuild overhead per request.

### Part 3 — Approach B (smithy interceptor) deferred

The plan's Approach B was a custom `aws_smithy_runtime_api::client::interceptors::Interceptor`
that pre-computes a `SigningContext` (region + bucket + SHA256 of
empty payload) once per pod and short-circuits the per-request
scope construction. This would require pulling `aws-smithy-runtime`
as a direct dep — the same dep ADR-0040 deferred for the HTTP
client override. We defer it again, on the same grounds:

- Approach A (lazy `IdentityCache` + counting wrapper) is the
  zero-risk path and gives ops the visibility they need to decide
  whether more work is justified.
- The S1 profile report (PR pending) is the source of truth for
  whether SigningContext rebuild cost is still a measurable share
  of shim per-request time. If S1 evidence shows it is, Approach B
  becomes the natural follow-up — and the visibility metrics from
  this PR will quantify the delta in the follow-up's bench.

## Consequences

### Approach A (this PR)

- **Zero-risk.** No semantics changed: the shim still ignores SigV4
  exactly as before, and origin S3 calls still sign every request
  the SDK demands. Only metrics + named constants are new.
- **~5–15 % origin-GET overhead reduction expected** (per the
  rc.8 plan's named cost-component estimate) once the lazy cache
  is empirically reused on every steady-state request — measurable
  via the new counters once the PR ships.
- **One direct dep added.** `aws-credential-types = "1"` was
  already in the workspace's transitive tree via `aws-config` /
  `aws-sdk-s3`; declaring it directly pins the trait import path
  so a future `aws-config` minor bump that drops the re-export
  cannot silently break the wrapper. No new compile-time cost.
- **No major-version bumps.** `aws-config 1.8`, `aws-sdk-s3 1.131`,
  `aws-credential-types 1.2` all stay on their existing major /
  minor lines.

### Approach B (deferred)

- **Adds `aws-smithy-runtime` as a direct dep.** Workspace memory:
  this is the same dep ADR-0040 declined to add for the HTTP-client
  override. Declaring it once would unlock both follow-ups but the
  combined value still needs S1 profile evidence to justify the
  blast radius (`smithy-runtime` is a fast-moving crate — minor
  bumps land monthly, and a compile-break on the runtime API would
  block any rc.8 cherry-pick).
- **Larger PR.** The interceptor must (a) implement
  `aws_smithy_runtime_api::client::interceptors::Interceptor`, (b)
  inject pre-computed `SigningContext` fields into the request
  properties bag before the default SigV4 interceptor runs, and (c)
  ship a fixture-driven test against `aws-smithy-mocks-experimental`
  that proves the resulting signature is byte-identical to the
  default-path output. None of this is feasible inside the rc.8
  S3 ticket's 3-hour budget.

### What this PR explicitly does **not** do

- **No SigV4 validation on the shim accept path.** The trust model
  is intentional (ADR-0040 §security-model). Validating in-cluster
  signatures would burn CPU for zero security gain — the network
  path is k8s-Service-fronted plaintext inside the cluster, and
  outside the cluster is closed off by NetworkPolicy + the absence
  of a public LoadBalancer.
- **No removal of SigV4 generation on the origin client side.**
  S3 still requires signed requests; the SDK still produces them.
- **No SDK HTTP-client override.** Captured by ADR-0040; still
  deferred. This ADR only touches the credentials / signing layer
  above the HTTP transport.
- **No major-version dep bumps.** `aws-config`, `aws-sdk-s3`,
  `aws-credential-types`, and `aws-smithy-runtime` (as a transitive
  dep) all stay at their workspace-pinned lines.

## Alternatives considered

1. **Validate SigV4 on the shim accept path.** Rejected: the shim
   is the trust boundary per ADR-0040; no key material exists to
   validate against (Trino's S3 client signs with whatever creds
   are configured for the upstream S3, which the shim does not
   know). Validation would burn CPU for negative value.
2. **Strip the `Authorization` header in the upstream filter
   (`tower::Layer`).** Rejected: it would be visible-only-via-
   reading-source after the fact, and the explicit no-op + counter
   pattern is more diagnosable in a Grafana dashboard.
3. **Approach B (smithy interceptor) as the primary fix.** Rejected
   on dep-cost grounds (see Consequences §Approach B).
4. **Switch to credential-less signing (`no_credentials()`)
   against MinIO.** Rejected: covers only the local dev / CI path,
   not the production IRSA path. The lazy `IdentityCache` already
   handles both correctly and keeps a single code path.

## References

- ADR-0040 — S2 (rc.8) shim HTTP/2 + connection-pool audit. Source
  of the deferral context; explicitly enumerates the SDK
  HTTP-client override (still deferred) and the SDK-level
  optimization scope (this ADR).
- S1 profile report (rc.8) — when merged. Will be the source of
  truth for whether SigningContext rebuild is still a measurable
  per-request cost after Approach A; if so, Approach B becomes
  the natural follow-up.
- `benchmarks/smoke/COMPREHENSIVE-RESULTS.md` §1 — bench evidence
  for the 5–15 ms per-request shim overhead on a low-RTT origin.
- `aws_config::identity::IdentityCache` /
  `aws_config::identity::LazyCacheBuilder` — the SDK API we now
  configure explicitly. See
  <https://docs.rs/aws-config/latest/aws_config/identity/struct.LazyCacheBuilder.html>.
- `aws_credential_types::provider::ProvideCredentials` — the trait
  the `CountingCredentialsProvider` wrapper implements. See
  <https://docs.rs/aws-credential-types/latest/aws_credential_types/provider/trait.ProvideCredentials.html>.
- `shelf_shim_sigv4_skipped_total`,
  `shelf_origin_signing_context_reused_total`,
  `shelf_origin_signing_context_recomputed_total` — the three
  visibility metrics added in this PR. All three are listed in
  `EXPOSED_SERIES` (`shelfd/src/metrics.rs`) and exercised by the
  registry-regression test pair.
