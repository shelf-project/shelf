# SHELF-24 — origin-S3 fallback passthrough

**Status**: design / deferred until after Stage 6 PASS.
**Plan reference**: `shelf zero-downtime + capacity` (a2fa5fe7),
post-Stage-6 follow-ups.
**Estimate**: ~2 days of code + integration tests.
**Tracker**: open as SHELF-24 in the issue tracker; do **not** block
Stage 5 cutovers on this work.

## Problem

Trino's native S3 client takes **a single static `s3.endpoint`** with
no built-in failover. v3 audit confirmed this against the public
[Trino 476 file-system-s3 docs](https://trino.io/docs/476/object-storage/file-system-s3.html):
no `s3.fallback-endpoint`, no `s3.endpoints` list, no health-check
hook. When the configured endpoint returns 5xx or refuses connections,
every cdp query through that catalog fails until the operator changes
the endpoint and the coord restarts.

Today's behavior under shelf-pool unhealth:

| shelf-pool state | What Trino sees | Recovery path | Observed MTTR |
|---|---|---|---|
| All pods 5xx (e.g. Foyer hot-path bug, OOM cascade) | Every cdp query fails with `S3Exception` | Operational rollback: revert MR → ArgoCD reconcile → coord restart | 3-5 min |
| Service has zero ready endpoints (e.g. all pods CrashLoopBackOff) | `connection refused` immediately | Same as above | 3-5 min |
| Partial: some pods 5xx, some healthy | k8s svc routes around unhealthy pod within ~3 s; queries on the unhealthy pod fail in-flight | k8s self-heals; no operator action | < 30 s |

The 3-5 min operational-rollback MTTR is the safety net we already
rely on (it has resolved every prod incident in this workstream). It
is **not** fast enough for a peak-window incident: at 09:00-11:00 IST
peak load, 3 min of total cdp-query failure is roughly 1k-3k queries
returned as `S3Exception` to the caller.

## Design

shelfd grows an L7 passthrough mode. **In-flight passthrough only** —
when the request is already inside shelfd, but every local + peer cache
path has failed. This is the case the operational rollback is too
slow for; it's also the case shelfd is uniquely positioned to fix
because it already has the request bytes and the upstream S3 client.

### Decision tree on `GET /<bucket>/<key>`

```
GET /<bucket>/<key>
  │
  ├── local cache hit  ──────────────────────────────────────────► return cached body
  │
  ├── local cache miss
  │     │
  │     ├── HRW owner == self
  │     │     │
  │     │     ├── origin S3 GET succeeds  ─────────────────────► fill cache, return body
  │     │     └── origin S3 GET fails
  │     │           │
  │     │           ├── retryable (5xx, timeout)  ─ retry once
  │     │           │     │
  │     │           │     └── still fails  ───────────────────► (TODAY) 502 to client
  │     │           │                                          (SHELF-24) origin-passthrough
  │     │           └── non-retryable (4xx, AccessDenied) ────► forward 4xx to client
  │     │
  │     └── HRW owner == peer
  │           │
  │           ├── peer fetch succeeds (SHELF-23)  ─────────────► fill local cache, return body
  │           └── peer fetch fails
  │                 │
  │                 ├── retry against origin (already in s3_shim)
  │                 │     │
  │                 │     ├── succeeds  ─────────────────────► fill cache, return body
  │                 │     └── fails  (TODAY)                ────► 502 to client
  │                 │           (SHELF-24)                  ────► origin-passthrough
  │                 └── (origin unreachable too)            ────► (BOTH) 502 to client
  │
  └── PUT/DELETE/Multipart etc.  ─────────────────────────────► (out of scope; existing path)
```

The added box is **(SHELF-24) origin-passthrough**: when shelfd has
exhausted local + peer + retried-origin paths and would otherwise
return 502, but a fresh origin-S3 connection is reachable, stream the
origin response unmodified to the client.

### Implementation outline (shelfd Rust side)

In `s3_shim::handle_get_object` and `store::get_or_fetch`, the existing
"return Err(S3Exception)" sites grow a guarded fallback:

```rust
// pseudocode — fits inside existing handle_get_object error branch
match origin.get_object(bucket, key, range).await {
    Ok(stream) => Ok(fill_cache_and_return(stream).await?),
    Err(GetError::Retryable(_)) => {
        match origin.get_object(bucket, key, range).await {
            Ok(stream) => Ok(fill_cache_and_return(stream).await?),
            Err(_e) => {
                // SHELF-24 fallback path
                if cfg.fallback_passthrough_enabled
                   && origin.is_reachable_quick().await   // <50ms HEAD
                {
                    metrics::record_passthrough();
                    let stream = origin.get_object_passthrough(
                        bucket, key, range
                    ).await?;
                    Ok(stream_without_cache_fill(stream))
                } else {
                    Err(S3Exception::ServiceUnavailable.into())
                }
            }
        }
    }
    Err(GetError::Permanent(e)) => Err(e.into()),
}
```

Notes:
- `origin.is_reachable_quick` is a tiny HEAD against an upstream-S3
  health probe path (e.g. the bucket root) gated to <50 ms; if it
  fails, the request 502s as today.
- `origin.get_object_passthrough` reuses the existing aws-sdk-s3
  client but does **not** route through the Foyer cache populate
  path. See open question O1 below.

### What's out of scope

- **shelf-pool itself unreachable** (k8s svc returns connection
  refused, kube-dns can't resolve, every shelfd pod is in
  CrashLoopBackOff): shelfd never sees the request. This case is only
  fixable by giving Trino a second `s3.endpoint`. That's a Trino
  patch (upstream), or a service-mesh sidecar in front of shelfd
  (option C in plan §post-Stage-6). Both are explicitly deferred —
  operational rollback (revert MR + coord restart) remains the
  safety net for "shelf-pool is gone".
- **Write-path passthrough** (PUT / DELETE / Multipart). Out of
  scope; the write path already passes through to origin and there's
  no shelfd cache state to bypass.
- **Selective table-level passthrough**. Out of scope for v1; the
  fallback either fires for every failing GET or for none.

### Estimated work

~2 engineering days, breakdown:

| Sub-task | Estimate |
|---|---|
| `Origin::get_object_passthrough` + `Origin::is_reachable_quick` traits | 0.25 d |
| `S3Origin` impl (reuse existing aws-sdk-s3 client) | 0.25 d |
| Wire into `s3_shim::handle_get_object` error branch | 0.25 d |
| Wire into `store::get_or_fetch` error branch (peer-fetch fail path) | 0.25 d |
| Config flag `fallback_passthrough_enabled` (default off, opt-in via shelfd.yaml + Helm value) | 0.1 d |
| Metrics: `shelf_origin_passthrough_total{outcome}`, `shelf_origin_passthrough_seconds_bucket` | 0.15 d |
| Unit tests: cache miss + peer fail + origin-up + origin-down matrix | 0.25 d |
| Integration test (kind + minio with chaos kill on shelfd, assert client gets 200 not 502) | 0.5 d |
| Helm chart value + `values-alluxio.yaml` toggle | 0.1 d |
| Runbook entry + dashboard panel | 0.15 d |

Total: 2.25 d. Round to 2.

## Open questions

### O1. Cache-fill on passthrough success — write-through or no-mutation?

When the SHELF-24 path streams a successful origin response back to
the client, should the bytes also fill the local Foyer cache?

**Option A — write-through (cache the bytes)**:
- pros: subsequent requests for the same key serve from cache, even
  during the origin-fallback episode
- cons: during a shelf incident we may be caching bytes from a
  partially-degraded shelfd — if the failure was due to a coherence
  bug, write-through risks persisting wrong content; the failure
  cause (peer-fetch returned a stale ETag, then origin filled in the
  rest) is exactly the SHELF-23 + SHELF-21f territory

**Option B — stream only, no cache mutation**:
- pros: the fallback path is provably orthogonal to the cache state;
  if shelfd has a coherence bug the fallback can't make it worse
- cons: every request during the incident pays full origin latency;
  no warm-up effect

**Tentative recommendation**: ship v1 as **Option B** (stream only).
The fallback's purpose is incident survival, not steady-state
performance. If we observe ≥1 long-tail incident where Option B's
"every request pays full origin" hurt, revisit as SHELF-24b.

### O2. Should the fallback path retain peer-broadcast invalidation semantics?

When SHELF-24 fires, the implication is that *both* local and HRW-peer
paths failed for this key. If the failure was a stale negative-cache
or coherence bug, should the SHELF-24 path also invalidate the
HEAD-LRU on this pod and broadcast invalidation to peers (as SHELF-23
does on PUT)? Or is that overreach because the origin-success doesn't
prove the cache was wrong?

**Tentative recommendation**: do not invalidate. The fallback handles
serving the request; SHELF-23 + SHELF-21f handle the coherence
correctness. Mixing the two violates the "fallback is orthogonal"
property in O1.

### O3. Default state of `fallback_passthrough_enabled`?

Off by default for v1 (operator opts in per-pool via Helm value).
Default-on can be a follow-up after a few weeks of opt-in production
data showing no surprises.

## Why deferred until after Stage 6 PASS

1. SHELF-24 is a safety-net feature. Until Stage 6 PASS proves the
   steady-state behavior of shelf-pool, we don't yet have a
   well-characterized baseline to measure SHELF-24's impact against.
2. The 3-5 min operational rollback MTTR has been sufficient for
   every prod incident in this workstream. SHELF-24 jumps to the
   front of the queue if and only if we observe ≥1 incident where
   operational rollback was too slow (recorded in the incident log
   and pinned in `#data-platform`).
3. Implementing SHELF-24 before Stages 0/1/1b/2 are green risks
   masking real bugs — a coherence bug that causes 502s today would
   silently be papered over by SHELF-24 and never get debugged.

## References

- [Trino 476 native-S3 docs](https://trino.io/docs/476/object-storage/file-system-s3.html)
- Plan `a2fa5fe7` — Stage 5 cutovers + post-Stage-6 follow-ups
- SHELF-22 cluster-svc design notes
- SHELF-23 peer-fetch design notes (this doc's prerequisite for
  the peer-failure branch of the decision tree)
