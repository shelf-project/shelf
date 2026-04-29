# SHELF-49 — Coalesced range-GET in `s3_shim.rs`

## TL;DR

Trino's native S3 client emits many small, almost-adjacent ranges
within the same Parquet file when planning a row-group scan: the
8-byte footer magic + footer struct (`bytes=-N`), then dictionary
pages and a handful of column chunks within a few MiB of one
another. The pre-SHELF-49 shim issues one origin GET per range, which
pays the S3 request charge **and** round-trip latency for every
slice.

This module batches concurrent ranges that share `(bucket, key,
etag)` into a single coalesced origin GET when the gap and the
total span fit operator-tunable budgets. Every requester still
receives byte-identical bytes for its specific span via a per-requester
`tokio::sync::oneshot`. **No changes to the bytes returned to
Trino**; only the on-the-wire request count drops.

Default-off (`cache.coalesce.enabled: false`) so the module ships
dark; flip per-replica via Helm + `helm upgrade` once the SHELF-37
plan-aware listener confirms the per-pool hit ratio is non-negative
post-flip.

## Algorithm

```text
fetch(bucket, key, etag, offset, length):
  if !cfg.enabled            → solo GET, label outcome=disabled
  if breaker.is_open()       → solo GET, label outcome=circuit_open
  if etag == ""              → solo GET, label outcome=solo
  group_key = (bucket, key, etag)
  (tx, rx) = oneshot::channel()
  with groups.lock():
    if groups[group_key] exists:
        push (offset, length, tx) into group.waiters; needs_dispatcher = false
    else:
        groups[group_key] = { waiters: [...], started_at: now() }; needs_dispatcher = true
  if needs_dispatcher:
      tokio::spawn(async {
          sleep(cfg.wait_window)
          dispatch(group_key)
      })
  return rx.await

dispatch(group_key):
  with groups.lock():  state = groups.remove(group_key)
  observe shelf_coalesce_window_seconds  # started_at → now
  sort waiters by offset
  for each greedy run [start, end_exclusive) under (max_gap, max_span):
      let span = end - start
      let bytes = fetcher.fetch_range(bucket, key, start, span)
      if bytes is Ok:
          for w in subgroup.waiters:
              w.tx.send(Ok(bytes.slice(w.offset - start .. + w.length)))
          if subgroup.size > 1:
              breaker.record_success()
              shelf_coalesce_bytes_saved_total += max(0, sum(w.length) - span)
          # actually: for non-overlapping waiters, sum(w.length) ≤ span,
          # so the *gap-bridging cost* is `span - sum(w.length)`; the
          # "saved" framing is "bytes the dispatcher avoided fetching
          # by amortising the round-trip". See "Bytes-saved metric" §.
      else:
          fan err to every waiter; breaker.record_failure() if subgroup.size > 1
      bump shelf_coalesce_ranges_total{outcome=coalesced|solo} by subgroup.size
```

## Frame diagram

```text
 Time →

 Trino (native S3 client)                                  shelfd (s3_shim)
 ────────────────────────                                  ──────────────────
 t0   GET /b/file.parquet  Range: bytes=0-99           ─►  enter group(b,k,e)
                                                            spawn dispatcher
 t1   GET /b/file.parquet  Range: bytes=100-199        ─►  push waiter (100,100)
 t2   GET /b/file.parquet  Range: bytes=200-299        ─►  push waiter (200,100)
                                                            sleep(200µs)
                                                            ◄── 1 origin GET (0..300)
       ◄────  responses dispatched, byte-identical slices to each
              of the three concurrent requesters
```

## Failure semantics

- **Single coalesced GET fails** → `crate::Error::Origin("coalesced
  GET failed: …")` is fanned to every waiter in the failing
  subgroup. No requester ever silently gets partial / zero bytes.
  The breaker bumps `failures += 1` only when the failed subgroup
  had ≥ 2 waiters (a solo GET failure is indistinguishable from a
  pre-SHELF-49 stock-path failure and would unfairly trip the
  breaker).
- **Breaker opens** after `consecutive_failures` (default 5)
  back-to-back coalesced GETs error. While open, every
  `Coalescer::fetch` falls through to the legacy single-range path
  for `cool_off` (default 30 s). The first successful coalesced
  GET after the cool-off resets the failure counter.
- **Receiver dropped** before the dispatcher delivered → the
  caller's oneshot returns `Err(...)` and we surface
  `crate::Error::Origin("coalesce dispatcher dropped before
  delivering bytes for …")`. This is the path a SHELF-23 peer-fetch
  hit takes if it cancels the leader's origin future, and is
  benign — peer hits don't enter the dispatcher (see below).
- **Length mismatch** (origin returned the wrong number of bytes)
  → the dispatcher synthesises a typed error rather than sending
  short or long buffers downstream, and the breaker counts the
  failure. This path is defensive; origin S3 always honours
  closed `Range:` requests.

## Interaction with SHELF-23 peer-fetch

`peer_or_origin_fetch` short-circuits to its own `origin_fut` when
the local pod is the HRW primary, when the ring is empty, or when
peer-fetch has been disabled at runtime. The s3_shim hot path
**always** routes its `origin_fut` through `Coalescer::fetch`, but
the `peer_or_origin_fetch` race **wraps** the future — so a peer
hit short-circuits before the leader's `origin_fut` is awaited,
the dispatcher never sees the request, and no waiter is ever
registered for a peer-served slice. This is intentional: peer hits
are already a single round-trip to a same-AZ pod, which is cheaper
than a cross-region S3 GET, and adding the coalesce wait window
on top would slow them down.

## Suffix and open-ended ranges

`bytes=-N` and `bytes=0-` route directly to the legacy single-range
path. SHELF-22 always issues a `HeadObject` upstream of the GET,
so by the time the shim builds an `origin_fut` we already have a
resolved `(offset, length)`. We still bypass the dispatcher for
those request shapes because two concurrent suffix-range readers
might observe different `total_size` values across a re-uploaded
object (different ETags, same key) — the shim's `RangeSpec::Closed`
check filters that risk out at the seam where it can be done with
one `matches!`.

## Bytes-saved metric

`shelf_coalesce_bytes_saved_total` is incremented per merge group
as `max(0, span - sum(w.length))`, lower-bounded at 0. In English:
"how many bytes of bridging gap the dispatcher had to fetch on top
of what the requesters individually asked for." It is **not** a
"how many origin GETs we saved" counter — that's
`shelf_coalesce_ranges_total{outcome=coalesced} -
   coalesced_groups_count` (derivable in PromQL). The lower bound
matters because non-overlapping waiters always have
`sum(w.length) ≤ span`; the metric captures the gap-cost,
which dashboards can inspect to tune `max_gap_bytes` if it climbs.

For overlapping waiters (rare; one Parquet stripe + one re-read of
its dictionary page), `sum(w.length)` can exceed `span` because
overlapping bytes are counted twice in the sum but once in the
span. We floor at zero so the counter is monotonic and never
flaps sign on overlapping requesters.

## Configuration

All knobs live under `cache.coalesce.*` in `charts/shelf/values.yaml`:

| Field                 | Default | Notes                                                 |
|-----------------------|---------|-------------------------------------------------------|
| `enabled`             | `false` | Master switch. Ship dark.                             |
| `maxGapBytes`         | 1 MiB   | Bridging cost ceiling per run.                        |
| `maxCoalescedBytes`   | 16 MiB  | Hard cap on a single merged GET.                      |
| `waitWindowMicros`    | 200     | Followers' opportunity window.                        |
| `consecutiveFailures` | 5       | Breaker trip threshold.                               |
| `coolOffSecs`         | 30      | Breaker open duration.                                |

The Rust mirror is `crate::config::CoalesceConfig`, all fields
`#[serde(default)]` so existing values files keep parsing.

## Metrics

| Series                                    | Type      | Labels    | Notes                                          |
|-------------------------------------------|-----------|-----------|------------------------------------------------|
| `shelf_coalesce_ranges_total`             | counter   | `outcome` | One increment per **input** range. `outcome ∈ coalesced, solo, disabled, circuit_open`. |
| `shelf_coalesce_bytes_saved_total`        | counter   | -         | Per-merge-group `max(0, span - sum(individual))`. |
| `shelf_coalesce_window_seconds`           | histogram | -         | Wall-clock seconds from leader registration to dispatch. One observation per dispatched group. |

All three are wired into `EXPOSED_SERIES` in `shelfd/src/metrics.rs`
and the two regression tests (`registry_exposes_documented_series`
+ `metrics_scrape_contains_documented_series_after_touch`).

## Tests

- **Unit (`shelfd/src/coalesce.rs`):** decision matrix on
  `group_waiters` (gap fits / doesn't fit, span fits / doesn't fit,
  overlap), circuit breaker open/close/reset, and a
  `MockFetcher`-backed dispatcher that asserts (a) three adjacent
  ranges collapse to one origin call, (b) distinct ETags don't
  share a group, (c) empty ETag routes solo, (d) failure fans to
  every requester, (e) circuit-open path bypasses the dispatcher.
- **Integration (`shelfd/tests/it_coalesce.rs`,** gated on
  `SHELF_INTEGRATION=1`**):** stands up against the existing MinIO
  docker-compose fixture, seeds a 1 MiB object, fires three
  concurrent adjacent ranges through `Coalescer` wrapping the
  production `S3OriginFetcher`, and asserts the
  `shelf_origin_request_seconds{op="get_range",outcome="ok"}`
  count delta is exactly 1 (and `shelf_coalesce_ranges_total
  {outcome="coalesced"}` delta is exactly 3). A second test with
  `enabled: false` confirms the disabled path is a 1:1
  pass-through (origin delta = 2, coalesced delta = 0).
- **Config (`shelfd/src/config.rs` `cfg(test)` mod):**
  `coalesce_config_defaults_to_disabled` (absent block →
  defaults), `coalesce_config_accepts_set_values` (round-trip),
  `coalesce_config_rejects_unknown_subfield`
  (`deny_unknown_fields` discipline matches the parent `Config`).

## Default-off rollout plan

1. Land this PR with `enabled: false` in the chart and the
   penpencil overlay.
2. Pre-SHELF-37: do **not** flip on production. The default-off
   path bumps `outcome=disabled` once per shim GET — this is
   intentional, it's the single observable on dashboards that
   confirms the dispatcher is wired but not active.
3. Once SHELF-37 listener ships and confirms the per-pool hit
   ratio impact is non-negative on a single-replica canary, flip
   `cache.coalesce.enabled: true` on the canary replica in the
   penpencil overlay; soak 24 h.
4. If canary green (origin GET rate down materially, no rise in
   `shelf_coalesce_ranges_total{outcome=circuit_open}`,
   `shelf_coalesce_window_seconds` p99 < 1 ms), flip the rest of
   the replicas one at a time.
5. Auto-rollback signal: `shelf_coalesce_ranges_total
   {outcome=circuit_open}` rate ≥ 1/min over 5 min, OR
   `shelf_coalesce_window_seconds` p99 > 50 ms over 5 min.

## Out of scope (this PR)

- Per-prefix or per-table tuning of the caps (one global setting
  per pod for now).
- Cross-pod coalescing (the dispatcher is in-process per shelfd
  pod; SHELF-23 peer-fetch already covers cross-pod sharing).
- Multipart write coalescing (PUT path is untouched per the SHELF-21
  / SHELF-25 landmines — see `shelfd/src/aws_chunked.rs`).
- Pre-fetching beyond the requested span ("read-ahead"). This
  module only batches **observed** demand; speculative read-ahead
  is SHELF-37 territory.
