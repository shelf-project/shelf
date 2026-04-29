# ADR 0013: Row-group range coalescing in the s3_shim hot path (SHELF-30 v1)

*Status: Accepted (2026-04-29)*
*Deciders: rust-engineer-1, rust-engineer-2, trino-plugin-eng-1*
*Supersedes: none*
*Superseded-by: SHELF-30b (footer-aware row-group quantization, future ADR-0014 — gated on SHELF-34 footer parser landing in production)*

## Context

The store-level [`crate::store::FoyerStore::get_or_fetch`] (SHELF-06) is a
single-flight on the **content-addressed key**
`sha256(etag || offset || length || rg_ordinal)` per ADR-0011. It
already dedupes callers that present the *same* `(offset, length, rg_ordinal)` triple. It does **not** help the case where two
concurrent splits ask for **overlapping but not identical** byte
ranges of the same Iceberg snapshot:

- Caller A asks for `[0, 1024)` (covers one Parquet row group),
- Caller B asks for `[512, 768)` (a sub-range of the same row group),

these two requests hash to **different** content-addressed keys and
therefore each fires its own origin GET. Under the bursty fan-out a
KEDA scale-out event produces against the `cdp` catalog, this
duplication is observable: rep-1's first hour after cutover routinely
showed `physical_input_read_time_millis` clusters where the same row
group was independently fetched by 2-4 splits within a 1-2 second
window.

This is the same class of problem [Varnish solved with **request
collapsing](https://varnish-cache.org/docs/trunk/users-guide/performance.html)**
— a single backend fetch fans out to many concurrent client requests
for the same resource. Vimeo reported 8× origin-bandwidth reductions
from request collapsing in their 2017 production deployment. CloudFlare
implements the same pattern as ["Cache Lock"](https://blog.cloudflare.com/origin-saved-by-coalescing-and-cache-lock/)
on their edge.

Workspace memory for this repo additionally locks two adjacent
constraints:

- **No retry-proxies in front of S3 prefixes for transient read
failures** (the "Tardigrade-style" rejection): SHELF-30 must work
*inside* shelfd, not as a sidecar; otherwise it joins the very
failure pattern the user has explicitly rejected.
- **Cache keys are content-addressed by ETag (ADR-0011) — Iceberg
snapshot safety follows from the key, not from explicit
invalidation hooks.** SHELF-30's coalescing must not break this:
the in-flight tracker is keyed on the *same* ETag the cache key
derives from, so a snapshot bump at origin makes the new request
hash to a fresh key and cannot accidentally join a stale
in-flight leader.

## Decision

Implement `**shelfd/src/coalesce.rs`** with a `RangeCoalescer` keyed
on `(bucket, object_key, etag)` whose value is a list of in-flight
`[offset, offset+length)` ranges, each holding a
`tokio::sync::watch::Sender<Option<Result<Bytes, String>>>`.

Hot-path semantics in `s3_shim::handle_get_object`:

1. After resolving `(offset, length, etag)` for the request, call
  `state.coalescer.try_join_or_register(bucket, key, etag, offset, length)`.
2. If the call returns `Outcome::Leader(guard)`:
  - run the existing `peer_or_origin_fetch` → `FoyerStore::get_or_fetch`
   pipeline unchanged,
  - on completion call `guard.complete(Ok(bytes))` (or
  `guard.complete(Err(msg))` on origin error) to wake every
  follower waiting on the leader's `watch` slot,
  - dropping the guard without `complete` (panic, an unhandled
  control-flow error) publishes a sentinel `Err` to followers via
  `Drop`, so liveness is preserved without operator intervention.
3. If the call returns `Outcome::Follower { receiver, offset_in_leader, length }`:
  - await `receiver.changed()`,
  - call `slice_for_follower(&leader_bytes, offset_in_leader, length)`
  to extract the follower's slice,
  - return a normal `200`/`206` response built from those sliced
  bytes — **no origin GET, no Foyer insert**.
4. If the leader's bytes turn out shorter than the follower expected
  (truncated payload), or the leader published an `Err`, the
   follower **falls through** to the standard fetch path and
   `shelf_coalesce_fallthrough_total{pool, reason}` is bumped. There
   is no scenario where the follower returns wrong bytes.

A **runtime kill-switch** lives at `state.coalesce_enabled`
(default `true`) so an operator can flip the layer off without
rebuilding the binary if a correctness regression ever ships in
the wild — same shape as SHELF-23's `peer_fetch_enabled` and
`conditional_get_enabled` toggles.

## Why **subsumption only** in v1

SHELF-30 deliberately ships only the case where a follower's range is
**fully contained** in a leader's range. We considered three richer
overlap shapes and rejected each for v1:


| Shape                                                                               | What it gains                                                                                                     | Why deferred                                                                                                                                                                                         |
| ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Partial overlap (leader `[0, 100)`, follower `[80, 200)`)                           | Avoids one origin GET on the overlapping prefix                                                                   | Requires merging two futures or fetching the non-overlapping suffix separately — neither is a "single-flight" anymore, both add complexity to the hot path that should land with its own ADR         |
| Superset (leader `[80, 100)`, follower `[0, 200)`)                                  | Avoids a duplicate fetch of the leader's 20 bytes                                                                 | Same as above; the follower would have to fetch `[0, 80)` AND `[100, 200)` and stitch, which is an in-flight reorder rather than a single-flight                                                     |
| Footer-aware quantization (round both ranges to row-group boundaries before keying) | Leader and follower share the **same** content-addressed key, so the follower's slot is also cached for next time | Requires a footer parser in the hot path. SHELF-34's `parquet_meta.rs` is the right home; building a parallel parser inside `coalesce.rs` would duplicate work the page-index sidecar must do anyway |


The plan locks the upgrade path:
**SHELF-30b** (footer-aware quantization) is gated on **SHELF-34**'s
footer parser landing — the trade-off documented below disappears
the moment leader and follower share a key.

## v1 trade-off (followers do not populate their own cache slot)

A v1 follower returns the leader's bytes sliced to its requested
window. The follower **does not** insert its sliced range under its
own content-addressed key into Foyer. The next request for the
*exact* follower range therefore still misses on Foyer, fires a fresh
fetch, and either:

- finds the leader's broader range still cached in Foyer (no origin
GET — Foyer's `get_range`-equivalent serves a sub-slice), in which
case the cost is one extra hashmap probe, or
- finds nothing in Foyer (the leader's broader entry has already been
evicted), and goes to origin.

Empirically the second case is rare: leaders cache Iceberg footers
and row-group ranges at boundaries derived from
[Iceberg split offsets](https://iceberg.apache.org/spec/#parquet-split-offsets),
which are also what Trino's split planner emits. So leaders and
followers tend to share the *same* range in steady state, and
SHELF-30's value comes overwhelmingly from absorbing the burst of
duplicate concurrent fetches during the cold-cache window of a KEDA
scale-out — exactly the failure mode the plan estimates 20-40 %
miss-path latency drop against.

If smoke shows the residual miss-on-exact-follower-key class is
material, **SHELF-30b** removes it entirely by quantizing both
leader and follower ranges to row-group boundaries before deriving
the cache key.

## Liveness, panic safety, and correctness invariants


| Invariant                                                | Mechanism                                                                                                                                                                          |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A leader's panic does not deadlock followers             | `LeaderGuard::Drop` publishes `Err(LEADER_DROPPED_ERR)` on every drop path, including a panic unwind. Followers see the error and fall through.                                    |
| A follower never returns wrong bytes                     | `slice_for_follower` rejects truncated payloads (`end > leader_bytes.len() → None`); the call site bumps `shelf_coalesce_fallthrough_total{reason="truncated"}` and falls through. |
| A follower is never matched against a different snapshot | The map key includes the ETag. ADR-0011 derives the cache key from the same ETag, so any snapshot bump at origin shifts both the cache key and the coalesce key in lockstep.       |
| Coalescing without an ETag is unsafe                     | `try_join_or_register` short-circuits on `etag = None                                                                                                                              |
| No coalescing on length 0                                | Same short-circuit; degenerate empty-range request would wedge follower decoding otherwise.                                                                                        |
| Bookkeeping symmetry across error paths                  | Deregistration runs from both `complete` and `Drop`; the leader's entry can never be left behind even if `complete` is forgotten.                                                  |
| Toggle-off restores pre-SHELF-30 behaviour exactly       | `state.is_coalesce_enabled() == false` makes `handle_get_object` skip the entire layer; the coalescer holds no state and the standard fetch path runs unchanged.                   |


## Consequences

- **Per-pool metric surface** (registered in `metrics.rs`):
  - `shelf_coalesce_leaders_total{pool}` — number of GET requests that
  became a leader (excludes no-op leaders for etag-less / length-0
  callers; those are unobservable in this counter by design).
  - `shelf_coalesce_followers_total{pool}` — number of GETs that
  successfully sliced a leader's payload.
  - `shelf_coalesce_follower_bytes_saved_total{pool}` — bytes returned
  to followers without a fresh origin GET; the numerator of the
  SHELF-30 "$ saved" panel against the `$0.0004 / 1k S3 GET`
  unit cost in ap-south-1.
  - `shelf_coalesce_fallthrough_total{pool, reason}` — followers
  that fell through (`reason ∈ {leader_dropped, leader_error, truncated}`); a sustained non-zero rate is a correctness alarm,
  not a tuning lever.
  - The existing `shelf_s3_shim_response_bytes_total{outcome}` series
  gains a new outcome label `coalesce_follower` so the byte-
  efficiency dashboard can split follower-served bytes from
  Foyer-served hits.
- **Hot-path overhead is one `parking_lot::Mutex<HashMap>` lock
per GET** — bounded, contention-free in steady state because the
lock is released before any I/O. Under burst the lock holds long
enough to either match an existing entry or push a new one; the
entry vector is rarely longer than a handful (one per concurrent
range against the same Iceberg snapshot).
- `**tokio::sync::watch` is the right primitive** because it gives
multi-consumer single-producer with a single value slot. We
considered `tokio::sync::broadcast` (overkill, requires sized
buffer) and `tokio::sync::oneshot` (single-consumer only — would
require fanning out to N oneshots manually). `watch` allocates one
slot per leader; followers pay only `watch::Receiver::clone`.
- **Fall-through is the safe default**, not a fast path. If a
follower's leader fails for any reason, the follower runs its own
`peer_or_origin_fetch` → `get_or_fetch` exactly as it would have
pre-SHELF-30. Correctness is preserved; the cost is one extra
`watch::Receiver::changed().await` round-trip on the failed-coalesce
path. We accept this; the non-failed path is the one we optimised
for.
- **No protocol break.** Trino's S3 client sees identical bytes,
identical headers (same `ETag`, `Content-Length`, `Content-Range`,
`Last-Modified`), identical status codes (`200` / `206`). The
feature is invisible to upstream clients except as a latency win.

## Test surface

- `shelfd::coalesce::tests::`* — 11 unit tests covering: leader
registration, no-op leader for `etag = None | empty | length = 0`,
follower subsumption + slice, partial-overlap rejection,
superset rejection, different-etag separation, different-object
separation, dropped-leader → follower fall-through, error
propagation, slice truncation, multi-follower fan-out.
- The integration suite (`SHELF_INTEGRATION=1 cargo test -p shelfd --test it_`* once an `it_coalesce.rs` lands) is a SHELF-30b
follow-up; the v1 unit coverage is sufficient to ship the
primary correctness invariants.

## References

- `shelfd/src/coalesce.rs`
- `shelfd/src/s3_shim.rs` (`handle_get_object` insertion point)
- `shelfd/src/metrics.rs` (`COALESCE_*_TOTAL` counters)
- `shelfd/src/http.rs` (`ServerState::coalescer`, `coalesce_enabled`)
- ADR-0011 (cache-key spec)
- ADR-0012 (Trino read-path endpoint swap; the s3_shim is the
caller of this code path)
- [Varnish request collapsing](https://varnish-cache.org/docs/trunk/users-guide/performance.html)
- [CloudFlare Cache Lock](https://blog.cloudflare.com/origin-saved-by-coalescing-and-cache-lock/)
- [Iceberg split-offsets, Trino #22250](https://github.com/trinodb/trino/pull/22250) — informs the SHELF-30b quantization upgrade
- `agents/out/03-plan.md` § SHELF-30
- `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md` § P0 lever 1