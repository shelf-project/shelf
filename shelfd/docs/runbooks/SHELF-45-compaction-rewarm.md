# Runbook — operator just ran `ALTER TABLE … EXECUTE optimize`

**Owner ticket:** SHELF-45
**Module:** `shelfd/src/compaction_rewarm.rs`

## What you might see

After a compaction (`OPTIMIZE`, `expire_snapshots`,
`remove_orphan_files`) on an Iceberg table that shelf has been
caching, the Trino dashboard for that table can show:

* a sudden drop in `shelf_rolling_hit_ratio_bps{pool="rowgroup"}`
  for the next batch of queries;
* a rate spike on `shelf_origin_request_seconds` p95;
* `ICEBERG_CANNOT_OPEN_SPLIT` errors on workers that race the
  reactor to the new file paths.

This is the cold-miss morning the reactor is designed to absorb. If
`cache.rewarm.enabled: true`, you should observe the spike *much*
narrower than pre-SHELF-45 — the reactor is racing to warm the new
keys before the next morning's batch arrives.

## What the reactor does

The reactor consumes `IcebergSnapshotEvent`s from a bounded mpsc,
classifies compaction-class transitions, and re-warms the
`added_files` set into the rowgroup pool through the same
single-flight `FoyerStore::get_or_fetch` surface client reads use.
It is rate-limited (default 50 MiB/s/pod) and concurrency-capped
(default 4 in-flight files) so it cannot itself become the
thundering herd.

## What metrics to look at

```promql
# Did the reactor see your compaction?
sum by (outcome) (rate(shelf_rewarm_events_total[5m]))

# Was it classified as compaction?  (`compaction_detected` should move,
# `non_compaction_skipped` should NOT for an OPTIMIZE.)
rate(shelf_rewarm_events_total{outcome="compaction_detected"}[5m])

# Are files actually being re-warmed?
sum by (outcome) (rate(shelf_rewarm_files_total[5m]))

# How long is the reactor taking from snapshot commit to last
# added-file warmed?  p95 is the SLO.
histogram_quantile(0.95, rate(shelf_rewarm_lag_seconds_bucket[15m]))

# Is anything failing?  Non-zero in any reason is a paging signal
# only if sustained > 10 minutes.
rate(shelf_rewarm_errors_total[5m])

# Reactor in-flight (gauge — should never approach the configured
# maxConcurrentFiles for long; if it does, the rate-limit budget is
# too tight or the producer is firing too aggressively).
shelf_rewarm_inflight_files
shelf_rewarm_queue_depth
```

## What to do

### 1 — confirm the reactor is alive

`cache.rewarm.enabled` must be `true`, AND a producer must be
plumbed in. Check:

```bash
kubectl -n <ns> exec <shelf-pod> -- curl -s localhost:9091/metrics \
  | grep -E '^shelf_rewarm_(events|files|errors|inflight|queue)'
```

If every series is exactly zero, either (a) the reactor never
received an event, or (b) `cache.rewarm.enabled: false`. The
default-off path is intentional — the SHELF-37 listener (PR #66)
is the natural producer; without it the reactor parks on an
empty channel.

### 2 — non-compaction event keeps showing up?

`shelf_rewarm_events_total{outcome="non_compaction_skipped"}` rate
much higher than `compaction_detected` means the producer is firing
on every Iceberg snapshot, not just `replace`-class ones. That is
correct behaviour — the predicate
[`is_compaction_event`](../../../shelfd/src/compaction_rewarm.rs)
filters them out. Operators do not need to act unless the
*producer* is generating absurd volume (then look at
[`shelf_rewarm_errors_total{reason="pool_full"}`](../../../shelfd/src/compaction_rewarm.rs)
— the bounded mpsc would be dropping events).

### 3 — `pool_full` errors

```
rate(shelf_rewarm_errors_total{reason="pool_full"}[5m]) > 0
```

`maxConcurrentFiles` is the in-flight semaphore; a non-zero
`pool_full` rate means the producer is firing faster than
`maxConcurrentFiles × maxBytesPerSec / file_size`. The fix is
**not** to bump `maxConcurrentFiles` aggressively — the property
test pins `maxConcurrentFiles ≤ origin.max_inflight / 4`. The fix
is to either (a) raise `maxBytesPerSec` (more re-warm bandwidth),
or (b) accept the dropped re-warms and let the next morning's
queries take the original miss path.

### 4 — `origin_get` errors

```
rate(shelf_rewarm_errors_total{reason="origin_get"}[5m]) > 0
```

The reactor's fetcher reported an error — usually a transient S3
error or a bucket policy issue. Cross-check with
`shelf_origin_request_bytes_total{outcome=~"error|timeout"}`:
if those are also up, the issue is upstream from shelf and the
reactor is just along for the ride.

### 5 — emergency stop

Flip `cache.rewarm.enabled: false` and `helm upgrade`. The reactor
exits its `run()` loop on the next select! tick (≤ a few ms).
Re-warm in flight is dropped on `tokio::task::JoinHandle::abort`
via the cancellation token; the Foyer single-flight slot for any
key the reactor was racing against degrades gracefully (the
follower client read takes the miss path).

### 6 — wait, I want re-warm OFF for ONE table

Out of scope for SHELF-45 — the reactor is on or off per pod. The
listener-side filter (SHELF-37) is the right place to add table
allowlists; until that lands, the per-event drop path
(`non_compaction_skipped`) only fires on the *predicate*, not on
table identity.

## Interactions

* **KEDA worker rotation:** orthogonal. The KEDA cold-miss spike
  on a fresh Trino worker is a different effect (worker has no
  warm shelfd-side bytes locally; SHELF-45 has nothing to do with
  the worker side).
* **B1 zstd compression** + **SHELF-66 coalesce**: the reactor's
  re-warmed entries flow through the same admission + Foyer
  surface, so they pick up compression and coalesce automatically.

## See also

* Design note: [`SHELF-45-compaction-aware-rewarm.md`](../design-notes/SHELF-45-compaction-aware-rewarm.md)
* Source: [`shelfd/src/compaction_rewarm.rs`](../../../shelfd/src/compaction_rewarm.rs)
* Acceptance criteria + tests:
  [`agents/out/SHELF-45-compaction-aware-rewarm.md`](../../../agents/out/SHELF-45-compaction-aware-rewarm.md)
