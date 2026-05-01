# ADR-0036 — A3: Compaction-rewarm via Iceberg `metadata.json` polling

- **Status**: Accepted (rc.7, A3)
- **Deciders**: shelf core
- **Date**: 2026-05-01
- **Tickets**: SHELF-A3 (rc.7 plan), SHELF-45 (compaction reactor), SHELF-37 (parked listener)
- **Supersedes**: none
- **Superseded by**: none

## Context

The SHELF-45 compaction-rewarm reactor (built default-OFF in
v1.0.0) was designed to absorb the cold-morning S3 spike: when an
Iceberg `EXECUTE optimize` rewrites a hot table's data files
overnight, the next morning's queries land 100% cold against the
new file ETags because the SHELF-04 content-addressed key changes
on every rewrite. Workspace memory pins this as **the single
largest daily S3 cost on rep-2** (post-`EXECUTE optimize` 100%
miss morning).

The reactor's *trigger source* — the SHELF-37 Iceberg `EventListener`
jar (PR #66) — is parked indefinitely on the JDK-25 absence
(workspace memory: `trino-spi:480.jar` ships class-file major 69;
our Trino containers run JDK 22). Until JDK 25 lands or the
listener is back-ported to a JDK ≤22 binary, the reactor sits idle
with `enabled: false` and the cold-morning spike remains
unaddressed.

A3 unblocks the reactor by **bypassing the Trino-side listener
entirely**: a small `shelfd`-internal worker polls each watched
table's `metadata.json` directly on a configurable interval
(default 30 s), classifies the latest snapshot, and forwards
compaction-class transitions to the existing reactor through its
public `SnapshotPublisher` surface.

## Decision

Add a new `shelfd::rewarm_poller` module that:

1. Reads each watched table's `metadata.json` every
   `cache.rewarm.poll_interval` (default `30s`). The cheap path
   uses S3 `If-None-Match` against the etag of the previous probe
   (304 short-circuit); the dirty path GETs the full body and
   parses the JSON.
2. When `current-snapshot-id` changes and the new snapshot's
   `summary["operation"]` is `"replace"`, walks the Iceberg
   manifest list / manifests (Avro) to recover the per-file diff,
   `HEAD`s each new data file to recover the ETag the SHELF-04
   key needs, and publishes an `IcebergSnapshotEvent` into the
   SHELF-45 reactor via `SnapshotPublisher::try_publish`.
3. Caps the per-snapshot byte budget at
   `cache.rewarm.max_bytes_per_snapshot` (default 5 GiB). Files
   beyond the cap bump `shelf_rewarm_bytes_capped_total` instead
   of joining the enqueue.
4. Short-circuits the enqueue when the local pod's
   `DrainSignal` (A2) is active — the reactor's downstream
   admit gate would refuse the bytes anyway, so spending S3 GETs
   to chase a draining pod is pure waste.

The poller is **default-OFF** (`cache.rewarm.enabled: false`)
and the OSS chart ships `cache.rewarm.tables: []` — operators
opt in per cluster after their first soak window. The penpencil
overlay (`infra/penpencil/charts/shelf/values-prod.yaml`) keeps
the same shape; bucket names live there as a commented-out
example, never as active config.

### Why polling, not the listener

| Path                                         | Pros                                       | Cons                                                                    |
| -------------------------------------------- | ------------------------------------------ | ----------------------------------------------------------------------- |
| SHELF-37 listener (PR #66)                   | Real-time; same JVM that committed the snapshot | Parked: requires JDK 25; would also need a JNI tunnel into shelfd       |
| **A3 metadata.json polling (this ADR)**      | No JVM dep; small, testable Rust loop      | 30 s detection lag; ~12 GETs/min for 100 tables (trivial)               |
| Iceberg REST catalog event subscription      | Real-time; catalog-native                  | Adds a new HTTP/SSE protocol surface and Trino-side infra; larger blast radius |

Polling wins on operational simplicity for the v1 unblock. The
listener path remains viable if/when JDK 25 lands; the reactor
can accept events from either source unchanged because the
poller publishes through the *exact same* `SnapshotPublisher`
the listener was designed to use.

### Composability with A1 (RSS) and A2 (drain)

The reactor calls `crate::store::FoyerStore::get_or_fetch` on
every fetch; that path routes through `_admit_or_insert` which
already composes:

1. **A2 drain check** (verified in `shelfd/src/store.rs:1154`,
   `if !ctx.pinned && self.drain_refuses_admits()` — pre-existing).
2. Size-threshold policy.
3. SHELF-21e LODC level gate.
4. SHELF-29 byte-rate token bucket, which itself layers on the
   **A1 RSS multiplier** in `crate::admission_limiter::LodcAdmission::try_admit`.

So A1 + A2 apply transparently to rewarm prefetches with **zero
new wiring** in `rewarm_poller`. The poller's belt-and-braces
drain-check before publish is purely an efficiency: it avoids
the GET round trip that would otherwise be refused by gate (1)
above.

### Removed-file shape

The reactor's `is_compaction_event` predicate
(`shelfd/src/compaction_rewarm.rs:149`) requires both
`removed_files` and `added_files` to be non-empty and
`added.bytes ≈ removed.bytes` within the configured tolerance.
Iceberg manifests after a `replace` snapshot do not always
carry `status=2` (DELETED) entries — some writers fold removals
into EXISTING-on-old-manifest semantics. When the per-file diff
is empty on the removal side, the poller synthesizes a
placeholder `removed_files` list whose `(count, total_bytes)`
matches the snapshot summary's
`removed-files-size` / `deleted-data-files`. The reactor's
predicate consumes only `(count, total_bytes)` from
`removed_files`; the synthetic placeholder paths are never
fetched. See the inline comment in `RewarmPoller::poll_once`.

## Consequences

### Positive

- Unblocks the SHELF-45 reactor without depending on JDK 25.
  Workspace memory pegs the lift at **15-25% daily S3 cost
  reduction** on rep-2 once the cold-morning spike is absorbed.
- Detection latency is bounded by `poll_interval` (default
  30 s); cold-morning queries arriving at 06:00 IST see the
  rewarm finished by 06:01-06:02 even on the largest configured
  tables (the reactor's own `max_bytes_per_sec=50 MiB/s` finishes
  a 5 GiB compaction in ≈100 s).
- Default-OFF + empty default `tables` means the OSS chart ships
  byte-identical to v1.0.0 for any operator who hasn't opted in.
- Per-snapshot cap (`max_bytes_per_snapshot`, default 5 GiB)
  defends against runaway prefetch on a 200 GiB
  `expire_snapshots` rewrite.

### Neutral

- Steady-state cost: ~12 GETs/min for 100 watched tables at 30 s
  cadence. The cheap path is a `version-hint.text` GET (a few
  bytes) plus a 304 on the metadata.json itself; the dirty path
  fires only when a snapshot actually changed.
- Adds `apache-avro` (~3 MB compile output) as a workspace dep.
  Default-features off (skips the snappy/zstd/bzip codec
  backends — Iceberg manifests use the built-in `deflate`).

### Negative / risks

- 30 s detection lag means a worst-case query arriving in the
  first 30 s after a snapshot rotation still misses cold. The
  reactor's `shelf_rewarm_lag_seconds` histogram tracks this;
  the SHELF-45 design accepted multi-minute lag already.
- Iceberg writers in the wild emit minor manifest-schema
  variants (v1 inlined `data_file` fields vs v2 nested
  `data_file` records). The parsers in
  `rewarm_poller::iceberg` accept both shapes; an unknown future
  shape would surface as zero added/removed entries (no
  enqueue, plus the `iceberg_metadata` error counter ticks). No
  panic, no client-traffic impact.
- Catalog-managed Iceberg tables (REST catalog, Glue, Hive) do
  not write `version-hint.text`. The poller falls back to
  listing `metadata/*.metadata.json` and picks the
  lexicographically largest entry — robust for `v<N>.metadata.json`
  writers up to N≈10⁹. Tables that use UUID-prefixed metadata
  filenames (some Glue catalogs) fall back to listing too,
  where lexicographic order doesn't match commit order; the
  poller will detect *some* snapshot change but may miss the
  most recent one until the next tick. We accept this for v1;
  follow-up A3-bis (or B3) wires a real catalog client.

### Test coverage

9 unit tests in `shelfd::rewarm_poller::tests` (per spec):

1. `disabled_config_does_not_spawn` — `enabled=false` path.
2. `empty_tables_no_op` — zero S3 calls when `tables: []`.
3. `etag_matches_returns_no_change` — 304 fast-path metric.
4. `non_replace_snapshot_does_not_trigger` — append/overwrite
   updates baseline without enqueueing.
5. `replace_snapshot_enqueues_new_files` — happy path: detected
   metric ticks, files + bytes counters move, event reaches
   the publisher.
6. `bytes_cap_enforced` — 10 GiB compaction with 5 GiB cap
   enqueues exactly 5 GiB and bumps `bytes_capped_total` for
   the rest.
7. `drain_active_short_circuits` — drain bit flipped → no
   publish, detected counter still ticks.
8. `s3_error_increments_error_counter_does_not_panic` —
   surfaced error returns `Err`, loop continues.
9. `defensive_same_snapshot_id_skipped` — rotated metadata.json
   with same `snapshot_id` updates etag without re-enqueuing.

Plus 2 micro-tests for the iceberg parser
(`split_s3_url_handles_known_schemes`,
`metadata_json_parses_minimal_replace_snapshot`).

## Alternatives considered

1. **Revive SHELF-37 by minting JDK 25**. Deferred — the JDK 25
   timeline is uncertain and we don't want the cold-morning
   spike held hostage to it.
2. **Iceberg REST catalog event subscription**. Adds a new
   protocol surface (HTTP SSE / WebSockets) and another piece
   of Trino-side infrastructure to operate. Larger blast
   radius for a path that polling already covers cheaply.
3. **S3 LIST diff on the data prefix**. Would avoid Avro
   entirely but explodes on hot tables (10⁵+ files / list).
   Not viable at production scale.
4. **Skip file-level prefetch in v1; emit detection metric only**.
   Considered for scope. Rejected because the metric alone
   doesn't deliver the 15-25% cost lift; the whole point of A3
   is to wire the reactor's *fetch* path to a real trigger.

## References

- Workspace memory: cold-morning post-`EXECUTE optimize` 100%
  miss morning (rep-2, daily).
- SHELF-45 — `shelfd/src/compaction_rewarm.rs` (the reactor
  this PR drives).
- SHELF-37 PR #66 (parked) — the listener path A3 bypasses.
- SHELF-A2 / ADR-0027 — drain gate this poller respects.
- SHELF-A1 / ADR-0029 — RSS gate this poller composes with via
  the unified admit path.
- `shelfd/src/store.rs:1154` — drain admit-gate composition
  reading verified before commit.
- `shelfd/src/admission_limiter.rs:1198` — RSS / rate-limiter
  composition reading verified before commit.
