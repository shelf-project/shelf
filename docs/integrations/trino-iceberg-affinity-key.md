# Wiring Shelf into Trino's Iceberg connector via `getAffinityKey()`

**Status:** in-scope: Shelf-side helper. **Out-of-scope (this repo):** the
Trino-upstream patch; that lands in `trinodb/trino` in `IcebergSplit.java`.

## TL;DR

[Trino PR #29182](https://github.com/trinodb/trino/pull/29182) ("Route splits
by affinity key in the scheduler", merged 2026-04-27) adds an affinity-key
hook to `ConnectorSplit`. With it, a connector can ask the engine to send
all splits sharing a key to the same worker — which lets a Shelf-fronted
Iceberg deployment exploit per-pod cache locality.

The Shelf-side deliverable is `io.shelf.scheduler.ShelfAffinityKey`. The
Trino-side deliverable is a roughly ten-line patch on `IcebergSplit` (or
the equivalent connector-internal split type) that calls into it.

## Pre-flight verification

Before dispatching the `ShelfAffinityKey` plugin to a Trino coordinator,
verify the running release contains PR #29182:

```
kubectl exec -n trino-db <coord-pod> -- trino --version
```

PR #29182 merged to `master` on 2026-04-27 but is **not** yet in a tagged
Trino release. Do NOT proceed if the cluster's Trino version predates
the first release that incorporates it. Running the plugin on an older
coordinator silently ignores `getAffinityKey()` (the SPI hook is
absent), so splits fall back to the default load-balanced scheduler and
the per-pod-locality gain is lost — which is easy to misread as "the
plugin is wired but not helping" without this check.

Source: F4 finding from the deep-research cross-check, 2026-04-30.

## Why an affinity key helps

| Without affinity key                              | With affinity key                                       |
| ------------------------------------------------- | ------------------------------------------------------- |
| Engine load-balances splits across workers.       | Engine consistent-hashes splits to one worker per key.  |
| Every worker opens TLS to every Shelf pod.        | Workers warm a small, stable Shelf TCP pool.            |
| KEDA scale-out hits Shelf as a thundering herd.   | New workers steal a fraction of keys, the rest stay.    |
| Cross-AZ shim hops are random.                    | Same-AZ pairing is achievable when sched policy aligns. |
| Iceberg JVM metadata cache thrashes on file churn.| Same files repeatedly hit the same worker JVM.          |

The expected production payoff (per the Shelf algorithmic optimization
roadmap, P3 lever): roughly halve cross-AZ data egress, materially improve
p99 by exploiting cache locality.

## Why `getAffinityKey()` and not `getAddresses()` alone

`ConnectorSplit#isRemotelyAccessible() + getAddresses()` already exists in
the Trino 480 SPI. Returning a `HostAddress` list scoped to the Shelf
pod's worker would already pin a split to that worker. But:

- `getAddresses()` is interpreted as a *locality hint*, except when
  `isRemotelyAccessible() == false` — in which case the addresses become
  hard pins. Hard-pinning Shelf-owner workers in a KEDA-scaled deployment
  produces stranded splits when a worker rotates.
- The address list semantics tie the connector to per-worker IP/host
  bookkeeping. Shelf's HRW selector already maps keys to *pod ids*, not
  worker addresses; bridging requires a worker→pod registry the connector
  doesn't own.
- `getAffinityKey()` is consistent-hashed by the engine over the *current*
  worker set. Workers come and go; the split-to-worker mapping
  rebalances gracefully. We get the locality benefit without the
  scheduling-strictness footgun.

So: keep the existing `isRemotelyAccessible() = true` default, override
`getAffinityKey()` only.

## Java helper API

`io.shelf.scheduler.ShelfAffinityKey`:

```java
Optional<String> forKey(byte[] key, List<String> podIds);
Optional<String> forKey(String key, List<String> podIds);
```

The function is byte-identical to `shelfd::router::hrw_score` (SHA-256 over
`key || podId.utf8()`, top-53-bit mantissa, `weight / -ln(x)`). Cross-language
parity is asserted by `io.shelf.client.HashRingTest#ownerMatchesGoldenFixture`
against the Rust-generated fixture at
`shelfd/tests/fixtures/hrw_golden_vectors.txt`. `ShelfAffinityKey` delegates
to `HashRing` so the parity guarantee carries over.

Returns `Optional.empty()` for an empty or `null` `podIds` list. Trino's
`ConnectorSplit#getAffinityKey()` should map this to `Optional.empty()` so
the engine falls through to default scheduling, preserving correctness when
the membership snapshot is stale.

## Trino-fork patch sketch

The patch is short and isolated. Add a field to the split type, populate it
where splits are produced, and override `getAffinityKey()`. This is sketched
against `trino-iceberg`'s `IcebergSplit`; the same shape applies to any
Iceberg-style connector.

### 1) Add the helper jar to the connector pom

```xml
<dependency>
    <groupId>io.shelf</groupId>
    <artifactId>shelf-trino-plugin</artifactId>
    <version>${shelf.version}</version>
</dependency>
```

### 2) Plumb the Shelf pod-id list into the split source

The pod list is published by `MembershipResolver` (Java) or fetched from
shelfd's `/stats` endpoint. The connector caches the snapshot per query
(or per scheduling decision) and passes it into split construction:

```java
// IcebergSplitSource (sketch):
private final List<String> shelfPodIds = membershipSnapshot.podIds();
// ... when building each IcebergSplit ...
new IcebergSplit(..., shelfPodIds);
```

If the connector cannot reach Shelf (membership resolver returns empty),
pass `List.of()` so `forKey` returns `Optional.empty()` and the engine
falls back to default scheduling.

### 3) Override `getAffinityKey()` on the split

```java
// IcebergSplit.java (≈10 LOC delta):
import io.shelf.scheduler.ShelfAffinityKey;

private final List<String> shelfPodIds;   // new field, JsonProperty-serialized

@Override
public Optional<String> getAffinityKey()
{
    if (shelfPodIds.isEmpty()) {
        return Optional.empty();
    }
    // Hash the file path; coarsest-but-stable choice, see "Choosing the
    // routing key" below for finer-grained alternatives.
    return ShelfAffinityKey.forKey(path(), shelfPodIds);
}
```

That is the entirety of the activation — provided #29182 is in the running
Trino release.

## Choosing the routing key

The bytes you hash determine the granularity of co-location. Three honest
choices:

| Routing key         | Granularity         | Pros                                | Cons                                              |
| ------------------- | ------------------- | ----------------------------------- | ------------------------------------------------- |
| File path string    | One owner per file  | Trivial; survives Iceberg snapshots | Coarsest; mixed-row-group files share an owner    |
| Iceberg file ETag   | One owner per file  | Stable across snapshots that don't  | Requires the connector to surface ETag (it does;  |
|                     |                     | rewrite the file                    | manifest entries carry it)                        |
| SHELF-04 cache key  | One owner per RG    | Matches shelfd's actual routing 1:1 | Requires the connector to compute                 |
|                     |                     |                                     | `sha256(etag || u64_le(off) || u64_le(len)        |
|                     |                     |                                     | || u32_le(rg_ordinal))` per row group split       |

For an MVP, file path is enough — the per-file warm-pool wins dominate the
per-row-group precision wins, and the file-path overload is one line of code.
Upgrade to SHELF-04 cache keys later if the replay (SHELF-35) shows
remaining locality headroom.

## Cluster-side activation criteria

This patch is harmless when the running Trino release does not contain PR
#29182 — `getAffinityKey()` simply isn't called by the scheduler. It
becomes effective only when:

1. The cluster's Trino release pin is bumped to a version that includes
   PR #29182 (the PR landed 2026-04-27; check the first published Trino
   release after that date and verify the commit is present in its
   `release-notes/` entry).
2. The connector running on those workers contains the patch above.
3. `ShelfAffinityKey` is reachable on the worker classpath (the
   `shelf-trino-plugin` jar is wired into the connector's plugin
   directory, same as `ShelfPrefetchListener`).

Test-validation surfaces:

- `io.shelf.client.HashRingTest#ownerMatchesGoldenFixture` — anchors the
  Java implementation to the Rust-generated golden vectors.
- `io.shelf.scheduler.ShelfAffinityKeyTest` — fixed-vector + uniformity +
  edge-case coverage of the helper API.
- Connector-level integration test: a Trino test fork can assert that
  splits with identical `getAffinityKey()` land on the same worker via
  `QueryRunner` + an instrumented scheduler.

## Rollback signals

Verbatim from the SHELF-39 plan entry:

| Trigger                                                                              | Action                                |
| ------------------------------------------------------------------------------------ | ------------------------------------- |
| Per-AZ EKS data-egress cost spikes > 20% vs pre-cutover for > 24h                    | revert connector to default routing   |
| `shelf_rolling_hit_ratio_bps` drops > 5pp (affinity collapse onto a few pods)        | revert                                |

The first signal catches an "anti-locality" regression where the scheduler
ends up routing to workers in different AZs from the Shelf owner. The
second signal catches a HRW imbalance amplified by the affinity-key path
— if a single key family dominates traffic, all of it lands on one worker
and one Shelf pod, depressing the cluster-wide hit ratio.

## Out of scope

- The Trino-upstream patch itself — it lives in `trinodb/trino`, not in
  this repo. Track as `SHELF-39-trino-upstream` once the cluster is on a
  release that contains #29182.
- Connector-side membership refresh — already covered by
  `io.shelf.client.MembershipResolver`. The connector reuses that class.
- Per-AZ topology hints layered on top of `getAffinityKey()` — out of
  scope for v1; revisit if EKS topology spread evidence warrants it.

## References

- Trino PR [#29182](https://github.com/trinodb/trino/pull/29182) — affinity
  key SPI.
- Trino PR [#22190](https://github.com/trinodb/trino/pull/22190) — earlier
  tightening of remote-accessible split scheduling.
- ADR-0002 — HRW over a vnode ring.
- ADR-0011 — SHELF-04 cache-key spec.
- ADR-0012 — Trino read-path endpoint swap (the architectural reason
  Shelf is not a connector).
