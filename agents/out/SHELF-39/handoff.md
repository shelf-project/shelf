# SHELF-39 — Cache-aware Trino split scheduling via `getAffinityKey()`

**Status:** `DELIVERED — gated on cluster Trino release pin + Trino-upstream Iceberg connector patch`
**Owner:** shelfd / clients/trino
**Plan ref:** `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md` (P3 lever, two passages: priority-table row at line 121, lever detail at lines 331–341)

## Discovery corrections (vs. the orchestrator prompt)

1. The orchestrator prompt said "discover the existing HRW implementation in
   `shelfd/src/peer.rs`". HRW is **not** in `peer.rs` — that module owns
   peer-vs-origin race primitives, not membership routing. The HRW algorithm
   lives in **`shelfd/src/router.rs`** (`hrw_score`, `Router::owner`,
   `is_local_owner`). The prompt's framing was misleading; the actual
   implementation was found and verified.
2. The orchestrator prompt said the hash function is "likely XXHash3-based".
   It is not. **`shelfd::router::hrw_score` uses SHA-256**, not XXHash3
   (verified by reading `shelfd/src/router.rs:145-160`). SHA-256 is built
   into the JDK via `java.security.MessageDigest`, so no new Java dependency
   was needed — the prompt's `net.openhft:zero-allocation-hashing` /
   pure-Java XXHash3 plan was discarded as it would have introduced a hash
   function that disagrees with shelfd. The honest path is the existing one.
3. The prompt asked to add a Rust known-vector test to `shelfd/src/peer.rs`.
   That was unnecessary because `shelfd/src/router.rs::tests::owner_matches_golden_vectors`
   already exists and locks 1000 expected `(key_i, owner)` pairs into
   `shelfd/tests/fixtures/hrw_golden_vectors.txt`. No Rust source was
   touched in this ticket.
4. The orchestrator prompt assumed there was no Java HRW today. There was —
   `clients/trino/src/main/java/io/shelf/client/HashRing.java` already
   implements the algorithm and is cross-validated against the same Rust
   fixture by `clients/trino/src/test/java/io/shelf/client/HashRingTest.java`.
   The new helper delegates to that class instead of duplicating the math.

## Branch + files

- Worktree: `/private/tmp/shelf-39-affinity-key`
- Branch: `shelf-39-affinity-key`
- Base: `dae78b6` (`origin/main` at start of session)

Files added:

| Path                                                                            | Purpose                                                                  |
| ------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `clients/trino/src/main/java/io/shelf/scheduler/ShelfAffinityKey.java`          | Helper bridging `HashRing` to Trino's `ConnectorSplit#getAffinityKey()`. |
| `clients/trino/src/test/java/io/shelf/scheduler/ShelfAffinityKeyTest.java`      | 13 JUnit tests (4 fixed vectors, uniformity, edge + overload coverage).  |
| `docs/integrations/trino-iceberg-affinity-key.md`                               | "How to wire in your Trino fork" doc + ~10 LOC patch sketch.             |
| `agents/out/SHELF-39/handoff.md`                                                | This file.                                                               |

Files modified: none. (The orchestrator prompt anticipated touching
`shelfd/src/peer.rs`; no Rust edit was warranted — see correction #3 above.)

## Test results

### Java — `ShelfAffinityKeyTest` (13 tests)

Maven build with the project's pinned `<release>25</release>` is impossible
locally (no JDK 25 installed; the workspace memory documents this is the
same gap that makes the GHA Maven cascade fail org-wide). The new helper
has zero `trino-spi` dependencies, so the suite was compiled and executed
standalone under JDK 21:

```
=== Running ShelfAffinityKeyTest ===
Test run finished after 359 ms
[ 13 tests found ]
[ 13 tests successful ]
[ 0 tests failed ]
```

Existing `HashRingTest` (the 1000-vector cross-language fixture) was
re-run under the same standalone harness as a regression guard:

```
=== Running HashRingTest (existing cross-language fixture) ===
Test run finished after 336 ms
[ 4 tests found ]
[ 4 tests successful ]
[ 0 tests failed ]
```

Once the cluster has JDK 25 available (or once the org-wide
`<release>25</release>` cascade is resolved), the canonical command is:

```
cd clients/trino && mvn -B clean test
```

That run is expected to execute 13 + N pre-existing tests under the
project Surefire configuration.

### Rust — `cargo test -p shelfd --lib router`

Rust HRW tests still green; included to confirm the Java fixed vectors
remain anchored to the same Rust truth set:

```
running 7 tests
test router::tests::empty_ring_is_not_local_owner ... ok
test router::tests::owner_panics_on_empty_ring - should panic ... ok
test router::tests::is_local_owner_agrees_with_owner ... ok
test router::tests::owner_is_stable_when_unrelated_member_joins ... ok
test membership::tests::resolver_loop_populates_router_from_scripted_io ... ok
test router::tests::owner_matches_golden_vectors ... ok
test router::tests::heavier_node_wins_more_often ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 225 filtered out; finished in 0.04s
```

### Cross-vector identity (Java ↔ Rust ↔ Python)

The fixed-vector tests in `ShelfAffinityKeyTest` were anchored to a
third independent Python reference implementation of HRW. The script
follows; `owner_matches_golden_vectors` (Rust) and the new Java tests
both agree with these vectors.

```python
import hashlib, math, struct

def hrw_score(key: bytes, pod_id: str, weight: float = 1.0) -> float:
    h = hashlib.sha256()
    h.update(key)
    h.update(pod_id.encode("utf-8"))
    digest = h.digest()
    u64_be = int.from_bytes(digest[:8], "big")
    top53 = u64_be >> 11
    x = top53 / (1 << 53)
    if x <= 0:
        return float("inf")
    return weight / (-math.log(x))

def owner(key: bytes, pods):
    best, best_s = None, float("-inf")
    for p in pods:
        s = hrw_score(key, p)
        if best is None or s > best_s or (s == best_s and p < best):
            best, best_s = p, s
    return best

def golden_key(i: int) -> bytes:
    h = hashlib.sha256()
    h.update(b"shelf-hrw-golden-v1")
    h.update(struct.pack("<I", i))
    return h.digest()
```

Fixed vectors used in the JUnit suite (4-pod uniform ring
`[shelf-0, shelf-1, shelf-2, shelf-3]`):

| `i`  | `key` (first 8 bytes hex) | Expected owner (Python = Java) |
| ---: | -------------------------- | ------------------------------ |
| 0    | `98c3b6ef46e4a2a4`         | `shelf-3`                      |
| 1    | `c0af0c5fec069afe`         | `shelf-3`                      |
| 7    | `7919149a548b0e33`         | `shelf-2`                      |
| 17   | `38c337b749ea2d03`         | `shelf-3`                      |
| 42   | `2333dac9d24b1d47`         | `shelf-2`                      |
| 100  | `02eb69dbadc4f6b1`         | `shelf-3`                      |
| 999  | `251a483343641416`         | `shelf-2`                      |

Distribution check (1000 keys, 4-pod uniform ring): 252 / 273 / 228 /
247, worst deviation 9.20% from the expected mean of 250 — well inside
the ±15% bound the JUnit suite asserts.

## Honest scope (what this PR is NOT)

Shelf is a filesystem-swap (ADR-0012), not a Trino connector fork. The
`getAffinityKey()` method that the engine calls lives on Trino's own
`ConnectorSplit` implementation (`IcebergSplit` and friends) inside
`trinodb/trino`, not in this repo. So the **scheduler activation cannot
land here** — only the helper that the future Trino patch calls into.

That's the gap between this PR and the plan's stated payoff
("halve cross-AZ traffic, materially improve p99"). The helper is
necessary but not sufficient.

## Open follow-ups

1. **Trino-upstream Iceberg connector patch.** ~10 LOC delta on
   `IcebergSplit` (or the per-table-format equivalent) that adds a
   `List<String> shelfPodIds` field, plumbs it through the split source,
   and overrides `getAffinityKey()` to call
   `ShelfAffinityKey.forKey(...)`. Patch sketch in
   `docs/integrations/trino-iceberg-affinity-key.md`. Out-of-scope for
   this repo; track as `SHELF-39-trino-upstream`.
2. **Cluster Trino release pin.** PR #29182 merged 2026-04-27; it must
   ship in a Trino release the cluster's Helm pin actually rolls to
   (Trino's release cadence is roughly monthly). Verify the running
   replica's `system.runtime.nodes` reports a release that contains the
   commit before the upstream patch above is dispatched.
3. **Membership snapshot wiring on the worker side.** The Trino patch
   needs to call `MembershipResolver` (or fetch `/stats` on demand) per
   query / per scheduling decision. The current `MembershipResolver` is
   coordinator-side; the patch will need to ensure it runs on workers
   too, or the snapshot is propagated via `ConnectorSplitSource`.
4. **Replay-quantified payoff.** Per the SHELF-35 / replay-harness
   gate, the actual cross-AZ-egress reduction should be measured with
   the harness once the upstream patch lands and a canary replica is
   on a `getAffinityKey()`-aware Trino release. The plan's "$100–300/mo"
   estimate is a P3 long-tail figure, not a measurement.

## Cluster-side activation gate

This change is harmless when the running Trino release does not contain
PR #29182 — `getAffinityKey()` is simply not called by the scheduler.
It becomes effective only when:

- The cluster's Trino release pin is bumped to a release that includes
  #29182.
- The Trino-upstream Iceberg patch (follow-up #1) lands.
- The patched `shelf-trino-plugin` jar is deployed to worker plugin
  directories.

Until then there is no rollback signal to monitor, because nothing on
the hot path changes.

## Rollback signals (verbatim from plan)

| Trigger                                                                              | Action                                |
| ------------------------------------------------------------------------------------ | ------------------------------------- |
| Per-AZ EKS data-egress cost spikes > 20% vs pre-cutover for > 24h                    | revert connector to default routing   |
| `shelf_rolling_hit_ratio_bps` drops > 5pp (affinity collapse onto a few pods)        | revert                                |

## References

- Trino PR [#29182](https://github.com/trinodb/trino/pull/29182) (merged 2026-04-27).
- Trino PR [#22190](https://github.com/trinodb/trino/pull/22190) — earlier locality-hint tightening.
- ADR-0002 — HRW over a vnode ring.
- ADR-0011 — SHELF-04 cache-key spec.
- ADR-0012 — Trino read-path endpoint swap.
- Plan: `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md`.
