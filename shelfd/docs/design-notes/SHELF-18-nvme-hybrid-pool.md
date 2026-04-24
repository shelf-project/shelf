# SHELF-18 — Foyer NVMe hybrid tier with S3-FIFO

_Status: implemented (v1)._
_Scope: `shelfd::store::FoyerStore`, `shelfd::config::RowGroupPoolConfig`,
 `shelfd::metrics`, `/stats` in `shelfd::http`._

## Problem

SHELF-17 gave us two **DRAM-only** Foyer pools (`metadata`, `rowgroup`).
The DRAM budget for `rowgroup` is bounded by the pod's memory request
(typically ~30 GiB per replica), which is too small to cache the
working set of a mid-sized warehouse. SHELF-18 bolts an NVMe tier onto
the `rowgroup` pool only; `metadata` stays DRAM-only per
[ADR-0008](../../agents/out/adr/0008-pools-and-eviction.md) so manifest
reads never pay a disk I/O.

## Decision

1. **Hybrid on `rowgroup`, DRAM-only on `metadata`.** When
   `pools.rowgroup.nvme_bytes > 0` the `rowgroup` pool is constructed
   as a `foyer::HybridCache<Key, Bytes>` with the `Engine::Large`
   disk backend and a `DirectFsDeviceOptions` device rooted at
   `pools.rowgroup.nvme_dir`. `metadata` keeps its `foyer::Cache`
   construction verbatim.
2. **S3-FIFO on the memory tier promotes into disk.** Foyer 0.12's
   `S3FifoConfig` is a *memory-tier* eviction policy; entries evicted
   from the small/main/ghost ring with sufficient access probability
   are admitted to the large-object disk engine. This delivers the
   intent of ADR-0009 ("S3-FIFO admission, SIEVE inside DRAM"): cold
   one-shots never touch the NVMe, warm entries do.
3. **Internal `PoolHandle` enum.** `FoyerStore` holds
   `metadata: PoolHandle` and `rowgroup: PoolHandle`, where
   ```rust
   enum PoolHandle {
       Dram { cache: foyer::Cache<Key, Bytes>, dram_capacity: u64 },
       Hybrid { cache: foyer::HybridCache<Key, Bytes>, dram_capacity: u64, disk_capacity: u64 },
   }
   ```
   Every call site (`get`, `insert`, `remove`, `contains`, pin-list
   residency, usage accounting) goes through `PoolHandle` helpers so
   the `Store` trait impl stays tier-agnostic. The enum is
   `pub(crate)` — never leaks past `src/store.rs`.
4. **`nvme_bytes == 0` remains a valid mode.** When the NVMe quota is
   zero, we build a DRAM-only `foyer::Cache` for `rowgroup` with no
   access to `nvme_dir`. This preserves the SHELF-17 behaviour and
   the existing pool-isolation tests pass unchanged. Helm chart
   values can ship `nvme_bytes: 0` for DRAM-only clusters (dev envs,
   CI) with zero boilerplate.
5. **Fail-fast on misconfiguration.** `RowGroupPoolConfig::validate_nvme`
   rejects empty or relative `nvme_dir` values, and the store's
   `build_hybrid_rowgroup` returns
   `Error::Store("pool.rowgroup NVMe init failed: …")` on any
   underlying Foyer error. Ops see the real error in the pod logs
   rather than a silent DRAM fall-back.

## `PoolHandle` rationale

`foyer::Cache` and `foyer::HybridCache` share most of the surface
(`get`, `insert`, `remove`) but not the signatures — `HybridCache::get`
is `async` because it may read from disk, while `Cache::get` is sync.
The `PoolHandle::get(&self, key) -> Option<(Bytes, Tier)>` helper
unifies these by:
- first consulting `cache.memory().get(key)` synchronously, and
- only calling `HybridCache::get(...).await` if the memory tier
  missed.

That lets `FoyerStore::get` (async) observe both outcomes cheaply and
bump `shelf_disk_hits_total` / `shelf_disk_misses_total` on the
right side of the memory/disk boundary — a full miss bumps the
regular `shelf_misses_total` plus `shelf_disk_misses_total`, a disk
hit bumps `shelf_hits_total` plus `shelf_disk_hits_total`, and a
memory hit only bumps `shelf_hits_total`. This matches the dashboard
panels defined in SHELF-27.

`FoyerStore::pin` intentionally uses `PoolHandle::memory_get_len`
(memory-tier only) rather than a hybrid async lookup. Pin is a
synchronous admin call and the pin invariant is "key must be
resident in memory when pinned" — pulling a key off disk into
memory would silently double the DRAM working set without the
operator asking.

## Metrics

Four new series live alongside the existing `shelf_*` family, all in
`shelfd/src/metrics.rs`:

| Metric                       | Type      | Labels         | When populated                                 |
| ---------------------------- | --------- | -------------- | ---------------------------------------------- |
| `shelf_disk_hits_total`      | counter   | `pool`         | memory-miss → disk-hit on a `HybridCache::get` |
| `shelf_disk_misses_total`    | counter   | `pool`         | memory-miss → disk-miss on a `HybridCache::get`|
| `shelf_disk_bytes_used`      | gauge     | `pool`         | `/stats` refresh (best-effort; see below)      |
| `shelf_disk_bytes_capacity`  | gauge     | `pool`         | `FoyerStore::open` + `/stats` refresh          |

All four are registered as module-level `Lazy<…>` so the hot-path
`store.rs` can bump them without plumbing an `Arc<Registry>` in;
`Registry::init` clones the handles for symmetry with the older
struct-carried counters. On a freshly-booted hybrid pool we
pre-touch each series with `inc_by(0)` / `set(0)` so Prometheus
emits a child row before any traffic arrives — dashboards stay green
on a cold replica.

### `disk_bytes_used` is an upper bound

Foyer 0.12's `HybridCache::stats()` exposes a cumulative
`write_bytes` counter but not the live occupancy of the disk ring.
We report `min(write_bytes, disk_capacity)` for
`shelf_disk_bytes_used`. Once the pool is warm this approximation is
exact (the ring always contains `disk_capacity` bytes of live data
once steady state is reached); during the warm-up window the value
may overshoot the real occupancy. SHELF-27 documents the caveat on
the dashboard.

## `/stats` contract change

The `/stats` JSON adds two fields to `rowgroup_pool`:

```json
{
  "rowgroup_pool": {
    "capacity_bytes": 32212254720,
    "used_bytes": 123456,
    "disk_capacity_bytes": 536870912000,
    "disk_used_bytes": 42000000000
  }
}
```

`capacity_bytes` / `used_bytes` still report **DRAM only** — the new
fields are additive, so SHELF-20's HRW weighting can choose to sum
or weight disk separately without ambiguity. `metadata_pool` reports
zero on `disk_capacity_bytes` / `disk_used_bytes` (serde defaults).

## Cluster-gated acceptance criteria

The AC "rowgroup survives pod restart" is **not** closed by SHELF-18
alone. The in-process test `hybrid_pool_survives_store_recreation`
confirms only that `FoyerStore::open` is idempotent against the same
`nvme_dir`; it cannot reproduce a real kubelet-driven pod restart
with a bound PVC. The durability AC is tracked as
`TODO(SHELF-18-ops)` inside that test and will be closed by the
chaos suite (`charts/shelf/tests/pvc-restart.sh`) once the PVC
template lands.

## Files touched

- `shelfd/src/config.rs` — `RowGroupPoolConfig::validate_nvme` +
  unit tests for the validator paths.
- `shelfd/src/store.rs` — `PoolHandle`, `build_hybrid_rowgroup`,
  metric wiring, tier-aware `get`.
- `shelfd/src/metrics.rs` — four new series, `Lazy` statics,
  `EXPOSED_SERIES` update.
- `shelfd/src/control.rs` — `PoolStats::{disk_used_bytes,
  disk_capacity_bytes}`.
- `shelfd/src/http.rs` — populate disk fields in `/stats`.
- `shelfd/tests/it_hybrid_pool.rs` — integration tests for
  hybrid boot, survives-recreation, metrics registration, and the
  `nvme_bytes = 0` regression.
- `shelfd/Cargo.toml` — enable `bytes/serde`, add
  `tempfile` dev-dep.
