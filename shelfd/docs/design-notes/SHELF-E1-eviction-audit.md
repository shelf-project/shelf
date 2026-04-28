# SHELF E1 — Foyer eviction policy audit

> **Update 2026-04-27 (E1b)** — the hybrid row-group pool now defaults
> to **LRU**, not S3-FIFO. The old default starved the NVMe tier under
> one-shot Metabase reads (`shelf_disk_bytes_used` stayed at 0 for
> 8+ hours of live traffic on rep-2). Operators can opt back into
> S3-FIFO via `cache.pools.rowgroup.evictionPolicy: s3_fifo`.
> See **SHELF-E1b — Why we walked back from ADR-0009** below.

## Current state (post-E1b)

| Pool | Tier | Builder | Effective policy | Source |
| --- | --- | --- | --- | --- |
| `Pool::Metadata` | DRAM only | `foyer::CacheBuilder::new(cap)` | **SIEVE** (Foyer builder default) | `shelfd/src/store.rs` |
| `Pool::RowGroup` (DRAM-only fallback) | DRAM only | `foyer::CacheBuilder::new(cap)` | **SIEVE** (Foyer builder default) | `shelfd/src/store.rs` |
| `Pool::RowGroup` (hybrid, nvme_bytes > 0) | DRAM + NVMe | `HybridCacheBuilder::memory(dram).with_eviction_config(<policy>)` | **LRU** in DRAM (default; configurable via `RowGroupPoolConfig::eviction_policy`), **round-robin with reclaim** on disk tier | `shelfd/src/store.rs` |
| `HeadLru.cache` | DRAM only | `foyer::CacheBuilder::new(cap)` | **SIEVE** (Foyer builder default) | `shelfd/src/head_lru.rs` |
| `HeadLru.negative` (D4, new) | DRAM only | `foyer::CacheBuilder::new(cap)` | **SIEVE** (Foyer builder default) | `shelfd/src/head_lru.rs` |

## ADR reconciliation

- **ADR-0008** specifies exactly two Foyer pools in v1.
- **ADR-0009** specifies S3-FIFO for the hybrid row-group pool. The
  audit confirms the hybrid path is wired exactly as ADR-0009
  prescribes.
- No ADR mandates a specific DRAM-tier eviction policy for the
  **DRAM-only fallback** of `Pool::RowGroup`. That path exists so CI
  and dev clusters (without an NVMe PVC) can still exercise the
  read path; production always takes the hybrid branch.

## What E1 asked

> Audit current Foyer eviction policy per pool (SIEVE vs S3-FIFO);
> if rowgroup is SIEVE today, flip to S3-FIFO per ADR-0009 and
> re-run SHELF-26 replay harness.

**Finding**: production `Pool::RowGroup` is already S3-FIFO (not
SIEVE) — the flip the plan hedged against is already in place. The
DRAM-only fallback branch is SIEVE and stays SIEVE: it runs in CI,
has no ADR, and SIEVE has lower per-entry overhead for the
single-digit MiB caches CI uses.

## Remaining decision: metadata-pool policy

`Pool::Metadata` currently runs SIEVE. Candidates are:

1. **Stay on SIEVE** — current production behaviour, mostly-static
   `metadata.json` / manifest workloads skew heavily towards a small
   hot set that SIEVE's "give each entry one second chance" handles
   efficiently.
2. **Flip to S3-FIFO** — better scan resistance under bursty
   commit-heavy workloads (snapshot rewriters, dbt full-refresh),
   because S3-FIFO demotes small-insertion scans quickly.

We flip to S3-FIFO **only if** the SHELF-26 replay harness shows a
hit-rate lift ≥ 1 percentage point on the 7-day rep-2 trace, at or
below the current `metadata_capacity`. Smaller lifts are lost in
replay variance.

## Replay procedure

```
cd benchmarks/trino_logs
make replay-rep2-7d SHELF_EVICTION_METADATA=sieve   OUT=out/e1/sieve
make replay-rep2-7d SHELF_EVICTION_METADATA=s3fifo  OUT=out/e1/s3fifo
python scripts/diff_sim.py out/e1/sieve out/e1/s3fifo --key hit_ratio_metadata
```

The harness already supports per-policy sweeps via `SimConfig`; the
only new knob is plumbing `SHELF_EVICTION_METADATA` into
`simulate::build_metadata_pool`.

## Action items (tracked, not executed in this session)

1. [ ] Thread `eviction_metadata: EvictionPolicy` through
   `config::PoolsConfig` and `FoyerStore::open`.
2. [ ] Run the sweep above over the 7-day trace.
3. [ ] If lift ≥ 1 pp, ship the flip behind
   `features.s3fifo_metadata = true`, default off for 7 days of
   soak, promote to default on in v0.6.
4. [ ] If lift < 1 pp, close as a no-change, document in
   `docs/changelog.md`, keep SIEVE.

## Gate

- SHELF-26 replay harness run reproducible from
  `benchmarks/trino_logs/README.md §Replay`.
- Output CSVs committed under `benchmarks/trino_logs/results/e1-audit/`.
- Conclusion (change vs no-change) committed as a follow-up PR
  that either flips the builder or updates this note with "audited
  — no change".

## Why we are not flipping today

The row-group pool — the dominant consumer of cache capacity — is
already on S3-FIFO via the hybrid builder. The metadata-pool
question is a tuning knob, not a correctness question, and the
SHELF-26 harness is the right instrument to decide it. Shipping a
policy flip without replay evidence would violate the same "evidence
before assertions" rule that gated the rest of Tier 0.

---

## SHELF-E1b — Why we walked back from ADR-0009 (2026-04-27)

### Symptom

After cutting `cdp_shelf` over to rep-2 we watched the Grafana
"Shelf — cache, disk and pods" board for 8 hours. Throughout the
window:

- `shelf_http_requests_total` climbed steadily (~3 k/s sustained).
- `container_memory_working_set_bytes` settled around 11–12 GiB
  inside the 32 GiB pod limit (after the Phase-2a OOM hotfix).
- **`shelf_disk_bytes_used` never left zero on any pod.**

NVMe was provisioned (240 GiB PVC, mounted, free), the metric
plumbing was correct (the same series fired in `it_hybrid_pool.rs`
under load), and the Foyer disk engine was opening cleanly at
boot. The hybrid tier was wired but unused.

### Root cause

The hybrid pool was built with **`S3FifoConfig::default()`**.

S3-FIFO splits memory into a small probationary queue and a main
queue. New entries land in the small queue; only entries that are
**re-accessed** while still in the small queue get promoted to the
main queue. **Eviction-to-disk only happens from the main queue.**

The Metabase admin workload that drives ~99% of rep-2 traffic
issues *one-shot* byte-range GETs: each Parquet row-group is read
by exactly one query and then never touched again before it ages
out. The default S3-FIFO ratios (`small_queue_capacity_ratio = 0.1`,
`small_to_main_freq_threshold = 2`) mean an entry has to be read at
least three times in a 10-percent slice of DRAM before it earns
promotion. None of our reads do.

So every byte-range was admitted to the small queue, expired from
the small queue without re-access, and was discarded **without
ever entering the main queue and therefore without ever being
written to NVMe**. The disk tier behaved as if it were not there.

The exact promotion gate lives in
`foyer-memory/src/eviction/s3fifo.rs:on_access` (foyer 0.12.2).

### Fix (SHELF-E1b)

Default the hybrid row-group pool to **LRU** (`LruConfig::default()`).
LRU has no probationary queue: every memory eviction is a
candidate for the disk ring, so even one-shot reads populate NVMe.

The trade-off is reduced scan resistance under bursty `INSERT INTO`
rewrites; we accept that for v0.5 because:

1. The current dominant workload (Metabase admin + ad-hoc BI) is
   one-shot, not bursty rewrites.
2. The cost of staying on S3-FIFO is "NVMe is permanently empty",
   i.e. the hybrid tier is a 240 GiB unused PVC.
3. Operators retain the choice — `evictionPolicy: s3_fifo` in
   values.yaml restores the prior behaviour without a code change.

S3-FIFO can be revisited per workload once the SHELF-26 replay
harness produces per-policy hit-ratio numbers on rep-2's 7-day
trace.

### Code surface

- `RowGroupPoolConfig::eviction_policy: EvictionPolicy`
  (`shelfd/src/config.rs`). Defaults to `Lru`. Backwards-compatible
  with existing values files because of `#[serde(default)]`.
- `EvictionPolicy::{Lru, S3Fifo, Lfu, Fifo}` enum.
- `build_hybrid_rowgroup` dispatches on the variant
  (`shelfd/src/store.rs`).
- Helm: `cache.pools.rowgroup.evictionPolicy` in
  `charts/shelf/values.yaml`, surfaced into the ConfigMap.
- Production: `evictionPolicy: lru` in `charts/shelf/values-prod.yaml`
  and the canonical
  `deployments-repo/.../shelf/values.yaml`.

### Coverage

- `config::tests::rowgroup_eviction_policy_defaults_to_lru`
- `config::tests::rowgroup_eviction_policy_accepts_all_known_variants`
- `config::tests::rowgroup_eviction_policy_rejects_unknown_variant`
- `it_hybrid_pool::hybrid_pool_opens_under_every_eviction_policy`
- `it_hybrid_pool::lru_one_shot_inserts_populate_nvme` —
  directional regression that fails on the old S3-FIFO default.

### What this does **not** change

- ADR-0009's reasoning still applies under bursty rewrite-heavy
  workloads. We have not deleted the ADR, only flipped the default
  and made the choice explicit and configurable.
- The metadata-pool SIEVE → S3-FIFO question (above) is unchanged
  and still gated on the SHELF-26 replay sweep.
