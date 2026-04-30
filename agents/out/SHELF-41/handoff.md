# SHELF-41 — zstd dictionary metadata pool — deferral memo

- **Ticket**: SHELF-41 — *"Block-level zstd dictionary compression for the metadata pool"* (P3 lever 19 in `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md`).
- **Status**: **DEFERRED — code lever explicitly out of scope this cycle.**
- **Branch**: `shelf-40-41-deferral-memos`
- **PR**: opened on push.

## Why deferred

Plan §"P3 — exploratory / long-tail" lever 19 (line 284 of the plan), verbatim:

> Iceberg `.avro` manifests are uncompressed-ish; CacheLib has block compression and reports 3–5× density on small JSON-like blobs. **Lower priority — metadata pool is not the bottleneck today (Trino coord-side `iceberg.metadata-cache.enabled` already absorbs the warm path; see SHELF-34 caveat).**

The deferral is grounded in the structural caveat from Pass 2 of the verification corrections (plan line 416):

> Trino coord-side metadata cache shadows shelf's metadata pool: [Trino PR #22739](https://github.com/trinodb/trino/pull/22739) wired `iceberg.metadata-cache.enabled` (default ON) to a JVM-local `MemoryFileSystemCache`. **This is the structural reason shelf's metadata pool runs at ~0.14 % hit ratio on rep-1 under load.** The actual lift on metadata is on the *page-index sidecar fed back to Trino* (SHELF-34), not on improving cache density of the manifest blobs themselves.

In other words: zstd compression on the metadata pool would let shelf hold 3–5× more manifest blobs, but **Trino's own JVM-local cache already serves the warm path**, so extra metadata-pool density doesn't translate into a measurable hit-ratio lift on the cluster. The metadata pool's value remains for **pod cold-starts and KEDA worker churn**, where the cache is empty and Trino's coord-side cache hasn't yet been populated — and even there, the pod-cold-start volume is small enough that compression density is not the binding factor (the binding factor is the cold-start *rate*, addressed by SHELF-43 prefetch listener).

The companion infrastructure already exists in the codebase:

- `shelfd/src/compression.rs` is wired with a `zstd` workspace dependency.
- `Cargo.toml` `[features] zstd_metadata = []` is the gate flag.
- The flag is **off by default** until "the benchmark in `benchmarks/compression/` shows a positive capacity × latency trade-off on rep-2's 7-day trace" per the `Cargo.toml` comment.

So the only missing piece is the benchmark. Running it speculatively (today, with the metadata pool at 0.14 % hit ratio) would just confirm "compression doesn't help when the upstream isn't bottlenecked by cache density".

## What the eventual lever will look like (preserved for future reference)

When the gate fires, SHELF-41 lands as:

- Train a dictionary against a representative sample of `.avro` manifest blobs (`zstd --train` over `~/scratch/manifest-sample/*.avro`).
- Wire the dictionary into `shelfd/src/store.rs` behind the `zstd_metadata` feature flag — apply on `Pool::Metadata` insert, decompress on get.
- Bench against rep-2's 7-day trace per the existing `Cargo.toml` rule.
- Ship only if (a) p99 metadata-pool hit-path latency regresses < 20 % vs uncompressed and (b) compressed footprint ≥ 3× density.

## When to revisit

Three concrete triggers:

1. **Trino removes / disables `iceberg.metadata-cache.enabled` upstream** — unlikely but would invert the structural caveat.
2. **`shelf_rolling_hit_ratio_bps{pool="metadata"}` rises above ~5 %** — meaning the cluster's traffic mix has shifted such that the metadata pool is doing real work. At that point compression density actually matters.
3. **Cold-pod start volume becomes the dominant cost** — if KEDA scale-out events become frequent enough that pod cold-starts (where shelf's metadata pool *is* the warm path) drive a measurable share of S3 GET cost, compression buys real headroom.

## Open follow-ups

1. **Track `shelf_rolling_hit_ratio_bps{pool="metadata"}` monthly** — if it stays < 1 % for 3 months, retire SHELF-41 outright.
2. **Track Trino PR backlog for any move that disables `iceberg.metadata-cache.enabled`** — this would invert the priority overnight.
3. **No code change this cycle.**

## Rollback signals

N/A — no code shipped this cycle.
