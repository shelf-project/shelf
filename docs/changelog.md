# Changelog

All notable user-facing changes to Shelf are captured here. This file
is hand-edited (no changelog-release automation yet — SHELF-01a will
flip this to `release-please` once we cut our first tagged release).

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
semver: `MAJOR.MINOR.PATCH`. Pre-v1.0 **any** minor bump can contain
breaking changes; we will call them out here explicitly.

## v0.5 — 4-replica Trino Iceberg rollout

### Added

- Compressed-canary rollout to all four example Trino replicas
(rep-0/1/2/3) for the Iceberg catalog via `s3.endpoint` swap. See
`docs/rollout-v1.md` and `docs/rollout-v1/`.
- Per-replica `replica` label on all `shelf_*` metrics via
`X-Shelf-Client-Replica` header (SHELF-27a). Design note in
`shelfd/docs/design-notes/SHELF-27a-per-replica-label.md`. Grafana
dashboard + alerts now group by `replica`.
- Hourly correctness-diff harness comparing Shelf-backed vs S3-direct
Iceberg catalogs on five canonical queries, with Kubernetes CronJob
and PrometheusRule. Lives at `benchmarks/correctness-diff/`.
- `shelf-replay prewarm` subcommand for online per-replica cache
seeding from 7-day `QueryCompletedEvent` traces, wrapped by
`make prewarm REPLICA=... TRACE=...` in `benchmarks/trino_logs/`.
- Capacity plan §4 — "v1 cluster target (4-replica rollout)" —
  worked example at N=5 pods.

### Closed

- SHELF-13 / SHELF-14 / SHELF-18 (cluster half) / SHELF-20 /
SHELF-21 / SHELF-27 / SHELF-28 — all closed by rollout-v1 and
14-day post-rollout soak. Cluster-gated chapter in
`docs/cluster-handoff.md` is retired.

## Unreleased

### Added

- S3-compatible `GetObject` / `HeadObject` shim on `:9092`
(SHELF-22). Protocol subset in
`shelfd/docs/design-notes/SHELF-22-s3-compat-shim.md`.
- Trino read-path wiring via `s3.endpoint` swap. See
[ADR 0012](../agents/out/adr/0012-trino-read-path-endpoint-swap-then-blob-cache-spi.md)
for the three-phase integration strategy.
- Docker-compose smoke harness that pulls MinIO + iceberg-rest +
Trino 480 + `shelfd` and asserts cache-hit climb on warm runs
(SHELF-12). CI runs it on every PR.
- NVMe hybrid pool for row-group bytes (SHELF-18 local-phase work).
- Offline replay analysis harness (SHELF-26) so simulator policy
work (SIEVE vs FrozenHot — SHELF-17a prep) can run without a live
cluster.

### Decided, not yet implemented

- `trino-blob-cache-shelf` plugin (SHELF-29) against the unified
blob-cache SPI proposed in
`[trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184)`.
Phase 2 of ADR 0012. Design note committed against the SPI
signatures read from branch `user/serafin/unified-caching-v2` on
2026-04-24:
`[clients/trino/docs/design-notes/SHELF-29-blob-cache-plugin.md](../clients/trino/docs/design-notes/SHELF-29-blob-cache-plugin.md)`.
Implementation blocked on upstream merge; ~1 sprint estimate, or
~3 days if SHELF-29a (OpenTelemetry plumbing) and SHELF-29b
(`FailOpenFetcher` extraction) land beforehand.

### Closed as "measured, not needed"

- Unix-socket mode on `shelfd:9092` (SHELF-22a). Benchmark put the
TCP-localhost hop at ~331 µs/req keep-alive, Trino's native S3
client has no UDS transport — full analysis in
`shelfd/docs/design-notes/SHELF-22a-unix-socket-mode.md`.

### Ops-gated (tracked by ops, not blocked on code)

- SHELF-13, SHELF-14, SHELF-18 acceptance, SHELF-20 E7,
SHELF-21 rollout, SHELF-28 drills. See
`[cluster-handoff.md](./cluster-handoff.md)` for the handoff packet.

## v0.0.1-pre — 2026-04-24

Pre-release marker. Not a tagged release. Everything above this line
is accumulating against `main` and will be rolled into v0.1.0 once
the v0.5 gate criteria land (ADR 0010: 7 consecutive days of rep-2
passing against the Alluxio baseline).