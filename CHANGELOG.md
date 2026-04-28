# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- _Pending v0.5.0._

## [0.1.0] — 2026-04-28

Initial public production state. Running on the origin Trino-on-EKS cluster
across two of four replicas; soak-clock for `v0.5.0` begins at full cutover.

### Added
- `shelfd` — Rust cache daemon with two Foyer pools (metadata DRAM-only,
  rowgroup hybrid DRAM + NVMe). Content-addressed keys per ADR-0011
  (`sha256(etag || u64_le(offset) || u64_le(length) || u32_le(rg_ordinal))`).
- S3-compatibility shim on `:9092` accepting GET/HEAD/PUT/DELETE; signature-
  agnostic by design so any S3 client (Trino native, dbt, Iceberg writer)
  drops in via a one-line `s3.endpoint=` flip.
- AWS-chunked PUT decoding (SHELF-25) — strips streaming-signed
  `Content-Encoding: aws-chunked` framing before re-uploading to origin.
- HRW (Highest Random Weight) consistent hashing across pods, with a
  membership resolver that periodically polls the headless service and
  honours a lameduck drain bit on `SIGTERM`.
- LODC submit-queue back-pressure (SHELF-21e) — bounded watermark gate at
  the admission seam, drop-on-full, never blocks reads.
- Helm chart at `charts/shelf` with a ClusterIP `shelf-pool` service +
  `minReadySeconds: 30` + gated `startupProbe` (5-min grace for Foyer
  NVMe recovery on rolling restart).
- Prometheus metrics surface: hits/misses by pool and table, NVMe disk
  fill, eviction reasons, LODC drops, rolling hit ratio, plus a
  reference Grafana dashboard (`shelf-overview`).
- `shelfctl` admin CLI for ring inspection, pin/unpin, drain.
- Built-in web UI on `:9090/ui/` (Vite/React/TS, 5-tab redesign:
  Story / Live / Hot tables / Lab / Admin) — opt-in via the `ui` cargo
  feature so stock builds don't pull npm.

### Documentation
- `BLUEPRINT.md` (architecture), `COMPARISON.md` (vs Alluxio), full ADR
  set under `agents/out/adr/0001-…`, design notes per ticket, rollout
  runbooks under `docs/rollout-v1/`.

[Unreleased]: https://github.com/shelf-project/shelf/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/shelf-project/shelf/releases/tag/v0.1.0
