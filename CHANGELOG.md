# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0-rc.0] — 2026-04-28

First release candidate. The 30-day post-`v0.5` calendar soak gate from the
launch playbook is **explicitly waived by BDFL decision**; substituting the
following runtime evidence:

- `shelf-2` cut over to `shelfd` (single-line `s3.endpoint=` flip) on 2026-04-27,
  observed stable on `0.1.0-preview-9` then `0.1.0-preview-10` for ≥24h with
  hit-ratio ≥ 78 % rowgroup, p99 read ≤ 100 ms, zero `ICEBERG_*` regressions
  (vs `Alluxio` baseline 366 → 18 infra failures, -95 %).
- `shelf-1` cut over 2026-04-27, stable on the same image stream.
- 4-replica `shelf-{0..3}` cluster running on the dedicated `alluxio` Karpenter
  NodePool with 56 GiB DRAM + 960 GiB NVMe aggregate cache.
- Critical write-path data-corruption bug (SHELF-25, `Content-Encoding: aws-chunked`
  decode) shipped in `0.1.0-preview-9` and validated against live Iceberg writers.
- LODC submit-queue overflow (SHELF-21e) bounded with drop-on-full back-pressure
  that does not couple write admission to the read path.
- Zero-downtime rolling-update path validated via `shelf-pool` ClusterIP +
  `minReadySeconds=30` + `startupProbe` (5-min Foyer NVMe-recovery grace).

`v1.0.0` final follows after the 7-day RC window unless a regression is found.

### Added
- Tag-driven release pipeline (`.github/workflows/release.yml`) — multi-arch
  container image to GHCR, Helm chart published OCI, `syft` SBOM, SLSA-v1.0
  provenance, `cosign sign --keyless` keyless signatures.
- Penpencil-overlay leak guard in the release workflow + `.gitattributes`
  `export-ignore` for `infra/penpencil/**`, `agents/out/**`, `docs/rollout-v1/**`.
- `docs/brand/` — locked tier-ordered primary mark + favicon.
- OSS hygiene set: `CODE_OF_CONDUCT.md`, `MAINTAINERS.md`, `GOVERNANCE.md`,
  `ROADMAP.md`, `RELEASING.md`, `CHANGELOG.md`, GitHub issue templates,
  `dependabot.yml`, `CODEOWNERS`, DCO check workflow.

### Fixed
- SHELF-25: PUT path now decodes `Content-Encoding: aws-chunked` before
  uploading to origin S3 — fixes Iceberg metadata corruption that surfaced
  as `ICEBERG_INVALID_METADATA` on write-capable replicas.
- SHELF-21e: replaced `RateLimitPicker` (which throttled reads) with a
  bounded LODC submit-queue + drop-on-full back-pressure.

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
