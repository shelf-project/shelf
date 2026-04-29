# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **SHELF-37 — Iceberg event-listener jar** (`clients/trino-listener/`).
  New Trino `EventListener` SPI plugin, factory name
  `shelf-iceberg-listener`, that captures every `QueryCompletedEvent`
  and writes it to a configurable Iceberg table via the official
  `iceberg-core` writer API. Append-only, partitioned by
  `day(create_time)`, fail-open by default (`fail-mode=drop`). Backed
  by a bounded queue + dedicated writer thread; the SPI hook never
  stalls Trino's coordinator threads. Exposes a JMX MBean and an
  optional Prometheus HTTP exporter on port 9099. Includes the
  `shelf.tag.*` session-property → `tags_json` contract that SHELF-40 /
  SHELF-42 build on. See `clients/trino-listener/README.md` for the
  full configuration matrix and schema. The origin-cluster overlay
  lives under `infra/` per the existing OSS-overlay convention and is
  stripped from the publish surface by `release.yml`.

## [1.0.0-rc.2] — 2026-04-29

**Hotfix release for the SHELF-21f LODC submit-queue overflow regression
observed on the penpencil `data-platform-cluster` alluxio NodePool the
night of 2026-04-28 → 2026-04-29.**

### Why a hotfix

The 2026-04-28 helm rev-16 soak proved the SHELF-21e LODC defaults
(`flushers=4`, `bufferPool=256 MiB`, `submitQueue=1 GiB`) were not
sufficient under sustained read load on a 4xlarge alluxio NodePool with
~27.3 GiB node-allocatable. By 09:07 IST on 2026-04-29:

- cluster-wide `shelf_lodc_drops_total` rate had grown 2.5× overnight
  (~2.7 M/h → 6.3 M/h), with shelf-2/3 `shelf_lodc_inflight_bytes`
  pinned at exactly 859 053 141 B (= 80 % watermark of the 1 GiB
  submit-queue threshold) for 6 h continuously — the LODC had fully
  saturated and every admission was being dropped;
- shelf-1 was OOMKilled (exit 137) at 06:40 IST, RSS peak 29.11 GiB;
- shelf-0 RSS peaked at 29.10 GiB earlier and 27.66 GiB at the time of
  the alert, both above the 27.30 GiB node-allocatable ceiling.

### What this changes

1. **`origin.pool.maxConnections` 256 → 128** (chart default and prod
   overlay) — caps worst-case origin in-flight RSS at
   `maxConnections × ~32 MiB ≈ 4 GiB`. The live cluster ConfigMap had
   been hand-applied to 512 during the 2026-04-28 chaos window, which
   raised origin worst-case to ~16 GiB and left zero RSS headroom under
   the 19 GiB DRAM caps + 1 GiB LODC submit queue. The deploy runbook
   for this rc explicitly resets the in-cluster ConfigMap.
2. **`cache.pools.rowgroup.dramSizeBytes` 14 GiB → 11 GiB** — frees
   ~3 GiB of node-allocatable headroom and reduces the rate at which
   the rowgroup pool evicts into the LODC.
3. **`cache.pools.rowgroup.diskCache.flushers` 4 → 8** and
   **`bufferPoolSizeBytes` 256 MiB → 384 MiB** — approximately doubles
   the gp3 drain parallelism so the LODC submit queue actually drains
   in steady state and `shelf_lodc_inflight_bytes` falls below the
   80 % watermark. Without this, the SHELF-21e back-pressure was
   correctly dropping admissions but the LODC was permanently saturated.
4. **`shelfd::config::default_max_inflight()` 256 → 128** — defensive
   matching default in the Rust struct so dev / CLI invocations that
   skip the chart inherit the same bound. Unit-test updated.

### Why these are not separate options

- **`RateLimitPicker` is not coming back** — the 2026-04-28 chaos window
  proved it pegs `hit_disk` p99 at the histogram-max bucket because it
  shares a queue with reads (see `lodc_backpressure.rs` module doc and
  AGENTS.md preview-9 note). The SHELF-21e level-based gate on shelfd's
  own admission seam stays.
- **`shelf-2`/`shelf-3` are not changed** beyond the uniform values
  bump. Their low rowgroup hit ratio is HRW-by-design (key family
  concentration on shelf-0/1) and is not the subject of this fix.

### Phase-A RCA verdict

H3 (RSS budget exhaustion) is the primary cause; H2 (LODC flusher
drain rate) is the secondary cause that pinned the inflight gauge at
the watermark. H1 (NVMe IOPS) is not the bottleneck — gp3 baseline
3 000 IOPS / 125 MiB/s is well under the observed sustained write
rate envelope (`node_disk_writes_completed_total` ≪ provisioned cap).
Full RCA + evidence in `shelfd/docs/runbooks/2026-04-shelf-1-oom.md`
(updated for this incident).

### RSS budget arithmetic, post-fix, on 4xlarge alluxio NodePool

```
   5  GiB  metadata DRAM
+ 11  GiB  rowgroup DRAM        (was 14)
+  4  GiB  origin in-flight     (= 128 × 32 MiB; was 16 GiB at 512)
+  1  GiB  LODC submit queue
+  3  GiB  Rust runtime + tokio + jemalloc fragmentation
= 24  GiB  worst-case RSS
———————
 27.3 GiB  node-allocatable ceiling
=  3.3 GiB  headroom
```

Previous budget left zero headroom under the same ceiling.

## [1.0.0-rc.1] — 2026-04-28

Released from the canonical home `github.com/shelf-project/shelf`. Re-cut
of `1.0.0-rc.0` after release-pipeline first-run bugs:

- `build-image` job timed out at 45 min on QEMU-emulated linux/arm64 Rust
  release build. Bumped to 90 min; GHA layer cache from the rc.0 attempt
  primes rc.1.
- `helm-publish` job's cosign sign step failed with `UNAUTHORIZED` because
  it relied on `helm registry login` only — cosign uses its own auth.
  Added `docker/login-action` before the cosign step.
- CI plumbing stabilized for org migration (gitleaks `pull-requests: read`,
  helm-template/`kubectl` server-API decoupling via Python YAML parser,
  `cargo-audit` advisory-DB workaround for the malformed
  `RUSTSEC-2026-0073.md`, `cargo-deny` advisories ignored under SHELF-30,
  `aquasecurity/trivy-action` rolled to `v0.36.0`, IAM-wildcard grep
  excludes its own self-documenting workflow file).

No runtime code changes vs `1.0.0-rc.0`. Same runtime evidence applies.

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
