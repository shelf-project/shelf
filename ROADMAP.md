# Roadmap

This roadmap is intentionally narrow. It covers the next ~12 months of
Shelf and is updated by PR whenever scope shifts. Items further out are
written down so contributors know they exist, not as commitments.

Versioning follows [Semantic Versioning 2.0](https://semver.org/). See
`RELEASING.md` for what each release ships and `CHANGELOG.md` for what has
already shipped.

> Note: 1.0.0-rc.* tags are part of the v0.5 maturity line; rc.4 is the current released RC.

## v0.5 → 1.0.0-rc.4 — current line (preview, soak in progress)

The v0.5 line locks down the production path for the read side of the
cache and the LODC (large-object direct-copy) write side. It is the
release candidate for v1.0.

In scope for v0.5:

- **S3 shim.** `shelfd` exposes an S3-compatible read path so unmodified
  S3 clients can hit the cache.
- **HRW membership.** Replicas converge on a Highest-Random-Weight
  hashing ring derived from the configured peer list.
- **Peer fetch.** Cache misses on one replica fan out to the HRW peer
  before falling through to the origin.
- **`aws-chunked` decode.** Streaming decode of `aws-chunked` request
  bodies on the LODC write path.
- **LODC back-pressure.** Bounded in-flight LODC writes per replica with
  explicit 503 surfacing instead of unbounded queueing.
- **ClusterIP service.** First-class Kubernetes ClusterIP service in the
  Helm chart with a documented client wiring story.
- **`startupProbe`.** Distinguishes cold-cache warmup from steady-state
  liveness so slow starts do not cause crash loops.

Exit criteria for v0.5: a successful soak on the origin Trino-on-EKS
cluster across all configured replicas, no P0/P1 regressions for the
soak window, and the v1.0 cut blockers in `CHANGELOG.md` resolved.

## v1.0 — T+30 days post-soak

v1.0 is the *public* commitment. After v1.0, the wire format and the
public API are covered by SemVer.

Planned for v1.0:

- **API freeze.** The S3 shim surface, the LODC write API, and the
  metrics/labels surface are versioned and frozen for the v1.x line.
- **Multi-arch images.** `linux/amd64` and `linux/arm64` for `shelfd`
  published from the same release pipeline.
- **Helm chart via OCI.** Chart distributed as an OCI artefact alongside
  the daemon image.
- **SBOM.** SPDX SBOM published per release artefact.
- **Signed releases.** Cosign keyless signatures over images, charts,
  and `shelfctl` binaries.

The detailed artefact list and release mechanics live in `RELEASING.md`.

## v1.x — incremental hardening

v1.x is where we keep adding production-grade features without breaking
the v1.0 contract.

- **SHELF-23: read-repair.** When a peer-fetch reveals a stale or
  inconsistent local copy, asynchronously repair the local entry so the
  next read hits a fresh copy.
- **SHELF-24: reverse-proxy fallback.** Optional reverse-proxy mode
  where `shelfd` can transparently forward unrecognised paths to the
  origin without requiring client reconfiguration.
- **Benchmarking suite.** First-class, repeatable benchmark harness
  (TPC-DS-shaped + synthetic) with results published per release.

## v2.x — direction, not commitment

The v2.x line is where we expect the larger architectural shifts to
land. None of these are committed; they are written down so contributors
have visibility into where the project is heading.

- **Blob-cache SPI.** A pluggable blob-cache SPI in upstream Trino
  ([trinodb/trino#29184](https://github.com/trinodb/trino/issues/29184)),
  with `shelfd` as a reference implementation behind that SPI.
- **Autoscaling.** Replica autoscaling driven by working-set pressure
  signals rather than CPU/memory alone.
- **Federation.** Cross-cluster cache federation for multi-region and
  multi-tenant deployments.

## How to influence the roadmap

Open an issue with the `roadmap` label or a discussion. Significant
direction shifts go through the ADR flow described in `GOVERNANCE.md`.
