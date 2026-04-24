# SHELF-09 — Dockerfile + helm-lint CI rail

Ticket scope: a reproducible, size-budgeted container image for
`shelfd` and a PR gate that keeps `charts/shelf/` installable. This
note captures the decisions that are not self-evident from the files.

## Base image — `distroless/cc-debian12:nonroot`

- `static` was rejected. `aws-sdk-s3`'s default TLS stack resolves to
  `aws-lc-rs`, which links `libgcc_s.so.1` at runtime for unwinding.
  `cc` ships it; `static` does not — builds on `static` fail at
  startup with `error while loading shared libraries`.
- `cc` also ships `/etc/ssl/certs/ca-certificates.crt`, required for
  TLS to S3 and (later) the OTLP collector.
- `:nonroot` gives UID/GID `65532:65532` matching
  `charts/shelf/values.yaml:podSecurityContext`.

## No `HEALTHCHECK`

Distroless has no shell and no `curl`; adding a busybox layer only to
answer `HEALTHCHECK` would blow the size budget and undo the security
posture. K8s probes against `/healthz` / `/readyz` (SHELF-02) are the
authoritative liveness signal.

## Size budget

- Target: **≤ 80 MB compressed** (ticket AC).
- CI proxy gate: **≤ 150 MB uncompressed** via
  `docker image inspect --format '{{.Size}}'`. Compressed ratio on the
  Rust-heavy layer is ~0.45–0.55, so 150 MB uncompressed tracks to
  the 80 MB compressed ceiling with headroom.
- Levers if we bust it later: drop the `prometheus` process feature,
  gate `opentelemetry-otlp` behind a cargo feature, or flip release
  profile from `lto = "thin"` to `"fat"`.

## Feature flags

Workspace default feature set: `foyer_pool = off`, `raft = off`,
`flight = off`. ADR-0001 and ADR-0004 keep the latter two as
permanently-off compile-time guards; `foyer_pool` flips on only when
SHELF-18 provisions the NVMe PVC.

## Chart wiring

`charts/shelf/values.yaml:image.repository` already points at
`ghcr.io/penpencil-oss/shelf/shelfd`. This Dockerfile is what we push
to that path. Tag policy stays overlay-driven: default
`image.tag: ""` → `.Chart.AppVersion`; prod overlays pin an explicit
digest (SHELF-21).

## CI rail (`.github/workflows/helm-lint.yml`)

- `helm-lint` — lints `charts/shelf` against
  `ci/lint-values.yaml` (every feature on, IRSA annotations present)
  and each env overlay (non-strict; prod demands extra annotations
  dev/staging omit).
- `helm-template` — renders the chart, pipes through
  `kubectl apply --dry-run=client` to catch schema breakage that
  `helm lint` misses.
- `docker-build` — buildx builds this Dockerfile with
  `push=false,load=true`, gates uncompressed size, writes the byte
  count to the job summary.

Concurrency group per-ref with `cancel-in-progress: true`.

## Out of scope

- Multi-arch buildx — v1 is `linux/amd64` only (rep-2 is x86).
- SBOM + cosign signing + ghcr→ECR promotion — SHELF-21.
