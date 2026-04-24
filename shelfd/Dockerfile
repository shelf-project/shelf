# syntax=docker/dockerfile:1.7
#
# shelfd container image — SHELF-09.
#
# Multi-stage: a Debian-slim Rust toolchain builds a statically-ish
# release binary, which is then copied into a distroless runtime so the
# attack surface is `libc + libgcc_s + CA roots + /shelfd`.
#
# Design rationale lives in
#   shelfd/docs/design-notes/SHELF-09-dockerfile-and-helm-lint.md
#
# Build:
#   docker build -f shelfd/Dockerfile -t shelfd:<tag> .
# (context MUST be the repo root so the workspace Cargo.toml + Cargo.lock
#  + shelfctl/ sibling crate are visible.)

########################################################################
# Stage 1 — builder
########################################################################
FROM rust:1.82-slim-bookworm AS builder

# Build deps for the native crates in the dep tree:
#   - aws-lc-sys  — C/C++ + cmake + perl + Go-free NASM not required on arm-less amd64.
#   - zstd-sys / foyer compression — pulled via libzstd / system cc.
#   - tonic / prost-build — protoc is only needed if .proto files compile
#     in build.rs; shelfd today does not generate protos, so we omit
#     `protobuf-compiler` to keep the layer lean.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      pkg-config \
      cmake \
      perl \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Pin the exact toolchain the repo expects. The base image ships 1.82,
# but rust-toolchain.toml asks for `stable` — letting rustup resolve
# guarantees CI and local builds agree.
COPY rust-toolchain.toml ./

# Workspace manifests + lockfile first so the COPY of sources below can
# invalidate the build cache cleanly when only source changes.
COPY Cargo.toml Cargo.lock ./

# Both workspace members must be present for `cargo build` to resolve
# the workspace graph, even though we only ship the shelfd bin.
COPY shelfd ./shelfd
COPY shelfctl ./shelfctl

# --locked enforces Cargo.lock immutability; strip removes debug symbols
# on top of the `strip = "symbols"` release profile already in
# workspace Cargo.toml (belt-and-braces — cheap, idempotent).
RUN cargo build --release --bin shelfd --locked \
 && strip target/release/shelfd

########################################################################
# Stage 2 — runtime (distroless cc + nonroot)
########################################################################
# `cc` variant (not `static`) ships libgcc_s.so.1 + libc + CA roots,
# which aws-sdk-s3's rustls/aws-lc-rs runtime needs. `nonroot` gives us
# UID/GID 65532 out of the box — matches podSecurityContext in values.yaml.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

# OCI metadata — registry UIs + supply-chain tools rely on these.
LABEL org.opencontainers.image.source="https://github.com/penpencil-oss/shelf" \
      org.opencontainers.image.title="shelfd" \
      org.opencontainers.image.description="Shelf cache daemon — Iceberg-native, row-group-granular read cache for Trino" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.vendor="penpencil-oss" \
      org.opencontainers.image.documentation="https://github.com/penpencil-oss/shelf/blob/main/BLUEPRINT.md"

# Ship only the binary. No shell, no package manager, no curl — which
# also means we cannot set a HEALTHCHECK that runs a command. K8s
# liveness/readiness probes hit /healthz and /readyz directly.
COPY --from=builder /build/target/release/shelfd /shelfd

USER 65532:65532

# 9090 data / 9091 metrics / 9092 s3-shim / 9093 admin — see
# charts/shelf/values.yaml:service.*Port.
EXPOSE 9090 9091 9092 9093

ENTRYPOINT ["/shelfd"]
CMD ["--config", "/etc/shelfd/shelfd.yaml"]
