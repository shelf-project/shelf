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
#
# To include the embedded admin UI at /ui (served from the same port
# as /stats, /metrics, /admin/*), build with:
#   docker build -f shelfd/Dockerfile \
#     --build-arg SHELFD_FEATURES=ui \
#     -t shelfd:ui .
# (context MUST be the repo root so the workspace Cargo.toml + Cargo.lock
#  + shelfctl/ sibling crate are visible.)

########################################################################
# Stage 1a — optional UI bundle (Vite + React + TS)
########################################################################
# We always emit this stage but `shelfd` only copies from it when the
# `ui` cargo feature is active. The committed dist/ placeholder at
# shelfd/ui/dist/index.html keeps `cargo build --features ui` working
# even when this stage has not produced a real bundle.
FROM node:20-alpine AS ui-builder
WORKDIR /ui
RUN corepack enable
COPY shelfd/ui/package.json shelfd/ui/pnpm-lock.yaml* ./
RUN pnpm install --frozen-lockfile 2>/dev/null || pnpm install
COPY shelfd/ui/ ./
RUN pnpm build

########################################################################
# Stage 1 — builder
########################################################################
# We track Debian trixie (slim-trixie) instead of bookworm because the
# pinned 1.82-slim-bookworm image ships a stale `debian-archive-keyring`
# that is missing the trixie cross-signing key Debian rolled out in
# early 2026 — `apt-get update` fails with "At least one invalid
# signature was encountered" on that base. The trixie image carries a
# keyring that validates the modern dual-signed InRelease files.
# rust-toolchain.toml still controls the toolchain version we resolve
# below, so a base bump does not silently change the compiler.
FROM rust:slim-trixie AS builder
ARG SHELFD_FEATURES=""

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

# All workspace members must be present for `cargo build` to resolve
# the workspace graph, even though we only ship the shelfd bin. The
# `shelf-advisor` crate joined the workspace in PR #33 (OSS feature
# rollup) but its COPY was missed at the time, leaving the Dockerfile
# broken on a clean checkout. Restored as part of the SHELF-29 cut.
COPY shelfd ./shelfd
COPY shelfctl ./shelfctl
COPY shelf-advisor ./shelf-advisor

# Overlay the freshly built UI bundle on top of the committed
# placeholder, but only when the `ui` feature is active. When
# SHELFD_FEATURES does not include `ui`, rust-embed is never compiled
# and this layer is a no-op.
COPY --from=ui-builder /ui/dist/ /build/shelfd/ui/dist/

# --locked enforces Cargo.lock immutability; strip removes debug symbols
# on top of the `strip = "symbols"` release profile already in
# workspace Cargo.toml (belt-and-braces — cheap, idempotent).
#
# `--features` is only passed when SHELFD_FEATURES is non-empty so
# the cache-friendly default build stays byte-identical.
RUN if [ -n "$SHELFD_FEATURES" ]; then \
      cargo build --release --bin shelfd --locked --features "$SHELFD_FEATURES"; \
    else \
      cargo build --release --bin shelfd --locked; \
    fi \
 && strip target/release/shelfd

########################################################################
# Stage 2 — runtime (distroless cc + nonroot)
########################################################################
# `cc` variant (not `static`) ships libgcc_s.so.1 + libc + CA roots,
# which aws-sdk-s3's rustls/aws-lc-rs runtime needs. `nonroot` gives us
# UID/GID 65532 out of the box — matches podSecurityContext in values.yaml.
#
# We use cc-debian13 (trixie) so the runtime glibc tracks the builder's
# glibc. Mixing a trixie-built binary with a bookworm runtime fails at
# load time:
#   /shelfd: /lib/.../libc.so.6: version `GLIBC_2.38' not found
# because rustc + native crates emit symbol references against whatever
# glibc the build host shipped.
FROM gcr.io/distroless/cc-debian13:nonroot AS runtime

# OCI metadata — registry UIs + supply-chain tools rely on these.
LABEL org.opencontainers.image.source="https://github.com/shelf-project/shelf" \
      org.opencontainers.image.title="shelfd" \
      org.opencontainers.image.description="Shelf cache daemon — Iceberg-native, row-group-granular read cache for Trino" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.vendor="shelf-project" \
      org.opencontainers.image.documentation="https://github.com/shelf-project/shelf/blob/main/BLUEPRINT.md"

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
