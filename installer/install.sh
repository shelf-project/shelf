#!/usr/bin/env bash
# SHELF-33 — curl … | sh installer for Shelf.
#
# Verifies kubectl/helm/shelfctl are on PATH (downloading shelfctl
# from the GitHub releases page if missing), then exec's
# `shelfctl install "$@"` so the user-facing flow lives in Rust.
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/shelf-project/shelf/main/installer/install.sh | sh
# or with explicit args:
#   curl -sSL ... | sh -s -- --namespace shelf --release shelf

set -euo pipefail

SHELF_RELEASE_BASE="${SHELF_RELEASE_BASE:-https://github.com/shelf-project/shelf/releases/latest}"

log() { printf '[shelf-install] %s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

require_or_install_shelfctl() {
  if command -v shelfctl >/dev/null 2>&1; then
    return 0
  fi

  local os arch asset url install_dir target
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  case "$os" in
    darwin|linux) ;;
    *) die "unsupported OS: $os (need darwin or linux)" ;;
  esac

  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64) arch="amd64" ;;
    arm64|aarch64) arch="arm64" ;;
    *) die "unsupported arch: $arch (need amd64 or arm64)" ;;
  esac

  asset="shelfctl-${os}-${arch}.tar.gz"
  # The placeholder release URL is intentional — replaced at release
  # tagging time by CI.
  url="${SHELF_RELEASE_BASE}/download/${asset}"

  install_dir="${HOME}/.local/bin"
  mkdir -p "$install_dir"
  target="${install_dir}/shelfctl"

  log "downloading ${asset} from ${url}"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "${target}.tar.gz"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "${target}.tar.gz" "$url"
  else
    die "neither curl nor wget on PATH; cannot download shelfctl"
  fi

  tar -xzf "${target}.tar.gz" -C "$install_dir"
  rm -f "${target}.tar.gz"
  chmod +x "$target"

  case ":$PATH:" in
    *":${install_dir}:"*) ;;
    *) log "installed shelfctl to ${target} — add ${install_dir} to PATH" ;;
  esac

  export PATH="${install_dir}:${PATH}"
}

main() {
  for tool in kubectl helm; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      die "$tool not found on PATH; install it before running shelf-install"
    fi
  done

  require_or_install_shelfctl

  exec shelfctl install "$@"
}

main "$@"
