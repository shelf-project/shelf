# Top-level Makefile for the Shelf repo - delegates to per-harness
# Makefiles under benchmarks/.
SMOKE_DIR      := benchmarks/smoke
TRINO_LOGS_DIR := benchmarks/trino_logs
UI_DIR         := shelfd/ui

# Package manager for the embedded UI build. Override on the command
# line (`make ui PKG=npm`) if pnpm is unavailable.
PKG            ?= pnpm

.PHONY: smoke smoke-up smoke-up-ui smoke-down smoke-logs \
        replay-rep2-7d replay-test \
        chaos-keda-rotation chaos-pod-kill \
        chaos-keda-rotation-smoke chaos-pod-kill-smoke \
        ui ui-install ui-dev ui-build ui-clean shelfd-ui

smoke:
	$(MAKE) -C $(SMOKE_DIR) smoke
smoke-up:
	$(MAKE) -C $(SMOKE_DIR) up
smoke-up-ui:
	$(MAKE) -C $(SMOKE_DIR) up-ui
smoke-down:
	$(MAKE) -C $(SMOKE_DIR) down
smoke-logs:
	$(MAKE) -C $(SMOKE_DIR) logs

# SHELF-26 — offline `trino_logs` replay analysis harness.
replay-rep2-7d:
	$(MAKE) -C $(TRINO_LOGS_DIR) replay-rep2-7d

replay-test:
	$(MAKE) -C $(TRINO_LOGS_DIR) test

# SHELF-28 chaos drills. The *-smoke variants run against the
# docker-compose harness and are green-in-CI. The bare targets delegate
# to chaos/*.sh and assume a live 3-pod StatefulSet (ops territory —
# see docs/runbook.md).
chaos-keda-rotation:
	./chaos/pod-kill.sh
chaos-pod-kill:
	./chaos/pod-kill.sh
chaos-keda-rotation-smoke:
	./chaos/smoke-keda-rotation.sh
chaos-pod-kill-smoke:
	./chaos/smoke-pod-kill.sh

# ---- Embedded admin UI (optional) ------------------------------------------
#
# The UI lives under shelfd/ui/ as a Vite + React + TS app. Its build
# output is baked into the shelfd binary by `rust-embed` under the
# `ui` cargo feature. The default shelfd build does NOT include the UI
# and does NOT require node — operators opt in with `make shelfd-ui`.

ui-install:
	$(PKG) --dir $(UI_DIR) install

ui-dev:
	$(PKG) --dir $(UI_DIR) dev

ui-build: ui-install
	$(PKG) --dir $(UI_DIR) build

ui-clean:
	rm -rf $(UI_DIR)/dist $(UI_DIR)/node_modules

# Convenience one-liner: build the SPA, then rebuild shelfd with the
# ui feature so `cargo run -p shelfd --features ui` picks up the new
# bundle. Used by the smoke `--profile ui` variant.
shelfd-ui: ui-build
	cargo build -p shelfd --features ui

# Alias so operators can just type `make ui`.
ui: shelfd-ui
