# SHELF-12 smoke harness Makefile.
#
# Targets:
#   up     - docker compose up -d, then wait-healthy
#   down   - docker compose down -v (removes volumes)
#   seed   - rerun the seed container only
#   smoke  - full cold + warm assertion loop (run-smoke.sh)
#   logs   - tail logs from every service

.PHONY: up down seed smoke logs build build-ui up-ui

SHELL := /bin/bash

build:
	docker compose build shelfd

# Build + run with the embedded admin UI baked in. The UI is served
# from the same port as /stats + /admin, so it comes up at
#   http://127.0.0.1:9091/ui
# alongside the normal smoke stack. No other service changes.
build-ui:
	SHELFD_FEATURES=ui docker compose build shelfd

up-ui: build-ui
	docker compose up -d
	./run-smoke.sh wait-healthy
	@echo
	@echo "shelfd UI: http://127.0.0.1:9091/ui"

up: build
	docker compose up -d
	./run-smoke.sh wait-healthy

down:
	docker compose down -v

seed:
	docker compose run --rm seed

smoke:
	./run-smoke.sh

logs:
	docker compose logs --tail=200
