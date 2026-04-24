# SHELF-12 smoke harness Makefile.
#
# Targets:
#   up     - docker compose up -d, then wait-healthy
#   down   - docker compose down -v (removes volumes)
#   seed   - rerun the seed container only
#   smoke  - full cold + warm assertion loop (run-smoke.sh)
#   logs   - tail logs from every service

.PHONY: up down seed smoke logs build

SHELL := /bin/bash

build:
	docker compose build shelfd

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
