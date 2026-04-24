# Top-level Makefile for the Shelf repo — delegates to per-harness
# Makefiles under benchmarks/.
SMOKE_DIR      := benchmarks/smoke
TRINO_LOGS_DIR := benchmarks/trino_logs

.PHONY: smoke smoke-up smoke-down smoke-logs \
        replay-rep2-7d replay-test

smoke:
	$(MAKE) -C $(SMOKE_DIR) smoke
smoke-up:
	$(MAKE) -C $(SMOKE_DIR) up
smoke-down:
	$(MAKE) -C $(SMOKE_DIR) down
smoke-logs:
	$(MAKE) -C $(SMOKE_DIR) logs

# SHELF-26 — offline `trino_logs` replay analysis harness.
replay-rep2-7d:
	$(MAKE) -C $(TRINO_LOGS_DIR) replay-rep2-7d

replay-test:
	$(MAKE) -C $(TRINO_LOGS_DIR) test
