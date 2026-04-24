# Top-level Makefile for the Shelf repo — currently only delegates to
# the SHELF-12 smoke harness. Add further targets as needed.
SMOKE_DIR := benchmarks/smoke

.PHONY: smoke smoke-up smoke-down smoke-logs
smoke:
	$(MAKE) -C $(SMOKE_DIR) smoke
smoke-up:
	$(MAKE) -C $(SMOKE_DIR) up
smoke-down:
	$(MAKE) -C $(SMOKE_DIR) down
smoke-logs:
	$(MAKE) -C $(SMOKE_DIR) logs
