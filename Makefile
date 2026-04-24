# Top-level Makefile for the Shelf repo — delegates to per-harness
# Makefiles under benchmarks/.
SMOKE_DIR      := benchmarks/smoke
TRINO_LOGS_DIR := benchmarks/trino_logs

.PHONY: smoke smoke-up smoke-down smoke-logs \
        replay-rep2-7d replay-test \
        chaos-keda-rotation chaos-pod-kill \
        chaos-keda-rotation-smoke chaos-pod-kill-smoke

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
