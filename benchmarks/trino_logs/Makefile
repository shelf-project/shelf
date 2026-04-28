# benchmarks/trino_logs/Makefile — SHELF-26 replay analysis harness.
#
# `make test` runs the whole offline pipeline against the committed
# synthetic fixture. `make replay-rep2-7d` is the acceptance target —
# same pipeline plus golden E5 verification.
#
# For real rep-2 inputs, set TRACE + MANIFEST_DIR + OUT and run
# `make analyze` / `make simulate` directly; see README.md.

PYTHON ?= python3
VENV   ?= .venv
PIP    := $(VENV)/bin/pip
PY     := $(VENV)/bin/python
SHELF_REPLAY := $(VENV)/bin/shelf-replay

FIXTURE     := fixtures/synthetic-7d
OUT         ?= results/synthetic-7d
TRACE       ?= $(FIXTURE)/trace.jsonl
MANIFEST_DIR?= $(FIXTURE)/manifests
CONFIGS     ?= $(FIXTURE)/sim-configs.json

# ---- `make prewarm` settings (rollout-v1 pre-req) ----
# REPLICA and TRACE are required (no defaults on purpose — passing
# the wrong trace to the wrong replica warms the wrong shelfd pool
# and wastes 2-4 h of the rollout window). ENDPOINT defaults to the
# in-cluster headless service; override for smoke / local runs.
REPLICA     ?=
ENDPOINT    ?= http://shelfd.shelf.svc.cluster.local:9092
BUCKET      ?=
CONCURRENCY ?= 32
PREWARM_OUT ?= results/prewarm-$(REPLICA).json

.PHONY: venv install fixture test analyze simulate replay-rep2-7d prewarm clean help

help:
	@echo "Targets:"
	@echo "  venv              create .venv/ (python -m venv)"
	@echo "  install           pip install -e .[dev] into .venv"
	@echo "  fixture           regenerate the committed synthetic fixture"
	@echo "  test              pytest -q"
	@echo "  analyze           run shelf-replay analyze (TRACE=... MANIFEST_DIR=... OUT=...)"
	@echo "  simulate          run shelf-replay simulate (CONFIGS=... OUT=...)"
	@echo "  replay-rep2-7d    end-to-end + golden verification (SHELF-26 AC)"
	@echo "  prewarm           online prewarm of shelfd for a replica cutover"
	@echo "                    (REPLICA=rep-2 TRACE=path/to/rep-2-last-7d.jsonl"
	@echo "                    MANIFEST_DIR=... [ENDPOINT=... BUCKET=... CONCURRENCY=32])"
	@echo "  clean             remove .venv/ and local results/"

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip
	$(PIP) install --quiet -e '.[dev]'

venv: $(VENV)/bin/activate

install: venv

fixture: venv
	$(PY) $(FIXTURE)/generate.py

test: venv
	$(VENV)/bin/pytest -q

analyze: venv
	$(SHELF_REPLAY) analyze \
	  --trace "$(TRACE)" \
	  --manifest-dir "$(MANIFEST_DIR)" \
	  --out "$(OUT)"

simulate: venv
	$(SHELF_REPLAY) simulate \
	  --trace "$(TRACE)" \
	  --manifest-dir "$(MANIFEST_DIR)" \
	  --configs "$(CONFIGS)" \
	  --out "$(OUT)"

replay-rep2-7d: venv
	$(SHELF_REPLAY) replay-rep2-7d \
	  --fixture "$(FIXTURE)" \
	  --out "$(OUT)"

# `make prewarm REPLICA=rep-2 TRACE=trino_logs/rep-2-last-7d.jsonl
#                MANIFEST_DIR=.../iceberg-manifests/2026-04`
#
# Exits non-zero if success_ratio < 0.95 so the Makefile target
# gates the rollout: the caller (rollout-v1.md §4) can `make prewarm`
# and proceed only on exit 0.
prewarm: venv
	@test -n "$(REPLICA)" || (echo "ERROR: REPLICA=... required (e.g. REPLICA=rep-2)"; exit 2)
	@test -n "$(TRACE)"   || (echo "ERROR: TRACE=... required (per-replica 7d trace file)"; exit 2)
	@test -n "$(MANIFEST_DIR)" || (echo "ERROR: MANIFEST_DIR=... required"; exit 2)
	@test -n "$(BUCKET)"  || (echo "ERROR: BUCKET=... required (origin bucket the trace addresses)"; exit 2)
	$(SHELF_REPLAY) prewarm \
	  --trace "$(TRACE)" \
	  --manifest-dir "$(MANIFEST_DIR)" \
	  --endpoint "$(ENDPOINT)" \
	  --bucket "$(BUCKET)" \
	  --replica "$(REPLICA)" \
	  --concurrency $(CONCURRENCY) \
	  --out "$(PREWARM_OUT)"

clean:
	rm -rf $(VENV) results/
