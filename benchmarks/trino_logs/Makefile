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

.PHONY: venv install fixture test analyze simulate replay-rep2-7d clean help

help:
	@echo "Targets:"
	@echo "  venv              create .venv/ (python -m venv)"
	@echo "  install           pip install -e .[dev] into .venv"
	@echo "  fixture           regenerate the committed synthetic fixture"
	@echo "  test              pytest -q"
	@echo "  analyze           run shelf-replay analyze (TRACE=... MANIFEST_DIR=... OUT=...)"
	@echo "  simulate          run shelf-replay simulate (CONFIGS=... OUT=...)"
	@echo "  replay-rep2-7d    end-to-end + golden verification (SHELF-26 AC)"
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

clean:
	rm -rf $(VENV) results/
