# shelf/benchmarks/correctness-diff/Makefile
#
# `make run CONFIG=config.prod.yaml` is the one invocation the
# docs/rollout-v1.md §5 cron wires up. See README.md for deployment
# patterns (container image, k8s CronJob, Trino gateway routing).

PYTHON ?= python3
VENV   ?= .venv
PIP    := $(VENV)/bin/pip
PY     := $(VENV)/bin/python
DIFF   := $(VENV)/bin/shelf-correctness-diff

CONFIG ?= config.example.yaml

.PHONY: venv install test run clean help

help:
	@echo "Targets:"
	@echo "  venv     create .venv/ (python -m venv)"
	@echo "  install  pip install -e .[dev] into .venv"
	@echo "  test     pytest -q"
	@echo "  run      run the harness: shelf-correctness-diff --config <CONFIG>"
	@echo "  clean    remove .venv/ and results/"

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip
	$(PIP) install --quiet -e '.[dev]'

venv: $(VENV)/bin/activate

install: venv

test: venv
	$(VENV)/bin/pytest -q

run: venv
	$(DIFF) --config $(CONFIG)

clean:
	rm -rf $(VENV) results/
