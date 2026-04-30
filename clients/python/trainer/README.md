# shelf-trainer

Nightly trainer for the Shelf cache. Emits `pin_list.json` (v1 default
admission signal per ADR-0003) and is **ready** to train a LightGBM
admission model in v1.x — **not ONNX**, per ADR-0003.

This package is a skeleton. Only one piece of real logic ships today:
`SizeThresholdAdmission` (the v1 admission policy). Everything else
raises `NotImplementedError("SHELF-NN: …")` so ops can trace the
owning ticket.

## Prerequisites

- Python ≥ 3.11
- [`uv`](https://docs.astral.sh/uv/) for dependency management
- AWS credentials with read on `your_query_log_table` and
  read/write on the shelf config bucket (see `src/shelf_trainer/config.py`)

## Install

```bash
cd shelf/clients/python/trainer
uv sync --all-extras
```

This installs runtime and dev dependencies in an isolated `.venv`.

## CLI

Four subcommands. All are stubs in this skeleton.

```bash
uv run shelf-trainer --help
uv run shelf-trainer version

# Will raise NotImplementedError('SHELF-48: …') today:
uv run shelf-trainer pin-list --dry-run

# Will raise NotImplementedError('SHELF-49: …') today:
uv run shelf-trainer train-admission --dry-run

# Will raise NotImplementedError('SHELF-50: …') today:
uv run shelf-trainer promote v42

# Will raise NotImplementedError('SHELF-51: …') today:
uv run shelf-trainer rollback v41
```

## Run the tests

```bash
uv run pytest
```

Only the `SizeThresholdAdmission` tests run real assertions. Every other
test is marked `@pytest.mark.skip(reason="TODO SHELF-NN: …")` so you can
see the skeleton shape in one command.

## Lint and format

```bash
uv run ruff check .
uv run ruff format --check .
```

Both should be clean on the skeleton.

## Type-check

```bash
uv run mypy src/shelf_trainer
```

`mypy` is configured `strict = true`. The stubs use `raise
NotImplementedError` rather than returning fake values, so the strict
signatures are preserved.

## Package layout

```
trainer/
├── pyproject.toml                 # uv-compatible; Python >=3.11
├── README.md                      # (this file)
├── docs/
│   ├── README.md
│   ├── labels.md                  # MATERIAL: label, split, leakage, metrics
│   └── runbook.md                 # stub; filled in as alerts ship
├── src/shelf_trainer/
│   ├── __init__.py
│   ├── cli.py                     # typer entrypoints (stubs)
│   ├── config.py                  # pydantic settings
│   ├── features.py                # 10-feature extractor (stub; order fixed)
│   ├── labels.py                  # time-split, leakage control (stub)
│   ├── pin_list.py                # v1 pin-list generator (stub)
│   ├── evaluation.py              # AUC-PR + calibration + coverage (stub)
│   ├── drift.py                   # PSI monitor (stub; v1.x)
│   ├── promotion.py               # candidate -> prod guardrails (stub)
│   └── models/
│       ├── __init__.py
│       ├── size_threshold.py      # IMPLEMENTED (v1 default)
│       └── lightgbm_admission.py  # stub (v1.x per ADR-0003; NOT ONNX)
└── tests/
    ├── test_size_threshold.py     # real
    └── test_<module>.py           # stubs marked @pytest.mark.skip
```

## Why no ONNX?

See `shelf/agents/out/adr/0003-size-threshold-admission-over-onnx-mlp.md`.

TL;DR: the v0.3 blueprint's 3-layer MLP → ONNX path was rejected for
being speculative (unmeasured latency claim, speculative hit-rate gain,
Python + ORT dependency in the hot data plane). v1 ships
size-threshold + pin-list. A LightGBM model via the Rust `lightgbm3`
binding is the **only** v1.x learned-admission candidate, and only if
Phase 4's 30-day replay shows ≥ 5 pp hit-rate lift at < 50 µs p99.

## Airflow DAG

`shelf/infra/dag/shelf-trainer-dag.py` is a three-task DAG
(`extract → train → promote`) that invokes this CLI once daily. All
tasks are stubs; see comments in the DAG file for the wiring pattern.
