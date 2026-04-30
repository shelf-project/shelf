# ADR 0003: Size-threshold admission + pin list in v1; no ONNX MLP

_Status: Accepted (planner amendment, 2026-04-23)_
_Deciders: eng-lead, scientist agent §3.3 + §4.1, critic §1.3 + §3_

## Context

The v0.3 blueprint proposes a nightly-trained 3-layer MLP (10
features → P(reaccess < 1h)) exported to ONNX and invoked on every
cold miss > 8 MB. Costs: ONNX Runtime C++ dependency in `shelfd`,
Python model-export pipeline, training-ops surface, a retraining
cadence to tune, and an unsourced "10-50 µs" inference latency claim
(scientist §1 verified this is plausible but unmeasured). Benefit:
10-15% marginal NVMe write-bandwidth reduction on ad-hoc scans vs
size-threshold alone — *speculative*, not measured on our workload.

3L-Cache (FAST '25) and S3-FIFO (SOSP '23) both show that on Zipf-heavy
workloads, simpler admission policies are within 3 pp of the best
learned ones. Our workload (dashboards + long-tail ad-hoc) is Zipfian.

## Decision

Ship **size-threshold + pin-list** admission in v1:

- Refuse admission for objects ≥ 1 GiB unless the key matches a
  pin-list entry.
- Pin list is a nightly-regenerated JSON of `scanned_bytes × wall_time
  × frequency` top-N per tenant from `cdp.trino_logs.trino_queries`,
  reviewed by ops via PR before publication.
- No model in `shelfd`. No ONNX Runtime. No Python at serve time.

Escape hatch (v1.x, conditional): if Phase 4's 30-day replay benchmark
shows a LightGBM model lifts hit rate ≥ 5 pp over size-threshold
*and* adds < 50 µs to the large-miss path, ship LightGBM via the Rust
`lightgbm3` binding — never ONNX.

## Alternatives considered

- **3-layer MLP via ONNX Runtime.** Rejected: C++ dependency;
  unsourced latency claim; speculative gain; retraining pipeline cost.
- **Hand-rolled Rust MLP.** Rejected: still solves a problem we
  haven't proven we have.
- **S3-FIFO admission filter (scientist §4.7).** Augment, not
  replace. Foyer's S3-FIFO policy already acts as a one-hit-wonder
  filter for the NVMe tier; that's free.

## Consequences

- **Positive.** Model-free, Python-free, ONNX-free data plane. An
  admission decision ops can read (`shelfctl stats --pinned`) without
  a Jupyter notebook.
- **Negative.** Lose some marginal NVMe write-bandwidth reduction on
  the tail of ad-hoc scans. Measurable in Phase 4; we may revisit.
- **Neutral.** The ONNX path in the blueprint was never measured;
  deferring it is honest, not cowardly.
