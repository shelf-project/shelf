# Tier 5 — LightGBM admission escape-hatch

**Status:** Not started. Gate-locked behind SHELF-26 replay evidence.
**Parent ADR:** [`agents/out/adr/0003-size-threshold-admission-over-onnx-mlp.md`](../../agents/out/adr/0003-size-threshold-admission-over-onnx-mlp.md)
**Related ticket:** SHELF-4x (admission evolution)

## Why this is a gate, not a plan

ADR-0003 rejected ONNX/MLP admission in v1 because the marginal win
is speculative (10–15 pp on simulated workloads, unmeasured on ours)
while the cost is concrete (a C++ runtime dep, a training-ops
surface, a retraining cadence).

ADR-0003 did **not** reject learned admission outright. It left a
door open to *Rust-native LightGBM* as a future upgrade path,
conditioned on empirical evidence.

This note is the contract for walking through that door.

## Trigger

LightGBM admission ships **only** if, after Tracks A–E and F2 land,
the SHELF-26 replay harness run against the last 7 days of
`cdp.trino_logs` shows:

1. ≥ **5 pp** absolute lift in combined hit rate (Pool::Metadata +
   Pool::RowGroup) over the current size-threshold + pin-list
   policy, AND
2. ≤ **2×** the DRAM working-set footprint of size-threshold at the
   same hit rate (so we're actually saving bytes, not just moving
   cost to pool pressure), AND
3. ≤ **100 µs** p99 per-decision inference latency on the `c6a.4xlarge`
   shelfd pod (measured, not estimated).

Anything short of all three: ship size-threshold + pin-list. Do not
attempt partial wins.

## If triggered — implementation sketch

- Rust-native inference via `lightgbm-rs` (MIT, no C++ runtime beyond
  a static archive built into the container) or `smartcore` (pure
  Rust, slightly slower but zero linkage issues).
- Feature vector (see ADR-0003 appendix):
  - `log2(size_bytes)`
  - `same_pool_inflight_count`
  - `pin_list_contains_table (bool)`
  - `seconds_since_last_snapshot_commit`
  - `prior_pool_pressure_ratio`
  - `log2(bytes_scanned_by_fingerprint_last_24h)`
- Output: P(reaccess < 1h) ∈ [0, 1]. Admit iff > threshold (start
  threshold at 0.4; auto-tune against the live trace replay).
- Pin-list **always** wins: pinned keys bypass the model. This
  preserves the ops-reviewed pin contract.
- Single-flight still wraps the whole path. The model runs once per
  miss, not once per waiter.

## Training

- Training data: last 28 days of `trino_logs` + shelfd `/metrics`
  scrapes, joined on `(etag, offset, length)`.
- Labels: did the byte range show up on a subsequent split within
  60 minutes?
- Training cadence: weekly, run as a dedicated Airflow task; artefact
  lands in `s3://penpencil-cdp-temp/shelf/models/<yyyymmdd>.lgb`.
- Rollout: the next shelfd pod restart picks up the newest model
  from its ConfigMap-mounted `MODEL_PATH`. No live reload in v1.
- Rollback: drop the ConfigMap → shelfd falls back to size-threshold
  on boot.

## Metrics (will add on ship)

- `shelf_admissions_total{pool, decision}` already exists (E8). Add
  a `model_score_bucket` label (5 buckets: 0-.2, .2-.4, .4-.6, .6-.8,
  .8-1).
- `shelf_model_inference_seconds` histogram.
- `shelf_model_version{version}` info metric.

## What this note explicitly does not do

- Ship any model code.
- Add any runtime dependency.
- Change any admission decision today.

Those happen only after the SHELF-26 replay harness delivers the
numbers above. Until then, the current admission policy stands and
this note is the place the next engineer starts if the evidence
lands.
