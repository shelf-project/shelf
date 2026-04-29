# SHELF-33 — W-TinyLFU admission gate — handoff

- **Ticket**: SHELF-33 — *"W-TinyLFU admission layer in front of Foyer"* (P1 lever 6 in `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md`).
- **Status**: **DELIVERED — code-only PR, not wired into `main.rs` yet by design.**
- **Branch**: `shelf-33-wtinylfu-admission`
- **Worktree**: `/private/tmp/shelf-33-wtinylfu`
- **PR**: opened on push (URL filled in below).
- **Image build**: NOT done in this PR — orchestrator owns the image build + cluster-side cutover.

## What lands

| File | Change |
|---|---|
| `shelfd/src/admission_wtinylfu.rs` | NEW — 660 LOC, 14 unit tests |
| `shelfd/src/lib.rs` | `pub mod admission_wtinylfu;` |
| `shelfd/src/metrics.rs` | +2 `IntCounterVec` (`shelf_wtinylfu_decisions_total`, `shelf_wtinylfu_decays_total`) + EXPOSED_SERIES additions + regression-test wiring |
| `shelfd/docs/metrics.md` | +2 metric rows |
| `Cargo.toml` (workspace) | `1.0.0-rc.4` → `1.0.0-rc.7` (staggered from SHELF-30 PR #40 rc.5 + SHELF-34 sidecar branch rc.6) |
| `charts/shelf/Chart.yaml` | `1.0.0-rc.4` → `1.0.0-rc.7` |
| `agents/out/adr/0020-w-tinylfu-admission-gate-in-front-of-foyer.md` | NEW |

ADR-0013 (SHELF-30 row-group coalesce, on PR #40), ADR-0014 (SHELF-34 page-index sidecar, on the parallel SHELF-34 branch), and ADR-0015..0019 (gated-ticket ADR sweep on the parallel ADR-sweep branch) are all reserved — this PR uses **0020** to avoid collision.

## Test results (verbatim)

```text
$ cargo test -p shelfd --lib admission_wtinylfu
...
running 14 tests
test admission_wtinylfu::tests::samples_total_increments_monotonically ... ok
test admission_wtinylfu::tests::capacity_hint_zero_does_not_panic ... ok
test admission_wtinylfu::tests::observe_returns_frequency_after_doorkeeper_promotion ... ok
test admission_wtinylfu::tests::sketch_halve_makes_progress ... ok
test admission_wtinylfu::tests::pinned_bypasses_frequency_gate ... ok
test admission_wtinylfu::tests::sketch_estimate_saturates_at_max ... ok
test admission_wtinylfu::tests::rare_item_is_rejected_before_threshold ... ok
test admission_wtinylfu::tests::doorkeeper_clears_on_window_roll_over ... ok
test admission_wtinylfu::tests::concurrent_observations_do_not_corrupt ... ok
test admission_wtinylfu::tests::frequent_item_admits_after_threshold ... ok
test admission_wtinylfu::tests::standalone_composition_ignores_inner ... ok
test admission_wtinylfu::tests::distinct_keys_do_not_collide_grossly ... ok
test admission_wtinylfu::tests::inner_reject_short_circuits_frequency ... ok
test admission_wtinylfu::tests::footprint_bytes_within_budget ... ok
test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 232 filtered out; finished in 0.02s

$ cargo test -p shelfd --lib
...
test result: ok. 246 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.53s

$ cargo clippy -p shelfd --lib --tests -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 3.41s
0 warnings, 0 errors

$ cargo fmt --all -- --check
clean
```

`SHELF_INTEGRATION=1` integration tests are NOT introduced in this PR. The admission gate is unit-testable end-to-end without booting shelfd or MinIO; the existing `pinned_keys_bypass_size_threshold` integration-style test in `shelfd/src/admission.rs` already exercises the wider admission flow. The follow-up MR that wires `WTinyLfuPolicy` into `main.rs` MUST add an `it_wtinylfu_*.rs` integration suite under `SHELF_INTEGRATION=1`.

## Why this is code-only (not wired into `main.rs`)

The PR ships the *implementation* of W-TinyLFU but does NOT swap the construction line in `main.rs` from `SizeThresholdPolicy::from_config(...)` to `WTinyLfuPolicy::new(SizeThresholdPolicy::from_config(...), AndAfter, ...)`. Two reasons:

1. **Rollback simplicity** — when the cluster-side cutover MR is opened, it is a one-line construction swap. Reverting it is a one-line revert. Bundling the policy code with the wiring would make a revert touch many files.
2. **Replay validation precondition** — per ADR-0020 §"Gate to ship", the cutover MR pairs with a SHELF-35 replay TSV showing the hit-ratio delta. SHELF-35 PR #41 ships the harness in pure Python; the operator runs it against the 30-day production trace and the cutover MR cites the resulting TSV. Putting the wiring on the same PR as the implementation forces those two activities to happen in lockstep, which they shouldn't.

## Open follow-ups (orchestrator to assign)

1. **Cluster-side cutover MR** — one-line `main.rs` change wiring `WTinyLfuPolicy` into the admission policy build. Pairs with the replay TSV.
2. **Integration suite** — `it_wtinylfu_*.rs` covering: (a) cold-pod first-N admissions reject as expected, (b) hot-key promotion under sustained load, (c) decay roll-over correctness, (d) pin-list interaction. Gated on `SHELF_INTEGRATION=1`.
3. **24–48 h rep-1 canary** — once cutover MR merges, smoke-watcher per the standard rollback-signal table (verbatim in ADR-0020).
4. **Cache-hit observation hook** (optional) — if SHELF-35 replay shows the bursty-new-key failure mode dominating, add an `observe()` method on `AdmissionPolicy` and call it from the hit path. Not required for v1 of W-TinyLFU.

## Rollback signals (active during the cluster-side cutover MR's smoke)

| Trigger | Action |
|---|---|
| `shelf_rolling_hit_ratio_bps{pool}` drops > 3 pp vs pre-cutover baseline for > 12 h | revert cutover MR |
| `shelf_evictions_total{pool, reason="capacity"}` rate doubles at constant traffic | revert cutover MR |

## What this does NOT touch

- `shelfd/src/admission.rs` — `SizeThresholdPolicy` is unchanged; W-TinyLFU sits in front of it via composition, not as a replacement.
- `shelfd/src/store.rs` — `FoyerStore::get_or_fetch` continues to consume `AdmissionPolicy` as a trait object; the existing call signature is preserved.
- `shelfd/src/admission_limiter.rs` (SHELF-29) — orthogonal back-pressure; W-TinyLFU is the *frequency* gate, SHELF-29 is the *rate* gate, both stack additively in the admission decision.
- `clients/trino/` — no Java changes.
- Penpencil-overlay files (`infra/penpencil/**`) — no changes.

## Concurrency-rule compliance

Plan §"Concurrency rules" line 354: *"At most ONE hot-path Rust builder running at a time."* This PR was developed **after** the SHELF-30 hot-path Rust builder finished its commit + push (PR #40), so the "running concurrently" condition was not violated. The SHELF-34 sidecar builder runs in parallel under the explicit "Sidecar builders run in parallel" exemption (line 355). The ADR-sweep agent runs in parallel under "Memory-updater" / docs-only role.
