# Handoff: SHELF-31 / 32 / 36 / 37 / 38 ADR sweep

**Status**: `DELIVERED — five ADRs ready, implementation gated as documented`

**Branch**: `shelf-adr-sweep-gated-tickets` (off `origin/main` HEAD `dae78b6`)

**Worktree**: `/private/tmp/shelf-adr-sweep`

## ADR files added

- `agents/out/adr/0015-shelf32-sieve-eviction-on-rowgroup-pool.md`
- `agents/out/adr/0016-shelf31-vegas-aimd-adaptive-concurrency.md`
- `agents/out/adr/0017-shelf37-bounded-load-hrw-top2-fallback.md`
- `agents/out/adr/0018-shelf36-3l-cache-learned-admission-eviction.md`
- `agents/out/adr/0019-shelf38-glommio-iouring-odirect-nvme-writer.md`

ADR-0013 (SHELF-30 row-group coalesce, on PR #40) and ADR-0014
(SHELF-34 page-index sidecar, parallel agent) are reserved — this
sweep numbers from 0015 to leave 0013 + 0014 untouched.

## Per-ticket gate condition (one line each)

| ADR | Ticket | Gate to ship |
|---|---|---|
| 0015 | SHELF-32 Sieve eviction | Dependabot PR [#22](https://github.com/shelf-project/shelf/pull/22) (Foyer 0.12.2 → 0.22.3) **merged to `main`** AND post-merge `cargo test -p shelfd --lib` + `SHELF_INTEGRATION=1 cargo test -p shelfd --tests` green. |
| 0016 | SHELF-31 Vegas / AIMD limiter | **≥ 7 days clean SHELF-29 soak** (no `rate_limit` drop spikes above baseline, zero OOMKills) **AND SHELF-35 replay** showing static path leaving ≥ 5 % bytes unadmitted at peak OR Vegas lifting p99 hit_disk by ≥ 100 ms vs static. |
| 0017 | SHELF-37 bounded-load HRW | **7-day post-SHELF-23 soak shows imbalance re-emerging**: per-pod p99 read latency divergence > 30 % across 4 pods OR per-pod NVMe-bytes divergence > 2× across the pool, sustained ≥ 24 h within the window. |
| 0018 | SHELF-36 3L-Cache learned policy | **SHELF-35 replay tsv shows ≥ 5 pp hit-ratio lift over Sieve+W-TinyLFU** AND ≥ 5 % origin-byte reduction on the same 30-day `cdp.trino_logs.trino_queries` trace. If < 5 pp, freeze and document as "headroom insufficient". |
| 0019 | SHELF-38 glommio io_uring writer | **OOMKill recurrence within SHELF-29's 7-day soak window OR `shelf_lodc_drops_total{reason="rate_limit"}` saturated > 1 k/sec sustained > 30 min on any pod** (SHELF-29 has hit its ceiling). |

## Open follow-ups (per ticket)

| Ticket | Follow-up implementation PR |
|---|---|
| SHELF-32 | Open after PR #22 merge: `feat(shelfd): SHELF-32 Sieve eviction on rowgroup pool` against `shelfd/src/store.rs` + `config.rs` + Helm overlay. ≤ 60 LOC + tests. Ship per ADR-0015. |
| SHELF-31 | Open after gate clears: `feat(shelfd): SHELF-31 Vegas/AIMD adaptive concurrency` against `shelfd/src/admission_limiter.rs` + `config.rs` + `metrics.rs`. ≤ 250 LOC + tests. Ship per ADR-0016. |
| SHELF-37 | Open after gate clears: `feat(shelfd): SHELF-37 top-2 HRW with bounded-load fallback` against `shelfd/src/router.rs` + `peer.rs` + `config.rs` + cross-language fixture sync. ≤ 200 LOC + tests. Ship per ADR-0017. |
| SHELF-36 | Open after SHELF-35 replay tsv lands ≥ 5 pp lift: `feat(shelfd): SHELF-36 3L-Cache learned admission` against `shelfd/src/wlearned.rs` (new) + `admission.rs` + `Cargo.toml` feature. ≤ 1 200 LOC + tests, behind `learned_admission` feature flag. Ship per ADR-0018. |
| SHELF-38 | Open after gate clears: `feat(shelfd): SHELF-38 glommio io_uring O_DIRECT NVMe writer` against `shelfd/src/nvme_writer.rs` + `runtime_bridge.rs` (both new) + `store.rs` + `Cargo.toml` feature. ≤ 1 500 LOC + tests, behind `glommio_nvme_writer` feature flag (Linux-only). Ship per ADR-0019. |

## Validation discipline (applies to every implementation PR)

Per plan §"Validation discipline" (lines 382–391):

- **Belady replay (SHELF-35) baseline + post-change** for every algorithmic lever.
- **24–48 h canary on rep-1** (lower-traffic, fastest-revert).
- **Hit-ratio ≥ 80 % after 12 h warm, p99 read ≤ 100 ms, 5xx ≤ 1 %**.
- **Hourly byte-identity diff harness** on 5 canonical Iceberg queries vs `cdp_direct` for the first 24 h.
- **Lock the cutover window upfront** (start, end, image-tag, no Trino coord restarts during the window).
- **No fabricated benchmark numbers**.
- **Integration-test gate**: `SHELF_INTEGRATION=1 cargo test …`.
- **Public ADR (this sweep) referenced in the implementation PR description**.

## Hard rules followed

- ✅ Five ADRs written, voice-matched to ADR-0011 / ADR-0012 (terse, evidence-backed, citation-heavy).
- ✅ Numbered 0015 → 0019 (0013 SHELF-30 PR #40 + 0014 SHELF-34 parallel-agent reserved).
- ✅ Plan file unchanged.
- ✅ No code, no Cargo / Helm churn — pure docs.
- ✅ Verbatim rollback-signal tables copied from the plan per ticket.
- ✅ Each ADR contains: Context / Decision / Alternatives / Gate-to-ship / Implementation-outline / Validation-discipline / Citations / Risk-register.
- ✅ Worktree confined to `/private/tmp/shelf-adr-sweep` on `shelf-adr-sweep-gated-tickets`.
- ✅ PR will be opened **draft, no auto-merge** (orchestrator decides).
