# ADR 0019: SHELF-38 glommio io_uring + O_DIRECT NVMe writer

- Ticket: SHELF-38 — io_uring + O_DIRECT NVMe writer (glommio) replacing the Foyer LODC NVMe path on the rowgroup pool
- Status: **Proposed (gated)** — ships only if the OOMKill class returns within SHELF-29's 7-day soak window OR `shelf_lodc_drops_total{reason="rate_limit"}` saturates at > 1 k/sec sustained for > 30 min on any pod (SHELF-29 has hit its ceiling).
- Author: AI agent on behalf of orchestrator
- Date: 2026-04-30 (UTC+5:30)
- Supersedes: none until the gate clears; would supersede the Foyer LODC NVMe writer path for the rowgroup pool only (metadata pool is DRAM-only and unaffected).
- Superseded-by: none

## Context

The 32 GiB pod limit on the `alluxio` Karpenter NodePool is *effectively*
~22–24 GiB because shelfd's RSS competes with the Linux kernel's page
cache for the same NVMe blocks (workspace memory: "shelfd RSS competes
with the kernel's page cache for the same NVMe blocks, which is the
root reason the 32 GiB pod limit is *effectively* 22 GiB"). The OOMKill
class observed pre-SHELF-29 was the symptom; SHELF-21e (level-based
gate) and SHELF-29 (static-rate token bucket, shipped 2026-04-29) are
band-aids on the symptom, not the cause.

The cause is kernel page-cache double-buffering on Foyer's LODC NVMe
writer. Bypassing it requires `O_DIRECT` + registered buffers, which
neither tokio core nor `tokio-uring` support natively (verified rejection
in plan §Verification corrections lines 477–478: "tokio-uring is
unstable as of late 2025, uses a global `io_uring` instance behind a
`Mutex` that bottlenecks high-throughput workloads, and Tokio core has
no native `O_DIRECT` support").

[glommio](https://github.com/DataDog/glommio) (DataDog, Linux-only,
thread-per-core, mature `io_uring` + registered-buffer support) is
the only production-quality Rust runtime that supports both `O_DIRECT`
and `io_uring` natively. Reference benchmark:
[steelcake's Zig+io_uring NVMe](https://steelcake.com/blog/nvme-zig/)
hit 7.33 GB/s read with `O_DIRECT` + `SQ_THREAD_POLL` + registered
buffers — the upper bound this lever targets.

Plan §"P2-conditional" (lines 273–286) explicitly demoted SHELF-38
from P2-default because **SHELF-29 just eliminated the OOMKill class
within 90 min of smoke**; the "RSS ceiling 22 → 30 GiB" payoff shrinks
if SHELF-29 stays clean, and the async-runtime surgery cost is too
high to do speculatively.

## Decision

When the gate fires, migrate the rowgroup pool's NVMe path off Foyer
LODC to a custom Rust writer using glommio. **Critical async-runtime
constraint: glommio's executor is NOT tokio-compatible.** The decision
is a **glommio thread-pool partition for the rowgroup pool only —
NOT a full tokio-to-glommio rewrite**. The metadata pool, S3 shim,
peer-fetch race, and admin plane all stay on tokio. A bounded
cross-runtime channel (`tokio::sync::mpsc` on the tokio side, glommio
local channel on the glommio side, with a dedicated bridge thread)
marshals write commands across the runtime boundary.

The rowgroup pool's *read* path stays on Foyer's hybrid cache (DRAM
hits never touch the glommio side). Only the **write-through to
NVMe** is partitioned to glommio with `O_DIRECT` + registered buffers.

## Why this and not the alternatives

| Option | Pro | Con | Why this / not |
|---|---|---|---|
| **glommio thread-pool partition (chosen, gated)** | Linux-only is fine for EKS; mature `io_uring` + `O_DIRECT` + registered buffers; published 7.33 GB/s reference benchmark; bypasses kernel page-cache double-buffering | glommio executor not tokio-compatible — partition design adds cross-runtime channel marshalling complexity; highest-risk lever in the P2 set | Only viable path if SHELF-29 hits its ceiling. The partition (not full rewrite) bounds blast radius. |
| `tokio-uring` | Tokio-compatible; would be a smaller surgery | Global `io_uring` instance behind `Mutex` that bottlenecks high-throughput; no native `O_DIRECT`; unstable as of late 2025 | Rejected — directly contradicts SHELF-38's reason for existing. |
| `monoio` (ByteDance) | `io_uring`-native; thread-per-core like glommio | Smaller community, less production track record than glommio at DataDog scale | Not preferred; documented as a fallback if glommio's API churns. |
| Keep Foyer LODC + tune harder | Zero new code; SHELF-29 + SHELF-21e already in production | Tuning surface is exhausted; the structural cause (kernel double-buffering) is unaddressable in user space without `O_DIRECT` | Acceptable status quo *until* the gate fires. This is the "do nothing" path the gate explicitly preserves. |
| Full tokio-to-glommio rewrite | Single runtime; no cross-runtime channels | Astronomical surgery cost; metadata pool, S3 shim, peer race, admin plane all need rewriting; no incremental rollback | Rejected — workspace-memory rule against thrash dominates. |

## Gate to ship

Either signal alone clears the gate (disjunctive):

- **OOMKill class recurrence within SHELF-29's 7-day soak window**:
  any `kube_pod_container_status_terminated_reason{reason="OOMKilled",
  namespace="alluxio", pod=~"shelf-.*"}` event within 7 days of
  SHELF-29 ship date (2026-04-29 ~21:41 IST). One OOMKill is enough.
- **SHELF-29 has hit its ceiling**:
  `rate(shelf_lodc_drops_total{reason="rate_limit"}[5m])` saturates
  at > 1 000 drops/sec sustained for > 30 min on any single shelf
  pod. The static-rate band-aid has reached the point where it is
  *forcing* the work to drop rather than queuing it.

If neither signal fires within 7 days, freeze the lever — the
async-runtime surgery cost is too high to do speculatively (workspace
memory rule). Re-evaluate at the next major Foyer version bump in
case the upstream LODC writer adopts `O_DIRECT` natively.

## Implementation outline

Files modified (target ≤ 1 500 LOC, isolated to a new module + bridge):

- `shelfd/src/nvme_writer.rs` — new module. Rust impl of an
  `O_DIRECT`-opening, registered-buffer, glommio-executor-pinned
  NVMe write path for the rowgroup pool. Exposes `submit(key,
  bytes) -> oneshot<Result<(), NvmeError>>` over a tokio mpsc.
- `shelfd/src/runtime_bridge.rs` — new module. Owns the dedicated
  bridge thread that pumps `tokio::sync::mpsc` ↔ glommio local
  channel; provides backpressure semantics via a bounded queue
  whose depth is `shelf_nvme_bridge_queue_depth` (gauge).
- `shelfd/src/store.rs` — under a Cargo feature flag
  `glommio_nvme_writer`, route the rowgroup pool's write path
  through `nvme_writer::submit` instead of Foyer LODC.
  `--no-default-features` falls back to Foyer LODC.
- `shelfd/Cargo.toml` — add `glommio_nvme_writer` feature gating
  the new modules + the `glommio` dep.
- `shelfd/src/metrics.rs` — add `shelfd_iouring_completion_errors_total`,
  `shelfd_iouring_completion_seconds` (histogram),
  `shelf_nvme_bridge_queue_depth` (gauge), `shelf_nvme_writer_mode`
  (info-style gauge with `mode={foyer_lodc, glommio}` label).
- Build constraint: `--features glommio_nvme_writer` is Linux-only;
  CI must verify the default build (without the feature) still
  builds on macOS dev laptops.

Rollback signals (verbatim from plan lines 282–286):

| Trigger | Action |
|---|---|
| Any OOMKill on any shelf pod | revert to Foyer LODC |
| `shelfd_iouring_completion_errors_total` rate > 0 sustained | revert |
| p99 NVMe write latency regresses > 50 % vs Foyer LODC baseline | revert |

Revert is a build-time path: redeploy the no-feature image (kept on
GHCR alongside the glommio image as `shelfd:<version>-foyer-lodc`).
The cluster-side flip is a single `kubectl set image` against the
in-cluster StatefulSet manifest (deploy-source-of-truth convention).

## Validation discipline

- **SHELF-35 replay**: not directly applicable (replay measures
  algorithm hit ratio, not write-path throughput); instead, run a
  dedicated `tools/replay/nvme-write-bench.rs` driver that replays
  the same 30-day write stream against Foyer LODC and the glommio
  writer, frozen at `agents/out/SHELF-38-nvme-writer-bench-<date>.tsv`.
- **24–48 h canary on rep-1**: hit-ratio ≥ 80 % after 12 h warm, p99
  read ≤ 100 ms, 5xx ≤ 1 %, **plus** zero
  `shelfd_iouring_completion_errors_total` events over the window.
- **Hourly byte-identity diff harness** on 5 canonical Iceberg queries
  vs `cdp_direct` for the first 24 h. Critically important here —
  a custom `O_DIRECT` writer is the highest blast-radius lever in
  the plan; a single byte-corruption bug returns silent cache
  poison.
- **Integration-test gate**: `SHELF_INTEGRATION=1 cargo test -p
  shelfd --features glommio_nvme_writer --tests` plus a fresh
  `tests/it_nvme_writer.rs` integration suite that boots a real
  `tmpfs`-backed `O_DIRECT` device and round-trips 100 GB.
- **Per-replica soak**: rep-1 first; rep-2 only after rep-1 stays
  green for 14 days (twice the standard 7-day workspace convention,
  because the lever is highest-risk).

## Citations

- [DataDog/glommio](https://github.com/DataDog/glommio) — Linux-only thread-per-core async runtime with mature `io_uring` + registered-buffer support.
- [steelcake's Zig + io_uring NVMe benchmark (2024)](https://steelcake.com/blog/nvme-zig/) — 7.33 GB/s read with `O_DIRECT` + `SQ_THREAD_POLL` + registered buffers; the upper bound SHELF-38 targets.
- [tokio-uring repository — design notes on global mutex bottleneck](https://github.com/tokio-rs/tokio-uring) — primary rejection evidence.
- Plan: `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md` lines 273–286 (P2-conditional lever 12), §Verification corrections lines 477–478.
- Workspace memory: "shelfd RSS competes with the kernel's page cache for the same NVMe blocks, which is the root reason the 32 GiB pod limit is *effectively* 22 GiB and the OOMKill class exists" — direct statement of the cause SHELF-38 addresses.
- Existing SHELF-29 ([shelf-project/shelf#39](https://github.com/shelf-project/shelf/pull/39)) — the band-aid SHELF-38 supplants only if its ceiling is hit.

## Risk register

1. **Thread-pool partition is non-trivial.** Cross-runtime channel
   marshalling between tokio (read path, peer race, shim) and
   glommio (NVMe write path) introduces a dedicated bridge thread,
   bounded queue, and a serialized backpressure model. A bug in
   the bridge surfaces as either a deadlock (queue full + readers
   waiting on writers) or silent data loss (write submission
   dropped without surfacing an error). Mitigation: the bridge is
   isolated in `runtime_bridge.rs`; the integration test must
   include a forced-backpressure case where the queue is filled
   and `submit` returns `NvmeError::QueueFull` deterministically.
2. **Linux-only constraint.** glommio does not build on macOS.
   Mitigation: the feature flag is Linux-only; CI must keep the
   no-feature build green on macOS so dev laptops are unaffected.
   This is fine for the EKS production target but constrains the
   shape of `nvme_writer.rs` (no shared types with the macOS path).
3. **Foyer compat with custom NVMe writer.** Foyer's LODC owns the
   on-disk format today; bypassing it for writes but reading
   through Foyer's hybrid cache means the on-disk file format
   becomes a SHELF-owned schema. Mitigation: SHELF-38 must either
   own the on-disk format end-to-end (independent of Foyer) or
   contribute an upstream Foyer PR exposing a writer-trait hook.
   Preferred path is the former (smaller upstream coupling); the
   format is documented in a companion `agents/out/SHELF-38-nvme-on-disk-format.md`
   spec before merge.
4. **Highest-risk lever in the P2 set.** A custom `O_DIRECT` writer
   that miscomputes alignment or sector boundaries returns silent
   data corruption that the byte-identity diff harness will catch
   on first run, but only after some bytes have already been
   served. Mitigation: 14-day rep-1 soak (double the standard
   window) before rep-2 cutover; treat any
   `shelfd_iouring_completion_errors_total` non-zero rate as a
   merge blocker, not a runtime warning.
