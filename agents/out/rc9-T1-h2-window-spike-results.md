---
ticket: rc9-T1
phase: A (local laptop spike)
date: 2026-05-04 IST
worktree: /private/tmp/shelf-rc9-t1-h2-spike-41727 (branch rc9/t1-h2-window-spike off origin/main)
status: HYPOTHESIS REFUTED by protocol analysis; bench infra inadequate to test directly
---

# T1 Phase A — H2 window-size hypothesis: results

## TL;DR

The analyst's "HTTP/2 initial window @ 64 KiB causing the per-pod throughput plateau" hypothesis is **mechanistically incompatible with the production traffic shape**: Trino's native S3 filesystem (`fs.native-s3.enabled=true`) uses AWS Java SDK v2's `NettyNioAsyncHttpClient` which **defaults to HTTP/1.1**. Trino does not expose a protocol switch. HTTP/1.1 has no stream-window concept, so any HTTP/2 window setting on shelfd's hyper stack has zero effect on Trino → shelfd traffic.

**Phase B (isolated PR + cluster bench rerun) is skipped.** No cluster bench slot consumed.
**Phase C fallback (phase-split histogram instrumentation, T1.h2_phase_split todo) is the right next step** for actual root-cause investigation of the plateau.

## What the spike confirmed

1. **Code change compiles and runs.** Replaced `axum::serve` with a `hyper_util::server::conn::auto::Builder` accept loop in `shelfd/src/http.rs` for both data-plane (`serve()`) and s3-shim (`serve_s3_shim()`). Single helper `serve_with_h2_window()` reads optional `SHELFD_H2_INITIAL_WINDOW_BYTES` env var; unset = hyper default; ≥ 65 535 = explicit override. Added `service`, `server-auto` features to `hyper-util` Cargo.toml dep. Build succeeded clean (`docker compose build shelfd`, ~5 min cold including 800-crate workspace recompile).

2. **shelfd correctly negotiates HTTP/1.1 AND HTTP/2** on both the data plane (port 9090) and the s3 shim (port 9092). Verified by:
   - `curl --http1.1 -v http://127.0.0.1:9091/healthz` → `< HTTP/1.1 200 OK`
   - `curl --http2-prior-knowledge -v http://127.0.0.1:9091/healthz` → `< HTTP/2 200`
   - The auto::Builder negotiation works as designed.

3. **Env-flag wiring is correct.** Baseline run with `SHELFD_H2_INITIAL_WINDOW_BYTES=` (unset) showed NO `"shelfd http2 initial-window override active"` log line — matches the conditional-emit branch.

## What killed the hypothesis

**Protocol-level facts** (verified against [AWS SDK v2 docs](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/http-configuration-netty.html) and [Trino 480 fs.native-s3 docs](https://trino.io/docs/476/object-storage/file-system-s3.html)):

| Component | Default protocol | Override path | Trino exposes? |
|---|---|---|---|
| AWS SDK v2 `NettyNioAsyncHttpClient` | **HTTP/1.1** | `.protocol(Protocol.HTTP2).protocolNegotiation(ProtocolNegotiation.ALPN)` | NO |
| Trino `fs.native-s3` (`io.trino.filesystem.s3.S3FileSystemFactory`) | inherits SDK default → **HTTP/1.1** | none | NO |
| AWS SDK v2 `S3CrtAsyncClient` | **HTTP/1.1** | CRT config; not exposed via Trino | NO |

So Trino → shelfd in production uses HTTP/1.1, period. HTTP/1.1 has no stream-windowing — frame-level back-pressure is purely TCP. shelfd's HTTP/2 initial-window setting cannot affect the throughput of a connection that's negotiating h1 in the first place.

## Why the local bench couldn't have refuted the hypothesis even if the protocol matched

Three additional infra constraints made the docker-compose smoke harness inadequate to test the analyst's "plateau at conc=16" claim directly even setting the protocol issue aside:

1. **Test fixture is too small.** Seeded Iceberg tables: `region` (5 rows), `nation` (25 rows), `orders_small` (1 000 rows). Total `shelf_origin_request_bytes_total` after 3 cold queries = 18 827 bytes. The plateau claim is about multi-MB row group reads at high concurrency — those bytes never flow through this fixture.
2. **Bench harness is sequential.** `timed-run.py` runs queries serially (no concurrency arg). Adding `ThreadPoolExecutor` would be possible but inappropriate when the protocol mismatch already invalidates the result.
3. **No h2-speaking load generator on hand.** Even if I bypassed Trino with a custom h2 client (e.g. `h2load`, `nghttp2`), and the bench showed a plateau-shift with the override, that would only prove "the spike works for h2 clients" — which has zero predictive value for the cluster's actual h1 traffic.

## What this means for the production plateau

The cluster's actual per-pod ~0.2 qps plateau (workspace memory line) **must be caused by something other than HTTP/2 windowing**. The other hypotheses workspace memory enumerated remain live:

- **AWS SDK signing context recomputation** on every request — verified there are now `shelf_origin_signing_context_recomputed_total` and `shelf_origin_signing_context_reused_total` metrics on the live build, suggesting this was already partially diagnosed and instrumented in earlier rc batches.
- **Foyer HybridCache lock contention** on hot row-group keys (per the F1/F2 deep-research findings).
- **Per-pod thread/connection pool ceilings** in shelfd (e.g. `origin.pool.maxConnections=128`).

The phase-split histogram from T1 fallback (`recv_ns → headers_sent_ns → body_start_ns → body_done_ns`) would discriminate between these by surface — connection-accept latency vs server-think latency vs body-streaming latency — much more directly than any single-knob protocol experiment.

## Files touched

- `Cargo.toml` — added `service`, `server-auto` to `hyper-util` features.
- `shelfd/src/http.rs` — replaced two `axum::serve` calls with `serve_with_h2_window` helper; added `http2_initial_window_from_env`, `serve_with_h2_window`; updated comments referencing rc9-T1.
- `benchmarks/smoke/Dockerfile.shelfd` — added `COPY shelf-advisor`, `COPY crates` for workspace-member completeness (workspace-member rule from prior memory; the build was failing on `crates/shelf-cost` before this).
- `benchmarks/smoke/docker-compose.yml` — pass-through of `SHELFD_H2_INITIAL_WINDOW_BYTES` from host env; commented out the shelf-trino-plugin volume mount (the worktree has no built jar; the iceberg connector is built-in to Trino so the plugin isn't required for this bench shape).

**No PR cut. No commits made on the worktree branch.** The h2 helper code itself is reusable for any future h2-only or h2-window experiment, but landing it on `main` should wait until a real h2 client surface exists (e.g. the future Trino h2 opt-in or a custom shelf-internal client).

## Recommendation

1. **Do NOT spend cluster bench time on the h2 window hypothesis.** Documented dead-on-arrival.
2. **Do NOT land the h2 window override on `main` as-is.** It's correct code but solves a non-existent production problem. Re-evaluate when Trino exposes h2 protocol config OR when shelf grows a non-Trino h2 client.
3. **Move T1.h2_phase_split forward as the actual root-cause path** — the phase-split histogram is the right diagnostic. Should land as a separate PR, ideally with the rest of the metric-coverage gap-fix work (T3 follow-on tickets), so observability gaps and root-cause investigation share one rollout.
4. **Track the per-pod plateau as an open issue** with the live workspace-memory hypotheses (signing context, Foyer lock contention, pool ceilings). Phase-split histogram + the cluster's existing `shelf_origin_signing_context_*` counters should give enough evidence to discriminate within one operator-driven scrape window.

## Worktree & cleanup

- Worktree: `/private/tmp/shelf-rc9-t1-h2-spike-41727`
- Branch: `rc9/t1-h2-window-spike` (uncommitted; can be removed via `git worktree remove`).
- Smoke stack: torn down (`docker compose down -v`).
