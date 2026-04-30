# SHELF-40 — Arrow Flight v2 / EFA RDMA — deferral memo

- **Ticket**: SHELF-40 — *"Arrow Flight v2 / EFA RDMA for ≥ 1 MB transfers"* (P3 lever 18 in `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md`).
- **Status**: **DEFERRED — code lever explicitly out of scope this cycle.**
- **Branch**: `shelf-40-41-deferral-memos`
- **PR**: opened on push.

## Why deferred

Plan §"P3 — exploratory / long-tail" lever 18 (line 276 of the plan), verbatim:

> Already on the blueprint. Worth it only if profiling after P0/P1 shows TLS / TCP CPU dominance — **today data-plane CPU is < 10 %, so this is not the binding constraint yet.**
>
> **Reference correction**: the blueprint's earlier mention of Apache Crail as the inspiration is dated — Crail [retired from Apache Incubation on 2022-06-20](https://crail.apache.org/). Cite Arrow Flight directly.
>
> Rustls is already competitive with OpenSSL on x86_64 + ARM64 ([rustls.dev/perf](https://rustls.dev/perf), 2024 benchmarks), so TLS itself is not the binding factor — the win is in data-copy elimination via RDMA, not crypto offload.

The deferral conditions are concrete and measurable:

1. **Profiling shows TLS / TCP CPU dominance** — i.e. > 30 % of `shelfd` CPU time spent inside rustls or kernel TCP stack on a representative production workload. Today the cluster's measured data-plane CPU is < 10 % per the workspace memory and the SHELF-23 / SHELF-25 / SHELF-29 soak data, so the binding constraint is elsewhere (origin S3 round-trip, Foyer NVMe latency, admission-queue back-pressure).
2. **EFA-capable instances available on the `alluxio` Karpenter NodePool** — current EC2NodeClass is `m6a/m5a/m7a/c6a 4xlarge` on-demand. EFA needs `c5n / c6gn / c6in` or similar EFA-capable types. Re-shaping the NodePool is a separate operational decision with cost implications and is outside Shelf's scope.

## What the eventual lever will look like (preserved for future reference)

When the gate fires, SHELF-40 lands as:

- New module `shelfd/src/flight_v2.rs` adding an Arrow Flight gRPC endpoint for transfers ≥ 1 MiB.
- Existing HTTP path stays for transfers < 1 MiB (Parquet footers, Iceberg manifests) — Arrow Flight's `RecordBatch` IPC framing is wasteful for sub-1 MB objects per BLUEPRINT §8.3.
- EFA enablement at the EC2NodeClass level (`networkInterfaces.0.enableEfa: true`).
- Catalog property flag for Trino-side opt-in so a partial rollout is possible.
- ADR + THREAT_MODEL.md before any code lands.

## Open follow-ups

1. **Establish the profiling baseline** — periodic pprof / `perf record` runs on `shelf-1`, `shelf-2` showing CPU breakdown by component. Until we have a steady stream of these, we cannot honestly say "TLS is the bottleneck" or "it's not". Suggested cadence: monthly profile attached to a Confluence page under `DEA/Shelf/profiling`.
2. **Track Arrow Flight v2 release** — the v2 RFC tightens framing for small payloads. If v2 lands with sub-1 MB efficiency improvements, the "split HTTP for < 1 MB / Flight for ≥ 1 MB" architectural decision in the BLUEPRINT may need re-evaluation.
3. **Re-evaluate at v1.1.0+** — if profiling still shows CPU < 30 % spent on TLS / TCP, retire SHELF-40 outright as "speculative future work, no production motivator".

## Rollback signals

N/A — no code shipped this cycle.
