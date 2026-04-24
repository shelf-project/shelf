# Shelf — Cluster handoff packet

This is the "what's left for ops" packet after the local code push closed
every ticket that doesn't need a real Kubernetes cluster. Sprints A / B / C
landed the following on `main`:

```mermaid
timeline
    title Shelf pre-handoff delivery
    section Sprint A
      SHELF-09 : Distroless Dockerfile + helm-lint CI
      SHELF-12 : docker-compose smoke (Trino + shelfd + MinIO)
    section Sprint B
      SHELF-17 : Iceberg manifest pool with 5 GiB / pool-isolation test
      SHELF-16 : Row-group byte-range key extension (Rust + Java)
    section Sprint C
      SHELF-22 : S3-compat read shim on :9092
      SHELF-23 : shelfctl stats/ring/pin/evict/reload subcommand bodies
      SHELF-24 : Pin-list loader with SIGHUP + 15m timer
      SHELF-27 : Four-big-numbers dashboard + alert rules
```

Bookkeeping commits (SHELF-01/04/11) and the Phase-0 / Phase-1 foundation
landed in earlier sprints. A follow-up pass also closed the four
locally-completable tickets (**SHELF-01a**, **SHELF-16b**, **SHELF-18**
code + local gate, **SHELF-26a**, **SHELF-28** runbook + smoke
variants). What remains is **four tickets that cannot be completed
without a live 3-pod StatefulSet on EKS** — ops owns those. SHELF-18
and SHELF-28 retain cluster-only acceptance items (NVMe PVC runtime
proof and live-traffic chaos drills); the design, tests, and CI rails
are done.

## The cluster-gated tickets

| Ticket | What it asserts | Why it needs a cluster | Owner (suggested) |
|---|---|---|---|
| **SHELF-13** Shadow-traffic rollout on rep-2 | 5 % → 50 % → 100 % shadow mirror via Trino Gateway; no incidents for 72 h at 100 % | Requires Trino Gateway config, replica-2-canary resource group, rep-2 query traffic | trino-platform + sre-1 |
| **SHELF-14** Experiments E1, E3, E10, E12 | Cold miss-mix, warm re-run, pod rotation mid-query, mixed Trino / DuckDB traffic | Needs SHELF-13 rollout to have real shadow traffic; experiment scripts live in `benchmarks/` but harness is cluster-bound | rust-engineer-1 + sre-1 |
| **SHELF-18** NVMe PVC rollout (runtime half) | 500 GiB PVC per pod, data survives real pod restart | Code + `it_hybrid_pool.rs` prove the runtime contract locally (S3-FIFO, DRAM+NVMe parity, recreation survives); the PVC mount + pod-restart proof needs a StatefulSet | rust-engineer-2 + k8s-eng-1 |
| **SHELF-20** Pod-rotation conformance (E7 only) | < 1 % mis-routed requests during a rolling restart | Java side + `/stats` contract landed in earlier sprint; the 1 %-mis-route measurement needs a 3-pod rolling restart on real traffic | trino-plugin-eng-1 + sre-1 |
| **SHELF-21** 3-pod StatefulSet rollout | Helm upgrade rehearsal, anti-affinity across AZs, NVMe PVC mount semantics | `charts/shelf/` renders fine under `helm lint` + `helm template`, but the rollout happens on the cluster | k8s-eng-1 |
| **SHELF-28** Cluster-mode chaos drills | Pod-kill / disk-fill / network-partition under real dashboard traffic | Runbook + green-in-CI smoke variants already landed (`make chaos-*-smoke` via `chaos/smoke-*.sh` in `smoke.yml`); cluster drills need live traffic | sre-1 |

## v0.5 gate (blocks `v0.5` tag)

The v0.5 gate is a **7-day production observation window** after
SHELF-13 / 14 / 21 / 27 / 28 are all green. Observation window metrics
are the four big-number panels on the SHELF-27 dashboard (shipped at
`charts/shelf/grafana/dashboards/shelf-read-path.json`) plus the alert
rules at `charts/shelf/grafana/alerts/shelf-read-path.yml`.

Green criteria:
- **Hit-ratio ≥ 80 %** weekly p50 on the overall panel (per-pool can
  dip for specific workload mixes).
- **p99 read latency ≤ 100 ms** at steady state — the
  `ShelfReadPathP99Degraded` alert must not fire.
- **5xx rate ≤ 1 %** — `ShelfReadPathHighErrorRate` must not fire.
- **Hit-ratio must not collapse** — `ShelfReadPathHitRatioCollapsed` is
  informational; one firing inside the 7-day window is fine, two in a
  row suggests a deeper problem.

## Pointers for the ops takeover

- **Runbook seeds** — `shelf/docs/oncall.md`, `shelf/docs/SLO.md`,
  `shelf/docs/capacity.md` all have bootstrap content already. SHELF-28
  extends them; don't rewrite them.
- **Helm chart** — `charts/shelf/` with `values-dev.yaml` and
  `values.yaml`. `helm lint charts/shelf` is green in CI via
  `.github/workflows/helm-lint.yml`.
- **Docker image** — `shelfd/Dockerfile` produces a distroless image
  ≤ 80 MB compressed. Build in CI via the `helm-lint` workflow; promote
  via whatever registry path ops uses.
- **Observability** — the dashboard ConfigMap is gated on
  `values.grafana.enabled = true`. Alert rules are raw Prometheus YAML
  at `charts/shelf/grafana/alerts/` — wire into the Prometheus
  operator's `PrometheusRule` CRD or drop into the alerting stack
  ops already runs.
- **Pin list** — `s3://<config-bucket>/shelf/pin_list.json`. Schema
  and reload semantics documented in
  `shelfd/docs/design-notes/SHELF-23-24-admin-surface-and-pinlist.md`.
  Initial list can be empty (`{"version":1,"entries":[]}`); fill it
  after running the **SHELF-26** replay harness (`make replay-rep2-7d`
  in `shelf/benchmarks/trino_logs/`) against a real 7-day rep-2 trace
  — the `sim-<config>.csv` output identifies the keys a size-only
  admission would have missed that a pinned workingset would have
  caught.
- **S3-compat shim** — port `:9092` on every pod. boto3/DuckDB/Polars
  access path in `shelfd/docs/design-notes/SHELF-22-s3-compat-shim.md`.
  No auth today; expose behind the same network policy as `:8080`.
  **Trino wiring**: this is *also* the Trino read-path wiring —
  Trino 480's public `Plugin` SPI does not expose
  `getFileSystemFactories()`, so we cannot register a Java FS
  factory through the plugin path. Instead, point the Iceberg
  catalog's `s3.endpoint` at `http://shelfd:9092` (see
  `benchmarks/smoke/config/trino/etc/catalog/iceberg.properties`).
  Trino's native S3 client then issues normal
  `HeadObject`/`GetObject(Range)` calls; the shim ignores SigV4
  signatures by design, caches in Foyer under the same
  content-addressed key the Java plugin would have used, and falls
  through to MinIO/S3 on miss. For the smoke harness,
  `iceberg.metadata-cache.enabled=false` forces the warm run to
  re-hit the shim so `shelf_hits_total` is observable; in production
  leave it at the default (the Iceberg JVM cache is a latency win
  on top of Shelf).
- **shelfctl** — `cargo build -p shelfctl --release` produces the
  operator CLI. Subcommands: `stats`, `ring`, `pin`, `unpin`, `evict`,
  `reload`. All talk to `/admin/*` over HTTP (default endpoint
  `http://127.0.0.1:8080`). Packaging via the same distroless-adjacent
  image or a separate `shelfctl` image — ops call.

## What is NOT in scope for this handoff

- Footer TCompactProtocol parser — **SHELF-16b — CLOSED.**
  `io.shelf.client.CompactProtocolReader` +
  `io.shelf.client.ParquetFooterIndex` ship the hand-rolled parser;
  116 Java tests green including 11 `ParquetFooterIndexTest` cases
  against real footers built by the in-repo test writer.
- FrozenHot eviction policy — tracked as **SHELF-17a**. SIEVE ships
  today. Re-evaluate after the SHELF-26 replay harness is pointed at
  a real rep-2 trace and shows whether manifest hot-set thrash is a
  real concern (the `metadata`-pool per-config hit-rate in
  `benchmarks/trino_logs/results/.../summary.json` is the signal).
- Unified PR CI rail — **SHELF-01a — CLOSED.**
  `.github/workflows/verify.yml` runs parallel `cargo fmt + clippy +
  test`, `mvn verify`, and `pytest benchmarks/trino_logs` lanes with
  a final `verify-gate` aggregation job. Dockerfile + helm-lint rails
  live under SHELF-09 / `helm-lint.yml` / `smoke.yml`; `security.yml`
  handles supply-chain scans.
- Join/subquery predicate extraction — **SHELF-26a — CLOSED.**
  `shelf_replay.predicates` does alias-aware sqlglot predicate
  extraction across joins, subqueries, CTEs, and `OR` collapses;
  `PredicateTerm.table_alias` lets the simulator prune per-scan. 29
  Python tests green.
- Foyer NVMe hybrid tier (local half) — **SHELF-18 — CLOSED locally.**
  `foyer::HybridCache` with `DirectFs` + `LargeEngine` +
  `S3FifoConfig::default()` ships behind
  `pools.rowgroup.nvme_bytes > 0`; DRAM-only path preserved.
  `it_hybrid_pool.rs` (4 tests) pins the contract; ADR-0009 captured
  in `shelfd/docs/design-notes/SHELF-18-nvme-hybrid-pool.md`. Cluster
  PVC rollout still lives under SHELF-21.
- v0.5 gate runbook + chaos smoke rails — **SHELF-28 — CLOSED
  locally.** `docs/runbook.md` documents the five green criteria,
  3-click eval path, weekly drills (cluster + smoke variants), and
  the kill-switch tree. Smoke scripts (`chaos/smoke-*.sh`) run in CI
  as the `chaos-smoke` job in `smoke.yml`. Cluster-mode drills
  (`make chaos-keda-rotation`, `make chaos-pod-kill`) are ops
  territory per the runbook.

## Cross-references

- Full plan: `agents/out/03-plan.md`
- Blueprint: `BLUEPRINT.md`
- ADRs: `agents/out/adr/`
- Design notes per ticket: `shelfd/docs/design-notes/SHELF-*.md` and
  `clients/trino/docs/design-notes/SHELF-*.md`
