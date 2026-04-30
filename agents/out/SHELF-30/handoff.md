# SHELF-30 hand-off — row-group range coalescing (single-flight, v1)

| Field                          | Value                                          |
|--------------------------------|------------------------------------------------|
| Ticket                         | SHELF-30                                       |
| Plan                           | `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md` § P0 lever 1 |
| Branch                         | `shelf-30-row-group-coalesce`                  |
| Worktree                       | `/private/tmp/shelf-30-coalesce`               |
| Off-branch base                | `origin/main` @ `a2cef45` (SHELF-29 merged)    |
| Cargo workspace version        | `1.0.0-rc.4` → `1.0.0-rc.5`                    |
| Helm chart version             | `1.0.0-rc.4` → `1.0.0-rc.5` (chart + appVersion) |
| Image tag (orchestrator owns)  | `1.0.0-rc.5` — NOT yet built / pushed          |
| Image digest (GHCR)            | pending build                                  |
| ADR                            | `agents/out/adr/0013-row-group-range-coalescing.md` |
| Cutover window (IST)           | pending — orchestrator + smoke-watcher own this |
| Replica                        | rep-1 (per plan § P0; lower-traffic, fastest revert) |
| `cargo fmt --all`              | clean                                          |
| `cargo clippy -p shelfd --all-targets -- -D warnings` | clean (0 warnings) |
| `cargo test -p shelfd --lib`   | **245 passed; 0 failed; 0 ignored** in 0.52 s  |
| `SHELF_INTEGRATION=1` result   | n/a — no integration test boots shelfd for this lever; v1 unit coverage is sufficient (see ADR § Test surface). Integration is a SHELF-30b follow-up. |
| Hit ratio start / 12h          | pending soak — orchestrator dispatches smoke-watcher |
| p50 / p99 read latency 12h     | pending soak                                   |
| `shelf_lodc_drops_total` delta | pending soak                                   |
| RSS peak (any pod)             | pending soak                                   |
| Rollback fired?                | n/a — pre-deploy                               |
| Open follow-ups                | • SHELF-30b: footer-aware row-group quantization (gated on SHELF-34 footer parser landing). Removes the v1 "follower does not populate own cache slot" trade-off.<br>• SHELF-30 integration test (`SHELF_INTEGRATION=1 cargo test -p shelfd --test it_coalesce`) — boots shelfd + MinIO and exercises the leader/follower fan-out under load. |

## Files changed

```
agents/out/adr/0013-row-group-range-coalescing.md   (new)
agents/out/SHELF-30/handoff.md                      (new — this file)
charts/shelf/Chart.yaml                             (version bump)
Cargo.toml                                          (workspace version bump)
shelfd/src/coalesce.rs                              (new — 480 LOC incl. 13 unit tests)
shelfd/src/lib.rs                                   (+1 line: `pub mod coalesce;`)
shelfd/src/metrics.rs                               (+4 IntCounterVec definitions)
shelfd/src/http.rs                                  (+ `coalescer` + `coalesce_enabled` on ServerState; +`is_coalesce_enabled` / `set_coalesce_enabled`)
shelfd/src/s3_shim.rs                               (+ SHELF-30 wiring inside `handle_get_object`)
shelfd/docs/metrics.md                              (+4 metric rows)
```

## Rollback signals (per plan § P0 lever 1, smoke-watcher fills these in live)

| Trigger | Action |
|---|---|
| `histogram_quantile(0.99, rate(shelf_origin_request_seconds_bucket[5m]))` regresses > 50 % vs pre-cutover baseline for > 10 min on rep-1 | revert image to `1.0.0-rc.4` |
| `shelf_misses_total{pool="rowgroup"}` rate up > 20 % vs baseline at constant traffic for > 10 min | revert |
| `rate(shelf_coalesce_fallthrough_total{reason!="leader_dropped"}[5m]) > 0` sustained | revert (correctness alarm — followers should never fall through for non-drop reasons in steady state) |
| Runtime kill-switch | `state.set_coalesce_enabled(false)` — no rebuild required; restores pre-SHELF-30 path bit-for-bit |

## What the next agent owns

1. **Image build (orchestrator)** — tag `v1.0.0-rc.5` on this branch's HEAD, push, watch GHA `release.yml` produce
   `ghcr.io/shelf-project/shelfd:1.0.0-rc.5` + Helm chart at
   `oci://ghcr.io/shelf-project/charts/shelf:1.0.0-rc.5`. Per
   workspace memory, free GHA arm64 emulation will time out — use
   `linux/amd64`-only for the rc.
2. **Cluster cutover (smoke-watcher)** — flip `shelf` StatefulSet image
   on the `alluxio` namespace via `kubectl set image` against the
   in-cluster manifest (NOT Helm — workspace deploy-source-of-truth
   convention). Rep-1 first per plan § P0.
3. **90-min smoke** with the rollback-signal table above active. Probe
   loop via `block_until_ms: 0` + terminal-file readback (workspace
   `nohup ... &` trap). Fill in the "pending soak" rows above and
   close the table.
4. **Post-soak verdict** — orchestrator flips the plan-file
   `todos:` `shelf-30` `status` from `in_progress` to `closed`.
   Builders do NOT edit the plan.

## Why the v1 design is subsumption-only (not partial-overlap, not footer-aware)

See ADR-0013 § "Why subsumption only in v1" — partial overlap and
footer-aware quantization both require either future-merging or a
Parquet footer parser, neither of which belongs in `coalesce.rs`. The
upgrade path is explicitly: SHELF-30b once SHELF-34's
`parquet_meta.rs` parser is in production. v1 captures the dominant
production case — KEDA scale-out producing many concurrent splits
asking for the same Iceberg row-group bytes — without taking on
parser risk.
