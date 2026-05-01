# ADR 0031: `shelfctl pool-status` — single-command fleet `/stats` aggregation (RC7 D1)

*Status: Accepted (2026-05-01)*
*Deciders: rust-engineer-1, ops-aamir*
*Supersedes: none*
*Superseded-by: none*
*Related: ADR-0024 (per-pod RSS watermark alerts), ADR-0025 (cap-ready endpoint),
SHELF-23 peer-fetch (the stats payload we aggregate), `agents/out/03-plan.md` rc.7 D1*

## Context

The Shelf StatefulSet exposes per-pod `/stats` (and `/metrics`) on
the `data` port (`9090` per `charts/shelf/values.yaml`). Operators
want a "give me the whole shelf-pool's state in one screen" view
during cutover windows, capacity-prep work, and post-OOM forensics.

Today the only paths to that view are:

1. The Grafana **Shelf — Cache, Disk and Pods** dashboard (uid
   `shelf-overview`). Authoritative for trend questions; not
   ergonomic for "is anyone serving 5xx right now? show me the
   six pod IDs side by side". Also requires a browser tab and
   a Grafana session.
2. Six manual `kubectl port-forward 19090:9090` invocations + six
   `curl 127.0.0.1:19090/stats | jq`. This is what operators
   actually do — and the single local-port `19090` is a death
   trap. **Workspace memory codified twice in one morning that an
   ad-hoc port-forward bash loop quietly steered every per-pod
   probe at the same backing pod (the first subprocess never
   died), producing "all pods at the same value" reports that
   misdiagnosed an HRW-imbalanced cache as a uniform fleet for
   ~30 minutes each time** (May 1 2026, capacity-fix worker on
   shelf-pool sizing).

The Grafana dashboard is the right answer for trend questions.
Single-screen-now is a different question and deserves its own
tool.

## Decision

Add `shelfctl pool-status` as a new subcommand. The command:

1. Lists shelfd pods via the kube apiserver (label selector
   `app.kubernetes.io/name=shelf` matching `_helpers.tpl`'s
   `shelf.selectorLabels`); operators can override via
   `--selector` or pass `--pods shelf-0,shelf-3` to skip
   discovery entirely (useful when RBAC permits `pods/portforward`
   but not `pods/list`).
2. For each pod, spawns `kubectl port-forward pod/<name> :<port>`.
   The leading colon asks the OS to allocate an ephemeral local
   port; the chosen port is read off kubectl's stdout
   `Forwarding from 127.0.0.1:<N> -> ...` line. **No two probes
   in a run can collide on the local socket** because each
   subprocess gets a fresh OS port — and there is structurally
   no way to collide with a stranger's stale `19090`
   port-forward, because we never bind a fixed port.
3. Concurrently issues `GET /stats` (and `/metrics` when
   `--metrics` is on) over each per-pod localhost socket.
4. Aggregates the results into one of three output shapes:
   `table` (default, fixed-width, greppable), `json` (for `jq`
   pipelines), `tsv` (for `cut -f` / `awk`). The TSV header and
   JSON field names are explicitly versioned as a contract — a
   unit test pins the column order so a future refactor cannot
   silently break operator scripts.
5. Captures Pod-side facts the apiserver already gives us for
   free (restart count summed over container statuses) so the
   table has a clear "is this pod stable?" column.

### Why a subprocess and not programmatic port-forward

`kube-rs` 0.95 supports `Api::<Pod>::portforward` returning a
`Streams` per port. Two reasons we picked subprocess instead:

- **Distroless-pod compat.** Shelf ships on
  `gcr.io/distroless/cc-debian12:nonroot` — no `wget` / `curl` /
  `nc` to shell out from inside the pod via `kubectl exec`. The
  port-forward path is the only HTTP-from-laptop path that works
  against the stock image.
- **Subprocess gives us the same OS-allocated-port guarantee with
  zero hyper-over-stream-bridge plumbing.** A fresh `kubectl
  port-forward :9090` instance per pod is one process per probe,
  trivially isolatable, kernel-supervised, and `kill_on_drop`d at
  end of run. The programmatic path requires a hyper conn-handle
  bridged over the kube-rs `AsyncRead`/`AsyncWrite` stream — more
  code, more failure surface, no operator benefit.

The cost is one `kubectl` fork per probed pod (≤ 6 in our deploy);
that's well under the budget and recovers the operator-side
latency (~5s per `pool-status` call against six pods) we already
pay.

### Why the drain task

The first cut held the kubectl stdout `BufReader` only across
the line-parse, then dropped it. **kubectl reacted by exiting
on SIGPIPE** the next time it tried to log `Handling connection
for <port>`, and the corresponding HTTP probe came back as
`Connection reset by peer (os error 54)` — observed on the
first live-cluster smoke. Fix: keep a `tokio::spawn`'d task
draining stdout for the lifetime of the subprocess. Costs a
single sleeping task per probe; pays back zero `Connection
reset by peer` flakiness.

## Wire shape

`/stats` parsing mirrors `shelfd::control::Stats` with three
backward-compat hardenings:

- `rowgroup_pool` is `Option<...>` with `#[serde(default)]` —
  pre-SHELF-18 builds shipped without the row-group pool, and
  this CLI is intentionally tolerant of mixed-version clusters
  during a rolling upgrade.
- `pinned_bytes`, `pinned_count`, `draining`, `rss_bytes` all
  carry `#[serde(default)]` — same reason `shelfd::control::Stats`
  declared them with `#[serde(default)]` in the first place.
- `disk_used_bytes` / `disk_capacity_bytes` on `ShelfPoolStats`
  default to `0`. Old daemons (pre-SHELF-18) didn't ship them;
  the metadata pool legitimately reports `0` for both even on
  current builds (it's DRAM-only by SHELF-17).

`/metrics` parsing is intentionally string-based — `prometheus-parse`
would double the dep graph for two integer extractions.
`shelf_hits_total` and `shelf_misses_total` are summed across all
label combinations (any pool / any table), with the resulting
hit-ratio rendered as a single percent in the `--metrics` table
column.

## Alternatives considered

### A. Build into shelfd itself ("`shelfd cluster-stats` subcommand")

Reject. shelfd's responsibility is the single-pod data plane;
a fleet-aware subcommand belongs in the operator CLI, not the
daemon. Also: shelfd doesn't have RBAC to list its own siblings
in production (the chart binds it to a strictly per-pod
ServiceAccount), so this would require either widening the
production SA or shipping yet another sidecar — both regressions
the operator-side `shelfctl` cleanly avoids.

### B. Just shell out to `kubectl get pods -o jsonpath` + a bash loop

Reject. That's the today-state, and the today-state is what
walked us into the `19090` collisions. The whole reason we're
shipping a binary subcommand is to make the right thing
(OS-allocated ports, parallel probes, single output) the path
of least resistance.

### C. Use `kube-rs` programmatic port-forward (`Api::portforward`)

Park. Workable, but described in "Why a subprocess and not
programmatic port-forward" above. Revisit if the subprocess
path develops measurable per-call overhead (~current ≤ 1s
warmup per pod is fine for a fleet of six; would be worth
revisiting at a fleet of ~50).

### D. ServiceMonitor / Prometheus Mimir HTTP API as the source

Reject for this command. Prometheus is the right place for
trend questions and is what powers the Grafana dashboard. For
"give me the right-now state of the fleet without going to a
browser" the dashboard is overkill and the Mimir API requires
a service-account token rotation discipline that's heavier than
"do you have kubeconfig?".

## Rollback

The command is **strictly additive** — a new subcommand on
`shelfctl`, zero changes to `shelfd`, the chart, or any
existing CLI subcommand. Rollback paths in order of speed:

- **Operator-side disable**: do nothing. The command is opt-in
  (`shelfctl pool-status` only fires when explicitly invoked);
  no scheduled job calls it.
- **Hard revert**: revert this PR. The other six subcommands
  (`stats`, `ring`, `pin`, `unpin`, `evict`, `reload`,
  `chaos`, `bundle`, `install`) are untouched.

There is **no production blast radius** for shipping this command
— it touches no shelfd state and exercises only standard
kubernetes-side primitives (`pods/list`, `pods/portforward`).

## Verification

- 8 new unit tests in `shelfctl/src/pool_status.rs::tests`:
  v1.0 wire-shape parses / missing-rowgroup-pool parses /
  table format locks layout / JSON format locks fields /
  TSV format locks column order / `kubectl` `Forwarding from`
  parser handles IPv4 + IPv6 + garbage / `/metrics` parser
  extracts hits + misses + computes ratio / byte formatter
  unit boundaries.
- All 22 `shelfctl` `--bins` tests pass (8 new + 14 pre-existing).
- `cargo clippy -p shelfctl --all-targets -- -D warnings` clean.
- `cargo fmt --all -- --check` clean.
- **Live-cluster smoke** against `data-platform-cluster` ns
  `alluxio` (6 pods `shelf-{0..5}` on shelfd v1.0): all three
  formats return parsed `/stats` from every pod in 4–5s wall
  clock; `--metrics` returns hit-ratio per pod (~40 % across
  the fleet at smoke time) in 5–6s. Each probe shows up on a
  unique ephemeral local port (e.g. `54373`, `54381`, `54399`,
  …) — proving the `19090` collision class is structurally
  prevented.

## References

- Workspace memory entries from May 1 2026:
  - "Per-pod port-forward bash loops can return identical
    metric data for every pod" (capacity-fix worker
    misdiagnosis).
  - "For per-pod metric attribution, use the Grafana `Shelf —
    Cache, Disk and Pods` dashboard" — this CLI is the
    complement, not a replacement, for trend questions.
- `charts/shelf/templates/_helpers.tpl` — defines
  `app.kubernetes.io/name: shelf` selector label this command
  defaults to.
- `charts/shelf/values.yaml` — `service.dataPort: 9090` is the
  port we default `--data-port` to.
- `shelfd/src/control.rs` — `Stats` / `PoolStats` wire
  contract; `pool_status::ShelfStats` mirrors it with the
  backward-compat softening described above.
