# ADR 0025: `/admin/cap-ready` cluster capacity gate (RC6 P1.2)

*Status: Accepted (2026-04-30)*
*Deciders: rust-engineer-1, ops-aamir*
*Supersedes: none*
*Superseded-by: none*
*Related: ADR-0001 (no embedded raft), ADR-0002 (HRW hashing), SHELF-23
(peer-fetch + resolver), `agents/out/03-plan.md` rc.6 P1.2*

## Context

The cluster-side ops rule when adding a new replica's traffic to the
shelf StatefulSet pool is recorded verbatim in workspace memory:

> **Capacity-headroom precondition** before adding a new replica's
> traffic to shelf-pool: verify ALL existing pods are `< 22 GiB RSS`
> (the warn watermark; OOM ceiling is ~27.3 GiB on `m6a/m5a/m7a/c6a
> 4xlarge`). If any pod is over warn, **scale +2 pods first**.

In practice today the operator does this manually before each cutover:

```
for pod in shelf-{0..N}; do
  kubectl -n alluxio top pod $pod | awk '{print $3}'
done
```

Two ways this fails open in flight:

1. **A peer that is over 22 GiB but the operator misreads the
   table** — the rep-1 cutover post-mortem (Apr 28) called this
   exact pattern out: `shelf-1` was at 23.4 GiB, the operator
   skimmed the column, and the new replica's traffic landed on a
   shelf that was already past warn. shelf-pool then OOMKilled
   within ~10 min and the cutover was reverted.
2. **An unreachable peer** is invisible to the operator's loop
   (`kubectl top` simply prints `<unknown>` and most operators don't
   set `-o json | jq` to assert exhaustiveness).

The ops rule is fundamentally a binary gate: every cutover MR template
should be able to ask the cluster "may I proceed?" and trust the
answer. That is the gate this endpoint provides.

### Why a daemon endpoint and not a `kubectl` shortcut

- `kubectl top` reads from `metrics-server`, which is a separate
  service, scrapes on a 60-second-ish cadence, and lags reality
  during pod restarts (the very moments a cutover is most fragile).
- shelfd already exposes `/stats` per pod with everything needed
  except RSS. Adding RSS to `/stats` is a one-line additive change.
- shelfd already runs the SHELF-23 resolver/HRW ring; it knows the
  authoritative peer set within `dns_refresh = 5 s`. No new
  membership wiring is needed.
- The aggregation has policy in it (conservative-503 on any
  unreachable peer). Encoding that policy in shelfd lets every
  caller — cutover MRs, dashboards, future automation — get the
  same answer.

## Decision

Add a single read-only HTTP endpoint:

```
GET /admin/cap-ready[?caller=<replica-name>]

→ 200 OK {
    "ready": true,
    "max_rss_gib": <f64>,
    "max_rss_bytes": <u64>,
    "max_rss_pod": "<pod-id>" | null,
    "peers_probed": <usize>,
    "threshold_bytes": 23622320128
  }

→ 503 Service Unavailable {
    "ready": false,
    "max_rss_gib": <f64>,
    "max_rss_bytes": <u64>,
    "max_rss_pod": "<pod-id>",
    "peers_probed": <usize>,
    "peers_unreachable": ["shelf-3", ...],
    "threshold_bytes": 23622320128
  }
```

Implementation lives in `shelfd/src/capacity_check.rs` (~470 lines
including unit tests + helpers); the route handler in
`shelfd/src/http.rs` is ~50 lines and threads through the existing
`peer_http` reqwest client and `peer_stats_port` so no new connection
pool or config knob is added.

### Threshold

`DEFAULT_CAP_READY_THRESHOLD_BYTES = 22 GiB` (binary, base 1024³).
Picked because:

- Karpenter `m6a/m5a/m7a/c6a 4xlarge` instances expose
  ~27.3 GiB allocatable.
- Foyer 0.12 LDC + DRAM rowgroup pool sustained-read peaks at
  14–20 GiB observed across rep-1 and rep-2 traces.
- Empirical OOMKill threshold sits at ~24 GiB (live evidence Apr 28
  shelf-1 chaos window, recorded in workspace memory).
- 22 GiB leaves a 5.3 GiB headroom over the OOM ceiling — roughly
  one preempt + Foyer disk replay envelope.

The threshold is a `pub const`, not configurable in v1. The
intentional friction is "operators who want to override the gate
must edit Rust" rather than "any cutover MR can quietly raise the
bar". A future overlay flag can land in a follow-on ticket without
breaking the wire.

### `rss_bytes` on `/stats`

`/admin/cap-ready` aggregates `Stats.rss_bytes` across peers, so
the `/stats` payload grows one new field:

```rust
pub struct Stats {
    // ... existing fields ...
    #[serde(default)]
    pub rss_bytes: u64,
}
```

`#[serde(default)]` keeps the wire compatible with pre-RC6 peers
that haven't been rolled yet — they simply contribute `0` to the
max-RSS aggregation. A `0` is treated as "no signal", **not** as
"healthy", because the gate also tracks reachability separately
(see below). A peer running pre-RC6 shelfd will return a parseable
`Stats` with `rss_bytes: 0`; that contributes nothing to the max,
which means the gate's verdict is governed by the other peers
(any one of which over threshold flips it to 503).

`Stats.rss_bytes` is populated at handler time via
`capacity_check::read_self_rss_bytes()`, which reads
`/proc/self/status` on Linux and parses the `VmRSS:` line. On
non-Linux dev hosts the file is absent and the helper returns 0;
production runs distroless Linux so this fallback never fires in
prod. We deliberately do **not** depend on a `libc::sysconf`
page-size lookup — `/proc/self/status` reports kB directly, which
is always portable Linux contract.

### Failure modes

1. **Peer over threshold** → `503` with `max_rss_pod` set to the
   offending pod. Operator's runbook: scale +2 shelf pods, wait
   ~78 s for SHELF-23 peer-fetch to redistribute load, re-curl.
2. **Peer unreachable** (timeout, non-2xx, body-parse error) →
   `503` with the unreachable pod listed in `peers_unreachable`.
   We never silently skip a peer because the ops rule explicitly
   says "verify ALL pods" — silently dropping a pod from the
   aggregation would let a saturated pod hide as "no signal".
3. **Empty router view** (boot-time placeholder, transient DNS
   blip) → `200` based on self-RSS only. The empty-ring case is
   an existing operational signal exposed via `/admin/ring`; it
   is not the cap-ready gate's job to alarm on it. Cutover
   tooling that wants belt-and-suspenders should curl both
   endpoints in series.
4. **Self-RSS read fails** (non-Linux dev host) → `read_self_rss_bytes`
   returns 0 and self contributes nothing to the max. This makes
   unit and integration tests on macOS / Windows produce
   deterministic `200` responses — exactly what the test harness
   needs.

### Audit metadata: `?caller=<replica-name>`

Optional query parameter, opaque to the daemon. The cutover MR
template threads the calling replica name (e.g. `rep-0`) through
it so `kubectl logs -l app=shelf -n alluxio | grep cap_ready`
shows which side initiated each gate check. We log it via
`tracing` at `info!` level on a pass and `warn!` on a fail. We
never act on it.

## Alternatives considered

### A. `metrics-server` / `kube-state-metrics` cluster-side check

Reject. Lag is too high (60 s for `metrics-server`, 30 s for
KSM/Prom scrape), and the policy ("conservative on unreachable")
has nowhere natural to live — `kubectl` callers would need a
PromQL or jq filter for every check, and inconsistencies between
team members are guaranteed.

### B. Aggregate over `/stats` `used_bytes` instead of adding RSS

Reject. `used_bytes` is the count of admitted entries; RSS is
process resident memory. They diverge sharply: in-flight S3
buffers, the LDC submit queue, and Foyer's read-path scratch are
all in RSS but not in `used_bytes`. The Apr 28 OOMKill pattern
that drove this ticket had `used_bytes` at ~13 GiB while RSS was
~24 GiB — a `used_bytes`-based gate would have passed and the
cutover would still have toppled the pod.

### C. Push the policy into the cutover MR template / shell script

Reject. We did this for two months; the operator-loop variant is
exactly the path that failed in the rep-1 incident. Putting the
policy in shelfd lets every consumer — MR templates, dashboards,
future automation — share the same verdict, with the same
reachability semantics, and the same audit trail.

### D. Make the threshold a Helm value

Defer. The point of the gate is that it is friction — a curl that
returns 503 is supposed to make the operator stop, not be tuned
into compliance. We can revisit if a cluster shape ever ships
with materially different memory headroom (e.g. switching to
m7a.8xlarge with 64 GiB allocatable). Until then, hard-coded
22 GiB is a feature, not a bug.

## Rollback

The endpoint is read-only and side-effect free.

- **Disable**: `kubectl set image sts/shelf shelf=<previous-image>`.
  No data-plane impact; no on-disk format change to undo.
- **Tactical bypass for an emergency cutover**: skip the curl and
  rely on the operator's manual loop, OR override the response by
  pinning `--header 'X-Cap-Ready-Override: true'` (not implemented
  in v1; explicitly out of scope per "intentional friction" above).
- **Hard revert**: revert this PR. The wire-additive `rss_bytes`
  field on `/stats` is harmless when not consumed; pre-RC6 peers
  already ignore unknown JSON fields under `serde(deny_unknown
  fields)` = false.

## Verification

- 7 unit tests in `shelfd/src/capacity_check.rs`:
  threshold-pass / threshold-fail / unreachable-peer-forces-503 /
  empty-ring-falls-back-to-self-only / self-pod-id-filtered /
  bytes_to_gib rounding / read_self_rss_bytes non-panicking smoke.
- 3 integration tests in `shelfd/tests/it_cap_ready.rs`:
  empty-ring 200 / `?caller=` parameter accepted / `/stats`
  carries `rss_bytes`.
- `cargo clippy --all-targets -- -D warnings` clean.
- `cargo fmt --check` clean.
- 367 existing shelfd lib tests + 8 it_admin tests still pass —
  the additive `rss_bytes` field on `Stats` did not regress the
  Agent-5 wire contract.

## References

- Workspace memory entry "rep-0 cutover-prep operational learnings
  (Apr 30 afternoon)" — capacity-headroom precondition.
- Workspace memory entry on the Apr 28 shelf-1 OOMKill chaos window.
- `agents/out/adr/0001-no-embedded-raft.md`,
  `agents/out/adr/0002-hrw-hashing-over-vnode-ring.md` — the
  membership/routing substrate this gate consumes.
- SHELF-23 design note for the `Resolver` / `Router::view` contract.
