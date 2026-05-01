# ADR-0027 — SIGTERM-only drain-aware admission (rc.7 / A2)

| Field      | Value                                                                                          |
| ---------- | ---------------------------------------------------------------------------------------------- |
| Status     | Accepted                                                                                       |
| Date       | 2026-05-01                                                                                     |
| Ticket     | A2 (rc.7 roadmap)                                                                              |
| Vehicle    | `feat(rc7): A2 SIGTERM drain-aware admission`                                                  |
| Supersedes | none                                                                                           |
| Related    | SHELF-20 (lameduck DrainSignal), ADR-0009 (eviction), ADR-0029 (A1 RSS multiplier), ADR-0010 (lameduck v0.5 gate) |

## Context

The 2026-05-01 morning [c6a OOM cascade RCA](../../docs/incidents/2026-05-01-c6a-cascade.md)
forced two operational changes inside the same window:

1. **Karpenter NodePool budget widening** — the `alluxio` NodePool's
   `disruption.budgets[0].nodes` was raised from `0` to `1` so the
   c6a-instance-family drop could actually drain a node. Without that
   change Karpenter held the cordon indefinitely; with it, the
   cordon flowed but exposed a second, narrower problem.
2. **Wasted admits during drain** — even after the budget widened
   and pods on the c6a nodes started terminating, those pods kept
   admitting bytes for the few hundred milliseconds between
   `SIGTERM` and the kubelet kill. Each wasted admit cost a Foyer
   insert, an NVMe write, and (on a miss) an S3 GET — bytes the
   pod was about to lose anyway. Counter `shelf_admissions_total`
   on the draining pods showed steady ingress through the
   termination window, not the expected sharp drop-off.

SHELF-20 already lights up the right signal for the local pod:

> 1. `main` receives `SIGTERM`.
> 2. `DrainSignal::begin()` flips the bit.
> 3. The next `GET /stats` response carries `draining: true`.
> 4. Peers' resolvers drop the pod from their HRW rings.
>
> *(`shelfd/src/membership.rs` module-level docs)*

…but the daemon's *own* admit gate did not consult that bit.
Peers learned about the drain on their next `dns_refresh + p99
stats probe` (≤ 6 s), and during those 6 s every read miss that
landed on the draining pod paid for an unwanted admit. On the c6a
cascade this stretched longer because the kubelet kill arrived
*before* the grace window closed: the wasted-admit class did not
self-bound.

## Decision

Plumb the existing `Arc<DrainSignal>` (SHELF-20) into the
`FoyerStore::get_or_fetch` admit gate, with a single config
opt-out and a dedicated metric.

**Mechanism.** When the local pod's `DrainSignal::is_active() ==
true` and `cache.drain.refuse_admits == true`, the admit gate
short-circuits **before** the policy / SHELF-21e level / SHELF-29
+ A1 rate gates and bumps a new
`shelf_admit_refused_total{reason="draining"}` counter. The
caller still receives the bytes (cache miss, not error); only the
side-effect of holding the bytes in Foyer is suppressed. Pinned
keys (operator-blessed via the SHELF-24 pin-list) bypass the
gate — pin replay during a drain remains observably the same.

**Reads keep serving from cache.** A2 narrowly targets writes /
admits. The existing `/stats` advertised drain bit is what causes
peers to reroute *new* traffic; in-flight reads continue against
the warm cache for the lameduck grace window
(`membership.drain_grace`, default 15 s) until the resolver
loop's `wait_drained` returns. The new `cache.drain.grace_seconds`
field documents the intended grace budget but does not enforce
its own sleep — `MembershipConfig::drain_grace` still owns the
behaviour. v1 keeps a single source of truth for the wait.

**Default-on.** `cache.drain.refuse_admits` defaults to `true`
because A2 is the operational fix for an incident that has
already cost us. We accept the marginal risk of an over-eager
gate (operator escape hatch is a config-only flip, no rolling
restart needed) over the demonstrated risk of unbounded wasted
admits during drain.

### Why not kube-rs / downward API for an earlier signal?

A v2 sketch we considered: subscribe to the Kubernetes
`DeletionTimestamp` via `kube-rs` so the gate engages at the
moment the API server marks the pod terminating, ~60 s before
SIGTERM in a Karpenter graceful drain. Three reasons we skipped
this in v1:

1. **Dependency surface.** `kube-rs` adds ~40 transitive crates
   and a ~6 MiB binary footprint, plus an in-cluster ServiceAccount
   permission for the daemon to GET its own pod object. SHELF-20's
   SIGTERM path is already plumbed and battle-tested; A2 just
   *consults* it.
2. **Marginal earlier signal.** Karpenter sends SIGTERM at the
   start of `terminationGracePeriodSeconds` by default. The 60 s
   "warning" the downward API would give us is real but bounded;
   the bytes admitted during those 60 s are bounded by the
   SHELF-29 rate cap (default 200 MiB/s) plus the A1 RSS
   multiplier, i.e. at most ~12 GiB per pod-drain. The c6a
   cascade's wasted admits were the *post-SIGTERM* tail (hundreds
   of MiB), and that's the tail A2 closes definitively.
3. **Risk asymmetry for v1.** A spurious `DeletionTimestamp`
   trigger (k8s upgrade, controller flap, eviction-API quirk)
   would silently disable cache writes on a healthy pod. SIGTERM
   is operator-evident: the pod really is terminating; if the
   gate engages there is no false-positive class to defend
   against.

If post-deploy data shows a meaningful "wasted admits in the
60 s before SIGTERM" residual, v2 can layer the kube-rs path on
top of A2 without changing the metric or the config key.

## Consequences

### Positive

- **Closes the wasted-admit class.** The c6a-cascade pattern
  (drain → kubelet kill → wasted admits) is now bounded to the
  duration of the read in-flight at SIGTERM; new admits stop on
  the same atomic load.
- **Observable.** `shelf_admit_refused_total{reason="draining"}`
  is a clean SLO trip wire. `shelf_drain_active` (0/1 gauge)
  cross-checks the gate engaged on the right signal.
- **Safe-by-default for tests.** `FoyerStore::open` creates the
  store with `refuse_admits = false`; a fresh signal is permanently
  inactive. Production wires the real one via
  `FoyerStore::with_drain`. Unit-test surface stays unchanged.

### Negative

- **One extra atomic per admit on the rowgroup hot path.** Same
  shape as the existing A1 multiplier read; benchmarked at
  <2 ns on amd64 / arm64 in the SHELF-29 perf sweep. Negligible.
- **Drain-grace window of fewer admits.** During the lameduck
  window, the pod no longer participates in the cluster's
  cache-fill workload. This is the *intended* effect — the pod
  is going away — but a hot rolling restart that touches every
  pod in sequence will see a slightly longer time-to-warm on the
  *new* pods because the *old* pods don't backfill them during
  drain. Bounded by `dns_refresh` (5 s default) per pod.

### Neutral

- The rollout is config-flag-able. The escape hatch
  (`cache.drain.refuse_admits=false`) reverts behaviour to the
  pre-A2 state on the next config reload — no rolling restart
  required for the gate to disengage. A pod that has *already*
  flipped its drain bit will of course continue terminating; the
  flag governs the *steady-state* behaviour for new SIGTERMs.

## Rollback

| Trigger                                                                                                   | Action                                                                                                                |
| --------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `shelf_admit_refused_total{reason="draining"}` rate spikes during a known-non-drain event                 | Investigate spurious DrainSignal trigger; revert via `cache.drain.refuse_admits=false` in the values overlay.         |
| Hit ratio drops > 5 pp post-deploy with no other changes                                                  | Drain detection too eager *or* unrelated; flip `cache.drain.refuse_admits=false`, observe, then re-enable selectively. |
| Pod restart loop where `shelf_drain_active` is `1` outside SIGTERM                                        | A signal handler regression; flip the config and open a SHELF-2x bug.                                                 |

The flag is a **config-only revert**; no rolling restart is
required for the gate to disengage on new admits. Existing draining
pods finish terminating regardless.

## References

- `shelfd/src/membership.rs` — SHELF-20 `DrainSignal` (lines 144 — 168).
- `shelfd/src/store.rs` — `FoyerStore::with_drain`, admit-gate hook.
- `shelfd/src/config.rs` — `DrainConfig`, `cache.drain.{refuse_admits, grace_seconds}`.
- `shelfd/src/metrics.rs` — `ADMIT_REFUSED_TOTAL`, `DRAIN_ACTIVE`.
- Workspace memory: 2026-05-01 morning bullet on c6a Karpenter
  drain widening (`/Users/aamir/trino/AGENTS.md`).
- ADR-0010 — original v0.5 lameduck gate (SHELF-20 PR baseline).
