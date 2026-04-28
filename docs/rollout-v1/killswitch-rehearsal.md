# Kill-switch rehearsal — pre-cutover dry run on rep-2

**Status**: pending — blocks rep-2 cutover at T-1h.
**Owner**: shelf-oncall (executes) + k8s-eng-1 (observes) + trino-platform (PR reviewer).
**Expected duration**: 45 min including paperwork.
**Requested by**: shelf-core (rollout-v1 pre-req).

## Why we rehearse

The compressed-canary rollout pre-commits to "any alert fire during
the 48 h canary triggers immediate rollback, no debate". For that
commitment to be credible, the rollback procedure must:

1. Be **muscle-memory** for the oncall who'll execute it at
   02:00 local time on a weekend.
2. Complete in **under 2 minutes** end-to-end (PR merge → pods
   rolled → traffic back to direct S3).
3. Have **no ambiguity** about whose PR is getting reverted and
   in what namespace the rollout is getting restarted.

The existing [`docs/runbook.md`](../runbook.md) §4 kill-switch is
scoped to the Java-plugin deployment pattern (ConfigMap flag
`fs.shelf.enabled=false`). This rollout uses the
**endpoint-swap** pattern from ADR-0012 Phase 1 — a one-line
`s3.endpoint` flip in `iceberg.properties`. The rehearsal
validates that the endpoint-swap kill-switch is genuinely the
simpler mechanism the ADR claimed.

## Pre-conditions

- rep-2 coordinator + workers are **not** yet cut over. Their
  `iceberg.properties` still points at the production S3
  endpoint; the rehearsal will flip it to shelfd **temporarily**
  (30 s) and then revert. This means the rehearsal itself
  is a dress-rehearsal cutover-and-rollback on the same pod set
  we'll shortly canary for real.
- shelfd is up and healthy in the `shelf` namespace (5/5
  pods `Running`, `/readyz` returning 200).
- The correctness diff harness is running idle against rep-2
  with both catalogs set to S3-direct (this is the harness
  self-check — expected to report zero diffs throughout the
  rehearsal).
- The shelf-oncall PagerDuty schedule is armed; we want any
  alert-system misfire to surface during rehearsal, not at
  02:00 on Saturday.

## Rehearsal script (30 s canary + 60 s rollback measurement)

All commands assume `kubectl` context = rep-2 production cluster
and the rollout PR template from
[`trino-platform-scope-check.md`](trino-platform-scope-check.md).

### Step 1 — Cutover (30 s window)

```bash
# T+0: merge the rep-2 iceberg.properties endpoint-flip PR
#      (this is THE PR that will later stay merged during the
#      actual cutover; for the rehearsal we revert it 30 s later).
git -C trino-db-manifests checkout -b rehearsal/rep-2-cutover
cp trino/rep-2/etc/catalog/iceberg.properties \
   trino/rep-2/etc/catalog/iceberg.properties.rehearsal-baseline
# Apply the diff from rollout-v1/cutover-rep2.md §Diff
# ... edit ...
git commit -am "rehearsal: rep-2 iceberg.properties → shelfd:9092"
git push -u origin rehearsal/rep-2-cutover
# Merge to main via the platform's automerge path.

# T+~30s: rolling restart once manifests have reconciled.
kubectl -n trino-db rollout restart deployment/trino-rep-2-coordinator
kubectl -n trino-db rollout restart deployment/trino-rep-2-worker
kubectl -n trino-db rollout status deployment/trino-rep-2-coordinator --timeout=3m
# Confirm Trino workers are reading from shelfd:
kubectl -n trino-db exec deployment/trino-rep-2-coordinator -c trino -- \
  curl -sf localhost:8080/v1/info/state
```

At this point rep-2 Trino is reading Iceberg data via shelfd. Run
a sentinel query — the same one the correctness diff harness
uses as query-01:

```bash
kubectl -n trino-db exec deployment/trino-rep-2-coordinator -c trino -- \
  trino --execute "SELECT COUNT(*) FROM iceberg.default.events WHERE event_date = DATE '2026-04-01'"
```

Expected: returns immediately, result matches the baseline
correctness diff reference row count.

### Step 2 — Rollback (the actual measurement)

**Start a stopwatch** at the moment someone calls "trigger
kill-switch":

```bash
# T+0 (rollback start): revert the PR.
git -C trino-db-manifests checkout main && git pull
git revert --no-edit HEAD   # reverts the rehearsal cutover commit
git push origin main

# T+~15-30s: manifests reconciled; rolling restart.
kubectl -n trino-db rollout restart deployment/trino-rep-2-coordinator
kubectl -n trino-db rollout restart deployment/trino-rep-2-worker
kubectl -n trino-db rollout status deployment/trino-rep-2-coordinator --timeout=3m
```

**Stop the stopwatch** when the coordinator rollout status returns
`successfully rolled out` AND a sentinel query succeeds against
the now-direct-S3 Iceberg catalog. Record the elapsed time —
this is the measured MTTR.

### Step 3 — Post-rehearsal verification

```bash
# 1) iceberg.properties on the pod now shows the direct-S3 endpoint.
kubectl -n trino-db exec deployment/trino-rep-2-coordinator -c trino -- \
  grep s3.endpoint /etc/trino/catalog/iceberg.properties

# 2) No Trino queries failed during the 30 s cutover + rollback.
kubectl -n trino-db exec deployment/trino-rep-2-coordinator -c trino -- \
  trino --execute "
    SELECT state, count(*)
    FROM system.runtime.queries
    WHERE created > current_timestamp - interval '5' minute
    GROUP BY state"

# 3) shelf_request_seconds_count{replica=\"rep-2\"} in Prometheus
#    shows the 30 s burst and then flattens to zero.
#    (This also doubles as a per-replica-label smoke test for
#     SHELF-27a; if the label doesn't show, SHELF-27a is not
#     deployed correctly.)
```

## Success criteria

All of:

1. **MTTR ≤ 2 min** measured end-to-end on the stopwatch in Step 2.
2. **Zero failed Trino queries** during the 30 s cutover + 60 s
   rollback (expected: queries either run against shelfd-backed
   reads successfully, or complete before the rolling restart
   kicks the coordinator).
3. **Correctness diff harness** reports zero diffs on its hourly
   run that spans the rehearsal window.
4. **No unexpected alerts** fire. We expect `ShelfReadPathHitRatioCollapsed`
   may fire (with `replica=rep-2`) briefly during the 30 s cutover
   window because hits are zero and misses non-zero — this is
   *acceptable* for the rehearsal and confirms the alert is wired.
5. The rep-2 `iceberg.properties` is back at the pre-rehearsal
   baseline byte-for-byte.
6. All four pods rolled cleanly (no crashloop, no pending, no PVC
   error events during or after).

If all six are green, rep-2 is **cleared for actual cutover**; we
schedule it for the rep-2 cutover window.

## Failure handling

- **MTTR > 2 min**: root-cause before actual cutover. Most likely
  causes: (a) manifest-reconciler lag > 30 s (escalate to
  trino-platform; they may need to pre-install a
  `git pull --fast-forward` hook that skips the 5-min default
  sync interval), (b) rolling-restart timeout due to readiness
  probe (check that `/v1/info/state` becomes `ACTIVE` within
  3 min — if not, we have a Trino config bug unrelated to shelf).
- **Queries fail during the 30 s window**: shelfd is not healthy;
  do NOT proceed with rep-2 cutover. Re-run the smoke harness.
- **Correctness diff fires**: this is a genuine cache-correctness
  bug; block indefinitely until investigated. (Almost certain false
  positive at rehearsal time because shelfd has no pre-warm state
  yet — but "almost certain" is not certain.)

## Record-keeping

File the rehearsal results at
`/Users/aamir/trino/shelf/docs/rollout-v1/killswitch-rehearsal-results.md`
in this shape:

```markdown
| attribute                    | value                    |
| ---------------------------- | ------------------------ |
| rehearsed on                 | YYYY-MM-DD HH:MM UTC     |
| MTTR (stopwatch)             | e.g. 1m 47s              |
| queries failed during window | 0 (or N)                 |
| correctness diff             | zero-diff / diff fired   |
| unexpected alerts            | none / list              |
| iceberg.properties restored  | yes / no                 |
| pods rolled cleanly          | yes / no                 |
| go/no-go for rep-2 cutover   | GO / NO-GO (with reason) |
```

`shelf-core` signs off on the go/no-go based on that sheet before
the rep-2 cutover window opens.

## Why NOT rehearse on rep-0/1/3

rep-2 was the smoke target and has the deepest prior test coverage;
rehearsing on it de-risks the rehearsal itself. The rep-0/1/3
cutovers will have rep-2's successful rehearsal + rep-2's actual
cutover as their collective rehearsal — measuring MTTR separately
per replica would be busywork.
