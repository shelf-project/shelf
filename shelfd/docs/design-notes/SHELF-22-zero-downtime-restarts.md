# SHELF-22 — Zero-downtime restarts for shelf StatefulSet

**Status:** design — pending review by user
**Author:** agent (this session)
**Date:** 2026-04-28
**Linked tickets:** SHELF-20 (graceful drain — implemented), SHELF-22 (this work)

---

## Problem

Today, restarting any shelf-N pod (rollout, OOM, eviction, node-drain) causes
trino-replica-N's `cdp` catalog to fail every read for ~60–120s, because trino
is hard-pinned to the per-pod headless DNS:

```
# trino-replica-1-values.yaml line 1002
s3.endpoint=http://shelf-1.shelf.cache.svc.cluster.local:9092
```

This DNS name resolves only to the IP of pod `shelf-1`. When that pod goes
NotReady, the lookup either fails (DNS removes the A-record) or returns the
old IP for 30s while the kubelet starts the replacement, and every Trino
S3 client request errors with `Connection refused` / `connect timed out`.

The SHELF-20 lameduck-drain protocol *is* implemented (deregister, wait
inflight, exit clean) — but the client never gets a chance to re-route
because the client only knows one address. The protocol's "tell other
shelf pods to take over" half works (HRW redistributes ownership inside
the pool); the "tell other clients to use other shelf pods" half is
silently bypassed.

Concrete user impact observed 2026-04-28:

- shelf-1 OOMKilled at 04:15 UTC → rep-1 cdp queries failed for ~90s
- cutover/revert MRs trigger ArgoCD-driven rollout of trino workers + the
  shelf StatefulSet → 2× outage (one per restart cycle)
- StatefulSet identity *prevents* "new pod up before old pod down". A
  StatefulSet by definition runs exactly one pod-N. We cannot fix this
  inside the StatefulSet contract.

## Goal

Restarting any shelf-N pod is invisible to Trino:

- 0 client-visible request failures (5xx, connection refused, timeout)
- ≤ ~80 ms one-shot cache-miss latency on the ~1/N keys whose primary
  owner is the restarting pod, until that pod re-warms
- StatefulSet behaviour, NVMe PVC, per-pod identity all preserved
- Membership resolver / SHELF-20 lameduck drain unchanged

## Non-goals

- Multi-region / multi-AZ failover (separate ticket)
- Coordinator-side trino HA (a separate, much larger problem)
- Eliminating the cold-cache penalty entirely (acceptable tradeoff)

## Design

Three independent changes:

### (a) Add a non-headless ClusterIP service `shelf-pool`

```yaml
# charts/shelf/templates/service-pool.yaml (new)
apiVersion: v1
kind: Service
metadata:
  name: {{ include "shelf.fullname" . }}-pool
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "shelf.labels" . | nindent 4 }}
spec:
  type: ClusterIP                  # NOT headless
  selector:
    {{- include "shelf.selectorLabels" . | nindent 4 }}
  ports:
    - name: s3shim
      port: {{ .Values.service.s3shimPort }}
      targetPort: s3shim
      protocol: TCP
  internalTrafficPolicy: Cluster   # full pool, not topology-aware
```

This service:

- has a stable cluster-IP (one IP for the whole pool)
- load-balances across all `Ready` shelf pods
- automatically removes a pod from the endpoint set the moment its
  readiness probe fails (which fires before SIGTERM thanks to
  `terminationGracePeriodSeconds=60`)
- exposes only the s3shim port (9092). Internal membership /
  metrics / admin keep using the existing headless `shelf` svc

The existing `shelf` headless service is **unchanged**. shelfd's
membership resolver continues to discover peers via `shelf.shelf.svc`
per ADR-0001.

### (b) Flip Trino's `s3.endpoint` per replica

Per-replica MRs in `deployments-repo`:

| Replica | Before | After |
|---|---|---|
| trino-replica-0 | `shelf-0.shelf.shelf.svc:9092` (currently reverted to S3) | `shelf-pool.shelf.svc:9092` |
| trino-replica-1 | `shelf-1.shelf.shelf.svc:9092` (currently reverted-in-flight) | `shelf-pool.shelf.svc:9092` |
| trino-replica-2 | `shelf-2.shelf.shelf.svc:9092` (live) | `shelf-pool.shelf.svc:9092` |
| trino-replica-3 | direct S3 (never cut over) | `shelf-pool.shelf.svc:9092` |

The shim sees identical traffic to before; **the router code in
shelfd already does HRW resolution internally**, so a request hashed
to `shelf-3` that lands on `shelf-1`'s pool slot will be forwarded /
served-from-cache identically to today. No router changes needed.

### (c) Add `minReadySeconds: 30` to StatefulSet

```diff
 spec:
   serviceName: {{ include "shelf.fullname" . }}
   replicas: {{ .Values.replicaCount }}
   podManagementPolicy: {{ .Values.statefulset.podManagementPolicy }}
+  minReadySeconds: {{ .Values.statefulset.minReadySeconds | default 30 }}
   updateStrategy:
     type: {{ .Values.statefulset.updateStrategy.type }}
```

This forces the StatefulSet controller to wait 30s after a new pod
becomes Ready before starting to terminate the next pod in the
rollout. Combined with the existing `OrderedReady` policy, the
sequence becomes:

```
T+0   shelf-0 SIGTERM (readinessProbe immediately fails ⇒ svc removes endpoint)
T+0   shelf-0 lameduck drain (SHELF-20: deregister, drain inflight)
T+~5  shelf-0 exits (depending on inflight)
T+~5  shelf-0 (new image) container starts
T+~25 shelf-0 readinessProbe passes ⇒ svc adds endpoint
T+55  sts controller observes ready≥30s, moves to shelf-1
... and so on
```

Total trino-visible footprint per restart: 1/N of cache cold for
~25–55s, **zero failed requests** because the svc removes shelf-N
*before* the SIGKILL.

### Optional polish (defer to follow-up)

- `preStopHook` invoking `/admin/drain` to enter lameduck *before*
  readiness flips — currently SHELF-20's drain is triggered by SIGTERM,
  which is fine, but a preStop hook would make the readiness flip
  deterministic at exactly T+0.
- `topologyAwareHints` on the new svc to prefer same-AZ shelf pod for
  trino requests (cheap egress, lower p99). Not free — it adds
  observability complexity.

## What this does **not** change

- StatefulSet ordinals, NVMe PVCs, pod identity
- shelfd's internal HRW + membership resolver (still uses headless svc)
- SHELF-20 lameduck drain protocol (still triggered by SIGTERM)
- ConfigMap / image tag / appVersion immutable-field rules
- Per-pod admin / stats endpoints (still reachable via per-pod DNS for
  shelfctl, debug, monitoring)

## Rollout plan

1. Land chart change (svc + minReadySeconds + values.yaml stub)
2. `helm template | diff` against current rendered manifests in
   `alluxio` namespace — verify only ADD operations (new svc), no
   immutable-field touches on the StatefulSet
3. `helm upgrade` the chart in `alluxio` ns. Service object is added,
   StatefulSet picks up `minReadySeconds` (NOT an immutable field —
   safe to update on existing sts; verified in k8s docs).
4. Verify DNS: `kubectl run -n alluxio --rm -it dnsutil --image=infoblox/dnstools -- dig shelf-pool.cache.svc.cluster.local`
5. End-to-end probe: `curl -X HEAD http://shelf-pool.cache.svc.cluster.local:9092/<bucket>/<key>` (should hit any of the 3 pods, response is identical because shelfd routes internally)
6. Open MRs to flip `s3.endpoint` for each trino replica (4 MRs)
7. Merge per-replica MRs one at a time, verify zero query failures
   during the resulting trino-coord restart
8. Trigger a deliberate test restart of one shelf pod (e.g. `kubectl
   delete pod shelf-2`) and confirm: ≤ 0 trino query failures, brief
   bump in `shelf_origin_request_seconds_count` for the keys hashed
   to shelf-2.

## Rollback

- Per-rep `s3.endpoint` flip: revert the deployments-repo commit; ArgoCD
  reconciles in 3-5 min; coord must be restarted to pick up the change
  (same as today).
- Chart svc add: `kubectl -n alluxio delete svc shelf-pool`. Idempotent.
- `minReadySeconds`: editable in-place; set to `0` to revert to old
  behaviour without an sts recreation.

## Capacity implication

A trino request that lands on shelf-N for a key whose HRW primary
owner is shelf-M (M ≠ N) costs:

- shelf-N internal lookup (~1ms) → hit local cache OR
- shelf-N → shelf-M peer call (~1ms, h2 keep-alive) OR
- shelf-N → S3 fetch (~30-80ms one-shot, then admitted to shelf-N's
  cache)

Under steady state, requests already go through the right pod ~1/N of
the time naturally, so this is "make existing fallback path the common
path". Net effect on aggregate p50/p99 should be ≤ +5% if the HRW
function is even, ≤ +10% if it's not.

If we observe sustained imbalance, SHELF-20c (plugin-side rebalance)
becomes the followup — the plugin can pre-route requests to their HRW
primary so the new svc is just a fallback when the primary is unavailable.

## Per-replica capacity sizing (TBD — separate work)

This design does not size the pool. Per-replica capacity numbers
(physical_input_bytes peak, peak_memory_bytes p99, qps) need to be
gathered from `your_query_log_table` per replica before we
decide whether 3 pods is enough or we need 4-6. That work is separate
from SHELF-22.

## Test plan

- `cargo test -p shelfd --test it_router_*` — already passes; HRW path
  is the same as today. No new code.
- Local kind cluster: deploy chart with replicas=3, verify
  `shelf-pool` svc resolves to all 3 pod IPs in round-robin.
- `kubectl rollout restart sts/shelf` and run a sustained
  `hey -z 60s -c 10 -m HEAD http://shelf-pool/...` probe. Expect
  zero non-2xx responses across the rollout window.

