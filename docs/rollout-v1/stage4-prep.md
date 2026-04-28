# Stage 4 + startup-probe fix — prep (next-session ready)

> Branch: `shelf-conductor-stage4-prep` (worktree `/private/tmp/shelf-stage4-prep`).
> Commits NOT pushed; chart change is local. Apply this when:
> 1. Both Agent C's MR (SHELF-23) and Agent F's MR (SHELF-25) have merged.
> 2. The combined `0.1.0-preview-9` image is in the registry and verified.
> 3. The session helm-budget for the day is fresh.
> 4. We're outside 09:00–11:00 IST peak.

## What this does

Two independent changes folded into one helm upgrade:

1. **Stage 4 sizing** (per `capacity-refresh-2026-04-28.md` recommendation):
   - `replicaCount`: 3 → 4 (one-pod headroom during rolling restart)
   - `origin.pool.maxConnections`: 256 → 512 (combined-peak burst budget)
2. **shelf-startup-probe-fix** (fast-follow from rev 23 rolling-restart):
   - Adds a `startupProbe` (5 min grace, 60 × 5 s) before liveness kicks in
   - Resolves the race where Foyer NVMe recovery (~64 s observed) bled past liveness's 75 s grace and produced 2 in-pod restarts on shelf-1 during the rev-23 rollout

## Files changed (chart)

- `charts/shelf/values.yaml` — adds `probes.startup` block with sane defaults (failureThreshold=60 = 5 min; set to 0 to disable for rollback)
- `charts/shelf/templates/statefulset.yaml` — gated `startupProbe` block ahead of livenessProbe; only renders when `probes.startup.failureThreshold > 0`

Render-validated:

```
$ helm template shelf charts/shelf -n alluxio --kube-version 1.30 | grep -A8 startupProbe
          startupProbe:
            httpGet:
              path: /healthz
              port: data
            initialDelaySeconds: 5
            periodSeconds: 5
            timeoutSeconds: 3
            failureThreshold: 60
```

Backwards-compatible: existing deployments without `probes.startup` set keep the legacy behaviour.

## Files changed (values overlay)

`/tmp/shelf-values-stage4.yaml` (NOT in chart repo — lives in conductor's `/tmp` and is the values file fed to `helm upgrade`).

Diff vs the rev-23 values overlay (`/tmp/shelf-values-new.yaml`):

```diff
-replicaCount: 3
+replicaCount: 4

 origin:
   pool:
-    maxConnections: 256
+    maxConnections: 512

 image:
-  tag: 0.1.0-preview-8
+  tag: 0.1.0-preview-9     # SHELF-23 + SHELF-25 combined image

+# explicit startup-probe defaults
+probes:
+  startup:
+    failureThreshold: 60
+    periodSeconds: 5
+    initialDelaySeconds: 5
+    timeoutSeconds: 3
```

## Application sequence (next session)

```bash
# 0. Verify pre-conditions
kubectl -n alluxio get sts shelf -o jsonpath='{.spec.replicas}{"\n"}'   # expect: 3
kubectl -n alluxio get endpoints shelf-pool                              # expect: 3 endpoints
kubectl get cm -n alluxio shelf-shelfd -o jsonpath='{.data.shelfd\.yaml}' \
  | grep -i admission_bytes_per_sec                                       # expect: empty (picker still off)

# 1. Verify combined preview-9 image exists
docker buildx imagetools inspect \
  registry.gitlab.com/penpencil-services/data/data-engineering/ranger/shelfd:0.1.0-preview-9

# 2. Land the chart change (cherry-pick or merge)
cd /Users/aamir/trino/shelf
git fetch
git checkout rep2-shelf-integration  # or whichever branch is the deployment source
git cherry-pick <commit-sha-from-shelf-conductor-stage4-prep>

# 3. Apply
helm upgrade shelf charts/shelf -n alluxio \
  --values /tmp/shelf-values-stage4.yaml \
  --wait --timeout 10m

# 4. Verify
kubectl -n alluxio get sts shelf -o jsonpath='{.spec.replicas}{"\n"}'   # expect: 4
kubectl -n alluxio get pods -l app.kubernetes.io/name=shelf             # expect: shelf-0..3
kubectl -n alluxio get endpoints shelf-pool -o jsonpath='{.subsets[0].addresses[*].ip}{"\n"}'  # expect: 4 IPs
kubectl -n alluxio describe pod shelf-3 | grep -A5 Probes               # expect: startupProbe present

# 5. Smoke
curl -sS http://<any-pod-IP>:9090/healthz
```

## Rollback

If the new pod hangs or anything goes sideways:

```bash
helm rollback shelf 23 -n alluxio --wait --timeout 5m   # back to rev 23 (3 pods, no startupProbe)
```

Or surgical (chart-only, keep replicaCount=4):

```bash
# Re-apply rev-23 values + replicaCount=4 to keep new pod, drop startupProbe
yq -i 'del(.probes.startup)' /tmp/shelf-values-stage4.yaml
helm upgrade shelf charts/shelf -n alluxio --values /tmp/shelf-values-stage4.yaml
```

## Confirmation criteria (post-apply)

- All 4 pods reach `Ready` within 5 min
- `startupProbe` appears in `kubectl describe pod` output for at least one new pod
- Zero pod restarts during the upgrade (rev-23 had 2 spurious restarts on shelf-1)
- `shelf-pool` endpoints = 4 IPs after settle
- Nothing about `aws-chunked` failures in shelfd logs (sanity that SHELF-25 fix is live)

## Coordination notes

- Agent F's preview-9 image and Agent C's preview-9 image must be the **same** image (one MR contains both fixes). If C and F land separately, the bumps need to be sequenced — preview-9 = whichever lands first, preview-10 = the other. Verify Cargo.toml + Chart.yaml at the time of build.
- The chart change here is on `shelf-conductor-stage4-prep`. It needs to be folded into the deployment-source branch before `helm upgrade` will pick it up.
