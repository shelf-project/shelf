---
ticket: rc9-T6
date: 2026-05-04 IST
status: closed — schema gap referenced by workspace memory IS already resolved on `origin/main`
---

# T6 — Pin-list schema unification: scope confirmation

## TL;DR

The "tools/gen_pin_list.py /admin/pin schema gap" referenced by workspace memory **has already been resolved**. `gen_pin_list.py` at `origin/main` emits a document that is byte-for-byte compatible with the `PinListDoc` struct consumed by the in-shelfd pin-list S3-polling loader. The original 150-LOC bridge work the analyst's "0.5 days top-5 pinning" assumed is unnecessary.

The pinned-protection path that the operator should use is the S3-polling pin-list loader (`pin_list.s3Uri`), NOT the per-key `/admin/pin` HTTP endpoint. The endpoint is correctly designed as single-key (admin debug surface), and `gen_pin_list.py` correctly targets the batch loader.

## What I verified

1. `tools/gen_pin_list.py::build_pin_list` returns:
   ```python
   {"version": 1, "entries": [{"key_hex": "...", "pool": "metadata"}, ...]}
   ```
   (verified at lines 268–274; format described in the file's module docstring.)

2. `shelfd/src/pinlist.rs::PinListDoc` deserializes:
   ```rust
   pub(crate) struct PinListDoc {
       pub(crate) version: u32,
       pub(crate) entries: Vec<PinListEntry>,
   }
   ```
   `PinListEntry` carries `key_hex` + `pool` — matching the Python output exactly.

3. `shelfd/src/http.rs::admin_pin` (POST `/admin/pin`) takes `PinEvictBody`:
   ```rust
   pub struct PinEvictBody {
       pub key_hex: String,
       pub pool: String,
       pub mv_name: Option<String>,
   }
   ```
   This is a **single-key** endpoint. It is not designed to take the batch `PinListDoc` shape, and there's no operational reason it should — the S3-polling loader is the correct batch path.

## Operator path for the analyst's "pin top-5 tables" recommendation

This is the working procedure today, no code change required:

```bash
# 1. Run gen_pin_list.py with --top-n=5 and target the cluster's pin-list bucket.
#    On the operator workstation with IRSA / AWS creds:
python3 tools/gen_pin_list.py \
    --trino-url  http://trino-replica-2.example.org:8080 \
    --trino-user dbt_user \
    --top-n      5 \
    --output     s3://example-cdp-temp/shelf/pin_list.json

# 2. Verify the chart's pin_list config is enabled and points at that bucket
#    in the per-replica overlay (cache.pinList.s3Uri must be set; today most
#    overlays leave it empty so pin_list.enabled=false at runtime — visible
#    in shelfd startup logs as "pin-list loader disabled by config").

# 3. Either wait for the next 15-min poll cycle, or SIGHUP shelfd to force
#    immediate reload:
kubectl -n alluxio exec shelf-0 -c shelfd -- kill -HUP 1
```

## Why the workspace-memory gap reference is now stale

Workspace memory line referenced *"its output is a 'replay-list' schema (manifest entries with bucket/key/etag)"*. The current `build_pin_list` does NOT emit that shape — it emits hex-encoded SHA-256 cache keys (the same format the in-cluster loader expects per ADR-0011). At some point between the workspace-memory entry and `origin/main`, `gen_pin_list.py` was refactored to compute the keys client-side and emit the strict `PinListDoc` format. The git log confirms `tools/gen_pin_list.py` is one of the recent additions (no prior format exists in history), so this is "shipped at first commit", not "refactored later".

## Action

- No new ticket. T6 closed.
- Recommend: add a short paragraph to `docs/runbook.md` under "pin-list operations" pointing operators at this exact procedure so the gap doesn't get re-discovered.

The analyst's "0.5 days for top-5 pinning" is essentially **0.0 days for code** + **0.5 days for the operator-side gen_pin_list.py run + chart values flip in deployments-repo** — already actionable today on every cluster.
