# Runbook: evict-poisoned-key

**Scenario:** A single key in the cache is returning corrupt or stale
bytes; force an evict + re-fetch.

## Symptom

- A user reports "wrong data" for a specific file/range despite
  Iceberg snapshot integrity.
- A per-row-group checksum has flagged a mismatch in worker logs.
- Corruption drill (`chaos/block-corruption.sh`) is being rehearsed.

## Impact

Until the key is evicted, every read for that range returns the bad
bytes. Content-addressed keys (`sha256(etag || offset || length)`)
mean the *next* request re-fetches from S3 and produces an authoritative
result — there's no silent stale-data loop.

## Diagnosis

```bash
# 1. What is the key? Derive from (etag, offset, length).
kubectl -n shelf exec shelf-0 -- shelfctl derive-key \
  --etag <ETAG> --offset <OFFSET> --length <LEN>

# 2. Which pod owns it?
kubectl -n shelf exec shelf-0 -- shelfctl ring --lookup <KEY>

# 3. Is the ETag stable? A multipart upload ETag is NOT MD5 (see
# ADR-0001 / plan risk R-10). Re-stat the object to confirm we're not
# chasing a post-rewrite key.
aws s3api head-object --bucket penpencil-cdp-prod --key <OBJECT_KEY>
```

## Mitigation

1. **Evict on the owner pod:**
   ```bash
   OWNER=$(kubectl -n shelf exec shelf-0 -- shelfctl ring --lookup <KEY> | awk '{print $1}')
   kubectl -n shelf exec $OWNER -c shelfd -- shelfctl evict <KEY>
   ```
   Next read re-fetches from S3 + re-inserts. Content-addressed so no
   risk of re-admitting the same corruption unless S3 itself is bad.
2. **If every pod has the key** (possible after a ring rebalance):
   ```bash
   for p in $(kubectl -n shelf get pod -l app.kubernetes.io/name=shelf -o name); do
     kubectl -n shelf exec $p -c shelfd -- shelfctl evict <KEY> || true
   done
   ```
3. **Nuke the NVMe** for the owner pod if eviction fails (extreme): see
   `shelf-pod-restarting.md` mitigation (2).

## Escalation

- Routine eviction: no escalation.
- Repeated corruption of different keys on the same pod → treat as
  `ShelfPodRestarting`: hardware fault suspected, rotate the node.

## Post-incident actions

- [ ] Record the key + ETag in the incident ticket.
- [ ] If S3 is serving bad bytes, open an AWS support ticket; do not
      blame Shelf.
- [ ] If a Foyer corruption path is proven, file against the pinned
      Foyer version (risk R-06).
