# ADR 0001: No embedded Raft for Shelf control plane

_Status: Accepted (planner amendment, 2026-04-23)_
_Deciders: eng-lead, scientist agent §4.10, critic §3 + §7_

## Context

The v0.3 blueprint puts `openraft` inside each `shelfd` pod to store
ring membership, pin list, and tenant quotas. openraft is pre-1.0 and
has a non-trivial operational surface (election storms, snapshot
chunking, RocksDB storage dependency). The team has never shipped a
Rust service in production, and Alluxio's own Raft master-quorum has
caused us a real outage (`POST_MORTEM.md`, 50+ `ICEBERG_COMMIT_ERROR`s
on master restart). A data-plane cache that is fail-open by
construction does not need strongly-consistent cluster state.

## Decision

Delete the openraft dependency from v1. Cluster state lives in three
places:

1. **Membership** — K8s headless service `shelf.shelf.svc.cluster.local`.
   Plugin resolves every 5 s; `shelfd` pods trust the DNS answer.
2. **Pin list + tenant quotas** — S3-backed versioned ConfigMap (a
   plain JSON file at `s3://config-bucket/shelf/pin_list.json`),
   pulled on SIGHUP or every 15 min. Trainer writes the next version;
   ops reviews diffs via PR before publication.
3. **Admission model (if ever shipped)** — same S3 path.

One pod may be elected "coordinator" via a K8s lease lock for jobs
that must run once cluster-wide (e.g. trainer ingest), but no
consensus is required on the hit path.

## Alternatives considered

- **Keep `openraft`.** Gives atomic multi-key updates. Rejected: no
  requirement for atomic multi-key updates in v1; cost is a pre-1.0
  dependency + RocksDB + new on-call failure mode.
- **3-node etcd sidecar.** Rejected: moves the problem, adds a second
  dependency, still gives us a consensus system to operate.
- **Gossip/CRDT (Riak-style).** Rejected: larger surface than DNS
  lookup for a system where membership changes are rare (pod rotation,
  not per-request).

## Consequences

- **Positive.** No Rust Raft crate in the build; no election storms;
  no snapshot chunking; operationally identical to every StatefulSet
  we already run.
- **Negative.** Lose atomic multi-key updates (e.g. "pin 7 tables in
  one transaction"). Mitigated: pin list is pulled whole on each
  reload — it is atomic at the JSON-file level.
- **Neutral.** If we ever discover a requirement for strongly
  consistent cluster state in Phase 5+, this ADR can be superseded.
- **Guardrail.** Codeowners reject any PR that adds `openraft` or
  `raft` crate to `Cargo.toml` without a superseding ADR.
