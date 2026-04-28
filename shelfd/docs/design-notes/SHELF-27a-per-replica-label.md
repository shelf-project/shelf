# SHELF-27a — Per-replica `replica` label on shelfd metrics

**Status**: design complete; implementation **scoped for the rollout
pre-req window** (Day 0-2 of `docs/rollout-v1.md` §9 timeline).
**Owner**: shelf-core
**Decided**: 2026-04-24
**Blocks**: the `replica` cut on `charts/shelf/grafana/dashboards/shelf-read-path.json`
and the `by (replica)` alert grouping in `charts/shelf/grafana/alerts/shelf-read-path.yml`
produce meaningful per-replica signal only after this work lands.

## TL;DR

The compressed-canary rollout needs per-replica observability
(dashboards + alerts) so that rep-2 canary degradations don't
silently mask rep-0/1/3 behaviour once multiple replicas share a
shelfd pool. We do **not** want to pay for per-replica shelfd
StatefulSets; HRW routing across a single pool is the point of
shelfd. The minimal wiring is:

1. Each Trino replica's `iceberg.properties` sets a custom S3 header
   `X-Shelf-Client-Replica: rep-N` via `s3.headers.x-shelf-client-replica`
   (AWS SDK v2 honours arbitrary headers via this configuration
   surface — verified against Trino 480's IcebergS3FileSystemFactory).
2. `shelfd`'s S3 shim (`shelfd/src/s3_shim.rs`) extracts the header
   from the incoming request in `handle_get_object` and
   `handle_head_object`, validates against the allow-list
   `{rep-0, rep-1, rep-2, rep-3, ""}` (empty = unknown client, NOT
   Trino), and passes the value through to the metrics macros.
3. Every `shelf_hits_total`, `shelf_misses_total`,
   `shelf_request_seconds`, `shelf_head_hits_total`,
   `shelf_head_misses_total`, `shelfd_error_total`,
   `shelf_pinned_bytes`, and `shelf_singleflight_followers_total`
   histogram / counter gains a `replica` label.

Total estimated effort: **~150 LOC + 4 tests + metrics-dict update**
= 1 dev-day.

## Why not per-replica StatefulSets

Four StatefulSets × 5 pods each = 20 shelfd pods; plus
four separate NVMe working sets (no HRW sharing); plus four
separate pin lists; plus four sets of `values.yaml` overlays.

Running-cost multiplier: ~4×. For an observability feature.
Rejected.

## Why not per-pod labels as a proxy

Pod labels (`pod="shelfd-2"`) are already on every metric — they
carry the shelfd-side identity but not the Trino-replica-side
identity. HRW means any Trino replica's request can land on any
shelfd pod, so `pod` and `replica` are orthogonal dimensions.
Using `pod` as a replica proxy is wrong.

## Why the header is added Trino-side, not derived

We considered:

1. Source-IP sniffing in shelfd. Brittle; fails under sidecar
   proxies, service mesh, IPv6 rollout.
2. User-agent suffix (Trino's `s3.user-agent-suffix` config). Works
   but expensive to parse on every request; structured header is
   cleaner.
3. Per-Trino-replica catalog names (`iceberg_rep0`, etc.). Catalog
   name isn't carried in the S3 URL, so shelfd cannot see it.

The custom header wins on: free-on-the-wire (h2 header compression),
trivially validated against an allow-list, and zero change to the
request path — it's just an additional header that shelfd's existing
`HeaderMap` extraction in s3_shim already has access to.

## Trino-side change

Per-replica `iceberg.properties` gains exactly one line:

```properties
s3.headers=x-shelf-client-replica:rep-2
```

Validated on Trino 480: `IcebergS3FileSystemFactory` forwards
`s3.headers` (comma-separated `k:v` pairs) to the AWS SDK HTTP
client as permanent request headers. Pre-existing smoke tests for
other custom-header use cases confirm the plumbing works
transparently with native S3 and with shelfd's S3 shim.

Single source of truth for the replica IDs: the rollout runbook in
`docs/rollout-v1.md` pins the exact values. Shelfd validates
against that same enum.

## shelfd-side change

Two touch points in `shelfd/src/s3_shim.rs`:

```rust
// New helper, in s3_shim.rs:
pub(crate) fn extract_replica(headers: &HeaderMap) -> &'static str {
    match headers.get("x-shelf-client-replica").and_then(|v| v.to_str().ok()) {
        Some("rep-0") => "rep-0",
        Some("rep-1") => "rep-1",
        Some("rep-2") => "rep-2",
        Some("rep-3") => "rep-3",
        _ => "unknown",
    }
}
```

Returning `&'static str` from an allow-list avoids Prometheus
cardinality explosion from arbitrary header values (a malicious or
misconfigured client sending `X-Shelf-Client-Replica: <random>` can't
create new label values). `"unknown"` is a single additional label
value covering everything off-allow-list.

Metrics-macro surface becomes:

```rust
counter!("shelf_hits_total", "pool" => pool.as_str(), "replica" => replica).increment(1);
```

Both `handle_get_object` and `handle_head_object` compute `replica`
once at entry and pass it through. The `store::FoyerStore::get_or_fetch`
path doesn't need to know about replicas; only the metric emission
sites do.

## Metrics-dict (`shelfd/docs/metrics.md`) update

Every counter / histogram listed in the "Read path" section gains a
`replica` label. Allowed values are exactly:

- `rep-0`, `rep-1`, `rep-2`, `rep-3` — example Trino replicas
  per `docs/rollout-v1.md`.
- `unknown` — any client that didn't set `X-Shelf-Client-Replica`
  (S3-compat shim users, shelfctl, smoke tests, malformed headers).

Cardinality cost: 5 label values × existing dimensions. Prometheus
series count rises by 5× on each shelf-metric that didn't already
have a similar-cardinality label. At current scale (<100 series per
metric) this is ~500 series per metric, well within the kube-prom
stack's default limits.

## Test plan

Four unit tests in `shelfd/src/s3_shim.rs`:

- `extract_replica_accepts_allowlist` — each of the four values returns verbatim.
- `extract_replica_returns_unknown_for_missing` — no header → `"unknown"`.
- `extract_replica_returns_unknown_for_bogus` — header value
  `"attacker-injected-rep-9999"` → `"unknown"`.
- `handle_get_object_emits_replica_label` — full request flow,
  asserting counter label via `prometheus::gather()`.

One integration test under `shelfd/tests/`:

- `shim_metrics_carry_replica_label` — variant of the existing
  `shim_read_bumps_hits_and_misses_counters` test; sends the header
  and asserts the `replica` label appears in the Prometheus text
  output with the expected value.

## Rollout sequencing

Because `docs/rollout-v1.md` mandates the dashboard + alerts
replica-cut as a Day 0-2 pre-req, this ticket sits on the pre-req
critical path:

1. **Day 0**: Land this ticket (shelfd PR + metrics-dict update);
   release new shelfd image tag.
2. **Day 1**: Scale up the StatefulSet with the new image (rolling
   restart, `maxUnavailable=1`). `unknown`-labelled series are what
   we see at this point — no Trino replicas carry the header yet.
3. **Day 2**: Per-replica Trino `iceberg.properties` PRs land the
   header one at a time, in the same order as the cutover sequence
   (rep-2, rep-0, rep-1, rep-3). Each replica's pod-roll is
   observationally identical to the cutover it's about to
   receive — a good dress rehearsal.
4. **Day 3+**: Dashboard + alerts now produce per-replica signal.
   rep-2 cutover can begin.

The header landing ≤ 24h **before** each replica's actual
`s3.endpoint` flip is important: it gives us per-replica
baselines on direct-to-S3 traffic, so the cutover's delta is
visible immediately.

## Re-opening criteria

- Label cardinality exceeds 5 (we add a replica, merge clusters,
  etc.) — add to the allow-list enum; no rearchitecture needed.
- We decide to split shelfd pools per replica after all — then the
  `replica` label becomes static per-pool and we can retire this
  plumbing (unlikely; see "Why not per-replica StatefulSets").

## References

- `charts/shelf/grafana/dashboards/shelf-read-path.json` — queries
  already filter by `{replica=~"$replica"}`.
- `charts/shelf/grafana/alerts/shelf-read-path.yml` — rules already
  group `by (replica)`.
- `docs/rollout-v1.md` — canonical replica ID list and rollout
  sequence.
- `shelfd/docs/metrics.md` — metric label surface, to be updated
  with this ticket.
- `shelfd/src/s3_shim.rs` — implementation site.
