# SHELF-G4 — `ShelfFilterService`

> Status: **scaffolded**.
> Ships: `shelfd::filter_service::ShelfFilterService` +
> `POST /filter/probe` HTTP endpoint.
> Pending: gRPC server (behind a `grpc` feature), G2 bloom
> provider, D3 native-index provider.

## Surface

- Proto IDL: `shelfd/proto/shelf_filter.proto`.
- REST today: `POST /filter/probe` — the same JSON body as the
  proto one-of, returned verbatim as
  `{ maybe_match: [...], fail_open: bool }`.
- Latency budget: 5 ms per probe.

## Routing

```
request (table, column, predicate)
    │
    ▼
consult G3 TableTag
    │
    ├─ column is clustered ──▶ NativeIndex (G1/D3) ──▶ {maybe_match}
    │                                   └─ no data ───▶ try bloom
    │
    ├─ predicate is Equal / InList ──▶ SideBloom (G2) ──▶ {maybe_match}
    │                                         └─ no data ─▶ try native
    │
    └─ everything else ──▶ NativeIndex once more (page-index cached?)
                                   └─ no data ──▶ fail_open = true
```

Fail-open is a first-class outcome: an empty `maybe_match` plus
`fail_open: true` means "shelf has no signal, do the full scan".
The G5 Trino plugin treats those two fields together — it only
drops splits when `fail_open: false` and `maybe_match` is a
non-universal subset.

## What ships vs what doesn't

- ✅ Signal-agnostic routing; trait objects for `NativeIndex`,
  `SideBloom`, `TableTagProvider`.
- ✅ REST endpoint, wired into `ServerState::with_filter_service`.
- ✅ Unit tests for the four routing paths.
- ⏳ gRPC server — parked behind the `grpc` feature. The proto
  is canonical; wiring tonic is mechanical once G5 demands it.
- ⏳ `NativeIndex` impl against the D3 page-index cache.
- ⏳ `SideBloom` impl — that's G2.

## Why REST first

G5 is the only caller we know of today, and the Trino event
listener plugin already has a `reqwest`-style HTTP client in
scope. Adding `tonic` to the hot plan path would force the
plugin process to pull in protobuf codegen at build time on
every dev laptop. We revisit once traffic justifies the
round-trip overhead.

## Test plan

- `fails_open_when_no_signals` — no providers → universe.
- `clustered_column_hits_native_index_first` — G3 tag routes to
  native index, and `SideBloom` is not consulted.
- `unclustered_equality_escalates_to_bloom` — equality without a
  G3 hint falls through to the bloom path.
- `range_predicate_skips_bloom_path` — ranges go straight to the
  native index; blooms are not asked.
