# SHELF-34 — handoff

| Field           | Value                                                                       |
|-----------------|-----------------------------------------------------------------------------|
| Ticket id       | SHELF-34                                                                    |
| Title           | page-index aware fetching + `/predicate-prune` sidecar                       |
| Plan reference  | `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md` (P1 lever) |
| Status          | **DELIVERED — awaiting orchestrator merge + sidecar threat-model sign-off** |
| Branch          | `shelf-34-page-index` (worktree at `/private/tmp/shelf-34-page-index`)       |
| Base SHA        | `dae78b6` (origin/main)                                                      |
| Version         | `1.0.0-rc.6`                                                                |

## Files modified / added

```
shelfd/src/parquet_meta.rs                                  rewritten — page-index extraction, validate_path, PageIndexCache, predicate_prune
shelfd/src/http.rs                                          + GET /predicate-prune handler, ServerState allowlist + cache, record_predicate_outcome helper
shelfd/src/metrics.rs                                       + 4 series (predicate_prune_requests_total, _seconds, page_index_cached_bytes, _parse_seconds)
shelfd/Cargo.toml                                           + parquet workspace dep (no default features) + writer dev-dep for tests
shelfd/docs/metrics.md                                      SHELF-34 series + operator notes table
shelfd/tests/it_predicate_prune.rs                          new — 5 tests (4 offline, 1 SHELF_INTEGRATION-gated MinIO E2E)
agents/out/adr/0014-page-index-predicate-prune-sidecar.md   ADR-0014 (design + parquet-crate API deviation note)
agents/out/SHELF-34/THREAT_MODEL.md                         5-item review with concrete shelfd/src/parquet_meta.rs:LINE refs
agents/out/SHELF-34/handoff.md                              this file
docs/integrations/trino-predicate-prune.md                  Trino-side wiring sketch (~120 lines, no code shipped)
Cargo.toml + Cargo.lock + charts/shelf/Chart.yaml           1.0.0-rc.5 → 1.0.0-rc.6
```

## Validation

```
cargo fmt --all -- --check                                          clean
cargo clippy -p shelfd --all-targets -- -D warnings                 0 warnings
cargo test -p shelfd --lib                                          256 passed; 0 failed; 0 ignored
SHELF_INTEGRATION=1 cargo test -p shelfd --test it_predicate_prune  5 passed; 0 failed; 0 ignored (0.23 s wall)
```

The MinIO-gated test (`end_to_end_predicate_prune_against_minio`)
exercised `tests/docker-compose.yml` MinIO at `127.0.0.1:9000` and
asserted (a) 200 OK, (b) `pages: [[offset, length], ...]` shape, (c)
PII containment (no `min`, `max`, `stats`, `readable_metrics` in the
JSON body), (d) cache-hit on the second call.

## Sidecar security checklist (per plan §)

| # | Item                          | Evidence                                                                  |
|---|-------------------------------|---------------------------------------------------------------------------|
| 1 | Path-traversal containment    | `validate_path()` + 9 negative tests + `THREAT_MODEL.md` §1               |
| 2 | Footer-parse DoS              | `MAX_FOOTER_BYTES`/`MAX_BLOB_COUNT`/`MAX_PAGE_INDEX_ENTRIES` + `THREAT_MODEL.md` §2 |
| 3 | Negative-cache poisoning      | Cache-write only on `Ok(idx)`; mirrors `head_lru::NEGATIVE_TTL_DEFAULT`; `THREAT_MODEL.md` §3 |
| 4 | PII leak containment          | Response shape is `(offset, length)` only; integration test asserts; `THREAT_MODEL.md` §4 |
| 5 | ADR + THREAT_MODEL            | `agents/out/adr/0014-…md`, `agents/out/SHELF-34/THREAT_MODEL.md`         |

## Rollback signals (verbatim from the plan)

- `shelf_origin_request_bytes_total` rate up `> 20%` vs pre-cutover for
  `> 10 min` (sidecar misroute) → disable sidecar.
- Sidecar 5xx rate `> 1%` for `> 5 min` → disable sidecar.

## Open follow-ups

1. **Trino-upstream Iceberg patch consuming `/predicate-prune`.** The
   sketch lives at `docs/integrations/trino-predicate-prune.md`; the
   real patch wires `IcebergSplitSource` (or a `ParquetReaderProvider`
   wrapper) to call the endpoint and feed the kept-pages list into
   `setSelectedPages(...)`. Iceberg PR #15211 + Trino #24007 (closed)
   are the upstream context.
2. **Operator populates the bucket allowlist.** OSS default is empty;
   operators ship a `sidecar-allowlist.toml` (or equivalent) through
   their out-of-tree overlay path and wire it into
   `ServerState::with_predicate_allowlist`.
3. **Cluster smoke after merge.** Pre-warm one Trino replica's `cdp`
   catalog via the existing pin-list replay, exercise `/predicate-prune`
   from the cluster, watch `shelf_predicate_prune_requests_total{outcome="error"}`
   and `shelf_origin_request_bytes_total` for the rollback signals.
4. **Multi-column predicate intersection.** v1 ships one column per
   request. Iceberg PR #10090 motivates a future multi-predicate
   variant — additive endpoint, not a wire break on this one.

## Hard rule reminders

- Draft PR. Do **not** auto-merge. Orchestrator merges.
- The plan file was not edited.
- All work confined to `/private/tmp/shelf-34-page-index`. No other
  worktrees touched.
- Bucket allowlist is config-loaded; no penpencil identifiers in
  `shelfd/src/`, `shelfd/docs/`, `docs/integrations/`, or any
  OSS-tracked file outside `infra/penpencil/**` / `agents/**` /
  `docs/rollout-v1/**`.
