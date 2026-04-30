# SHELF-34 — `/predicate-prune` sidecar threat model

Status: **complete** (line numbers refer to commits on branch
`shelf-34-page-index`; refresh after rebases).
Reviewer sign-off: pending orchestrator merge.
Authoritative source files referenced below:

- `shelfd/src/parquet_meta.rs`
- `shelfd/src/http.rs`
- `shelfd/src/head_lru.rs` (cross-reference for §3, negative-cache policy)

The five items in this document are the merge-blocker checklist
called out by the SHELF-34 lever-detail block in
`/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md`
under *§ Sidecar security review*. ADR-0014 records the design
rationale; this file is the implementation evidence.

---

## 1. Path-traversal containment

**Risk.** A caller submits a `path=` query parameter pointing at a
bucket the operator did not authorise, or sneaks `..` segments through
to make shelfd open arbitrary keys.

**Containment.** All `/predicate-prune` requests pass through
`validate_path()` *before* shelfd touches the origin. The validator
strips the `s3a://` or `s3://` scheme, splits bucket/key, then runs
each of the rules below in order, short-circuiting on the first
failure. The allowlist is operator-supplied (default OSS = empty); the
endpoint is therefore *closed by default* on a stock OSS build.

| Rule                                | Code reference                                 |
|-------------------------------------|-------------------------------------------------|
| Scheme must be `s3a://` or `s3://`  | `shelfd/src/parquet_meta.rs:599-602`            |
| Empty bucket / empty key rejected   | `shelfd/src/parquet_meta.rs:608-613`            |
| Bucket must be in operator allowlist | `shelfd/src/parquet_meta.rs:614-616`            |
| Key may not begin with `/`          | `shelfd/src/parquet_meta.rs:617-619`            |
| Key bytes contain no NUL (`\0`)     | `shelfd/src/parquet_meta.rs:620-622`            |
| No `..` segment in the key path     | `shelfd/src/parquet_meta.rs:623-627`            |
| HTTP handler invokes the validator first thing | `shelfd/src/http.rs:1535-1545` |

The OSS default allowlist is the empty `Vec` initialised at
`shelfd/src/http.rs:226`. Operators populate it via the builder
`ServerState::with_predicate_allowlist`
(`shelfd/src/http.rs:286-289`); the
`infra/penpencil/sidecar-allowlist.toml` overlay (out of tree) is the
production path.

**Negative tests** (proving each rule rejects):
- `validate_path_rejects_unscheme_path` — bare `foo.parquet`
  (`shelfd/src/parquet_meta.rs:1057-1064`).
- `validate_path_rejects_other_bucket` — bucket outside allowlist
  (`shelfd/src/parquet_meta.rs:1066-1071`).
- `validate_path_rejects_dotdot_segment` — `pw-…/../etc/passwd`
  (`shelfd/src/parquet_meta.rs:1073-1083`).
- `validate_path_rejects_absolute_key` — leading slash
  (`shelfd/src/parquet_meta.rs:1085-1090`).
- `validate_path_rejects_empty_key`
  (`shelfd/src/parquet_meta.rs:1092-1097`).
- `validate_path_rejects_nul_byte_in_key`
  (`shelfd/src/parquet_meta.rs:1099-1104`).
- `validate_path_default_oss_allowlist_rejects_everything`
  (`shelfd/src/parquet_meta.rs:1106-1111`).
- `validate_path_does_not_match_bucket_prefix`
  (`shelfd/src/parquet_meta.rs:1113-…`) — `pw-data` ≠ `pw-data-extra`.
- HTTP-level integration coverage at
  `shelfd/tests/it_predicate_prune.rs::empty_allowlist_rejects_every_path`,
  `unallowed_bucket_rejected`, and `path_traversal_rejected`.

---

## 2. Footer-parse DoS

**Risk.** A malicious or accidentally-huge Parquet footer on an
allowlisted bucket forces shelfd to allocate gigabytes (or to spin a
worker thread for minutes) inside the parser.

**Containment.** Three independent caps are checked *in order*, each
before the parser allocates the next data structure. All caps are
constants in `shelfd/src/parquet_meta.rs`:

| Cap                            | Value         | Code reference                                  |
|--------------------------------|---------------|-------------------------------------------------|
| `MAX_FOOTER_BYTES`             | 8 MiB         | `shelfd/src/parquet_meta.rs:190`                |
| `MAX_BLOB_COUNT`               | 4 096 chunks  | `shelfd/src/parquet_meta.rs:195`                |
| `MAX_PAGE_INDEX_ENTRIES`       | 65 536 pages  | `shelfd/src/parquet_meta.rs:200`                |

Enforcement points (each returns `Err(ParquetMetaError::*)`, mapped
1-to-1 to a 4xx response in the handler):

- Input-size check at `shelfd/src/parquet_meta.rs:431-436` —
  rejects before `Bytes::copy_from_slice`.
- Column-chunk count check at `shelfd/src/parquet_meta.rs:451-458` —
  rejects after metadata parse but before iterating pages.
- Page-location count check at `shelfd/src/parquet_meta.rs:469-480` —
  rejects before allocating the per-column `Vec<PageRange>`.

The HTTP handler additionally enforces the size cap *before* fetching
the object, so a huge S3 object never even touches the parser:
`shelfd/src/http.rs:1621-1635`. Parser errors map to status codes via
`shelfd/src/http.rs:1671-1690`:

- `FooterTooLarge` → `413 Payload Too Large`.
- `TooManyBlobs`, `TooManyPages` → `413 Payload Too Large`.
- `NoPageIndex` → `422 Unprocessable Entity`.
- `Parse(_)` → `502 Bad Gateway` (origin returned malformed bytes).

**Negative tests**:
- `extract_page_index_rejects_oversized_input` — synthetic
  `MAX_FOOTER_BYTES + 1` buffer (`shelfd/src/parquet_meta.rs:1026-1031`).
- `extract_page_index_rejects_garbage_input` — confirms a non-Parquet
  blob never panics (`shelfd/src/parquet_meta.rs:1033-1039`).

---

## 3. Negative-cache poisoning

**Risk.** The page-index cache is keyed by ETag; a transient origin
4xx (e.g. an IRSA-token blip returning 403) could be memoised as
"page index empty" and propagated to every subsequent caller, defeating
the cache.

**Containment.** Two complementary policies, intentionally mirroring
the SHELF-A4 / `head_lru.rs` shape:

1. **Origin errors never become positive cache entries.** In
   `predicate_prune_inner` the origin's `head` and `get_range` results
   short-circuit out of the function as `record_predicate_outcome(...,
   "error", ...)` — the in-process `PageIndexCache` is *only* written
   inside the `Ok(idx)` arm of `extract_page_index`:
   - origin.head error → `BAD_GATEWAY`, no insert
     (`shelfd/src/http.rs:1599-1606`).
   - origin.get_range error → `BAD_GATEWAY`, no insert
     (`shelfd/src/http.rs:1644-1651`).
   - parse error → mapped 4xx/5xx, no insert
     (`shelfd/src/http.rs:1666-1690`).
   - cache populate happens only here:
     `shelfd/src/http.rs:1653-1665`.

2. **`extract_page_index` surfaces every non-Ok parse as `Err`** at
   `shelfd/src/parquet_meta.rs:430` (function declaration / contract)
   and `shelfd/src/parquet_meta.rs:444-446`
   (`map_err(|e| Parse(e.to_string()))?`). There is no `Ok(empty)`
   degrade path that the cache could observe.

Cross-reference: `head_lru::NEGATIVE_TTL_DEFAULT = 5s`
(`shelfd/src/head_lru.rs:55`) is the upper bound on how long a 404
response may be cached; the page-index cache does not introduce a
parallel negative-cache path, so SHELF-34 inherits the bounded
exposure of the existing HEAD layer.

ETag is the discriminant
(`shelfd/src/http.rs:1612`); a successful `PUT` invalidates the
old key by changing the ETag — old `(etag, "page-index")` entries
become orphans that Foyer evicts on capacity per ADR-0011.

---

## 4. PII leak containment

**Risk.** Iceberg's `readable_metrics` JSON columns and Parquet
column statistics include user data (e.g. customer-name `min`, account
`max`). A naive sidecar that returns "min/max per page" would leak
these values directly to any caller hitting `/predicate-prune`.

**Containment.** The response payload is constructed at
`shelfd/src/http.rs:1700-1706` and contains only structural fields:

```json
{
  "path": "...",
  "column": "...",
  "total_pages": <u64>,
  "kept_pages": <u64>,
  "pages": [[<u64 offset>, <u64 length>], ...]
}
```

`pages` is a list of `(u64, u64)` tuples returned by `predicate_prune`
at `shelfd/src/parquet_meta.rs:535-545`. The function reads
`PageRange.min` and `PageRange.max` only for *predicate evaluation*
(see the `matches_predicate` helper) and never returns or serialises
those values. There is no `min`, `max`, `stats`, or `readable_metrics`
field in the response shape and no code path that writes one.

**Defensive test.**
`shelfd/tests/it_predicate_prune.rs::end_to_end_predicate_prune_against_minio`
parses the JSON response and asserts `min`, `max`, `stats`, and
`readable_metrics` are absent — a future change that adds any of those
fields fails the gated SHELF_INTEGRATION suite.

---

## 5. ADR + threat-model paper trail

The plan §Sidecar security review names the artefacts that must land
together:

- ADR: `agents/out/adr/0014-page-index-predicate-prune-sidecar.md`
  (next number after the SHELF-25 ADR-0013).
- THREAT_MODEL: this file (`agents/out/SHELF-34/THREAT_MODEL.md`).
- Hand-off: `agents/out/SHELF-34/handoff.md` for the orchestrator.
- Trino-side wiring: `docs/integrations/trino-predicate-prune.md`.

Both ADR-0014 and this document live in the same PR as the code that
implements them, which is the convention from ADR-0011 / ADR-0012 /
ADR-0013. Future audits can reproduce the line-number references
above by checking out the merge commit; if a refactor moves the cited
lines, it must update this file in the same PR.

---

## Out of scope for v1 (deliberate)

- **Open-ended predicates** (`col > 100` with no upper bound): the v1
  wire shape is `(min, max)` or `eq`. Open-ended forms can land later
  by adding a new query param without breaking existing callers — see
  `shelfd/src/http.rs:1556-1559` for the strict-shape rationale.
- **Multi-column predicate intersection**: clients today issue one
  request per column. Iceberg PR #10090 motivates a future
  `(col, lo, hi)[]` shape; that change ships a new endpoint, not a
  signature break on this one.
- **Authentication**: `/predicate-prune` is unauthenticated, like the
  rest of the shelfd data plane. The boundary is the cluster network
  policy + the bucket allowlist; Trino workers are the only callers.
