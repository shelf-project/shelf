# ADR 0014: Page-index aware fetching + `/predicate-prune` sidecar

*Status: Accepted (2026-04-29)*
*Deciders: rust-engineer-1, trino-plugin-eng-1*
*Supersedes: none*
*Superseded-by: none*
*Related: SHELF-D3 design note (`shelfd/docs/design-notes/SHELF-D3-page-index-bloom.md`),
ADR-0011 (cache-key spec), ADR-0012 (Trino read-path endpoint swap)*

## Context

Trino's Iceberg connector already ships predicate push-down at the
row-group level: when a query carries a comparison predicate on a
column with sort-order or min/max stats, the executor uses the
manifest-level row-group statistics to drop entire row groups before
issuing any data GETs. The lift stops at the row-group boundary; once
a row group is opened, every page in that group is decoded.

Apache Parquet 2.9 added a finer-grained pruning surface: the
**page index** ([Parquet `PageIndex` spec](https://parquet.apache.org/docs/file-format/pageindex/)).
Each Parquet file's footer points at two thrift regions per column
chunk:

- `ColumnIndex` — per-page `(min, max, null_count)` statistics.
- `OffsetIndex` — per-page `(file_offset, compressed_page_size,
  first_row_index)`.

Together they allow a reader to skip pages without decoding the
column chunk at all: compute which pages overlap the predicate's
[lo, hi] range, fetch only those byte ranges. Iceberg has been
moving in this direction upstream:

- [Iceberg #15211](https://github.com/apache/iceberg/pull/15211) —
  vectorized reader page skipping (in review).
- [Iceberg #10090](https://github.com/apache/iceberg/pull/10090) —
  multi-predicate row-group filter cooperation (Apr 2024).

A Trino-side optimization that loaded the footer's page index
once per file ([Trino #24007](https://github.com/trinodb/trino/pull/24007))
appeared to be heading in this direction but is **CLOSED, NOT MERGED**
(`mergedAt: null`, verified 2026-04-29). The optimization did not
land upstream; SHELF-34 stands on its own merits and does not depend
on a Trino release pin.

Why a sidecar in shelfd rather than a pure Trino patch:

1. **Manifest/footer reads already flow through shelfd.** When
   shelfd parses the footer to admit it into Pool::Metadata
   (the existing SHELF-D3 phase-1 contract), it has the page
   index in hand. Caching the parsed structure costs nothing extra
   per file on the warm path.

2. **Trino's `iceberg.metadata-cache.enabled` shadows shelfd's
   metadata pool** ([Trino #22739](https://github.com/trinodb/trino/pull/22739),
   default ON). This is structurally why shelfd's metadata pool
   runs at ~0.14 % hit ratio on rep-1 under load — every Iceberg
   engine ships an in-process Caffeine cache. The lift available
   to shelfd here is **not** on cached blob density of the
   manifest blobs themselves; it is on a sidecar lookup endpoint
   that Trino's reader can call to pre-compute page byte ranges
   without re-reading the footer.

3. **Coordination point for future bloom-filter / Puffin
   sidecars** (SHELF-42, SHELF-46). They will share the same path
   validator, the same threat model, the same allowlist config
   plumbing. Establishing the pattern once is cheap.

## Decision

Add a `/predicate-prune` HTTP endpoint to `shelfd`'s data plane
(port 9090 — same listener as `/cache/...`, NOT the S3 shim on 9092).

```
GET /predicate-prune?path=s3a://<bucket>/<key>&col=<name>&min=<v>&max=<v>
→ 200 application/json {"pages":[[offset, length], ...]}
→ 400 invalid path / predicate
→ 403 path outside operator allowlist
→ 404 page index unavailable for this column
→ 502 origin error
```

Implementation lives in `shelfd/src/parquet_meta.rs`. The module
keeps the existing SHELF-D3 phase-1 surface (`FooterRange`,
`FooterRangeKind`, `Extracted`, `FooterExtractor`, `NoopExtractor`,
`ExtractError`) for backward compat, and adds:

- `PageIndex` — parsed structural representation: `column_idx ->
  Vec<PageRange>` keyed by column-chunk ordinal AND column name.
- `PageRange { offset, length, min, max, null_count }`.
- `ColumnValue` — the union of value types we can compare against
  (`Int64`, `Float64`, `Bytes`, `Null`).
- `Predicate` — `{ Equals, GreaterThan(Equals), LessThan(Equals),
  Range { lo, hi } }`.
- `extract_page_index(bytes: &[u8]) -> Result<PageIndex,
  ParquetMetaError>` — uses the upstream `parquet` crate's
  `ParquetMetaDataReader::with_page_indexes(true).with_offset_indexes(true)`
  pipeline.
- `predicate_prune(idx: &PageIndex, column: &str, predicate:
  &Predicate) -> Vec<(u64, u64)>` — pure function, no I/O.
- `validate_path(path: &str, allowlist: &[String]) -> Result<S3Path,
  PathError>` — path-traversal containment, allowlist enforcement.

A small per-state LRU `parquet_meta_cache` (Foyer-backed,
key = `format!("{etag}::page-index")`) holds parsed `PageIndex`
values so subsequent prune calls on the same file are O(log N).
This is layered on top of (not replacing) the metadata pool's
byte-range cache: the parsed page index is small and structural;
the raw footer bytes still occupy Pool::Metadata under their
SHELF-04 content-addressed key.

### Cache-key alignment with ADR-0011

Per ADR-0011, every byte-range entry in either Foyer pool is keyed
by `sha256(etag || offset || length || rg_ordinal)`. The parsed
`PageIndex` is **not** a byte range — it is a parsed structure
holding `(offset, length, min, max)` tuples. We therefore key the
parsed-structure cache by a string `"<etag>::page-index"` rather
than reusing the SHELF-04 32-byte content key. This avoids
collision with byte-range entries that happen to share the same
sha256 prefix. The naming is intentional: a separate keyspace,
clearly labelled, distinct from the Foyer Pool::Metadata bytes
namespace.

### Path validation rules

`validate_path` enforces (concrete checks in
`shelfd/src/parquet_meta.rs::validate_path`):

1. Scheme must be exactly `s3://` or `s3a://`. Reject anything
   else (including bare paths, `file://`, etc.).
2. The bucket segment must be **exactly equal** (`==`) to one
   of the operator-supplied `allowlist` entries. Suffix / prefix
   matching is NOT permitted — it would let `pw-data-cdp-prod-temp-evil`
   masquerade as `pw-data-cdp-prod-temp`.
3. The key must be non-empty.
4. The key must NOT contain a `..` path component anywhere
   (rejected via segment-by-segment iteration after splitting
   on `/`).
5. The key must NOT start with `/` (which would be an absolute
   path; S3 keys are relative).
6. The key must NOT contain ASCII NUL (`\0`) — defensive against
   path-truncation surprises in downstream tooling.

The default OSS allowlist is **empty**: a freshly-deployed shelfd
will reject every `/predicate-prune` request with a 403 until
the operator populates the allowlist via the
`PARQUET_META_ALLOWLIST` env var (comma-separated bucket names)
or, in cluster overlays, via `infra/penpencil/sidecar-allowlist.toml`.

### Footer-parse DoS containment

`extract_page_index` enforces three caps before any allocation
that scales with the input (concrete numbers in
`shelfd/src/parquet_meta.rs`):

- `MAX_FOOTER_BYTES = 8 * 1024 * 1024` (8 MiB). Inputs larger than
  this are rejected with `ParquetMetaError::FooterTooLarge`.
- `MAX_BLOB_COUNT = 4096`. Total column-chunk count across all
  row groups; rejected with `ParquetMetaError::TooManyBlobs`.
- `MAX_PAGE_INDEX_ENTRIES = 65_536`. Total page locations across
  all (row_group, column) pairs; rejected with
  `ParquetMetaError::TooManyPages`.

A 1 GB malicious footer cannot OOM the pod: the size cap is
applied to `bytes.len()` before construction of the parquet
reader, and the per-(row group × column) parsing loop trips the
blob/page caps deterministically before allocation grows.

### Negative-cache discipline

Per the SHELF-A4 negative-cache safety policy mirrored from
`shelfd/src/head_lru.rs::NEGATIVE_TTL_DEFAULT` (5 s) and
`shelfd/src/origin.rs::is_persistent_forbidden_code`:

- A 4xx origin response on the GET that backs a sidecar parse
  is NOT cached as a positive result.
- A transient 403 (e.g. credential glitch, IRSA token rotation)
  surfaces as `Err`, not `Ok(None)`.
- The parsed-PageIndex cache only holds `Ok(PageIndex)` values;
  failures are not memoised at all in v1.

### PII leak containment

`/predicate-prune` returns ONLY structural mappings:

```json
{"pages": [[offset, length], ...]}
```

Never:

- the `min` / `max` values themselves (they may be user data),
- Iceberg `readable_metrics` JSON (which has been observed in
  production to leak min/max user data),
- any column name beyond what the caller already supplied,
- any bucket name or path segment beyond what the caller already
  supplied.

The `min`/`max` values from the page index are used **only**
inside the pure `predicate_prune` function to filter the list;
they never leave the daemon.

## Alternatives considered

- **Implement page-index pruning Trino-side only.** Rejected:
  every Trino worker would re-parse the footer per query; the
  in-process Caffeine cache helps the warm path but cold-pod /
  KEDA scale-out events still pay the parse cost. Sidecar-side
  parse is sharable across replicas via shelfd's HRW peer race.
  Also, Trino #24007 demonstrated upstream is not currently
  pursuing this exact angle (closed not merged).

- **Embed the parser in the S3 shim PUT path.** Rejected:
  shelfd's shim is read-mostly; PUT is a thin pass-through with
  buffering caps. Parsing on PUT would tie footer parse latency
  to a Trino-Iceberg write commit, which has no benefit (the
  predicate-prune callers are readers, not writers).

- **Use the parquet crate's `arrow` feature for richer types.**
  Rejected: pulls in `arrow-*` (~3 MB compile output, adds
  ~60 s to release CI on free runners — verified empirically
  on the rc.0 / rc.1 builds, see workspace memory). The
  `default-features = false, features = ["thrift"]` slice gives
  us page-index parsing without the arrow tree.

- **Cache the parsed page index in the Foyer Pool::Metadata
  byte-range cache by serialising it with bincode.** Rejected:
  cache-key namespace pollution. The Foyer pool keyspace is
  byte-range-only by ADR-0011; introducing a parallel parsed-
  structure value class through the same cache would force a
  bincode-version invariant that doesn't exist today and would
  invalidate cleanly across minor crate bumps.

## Consequences

- **A new dependency on `parquet 58.1.0`** (default-features = false,
  features = ["thrift"]). Compile-time impact: ~30 s additional
  release build on a clean cargo cache. Runtime size: ~1.6 MB
  added to `shelfd` static binary. Both within the 150 MiB image
  budget.

- **The `iceberg.metadata-cache.enabled` shadow remains.**
  Trino's coord-side cache will continue to shadow the metadata
  pool's *byte-range* density. This sidecar is specifically a
  *predicate prune* surface; the dashboards must read `shelf_predicate_prune_*`
  metrics, not `shelf_hits_total{pool="metadata"}`, to measure
  its value.

- **The Trino-side patch lands in a follow-up.** A `ShelfFileSystem`
  hook that calls `/predicate-prune` before the row-group reader
  iterates pages is documented in `docs/integrations/trino-predicate-prune.md`
  but not shipped in this PR. The sidecar can be deployed without
  the patch (Trino simply doesn't call it; the endpoint is
  inert), and the patch can be developed against a stable
  endpoint contract.

- **An OSS / penpencil split is introduced for sidecar config.**
  The default OSS allowlist is empty; penpencil cluster overlays
  populate the allowlist via `infra/penpencil/sidecar-allowlist.toml`
  (matching the existing OSS-overlay convention). No
  penpencil-specific bucket names appear in `shelfd/src/`,
  `charts/shelf/`, or any other OSS-tracked file.

- **Threat-model gate.** Per the plan §"Sidecar security review",
  this lever's PR does NOT merge until `agents/out/SHELF-34/THREAT_MODEL.md`
  enumerates items 1–5 with concrete `shelfd/src/parquet_meta.rs:LINE`
  references. The threat model lands in the same PR as the
  implementation but with line numbers verified against the
  final commit.

## Rollback signals

(Verbatim from the plan's lever-detail block; emitted as
Prometheus alert templates in the Grafana dashboard once SHELF-34
is live.)

| Trigger | Action |
|---|---|
| `shelf_origin_request_bytes_total` rate up > 20 % vs pre-cutover for > 10 min (sidecar misroute) | Disable sidecar via empty allowlist (`PARQUET_META_ALLOWLIST=`) and roll forward |
| Sidecar 5xx rate > 1 % for > 5 min | Disable sidecar (same path) |

The "disable" path is config-only — no image roll required —
because an empty allowlist makes every `/predicate-prune` request
return 403 before any parquet code runs.

## Triggers for promotion / Phase-2

Phase-2 of this lever is the Trino-side patch consuming
`/predicate-prune` (`docs/integrations/trino-predicate-prune.md`).
Promote when **all four** hold:

1. Page-index sidecar has been live ≥ 7 days with `5xx` rate
   continuously below 0.1 % per pod.
2. SHELF-35 replay shows ≥ 10 % scan-byte reduction on the
   selective-Power-BI query cohort against the parsed-but-not-
   consumed sidecar (pure observability).
3. The Trino patch's reader-side branch has unit tests against
   a synthetic page-index fixture.
4. A diff harness over 5 canonical Power BI queries shows
   byte-identical output between the sidecar-pruned read path
   and the unmodified read path.

## Test surface

- Unit (Rust): `shelfd/src/parquet_meta.rs::tests` — golden-vector
  extraction against a known-good footer built with the parquet
  crate's writer; oversized-footer rejection; predicate filtering
  correctness; `validate_path` allow / reject matrix.
- Integration (Rust, `SHELF_INTEGRATION=1` gated):
  `shelfd/tests/it_predicate_prune.rs` — boot shelfd against
  a synthetic Parquet file and call `/predicate-prune`, assert
  JSON shape and byte-range subset.
- Java (deferred): the Trino-side reader hook lands with the
  Phase-2 PR.

## References

- `shelfd/src/parquet_meta.rs` (this PR)
- `shelfd/docs/design-notes/SHELF-D3-page-index-bloom.md` —
  phase-1 SHELF-D3 plan; SHELF-34 is its phase-2 promotion
- `agents/out/SHELF-34/THREAT_MODEL.md` — sidecar security review
- `docs/integrations/trino-predicate-prune.md` — Trino-side
  reader hook sketch
- Apache Parquet PageIndex: <https://parquet.apache.org/docs/file-format/pageindex/>
- [Iceberg #15211](https://github.com/apache/iceberg/pull/15211) —
  vectorized reader page skipping
- [Iceberg #10090](https://github.com/apache/iceberg/pull/10090) —
  multi-predicate row-group filter cooperation
- [Trino #24007](https://github.com/trinodb/trino/pull/24007) —
  CLOSED, NOT MERGED footer-reader optimization (verification
  correction Apr 29 2026)
- [Trino #22739](https://github.com/trinodb/trino/pull/22739) —
  `iceberg.metadata-cache.enabled` (default ON, shadows shelf
  metadata pool)
- ADR-0011 — SHELF-04 cache-key spec
- ADR-0012 — Trino read-path strategy
- Plan: `/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md`
  §"P1 — medium impact, moderate risk, 1–2 months", lever 7
  (SHELF-34) and §"Sidecar security review (mandatory)"
