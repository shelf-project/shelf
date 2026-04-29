# SHELF-53: `shelf-advisor` full binary implementation

**Status:** Draft
**Tier:** S
**Estimated effort:** L
**Depends on:** SHELF-37, SHELF-34
**Blocks:** SHELF-47, SHELF-52

## Problem (OSS-cited)

Trino has no native loop that emits "this table needs `OPTIMIZE`," "add a bloom filter on this column," "this MV is hotter than its base scan." Pinterest's STG211 talk ([re:Invent 2025](https://dev.to/kazuya_dev/aws-reinvent-2025-scaling-pinterest-iceberg-solutions-for-petabyte-scale-challenges-stg211-53i6)) and the [RisingWave small-file blog](https://risingwave.com/blog/iceberg-small-file-problem/) document the homegrown pipelines every shop builds. Iceberg issue [apache/iceberg #9674](https://github.com/apache/iceberg/issues/9674) (small files) and Trino issue [trinodb/trino #28636](https://github.com/trinodb/trino/issues/28636) are still open. SHELF-34 landed a scaffold under `shelf-advisor/`; this ticket fills it in.

## Goal

`shelf-advisor` is a standalone Rust binary that mines `QueryStatistics.getOperatorSummaries()` from the SHELF-37 Iceberg-sink log table plus Iceberg manifests, emits a JSON document per recommendation kind (`bloom_filter_columns`, `optimize_targets`, `mv_candidates`, `pin_list_candidates`) with confidence ∈ [0, 1], and **does not** ship an MR opener (operator's own GitHub Action / GitLab CI / Bytebase workflow consumes the JSON).

## Approach

Build on the SHELF-34 scaffold (`shelf-advisor/src/{config.rs, error.rs, output.rs}` + the empty `recommenders/` and `input/` modules visible in the current tree). Final layout:

```
shelf-advisor/
├── Cargo.toml
├── README.md
├── schema/
│   ├── bloom_recommendation.schema.json
│   ├── optimize_recommendation.schema.json
│   ├── mv_recommendation.schema.json
│   ├── pinlist_recommendation.schema.json
│   └── envelope.schema.json
├── src/
│   ├── main.rs            # CLI: `shelf-advisor recommend [bloom|optimize|mv|pin|all]`
│   ├── config.rs          # exists (scaffold)
│   ├── error.rs           # exists (scaffold)
│   ├── output.rs          # exists (scaffold) — JSON emitter
│   ├── input/
│   │   ├── log_table.rs    # JDBC pull from SHELF-37
│   │   ├── iceberg_metadata.rs  # metadata.json + manifests
│   │   ├── predicate_extractor.rs   # sqlglot sidecar shell-out
│   │   └── shelfd_stats.rs  # /stats poll (SHELF-23)
│   └── recommenders/
│       ├── bloom.rs       # SHELF-52
│       ├── optimize.rs    # small-file detection
│       ├── mv.rs          # SHELF-47
│       └── pin_list.rs    # top-N pin candidates by score
└── tests/
    ├── it_bloom.rs
    ├── it_optimize.rs
    ├── it_mv.rs
    └── it_pin.rs
```

CLI surface:

```
shelf-advisor recommend [all|bloom|optimize|mv|pin] \
  --log-trino-url <url> \
  --log-table <fqdn> \
  --window 7d|30d \
  --output-dir ./recommendations/ \
  --max-per-table 3 \
  --min-confidence 0.5 \
  --format json|markdown
```

Output: one file per kind under `<output-dir>/<date>/<kind>.json`, each conforming to the committed JSON schema. The envelope schema includes `generator: "shelf-advisor"`, `version`, `as_of`, `window`, and the array of recommendations. Operator consumes this in their own CI to open MRs (Bytebase, dbt, manual review).

Recommendation kinds:
1. **`bloom`** (delegated to SHELF-52) — `WHERE col = literal` mining → bloom-filter recommendations.
2. **`optimize`** — small-file detection: tables where `avg_data_file_size_bytes < 64 MiB` AND files-per-snapshot growing. Cite Iceberg #9674.
3. **`mv`** (delegated to SHELF-47) — MV-aware pinning.
4. **`pin_list`** — top-N pin candidates by `(scanned_bytes × wall_time × frequency) / (1 + total_bytes / pool_capacity)` score.

Inputs are read-only; no shelfd state mutation. The advisor binary runs as a periodic CronJob (Helm chart additions go to `charts/shelf-advisor/` — separate from `charts/shelf/`).

Confidence calibration: each recommender returns a `confidence ∈ [0, 1]`; advisor refuses to emit below `--min-confidence` (default 0.5). Confidence reflects evidence strength (e.g. number of matching queries × distinct days observed × non-zero `bytes_read_externally`).

## Acceptance criteria

- [ ] `shelf-advisor recommend all --window 7d` runs end-to-end on a seeded fixture in ≤ 5 min on a 4-core dev pod.
- [ ] Output for each kind validates against its committed JSON schema.
- [ ] At least one quantitative gate per recommender:
  - `bloom`: ≥ 80 % precision, ≥ 70 % recall on the SHELF-52 fixture.
  - `optimize`: 100 % recall on a fixture with 3 small-file tables (no false negatives).
  - `mv`: ≥ 80 % S3 GET-byte reduction at next refresh on a synthetic 10-MV fixture (per SHELF-47 gate).
  - `pin_list`: top-10 candidates' bytes-saved-projection within ±20 % of a hand-computed reference.
- [ ] Disabled recommender flag (`--exclude bloom`) skips that kind cleanly.
- [ ] No MR opener anywhere in the binary (verified by codeowner review + a CI grep for `git push` / `gh pr create` / `glab mr create`).
- [ ] Unit + integration tests ≥ 40 cases total across the four recommenders.
- [ ] `shelf-advisor --help` is layered (per `cli-for-agents` skill conventions) with examples for each subcommand.
- [ ] Helm chart `charts/shelf-advisor/` includes a CronJob template, default schedule `0 3 * * *` (overnight).

## Out of scope

- MR opening (explicitly dropped, ADR-line in `docs/adr/0013-advisor-json-only.md` to be filed alongside).
- Live (in-flight) recommendations — batch / nightly only in v1.
- Cross-catalog correlation (single-catalog input per run; multi-catalog is a runner-side concern).
- LightGBM / ONNX models — heuristics only in v1; ADR-0003 deferral applies.
- Auto-pin loop (rejected per item #28 in `docs/launch/feature-ideas-ranked.md`).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| False-positive flood overwhelming operators | Per-kind top-N + min-confidence gates; operator review required (no MR opener). |
| Heavy SQL load on the SHELF-37 log table | Window predicate pushdown on `end_time`; recommend partitioning the log table by `day(end_time)`; advisor batches reads. |
| Schema drift between advisor versions | Each schema file is versioned; envelope carries `version`; consumers can pin. |
| sqlglot Python dependency | Sidecar shell-out; documented in README; alternative pure-Rust `sqlparser-rs` path tracked as follow-up. |
| Auth: log table requires Trino JDBC credentials | Read from env / k8s Secret; never echoed in logs. |

## Test plan

- Unit tests per recommender (covering at minimum: empty input, single-table, multi-table, top-N gate, min-confidence gate, malformed-SQL fallback).
- Integration tests: seeded SHELF-37 log fixture + Iceberg metadata fixture; assert byte-identical golden JSON for each kind.
- Schema-conformance: every emitted JSON validated by `schemars`-derived JSON-schema in CI.
- (If applicable) docker compose smoke: SHELF-12 + listener; run `shelf-advisor recommend all`; assert all four output files non-empty.

## Open questions

- Should `pin_list` candidates feed directly into SHELF-24's loader (e.g. via a `--apply` flag)? Recommend no in v1 — operator merges via PR. Re-evaluate if operator survey demands `--apply`.
- Should the advisor maintain longitudinal state (which recommendations were accepted, rejected) to improve confidence scoring? Recommend post-v1; v1 is stateless.
- CronJob default schedule overnight (03:00) vs evening (21:00)? Recommend 03:00 local cluster TZ; operator-tunable.
- Should we ship a `recommend explain <recommendation_id>` subcommand to print the evidence behind a recommendation? Recommend yes — adds operator trust; minimal effort.
