# SHELF-53 — `shelf-advisor` core

**Status:** Implemented (this PR)
**Lives at:** `shelf-advisor/` (workspace member, flat layout)

## TL;DR

`shelf-advisor` is a standalone Rust binary that mines Trino
event-listener data (SHELF-60 jar — design at
`agents/out/SHELF-37-iceberg-event-listener-jar.md`, tracked in
[PR #66](https://github.com/shelf-project/shelf/pull/66)),
Iceberg manifests, and shelfd `/stats`, and emits versioned JSON
recommendations for table-layout / pinning changes operators
apply through their own CI/CD. SHELF-53 ships the framework + two
real recommenders (`OptimizeRecommender`, `PinListRecommender`);
SHELF-52 (bloom-write) and SHELF-65 (MV-aware pinning) land their
real recommenders against the same `Recommender` trait.

## Why a separate binary, not a crate inside `shelfd`

This question recurs because shelfd already has a sprawl of
in-process modules (`mv_registry`, `freshness`, `decoded_meta`,
the dormant `side_bloom` / `filter_service` set). The same
architectural call BLUEPRINT.md §14 made for `shelf-result-cache`
(*"…is a separate companion binary, not part of shelfd"*) applies
here. Three reasons:

1. **Deployment cadence.** The advisor reads cluster history —
   it should run as a CronJob, not as a long-lived per-pod
   sidecar. shelfd's deployment cadence is "one helm rev per
   hot-path lever per week"; shipping the advisor through that
   pipeline would slow operator iteration on heuristics.
2. **Failure-mode isolation.** A bug in the recommender pipeline
   (bad query, OOM on a chatty fixture, sqlglot regex blowup once
   SHELF-52 lands) must never crash a shelfd replica's read path.
   A separate process binary is the cleanest blast-radius cut.
3. **Privilege isolation.** The advisor wants Trino read
   credentials + Iceberg catalog read; shelfd wants neither. Two
   binaries → two trust domains.

## Architecture in one paragraph

Three input adapters
([`IcebergEventLogReader`](../../shelf-advisor/src/input/event_listener.rs),
[`IcebergManifestReader`](../../shelf-advisor/src/input/manifest.rs),
[`ShelfdStatsReader`](../../shelf-advisor/src/input/shelfd_stats.rs))
feed an
[`AnalysisContext`](../../shelf-advisor/src/recommenders/mod.rs)
which the
[`Recommender`](../../shelf-advisor/src/recommenders/mod.rs)
trait consumes. The default recommender set runs sequentially;
output is a deterministic
[`Envelope`](../../shelf-advisor/src/output.rs) versioned at
[`schema/envelope.schema.json`](../../shelf-advisor/schema/envelope.schema.json).
CLI surface: `recommend [all | optimize | pin-list | bloom | mv]`
(kebab-case at the CLI per clap convention; `recommendation_type`
in the JSON output stays snake_case),
`watch`, `dry-run`, plus a backward-compat `analyze` alias for the
SHELF-34 phase-1 scaffold's CLI contract.

## Extension seam — how SHELF-65 / SHELF-52 plug in

Both sibling tickets land their recommenders against the
`Recommender` trait + `AnalysisContext` shipped here:

```rust
pub struct AnalysisContext<'a> {
    pub config: &'a AdvisorConfig,
    pub event_log: &'a dyn IcebergEventLogReader,
    pub manifests: &'a dyn IcebergManifestReader,
    pub shelfd_stats: &'a dyn ShelfdStatsReader,
    pub tables: &'a [String],
}

pub trait Recommender: Send + Sync {
    fn kind(&self) -> &'static str;
    fn analyze(&self, ctx: &AnalysisContext<'_>) -> Result<Vec<Recommendation>>;
}
```

- **SHELF-52 (bloom-write advisor)** ships
  `src/recommenders/bloom.rs` (replaces the stub) + adds
  `src/input/predicate_extractor.rs` for the sqlglot sidecar.
  Uses `event_log` + `manifests`; doesn't need a new reader on
  the context. Adds a per-recommender YAML block (`bloom.*`)
  that's already declared in `AdvisorConfig`.
- **SHELF-65 (MV-aware pinning)** ships
  `src/recommenders/mv.rs` (replaces the stub) + extends
  `src/input/manifest.rs` with metadata-table property parsing.
  Uses all three readers; the existing `shelfd_stats` reader
  carries the `nvme_quota` figure.

Both tickets register their recommender in
`src/recommenders/mod.rs::default_recommenders()` and add an
entry to the `kind_filter` table so the `recommend <kind>` CLI
narrows correctly.

## Why no Trino-Rust client in this PR

The user override on SHELF-53 explicitly forbids adding `prusto`
or any other heavy Trino-Rust client to the workspace dep graph
in this ticket. The `IcebergEventLogReader` trait is the seam
for the production reader; today the binary ships:

- `FixtureEventLogReader` — JSON-fixture path used by `dry-run`,
  every integration test, and operators replaying captured
  workloads,
- `LiveEventLogReader` placeholder that returns `Ok(vec![])`;
  the trait is honoured, the reader ships empty until the
  follow-up ticket picks an access strategy.

A separate ticket (filed alongside this PR) decides between
sqlglot-sidecar + JDBC-via-`trino-cli` shellout, a `prusto`
dependency upgrade, or a `trino-mcp` integration. None of those
choices change the trait shape — every recommender shipped by
SHELF-53 / SHELF-52 / SHELF-65 keeps working when the production
reader lands.

## Determinism

Output is byte-identical between runs given the same inputs:

- Recommendations sort by
  `(recommendation_type, table, -confidence, sorted-rationale)`
  before emission. (`output::sort_for_emission`.)
- Numeric fields are rounded to 4 decimal places via integer math
  (`round_to_4`, `round_conf`) so f32 → f64 → ryu round-trip
  doesn't produce architecture-dependent trailing digits.
- `as_of` is the only wall-clock field; `--as-of` overrides it.
- The integration test
  `tests/it_recommend.rs::dry_run_byte_identical_between_runs`
  asserts the property by running the binary twice on the same
  fixture and `assert_eq!`-ing the bytes.

## Cross-references

- Cost-reduction plan: `[shelf-cost-reduction-research](97107ffb-cost-reduction-plan)` §Tier 3.
- Canonical design note (this ticket):
  [`agents/out/SHELF-53-shelf-advisor-full-impl.md`](../../agents/out/SHELF-53-shelf-advisor-full-impl.md).
- Sibling MV-pinning advisor (SHELF-65 — design note still filed
  under the pre-renumber id):
  [`agents/out/SHELF-47-mv-aware-pinning.md`](../../agents/out/SHELF-47-mv-aware-pinning.md).
- Sibling bloom-write advisor (SHELF-52):
  [`agents/out/SHELF-52-bloom-advisor.md`](../../agents/out/SHELF-52-bloom-advisor.md).
- Listener jar landing PR:
  [shelf-project/shelf#66](https://github.com/shelf-project/shelf/pull/66).
