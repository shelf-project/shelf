# ADR-0038 — B3 (rc.7) Intermediate-table opt-out admission gate

| Field          | Value                                                              |
| -------------- | ------------------------------------------------------------------ |
| Status         | Accepted                                                           |
| Date           | 2026-05-01                                                         |
| Track          | rc.7 — B3 (closes the hot-path Rust serial chain after A1/A2/A3/A4/A6). |
| Tickets        | B3 (rc.7 roadmap), composes with A1 (RSS-aware multiplier),        |
|                | A2 (drain-aware admission), A3 (compaction rewarm),                |
|                | A4 (net dollars-saved), A6 (cooperative peer admission).           |
| Authors        | Aamir + plan synthesis (`shelf_rc.7_roadmap_792a311b.plan.md`)     |

## Context

The post-A2 / A6 admit chain (drain → policy → LODC → rate → coop) does
a good job defending the shelf-pool against pressure-class admissions
(a draining pod, a back-pressured LODC submit queue, a saturated
rate-limiter, a defensive secondary copy). What none of those gates
catches is **a cleanly-admitting Iceberg table whose data churns faster
than the cache can pay itself back**.

Workspace memory: rep-1 specifically runs a heavy dbt batch + scratch
workload. Tables in `dbt_*` and `scratch.*` namespaces are written,
read once or twice, and snapshot-expire in 1-3 days. Each admit pays
the full cost: a Foyer LODC submit-queue token, an NVMe write, eviction
pressure on neighbouring (warmer) keys. The dashboards from the
2026-04-27 post-cutover snapshot estimate **roughly 10-20% of NVMe
occupancy on rep-1 is intermediate-table churn** — bytes that landed,
were read once, and got evicted before the read-amplification they
saved exceeded the write-amplification they cost.

The pre-B3 chain has no signal to refuse those admits. SHELF-25's size
threshold + W-TinyLFU is frequency-based, not retention-based; LODC and
the rate-limiter are pressure-based. None of them know an admitted
range belongs to a table that will be `expire_snapshots`'d in 36 hours.

The Iceberg metadata layer already carries the signal we need. Tables
configured with a short snapshot retention (`history.expire.max-snapshot-age-ms`
< a few days) are by construction transient. Operators also have a
canonical custom property — `shelf.cache-policy` — for engine-specific
tuning hints (the namespaced-property convention dbt itself uses).
Reading either signal is one S3 GET per table per refresh interval.

## Decision

Add a new admit gate, **B3 (transient-table)**, consulted in the
admit chain **after** the A2 drain check but **before** the SHELF-25
size threshold + W-TinyLFU. This position is deliberate:

- A2 drain wins because the pod is going away regardless of the
  table's policy.
- B3 short-circuits before the more expensive SHELF-25 / SHELF-21e /
  SHELF-29 / A6 work, so a refusal saves W-TinyLFU promotions, LODC
  reservations, rate-limiter token deductions, and coop RNG draws on
  the same call.

### Decision sources, highest priority first

1. **Explicit override** — `cache.transientAdmission.overrides[<schema.table>]
   = admit | refuseTransient`. Operator-blessed; wins over anything
   derived from `metadata.json`. Lets a known-bad scratch namespace
   be force-refused before the property-based detection lands.
2. **`shelf.cache-policy` table property** — the canonical custom
   knob. `transient` ⇒ refuse; anything else ⇒ admit. Mirrors the
   Iceberg convention of namespaced properties for engine-specific
   tuning. Default behaviour for tables that do not set the property:
   fall through to (3).
3. **Iceberg snapshot retention** — `history.expire.max-snapshot-age-ms`
   below `cfg.transient_threshold` (default 7 days, matching the
   typical dbt batch window) ⇒ flag transient. Tables with no
   retention property set, or retention above the threshold, admit.

### Surface

A new `shelfd::transient_admission` module ships:

- `enum TableAdmission { Admit, RefuseTransient }`
- `enum OverrideValue { Admit, RefuseTransient }`
- `struct TransientAdmissionConfig { enabled, transient_threshold,
  decision_cache_ttl, overrides }` — wired through
  `cache.transientAdmission.*` in `values.yaml`. Defaults:
  `enabled = false`, `transient_threshold = 168h`,
  `decision_cache_ttl = 10m`, `overrides = {}`.
- `trait MetadataReader` — pluggable read surface for the
  `metadata.json` refresh path; mirrors A3's `MetadataSource`
  pattern.
- `struct TransientGate` — holds the config + a per-table cached
  `TableAdmission` behind `parking_lot::RwLock<HashMap>` plus an
  in-flight `HashSet<String>` for the single-flight refresh
  pattern. Hot-path `decide(&str)` is sync, lock-free in the
  cached steady state.

`FoyerStore` gains a `transient_gate: Arc<TransientGate>` field,
populated via a new `with_transient_admission(gate)` builder (mirroring
the existing `with_coop_admission` shape). The admit chain in
`get_or_fetch_for_table` consults the gate after the A2 drain check
and, on `RefuseTransient`, short-circuits to `Miss` without spending
the rest of the chain.

A new `pub async fn get_or_fetch_for_table(pool, key, table_label,
admission, fetch)` is added alongside the existing
`get_or_fetch(pool, key, admission, fetch)`. The old method delegates
to the new one passing `"other"` (the s3_shim sentinel for non-Iceberg
paths), so non-shim callers (HTTP cache plane, compaction-rewarm,
admission-test fixtures) are not modified and the gate is a strict
no-op for them. Only `s3_shim::handle_get_object` (and the
`run_conditional_get` repopulate-on-revalidate site, for symmetry)
opt in: both already compute `s3_shim::table_label(&key)` from the
raw S3 path for the existing per-table hit/miss counters.

### Refresh model

`TransientGate::decide(table_label)` is **synchronous**. The hot path
is one `RwLock` read + a `HashMap::get`. When the cached value is
missing or older than `decision_cache_ttl`, the gate spawns a
background refresh via `tokio::spawn`, single-flighted via a
`parking_lot::Mutex<HashSet<String>>` so 100 concurrent decides for
the same uncached table fire exactly **one** `metadata.json` fetch
(pinned by the `concurrent_refresh_does_not_double_fetch` test).
The hot-path return is always immediate:

- Cached value present ⇒ that value.
- Cached value missing ⇒ `Admit` (fail-open).

Fetch errors leave the cache empty and bump
`shelf_transient_refresh_errors_total{table=...}` so operators see
the `Admit`-by-default fallback explicitly.

### Metrics

| Metric                                                      | Type                  | Labels      |
| ----------------------------------------------------------- | --------------------- | ----------- |
| `shelf_transient_refusals_total`                            | Counter               | `table`     |
| `shelf_transient_decisions_cached`                          | Gauge                 | (none)      |
| `shelf_transient_refresh_errors_total`                      | Counter               | `table`     |
| `shelf_admissions_total{decision="reject_transient"}`       | Counter (new label)   | `pool, decision` |

All three new series are added to `EXPOSED_SERIES` and the registry-
regression / `/metrics`-after-touch tests, so a freshly booted pod
publishes them as zeros even with the gate default-off.

## Consequences

### Wins

- Reduces NVMe write amplification + occupancy waste from
  intermediate-table churn (workspace memory: ~10-20% on rep-1).
  Long-term, may let operators shrink the shelf-pool from 6 → 5 pods
  on rep-1 once the saved capacity is observed across a full week.
- Composes cleanly with every prior gate. Each gate's blast radius
  stays observable on the existing
  `shelf_admissions_total{decision=reject_*}` panel — adding
  `reject_transient` does not perturb the others.
- The decision is **table-level**, not key-level. Cardinality is
  bounded by the cluster's Iceberg table count (≤ ~500 in cdp), well
  below the per-key cardinality budget the rest of shelfd respects.

### Costs

- One `metadata.json` GET per table per `decision_cache_ttl`
  (default 10 min) when a refresher is wired. v1 ships **without** an
  automatic refresher (`overrides` map is the only signal source);
  the trait + threshold + property parsers are all in tree so a
  follow-up B3.1 plugs an `S3MetadataReader` mirroring the rewarm
  poller's `S3MetadataSource`.
- Adds two atomic-equivalent operations on the admit hot path
  (RwLock read + HashMap::get) per call. Branch-predictable when the
  gate is disabled (the early `if !cfg.enabled` short-circuits before
  the lock).
- `tokio::spawn` is required for the refresh path. Production callers
  go through `FoyerStore::get_or_fetch` which is itself async, so the
  ambient runtime is always present.

### Failure modes

- **`metadata.json` 5xx / network error** ⇒ no cache write, gate
  stays at fail-open `Admit`. Bumps
  `shelf_transient_refresh_errors_total`. Test
  `s3_error_during_refresh_admits_default` pins this invariant.
- **No reader wired** (v1 default) ⇒ overrides-only mode. The gate
  consults `cfg.overrides` and otherwise admits. The reader is wired
  via `TransientGate::with_reader` in B3.1.
- **Operator typo in YAML** (`refusetransient` vs `refuseTransient`)
  ⇒ `serde_yaml` errors at config load (`#[serde(deny_unknown_fields)]`
  catches the misspelt enum value). Pod fails to start; loud, not silent.

### Rollback

Set `cache.transientAdmission.enabled: false` in the values overlay
and rolling-restart the StatefulSet. The gate becomes a strict
no-op on the very next pod boot; previously refused admits are not
reverted (they stay in the cache wherever they happen to be) but
the next read-miss admits unchanged.

## References

- `shelfd/src/transient_admission.rs` — the module; 12 unit tests.
- `shelfd/src/store.rs` — admit-chain insertion + builder.
- `shelfd/src/main.rs` — wiring + log lines.
- `shelfd/src/s3_shim.rs` — opt-in via `get_or_fetch_for_table`.
- `shelfd/src/rewarm_poller.rs` — A3's `metadata.json` reader pattern
  reused for the future B3.1 refresher.
- `charts/shelf/values.yaml` — operator-facing knobs.
- ADR-0027 (A2 drain), ADR-0036 (A3 rewarm poller), ADR-0037 (A6 coop).

## Alternatives considered

- **Static blocklist via Helm only.** Equivalent to the `overrides`
  map without metadata-derived flags. Too rigid as a long-term answer:
  every new dbt model requires a values.yaml edit. The chosen design
  ships overrides AND a property-based path so the ops surface stays
  small.
- **Admit always + faster eviction on transient tables.** Would have
  to plumb table identity through Foyer's admission/eviction surface,
  which we explicitly want to keep ADR-0009-clean. Also doesn't help
  write amplification — the admit was already paid.
- **Per-key TTL.** Too granular: cardinality blows up
  (every Parquet row group), and the right answer is genuinely table-
  level (every row group of a transient table is transient by
  inheritance). The chosen design keeps cardinality bounded by table
  count.
- **Reuse SHELF-25 size threshold.** Tables that are transient and
  large would refuse anyway under SHELF-25 — but the dbt scratch
  workload is dominated by smallish row groups that pass the size
  policy. B3 is orthogonal: it complements SHELF-25 rather than
  competing with it.
