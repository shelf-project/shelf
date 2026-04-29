# `shelf-advisor` — recommenders

One section per recommender, in the order the default pipeline
runs them. SHELF-53 ships real implementations for `optimize` and
`pin_list`; sibling tickets fill in the rest.

The general output shape every recommender emits is
[`schema/recommendation.schema.json`](../schema/recommendation.schema.json),
narrowed per kind by:

- [`schema/optimize_recommendation.schema.json`](../schema/optimize_recommendation.schema.json)
- [`schema/pinlist_recommendation.schema.json`](../schema/pinlist_recommendation.schema.json)

## `optimize_targets` — `OptimizeRecommender`

**Owner:** SHELF-53 — real implementation in this PR.

### Inputs

| Series                                   | Source                                       |
| ---------------------------------------- | -------------------------------------------- |
| `manifests.list_files(<fqdn>)`           | `IcebergManifestReader`                      |
| `data_file.file_size_bytes`              | Iceberg manifest `data_file_size_in_bytes`   |

### Math

```text
small_files = files where file_size_bytes < small_file_bytes_threshold
ratio       = small_files / total_files
emit if ratio >= optimize.small_file_ratio_min
        AND total_files >= optimize.min_files_per_table
```

`confidence = clamp(ratio, 0.5, 0.95)` rounded to 4 decimal places.

### Thresholds

| Knob                                | Default | Why                                                                                                     |
| ----------------------------------- | ------- | ------------------------------------------------------------------------------------------------------- |
| `optimize.small_file_bytes`         | 32 MiB  | Matches the RisingWave small-file blog cited in the canonical SHELF-53 design note.                     |
| `optimize.small_file_ratio_min`     | 0.30    | Conservative — high enough not to drown ops in noise; low enough to flag drift past 50 % small files.   |
| `optimize.min_files_per_table`      | 8       | Avoids "100 % small" verdicts on 3-file tables that just received their first append.                   |

### Tuning

If you see false positives on streaming append tables that compact
hourly, raise `min_files_per_table` to ~16 (a streaming append
typically lands 4–8 files per micro-batch; 16 reads as "drift
across at least two batches"). If you see misses on the long
tail, lower `small_file_ratio_min` to 0.20 — the per-table cap on
the recommendation set still bounds the noise.

### Output

```json
{
  "recommendation_type": "optimize_targets",
  "table": "<fqdn>",
  "confidence": 0.85,
  "rationale": {
    "small_file_bytes_threshold": 33554432,
    "small_files":   142,
    "total_files":   167,
    "small_file_ratio": 0.85,
    "small_bytes":   873435136,
    "total_bytes":   9876543210,
    "avg_file_bytes": 59141647
  },
  "suggested_change": {
    "alter_table": "ALTER TABLE <fqdn> EXECUTE optimize(file_size_threshold => '32MB')",
    "rewrite_data_files": true
  }
}
```

The advisor never runs the SQL; the `alter_table` string is what
the operator's own CI pipeline (Bytebase / dbt-cloud / GitHub
Action) splices into a PR.

## `pin_list_candidates` — `PinListRecommender`

**Owner:** SHELF-53 — real implementation in this PR.

### Inputs

| Series                                | Source                              |
| ------------------------------------- | ----------------------------------- |
| `event_log.read_window(<window>)`     | `IcebergEventLogReader`             |
| Per-row `physical_input_bytes`        | `QueryRecord.physical_input_bytes`  |
| Per-row `wall_time`                   | `QueryRecord.wall_time`             |
| `rowgroup_pool.capacity_bytes`        | `ShelfdStatsReader.read_all()`      |

### Math

```text
agg.frequency       = count of rows for the table
agg.wall_secs       = sum of QueryRecord.wall_time, in seconds
agg.scanned_bytes   = sum of physical_input_bytes
pool_capacity       = sum of rowgroup_pool.capacity_bytes across pods,
                      else pin_list.default_pool_capacity_bytes

score = (scanned_bytes × wall_secs × frequency)
        / (1 + scanned_bytes / pool_capacity)

confidence = clamp(0.4 + 0.05 × log10(frequency × wall_secs), 0.5, 0.95)
```

A table earns a recommendation when:

- `frequency >= pin_list.min_frequency`,
- `confidence >= max(pin_list.min_confidence, global min_confidence)`.

The score is informational — every recommendation already cleared
the frequency + confidence floors. Sorting by score descending
just gives operators a triage order.

### Thresholds

| Knob                                       | Default | Why                                                                                              |
| ------------------------------------------ | ------- | ------------------------------------------------------------------------------------------------ |
| `pin_list.min_frequency`                   | 5       | Drops one-off / two-off queries; keeps recurring workloads.                                      |
| `pin_list.min_confidence`                  | 0.6     | A frequency × wall ≥ 1e4 is the first product that crosses 0.6 in the calibration above.         |
| `pin_list.default_pool_capacity_bytes`     | 11 GiB  | Matches the rc.2 rowgroup pool default in `charts/shelf/values.yaml`.                            |

### Tuning

If the advisor is over-pinning during a workload spike, raise
`min_frequency` to 10 — it filters the tail without affecting the
hot core. If the score's denominator is making large-table
recommendations sticky to the bottom of the list, double
`default_pool_capacity_bytes` to flatten the curve (the score
is unitless; only the relative ordering matters in practice).

### Output

```json
{
  "recommendation_type": "pin_list_candidates",
  "table": "<fqdn>",
  "confidence": 0.78,
  "rationale": {
    "frequency": 124,
    "wall_time_seconds": 9433.7,
    "scanned_bytes": 8273612800,
    "pool_capacity_bytes": 11811160064,
    "score": 8.273e15,
    "score_formula": "(scanned_bytes * wall_time_seconds * frequency) / (1 + scanned_bytes / pool_capacity)"
  },
  "suggested_change": {
    "pin_list_entry": {
      "table": "<fqdn>",
      "partition_filter": null,
      "ttl": "24h",
      "pool": "rowgroup"
    },
    "format": "shelfd/docs/design-notes/SHELF-23-24-admin-surface-and-pinlist.md"
  }
}
```

`partition_filter` is `null` in SHELF-53; SHELF-65 adds
predicate-scoped filters for the MV-aware-pinning case.

## `bloom_filter_columns` — `BloomFilterRecommender`

**Owner:** SHELF-52 (`agents/out/SHELF-52-bloom-advisor.md`).

SHELF-53 ships the trait stub that returns `Ok(vec![])`. The real
implementation mines `WHERE col = literal` patterns from the
SHELF-60 event-listener log table (sqlglot sidecar or
`sqlparser-rs` fallback) and scores each `(table, column)` pair
against equality selectivity × frequency × wall-time × bytes-scanned.

The recommender lives in `src/recommenders/bloom.rs` and consumes
`AnalysisContext` like every other recommender — no extra reader
shape needed.

## `mv_candidates` — `MaterializedViewRecommender`

**Owner:** SHELF-65 (the cost-reduction-plan rename of the design
note filed at `agents/out/SHELF-47-mv-aware-pinning.md`).

SHELF-53 ships the trait stub. The real implementation parses
Trino MV definitions stored as Iceberg metadata-table properties,
mines the SHELF-60 log table for `REFRESH MATERIALIZED VIEW`
frequency, and emits pin-list entries scoped to the MV's defining
predicate + TTL'd to the next refresh + 1h. The
`mv.pin_fraction` knob carries the SHELF-65 `nvme_quota *
pin_fraction` cap.
