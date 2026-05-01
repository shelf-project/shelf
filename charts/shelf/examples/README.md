# Examples

Reference configs for common shelf integration patterns. None of these
are wired into the chart by default; they are drop-in templates the
operator can copy into their own values overlay.

## `trino-catalog-recipe.yaml`

Recommended Trino Iceberg catalog block when fronting tables through
`shelf-pool`. Per-property rationale is inline in the YAML, including
a link to the relevant `@Config(...)` annotation in Trino 480 source.

The five-property tuning block is **cost-neutral** by construction
(no extra EC2 spend, no new infra) and **performance-up**:

| Property                                  | What it fixes                                                       |
|-------------------------------------------|---------------------------------------------------------------------|
| `iceberg.metadata-cache.enabled=false`    | Unshadows the shelf metadata pool (in-JVM cache otherwise pegs hit ratio at ~0%) |
| `iceberg.dynamic-filtering.wait-timeout=3s` | Caps DF stall on the ~50% JOIN cohort                              |
| `s3.max-connections=256`                  | Sized to a 6-pod shelf-pool (~1536 cluster-wide cap)                |
| `parquet.small-file-threshold=8MB`        | Merges small footer reads into single GET                           |
| `s3.tcp-keep-alive=true`                  | Survives idle TCP drop by kube-proxy / LB                           |

### When to revisit

Update this example when any of the following hold:

- Shelf metadata pool warm hit rate sustains above 80% (you may be able
  to drop `iceberg.metadata-cache.enabled=false` once the shim layer is
  uncontested).
- Phase 4 lever flips land (decoded-metadata cache, range coalescing,
  bloom-aware admission, NVMe compression).
- Your shelf-pool size or per-pod connection budget changes
  (re-derive `s3.max-connections`).

### Related

- `charts/shelf/values.yaml` — `cache.abTag` block: SHELF-42 A/B
  tagging knob for attributing hit/miss across query cohorts. Pair with
  this recipe when measuring the impact of a single property flip.
