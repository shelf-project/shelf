# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **SHELF-45 — Compaction-aware re-warm reactor.** New module
  `shelfd/src/compaction_rewarm.rs` watches a stream of
  `IcebergSnapshotEvent`s and proactively re-warms the new file paths
  emitted by `replace`-class snapshots
  (`ALTER TABLE … EXECUTE optimize`, `expire_snapshots`,
  `remove_orphan_files`) before the cold-miss thundering herd hits
  S3. Producer is pluggable via the `IcebergEventStream` trait; the
  default `LoggingEventStream` stub keeps the reactor a no-op while
  the SHELF-37 listener (PR #66) is finishing its soak. Re-warm
  flows through the existing `FoyerStore::get_or_fetch` single-flight
  surface, is rate-limited (default 50 MiB/s/pod) and concurrency-
  capped (default 4 in-flight files), and stays strictly below the
  client read budget. Adds seven Prometheus families
  (`shelf_rewarm_events_total`, `shelf_rewarm_files_total`,
  `shelf_rewarm_bytes_total`, `shelf_rewarm_lag_seconds`,
  `shelf_rewarm_inflight_files`, `shelf_rewarm_queue_depth`,
  `shelf_rewarm_errors_total`) all exercised by the regression
  tests in `metrics.rs`. Helm chart gains `cache.rewarm.{enabled,
  maxBytesPerSec, maxConcurrentFiles, queueCapacity,
  snapshotLagToleranceSecs}`; `enabled: false` by default both in
  the OSS values and operator overlays (overlay carries a
  commented hint to flip after the Tier-1 measurement substrate is
  green for 7 days). Design note in
  `shelfd/docs/design-notes/SHELF-45-compaction-aware-rewarm.md`,
  operator runbook in
  `shelfd/docs/runbooks/SHELF-45-compaction-rewarm.md`.

### Added — B1 (Tier-1 cost reduction)

- **`cache.pools.rowgroup.compression`** — opt-in zstd compression of
  cached row-group payloads. Disabled by default. When enabled,
  every byte range Foyer holds is zstd-encoded with a 1-byte version
  header, so the same NVMe budget holds 1.4–2.5× more keys on real
  Iceberg / Parquet workloads. At constant pod count this lifts hit
  ratio by ~5–10 pp; at constant hit ratio it makes the StatefulSet
  shrinkable by one pod.
- **On-disk safety marker** — `<nvme_dir>/.shelf-compression.json`
  records the active compression descriptor (e.g. `"zstd@3"`) and
  the configured `min_size_bytes`. Boot aborts loudly if the
  marker disagrees with the configured pipeline, instead of
  corrupt-reading silently. The "switch compression mode" playbook
  is documented inline in `charts/shelf/values.yaml`.
- **Prometheus series** — `shelf_compress_bytes_in_total{pool}`,
  `shelf_compress_bytes_out_total{pool}`,
  `shelf_compress_outcomes_total{pool, outcome}` (where `outcome` ∈
  {`compressed`, `skipped_small`, `skipped_incompressible`,
  `decompressed_ok`, `decompressed_uncompressed`,
  `decompress_error`}), and `shelf_compress_seconds{pool, op}`
  (histogram, op ∈ {`encode`, `decode`}).
- **`CompressionPipeline`** — pool-agnostic helper in
  `shelfd::compression` exposed for re-use by the metadata pool's
  legacy `zstd_metadata` feature gate. The metadata-pool wiring
  remains feature-gated in v1; only the rowgroup pool is wired
  through runtime config.

### Added

- **SHELF-46 — Bloom-aware footer admission.** Optional admission
  policy that promotes Parquet footer suffix reads (last
  `cache.bloom.minFooterBytes`, default 64 KiB) and known bloom-filter
  byte ranges into `Pool::Metadata` (DRAM-only, longer residency)
  regardless of the file extension's default pool routing. Both
  classes of read get a hard `Admit` regardless of the size-threshold
  policy, so a footer larger than `cache.admission.sizeThresholdMiB`
  can still cache. Intended to lower S3 GET cost on Iceberg/Parquet
  workloads where Trino's predicate pushdown re-reads bloom blocks
  across queries.
  - New module `shelfd::parquet_admit` carries the classifier, an
    LRU `etag → Vec<BloomBlockRange>` index (default 50 000 entries,
    ~4 MiB worst-case RSS), and a `FORCE_ADMIT` admission policy
    used for footer/bloom-block reads.
  - Three new Prometheus series, all in `EXPOSED_SERIES`:
    `shelf_bloom_admit_total{kind=footer|bloom_block|not_applicable}`,
    `shelf_bloom_index_entries`,
    `shelf_bloom_parse_errors_total{reason}`.
  - Helm values surface as `cache.bloom.{enabled,maxIndexEntries,
    minFooterBytes}` and are rendered into the shelfd ConfigMap as
    `bloom_admission.{enabled,max_index_entries,min_footer_bytes}`.
  - **Default off** (`cache.bloom.enabled=false`) on the OSS chart
    and on operator overlays. Flip per replica as a 24 h canary
    after SHELF-49 (range coalesce) and B1 (zstd metadata
    compression) have soaked. See
    `shelfd/docs/design-notes/SHELF-46-bloom-aware-footer-admission.md`
    for the rollout playbook and the
    `iceberg.metadata-cache.enabled=false` Trino caveat.
  - Footer parser is gated behind a non-default `parquet_meta` cargo
    feature so stock builds stay lean (the `parquet` crate adds
    ~4 MB of compile output and ~60 s of CI time). Without the
    feature the footer-suffix heuristic still routes trailing reads
    to `Pool::Metadata`; only the bloom-block index is empty.
- **SHELF-50 — Decoded metadata in-process cache.** New module
  `shelfd/src/decoded_meta.rs` exposing two parallel
  `parking_lot::Mutex<lru::LruCache<EtagKey, Arc<…>>>` caches:
  `ManifestCache` (ETag → `Arc<ManifestFile>`) and
  `ParquetFooterCache` (ETag → `Arc<parquet::file::metadata::ParquetMetaData>`).
  Producer hook `on_metadata_admit(etag, hint, bytes)` is called
  from the `Pool::Metadata` admission path; the heavy decode runs
  fire-and-forget on `tokio::task::spawn_blocking` so the hot read
  path is not extended. Consumer accessors `get_manifest(etag)`
  and `get_parquet_footer(etag)` (no existing call site reads
  yet — SHELF-46 / SHELF-37 / SHELF-47 are the planned consumers).
  ETag-keyed invalidation via `invalidate(etag)` keeps the decoded
  cache consistent with ADR-0011's content-addressed invariant.
  New metrics: `shelf_decoded_meta_hits_total{kind}`,
  `shelf_decoded_meta_misses_total{kind}`,
  `shelf_decoded_meta_decode_seconds{kind}` histogram,
  `shelf_decoded_meta_entries{kind}` gauge,
  `shelf_decoded_meta_decode_errors_total{kind, reason}`. Helm
  knob `cache.decodedMeta.{enabled, maxManifestEntries, maxFooterEntries}`
  defaults to **`enabled: false`** because v1 ships ahead of any
  consumer; downstream tickets flip it on. Design note at
  `shelfd/docs/design-notes/SHELF-50-decoded-metadata-cache.md`.
- **SHELF-42 — A/B query tagging.** Trino sessions can now stamp shelf-bound
  HTTP requests with an `X-Shelf-Tag` header carrying URL-encoded JSON
  (e.g. `{"experiment":"b1_compression_on","cohort":"prod_rep1"}`),
  derived from `shelf.tag.<key>` session properties via the new
  `io.shelf.tag` package. shelfd parses the header in
  `s3_shim`, enforces a per-pod cardinality cap (configurable via
  `cache.abTag.maxDistinctTags`, default 16) with an `other` sentinel,
  and splits the existing hit / miss / response-bytes counters across
  the new companion series `shelf_hits_by_tag_total`,
  `shelf_misses_by_tag_total`, and
  `shelf_s3_shim_response_bytes_by_tag_total`. A dedicated
  `shelf_ab_tag_cap_violations_total{reason="cardinality"}` counter
  fires (and emits a one-shot WARN) when the cap is exceeded inside a
  scrape window. The receive path is **default-off** (`cache.abTag.enabled
  = false`); operators flip to `true` on the Penpencil overlay where
  Prometheus retention is sized for the per-tag series. Trino-side tag
  forwarding is always on — it is metadata, no perf cost — and tags
  belong to a single request (not cached, not stashed). See
  `docs/contracts/ab-tag.md` for the wire-level contract and
  `shelfd/docs/design-notes/SHELF-42-ab-query-tagging.md` for the
  lifecycle diagram.
- **SHELF-40 — `shelf_s3_dollars_saved_total` counter and shared `shelf-cost` crate.**
  New cargo workspace member at `crates/shelf-cost/` exposes a
  region-aware `CostModel` that converts cache hits into integer-cents of
  S3 GET + EC2 cross-AZ data-transfer + (opt-in) NAT-gateway data-processing
  spend avoided. `shelfd` registers two new Prometheus series wired from
  the existing `s3_shim` / `peer_fetch` hot paths via `ReadOutcome`:
    - `shelf_s3_dollars_saved_total{region, outcome}` — `IntCounterVec`,
      unit **cents**. `outcome ∈ {hit_memory, hit_disk, peer}`.
    - `shelf_s3_dollars_saved_rate_cents_per_sec{region, outcome}` —
      `IntGaugeVec`, 60 s rolling rate published by an in-process updater
      task so dashboards don't have to `rate(... [60s]) * 0.01` themselves.
  Default coefficients match the published AWS price list for `us-east-1`
  and `ap-south-1` (S3 GET `$0.0004 / 1k`, cross-AZ data transfer
  `$0.01 / GiB`, NAT processing `$0.045 / GiB`); see
  [`crates/shelf-cost/README.md`](crates/shelf-cost/README.md) for the
  citation table and the quarterly refresh runbook. The counter is
  on by default; flip `cache.cost.enabled: false` in your overlay to
  disable. Dashboard `charts/shelf/grafana/dashboards/shelf-read-path.json`
  gains "S3 cost saved (cents/sec, last 60s)" and "S3 cost saved this
  month (running)" panels. Design note:
  [`shelfd/docs/design-notes/SHELF-40-dollars-saved-counter.md`](shelfd/docs/design-notes/SHELF-40-dollars-saved-counter.md).

## [1.0.0-rc.4] — 2026-04-29

Admission-side back-pressure follow-on to rc.3, plus the post-org-migration
dependency wave.

### Added
- **SHELF-29**: independent-queue admission rate-limiter (#39). A second
  bounded queue at the admission seam — distinct from the LODC submit
  queue introduced in SHELF-21e — that decouples write admission rate
  from the read path. Drops on full surface as
  `shelf_lodc_drops_total{reason="rate_limit"}` so they stay visible in
  the ops dashboard. Default config is byte- and item-rate gated with an
  env-var off-switch for emergencies; see `shelfd/src/admission.rs`.

### Changed
- Bumped `axum` 0.7.9 → 0.8.9 (#25). Route-syntax migration applied across
  `s3_shim.rs`, `peer.rs`, and `http.rs` (`:cap` → `{cap}`,
  `*key` → `{*key}`); `metrics::get_name()` → `metrics::name()`.
- Bumped `prometheus` 0.13.4 → 0.14.0 (#27).
- Bumped `thiserror` 1.0.69 → 2.0.18 (#28). No source touch required.
- Bumped `sha2` 0.10.9 → 0.11.0 (#12). Cache-key spec (ADR-0011) is
  unaffected — keys use the raw `sha256(etag || …)` byte sequence, not
  the API-level type, so byte-identity across the bump is preserved.
- Coordinated OpenTelemetry batch (#42, supersedes #17 / #23 / #24 / #26
  / #29 via `closes #N`): `opentelemetry` 0.27.1 → 0.31.0,
  `opentelemetry_sdk` 0.27.1 → 0.31.0, `opentelemetry-otlp` 0.27.0 →
  0.31.1, `tracing-opentelemetry` 0.28.0 → 0.32.1, `prost` 0.13.5 →
  0.14.3. `shelfd/src/telemetry.rs` adapted: `TracerProvider` →
  `SdkTracerProvider`, `Resource::new(…)` →
  `Resource::builder().with_attributes(…).build()`,
  `with_batch_exporter()` no longer takes a runtime argument.
- UI dev-dependencies: `react` family (#9), `typescript` 5.9.3 → 6.0.3
  (#8). The TypeScript bump required an ambient `*.css` declaration in
  `shelfd/ui/src/types.d.ts` for side-effect imports.

## [1.0.0-rc.3] — 2026-04-29

Peer-fetch race + OSS feature rollup. No Chart.yaml bump landed in this
window; rc.3 is a logical milestone collapsed into the rc.4 image stream.

### Added
- **SHELF-23**: peer-fetch race + ETag-conditional GET (#38). Any shelf
  pod can now serve any key by racing the HRW peer against origin S3
  with an ETag-conditional GET, falling back to origin on peer miss /
  drain / disagreement. Wires `peer.rs`, `peer_fetch.rs`, and
  `freshness.rs` (~1.8 kLOC) into the S3-shim hot path. The rule of
  thumb stays "1 shelf pod per consumer replica" but cache content is
  now shareable across pods, structurally unblocking autoscaling and
  rebalancing the HRW key-family concentration that was leaving cold
  pods at ~1 % rowgroup hit ratio under heavy single-table read patterns.
  Smoke evidence on a 4-pod soak: hit ratio normalised to within
  0.8 pp across pods, peer-win share ~25 %, RSS peak well under the
  per-pod ceiling, zero pod restarts.
- **OSS feature rollup** (#33): multi-engine read-path examples
  (`examples/duckdb/`, `examples/daft/`, `examples/spark/`,
  `examples/pyiceberg/`, `examples/starrocks/`), `shelfctl` extender
  scaffold, `shelf-advisor` crate scaffold, in-shim `MemoryFS` banner
  for stub-mode operation, and 17 backlog ticket specs under
  `agents/out/SHELF-*/`. Sets up the multi-engine compose smoke matrix
  the post-rc test rail relies on.

### Changed
- Wave of GitHub-Actions dependabot bumps (org-migration cleanup):
  `actions/checkout` 4 → 6 (#14), `actions/github-script` 7 → 9 (#21),
  `azure/setup-helm` 4 → 5 (#20), `docker/build-push-action` 6 → 7
  (#19), `docker/setup-qemu-action` 3 → 4 (#18),
  `aws-actions/configure-aws-credentials` 4 → 6 (#16),
  `actions/upload-artifact` 4 → 7 (#15), `actions/setup-python` 5 → 6
  (#13), `actions/download-artifact` 4 → 8 (#11), and the actions
  minor-and-patch group (#32). UI dev-dep bump
  `@vitejs/plugin-react` (#7).

## [1.0.0-rc.2] — 2026-04-29

**Hotfix release for the SHELF-21f LODC submit-queue overflow regression
observed on the originating cluster's alluxio NodePool the night of
2026-04-28 → 2026-04-29 (full RCA + evidence in `docs/rollout-v1/`).**

### Why a hotfix

The 2026-04-28 helm rev-16 soak proved the SHELF-21e LODC defaults
(`flushers=4`, `bufferPool=256 MiB`, `submitQueue=1 GiB`) were not
sufficient under sustained read load on a 4xlarge alluxio NodePool with
~27.3 GiB node-allocatable. By 09:07 IST on 2026-04-29:

- cluster-wide `shelf_lodc_drops_total` rate had grown 2.5× overnight
  (~2.7 M/h → 6.3 M/h), with shelf-2/3 `shelf_lodc_inflight_bytes`
  pinned at exactly 859 053 141 B (= 80 % watermark of the 1 GiB
  submit-queue threshold) for 6 h continuously — the LODC had fully
  saturated and every admission was being dropped;
- shelf-1 was OOMKilled (exit 137) at 06:40 IST, RSS peak 29.11 GiB;
- shelf-0 RSS peaked at 29.10 GiB earlier and 27.66 GiB at the time of
  the alert, both above the 27.30 GiB node-allocatable ceiling.

### What this changes

1. **`origin.pool.maxConnections` 256 → 128** (chart default and prod
   overlay) — caps worst-case origin in-flight RSS at
   `maxConnections × ~32 MiB ≈ 4 GiB`. The live cluster ConfigMap had
   been hand-applied to 512 during the 2026-04-28 chaos window, which
   raised origin worst-case to ~16 GiB and left zero RSS headroom under
   the 19 GiB DRAM caps + 1 GiB LODC submit queue. The deploy runbook
   for this rc explicitly resets the in-cluster ConfigMap.
2. **`cache.pools.rowgroup.dramSizeBytes` 14 GiB → 11 GiB** — frees
   ~3 GiB of node-allocatable headroom and reduces the rate at which
   the rowgroup pool evicts into the LODC.
3. **`cache.pools.rowgroup.diskCache.flushers` 4 → 8** and
   **`bufferPoolSizeBytes` 256 MiB → 384 MiB** — approximately doubles
   the gp3 drain parallelism so the LODC submit queue actually drains
   in steady state and `shelf_lodc_inflight_bytes` falls below the
   80 % watermark. Without this, the SHELF-21e back-pressure was
   correctly dropping admissions but the LODC was permanently saturated.
4. **`shelfd::config::default_max_inflight()` 256 → 128** — defensive
   matching default in the Rust struct so dev / CLI invocations that
   skip the chart inherit the same bound. Unit-test updated.

### Why these are not separate options

- **`RateLimitPicker` is not coming back** — the 2026-04-28 chaos window
  proved it pegs `hit_disk` p99 at the histogram-max bucket because it
  shares a queue with reads (see `lodc_backpressure.rs` module doc and
  AGENTS.md preview-9 note). The SHELF-21e level-based gate on shelfd's
  own admission seam stays.
- **`shelf-2`/`shelf-3` are not changed** beyond the uniform values
  bump. Their low rowgroup hit ratio is HRW-by-design (key family
  concentration on shelf-0/1) and is not the subject of this fix.

### Phase-A RCA verdict

H3 (RSS budget exhaustion) is the primary cause; H2 (LODC flusher
drain rate) is the secondary cause that pinned the inflight gauge at
the watermark. H1 (NVMe IOPS) is not the bottleneck — gp3 baseline
3 000 IOPS / 125 MiB/s is well under the observed sustained write
rate envelope (`node_disk_writes_completed_total` ≪ provisioned cap).
Full RCA + evidence in `shelfd/docs/runbooks/2026-04-shelf-1-oom.md`
(updated for this incident).

### RSS budget arithmetic, post-fix, on 4xlarge alluxio NodePool

```
   5  GiB  metadata DRAM
+ 11  GiB  rowgroup DRAM        (was 14)
+  4  GiB  origin in-flight     (= 128 × 32 MiB; was 16 GiB at 512)
+  1  GiB  LODC submit queue
+  3  GiB  Rust runtime + tokio + jemalloc fragmentation
= 24  GiB  worst-case RSS
———————
 27.3 GiB  node-allocatable ceiling
=  3.3 GiB  headroom
```

Previous budget left zero headroom under the same ceiling.

## [1.0.0-rc.1] — 2026-04-28

Released from the canonical home `github.com/shelf-project/shelf`. Re-cut
of `1.0.0-rc.0` after release-pipeline first-run bugs:

- `build-image` job timed out at 45 min on QEMU-emulated linux/arm64 Rust
  release build. Bumped to 90 min; GHA layer cache from the rc.0 attempt
  primes rc.1.
- `helm-publish` job's cosign sign step failed with `UNAUTHORIZED` because
  it relied on `helm registry login` only — cosign uses its own auth.
  Added `docker/login-action` before the cosign step.
- CI plumbing stabilized for org migration (gitleaks `pull-requests: read`,
  helm-template/`kubectl` server-API decoupling via Python YAML parser,
  `cargo-audit` advisory-DB workaround for the malformed
  `RUSTSEC-2026-0073.md`, `cargo-deny` advisories ignored under SHELF-30,
  `aquasecurity/trivy-action` rolled to `v0.36.0`, IAM-wildcard grep
  excludes its own self-documenting workflow file).

No runtime code changes vs `1.0.0-rc.0`. Same runtime evidence applies.

## [1.0.0-rc.0] — 2026-04-28

First release candidate. The 30-day post-`v0.5` calendar soak gate from the
launch playbook is **explicitly waived by BDFL decision**; substituting the
following runtime evidence:

- `shelf-2` cut over to `shelfd` (single-line `s3.endpoint=` flip) on 2026-04-27,
  observed stable on `0.1.0-preview-9` then `0.1.0-preview-10` for ≥24h with
  hit-ratio ≥ 78 % rowgroup, p99 read ≤ 100 ms, zero `ICEBERG_*` regressions
  (vs `Alluxio` baseline 366 → 18 infra failures, -95 %).
- `shelf-1` cut over 2026-04-27, stable on the same image stream.
- 4-replica `shelf-{0..3}` cluster running on the dedicated `alluxio` Karpenter
  NodePool with 56 GiB DRAM + 960 GiB NVMe aggregate cache.
- Critical write-path data-corruption bug (SHELF-25, `Content-Encoding: aws-chunked`
  decode) shipped in `0.1.0-preview-9` and validated against live Iceberg writers.
- LODC submit-queue overflow (SHELF-21e) bounded with drop-on-full back-pressure
  that does not couple write admission to the read path.
- Zero-downtime rolling-update path validated via `shelf-pool` ClusterIP +
  `minReadySeconds=30` + `startupProbe` (5-min Foyer NVMe-recovery grace).

`v1.0.0` final follows after the 7-day RC window unless a regression is found.

### Added
- Tag-driven release pipeline (`.github/workflows/release.yml`) — multi-arch
  container image to GHCR, Helm chart published OCI, `syft` SBOM, SLSA-v1.0
  provenance, `cosign sign --keyless` keyless signatures.
- Origin-overlay leak guard in the release workflow + `.gitattributes`
  `export-ignore` for the in-repo origin-cluster overlay subtree,
  `agents/out/**`, and `docs/rollout-v1/**`.
- `docs/brand/` — locked tier-ordered primary mark + favicon.
- OSS hygiene set: `CODE_OF_CONDUCT.md`, `MAINTAINERS.md`, `GOVERNANCE.md`,
  `ROADMAP.md`, `RELEASING.md`, `CHANGELOG.md`, GitHub issue templates,
  `dependabot.yml`, `CODEOWNERS`, DCO check workflow.

### Fixed
- SHELF-25: PUT path now decodes `Content-Encoding: aws-chunked` before
  uploading to origin S3 — fixes Iceberg metadata corruption that surfaced
  as `ICEBERG_INVALID_METADATA` on write-capable replicas.
- SHELF-21e: replaced `RateLimitPicker` (which throttled reads) with a
  bounded LODC submit-queue + drop-on-full back-pressure.

## [0.1.0] — 2026-04-28

Initial public production state. Running on the origin Trino-on-EKS cluster
across two of four replicas; soak-clock for `v0.5.0` begins at full cutover.

### Added
- `shelfd` — Rust cache daemon with two Foyer pools (metadata DRAM-only,
  rowgroup hybrid DRAM + NVMe). Content-addressed keys per ADR-0011
  (`sha256(etag || u64_le(offset) || u64_le(length) || u32_le(rg_ordinal))`).
- S3-compatibility shim on `:9092` accepting GET/HEAD/PUT/DELETE; signature-
  agnostic by design so any S3 client (Trino native, dbt, Iceberg writer)
  drops in via a one-line `s3.endpoint=` flip.
- AWS-chunked PUT decoding (SHELF-25) — strips streaming-signed
  `Content-Encoding: aws-chunked` framing before re-uploading to origin.
- HRW (Highest Random Weight) consistent hashing across pods, with a
  membership resolver that periodically polls the headless service and
  honours a lameduck drain bit on `SIGTERM`.
- LODC submit-queue back-pressure (SHELF-21e) — bounded watermark gate at
  the admission seam, drop-on-full, never blocks reads.
- Helm chart at `charts/shelf` with a ClusterIP `shelf-pool` service +
  `minReadySeconds: 30` + gated `startupProbe` (5-min grace for Foyer
  NVMe recovery on rolling restart).
- Prometheus metrics surface: hits/misses by pool and table, NVMe disk
  fill, eviction reasons, LODC drops, rolling hit ratio, plus a
  reference Grafana dashboard (`shelf-overview`).
- `shelfctl` admin CLI for ring inspection, pin/unpin, drain.
- Built-in web UI on `:9090/ui/` (Vite/React/TS, 5-tab redesign:
  Story / Live / Hot tables / Lab / Admin) — opt-in via the `ui` cargo
  feature so stock builds don't pull npm.

### Documentation
- `BLUEPRINT.md` (architecture), `COMPARISON.md` (vs Alluxio), full ADR
  set under `agents/out/adr/0001-…`, design notes per ticket, rollout
  runbooks under `docs/rollout-v1/`.

[Unreleased]: https://github.com/shelf-project/shelf/compare/v1.0.0-rc.4...HEAD
[1.0.0-rc.4]: https://github.com/shelf-project/shelf/releases/tag/v1.0.0-rc.4
[1.0.0-rc.3]: https://github.com/shelf-project/shelf/releases/tag/v1.0.0-rc.3
[1.0.0-rc.2]: https://github.com/shelf-project/shelf/releases/tag/v1.0.0-rc.2
[1.0.0-rc.1]: https://github.com/shelf-project/shelf/releases/tag/v1.0.0-rc.1
[1.0.0-rc.0]: https://github.com/shelf-project/shelf/releases/tag/v1.0.0-rc.0
[0.1.0]: https://github.com/shelf-project/shelf/releases/tag/v0.1.0
