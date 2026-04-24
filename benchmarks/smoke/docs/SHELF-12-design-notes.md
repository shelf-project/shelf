# SHELF-12 · Smoke Harness — Design Notes

Status: **PARTIAL**. The harness scaffolding is complete and every step
except the final shelfd-hit assertion runs end-to-end. The conformance
check documented for SHELF-15 / SHELF-20 correctly fails because the
Trino-side read path does not yet route through shelfd — that wiring
is a known TODO in the plugin (see
`clients/trino/src/main/java/io/shelf/plugin/ShelfPlugin.java`
Javadoc, and the `TODO(SHELF-PHASE-2)` stubs in
`ShelfPrefetchListener.java`).

This note describes what the harness does verify today, what it
deliberately does not, and the exact blockers with evidence.

## What the harness verifies

With `docker compose up` the following all work:

1. **MinIO** serves the `iceberg-warehouse` + `shelfd-pin-list`
   buckets at `minio:9000` (HTTP, path-style).
2. **iceberg-rest** (`tabulario/iceberg-rest:1.6.0`) exposes an Iceberg
   REST catalog backed by a JDBC-sqlite tracker + S3FileIO pointed at
   MinIO.
3. **seed container** (`python:3.12-slim` with
   `pyiceberg[pyarrow,s3fs]>=0.9,<0.11`) creates the `default`
   namespace and writes three Iceberg tables with real Parquet data
   files + Avro manifests (see `mc ls -r local/iceberg-warehouse`
   output in the commit description).
4. **shelfd** (Rust, multi-stage Debian image built via
   `benchmarks/smoke/Dockerfile.shelfd`) starts, reads
   `/etc/shelfd/shelfd.yaml`, binds the data-plane listener on
   `0.0.0.0:9090`, and its `/healthz` + `/metrics` endpoints return
   200. The `/metrics` port is mapped to host `127.0.0.1:9091` for
   the smoke driver.
5. **Trino 480** boots with `iceberg.catalog.type=rest`, loads the
   shelf plugin (`Installing io.shelf.plugin.ShelfPlugin` +
   `Registering event listener shelf-prefetch` in server log), and
   answers all 10 canonical queries.
6. **run-smoke.sh** runs the 10 queries twice via the bundled
   `/usr/bin/trino` CLI inside the coordinator. **Cold and warm
   outputs are byte-identical** on every query — correctness PASS.

## What the harness does NOT verify (yet)

The final assertion — warm `shelf_hits_total` strictly greater than
cold on at least one of the `metadata` / `rowgroup` pools — is
**guaranteed to fail** in the current tree. Two independent blockers:

### Blocker A · `ShelfFileSystemFactory` not wired into Trino 480 SPI

Evidence, verbatim from
`clients/trino/src/main/java/io/shelf/plugin/ShelfPlugin.java`:

> The FileSystem is wired via the Trino 480 plugin FS factory
> registry. We expose it through a small in-process holder on the
> plugin rather than via `Plugin` directly, since the Trino 480 SPI
> does not have a `getFileSystemFactories()` method yet (this lives
> in `io.trino.filesystem` outside `io.trino.spi`). Ticket SHELF-10
> finalises the wiring once we've decided whether to load Shelf as a
> Trino connector or as a standalone plugin.

Until SHELF-10 / SHELF-22 lands, Iceberg-connector reads go straight
to the native-s3 filesystem (MinIO), not through Shelf. That's why
no `shelf_hits_total` / `shelf_misses_total` series are emitted on
the data path. The only shelf series present in
`results/metrics-after-warm.txt` come from a synthetic
`curl /cache/metadata/...` probe the driver issues during startup
for liveness.

### Blocker B · `ShelfPrefetchListener` hooks are SHELF-PHASE-2 stubs

`clients/trino/src/main/java/io/shelf/eventlistener/ShelfPrefetchListener.java`:

```java
@Override
public void queryCreated(QueryCreatedEvent event) {
    ...
    // TODO(SHELF-PHASE-2): extract tables + predicates from QueryMetadata.
```

Even if we wired the listener via `event-listener.config-files`, the
hook bodies are no-ops and would not call shelfd's `PrefetchClient`.

Both blockers are upstream of SHELF-12 — they sit in plugin source
that this ticket is explicitly forbidden from touching (Rust / Java
source is out-of-scope per the ticket spec).

## What the PARTIAL deliverable is good for

- Any developer can run `make smoke-up && make smoke` locally and
  reproduce the stack today. Correctness regressions in the Iceberg
  read path surface as cold-vs-warm query-output diffs.
- When SHELF-22 lands and the plugin's FS factory registers through
  Trino 480's plugin FS registry, the same smoke loop will start
  emitting non-zero `shelf_hits_total` and the assertion will flip
  to PASS without any further work in this ticket's surface.
- CI gate (`.github/workflows/smoke.yml`) is wired and path-gated;
  it will surface any regression in the harness itself.

## Scope cuts / fallbacks that were NOT needed

The ticket allowed skipping `orders_small` if seeding was too fiddly;
that turned out not to be necessary. All three tables seed cleanly
with `pyiceberg[pyarrow,s3fs]` >= 0.9 against the `tabulario/iceberg-rest`
REST catalog. The harder escape hatch — hand-authoring
`metadata.json` + manifest Avro — was not needed.

Two compose-level corrections relative to the ticket spec were
required to get past tool/image issues on macOS + arm64:

1. `minio/mc:RELEASE.2024-12-13T22-19-12Z` does not exist on Docker
   Hub (the minio/mc side releases under `2024-12-14T21-28-49Z`, but
   that tag has also been removed at time of writing). Using
   `minio/mc:latest` — the image is only used for one-shot `mc mb`.
2. The ticket specified the Shelf plugin would consume
   `shelf.endpoint.base-url=...` +
   `shelf.membership.resolver.endpoints=...`. The real key registry
   in `ShelfConfig.java` uses `shelf.endpoint=host:port` (plain
   host:port, not URL) and does not expose
   `shelf.membership.resolver.*` at all — membership resolution is
   driven entirely by DNS + `/stats` polling (SHELF-20). The written
   `shelf.properties` under `config/trino/etc/plugin/shelf/` uses
   the real keys. This file is inert in Trino today (Trino does not
   auto-consume per-plugin properties dirs for this plugin), but it
   is the canonical source of truth for when the plugin's FS
   factory is registered.
3. shelfd exposes `/metrics` on the data-plane listener (port 9090)
   per `shelfd/src/http.rs`, not on the control-plane listener (the
   control listener in the current tree is a stub). The compose file
   maps host `127.0.0.1:9091 → container 9090` so the smoke driver
   hits the right endpoint.

## Results

Each `run-smoke.sh` invocation writes under
`benchmarks/smoke/results/`:

```
results/
├── cold/NN.txt                     # 10 per-query CSV_HEADER outputs
├── warm/NN.txt                     # same, second run
├── metrics-after-cold.txt          # shelfd /metrics after cold queries
└── metrics-after-warm.txt          # shelfd /metrics after warm queries
```

Results are `.gitignore`d; this directory exists only on a live run.

Observed totals on the last run captured in the commit description:

- Query correctness (10/10): cold == warm, byte-identical.
- `shelf_hits_total{pool="metadata"}`: cold **0** → warm **0**
- `shelf_hits_total{pool="rowgroup"}`: cold **0** → warm **0**
- Conformance assertion: **FAIL** with the documented error, per
  Blockers A + B above.
- Wall-clock for one full smoke loop (after images pulled +
  shelfd image cached): ~14 s.

## Footnote for SHELF-15 / SHELF-20

The harness is the deliverable for the deferred conformance tests
listed under SHELF-15 and SHELF-20 in `agents/out/03-plan.md`. The
cold-vs-warm loop and the `shelf_hits_total` gate will turn green
automatically when the upstream plugin wiring lands. Until then, the
assertion correctly fails loud so nobody accidentally declares the
gate "green by default".
