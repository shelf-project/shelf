# shelfd design notes

One file per `SHELF-NN` ticket lands in this directory. The pattern is
set by `agents/4-shelfd-builder.md` Pass 1 ("Design sketch") — a
one-page note written before code and kept in the PR.

## Index

No ticket-specific notes yet; this scaffold is the bootstrap pass. The
notes will land as each of SHELF-02 through SHELF-28 is picked up.

## Open decisions (scaffold-era)

Choices made during scaffolding that were not pre-answered by the plan
or an ADR. Each will be revisited by its owning ticket.

1. **`async_trait` vs RPITIT (return-position-`impl`-trait-in-traits).**
   Traits `Store` and `Origin` use `impl Future` directly. This keeps
   the scaffold dep-light but means the traits are not dyn-compatible.
   If we need `dyn Store` for test doubles, SHELF-NN will either add
   the `async_trait` crate or introduce a separate object-safe facade.

2. **Axum version.** Pinned to `axum = "0.7"`. `0.8` is out but trims
   some handler-signature ergonomics we want for SHELF-06. Re-evaluate
   once the handler bodies are real code.

3. **Prometheus crate.** Using the `prometheus` crate (0.13) rather
   than `metrics` / `metrics-exporter-prometheus`. Reason: the Alluxio
   → Shelf migration team already has Grafana dashboards keyed on the
   prometheus-text-format output exactly. Switch costs are paid if we
   move.

4. **Tonic as a workspace dep, feature-gated at the binary level.**
   The control-plane gRPC shape (SHELF-23) is not final; tonic is
   pulled in as a dep now so its version is fixed across the
   workspace, but the server is an `todo!()` stub. No proto files are
   committed yet (`contracts/protobuf/shelf.proto` lands with
   SHELF-23).

5. **No `fuzz/` directory yet.** `agents/4-shelfd-builder.md` Pass 3
   calls for fuzz targets on anything that parses S3 bytes / Parquet
   footers / Iceberg manifests. The scaffold has no parsers, so `fuzz/`
   is deferred to the first ticket that introduces one (likely
   SHELF-15 footer reader).

## Scaffold summary (2026-04-23)

### What was scaffolded

- Workspace `Cargo.toml` with `shelfd` + `shelfctl` members; shared
  deps centralised in `[workspace.dependencies]`.
- `rust-toolchain.toml` pinned to `stable` with `rustfmt` + `clippy`;
  MSRV tracked as `rust-version = "1.82"` in
  `[workspace.package]`.
- `.gitignore`, `deny.toml` (cargo-deny policy, permissive-only
  license allow-list), root workspace profile tuning.
- `shelfd/src/{lib.rs,main.rs,error.rs,config.rs,router.rs,store.rs,
  origin.rs,admission.rs,http.rs,control.rs,metrics.rs,
  membership.rs}` — each module compiles under `cargo check --all`;
  every public function body is `todo!()` with a ticket-tagged
  message.
- `shelfd/tests/smoke.rs` — integration test pattern; SHELF-12
  docker-compose test is `#[ignore]`'d.
- `shelfd/benches/hashring.rs` — criterion harness registered as
  `[[bench]]` in `shelfd/Cargo.toml`.
- `shelfd/docs/metrics.md` — initial metric dictionary scoped to
  phases 0 + 1.
- `shelfctl/Cargo.toml` + `src/main.rs` — clap-based CLI with
  `stats`, `pin`, `unpin`, `evict`, `ring`, `reload` subcommands, all
  `todo!()` stubs.
- `shelfd/README.md`, this file.

### What remains

Every runtime behaviour. The scaffold intentionally stops at the
module-boundary level so the v0.5 gate ADRs are encoded in types and
doc-comments before a single byte of cache logic is written. In
priority order (Phase 0 tickets):

- SHELF-02 — Axum server, graceful shutdown, config loader.
- SHELF-03 — DRAM-only Foyer pool with SIEVE.
- SHELF-04 — content-addressed key function + Java golden vectors.
- SHELF-05 — S3 origin client with pooled `HyperClient`.
- SHELF-06 — `GET /cache/…` with read-through + single-flight.
- SHELF-07 — `HEAD /cache/…`.
- SHELF-08 — Prometheus + OTel wiring.
- SHELF-09 — Dockerfile + base Helm chart (not in this crate; lives
  under `charts/shelf/`).
- SHELF-12 — docker-compose integration harness.

Phase 1 tickets SHELF-15 through SHELF-28 flesh out row-group
granularity, HRW hashing body, 3-pod StatefulSet, pin list loader,
size-threshold admission, replay benchmark, Grafana dashboard, and
chaos drills.

### Ticket → stub map

| Ticket        | File(s)                              | `todo!()` site(s)                                                       |
|---------------|--------------------------------------|-------------------------------------------------------------------------|
| SHELF-02      | `src/config.rs`, `src/http.rs`       | `Config::from_path`, `http::serve`, `http::handlers::readyz`           |
| SHELF-03      | `src/store.rs`                       | `FoyerStore::open` (shared), metadata pool path                        |
| SHELF-04      | `src/store.rs`                       | `store::key_from_tuple`                                                |
| SHELF-05      | `src/origin.rs`                      | `S3Origin::new`, `S3Origin::get_range`                                 |
| SHELF-06      | `src/http.rs`, `src/store.rs`        | `http::handlers::get_cache`, `FoyerStore::{get,insert}`                |
| SHELF-07      | `src/origin.rs`, `src/http.rs`, `src/head_lru.rs` | `S3Origin::head` (wired), `http::handlers::head_cache` (wired), `HeadLru` |
| SHELF-08      | `src/metrics.rs`                     | (skeleton populated; counters ready)                                   |
| SHELF-12      | `tests/smoke.rs`                     | `smoke_read_through_against_minio`                                     |
| SHELF-17      | `src/store.rs`                       | `FoyerStore::open` (shared)                                             |
| SHELF-18      | `src/store.rs`                       | `FoyerStore::open` (shared)                                             |
| SHELF-19      | `src/router.rs`, `benches/hashring.rs` | `Router::owner`, `Router::is_local_owner`, bench body               |
| SHELF-20      | `src/membership.rs`                  | `Resolver::spawn`                                                      |
| SHELF-23      | `src/control.rs`, `shelfctl/…`       | `control::serve`, every `shelfctl` subcommand                          |
| SHELF-24      | `src/admission.rs`, `src/control.rs` | `PinList::contains`, `PinListReloadHandle::reload`                     |
| SHELF-25      | `src/admission.rs`                   | `SizeThresholdPolicy::decide`                                          |

`cargo check --all` output: green (see `shelf/shelfd/docs/design-notes/BOOTSTRAP.md`
if we later capture the exact output of the first CI run).
