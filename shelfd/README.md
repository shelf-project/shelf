# shelfd

`shelfd` is the Rust cache daemon of the [Shelf](../BLUEPRINT.md) project —
an Iceberg-native, row-group-granular read cache for Trino. This
crate scaffolds phases 0 and 1 of
[`agents/out/03-plan.md`](../agents/out/03-plan.md).

This is the **initial scaffold**, not a working cache. Function bodies
are `todo!()` stubs annotated with the `SHELF-NN` ticket that will
implement them.

## Layout

```
shelfd/
├── src/
│   ├── admission.rs   # SHELF-24 / SHELF-25, ADR-0003
│   ├── config.rs      # SHELF-02
│   ├── control.rs     # SHELF-23 admin surface
│   ├── error.rs       # typed top-level error
│   ├── http.rs        # SHELF-02 / SHELF-06 / SHELF-07, ADR-0004
│   ├── lib.rs         # re-exports
│   ├── main.rs        # binary entry point
│   ├── membership.rs  # SHELF-20, ADR-0001
│   ├── metrics.rs     # SHELF-08
│   ├── origin.rs      # SHELF-05
│   ├── router.rs      # SHELF-19, ADR-0002
│   └── store.rs       # SHELF-03 / SHELF-17 / SHELF-18, ADR-0008 + 0009
├── benches/
│   └── hashring.rs    # criterion skeleton (SHELF-19)
├── tests/
│   └── smoke.rs       # integration test pattern (SHELF-12)
└── docs/
    ├── design-notes/
    └── metrics.md
```

## Build

```bash
cd shelf/
cargo check --all          # must be green on stable
cargo build -p shelfd      # build the binary
cargo test -p shelfd       # fast tests (ignored tests skipped)
cargo test -p shelfd -- --ignored  # integration tests (need docker)
cargo bench -p shelfd      # criterion benches
cargo clippy --all -- -W clippy::all  # style
cargo deny check            # policy (see ../deny.toml)
```

## Running the binary

`shelfd` takes a YAML config. The loader lands in SHELF-02; until then
the binary logs a startup line and exits.

```bash
RUST_LOG=info,shelfd=debug cargo run -p shelfd -- \
    --config examples/config.yaml
```

## Architectural constraints (scope locks)

These override `BLUEPRINT.md` where they disagree. See
`../agents/out/adr/` for the full decisions.

| ADR  | Constraint |
|------|------------|
| 0001 | No embedded Raft. Membership is K8s headless service + ConfigMap. |
| 0002 | HRW hashing (capacity-weighted), not a 2000-vnode ring. |
| 0003 | Size-threshold admission in v1, no ONNX MLP. |
| 0004 | HTTP/2 only. No Arrow Flight in v1. |
| 0008 | Two Foyer pools only: `metadata` (DRAM) + `rowgroup` (hybrid). |
| 0009 | Foyer's built-in S3-FIFO on NVMe. No GL-Cache. |

## Status

Scaffolding complete, no runtime behaviour implemented. See
[`docs/design-notes/README.md`](docs/design-notes/README.md) for the
inventory of `todo!()` sites mapped to ticket IDs.
