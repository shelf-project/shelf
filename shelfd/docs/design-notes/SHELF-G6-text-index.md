# SHELF-G6 — Lucene-style text-index acceleration

> Status: **primitive scaffold landed**.
> Ships: `shelfd::text_index::{KeywordIndex, TextPattern,
> TextProbeRequest, TextProbeResponse}`, `POST /textindex/probe`
> HTTP endpoint, in-memory `HashMap<(table, column),
> KeywordIndex>` slot on `ServerState`.

## Goal

Close the Warp Speed gap on `LIKE` / prefix / suffix predicates
over text columns. BLUEPRINT §7.4 calls for a `Pool::TextIndex`
with a 10 GiB DRAM budget, per-column opt-in via an ops allowlist,
and Lucene-style probe semantics.

## Pattern surface (v1)

- `Exact(term)`
- `Prefix(term)` — `LIKE 'term%'`
- `Suffix(term)` — `LIKE '%term'`

Arbitrary infix patterns fall back to a full scan; the Trino
translator (G5 sibling) only emits probes for the three shapes
above.

## Engine choice — ADR-0010 (pending)

Two viable backends:

| Option                | Pros                                       | Cons                                                         |
| --------------------- | ------------------------------------------ | ------------------------------------------------------------ |
| **Tantivy in shelfd** | Pure Rust, no JVM, reuses `Pool::Metadata` | +8 MiB binary, ~80 MiB heap per hot column                   |
| **Lucene sidecar**    | Exact parity with Warp Speed               | Extra JVM pod, cross-process RTT on every `LIKE`, cold starts |

The scaffold ships a placeholder `KeywordIndex` so the HTTP
surface is exercisable today; the engine pick lands once the
benchmark harness (F2-F3) has numbers for a Tantivy proof of
concept on TPC-DS queries 28, 33, 91 (the `LIKE`-heavy set).

## What ships today

- `KeywordIndex::insert(term, ordinal)` / `maybe_match(pattern)`.
- `/textindex/probe` HTTP endpoint. Missing `(table, column)`
  responds `fail_open: true` and an empty `row_group_ordinals`.
- `ServerState.text_index` slot behind an `RwLock` so admin
  RPCs can hot-swap indexes.

## What does not ship yet

- Index build pipeline. The G2 producer note applies: fire
  build async after row-group admission, limit to allowlisted
  columns, bound per-column footprint.
- `Pool::TextIndex`. The scaffold lives in a free-standing
  `HashMap`; swapping to a pool-backed store is mechanical once
  admission policy sizes it.
- Trino-side translator. Parks with G5 until the upstream
  cache SPI lands or the `ConnectorFactory` wrapper path is
  blessed.
- Tantivy vs Lucene ADR-0010 decision.

## Test plan

- Exact / prefix / suffix lookups against a fixed index.
- Empty index returns `fail_open: true`.
- Footprint accounting grows monotonically with inserts.

End-to-end validation deferred to the Track G gate alongside G5.
