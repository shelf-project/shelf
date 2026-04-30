# Shelf agents

Ten composable agent prompts that take `shelf/BLUEPRINT.md` from
design draft to a public OSS release. They run in order; later agents
consume earlier agents' outputs under `shelf/agents/out/`.

```
                BLUEPRINT.md
                     │
         ┌───────────┴───────────┐
         │      DESIGN CHAIN     │   (must run in sequence)
         │                       │
   ┌─────▼────┐   ┌──────────┐   ┌──────────┐
   │ 1. scien │──►│ 2. crit  │──►│ 3. plan  │
   │   tist   │   │  thinker │   │   ner    │
   └──────────┘   └──────────┘   └─────┬────┘
                                       │
         ┌──────────────┬──────────────┼──────────────┐
         │              │              │              │
         │         BUILD TRACKS (parallel after 3)    │
         │              │              │              │
   ┌─────▼────┐   ┌─────▼────┐   ┌─────▼────┐   ┌─────▼────┐
   │ 4. shelfd│   │ 5.plugin │   │ 6.trainer│   │ 7.bench  │
   │  builder │   │  builder │   │  builder │   │  marker  │
   └─────┬────┘   └─────┬────┘   └─────┬────┘   └─────┬────┘
         │              │              │              │
         └──────────────┴──────┬───────┴──────────────┘
                               │
             ┌─────────────────┼─────────────────┐
             │         SHIP TRACKS               │
             │                 │                 │
       ┌─────▼────┐       ┌────▼─────┐      ┌────▼─────┐
       │  8. op-  │       │ 9. sec-  │      │ 10.      │
       │  erator  │       │ auditor  │      │  scribe  │
       └──────────┘       └──────────┘      └──────────┘
```

## The cast

| #  | File                         | Primary phase(s) in BLUEPRINT.md          | Primary output |
| -- | ---------------------------- | ----------------------------------------- | -------------- |
| 1  | `1-scientist.md`             | Design (pre-phase-0)                      | `out/01-scientist-review.md` |
| 2  | `2-critical-thinker.md`      | Design (pre-phase-0)                      | `out/02-critical-review.md` |
| 3  | `3-planner.md`               | Design (pre-phase-0)                      | `out/03-plan.md`, `out/BLUEPRINT-DIFF.md`, `out/adr/*`, `BLUEPRINT.md` (patched) |
| 4  | `4-shelfd-builder.md`        | Phases 0, 1, 3, 4, 8 (and 10 MV-refresh)  | `shelfd/`, `shelfctl/`, `shelf-mv-refresh/`, `snapshot-watcher/` |
| 5  | `5-plugin-builder.md`        | Phases 0, 1, 2, 8 (filter probe)          | `clients/trino/` JARs |
| 6  | `6-trainer-builder.md`       | Phases 4, 8 (bloom recommender), 9 (MV recommender) | `clients/python/trainer/` |
| 7  | `7-benchmarker.md`           | Phases 1+ (runs through every release)    | `benchmarks/` harness + results |
| 8  | `8-operator.md`              | Phases 5, 6, 10                           | `charts/`, `observability/`, `runbooks/` |
| 9  | `9-security-auditor.md`      | Phases 5, 7 (pre-launch), 10 (result cache) | `SECURITY/`, CI policy |
| 10 | `10-scribe.md`               | Phase 7, refreshed every release          | `docs/`, `blog/`, TIP, launch plan |

BLUEPRINT.md phases referenced above (see BLUEPRINT §12):

- −1 stabilise existing tools, 0 POC, 1 columnar, 2 plan-aware prefetch,
  3 ring + Raft, 4 learned admission, 5 prod rep-2, 6 roll out,
  7 OSS launch, 8 approximate in-cache indexes, 9 MV-aware caching,
  10 incremental MV refresh on snapshot delta.

## Milestones (aligned with BLUEPRINT.md v0.3)

| Milestone | BLUEPRINT phases reached | Elapsed | Agents active |
|---|---|---|---|
| **v0.1 POC** | 0 (+ stabilisation of phase −1) | 2-3 weeks | 1, 2, 3 (design) then 4, 5, 7 (overhead-only) |
| **v0.5 rep-2 Alluxio replacement** | through 5 | ≈ 2-3 months | adds 6, 8 |
| **v1.0 public OSS launch** | through 7 | ≈ 5 months | adds 9, 10 |
| **v1.5 gap-closers** | 8 + 9 | ≈ 7 months | extends agents 4, 5, 6 with Phase 8/9 tickets |
| **v2.0 incremental MV refresh (TIP)** | 10 | ≈ 9-10 months | extends agents 4, 8; agent 10 drafts the TIP |

## How to run one

Open a fresh chat (or dispatch via the `Task` tool):

> Read and follow `/Users/aamir/trino/shelf/agents/<N>-<name>.md`
> exactly. Produce the output(s) at the paths specified in the file.
> Do not modify BLUEPRINT.md unless the file explicitly says you own it
> (only agent 3 does).

Design agents (1, 2, 3) are single-shot per amendment cycle. Build
agents (4, 5, 6, 7) are dispatched **per ticket** — you'll invoke
agent 4 many times as phases 0, 1, 3, 4, 8 progress. Ship agents
(8, 9, 10) are hybrid: mostly single-shot but refreshed at every
tagged release.

## Amendment flow (when BLUEPRINT changes)

Two paths. Pick the right one; do not skip.

- **Major amendment.** New phase, new killer feature, algorithm swap,
  or any scope the team hasn't seen before. → Run the full chain
  (1 → 2 → 3). Agent 3 applies the diff to `BLUEPRINT.md`, bumps the
  version header, and archives `out/*` under `out/archive/v<N>/`.
- **Minor amendment.** Numeric correction, clarification, wording, or
  ordering tweaks inside an existing section. → Skip agents 1 and 2.
  Write the change directly to `out/BLUEPRINT-DIFF.md` plus a short
  ADR. Agent 3 (or the blueprint owner) applies it, bumps the patch
  version (v0.3 → v0.3.1), and notes the change in the header log.

Anything in between ("new sub-section §7.6", say) defaults to the
major path unless the planner explicitly judges it minor in the ADR.

## Who owns BLUEPRINT.md

- The **planner (agent 3)** is the only agent that writes to
  `BLUEPRINT.md`, and only at the end of its run after producing
  `BLUEPRINT-DIFF.md`. Every other agent reads BLUEPRINT.md and may
  read the open diff, but must not modify it.
- Build and ship agents read the **BLUEPRINT.md + ADRs + any open
  `BLUEPRINT-DIFF.md`**. They do **not** consult the critic's or
  scientist's raw outputs as design source of truth; those are
  reference material, not decisions.

## Cross-agent contracts (shared interfaces)

Every interface shared between agents lives under `contracts/` at the
repo root, not in any single agent's doc folder. This prevents the
"agent 4 publishes under `shelfd/docs/...` and agent 5 happens to
know that" brittleness.

| Path | Owner | Consumers |
|---|---|---|
| `contracts/protobuf/shelf.proto` | agent 4 | agents 5, 7 |
| `contracts/flight/schemas/` | agent 4 | agents 5, 7 |
| `contracts/metrics.md` | agent 4 (shelfd), agent 5 (plugin) | agent 8 |
| `contracts/config-keys.md` | agents 4, 5 | agent 8, agent 10 |
| `contracts/admission-model.md` (feature order, ONNX version) | agent 6 | agent 4 |
| `contracts/slos.md` | agent 3 (authoritative), updated by agent 7 if measurements contradict | agents 8, 10 |
| `contracts/errors.yaml` (typed error codes) | agent 4 | agents 5, 8 |

A change to anything under `contracts/` follows the amendment flow
above. Silent contract changes are a merge-blocker.

## Feedback loop (ship → design)

Once per tagged release, agents 8 and 9 each write a
`feedback/RELEASE-v<N>.md` with:

- Design assumptions that did not survive production.
- SLOs that proved wrong (too strict, too loose).
- Security findings discovered post-design.

The planner reads these at the start of the next amendment cycle.
This is the only mechanism that sends information **backwards**
through the chain.

## Rules every agent shares

- Never edit `BLUEPRINT.md` directly (only agent 3 does, once per
  amendment cycle).
- Read another agent's **output** (declared artefact), never its
  **prompt**. If you find yourself reading a sibling agent's
  `*.md` prompt, stop — you are second-guessing a role boundary.
- Every numeric or factual claim in an output must be cited — to a
  paper, a benchmark run ID, a commit SHA, a production log, or a
  GitHub PR / issue URL.
- Every recommendation is actionable: a decision, a ticket, or an
  experiment with a measurable outcome.
- When a later agent disagrees with an earlier one, it says so
  explicitly with a one-line reason. No papering-over.
- Research questions are the scientist's to settle; engineering
  questions are the critic's; scheduling and scope are the
  planner's; anything after phase 0 is the relevant build/ship
  agent's.
- Cross-agent interfaces live under `contracts/` (above).
