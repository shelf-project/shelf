# Shelf trainer — docs

This folder is the agent-6 documentation surface for the Shelf trainer
package.

Contents:

| File              | Status     | Purpose                                                                     |
|-------------------|------------|-----------------------------------------------------------------------------|
| `labels.md`       | material   | Label definition, prediction horizon, split, leakage controls, metrics.     |
| `runbook.md`      | stub       | What to do when the trainer alerts (Phase 4 on-call surface).               |

## How this doc set is meant to be read

- Anything touching admission labels **must** be reconciled with
  `labels.md`. A model change that does not also update `labels.md` is a
  bug.
- `runbook.md` is consumed by on-call. Agent-8 owns the Phase 4 wiring
  of alerts; agent-10 (scribe) edits the user-facing prose.
- Contract-surface documents (feature order, normalisation, artifact
  schema) live in `contracts/admission-model.md` at the repo root, **not
  here**.

## Reading order for a new engineer

1. `../README.md` — how to run the CLI locally.
2. `labels.md` — what we are predicting and why.
3. ADR-0003 in `shelf/agents/out/adr/` — why there is no ONNX MLP.
4. BLUEPRINT §7.3 — the original feature set.
5. Phase 4 tickets in `shelf/agents/out/03-plan.md` §4 — the work that
   turns each stub in `src/shelf_trainer/` into real code.
