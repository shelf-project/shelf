# shelf-trino-plugin — PR descriptions

One markdown file per ticket, named `SHELF-NN.md`. Agent-5 Pass 5 populates
these; reviewers paste the body into the GitHub PR description.

Expected structure for each file:

```markdown
# SHELF-NN — <short title>

## Design note
Link to docs/design-notes/SHELF-NN-<slug>.md.

## Test evidence
- Unit: …
- Integration: …
- Property (if applicable): …
- Load (if applicable): …

## Acceptance criteria
- [ ] …
- [ ] …

## Risk + rollback
- Config flag `…` reverts to pre-ticket behaviour.
- No schema change / no public-API break.
```

The skeleton commit does not ship any ticket-level PR; this folder is the
landing spot for the first Phase 0 ticket PR (`SHELF-10`).
