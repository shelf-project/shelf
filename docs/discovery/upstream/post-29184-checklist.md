# Trino #29184 — upstream engagement checklist (Fix 7)

Blob-cache SPI is **draft** on [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184). This repo tracks Shelf-side readiness separately from merge state.

## Human steps (cannot be fully automated)

1. **PR comment** — Paste the technical review body from `docs/discovery/upstream/29184-review-comment.md` (adjust if the PR body moved).
2. **Slack** — DM `@wendigo` on [trino.io/slack](https://trino.io/slack) with a one-paragraph context + link to Shelf’s blob-cache interest.
3. **Watch SPI** — When `Plugin.getBlobCacheManagerFactories()` (or equivalent) lands, replace `ShelfBlobCacheManagerFactoryStub` with a real factory wired from `ShelfPlugin`.

## Code seam

- `clients/trino/.../ShelfBlobCacheManagerFactoryStub.java` — documents intended packaging; compiles today without draft SPI classes.

## Do not

- Pin production clusters to unmerged Trino forks without an explicit operator window.
- Post penpencil-internal hostnames or ARNs into public GitHub comments.
