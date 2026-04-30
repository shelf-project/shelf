# ADR 0023: CI tripwire — every Cargo workspace member must be in shelfd/Dockerfile's build context

_Status: Accepted (2026-04-30)_
_Deciders: shelfd-maintainers_
_Supersedes: none_
_Superseded-by: none_

## Context

`shelfd/Dockerfile` is multi-stage. The builder stage runs
`cargo build --release --bin shelfd --locked`, which requires the
**entire** workspace graph to resolve — even though we only ship
the `shelfd` binary, Cargo refuses to build if any member declared
in `Cargo.toml#workspace.members` is missing from the build
context.

The Dockerfile currently lists each member by hand:

```
COPY shelfd ./shelfd
COPY shelfctl ./shelfctl
COPY shelf-advisor ./shelf-advisor
COPY crates ./crates
```

This list has drifted from the workspace twice now:

1. **PR #33 (SHELF-29 cut)** — `shelf-advisor` joined the
   workspace. The Dockerfile COPY was missed at first; the build
   failed at `cargo build` with `failed to load manifest for
   workspace member ... shelf-advisor/Cargo.toml: not found`.
   Caught manually during the rc.4 image-build job. Fixed in
   commit 36fdb8a.
2. **PR #68 (SHELF-40)** — `crates/shelf-cost` joined the workspace.
   Same failure mode. Fixed on `release-prep/1.0.0-rc.5` in commit
   9b066a2 — but `main` was left broken until rc.6 P0.4 (this
   ADR's PR).

Both incidents follow the same pattern: a contributor adds a member
to `Cargo.toml#workspace.members`, the workspace builds locally
(because the file system already has the directory), CI's
`cargo` jobs pass (same reason), and the regression only surfaces
in the **image-build** job — which on the free GHA runner takes
~12-30 minutes (linux/amd64 release build with full dep graph).
That is an expensive failure to trip on.

## Decision

A new workflow `.github/workflows/dockerfile-tripwire.yml` runs on
every PR that touches `Cargo.toml`, `shelfd/Dockerfile`, or itself.
The single job:

1. Parses `Cargo.toml#workspace.members` via Python's stdlib
   `tomllib` (Python 3.11+, set up on `ubuntu-22.04` via
   `actions/setup-python@v5`).
2. Parses `shelfd/Dockerfile` for `COPY <src...> <dest>` directives,
   skipping `COPY --from=<stage>` (those refer to a build stage,
   not the build context).
3. Asserts every workspace member is **covered** by at least one
   COPY source. Coverage is exact-match OR
   the COPY source is a parent directory of the member
   (`COPY crates ./crates` covers `crates/shelf-cost` and any
   future `crates/*` member).

The workflow exits non-zero with a clear `::error::` annotation
naming the uncovered members and a one-line fix instruction
(`Fix: add COPY <member> ./<member>` or a parent path).

## Why a separate workflow rather than a step in helm-lint.yml

helm-lint.yml's trigger paths and job times are already tuned for
the chart story. Workspace-graph coverage is orthogonal — adding a
new member doesn't always touch charts/, and adding a chart change
shouldn't gate on Cargo metadata. Separation also lets the tripwire
fail fast (~10 s including Python setup) without contending with
the slower helm jobs.

## Alternatives considered

- **Replace the explicit COPY list with `COPY . .`** — fastest fix,
  but blows out the Docker-layer cache on every source change and
  ships the whole repo (`docs/`, `agents/`, `infra/penpencil/`,
  worktrees) into the build context. Currently each `COPY <member>
  ./<member>` invalidates the layer only when its own subtree
  changes; we keep that property.
- **Run `cargo metadata --format-version 1`** to enumerate members
  — would need the full Rust toolchain in the workflow (~3 minute
  install) and produces a JSON form that needs additional parsing
  to extract relative paths. The Python stdlib `tomllib` path is
  faster and has no toolchain dep.
- **Lint inside the existing helm-lint.yml `helm-template` job** —
  rejected for the orthogonality reason above.

## Side effect of this PR

This PR also adds the missing `COPY crates ./crates` line to
`shelfd/Dockerfile`. Without that, the tripwire would correctly
fail on `main`, but `main` would be broken in a different way (the
image build would fail). Adding the line restores `main` to a
buildable state and lets the tripwire pass. The fix has been live
on the rc.5 release-prep branch since 2026-04-30; this is the
backport to `main`.

## Consequences

- **Positive**: any future workspace member addition that forgets
  the Dockerfile COPY trips a 10-second CI failure with a clear
  fix message — instead of a 25-minute image-build failure during
  a release tag.
- **Negative**: contributors who add a top-level workspace member
  must also add a Dockerfile COPY. Documented in the workflow
  failure message and in this ADR.

## Test plan (validated locally during PR authoring)

- **Pass case**: current `Cargo.toml` (`shelfd, shelfctl,
  shelf-advisor, crates/shelf-cost`) + current Dockerfile
  (with `COPY crates ./crates`) → tripwire passes.
- **Regression case** (synthetic): drop `COPY crates ./crates`
  from a copy of the Dockerfile → tripwire fails with
  `::error:: missing COPY for crates/shelf-cost`.
- **New-member case** (synthetic): add a top-level dummy member
  `_tripwire_test` to `Cargo.toml` → tripwire fails with
  `::error:: missing COPY for _tripwire_test`. (Adding under
  `crates/_tripwire_test` correctly passes because the existing
  `COPY crates ./crates` is a parent.)

## References

- `Cargo.toml#workspace.members` — the source of truth.
- `shelfd/Dockerfile` lines 83-86 — the COPY block this guards.
- `.github/workflows/dockerfile-tripwire.yml` — the gate.
- Workspace memory `/Users/aamir/trino/AGENTS.md` — locked the
  rule on 2026-04-30 after the rc.5 incident.
