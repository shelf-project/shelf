# Contributing to Shelf

First: thank you. A cache worth contributing to is a cache worth using.

This is a short, operational guide. The goals are:

1. You can build and test the component you changed without reading the whole tree.
2. A good bug report gets triaged in hours, not days.
3. A design change is debated against a written ADR, not against vibes.

## Code of conduct

All participation is governed by the [Contributor Covenant 2.1](./CODE_OF_CONDUCT.md). Violations go to `conduct@shelf-project.dev`.

## Governance and decision process

See [docs/governance.md](./docs/governance.md). TL;DR: BDFL for year one, PMC afterwards; monthly roadmap cadence; weekly office hours.

## Dev environment

You need:

| Component | Tool | Version |
|-----------|------|---------|
| Rust toolchain | `rustup` | pinned by `rust-toolchain.toml` |
| Java build | JDK | 22 (Trino 480 LTS target) |
| Java build | Maven | 3.9.x |
| Containers | Docker or Podman | any recent |
| Local K8s | `k3d` ≥ v5 or `kind` ≥ v0.22 | for the quickstart and integration tests |
| Helm | `helm` | 3.x |
| Task runner | `task` (go-task) | 3.x (for top-level `task <target>`) |

Optional but recommended: `cargo-nextest`, `cargo-deny`, `cargo-audit`, `mdformat`, `mkdocs-material`.

Clone and bootstrap:

```bash
git clone https://github.com/shelf-project/shelf.git
cd shelf
rustup show                                               # installs pinned toolchain
```

The first full `cargo build` and `mvn verify` runs are documented as "will be executable after phase 0" (tracked in `SHELF-01`). Until then, the workspace compiles empty binaries.

## Building and testing per component

Every component lives under its own directory and is buildable in isolation. Every command below is tracked against a Phase-0 ticket.

| Component | Path | Build | Test |
|-----------|------|-------|------|
| `shelfd` (Rust cache server) | `shelfd/` | `cargo build -p shelfd` | `cargo nextest run -p shelfd` |
| `shelfctl` (admin CLI) | `shelfctl/` | `cargo build -p shelfctl` | `cargo nextest run -p shelfctl` |
| Trino plugin (Java) | `clients/trino/` | `mvn -pl clients/trino -am package` | `mvn -pl clients/trino test` |
| Python trainer | `clients/python/trainer/` | `uv sync` | `uv run pytest` |
| Helm chart | `charts/shelf/` | `helm lint charts/shelf` | `helm template charts/shelf` |
| Smoke harness | `benchmarks/smoke/` | `task smoke:build` | `task smoke:run` |

> **Will be executable after phase 0.** Commands above are the target contract (tracked in SHELF-01..SHELF-12). Anything marked `TODO_SHELF-NN` means the binary does not yet exist; run the command to see its ticket ID in the failure message.

## Running the full integration harness

```bash
task integration           # TODO_SHELF-12 — docker-compose smoke
```

This brings up Trino 480, `shelfd`, and MinIO, loads three Iceberg tables, and runs the 10-query smoke set. On the second run, `shelf_hits_total > 0` should appear in the Prometheus snapshot.

## Filing a bug

Please use one of the issue templates in [`.github/ISSUE_TEMPLATE/`](./.github/ISSUE_TEMPLATE/):

- `bug_report.md` — unexpected runtime behaviour.
- `regression.md` — the same call used to work and now doesn't; include the last-known-good version.
- `feature_request.md` — new behaviour.

A good bug report has:

1. Shelf + Trino + Iceberg + Foyer versions.
2. What you ran. The fewer lines of SQL / config, the better.
3. What you expected, what you saw, and — critically — the Grafana panel `shelf-overview` screenshot or the two or three Prometheus series that show the failure.
4. Logs from the affected `shelfd` pod at `RUST_LOG=info` or higher, preferably with the `x-amz-request-id` of any S3 call involved.
5. The SLO that was violated. Cache hit rate, p95 latency, fallback rate. If nothing is measurably worse, you probably don't have a bug; you have a preference.

Do **not** file the same bug twice — comment on the existing issue. Do **not** paste credentials, tokens, or IRSA ARNs; we redact, but you shouldn't make us.

Security issues go to [SECURITY.md](./SECURITY.md), not to the public tracker.

## Proposing a design change: the ADR process

Any change that affects:

- the on-disk or on-wire format,
- the plugin contract (Java API seen by `TrinoFileSystem` consumers),
- the admission policy,
- the consensus model,
- or the public HTTP/gRPC API,

requires a written ADR before code merges.

1. Copy `docs/adr/template.md` (to be added by SHELF-29) to `docs/adr/NNNN-short-title.md` using the next integer.
2. Fill in Context, Decision, Consequences, Alternatives considered, and References.
3. Open a PR titled `adr: NNNN short title`. Link the tracking issue. Solicit reviewers listed in `CODEOWNERS`.
4. Status field: `Proposed` while in review, `Accepted` when merged, `Superseded by NNNN` when later overridden.
5. Code implementing the ADR can ship in the same PR or a follow-up; the ADR must land first.

The ten ADRs that produced v0.4 of the blueprint live at [docs/adr/](./docs/adr/) — skim them as worked examples before writing your own.

## Coding standards

- **Rust.** `cargo fmt` + `cargo clippy --all-targets -- -D warnings`. No `unsafe` without a `SAFETY:` comment. No `unwrap()` outside tests. Every public item has a rustdoc comment.
- **Java.** `mvn spotless:apply` before commit. `javadoc` on every public class. No static mutable state.
- **Python.** `ruff check && ruff format`. Typed, `mypy --strict` on the trainer module.
- **Shell.** `shellcheck` clean. Every script starts with `set -euo pipefail`.
- **Markdown.** `mdformat` (via `pre-commit`). US English spelling.
- **Commits.** Conventional-commit style (`feat(shelfd): ...`, `fix(plugin): ...`). One logical change per commit. Rebased against `main` before merge.

## Good first issues

Look for the `good-first-issue` label on GitHub. Every one has:

- A clear acceptance criterion.
- A named mentor from CODEOWNERS.
- An effort estimate ≤ 1 day.

Typical candidates in Phase 0: documentation polish, metric dictionary entries, `shelfctl --help` examples, `mdformat` cleanups, new unit tests for the circuit-breaker reference class (SHELF-11).

## Release process

Automated via [`release-please`](https://github.com/googleapis/release-please) (tracked in SHELF-01). Humans do not tag releases; the bot opens a PR based on conventional-commit history. Maintainers merge it; signed Docker images and the Helm chart publish via OCI.

---

Questions we haven't answered? Ask in office hours (calendar in [docs/governance.md](./docs/governance.md)) or open a discussion.
