# Maintainers

Shelf is currently in **Year-1 BDFL governance**. One named maintainer holds
final say on technical direction, release cuts, and code merges, with the
explicit intent to graduate the project to a Project Management Committee
(PMC) once the maintainer pool has matured (see "Trajectory" below).

This file is the source of truth for who is on the hook. If a name appears
here it means the person has accepted both the responsibility and the
notification load that comes with it.

## Year-1 BDFL

| Role | Handle      | Email                       |
|------|-------------|-----------------------------|
| BDFL | `@aamir306` | `aamir.siddiqui@pw.live`    |

The BDFL:

- Has final say on architecture, releases, and conduct decisions.
- Is the default escalation path for any unresolved review thread.
- Is on the hook for the security inbox during Year 1 (see `SECURITY.md`).

## Module ownership

Until additional maintainers are added, every area routes to `@aamir306`. As
new maintainers come in, this table is the primary lever for distributing
responsibility — entries are updated in the same PR that adds a new
maintainer.

| Area        | Path             | Owner       |
|-------------|------------------|-------------|
| Daemon      | `shelfd/`        | `@aamir306` |
| Trino plugin| `clients/`, `plugins/` | `@aamir306` |
| Helm charts | `charts/`        | `@aamir306` |
| Docs        | `docs/`          | `@aamir306` |
| CI / repo   | `.github/`       | `@aamir306` |

## Becoming a maintainer

We do not gate on velocity or surface area. We gate on *sustained, technical
contribution* and on whether existing maintainers trust the candidate to
review code in their own area.

A contributor becomes a maintainer when **all** of the following hold:

1. **Twelve weeks** of substantive contribution. "Substantive" means
   non-trivial PRs landed, design discussion participation, issue triage, or
   review comments that meaningfully changed a PR — not drive-by typo fixes.
2. **Two existing maintainer +1s** on a maintainer-nomination issue. During
   Year 1, one of those +1s must be the BDFL.
3. The candidate has explicitly accepted the responsibility, including the
   pager / security-inbox rotation expectations once those exist.

A maintainer is added by a PR that updates this file *and* the CODEOWNERS
table in `.github/CODEOWNERS`. The PR is the ratification.

## Stepping down

A maintainer may step down at any time by opening a PR that removes their
entry. The BDFL (or, post-PMC, the PMC) may move a maintainer to *emeritus*
status after **90 days of inactivity** with no objection from the maintainer
themselves; emeritus maintainers retain credit in the project but lose merge
rights and CODEOWNERS coverage.

## Trajectory: BDFL → PMC

Year-1 BDFL is a deliberately temporary structure. Shelf transitions to a
PMC when:

- **Five non-BDFL maintainers** have each sustained the maintainer role for
  at least **12 weeks**, and
- The cohort agrees in writing (a PR to `GOVERNANCE.md`) that the project
  is ready to operate by lazy consensus among peers without a tie-breaker.

At that point the BDFL becomes a regular PMC member, the PMC adopts the
governance flow described in `GOVERNANCE.md`, and this file is updated to
reflect the new structure.

## Contact

- General: open an issue or a discussion.
- Security: see `SECURITY.md`.
- Conduct: `conduct@shelf-project.dev` (see `CODE_OF_CONDUCT.md`).
