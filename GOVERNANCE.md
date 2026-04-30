# Governance

Shelf is an open-source project. This file describes how decisions get made,
who can make them, and how the project's governance evolves over time. It
is intentionally short — when the rules are simple, people read them.

## Operating model

Shelf is in **Year-1 BDFL governance**. A single Benevolent Dictator For
Life (currently year-bounded — see `MAINTAINERS.md`) holds final authority
on technical direction, release cuts, and conduct escalations. The BDFL's
role is to keep the project moving and unblock disputes; it is not a
license to override consensus when consensus exists.

The project transitions to a **Project Management Committee (PMC)** once
five non-BDFL maintainers have each sustained the role for 12 weeks (see
`MAINTAINERS.md` for the exact criteria). After that point this file is
updated by PR to reflect the PMC structure, and the BDFL becomes a regular
PMC member with one vote.

## Day-to-day decisions: lazy consensus

Most changes ship under **lazy consensus**:

1. A contributor opens a PR.
2. CODEOWNERS-listed maintainers (see `.github/CODEOWNERS`) review.
3. If at least one maintainer approves and **no maintainer objects within
   72 hours** of the request-for-review, the PR may be merged.
4. A request for changes from any maintainer blocks the merge until
   addressed or withdrawn.

PRs that touch governance (`GOVERNANCE.md`, `MAINTAINERS.md`,
`.github/CODEOWNERS`) require **two maintainer approvals**, one of which
must be the BDFL during Year 1.

## Larger changes: RFCs

For changes that meaningfully affect users, operators, or downstream
integrations — wire formats, public APIs, durable storage layouts, security
posture, project scope — the contributor must first land an Architecture
Decision Record (ADR) in `agents/out/adr/`.

The ADR flow is:

1. Open a PR adding a new file under `agents/out/adr/NNNN-title.md`,
   following the template already present there.
2. Discussion happens on the ADR PR. Anyone may comment; maintainers'
   approvals are what gate merge.
3. The ADR is merged in `Proposed` state, then updated to `Accepted` (or
   `Rejected`) once consensus has formed. Implementation PRs reference the
   ADR by number.

ADRs are how Shelf records *why* it is the way it is. Implementation PRs
that contradict an `Accepted` ADR must first land an ADR amendment.

## Disputes

Most disputes are resolved on the PR or ADR thread. When that fails:

1. **Cool-down**: any participant may pause the thread for up to 7 days by
   saying so on the thread. Use this when the discussion has stopped
   making progress.
2. **Maintainer call**: any maintainer may call for a synchronous
   discussion (video or an RFC issue) and summarise the outcome on the
   original thread. The summary is binding unless rebutted in the same
   thread within 72 hours.
3. **BDFL tie-break (Year 1)**: if maintainers cannot agree, the BDFL
   decides. The decision is recorded as a comment on the relevant PR or
   ADR and, if architecturally significant, captured as an ADR amendment.
4. **PMC vote (post-PMC)**: a simple majority of PMC members. Ties fail.

Conduct disputes follow `CODE_OF_CONDUCT.md` and route to
`conduct@shelf-project.dev`, not to the technical dispute path above.

## Changes to this file

Changes to `GOVERNANCE.md` are themselves governance changes. They follow
the "two maintainer approvals" rule above and, during Year 1, require BDFL
sign-off.
