# Shelf — OSS launch playbook

**Status**: draft plan, not yet executed.
**Owner**: shelf-core (single accountable human, not a rotation).
**Target launch window**: ≥ 30 days after the 14-day post-rollout soak
lands green (see [rollout-v1/v0.5-promote.md](../rollout-v1/v0.5-promote.md))
AND ≥ 30 days after the last Shelf-attributed production page.

The rest of this doc is the plan, not the narrative. For the "why" +
product framing see [COMPARISON.md](../../COMPARISON.md) and
[README.md](../../README.md).

**Important positioning.** Shelf is a **neutral open-source project**.
It originated on an internal penpencil Trino-on-EKS footprint, which
is how the rollout-v1 production evidence exists — but the project
is not a penpencil product, does not ship under a penpencil domain,
and its governance does not report to penpencil. The internal
rollout story is referenced (honestly) in the announce post as
real-world production evidence; the project itself lives on its
own identity from T-0.

---

## 0. Decisions — locked

These are the four decisions that bound everything downstream. They
are settled; do not reopen without a written ADR.

| Decision | Locked choice | Why |
| -------- | ------------- | --- |
| **GitHub org** | **`github.com/shelf-project/shelf`** — new neutral org, stood up before T-0 | Project is independent of the org it originated in. Keeping the repo on a company-owned org would read as a vendor project and muddy governance. One-time org-standup cost (~1 day) beats perpetual "is this a penpencil thing?" question on every thread. |
| **Public domain** | **`shelf-project.dev`** — registered, HTTPS-required, matches org name | .dev TLD is inexpensive (~$12/yr), mandatory HSTS makes http→https leaks impossible, signals "developer tool" at a glance. Domain decision doubles as the `conduct@` and `security@` inbox host. |
| **Contributor agreement** | **DCO** (Developer Certificate of Origin), enforced by [`dcoapp/app`](https://github.com/dcoapp/app) as a required status check | No CLA bot, no corporate signing process, no entity behind the project. `Signed-off-by:` in commit trailers is the Linux/kernel/k8s/bazel standard. |
| **Governance model year 1** | **BDFL (shelf-core) → PMC** after 5 non-BDFL maintainers sustain activity for 12 weeks | BDFL is fine for year 1 if explicitly time-bounded in [governance.md](../governance.md). Committing to a PMC before we have maintainers is theatre. |
| **Release cadence** | **full release on v1.0.0** at launch; monthly minors post-1.0; patches on-demand | Post-rollout soak + 30-day production quiet is sufficient evidence for API stability. Pre-1.0 would undersell the contract and invite "is this even ready?" threads. |
| **Launch tag** | **`v1.0.0`** — not an rc, not a v0.x | User-facing surfaces (S3 shim protocol, `shelfctl` CLI, metrics names, Helm values, chart API) have been stable for the 14-day soak. v1.0 commits to semver from here; the [blob-cache SPI work](../../clients/trino/docs/design-notes/SHELF-29-blob-cache-plugin.md) goes in v1.1 additively, not breakingly. |

If any of those six flip, walk §1-§7 again.

---

## 1. Pre-launch audits (T-28 days → T-14 days)

Four audit lanes, run in parallel by the same owner over two weeks.

### 1.1 Internal-identifier sweep

Problem: `penpencil` appears in ~30 files. Two classes:

**Class A — org/domain references that MUST change (hard links that
break or mislead external users)**:

| File | Reference | Replace with |
| ---- | --------- | ------------ |
| `Cargo.toml` | `repository = "https://github.com/penpencil-oss/shelf"` | `https://github.com/shelf-project/shelf` |
| `charts/shelf/Chart.yaml` | `home`, `sources`, `maintainers[].email` | `https://github.com/shelf-project/shelf`, `shelf-oncall@shelf-project.dev` |
| `charts/shelf/values.yaml` | `image.repository: ghcr.io/penpencil-oss/shelf/shelfd` | `ghcr.io/shelf-project/shelfd` |
| `charts/shelf/ci/lint-values.yaml` | same as values.yaml | same substitution |
| `clients/trino/pom.xml` | `<url>https://github.com/penpencil-oss/shelf</url>` | `https://github.com/shelf-project/shelf` |
| `shelfd/Dockerfile` | `org.opencontainers.image.{source,vendor,documentation}` | `shelf-project` everywhere |
| `shelfd/docs/design-notes/SHELF-09-dockerfile-and-helm-lint.md` | image path | `ghcr.io/shelf-project/shelfd` |
| `benchmarks/bootstrap.sh` | `SHELF_IMAGE` default | `ghcr.io/shelf-project/shelfd` |
| `benchmarks/configs/shelf/README.md` | image repo | `ghcr.io/shelf-project/shelfd` |
| `benchmarks/correctness-diff/k8s/cronjob.example.yaml` | image path | `ghcr.io/shelf-project/shelf/correctness-diff` |
| `charts/shelf/grafana/dashboards/shelf-read-path.json` | embedded GitHub URLs | `https://github.com/shelf-project/shelf/...` |
| `SECURITY/SUPPLY_CHAIN.md` | release.yml provenance regex | `github.com/shelf-project/shelf/...` |
| `SECURITY.md` | disclosure_policy_url, advisories URL | `shelf-project/shelf` |
| `CONTRIBUTING.md` | git clone URL + `conduct@shelf.example` placeholder | `shelf-project/shelf`, `conduct@shelf-project.dev` |
| `docs/quickstart/index.md` | git clone URL | `shelf-project/shelf` |

**Class B — internal URLs that are honest artifacts of the origin
cluster but break for external users**. These live in
`observability/` and `runbooks/` and point at internal Grafana /
Airflow / runbook hosts on `*.penpencil.internal`. Two sub-choices:

- **Template them**: replace the host with `${SHELF_DASHBOARD_BASE}` /
  `${SHELF_RUNBOOK_BASE}` and document substitution in the README of
  each directory. Honest, portable, costs ~30 minutes of editing.
- **Strip them entirely**: remove `dashboard_url` / `runbook_url`
  annotations so alerts ship with label-only routing. Loses
  operator signal for *any* user who doesn't set up their own.

**Recommend template them** — the annotations are high-value
operator signal and external users will set `${SHELF_DASHBOARD_BASE}`
exactly once when they install the Helm chart. Files affected:
7 runbooks in `shelf/runbooks/`, `observability/alerts/shelf-prometheus-rules.yaml`,
`observability/dashboards/shelf-overview.json`.

**Class C — honest historical attribution that stays**:

- `agents/out/03-plan.md`, ADRs under `agents/out/adr/` — these
  are the audit trail of how the project was built, including
  references to the origin cluster by name. That's fact, not
  identifier-leak. Leave as-is.
- `docs/rollout-v1.md` + `docs/rollout-v1/` — "we ran this on a
  penpencil Trino-on-EKS cluster" is the rollout narrative; it is
  the evidence. Do not rewrite; link to it from the announce post
  with the "measured internally on the penpencil cluster that
  originated the project" framing.
- `docs/cluster-handoff.md` — historical record of the v0.5
  handoff; stays. Add a one-line pointer at the top noting the
  project is now neutral-org.

**Other audit steps**:

- Per-replica traces in `benchmarks/trino_logs/traces/*.parquet`
  MUST be confirmed either (a) never committed or (b) synthetic.
  Internal production query logs cannot go public. Check:

  ```bash
  git -C /Users/aamir/trino/shelf ls-files benchmarks/trino_logs/traces/
  # Expected: empty, or only synthetic fixtures.
  ```

**Deliverable**: a single PR that lands Classes A + B. Class C is
explicitly NOT touched and the PR description documents why.

### 1.2 Git history audit

Problem: the git history was built during internal work with
AI-agent iteration; may contain transient secrets, draft design
notes pulled back, and commit messages referring to internal
tickets.

Options:

| Option | Cost | Risk |
| ------ | ---- | ---- |
| Publish full history as-is | 0 | High — anything ever committed is findable forever. |
| `git filter-repo` to strip secrets + bad paths | ~1 day | Medium — easy to miss one. |
| Squash to a single `v0.0-initial` commit, keep history in an internal-only backup | ~2h | Low; but destroys `git blame` for everyone. |

**Recommend**: run `git log --all -p | rg -iE 'aws_access|aws_secret|BEGIN PRIVATE|BEGIN RSA|trufflehog'`
+ `gitleaks detect --source=.`. If zero hits, publish full history.
If any hits, escalate to filter-repo with a written record of what
was stripped and why.

**Deliverable**: `docs/launch/history-audit.md` with the scan output
pasted in (redacted), plus the decision.

### 1.3 Binary and benchmark audit

- `Cargo.lock` is fine to publish (it *should* be).
- `benchmarks/smoke/` docker-compose is fine; it's synthetic.
- `benchmarks/trino_logs/traces/` — see §1.1.
- `benchmarks/correctness-diff/config/*.yaml` — only
  `config.example.yaml` is committed; confirm no prod configs slipped
  in post-rollout.
- Helm chart `values-prod.yaml` — contains prod-tuned defaults but no
  secrets; confirm with a `rg -i 'password|token|key.*=' charts/`.

**Deliverable**: confirmation line in
`docs/launch/history-audit.md` that this audit was run, with a paste
of the `rg` output.

### 1.4 Claim audit (the one people actually read)

Any number in the README or `COMPARISON.md` that cannot be
reproduced from the public repo against a public dataset gets
either (a) a link to how to reproduce or (b) a "measured internally
on 900 TiB/mo workload; public reproducer pending" footnote. "94 %
warm hit ratio on 10 Iceberg queries" in the smoke harness — that's
fine, it runs in `benchmarks/smoke/` on every PR. "Beat Alluxio on
rep-2" — **not fine** in public docs without a public reproducer;
move to a blog post where "internal" is the honest framing.

**Deliverable**: revised README + COMPARISON where every number
either has a reproducer badge or a plain-English "measured
internally on Y workload" qualifier. No marketing-only numbers.

---

## 2. Author the missing launch files (T-21 days → T-7 days)

CONTRIBUTING.md, Chart.yaml, and README already reference files
that do not exist. Each is a bug; each is cheap to fix; each is
a launch-blocker because the first external visitor will find the
404s within minutes.

### 2.1 Write list (priority order)

| File | Status | Notes |
| ---- | ------ | ----- |
| `CODE_OF_CONDUCT.md` | referenced in CONTRIBUTING, **missing** | Copy Contributor Covenant 2.1 verbatim. Decide + substitute `conduct@<domain>` — this is the §0 domain decision. |
| `docs/governance.md` | referenced in CONTRIBUTING, **missing** | Document the BDFL→PMC transition rule from §0. |
| `MAINTAINERS.md` | not yet referenced, **should exist** | One person at launch (shelf-core). Document the add-maintainer criterion: 3 merged non-trivial PRs + BDFL approval. |
| `ROADMAP.md` | not yet referenced, **should exist** | Six-month lookhead. v0.5 → v0.6 (public launch) → v0.7 (blob-cache SPI, SHELF-29) → v0.8 (second origin — GCS or Azure, TBD). Explicitly mark non-goals. |
| `RELEASING.md` | **missing** | Document the tag → build → sign → publish chain from §4. |
| `.github/ISSUE_TEMPLATE/bug.yml` | **missing** | Structured bug report; required for good triage. |
| `.github/ISSUE_TEMPLATE/feature.yml` | **missing** | "What problem, why now, what's the smallest version" template. |
| `.github/ISSUE_TEMPLATE/config.yml` | **missing** | Disable blank issues; point at Discussions for questions. |
| `SECURITY.md` | **exists** | Audit: it needs a real reporting channel (see §2.2). |
| `docs/launch/history-audit.md` | **missing** | Output of §1.2 / §1.3. |
| `docs/launch/announce.md` | **missing** | See §5. |
| `docs/launch/benchmarks-public.md` | **missing** | Public-reproducer doc; substitutes for internal traces per §1.4. |

**Deliverable**: eleven files landed on `main`, all referenced-but-missing
links resolved.

### 2.2 Security disclosure

Current `SECURITY.md` likely has a placeholder email. Launch-blocking
decision: who reads `security@<domain>` when an external researcher
reports a CVE at 03:00? If the answer is "nobody (yet)", either:

- Set up a shared inbox with two humans who commit to 24h ack
  (ideal but requires corporate infra); OR
- Route to GitHub's private vulnerability reporting (no infra, 0-touch
  setup — just enable the feature on the repo).

**Recommend** GitHub's private vulnerability reporting for launch.
Upgrade to a real inbox if/when external research warrants.

---

## 3. Public repo hygiene (T-14 days → T-7 days)

The repo should pass a first-visitor 60-second smoke:

1. README explains what Shelf is, why, and how to try it.
2. Quickstart actually works on a clean Mac in ≤ 10 min.
3. CI badge is green on `main`.
4. LICENSE, CODE_OF_CONDUCT, CONTRIBUTING, SECURITY visible at the
   top of the file tree.
5. At least one [Good First Issue] labelled issue is open.

### 3.1 CI hardening

Existing workflows: `verify.yml`, `smoke.yml`, `helm-lint.yml`,
`bench.yml`, `security.yml`. Gaps to close before launch:

- **Required status checks** — mark `verify` + `smoke` + `helm-lint`
  as required on branch protection rules for `main`. Can't merge if
  they're red.
- **DCO check** — add the
  [DCO GitHub App](https://github.com/dcoapp/app) as a required check.
- **Dependabot** — `.github/dependabot.yml` for Cargo + GitHub
  Actions + Docker.
- **CodeQL** — lightweight static analysis on every PR for
  Rust + (if the Java plugin ships at launch) Java.

### 3.2 Quickstart first-try SLA

The quickstart is the single most important surface after the
README tagline. Launch blocker: a human who has never touched the
repo runs it end-to-end from the README link and reports back with
timings. Acceptance: ≤ 10 min total, ≤ 2 error messages encountered.
If it fails, fix before launch.

This is not optional. First-impression is made once.

### 3.3 Seed issues

At launch we want the issue tracker populated enough to look alive
but not so full it looks abandoned. Target:

- 5-8 `good-first-issue` tagged — small, self-contained, genuinely
  useful (e.g. "add prometheus metrics suffix `_total` consistency
  audit", "swap `clap` derive for builder in one CLI").
- 2-3 `help-wanted` tagged — larger, known-need (e.g. "GCS origin
  adapter", "benchmark against IOMMU-passthrough NVMe").
- 2-3 `roadmap` tagged — tracks ROADMAP.md items (v0.7 blob-cache
  SPI, v0.8 second origin).

No "bug" tags at launch — if we know a bug, fix it. An empty bug
tracker at launch says "we shipped something we believe in".

---

## 4. Release mechanics (T-7 days → T-0)

### 4.1 Tag plan

- **`v0.5`** — already tagged post-rollout-soak. Internal milestone;
  not published, stays only in the origin repo.
- **`v1.0.0-rc.0`** — cut T-7 days on the `shelf-project/shelf`
  public repo. This is a **release candidate**, not a public v0.x.
  Private preview only (§5.1). Kept because declaring v1.0 without
  external eyes on the actual tagged artefacts first is how you
  ship a v1.0.1 the next morning.
- **`v1.0.0`** — cut T-0 (launch day). Identical to rc.0 unless a
  launch-blocker was found during preview week.

**On tagging straight to v1.0**: the user-facing surface (S3 shim
protocol, `shelfctl` CLI shape, Prometheus metric names, Helm chart
values, config-file schema) has been stable for the 14-day soak.
v1.0 commits to semver: no breaking changes in minor/patch bumps,
only in major. The blob-cache SPI work (SHELF-29) goes in v1.1
additively. If any of the above surfaces breaks between v1.0 and
v1.0.x, that's a bug, not a minor bump.

### 4.2 Artefact set per tag

1. Source tarball (auto by GitHub).
2. `shelfd` container image — multi-arch (linux-amd64, linux-arm64),
   distroless base. Push to `ghcr.io/shelf-project/shelfd:v1.0.0`
   AND `ghcr.io/shelf-project/shelfd:latest` on non-pre-release tags.
3. `shelfctl` static binaries — linux-amd64, linux-arm64, darwin-amd64,
   darwin-arm64. Attached to the GitHub Release as assets.
4. Helm chart — packaged from `charts/shelf/`, published to
   `ghcr.io/shelf-project/charts/shelf` (OCI) AND attached to the
   Release as `shelf-1.0.0.tgz`.
5. SBOM — `syft` output for both source + container, attached as
   `shelfd-v1.0.0-sbom.spdx.json`.
6. Signed everything — `cosign sign --keyless` against the Sigstore
   public log with OIDC identity from the `release.yml` workflow
   run. Verify instructions in `RELEASING.md`.
7. Provenance — SLSA v1.0 build-provenance attestation via
   `actions/attest-build-provenance@v1`.

### 4.3 Release CI

A new `.github/workflows/release.yml` triggered by tags matching
`v[0-9]+.[0-9]+.[0-9]+*`:

1. Re-run `verify` + `smoke` gates (belt and braces).
2. Build multi-arch image with `docker buildx`.
3. Build binaries with `cargo build --release` in a matrix of targets.
4. Package Helm chart.
5. Generate SBOM.
6. Sign with cosign (keyless, OIDC).
7. Build-provenance attestation.
8. Draft a GitHub Release with auto-generated changelog from
   `CHANGELOG.md` diff since the previous tag.
9. Human promotes draft → published.

**Launch-blocker**: steps 1-8 must work end-to-end on a throwaway
`v1.0.0-rc.0` tag before T-0. "I'll figure out the release on launch
day" is how projects die before they start.

---

## 5. Announce choreography (T-2 days → T+1 day)

### 5.1 Private preview week (T-7 → T-0)

Share `v0.6.0-rc.0` + the draft announce post with a small private
list (≤ 20 people): Trino committers known through professional
network, one cache/storage expert, one Rust-ecosystem reviewer, a
handful of Trino-on-EKS operators. One explicit ask: *"does the
quickstart work on your machine in ≤ 10 min?"* plus *"is the
COMPARISON.md honest?"*. Two open-ended asks: anything broken,
anything misleading.

Budget: 5 days for inbound, 2 days for fixes. If a launch-blocker
surfaces, slip launch — do not launch knowingly-broken.

### 5.2 Launch day (T-0)

Morning (your timezone):

1. Push `v0.6.0` tag. Wait for release CI green. Verify signatures.
2. Flip repo visibility to public.
3. Merge the pre-staged `docs/launch/announce.md` into `docs/blog/` +
   publish on project site (if one exists) / GitHub Discussions.
4. Post to:
   - Hacker News (Show HN format, no self-aggrandising title).
   - `r/rust` (high-quality-link flair).
   - `r/dataengineering`.
   - Trino Community Slack `#announcements` — ask a committer to
     amplify, do not drop-and-run.
   - Twitter/X + Mastodon + LinkedIn posts — each a one-paragraph
     variant, not the full post. Each links to the blog.
5. Update your own company blog (if applicable); link back to the
   project blog.

Afternoon:

1. Triage inbound issues as they arrive; acknowledge every issue
   within 4 business hours on launch day. This is the single
   highest-ROI work the maintainer does on launch day.
2. Reply to HN / Reddit / Slack threads — technical, direct, not
   marketing. Never fight; always clarify.

Evening:

1. Post a launch-day retrospective Discussion — "what we heard,
   what we're fixing first". Seeds the issue triage narrative.

### 5.3 Announce post skeleton (`docs/launch/announce.md`)

- Title: `Shelf: a row-group-granular read cache for Trino`.
- Lede: one paragraph, one honest claim ("we replaced Alluxio on a
  4-replica Trino-on-EKS stack; here's the code"). No adjectives.
- Five bullets of what Shelf does. Link to architecture.
- Two bullets of what Shelf deliberately does not do (§0-style
  non-goals). This is the trust-builder.
- A small diff: "our Trino catalog config changed from X to Y". One
  line. Makes the "endpoint swap" wiring instantly understandable.
- One graph: hit-ratio over the rollout (reproducible by the public
  smoke, not the internal 4-replica soak).
- Quickstart link.
- Two paragraphs on why we're open-sourcing it: intellectual
  honesty. "We think it's useful. We think the design is defensible.
  We want it reviewed in the open before we commit to v1.0."
- Governance + license paragraph.
- Links to Discussions, Issues, ROADMAP.

Draft once; iterate in §5.1 preview week.

---

## 6. Post-launch weeks 1-4 (T+0 → T+28)

The first four weeks decide whether the project gets a second life.

### 6.1 Week 1 SLAs (self-imposed; written down; publicly tracked)

- **Issue ack**: within 24h, weekdays.
- **PR first-review**: within 72h.
- **Security report ack**: within 24h, any day.
- **Discussions**: best-effort, no SLA (this is where we set expectation).

Publish these in `CONTRIBUTING.md` so contributors know what to
expect.

### 6.2 First-month deliverables

- One office-hours session recorded and posted. Short (30 min);
  public agenda in the corresponding Discussion.
- One maintainer add *IF* a non-BDFL contributor lands 3+ non-trivial
  PRs (matches the §2.1 criterion). Be strict; being a maintainer is
  a contract.
- A "month-one retro" blog post: what surprised us in inbound, what
  we got wrong in docs, what we're prioritising for v0.7.
  Published at T+30.

### 6.3 Red-flag triggers (walk these back toward us, don't ignore)

- **Issue backlog growing > 2× capacity to triage**: pause new feature
  work; publish a triage plan Discussion.
- **First-time contributor PR stalling > 5 days**: lights on, alarms
  out. This is the single signal new contributors use to decide if
  the project is alive.
- **First CVE report**: rehearse before it happens; see §7.

---

## 7. Failure modes we are choosing to accept

These are the honest residuals after §1-§6. Documented so we don't
surprise ourselves.

1. **First-week bus factor is 1.** shelf-core as BDFL means if they're
   unavailable, triage stalls. Mitigation: GitHub notifications to a
   backup human (name them in MAINTAINERS.md as "shadow for year 1").
2. **No third-party benchmark at launch.** COMPARISON.md will link to
   the `benchmarks/smoke/` numbers and the
   [benchmarks-public.md](benchmarks-public.md) reproducer against
   TPC-DS on MinIO; the "beat Alluxio on rep-2" internal number will
   live in the announce post with the "measured internally"
   qualifier. Someone on HN will call this out. Response: "yes,
   that's why we're open-sourcing it; help us reproduce it publicly."
3. **We are launching on a v0.x.** Pre-1.0 explicitly allows breakage;
   CHANGELOG must call out every breakage. One public API break
   without a CHANGELOG entry = trust destroyed.
4. **First CVE will happen.** Process: GitHub private vulnerability
   report → 24h ack → patched release within 7 days for high-severity
   (CVSS ≥ 7) or next monthly release otherwise. Advisory published
   after patch ships.
5. **Trademark.** "Shelf" is not distinctive enough to defend. We
   accept that someone could fork the name. Mitigation: trust the
   project's URL, not the word.

---

## 8. Explicit non-goals at launch

Things that people will ask about on launch day that we say *no* to,
and where to point them instead:

| Ask | Response | Pointer |
| --- | -------- | ------- |
| "Does it support Azure / GCS?" | Not today; v0.8 roadmap. | [ROADMAP.md](../../ROADMAP.md) |
| "What about Iceberg tables on HDFS?" | Not on the roadmap. The S3 protocol is our surface. | README non-goals |
| "Does it work for Spark?" | The cache is protocol-level; Spark's S3 client should Just Work but is untested. PRs welcome. | `good-first-issue`-seeded test harness |
| "Raft? etcd?" | [ADR-0001](../../agents/out/adr/0001-no-embedded-raft.md) — we chose K8s-native membership. | ADR link |
| "Arrow Flight?" | Deferred to v1.x contingent on measured EKS throughput. | [ADR-0004](../../agents/out/adr/0004-http2-only-in-v1.md) |
| "Why not just fix Alluxio?" | Honest answer: [COMPARISON.md](../../COMPARISON.md) | COMPARISON link |
| "Will you accept contributions?" | Yes, governed by [CONTRIBUTING.md](../../CONTRIBUTING.md) + DCO. | CONTRIBUTING link |

---

## 9. Timeline (calendar)

Working backwards from launch day `L`:

| Phase | Window | Owner | Deliverable |
| ----- | ------ | ----- | ----------- |
| §1 audits | L-28 → L-14 | shelf-core | history-audit.md, claim-audit done |
| §2 missing files | L-21 → L-7 | shelf-core | 11 files landed |
| §3 repo hygiene | L-14 → L-7 | shelf-core + 1 external | quickstart first-try green |
| §4 release mechanics | L-7 → L-0 | shelf-core | v0.6.0-rc.0 tag green through release CI |
| §5.1 private preview | L-7 → L-0 | shelf-core | ≤ 20 reviewers, all feedback triaged |
| §5.2 launch day | L-0 | shelf-core | repo public, announce posts, triage |
| §6.1 week 1 SLAs | L+0 → L+7 | shelf-core (+ shadow) | 24h issue ack maintained |
| §6.2 month 1 retro | L+28 → L+30 | shelf-core | blog post |

Total investment: ~4 weeks of concentrated effort before launch, ~10h/week
of maintenance the month after. Past that, maintenance scales with
contributor inflow — the point at which the BDFL → PMC conversation
becomes real.

---

## 10. Go / no-go gates

Do **not** launch if any of these is red on the morning of L-0:

1. [ ] `v0.5` tagged on the origin repo AND 14-day post-rollout soak
       signed off ([rollout-v1.md](../rollout-v1.md)).
2. [ ] 30 consecutive days since `v0.5` tag with zero Shelf-attributed
       production pages (cumulative 4-replica soak).
3. [ ] `shelf-project` GitHub org stood up; `shelf-project/shelf` repo
       created (private during the preview week, public at T-0).
4. [ ] `shelf-project.dev` domain registered; `conduct@`,
       `security@`, and `oncall@` inboxes routed to real humans with
       24h ack SLAs.
5. [ ] §1.1 Class A sweep: every `penpencil-oss` / `penpencil.internal` /
       `shelf.example` reference in the 15 files listed is replaced.
6. [ ] §1.2 history audit: zero secret hits OR filter-repo completed
       with written record in `docs/launch/history-audit.md`.
7. [ ] §1.4 claim audit: every number in README / COMPARISON has a
       reproducer badge or an "internal" qualifier.
8. [ ] All 11 files from §2.1 landed on `main`.
9. [ ] First-try quickstart green on at least one external machine.
10. [ ] `v1.0.0-rc.0` release CI green end-to-end, including cosign
        signature verification from a clean machine.
11. [ ] Private preview week (§5.1): ≥ 3 reviewers confirmed quickstart
        works; zero launch-blocker bugs open.
12. [ ] Branch protection + DCO check enforced on `main`.

Any red → slip launch by one week and re-check. There is no prize for
hitting a calendar date if the thing is broken.

---

## References

- Rollout-v1 (predecessor): [rollout-v1.md](../rollout-v1.md)
- Governance draft (to land per §2): [governance.md](../governance.md)
- Contributor guide: [CONTRIBUTING.md](../../CONTRIBUTING.md)
- Blueprint: [BLUEPRINT.md](../../BLUEPRINT.md)
- Comparison doc (audit per §1.4): [COMPARISON.md](../../COMPARISON.md)
