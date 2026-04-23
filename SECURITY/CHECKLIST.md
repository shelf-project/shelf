# Shelf pre-release security gate

_Status: v0.1 scaffold, agent-9, 2026-04-23._
_Gate owner: security rota (see `SECURITY.md §5`)._
_Applies to every tagged release `v0.x`, `v1.0`, and any tag
signalling a new public artefact (container image, jar, Helm chart)._

> This is the hard gate. If anything here is red, the release does
> not ship. "Ship and fix forward" is not an option for security
> items.

---

## 0. How to use this file

1. Copy this checklist into the release PR description, replacing
   the bracketed text with evidence links (CI run, GitHub issue,
   runbook URL, artefact URL).
2. Two rota members sign off (comment `LGTM-security: <name>`).
3. Once both LGTMs are in, the release-cut CI job reads the PR
   description for the "**Gate passed**" line and proceeds.

---

## 1. Advisories

- [ ] **All RustSec advisories resolved or explicitly accepted.**
      Evidence: `cargo-audit` job on release commit is green or only
      warns on pre-accepted entries. Accepted entries have an
      expiry in `deny.toml` and a ticket link in this PR.
- [ ] **All OSV (Java) advisories resolved or accepted.**
      Evidence: OSV-Scanner job green on the release commit.
- [ ] **All `pip-audit` advisories resolved or accepted.**
      Evidence: link to CI job.
- [ ] **Trivy CRITICAL/HIGH findings resolved or accepted** on
      source + every image built from this commit. Same for Grype.
- [ ] **No accepted advisory is past its expiry date.** Any
      entry in `deny.toml` that has expired either gets renewed
      (new ticket, re-review) or the release is blocked.

## 2. SBOMs

- [ ] **Source-tree SBOM** (`syft packages dir:.`) produced and
      attached to the GitHub release.
- [ ] **Image SBOMs** (one per image: `shelfd`, `shelfctl`,
      trainer, snapshot-watcher if applicable) attached via
      `cosign attach sbom`.
- [ ] **Binary SBOMs** (`shelfd` tarball, `shelfctl` tarball)
      attached.
- [ ] SBOMs are CycloneDX JSON format (consistent format for
      downstream consumers).

## 3. Signatures

- [ ] **Every container image** cosign-signed via keyless OIDC.
      Evidence: `cosign verify --certificate-identity-regexp ...`
      succeeds; verification command captured in release notes.
- [ ] **Every binary** (`shelfd`, `shelfctl`) has:
      - a `cosign sign-blob` signature file
      - a SHA256 checksum in `CHECKSUMS.txt`
      - both captured in release notes
- [ ] **Java jar** (`shelf-trino-plugin-*.jar`) is:
      - PGP-signed (detached `.asc` file)
      - Sigstore-signed (`cosign sign-blob`)
- [ ] **Helm chart** (`.tgz`) cosign-signed; verification
      command in release notes.
- [ ] **Signature verification is end-to-end** — pull the release
      artefact fresh from the registry / release page and verify
      with the documented command. Two rota members do this
      independently.

## 4. Threat model + IAM

- [ ] `SECURITY/THREAT_MODEL.md` has been updated if the release
      changed any component on the trust-boundary map (§0.1).
      Evidence: diff link or "no architecture change" statement.
- [ ] `SECURITY/IAM.md` rotation table is up to date (every key
      listed exists, has a live owner, has a runbook).
- [ ] No new `*:*` actions or `Resource: "*"` patterns in any
      IAM policy shipped by the Helm chart. Grep guard CI job
      confirms.

## 5. Secrets hygiene

- [ ] `gitleaks` scan green on the release commit (no new findings).
- [ ] `trufflehog git` scan green on the PR range.
- [ ] `<REDACTED>` placeholder audit: every sample `values.yaml`,
      `.env.example`, and docs code block uses literal
      `<REDACTED>` or `<TBD>` — no real-looking credentials.
- [ ] No secret names referenced in chart `values.yaml`; secrets
      are delivered via External Secrets Operator or Sealed
      Secrets (agent 8).

## 6. Chaos + resilience drills

- [ ] **Pod-kill drill** passed in the week before release
      (plan SHELF-28). Evidence: CI run link.
- [ ] **KEDA rotation drill** passed in the week before release.
- [ ] **Partial-compromise drill** — at least once per major
      release, simulate a compromised `shelfd` pod that is still
      reachable by workers:
      1. Start a rogue container that serves arbitrary bytes on
         :9090 with a valid-looking but untrusted cert.
      2. Confirm the Trino plugin refuses to trust it (pinned CA
         + SAN mismatch).
      3. Confirm the circuit breaker opens within 5 failures and
         traffic falls back to S3.
      4. Confirm the rogue pod does not appear in the legitimate
         ring (NetworkPolicy denies its ingress).
      5. Evidence: drill report with timestamps + screenshots;
         archived under `docs/security-drills/YYYY-MM-DD/`.
      6. Runbook referenced: `runbooks/partial-shelfd-compromise.md`
         (owned by agent 8 — do not duplicate here).
- [ ] **Cert-rotation drill** — rotate `shelf-ca` on a staging
      cluster within the 30 days preceding release; observe zero
      workload impact.

## 7. Audit + logging

- [ ] Every admin RPC (`Pin`, `Evict`, `Reload`) emits an audit
      line with `(ts, actor_fp, method, args_hash)`; spot-check one
      from each category in the release PR.
- [ ] Audit log bucket has Object Lock (GOVERNANCE, 90-day)
      enabled; verified from AWS console or `aws s3api get-object-
      lock-configuration`.
- [ ] CloudTrail data events enabled for:
      - S3 config bucket (`pw-shelf-config-prod`)
      - KMS CMK for JWT signing

## 8. Release metadata

- [ ] Release notes include a **"Supply-chain provenance"** section
      with verification commands for: image, binary, jar, Helm chart.
- [ ] Release notes link to the `SECURITY.md` disclosure policy.
- [ ] If advisories were patched, a GitHub Security Advisory has
      been published with CVE (or CVE-reservation) + credit.
- [ ] CHANGELOG entry mentions any security-relevant change.

## 9. Pre-v1.0 additional gates

For the **v1.0 public release only**, add these:

- [ ] **External pen-test or third-party review** complete with a
      written report; every HIGH/CRITICAL finding has a fix or an
      accepted-risk doc. Evidence: report + sign-off from security
      rota + external reviewer.
- [ ] **Public fuzz corpus** published (so external researchers can
      contribute findings without bootstrapping).
- [ ] **`SECURITY.md` public.** `TBD` placeholders replaced with
      real contact email, real PGP fingerprint, real disclosure URL.
      GitHub private-vulnerability-reporting enabled on the repo.
- [ ] **Apache 2.0 license header** on every shipped source file.
- [ ] **CLA bot** live; first external contribution accepted.
- [ ] **OpenSSF Scorecard** score published; any "Dangerous
      Workflow" or "Token Permissions" below 7/10 has a written
      justification.

---

## 10. Sign-off block

To be filled in during the release PR.

```
Gate passed: yes
Release tag: v0._._
Release commit: <sha>
Advisories review: <link>
SBOM review: <link>
Signature verification: verified by <rota-a> and <rota-b>
Threat model delta: <none | link to diff>
Drills: <run-links>
External pen-test (v1.0 only): <report-link>
LGTM-security: @rota-a
LGTM-security: @rota-b
```

If either reviewer has concerns, they comment `BLOCK-security:
<reason>` and the gate is not passed.

---

## 11. Post-release actions

- Within 24 h of release, open a new issue to track any **accepted
  advisory** whose expiry would land before the next planned
  release. Triage before next cut.
- Update `SECURITY.md §6` (Past advisories) if any advisory was
  published alongside this release.
