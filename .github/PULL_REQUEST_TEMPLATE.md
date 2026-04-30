<!--
  Shelf Pull Request template. Fill in what's relevant; delete what's not.
  The Security section is required on every PR (agent-9 Pass-6).
-->

## Summary

<!-- One or two sentences: what changed and why. Link the issue / ticket. -->

## Testing

<!-- What did you run locally? Which CI jobs matter? Links. -->

## Screenshots / logs (optional)

<!-- Grafana, kubectl output, curl, whatever. Redact keys. -->

---

## Security checklist (agent-9 Pass-6 — required)

Tick every applicable box, or write **N/A — <reason>** next to items
that genuinely do not apply (e.g., docs-only change).

- [ ] **No new `unsafe` Rust block** introduced. If one is, there is
      a doc-comment on the block explaining why it is safe, and
      `cargo-geiger` count is updated.
- [ ] **No new external dependency** (`Cargo.toml`, `pom.xml`,
      `pyproject.toml`, Helm chart deps) added without:
      - license compatible with `deny.toml` allow-list
      - no open `cargo-audit` / `OSV` / `pip-audit` advisory
      - pinned to an exact version
      - if a Rust crate pre-1.0, a justification comment in this PR
- [ ] **No new RPC surface** (HTTP endpoint, gRPC method, admin CLI
      command) added without:
      - authentication (mTLS) wired up
      - authorisation (RBAC table in `SECURITY/IAM.md §2.4`) updated
      - audit-log line emitted for admin operations
- [ ] **No new config key** that could carry a secret or a
      sensitive value, without:
      - sample value is `<REDACTED>` (not a real-looking credential)
      - secret delivery path is External Secrets / Sealed Secrets,
        not ConfigMap
- [ ] **Threat model updated** if architecture changed
      (`SECURITY/THREAT_MODEL.md §0.1` trust-boundary map,
      affected component tables).
- [ ] **IAM policies** reviewed if any S3 / STS / KMS action was
      added. No new `*:*` patterns. Resources are specific ARNs
      or narrow patterns.
- [ ] **No secrets in the diff.** `gitleaks` CI check passed; I
      also re-grep'd my diff for `AKIA`, `eyJ`, `-----BEGIN` before
      pushing.
- [ ] **Logging redaction** — any new log line that touches object
      keys, user SQL, or tenant identifiers uses the redacted
      wrappers (`RedactedKey`, SQL fingerprint, tenant short-id).
- [ ] **Circuit-breaker / fail-open invariants preserved** — if
      this PR touches the plugin or `shelfd` data plane, I confirm
      that Shelf failures still fall through to direct S3
      (BLUEPRINT §9.5).

## Release impact

- [ ] Needs a `SECURITY/CHECKLIST.md` item added / updated before
      next tagged release.
- [ ] No release-note-worthy security change.

## CODEOWNERS

<!-- The CODEOWNERS file routes security-sensitive paths to the
security rota. Do not self-approve those paths even if you have
permission. -->

---

_By opening this PR I confirm I have read `SECURITY.md` and
understand the disclosure policy. If this change patches an
unpublished vulnerability, **do not** file as a public PR — follow
the private disclosure process in `SECURITY.md §1`._
