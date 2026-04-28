---
# Front matter (machine-readable; mirrored in docs/security.md on launch)
policy_version: 0.1-scaffold
contact_channel: security@shelf-project.dev
pgp_fingerprint: TBD-PGP-FINGERPRINT-PLACEHOLDER  # replaced on first rotation
disclosure_policy_url: https://github.com/shelf-project/shelf/blob/main/SECURITY.md
response_sla:
  acknowledge: 48h
  triage: 5 business days
  fix_p0_critical: 7 days
  fix_p1_high: 30 days
  fix_p2_moderate: 90 days
  fix_p3_low: next minor release
embargo_default_days: 90
rota:
  - "@aamir"
  - "TBD-second-reviewer"
---

# Security policy

> **Status.** This is the v0.1 scaffold produced by agent-9. It will be
> superseded by the public-facing policy at OSS launch (plan §7). The
> email, PGP key, and rota names are placeholders — every `TBD` must be
> replaced before the repository is made public.

Shelf is a read-through cache sitting directly on the path between
Trino workers and S3. A defect in `shelfd`, the Trino plugin, or the
trainer could:

- leak rows across tenants (confidentiality)
- serve stale or tampered bytes (integrity)
- take Trino down by way of fail-*closed* instead of fail-open
  (availability)

Because the blast radius is large, we commit to the disclosure and
response process documented here.

---

## 1. How to report a vulnerability

**Do not open a public GitHub issue.** Do not discuss in Discord or
Slack. Use one of the channels below.

### 1.1 Preferred — email + PGP

Send to **`security@shelf-project.dev`**. Encrypt sensitive findings
with our PGP key:

```
Fingerprint : TBD-PGP-FINGERPRINT-PLACEHOLDER
Key URL     : https://shelf-project.dev/.well-known/shelf-security.asc  (TBD)
```

Or open a
[private vulnerability report on GitHub](https://github.com/shelf-project/shelf/security/advisories/new)
— routed to the same rota.

Include, to the extent you can:

- Shelf version / commit hash / chart version
- Affected component (`shelfd`, `clients/trino`, trainer, snapshot-watcher,
  Helm chart, CI)
- Reproduction steps, proof of concept, or a minimal patch
- Impact estimate (tenant isolation, RCE, DoS, info leak, etc.)
- Whether the issue is already public anywhere (pre-prints, forks,
  CVE drafts)

### 1.2 GitHub Private Vulnerability Reporting

Once the repo is public, you may also use GitHub's [Private
Vulnerability Reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
flow from the **Security** tab. Reports land in the same triage queue
as the mailbox.

### 1.3 What **not** to do

- Do not run automated scanners against `shelfd` instances you do not
  own. We assume any traffic hitting our production clusters is
  production traffic and will page on-call.
- Do not exfiltrate data beyond the minimum required to demonstrate
  impact. One record is plenty.
- Do not contact customers or external maintainers on our behalf.

---

## 2. Response workflow

```
report  →  acknowledge  →  triage  →  fix  →  disclose
```

| Step        | Owner             | SLA (from receipt)              | Artefact                       |
| ----------- | ----------------- | ------------------------------- | ------------------------------ |
| Acknowledge | Security rota     | ≤ 48 h                          | Reply mail (no technical content, just "we have it")                        |
| Triage      | Security rota     | ≤ 5 business days               | Severity assignment (§3), CVE reservation, private advisory opened on GitHub |
| Fix         | Code owner + rota | per severity (§3)               | Patch on a private branch, regression test, advisory draft                  |
| Validate    | Reporter + rota   | ≤ 14 days after patch           | Reporter confirms fix; we run full CI + chaos drill (SECURITY/CHECKLIST.md) |
| Disclose    | Rota + scribe     | per embargo (§4)                | Published advisory + patched release + credit line                          |

### 2.1 Severity matrix

| Severity | Examples                                                                 | Fix SLA             |
| -------- | ------------------------------------------------------------------------ | ------------------- |
| P0 / Critical | RCE on `shelfd`; cross-tenant S3 read; auth bypass on Pin / Evict RPC | 7 calendar days     |
| P1 / High     | Cache poisoning; plugin fails closed instead of open; trainer writes outside config bucket | 30 calendar days    |
| P2 / Moderate | Information leak in logs; DoS requiring authenticated attacker; TLS downgrade | 90 calendar days    |
| P3 / Low      | Dependency advisory with no reachable code path; documentation error | Next minor release  |

Severity is ours to assign, but we record the reporter's view. If we
downgrade, we say why.

### 2.2 Out of scope

- Social engineering of maintainers or the company that employs them
- DoS that requires unrealistic resource ratios (e.g. 1 TB/s of S3
  traffic to knock over `shelfd`)
- Bugs in dependencies already fixed upstream — we will cut a release
  bumping the pin, but we credit the upstream reporter, not you
- Attacks requiring physical access to NVMe
- Clickjacking on the docs site

---

## 3. Embargo policy

- **Default embargo: 90 days** from report receipt, or date of fix,
  whichever is later.
- **Negotiable.** If a fix is shipped faster and the reporter agrees,
  we disclose earlier. If the issue is in an upstream dependency with
  a longer embargo, we honour theirs.
- **Hard stop.** If the issue is being actively exploited in the wild,
  embargo drops to whatever minimum is needed to ship a fix. We tell
  the reporter before going public.
- **Credit.** Reporter chooses: full name, handle, or anonymous. We
  do not credit without permission.

---

## 4. Coordinated disclosure with upstream

Some defects will be in code we depend on:

| Upstream                     | Channel                                                                 |
| ---------------------------- | ----------------------------------------------------------------------- |
| Trino (the engine + plugin SPI)  | [Trino security](https://trino.io/security) — we file, we do not republish before they do |
| Foyer cache                  | [foyer-rs/foyer#security](https://github.com/foyer-rs/foyer/security/policy) |
| tokio / hyper / axum / tonic | Rustsec + maintainer channel per crate                                  |
| Arrow / Parquet (Java + Rust)| [apache.org/security](https://www.apache.org/security/)                 |
| AWS SDK (Rust)               | `aws-security@amazon.com`                                               |

We will **never** publish a Shelf advisory that discloses an upstream
unfixed issue.

---

## 5. Security-response rota

| Role                   | Initial owner       | Backup   |
| ---------------------- | ------------------- | -------- |
| Primary triage         | `@aamir`            | TBD      |
| Cryptographic review   | TBD (external)      | `@aamir` |
| Disclosure comms       | `@aamir` + scribe   | TBD      |

The rota expands as the team does. When it does, this file changes in
the same PR as the rota change.

---

## 6. Past advisories

None. This file is maintained as advisories are published.

The canonical list lives on GitHub once the repo is public:

- https://github.com/shelf-project/shelf/security/advisories (TBD)

---

## 7. Related documents

- `SECURITY/THREAT_MODEL.md` — STRIDE per component
- `SECURITY/IAM.md` — IRSA, STS chaining, mTLS, RBAC
- `SECURITY/SUPPLY_CHAIN.md` — SBOM, signing, scanners
- `SECURITY/CHECKLIST.md` — pre-release gate
- `.github/PULL_REQUEST_TEMPLATE.md` — per-PR security checklist
- `CODEOWNERS` — security-sensitive routing
- `.github/workflows/security.yml` — CI enforcement

Runbooks for incident handling on the operational side live in
`runbooks/` (owned by agent 8 — do not duplicate here).
