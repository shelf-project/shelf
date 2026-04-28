# Shelf supply-chain security

_Status: v0.1 scaffold, agent-9, 2026-04-23._
_Scope: every artefact that ships from this repo — Rust binaries,
Java jars, Python wheels, container images, Helm charts, and the
release-signing metadata that binds them together._

---

## 0. Principles

1. **SBOM or it didn't ship.** Every release artefact has a
   Syft-produced SBOM attached and signed.
2. **Signature or it didn't ship.** Every release artefact (binary,
   jar, image, Helm chart) is signed. Verification is mandatory in
   the deploy path.
3. **Pin everything.** Exact versions in `Cargo.lock`, `uv.lock`,
   Maven `pom.xml` (no `[1.0, 2.0)` ranges), Helm `Chart.lock`.
   Renovate opens PRs; humans review.
4. **Scanners fail closed.** If cargo-deny, pip-audit, Trivy, Grype,
   or OSV-Scanner exits non-zero, the PR is red. There is no
   "scanner is flaky" bypass. Explicitly allow-listed advisories
   live in this repo with a ticket link.
5. **Short reproducible builds.** `cargo build --release` on
   `rust-toolchain.toml`-pinned toolchain; `mvn verify -o`; image
   builds via `docker buildx` with `--provenance=true`.

---

## 1. Rust (`shelfd`, `shelfctl`, `shelf-hashring`)

### 1.1 Lockfiles + toolchain

- `Cargo.lock` committed for the workspace (SHELF-01 ticket ensures this).
- `rust-toolchain.toml` pins the compiler version (already present).
- `cargo --frozen` used in CI — any drift fails the build.

### 1.2 `cargo-deny`

Config at repo root `deny.toml` (this repo, not vendored). Categories:

| Section      | Behaviour                                                           |
| ------------ | ------------------------------------------------------------------- |
| `advisories` | All Rustsec advisories are `deny`; yanked crates are `deny`; explicit `ignore` entries require a ticket ID + expiry |
| `licenses`   | Allow-list only; MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, MPL-2.0, Unicode-DFS-2016; everything else denied |
| `bans`       | Banned crates list includes: `openssl` (use `rustls`), `openraft` / `raft` (ADR-0001), `pickle`, anything pre-1.0 in the data-plane hot path (reviewed manually) |
| `sources`    | Only `crates.io`; git dependencies require a commit hash + a ticket |

`cargo-deny` runs on every PR and on the daily schedule (see
`.github/workflows/security.yml`).

### 1.3 `cargo-audit`

- Runs on every PR.
- Syncs the RustSec advisory DB daily.
- Any advisory with a matching versioned crate in `Cargo.lock` →
  fail.
- Informational advisories (`informational = "unmaintained"`) are
  downgraded to **warn** and tracked as open items with a 30-day
  SLA.

### 1.4 `cargo-geiger`

Counts `unsafe` blocks transitively; the count is reported in PR
comments. Threshold: no PR may raise the count unless the diff
includes a justifying doc-comment on every new `unsafe` block.

### 1.5 Fuzzing

`cargo-fuzz` targets live in `shelfd/fuzz/`:

- `fuzz_range_parser` — HTTP `Range` header parsing
- `fuzz_prefetch_request` — Tonic-decoded `PrefetchRequest`
- `fuzz_pin_list_json` — pin-list JSON parser

Fuzz corpus is seeded from unit tests; nightly job runs each target
for 10 minutes on `ubuntu-latest`. A crash → GitHub issue
auto-opened, labelled `security`.

---

## 2. Java (`clients/trino`)

### 2.1 Build + pins

- Maven `pom.xml` uses exact versions (`[1.2.3]`) on every dependency.
- `mvn dependency:resolve-plugins` + `dependencyManagement` locks
  transitive versions at the top-level.
- Release build runs in a hermetic offline mode: `mvn verify -o`
  against a Nexus/Artifactory mirror populated by the CI job
  before it starts.

### 2.2 `OSV-Scanner`

- Runs on every PR against `clients/trino/pom.xml` and the
  resolved `target/dependency-list.txt` (emitted by
  `dependency:list`).
- Vulnerability database: GitHub's public OSV database.
- Severity gate: any **MEDIUM or higher** fails the build. LOW
  logs a warning.

### 2.3 OWASP Dependency-Check (optional, quarterly)

Richer than OSV but slower (NVD DB, 5-15 min). Run in the scheduled
workflow, not per PR. Produces `dependency-check-report.html` for
the security rota to review.

### 2.4 Spotbugs + Checker Framework

- Spotbugs runs with `fb-contrib` + `find-sec-bugs` profiles on
  every PR.
- Checker Framework's `@Untainted` taint analysis applied to the
  plugin config-read path (for P-E1 mitigation, §THREAT_MODEL.md).

---

## 3. Python (trainer, snapshot-watcher)

### 3.1 Lockfiles

- `uv.lock` (or `pip-compile --generate-hashes` output) committed
  for every Python image.
- No `requirements.txt` with unpinned ranges.

### 3.2 `pip-audit`

- Runs on every PR against the resolved `uv.lock`.
- Index restricted to PyPI (`--index-url https://pypi.org/simple/`);
  no private/secondary indexes.
- Any advisory with a matching version → fail.

### 3.3 Image base

- Python base is `python:3.12-slim-bookworm` (distroless not
  available for Python with our runtime extensions). Non-root user;
  read-only rootfs at runtime.
- Trivy + Grype scan of the image — see §4.

---

## 4. Container images

### 4.1 Base + build

- `shelfd`: `gcr.io/distroless/cc-debian12:nonroot`.
- `shelfctl`: same.
- Trainer / watcher: `python:3.12-slim-bookworm`.
- Multi-stage: compile in a build image (`rust:1.84-slim-bookworm`
  or `eclipse-temurin:21-jdk`), copy artefact into distroless.
- Every image is reproducible: `SOURCE_DATE_EPOCH`, `docker buildx
  --provenance=true`, `--sbom=true`.

### 4.2 Scanners

Runs in `.github/workflows/security.yml`:

- **Trivy filesystem** scan (source tree). Fails on `CRITICAL` or
  `HIGH` with a fixed-version upstream.
- **Trivy image** scan (post-build). Same severity gate.
- **Grype image** scan as a second opinion. Discrepancies between
  Trivy and Grype log a warning (not a failure) — tracked in the
  security issue tracker.

### 4.3 SBOM

- `syft packages dir:. -o cyclonedx-json=sbom.cdx.json` on the source.
- `syft packages <image> -o cyclonedx-json=image.sbom.cdx.json` on
  the final image.
- Both SBOMs attached to the release artefact via `cosign attach
  sbom`.

### 4.4 Image signing (cosign keyless)

- `cosign sign --yes <registry>/shelfd:<tag>` in a GitHub Actions
  job with `id-token: write` — Sigstore's keyless signing backed by
  the workflow's OIDC identity.
- Verification: `cosign verify --certificate-identity-regexp
  "^https://github.com/shelf-project/shelf/\.github/workflows/release\.yml@.*$"
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
  <registry>/shelfd:<tag>`.
- Helm chart deployment (`charts/shelf/`) has a pre-install hook
  that verifies the image signature before applying. Agent 8 owns
  the hook; we own the verification-command string.

---

## 5. Binary + artefact signing

### 5.1 Rust binaries (`shelfd`, `shelfctl`)

- `cosign sign-blob` with Sigstore keyless on the release tarball.
- Signature published alongside the tarball on the GitHub release
  page.
- Minimum checksum: `sha256sum` in a `CHECKSUMS.txt` file co-signed.

### 5.2 Java jar (`shelf-trino-plugin`)

- `gpg --detach-sign` with the release PGP key (see §7) on the
  shaded jar — required for Maven Central publish if we go that
  route.
- Dual-signed: PGP for Maven Central, Sigstore keyless for direct
  download.

### 5.3 Helm chart

- `helm package` + `cosign sign` on the `.tgz`.
- Chart stored in an OCI-compliant registry; consumers verify via
  `cosign verify`.

### 5.4 Pin-list signatures (runtime)

- Trainer signs `pin_list.json` with a Sigstore OIDC identity
  derived from its GitHub Actions job (for the nightly DAG) or its
  IRSA role (for the in-cluster Airflow job).
- `shelfd` verifies the signature against the Fulcio root before
  applying the pin list (see `IAM.md §2.6`, threat C-T1).

---

## 6. Secrets

- **No secrets in ConfigMaps, Helm values.yaml, sample files, or
  docs.** Period.
- Production secret delivery: External Secrets Operator with AWS
  Secrets Manager as the backend, or Sealed Secrets (agent 8
  decides).
- Every sample config uses the literal placeholder `<REDACTED>`
  (matched by a CI grep guard).
- Pre-commit: `gitleaks` scan against the full diff, and a
  `trufflehog git`-based sweep of history on every PR (cached).
- Historic secret removal: if a secret is ever committed, the
  control is **revoke** (not rewrite). A runbook entry is required
  (`runbooks/revoke-leaked-credential.md`, owned by agent 8).

---

## 7. Release-signing checklist

This is the human-executable form. The CI workflow enforces the
mechanical parts.

Before cutting **any** tagged release:

- [ ] All `cargo-deny`, `cargo-audit`, `pip-audit`, `OSV-Scanner`,
      `Trivy`, `Grype` CI jobs green on the release commit.
- [ ] `cargo-geiger` count not increased vs. previous release without
      a written justification in the PR description.
- [ ] Syft SBOM produced for:
      - source tree
      - every container image
      - `shelfd` and `shelfctl` binaries
- [ ] SBOMs attached to the GitHub release.
- [ ] Every container image cosign-signed via keyless OIDC;
      verification command documented in release notes.
- [ ] Every binary cosign-signed (blob signature) and SHA256
      checksum file co-signed.
- [ ] Java jar PGP-signed **and** Sigstore-signed.
- [ ] Helm chart `.tgz` cosign-signed.
- [ ] `SECURITY/CHECKLIST.md` gate items all green.
- [ ] No `TODO`, `XXX`, or `FIXME` in files under `SECURITY/*`,
      `.github/workflows/security.yml`, `deny.toml`, CODEOWNERS.
- [ ] Release notes include a "Supply-chain provenance" section
      with verification commands for each artefact type.

---

## 8. Scheduled maintenance

| Cadence  | Action                                                                                  |
| -------- | --------------------------------------------------------------------------------------- |
| Daily    | `cargo-audit`, `pip-audit`, OSV-Scanner run via scheduled workflow; surface new advisories |
| Weekly   | Renovate auto-opens PRs for patch-level bumps; security rota triages                    |
| Nightly  | `cargo-fuzz` targets run for 10 min each; crashes auto-file issues                      |
| Monthly  | Full Grype rescan of the last 3 releases' images; notify on newly-discovered CVEs in previously-released artefacts (and publish an advisory if reachable) |
| Quarterly | Banned-crates list review; license allow-list review; OWASP Dependency-Check deep scan |
| Annually | Key rotations per `IAM.md §3`; threat model refresh                                    |

---

## 9. References

- `SECURITY.md` — disclosure policy.
- `SECURITY/THREAT_MODEL.md` — the threats these controls mitigate.
- `SECURITY/IAM.md` — identity model for in-cluster services.
- `SECURITY/CHECKLIST.md` — pre-release gate.
- `deny.toml` — the machine-readable form of §1.2.
- `.github/workflows/security.yml` — the CI that enforces everything
  in this document.
