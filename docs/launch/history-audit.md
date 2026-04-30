# Pre-public-flip git history audit

Evidence file for [docs/launch/playbook.md](./playbook.md) §1.2 + Blocker 3
of the v1.0.0 OSS-launch readiness audit. Re-run before every major
visibility flip (private → public, transfer to a new org, etc.).

## Method

```bash
gitleaks detect --source=. --report-format=json \
  --report-path=docs/launch/history-audit.json --redact
```

`gitleaks` 8.30.1, scanning **226 commits / 14.26 MB / 1.39 s** on the tip
of `main` at the time of the audit.

The default ruleset is intentionally aggressive — it pattern-matches
generic API-key shapes, AWS access tokens, JWTs, etc. across the full
commit graph reachable from `HEAD`. Every hit is reviewed by hand; the
verdict is recorded below.

## Result — 10 hits, all false positives

The raw report is in [history-audit.json](./history-audit.json). All
10 hits resolve to synthetic test-fixture material with no real
credential value.

| # | Rule              | File                                                              | Line | Verdict | Reason |
|---|-------------------|-------------------------------------------------------------------|------|---------|--------|
| 1 | generic-api-key   | `shelfd/src/admin_pin_payload.rs`                                 | 307  | False positive | Synthetic 64-hex-char fixture (`0011…ddeeff…`) for the strict pin-payload deserialiser unit test. Not a key. |
| 2 | generic-api-key   | `shelfd/src/admin_pin_payload.rs`                                 | 307  | False positive | Same line, prior commit. |
| 3 | generic-api-key   | `shelfd/tests/it_dollars_saved.rs`                                | 102  | False positive | `let key = "shelf-40-dollars-fixture"` is the **S3 object name** for the test artefact, not an auth key. |
| 4 | generic-api-key   | `shelfd/tests/it_dollars_saved.rs`                                | 102  | False positive | Same line, prior commit. |
| 5 | generic-api-key   | `shelfd/tests/it_dollars_saved.rs`                                | 102  | False positive | Same line, earliest commit on the file. |
| 6 | aws-access-token  | `shelfctl/src/bundle.rs`                                          | 340  | False positive | Synthetic input to the `redacts_aws_access_key_and_bearer` unit test. The code's job is to **scrub** AKIA-prefixed strings; the test asserts the redactor sees this fixture and replaces it. |
| 7 | aws-access-token  | `shelfctl/src/bundle.rs`                                          | 344  | False positive | Same test, assertion line. |
| 8 | aws-access-token  | `shelfctl/src/bundle.rs`                                          | 341  | False positive | Same test, prior commit. |
| 9 | aws-access-token  | `shelfctl/src/bundle.rs`                                          | 345  | False positive | Same test, prior commit. |
| 10| generic-api-key   | `clients/trino/src/test/java/io/shelf/client/FooterPrefetcherTest.java` | 50 | False positive | Synthetic 64-hex-char cache key in ADR-0011 format (`sha256(etag‖offset‖length‖rg)`) used as a test constant. Content-addressed, not auth. |

## `benchmarks/trino_logs/traces/` audit

Per playbook §1.1 the production query traces must be either never
committed or synthetic before flipping public.

```bash
$ git ls-files benchmarks/trino_logs/traces/
# (no output — never committed)
$ ls benchmarks/trino_logs/traces/
ls: benchmarks/trino_logs/traces/: No such file or directory
```

The directory does not exist in the working tree and has never been
committed to the repository. **Clean.**

## Verdict

**Repository history is safe to publish unchanged.** The squash-to-one-commit
escape hatch from playbook §1.2 is not needed. The honest agent-iteration
history under `agents/out/` and the rollout narrative under `docs/rollout-v1/`
remain in place as audit-trail evidence.

## Next-run hygiene

Future audits should run before every public-visibility flip and any time
the repo is mirrored to a new organisation. Consider adding a
`.gitleaks.toml` allowlist for the four false-positive paths above so
the CI gate (already wired in `.github/workflows/security.yml`) doesn't
re-flag them on every PR.
