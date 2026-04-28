# Agent E status — Docs + MR drafting

Owner: Agent E (Docs + MR drafting). Plan: `shelf zero-downtime + capacity` (a2fa5fe7).

## State

- 2026-04-28 12:47 IST — kicked off. Read plan, prior cutover commits (rep-0
  `4174d15fe3`, rep-1 `d458f7dda2` / `c4a19ba12f` reverted, rep-2 `649c0732dc`
  landed on `cicd-v2`), and the existing `cutover-rep2.md` template. Confirmed
  current state on `cicd-v2`:
  - rep-0 → direct S3 (`https://s3.ap-south-1.amazonaws.com`)
  - rep-1 → direct S3 (cutover landed and reverted)
  - rep-2 → shelf-2 per-pod (`shelf-2.shelf.alluxio.svc.cluster.local:9092`)
  - rep-3 → direct S3
  All 4 cutover branches flip the cdp `s3.endpoint` to
  `http://shelf-pool.shelf.svc.cluster.local:9092` (Stage 1 chart drop-in is
  the prerequisite that ships that ClusterIP svc).
- 2026-04-28 13:05 IST — 4 deployments-repo branches committed locally:
  - `shelf-cutover-rep3` (commit `c69546f3c9`) — Stage 5.1 / 30 min soak
  - `shelf-cutover-rep2` (commit `1fb60deda2`) — Stage 5.2 / 30 min soak (per-pod → cluster-svc)
  - `shelf-cutover-rep1` (commit `683c2197e3`) — Stage 5.3 / 60 min soak (write path)
  - `shelf-cutover-rep0` (commit `cade9bbbd3`) — Stage 5.4 / 90 min soak (high-concurrency)
  Each branch contains exactly 1 commit beyond `origin/cicd-v2`.
- 2026-04-28 13:20 IST — 4 cutover runbooks written at
  `shelf/docs/rollout-v1/cutover-rep{0,1,2,3}.md`. Each carries: pre-cutover
  checklist (image+helm rev locked, no concurrent MRs, smoke harness PASS,
  pin-list pre-warm), `<TBD-after-push>` MR placeholder, replica-specific
  soak duration, monitoring SQL against `cdp.trino_logs.trino_queries`
  (failed-query rate, P95/P99 wall time, error_code histogram), PASS
  criteria (P95/P99 ≤ 1.2× baseline / hard fail at 2×, zero new failure
  classes), 3-5 min rollback procedure with smoke-harness re-run, and
  T+30 min post-cutover validation.
- 2026-04-28 13:25 IST — `shelf/docs/rollout-v1/locked-window-template.md`
  written. Stage 2 defaults inline; reusable for any future locked
  A/B (SHELF-23 validation, picker re-test, etc.).
- 2026-04-28 13:35 IST — `shelf/shelfd/docs/design-notes/SHELF-24-s3-fallback-passthrough.md`
  written. In-flight passthrough scope; out-of-scope cases (shelf-pool
  unreachable, write-path) explicit; ~2 d estimate; 3 open questions
  (cache-fill-on-success, peer-broadcast invalidation, default-on flag).

## Deliverables

| # | Item | State |
|---|------|-------|
| 1a | Local branch `shelf-cutover-rep3` in deployments-repo (`c69546f3c9`) | DONE |
| 1b | Local branch `shelf-cutover-rep2` in deployments-repo (`1fb60deda2`) | DONE |
| 1c | Local branch `shelf-cutover-rep1` in deployments-repo (`683c2197e3`) | DONE |
| 1d | Local branch `shelf-cutover-rep0` in deployments-repo (`cade9bbbd3`) | DONE |
| 2a | `shelf/docs/rollout-v1/cutover-rep3.md` | DONE |
| 2b | `shelf/docs/rollout-v1/cutover-rep2.md` | DONE |
| 2c | `shelf/docs/rollout-v1/cutover-rep1.md` | DONE |
| 2d | `shelf/docs/rollout-v1/cutover-rep0.md` | DONE |
| 3 | `shelf/docs/rollout-v1/locked-window-template.md` | DONE |
| 4 | `shelf/shelfd/docs/design-notes/SHELF-24-s3-fallback-passthrough.md` | DONE |

## Constraints honored

- No `git push`, no `gh pr create`, no `glab mr create`. Local commits only.
- No helm upgrade.
- Plan file at `/Users/aamir/.cursor/plans/shelf_zero-downtime_+_capacity_a2fa5fe7.plan.md` not edited.
- HEREDOC commit messages.
- `deployments-repo` HEAD restored to `feat/trino-replica-0-cdp-shelf` (the
  branch the operator was on before this work).
- `trino` repo work landed on a fresh branch `shelf-docs-stage5` (per task
  default).

## Handoff to Conductor A

- **Push order at cutover time**: rep-3 → rep-2 → rep-1 → rep-0. After each
  push, open the MR, fill in the runbook's `<TBD-after-push>` placeholder
  with the MR ID, then proceed with the runbook's pre-cutover checklist.
- **Soak overlap rule**: do not start the next replica's pre-cutover
  checklist until the current replica's soak has passed AND its
  T+30 min post-cutover validation is green. Stage 5 PASS criteria
  carry forward (P8 in each runbook checks that already-on-shelf
  replicas don't lose more than 5 pp hit-ratio during the new
  replica's soak).
- **If pin-list / smoke harness from Agent D land late**: each
  runbook's pre-cutover checklist already references the tools by
  path and notes "see when committed" so Conductor A can verify
  presence at T-1h.
- **If SHELF-23 (Agent C) lands late**: each runbook's row 6/7
  flags SHELF-23 as a hard gate. Cutovers must wait. The plan
  already names this dependency at Stage 1b.
