# Per-ticket prompt template (SHELF-30 .. SHELF-49)

This template is the canonical handoff prompt the orchestrator pastes into a
fresh agent invocation for one SHELF-NN ticket. Linked from
`/Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md`
(Multi-agent execution model § per-ticket prompt template).

---

## Template (copy verbatim, fill `<...>` placeholders)

```
You are implementing SHELF-<NN>: <one-line ticket title>.

Plan reference (load-bearing — read first):
  /Users/aamir/.cursor/plans/shelf_algorithmic_optimization_roadmap_e40a11c9.plan.md

Workspace memory (mandatory pre-read):
  /Users/aamir/trino/AGENTS.md  →  Learned Workspace Facts → Shelf section.
  Skim sibling Pre-flight items: ADR-0011 (cache-key spec), ADR-0012
  (Trino read-path endpoint swap), recent SHELF-21..29 incident notes.

Bootstrap checklist (do all five before touching code):
  1. cd /Users/aamir/trino/shelf && git status
     - Tree MUST be clean on `main`. If not, STOP and report.
       Workspace policy bans inheriting another agent's WIP tree.
  2. git fetch origin && git pull --ff-only origin main
  3. git worktree list
     - If a sibling agent already owns a worktree on a SHELF-<NN>* branch,
       STOP and report — do NOT race them on the same branch.
  4. Read the SHELF-<NN> body in the plan file. Note its rollback-signal
     table; you will fill it with live numbers in the smoke phase.
  5. If the ticket is in {SHELF-34, SHELF-42, SHELF-46}, also read its
     Threat Model section. The sidecar security-review gate is a hard merge
     blocker.

Branching:
  Branch name:    shelf-<NN>-<kebab-title>
  Off:            origin/main
  Worktree path:  /private/tmp/shelf-<NN>-<short-id>   (mandatory if any
                  sibling agent is concurrently active; otherwise in-tree
                  is fine)

Implementation constraints (agentless quality bar):
  a. Cargo + Helm version bumps must move together. shelfd/Cargo.toml
     `version = "X.Y.Z"`, charts/shelf/Chart.yaml `appVersion: X.Y.Z`,
     and `version: X.Y.Z` for the chart. Mismatched versions fail
     the in-cluster operator's image-vs-chart check.
  b. Format:  cargo fmt --all
  c. Lint:    cargo clippy -p shelfd --all-targets -- -D warnings
              (workspace-wide clippy fails on pre-existing
              shelfctl/install.rs from PR #33 — explicitly skip
              `cargo clippy --workspace`. The shelfd-scoped command above
              is the contract.)
  d. Unit:    cargo test -p shelfd --lib
  e. Integration (if the ticket boots shelfd / MinIO / Foyer):
              SHELF_INTEGRATION=1 cargo test -p shelfd --test it_<name> -- --nocapture
              Without `SHELF_INTEGRATION=1` the suite exits in 0.00s
              pretending to pass — that is the documented SHELF-09 trap.
              Paste the ACTUAL wall-time + pass-count line into the PR
              description, not a stock "tests pass" claim.
  f. Public ADR:  if SHELF-<NN> ∈ {30, 32, 33, 36, 37, 38, 46, 47}, write
              `shelf/agents/out/adr/00NN-<kebab-title>.md` BEFORE opening
              the implementation PR. Voice: ADR-0011 / ADR-0012. Cite
              primary research (paper / Trino PR / Foyer doc) and the
              workspace-memory rule it complies with.

Image build (GitLab first, GHCR second):
  Tag scheme:  1.0.0-rc.<N>  (orchestrator allocates N)
  GitLab:      registry.gitlab.com/penpencil-services/data/data-engineering/ranger/shelfd:1.0.0-rc.<N>
  GHCR:        ghcr.io/shelf-project/shelfd:1.0.0-rc.<N>
  Platform:    linux/amd64 only is acceptable for an rc; multi-arch is
               only required at the final v1.x.y tag.
  Build host:  use the alluxio-pool builder if QEMU-on-Mac would push past
               GHA's 90 min cap (workspace memory: arm64 emulated
               aws-sdk-s3 builds time out).

Deploy step (in-cluster STS YAML is authoritative — NOT Helm):
  kubectl --context infra:data-platform-cluster -n alluxio set image \
    statefulset/shelf shelfd=ghcr.io/shelf-project/shelfd:1.0.0-rc.<N>
  Watch:
    kubectl -n alluxio rollout status statefulset/shelf -w
    kubectl -n alluxio get pods -l app=shelf -o wide
  Zero-downtime rule (workspace memory): the old pod MUST stay Ready until
  the new pod is Ready. If the rollout pauses or surge-zero is observed,
  STOP and revert to the previous image tag.

Smoke step (90-min watch with the ticket's rollback-signal table active):
  - Pin the cutover window in your status update: (start_ist, end_ist,
    image_tag, replica, hit_ratio_floor, p99_ceiling).
  - During the window: NO Trino coord restarts, max 1 helm upgrade,
    avoid 09:00-11:00 IST traffic peak unless the orchestrator waives.
  - Probe loop: every 3 min for 90 min, write to
    /tmp/shelf-<NN>-soak.tsv with timestamp + RSS + restart_count +
    rolling_hit_ratio_bps + lodc_drops_total{reason} delta.
  - Use `block_until_ms: 0` for the long probe loop and read
    ~/.cursor/projects/Users-aamir-trino/terminals/<id>.txt to check
    progress — `nohup ... &` from a single Shell call does NOT survive
    across calls (workspace memory).
  - If ANY entry in the rollback-signal table fires: revert image tag
    immediately and write the trigger evidence into the ticket handoff
    file.

Final report shape (paste verbatim into the PR description):

  | Field                          | Value                                |
  |--------------------------------|--------------------------------------|
  | Ticket                         | SHELF-<NN>                           |
  | Branch SHA                     | <git rev-parse HEAD>                 |
  | Image tag                      | 1.0.0-rc.<N>                         |
  | Image digest (GHCR)            | sha256:<...>                         |
  | Cutover window (IST)           | <start> -> <end>                     |
  | Replica                        | rep-<n>                              |
  | Hit ratio start / 12h          | <bps> / <bps>                        |
  | p50 / p99 read latency 12h     | <ms> / <ms>                          |
  | shelf_lodc_drops_total delta   | <count> over <duration>              |
  | RSS peak (any pod)             | <GiB>                                |
  | Rollback fired?                | yes / no  (+ trigger row if yes)     |
  | SHELF_INTEGRATION=1 result     | <wall-time>, <N passed / 0 failed>   |
  | Open follow-ups                | <SHELF-NN bullets>                   |

Hand-off file (mandatory, last action before the agent exits):
  Write to:   shelf/agents/out/SHELF-<NN>/handoff.md
  Contents:   ticket id, branch SHA, image digest, smoke result table
              (the verbatim block above), open follow-ups, links to
              dashboards used during the soak.
  The next agent (orchestrator or smoke-watcher) reads this file before
  starting its phase.

Hard rules:
  - DO NOT auto-merge the PR. Open with `--draft` if CI is still settling.
  - DO NOT push direct to main. DO NOT amend the orchestrator's commits.
  - DO NOT edit the plan file's `todos:` block — only the orchestrator
    flips `status:` fields.
  - DO NOT run more than ONE `cargo build --release` concurrently with
    another hot-path Rust builder (Cargo.lock collisions; SHELF-23
    cherry-pick saga is the cautionary tale).
  - DO NOT skip the SHELF_INTEGRATION=1 gate even if the unit tests pass.

Begin.
```

---

## Notes for the orchestrator (do not paste into worker prompt)

- Allocate `rc.<N>` monotonically across the SHELF-30..49 batch; do not
reuse digits between sibling tickets.
- For the conditional levers (SHELF-31, SHELF-37, SHELF-38), skip
dispatching the worker until the gating condition has been observed
(SHELF-29 7-day soak / SHELF-23 7-day balance / OOMKill recurrence).
- For SHELF-39, the upstream prerequisite (Trino PR #29182) merged
2026-04-27 and is in 481+. Confirm replica's running Trino version
before dispatching the worker.

