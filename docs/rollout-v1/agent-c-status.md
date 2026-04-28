# Agent C — SHELF-23 status

Branch: `shelf-23-peer-fetch` (off `rep2-shelf-integration`).
Plan: Stage 1b in `/Users/aamir/.cursor/plans/shelf_zero-downtime_+_capacity_a2fa5fe7.plan.md`.
Estimated effort: 3–5 days.

## Deliverables (from prompt)

- [ ] D1 — Wire peer-fetch into `s3_shim::handle_get_object` (HRW
  primary-vs-self branch, race against origin).
- [ ] D2 — Same logic in `store::get_or_fetch` (metadata-prefetch path).
- [ ] D3 — Cross-pod write coherence via `If-None-Match` conditional
  GET, with a freshness-window optimisation.
- [ ] D4 — Integration test on 3-pod kind / mock peers
  (`shelfd/tests/it_peer_fetch.rs`).
- [ ] D5 — Image build `0.1.0-preview-9`, push to GitLab Container
  Registry. (Bumps: `shelfd/Cargo.toml`, `charts/shelf/Chart.yaml`
  `appVersion`.)
- [ ] D6 — Draft MR (do-not-merge) on `shelf-23-peer-fetch`.
- [x] D7 — Design notes:
  `shelfd/docs/design-notes/SHELF-23-peer-fetch-and-coherence.md`.

## Structural notes / blockers

- **Existing `peer.rs` ships only the probe + decision primitives**
  (`probe_peer_contains`, `peer_is_better`). The plan refers to
  `race_peer_or_origin` as already designed; it does not exist. SHELF-23
  ships it as part of D1 (see design notes §D2).
- **Router public API.** `router::Router::primary_for` (per the prompt)
  does not exist; the equivalent is `router::Router::owner(key)`. No
  blocker; the design notes use the real name.
- **Branch base.** `rep2-shelf-integration` carries large in-flight
  changes (peer.rs is itself untracked, plus s3_shim/store/metrics
  modifications), so I cannot branch from `main` without losing the
  primitives this work depends on. Branch is taken from current HEAD;
  MR base will be `rep2-shelf-integration` until Conductor A signals
  otherwise.

## Session log

### 2026-04-28 (day 1)

- Read full plan + existing `peer.rs`, `router.rs`, `membership.rs`,
  `s3_shim.rs::handle_get_object/handle_put_object`,
  `head_lru.rs`, `origin.rs::Origin trait`, `store.rs::get_or_fetch`.
- Confirmed baseline `cargo check -p shelfd` is clean on the dirty
  in-flight tree.
- Wrote design notes (D7).
- Cut branch `shelf-23-peer-fetch` from `rep2-shelf-integration` HEAD.

Pending today (target stop point for session 1):

- Add `shelf_peer_*` Prometheus counters in `metrics.rs`.
- Implement `peer::race_peer_or_origin` with unit tests.
- Wire `race_peer_or_origin` into `s3_shim::handle_get_object`.
- Run `cargo fmt --all && cargo clippy -p shelfd --all-targets -- -D warnings && cargo test -p shelfd`.
- Commit-by-commit, each individually testable.

Day 2–5 (deferred):

- D2: same peer-fetch wiring inside `store::get_or_fetch`.
- D3: ETag-conditional GET with freshness window (this is the largest
  single chunk of work — ~1 day on its own).
- D4: integration test against 3 in-process mock peers (axum-based;
  kind cluster optional follow-up).
- D5–D6: version bump, image build, draft MR.
