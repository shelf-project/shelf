# Agent C — SHELF-23 status

Branch: `shelf-23-peer-fetch` (off `rep2-shelf-integration`).
Plan: Stage 1b in `/Users/aamir/.cursor/plans/shelf_zero-downtime_+_capacity_a2fa5fe7.plan.md`.
Estimated effort: 3–5 days.

## Deliverables (from prompt)

- [x] D1 — Wire peer-fetch into `s3_shim::handle_get_object` (HRW
  primary-vs-self branch, race against origin) — landed
  `SHELF-23: wire peer-fetch into s3_shim::handle_get_object (D1)`.
- [x] D2 — Same logic in `store::get_or_fetch` (metadata-prefetch path)
  — landed via `peer_fetch` extraction +
  `GET /cache/:pool/:key/:range` wrapping
  (commit `SHELF-23: extract peer_fetch module + wire into /cache/* (D2)`).
- [x] D3 — Cross-pod write coherence via `If-None-Match` conditional
  GET, with a freshness-window optimisation — landed
  (commit `SHELF-23: ETag-conditional GET + freshness window (D3)`).
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

Session 1 outcomes:

- Added `shelf_peer_{hit,miss,timeout,error}_total` counters in
  `metrics.rs` (commit `SHELF-23: race_peer_or_origin primitive +
  shelf_peer_*_total counters`).
- Implemented `peer::race_peer_or_origin` with 7 race-test cases
  covering hit / miss / probe-timeout / probe-error / body-error /
  unreachable-peer / origin-wins (same commit).
- Wired `peer_or_origin_fetch` into `s3_shim::handle_get_object`,
  including HRW self-check, runtime kill-switch
  (`SHELFD_PEER_FETCH_ENABLED`), peer-port translation
  (Member::endpoint carries data_port 9092; /cache/* lives on
  stats_port 9090), and `peer_http: reqwest::Client` plumbed
  through `ServerState` (commit `SHELF-23: wire peer-fetch into
  s3_shim::handle_get_object (D1)`).
- All 193 lib tests + every existing integration suite (`it_admin`,
  `it_shim_write`, `it_traces`, `it_read_path`, `it_hybrid_pool`,
  `it_ui`, `smoke`) green.

### 2026-04-28 (day 2)

Outcomes:

- D2 closed via the cleaner layering: `peer_or_origin_fetch` lifted
  into a new `shelfd::peer_fetch` module, then wired into the
  `GET /cache/:pool/:key/:range` handler in `http.rs` next to the
  existing s3-shim wiring. Recursion guard via
  `x-shelf-peer-fetch: 1` header — the receiving pod recognises an
  inbound peer hop and skips its own peer-fetch wrapping so a hop
  never bounces off a third pod (`peer.rs::peer_body_fetch` sets
  the header, `http::handlers::get_cache` reads it).
- D3 closed: ETag-conditional GET on every local cache hit with a
  freshness-window short-circuit.
  - New trait method `Origin::get_range_conditional(... if_none_match)`;
    `S3Origin` impl uses `aws_sdk_s3 ... .if_none_match(...)` and
    branches `SdkError::raw_response().status() == 304` into
    `ConditionalGet::NotModified` (no body collected). 304 emits a
    `s3.get_object_conditional` Tempo span; `record_origin` splits
    ok / not_modified / error / timeout.
  - New `shelfd::freshness::FreshnessTracker` (foyer-backed,
    keyed `(bucket, s3_key)` like `head_lru`). Defaults: `N = 10`
    consecutive 304s, `T = 5s` trust window. Setting either to 0
    is a kill-switch (always validate).
  - `s3_shim::handle_get_object` now branches into a conditional
    block when `state.is_conditional_get_enabled()` and the
    HEAD-LRU's `meta.etag` is present and the cache is hit:
    - freshness window open → serve cached, bump
      `shelf_conditional_skipped_total`.
    - 304 → serve cached, bump `not_modified_total`,
      `freshness.record_not_modified`.
    - 200 → invalidate `head_lru`, `freshness.record_modified`,
      repopulate cache under new content-addressed key (best-effort
      via `get_or_fetch` + admission), serve fresh body, bump
      `modified_total`.
    - error → fall through to the normal `get_or_fetch` path; bump
      `conditional_error_total`.
  - 4 new Prometheus counters: `shelf_conditional_{not_modified,
    modified,skipped,error}_total{pool}`. Listed in
    `EXPOSED_SERIES`; registered in `metrics::tests`.
- ServerState gained `freshness: Arc<FreshnessTracker>`,
  `conditional_get_enabled: AtomicBool` (runtime kill-switch) +
  `is_conditional_get_enabled` / `set_conditional_get_enabled`.
- 204 lib tests pass (+7 from `freshness::tests`, +4 from
  `peer_fetch::tests`); it_admin / it_shim_write / it_traces /
  it_read_path / it_hybrid_pool / smoke all green.

Day 3 (planned):

- D4: integration test against 3 in-process mock peers
  (`tests/it_peer_fetch.rs`; axum-based; full kind cluster only if
  the in-process mock proves insufficient — the prompt allows
  either).
- D5–D6: version bump (`0.1.0-preview-9`), multi-arch image build,
  draft MR.

Pre-existing clippy debt observed on the baseline branch (NOT
introduced by SHELF-23): 6 errors in `compression.rs`,
`config.rs`, `fingerprint.rs`, `http.rs:1238` (now `:1340` after
my edits), `side_bloom.rs`. The prompt requires
`cargo clippy --all-targets -- -D warnings` to be green before MR
open, so these will need a small "lint cleanup" commit on this
branch before the draft MR opens.
